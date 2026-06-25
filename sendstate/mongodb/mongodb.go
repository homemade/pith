// Package mongodb provides a Mongo-backed [sendstate.Store] for
// multi-instance (cross-process) use — the shared-backing counterpart to
// the in-process pith/sendstate/memory store. It mirrors the memory store's
// two-store model as two collections with different lifecycles:
//
//   - entries — TTL'd working state that drives the policies. One
//     document per key, expired by a TTL index on expireAt.
//   - metrics — permanent lifetime rollup (observability). Never
//     expires.
//
// Design (see the sendstate package doc):
//
//   - Sliding TTL via an explicit expireAt date + a TTL index with
//     expireAfterSeconds: 0, refreshed on every write.
//   - TTL-honoring reads: every entry read filters expireAt > now, so an
//     expired-but-unswept document reads as absent. Deletion is pure
//     storage reclamation and never affects an answer, so this backend
//     matches the memory store regardless of when Mongo's background
//     deleter runs.
//   - One atomic op drives a CheckAndReserve: policies read entries and
//     reserve send-slots in the same findOneAndUpdate; metrics is
//     observability-only.
//
// Cross-instance correctness depends on the *mongo.Client (or one of its
// derived [*mongo.Database] / [*mongo.Collection] handles) using majority
// write concern, so a RecordAsSent is visible to the next instance's
// ReadEntry. Configure that on the client before passing it to the
// protect/mongodb constructors — a w:1 default can let a racing instance
// miss a just-recorded send.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/protect"
	"github.com/homemade/pith/sendstate"
)

// entryDoc is the entries-collection document. sendstate.Entry stays
// backend-agnostic (no bson tags), so the store defines its own shape
// and converts. lastSentAt/lastDeferredAt are scalar tails of the
// timestamp lists, denormalized on each write so [Store.RangeDeferred]
// can filter ("pending": deferred newer than sent) and sort without
// reaching into the arrays.
type entryDoc struct {
	Key                    string      `bson:"_id"`
	ContentHash            string      `bson:"contentHash,omitempty"`
	LastDeferredMessageRef []byte      `bson:"lastDeferredMessageRef,omitempty"`
	LastNSendTimes         []time.Time `bson:"lastNSendTimes,omitempty"`
	LastNDeferredTimes     []time.Time `bson:"lastNDeferredTimes,omitempty"`
	LastSentAt             time.Time   `bson:"lastSentAt,omitempty"`
	LastDeferredAt         time.Time   `bson:"lastDeferredAt,omitempty"`
	Tenant                 string      `bson:"tenant,omitempty"`
	Namespace              string      `bson:"namespace,omitempty"`
	ExpireAt               time.Time   `bson:"expireAt"`
}

func (d entryDoc) entry() sendstate.Entry {
	return sendstate.Entry{
		ContentHash:            d.ContentHash,
		LastDeferredMessageRef: d.LastDeferredMessageRef,
		LastNSendTimes:         d.LastNSendTimes,
		LastNDeferredTimes:     d.LastNDeferredTimes,
	}
}

// metricsDoc is the metrics-collection document — the lifetime rollup.
type metricsDoc struct {
	Key             string    `bson:"_id"`
	Tenant          string    `bson:"tenant,omitempty"`
	Namespace       string    `bson:"namespace,omitempty"`
	TotalSent       uint64    `bson:"totalSent"`
	TotalDeferred   uint64    `bson:"totalDeferred"`
	FirstSentAt     time.Time `bson:"firstSentAt,omitempty"`
	FirstDeferredAt time.Time `bson:"firstDeferredAt,omitempty"`
	LastSentAt      time.Time `bson:"lastSentAt,omitempty"`
	LastDeferredAt  time.Time `bson:"lastDeferredAt,omitempty"`
	Peak1h          uint64    `bson:"peak1h,omitempty"`
	Peak1hAt        time.Time `bson:"peak1hAt,omitempty"`
	Peak24h         uint64    `bson:"peak24h,omitempty"`
	Peak24hAt       time.Time `bson:"peak24hAt,omitempty"`
}

func (d metricsDoc) metrics() sendstate.Metrics {
	return sendstate.Metrics{
		Tenant:          d.Tenant,
		Namespace:       d.Namespace,
		TotalSent:       d.TotalSent,
		TotalDeferred:   d.TotalDeferred,
		FirstSentAt:     d.FirstSentAt,
		FirstDeferredAt: d.FirstDeferredAt,
		LastSentAt:      d.LastSentAt,
		LastDeferredAt:  d.LastDeferredAt,
		Peak1h:          d.Peak1h,
		Peak1hAt:        d.Peak1hAt,
		Peak24h:         d.Peak24h,
		Peak24hAt:       d.Peak24hAt,
	}
}

// Compile-time check that Store satisfies [sendstate.Store].
var _ sendstate.Store = (*Store)(nil)

// Store is a Mongo-backed [sendstate.Store] — the cross-process
// counterpart to [pith/sendstate/memory.Store]. State lives in two
// collections on the [*mongo.Database] passed to [New]:
//
//   - entries — TTL'd working state, one document per key.
//   - metrics — permanent lifetime rollup (never expires).
//
// Typical callers don't construct a Store directly: the
// [pith/protect/mongodb.NewReadProtector] / [pith/protect/mongodb.NewWriteProtector]
// factories build one internally over a caller-owned [*mongo.Client], call
// [Store.EnsureIndexes], and derive [WithMaxSendTimes] from the largest attached
// Coalescer hardCap (so the storage-side bound can't be forgotten). Direct
// construction via [New] + [Store.EnsureIndexes] is supported for tests or
// callers that need raw store access — in that path the caller is responsible
// for passing [WithMaxSendTimes] explicitly (>= the largest attached Coalescer
// hardCap), or in-window timestamps will be dropped by $slice and caps will
// leak. See the package doc for the majority-write-concern requirement.
type Store struct {
	entries      *mongo.Collection
	metrics      *mongo.Collection
	holds        *mongo.Collection // per-tenant append-only hold audit log; one doc per tenant (_id = tenant)
	ttl          time.Duration
	maxSendTimes int // bounds lastNSendTimes/lastNDeferredTimes; must be >= the largest Coalescer hardCap
}

// Option configures a [Store].
type Option func(*Store)

// WithMaxSendTimes bounds each key's lastNSendTimes list. Set it >= the
// largest attached Coalescer hardCap. Unlike the in-memory store, the
// [pith/protect/mongodb] factories do NOT auto-size this backend through
// a structural GrowMaxSendTimes hook (this Store deliberately doesn't
// expose one) — they derive the cap from the attached Coalescers and
// pass it via this option. Callers constructing the Store directly are
// responsible: too small a bound drops in-window timestamps via $slice,
// making [sendstate.Entry.CountSentInWindow] undercount and the cap leak.
func WithMaxSendTimes(n int) Option { return func(s *Store) { s.maxSendTimes = n } }

// New returns a Mongo-backed Store over the given database. entryTTL is
// required (mirroring pith/sendstate/memory.New — pith holds no default TTL) and
// MUST be >= the largest Coalescer window in use; the [pith/protect/mongodb]
// factories validate that against the EntryTTL reported here. Panics if
// entryTTL <= 0.
//
// The database's read/write concern is the caller's to set — see the
// package doc on majority write concern for cross-instance correctness.
func New(db *mongo.Database, entryTTL time.Duration, opts ...Option) *Store {
	if entryTTL <= 0 {
		panic("mongodb: New requires a positive entryTTL")
	}
	s := &Store{
		entries: db.Collection("entries"),
		metrics: db.Collection("metrics"),
		holds:   db.Collection("holds"),
		ttl:     entryTTL,
	}
	for _, o := range opts {
		o(s)
	}
	if s.maxSendTimes == 0 {
		s.maxSendTimes = 1 // dedupe-only floor; protect-driven callers pass the cap
	}
	return s
}

// EntryTTL reports the configured TTL so the [pith/protect/mongodb]
// factories can validate it against the attached Coalescer windows.
func (s *Store) EntryTTL() time.Duration { return s.ttl }

// Config bundles the parameters the protect/mongodb constructors need to
// build a Store on a caller-owned [*mongo.Client]. Database is deployment
// config (typically read from the environment); EntryTTL and MaxSendTimes
// are program config (typically code constants).
//
// The client itself is the caller's responsibility — open it with
// [mongo.Connect] (configured with majority write concern; see the package
// doc) and pass it into [pith/protect/mongodb.NewReadProtector] /
// [pith/protect/mongodb.NewWriteProtector]. Connection-level concerns (URI,
// timeouts, write concern) belong on the client, not on this Config — a
// process sharing one client across multiple pith stores (and/or other
// libraries) configures them once at the client.
//
// No JSON tags by design: callers serialize their own env-var shape
// and map fields onto Config. That keeps pith decoupled from the
// caller's env-var convention (flat vs composite JSON vs anything
// else).
type Config struct {
	// Database is the database name within the cluster. Required.
	Database string

	// EntryTTL is the working-state TTL. Required and must be >= the
	// largest attached Coalescer window; the [pith/protect/mongodb]
	// factories validate this.
	EntryTTL time.Duration

	// MaxSendTimes bounds lastNSendTimes via $slice. Pass the largest
	// attached Coalescer hardCap; too small a value drops in-window
	// timestamps and caps will leak. Zero defers to the New default
	// (dedupe-only floor of 1) — only sensible for callers running
	// dedupe alone.
	MaxSendTimes int
}

// EnsureIndexes creates the TTL index on expireAt, the lastDeferredAt index
// that serves an unscoped [Store.RangeDeferred] sort, and the compound
// {namespace, lastDeferredAt} index that serves a namespace-scoped sort.
// Idempotent; call once at startup.
func (s *Store) EnsureIndexes(ctx context.Context) error {
	_, err := s.entries.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "expireAt", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(0),
	})
	if err != nil {
		return err
	}
	// Serves an unscoped RangeDeferred's {lastDeferredAt: 1} sort+limit;
	// the pending $expr predicate is a cheap residual on the bounded scan.
	_, err = s.entries.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "lastDeferredAt", Value: 1}},
	})
	if err != nil {
		return err
	}
	// Serves a namespace-scoped RangeDeferred: equality on namespace leads, so
	// lastDeferredAt still serves the sort+limit within the namespace (ESR).
	_, err = s.entries.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "namespace", Value: 1}, {Key: "lastDeferredAt", Value: 1}},
	})
	return err
}

// RecordAsSent appends the send timestamp (bounded to the last
// maxSendTimes via $slice), stamps the scalar lastSentAt tail,
// refreshes the TTL, and bumps the lifetime counters. It touches only
// send-side state — the deferred ref / lastNDeferredTimes /
// lastDeferredAt are left intact (recency makes any prior deferral no
// longer pending).
//
// Two round-trips: entries update, then metrics update. Eventually
// consistent across the two collections; only entries drives
// decisions, so this never affects an answer.
func (s *Store) RecordAsSent(ctx context.Context, key, tenant, namespace, contentHash string) error {
	now := time.Now()

	// Dedupe-only floor: at maxSendTimes == 1 the send-time list holds a
	// single element, so any rolling-window count is a constant 1 and the
	// peak high-water marks convey nothing. Keep the cheap path — a plain
	// UpdateByID entry write (no read-back) and classic-operator metrics (no
	// pipeline) — and skip the peaks. maxSendTimes only rises above the floor
	// when a windowing policy is in use (see GrowMaxSendTimes), which is
	// exactly when the peaks become meaningful.
	if s.maxSendTimes <= 1 {
		_, err := s.entries.UpdateByID(ctx, key, bson.M{
			"$set":  bson.M{"contentHash": contentHash, "lastSentAt": now, "tenant": tenant, "namespace": namespace, "expireAt": now.Add(s.ttl)},
			"$push": bson.M{"lastNSendTimes": bson.M{"$each": bson.A{now}, "$slice": -s.maxSendTimes}},
		}, options.UpdateOne().SetUpsert(true))
		if err != nil {
			return err
		}
		_, err = s.metrics.UpdateByID(ctx, key, bson.M{
			"$inc": bson.M{"totalSent": 1},
			"$set": bson.M{"lastSentAt": now, "tenant": tenant, "namespace": namespace},
			"$min": bson.M{"firstSentAt": now},
		}, options.UpdateOne().SetUpsert(true))
		return err
	}

	// Windowed mode: FindOneAndUpdate (not UpdateByID) so we read back the
	// post-$push, post-$slice send-time list to count the rolling windows
	// below. Same write and round-trip — it just returns the document.
	var doc entryDoc
	err := s.entries.FindOneAndUpdate(ctx, bson.M{"_id": key}, bson.M{
		"$set":  bson.M{"contentHash": contentHash, "lastSentAt": now, "tenant": tenant, "namespace": namespace, "expireAt": now.Add(s.ttl)},
		"$push": bson.M{"lastNSendTimes": bson.M{"$each": bson.A{now}, "$slice": -s.maxSendTimes}},
	}, options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After)).Decode(&doc)
	if err != nil {
		return err
	}

	// Peak high-water marks: the count is taken at this send (always at an
	// arrival, where a trailing-window count peaks), so the running maximum
	// over all sends is the exact rolling-window peak. int64 avoids uint64
	// BSON encoding quirks and decodes cleanly into the uint64 doc fields.
	ent := doc.entry()
	// A window's peak is folded ONLY when EntryTTL covers it. Below that the
	// trailing count is a misleading lower bound — the entry expires after a
	// gap shorter than the window, so the "peak" silently resets on ordinary
	// idle periods. The field is then left unset (omitempty → absent), which a
	// reader can tell apart from a real 0. (E.g. a 2h-TTL store folds peak1h
	// but skips peak24h.)
	set := bson.M{
		"tenant":      tenant,
		"namespace":   namespace,
		"totalSent":   bson.M{"$add": bson.A{bson.M{"$ifNull": bson.A{"$totalSent", 0}}, 1}},
		"lastSentAt":  now,
		"firstSentAt": bson.M{"$min": bson.A{"$firstSentAt", now}},
	}

	// A pipeline update (one $set stage) rather than classic operators: a
	// bare $max can fold the peak magnitude but cannot also stamp the
	// peak…At timestamp conditionally — that needs $cond, which only exists
	// in aggregation expressions and can't be mixed with $inc/$set/$min. All
	// expressions in the stage read the pre-stage document, so each peak…At
	// compares the incoming count against the *previously stored* peak.
	//   - $add/$ifNull reproduces the old $inc on totalSent.
	//   - $min on firstSentAt keeps set-once semantics (missing field → now;
	//     otherwise the earlier stored value wins).
	//   - strict $gt stamps peak…At only when the peak rises, so it marks the
	//     first time a level is reached and is not moved by later ties.
	if s.ttl >= time.Hour {
		sent1h := int64(ent.CountSentInWindow(now, time.Hour))
		set["peak1h"] = bson.M{"$max": bson.A{bson.M{"$ifNull": bson.A{"$peak1h", 0}}, sent1h}}
		set["peak1hAt"] = bson.M{"$cond": bson.A{
			bson.M{"$gt": bson.A{sent1h, bson.M{"$ifNull": bson.A{"$peak1h", 0}}}},
			now, "$peak1hAt",
		}}
	}
	if s.ttl >= 24*time.Hour {
		sent24h := int64(ent.CountSentInWindow(now, 24*time.Hour))
		set["peak24h"] = bson.M{"$max": bson.A{bson.M{"$ifNull": bson.A{"$peak24h", 0}}, sent24h}}
		set["peak24hAt"] = bson.M{"$cond": bson.A{
			bson.M{"$gt": bson.A{sent24h, bson.M{"$ifNull": bson.A{"$peak24h", 0}}}},
			now, "$peak24hAt",
		}}
	}

	_, err = s.metrics.UpdateByID(ctx, key, mongo.Pipeline{{{Key: "$set", Value: set}}}, options.UpdateOne().SetUpsert(true))
	return err
}

// RecordAsDeferred stamps the breadcrumb, appends the deferral
// timestamp (and scalar lastDeferredAt tail), stamps the tenant +
// sweep-scoping namespace, and refreshes the TTL — without touching
// contentHash, lastNSendTimes, or lastSentAt; then bumps the
// deferred-side counters.
func (s *Store) RecordAsDeferred(ctx context.Context, key, tenant, namespace string, messageRef []byte) error {
	now := time.Now()

	// tenant + namespace are set unconditionally (constant per key, ""
	// leaves them unscoped). Both survive across send/defer transitions —
	// RecordAsSent re-stamps the same passed values.
	_, err := s.entries.UpdateByID(ctx, key, bson.M{
		"$set":  bson.M{"lastDeferredMessageRef": messageRef, "lastDeferredAt": now, "tenant": tenant, "namespace": namespace, "expireAt": now.Add(s.ttl)},
		"$push": bson.M{"lastNDeferredTimes": bson.M{"$each": bson.A{now}, "$slice": -s.maxSendTimes}},
	}, options.UpdateOne().SetUpsert(true))
	if err != nil {
		return err
	}

	// $min on firstDeferredAt — set-once semantics symmetric with
	// firstSentAt on RecordAsSent. See that method's comment.
	_, err = s.metrics.UpdateByID(ctx, key, bson.M{
		"$inc": bson.M{"totalDeferred": 1},
		"$set": bson.M{"lastDeferredAt": now, "tenant": tenant, "namespace": namespace},
		"$min": bson.M{"firstDeferredAt": now},
	}, options.UpdateOne().SetUpsert(true))
	return err
}

// CheckAndReserve runs the atomic check-and-reserve aggregation pipeline
// against the entries collection: a single `findOneAndUpdate` computes the
// outcome (Deduped / Deferred / Proceed) from the pre-update document and
// conditionally applies the matching writes inline. The post-update
// document is read back for the outcome fields. The metrics doc is
// updated in a separate round-trip (same shape as RecordAsSent —
// observability writes never block the gate).
//
// Pipeline shape (compact):
//
//	[ {$set: outcomeFlag, outcomeReason}   // computes the policy verdict
//	, {$set: entry writes + reservedAt}    // conditional pushes / sets
//	]
//
// The pre-update document state drives every flag, so concurrent reserves
// serialise on the single-document write lock — the second one's $size
// over $lastNSendTimes already includes the first's just-pushed timestamp.
// Strict cap behaviour follows.
//
// Fail-policy: a backing-store error returns Deferred + non-nil err
// (fail-closed — the replay sweep re-drives). Callers MUST treat the
// Decision regardless of err.
func (s *Store) CheckAndReserve(ctx context.Context, req sendstate.ReserveRequest, messageRef []byte) (sendstate.ReserveResult, error) {
	if len(req.Coalescers) == 0 {
		return sendstate.ReserveResult{Deferred: true, Reason: "configuration error"},
			errors.New("mongodb: CheckAndReserve requires at least one Coalescer")
	}

	deferReasonExpr, err := composeDeferReasonExpr(req.Coalescers)
	if err != nil {
		return sendstate.ReserveResult{Deferred: true, Reason: "configuration error"}, err
	}

	now := time.Now()

	// Dedupe expression: true iff lastContentHash equals the incoming
	// hash AND a send-timestamp is currently recorded for this key. The
	// lastNSendTimes-non-empty guard mirrors the memory backend's
	// [sendstate.Entry.Seen] semantics: contentHash is preserved through
	// a [Store.ReleaseReservation] (release pops the timestamp by value
	// only, not the hash), so without this guard a wire failure followed
	// by an identical retry would be incorrectly dedupe-suppressed. With
	// the guard, an empty lastNSendTimes — whether from a release, a
	// brand-new key, or a TTL-swept entry — leaves the dedupe gate inert
	// and the retry proceeds, matching the record-on-success invariant
	// callers rely on. Read gates pass an empty ContentHash so dedupe is
	// wired off; the Deduped outcome is then unreachable.
	var dedupedExpr any
	if req.ContentHash != "" {
		dedupedExpr = bson.M{"$and": bson.A{
			bson.M{"$eq": bson.A{"$contentHash", req.ContentHash}},
			bson.M{"$gt": bson.A{
				bson.M{"$size": bson.M{"$ifNull": bson.A{"$lastNSendTimes", bson.A{}}}},
				0,
			}},
		}}
	} else {
		dedupedExpr = false
	}

	// Stage 1 — compute the outcome flag + reason as fields on the doc.
	// Order: deduped beats deferred beats proceed (matches CheckAndReserve's
	// layer order). Later stages reference these.
	stage1 := bson.M{
		"_reserveOutcome": bson.M{"$cond": bson.A{
			dedupedExpr, "deduped",
			bson.M{"$cond": bson.A{
				bson.M{"$ne": bson.A{deferReasonExpr, ""}},
				"deferred",
				"proceed",
			}},
		}},
		"_reserveReason": bson.M{"$cond": bson.A{
			dedupedExpr, "duplicate content",
			deferReasonExpr, // "" on Proceed; coalescer name on Deferred
		}},
	}

	// Stage 2 — apply the conditional writes based on the outcome.
	//
	//   - Proceed: push $$NOW onto lastNSendTimes (sliced), set
	//     contentHash (write side only), stamp lastSentAt, snapshot
	//     _reservedAt for the caller.
	//   - Deferred: push $$NOW onto lastNDeferredTimes (sliced), set
	//     lastDeferredMessageRef + lastDeferredAt.
	//   - Deduped: no field writes beyond the always-set scope fields.
	//
	// Every "$cond" branch preserves the pre-update value via $ifNull so
	// upserts of a brand-new doc don't materialise nulls.
	isProceed := bson.M{"$eq": bson.A{"$_reserveOutcome", "proceed"}}
	isDeferred := bson.M{"$eq": bson.A{"$_reserveOutcome", "deferred"}}

	pushedSendTimes := bson.M{"$slice": bson.A{
		bson.M{"$concatArrays": bson.A{
			bson.M{"$ifNull": bson.A{"$lastNSendTimes", bson.A{}}},
			bson.A{"$$NOW"},
		}},
		-s.maxSendTimes,
	}}
	pushedDeferredTimes := bson.M{"$slice": bson.A{
		bson.M{"$concatArrays": bson.A{
			bson.M{"$ifNull": bson.A{"$lastNDeferredTimes", bson.A{}}},
			bson.A{"$$NOW"},
		}},
		-s.maxSendTimes,
	}}

	stage2 := bson.M{
		"tenant":    req.Tenant,
		"namespace": req.Namespace,
		"expireAt":  bson.M{"$add": bson.A{"$$NOW", s.ttl.Milliseconds()}},

		"lastNSendTimes": bson.M{"$cond": bson.A{
			isProceed,
			pushedSendTimes,
			bson.M{"$ifNull": bson.A{"$lastNSendTimes", bson.A{}}},
		}},
		"lastSentAt": bson.M{"$cond": bson.A{
			isProceed, "$$NOW", "$lastSentAt",
		}},
		"_reservedAt": bson.M{"$cond": bson.A{
			isProceed, "$$NOW", nil,
		}},

		"lastNDeferredTimes": bson.M{"$cond": bson.A{
			isDeferred,
			pushedDeferredTimes,
			bson.M{"$ifNull": bson.A{"$lastNDeferredTimes", bson.A{}}},
		}},
		"lastDeferredMessageRef": bson.M{"$cond": bson.A{
			isDeferred, messageRef, "$lastDeferredMessageRef",
		}},
		"lastDeferredAt": bson.M{"$cond": bson.A{
			isDeferred, "$$NOW", "$lastDeferredAt",
		}},
	}

	// Write side updates contentHash on a Proceed (the reserve is the
	// optimistic record — Release does not roll this back). Read side
	// passes ContentHash == "" and we don't touch the field at all.
	if req.ContentHash != "" {
		stage2["contentHash"] = bson.M{"$cond": bson.A{
			isProceed, req.ContentHash, "$contentHash",
		}}
	}

	pipeline := mongo.Pipeline{
		{{Key: "$set", Value: stage1}},
		{{Key: "$set", Value: stage2}},
	}

	// Decode shape — superset of entryDoc with the outcome fields.
	var doc reserveOutcomeDoc
	err = s.entries.FindOneAndUpdate(ctx, bson.M{"_id": req.Key}, pipeline,
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After),
	).Decode(&doc)
	if err != nil {
		// Fail-closed: surface Deferred so the replay sweep re-drives.
		return sendstate.ReserveResult{Deferred: true, Reason: "store error"}, err
	}

	switch doc.ReserveOutcome {
	case "deduped":
		return sendstate.ReserveResult{Deduped: true, Reason: "duplicate content"}, nil
	case "deferred":
		// Update deferred-side metrics; failure here doesn't change the
		// gate decision (observability only).
		if mErr := s.bumpDeferredMetrics(ctx, req.Key, req.Tenant, req.Namespace); mErr != nil {
			return sendstate.ReserveResult{Deferred: true, Reason: doc.ReserveReason}, mErr
		}
		return sendstate.ReserveResult{Deferred: true, Reason: doc.ReserveReason}, nil
	case "proceed":
		// Update sent-side metrics + peaks using the post-update entry
		// (so the rolling-window counts include the just-pushed timestamp).
		if mErr := s.bumpSentMetrics(ctx, req.Key, req.Tenant, req.Namespace, now, doc.entry()); mErr != nil {
			return sendstate.ReserveResult{ReservedAt: doc.ReservedAt}, mErr
		}
		return sendstate.ReserveResult{ReservedAt: doc.ReservedAt}, nil
	default:
		// Should never happen — fail closed.
		return sendstate.ReserveResult{Deferred: true, Reason: "store error"},
			errors.New("mongodb: unexpected reserve outcome: " + doc.ReserveOutcome)
	}
}

// ReleaseReservation pops the reservedAt timestamp from the key's
// lastNSendTimes via `$pull` (by value). Lifetime metrics are not rolled
// back — see the sendstate package doc.
//
// Best-effort: a missing document or missing timestamp is silently a
// no-op; only a backing-store error surfaces.
func (s *Store) ReleaseReservation(ctx context.Context, key string, reservedAt time.Time) error {
	_, err := s.entries.UpdateByID(ctx, key, bson.M{
		"$pull": bson.M{"lastNSendTimes": reservedAt},
	})
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil
	}
	return err
}

// holdEntryDoc is the per-element shape inside a holds doc's array.
// Mirrors [protect.Hold] field-for-field with bson tags. `omitempty`
// on `clearedAt` keeps naturally-expired entries free of the field
// (matching the zero-value contract in protect.Hold's godoc).
type holdEntryDoc struct {
	From      time.Time `bson:"from"`
	To        time.Time `bson:"to"`
	Reason    string    `bson:"reason"`
	SetAt     time.Time `bson:"setAt"`
	ClearedAt time.Time `bson:"clearedAt,omitempty"`
}

func (d holdEntryDoc) hold() protect.Hold {
	return protect.Hold{From: d.From, To: d.To, Reason: d.Reason, SetAt: d.SetAt, ClearedAt: d.ClearedAt}
}

// PlaceHold appends a hold entry to the tenant's audit doc (one doc per
// tenant, `_id = tenant`). Atomic single-doc upsert via a `$set`
// aggregation pipeline that computes the resolved `from` (zero → $$NOW)
// and the `setAt` timestamp server-side, then `$concatArrays` appends
// onto the existing audit array. Concurrent PlaceHold calls on the same
// tenant each contribute their own entry; the single-doc update
// linearises them.
func (s *Store) PlaceHold(ctx context.Context, tenant string, from, to time.Time, reason string) error {
	// Server-side resolve of `from` so a zero time passed by the caller
	// becomes $$NOW at the database, not at this process — mirrors the
	// memory backend's "from.IsZero() → now" behaviour but anchored to
	// the DB's clock for cross-instance consistency.
	resolvedFrom := bson.M{"$cond": bson.A{
		bson.M{"$eq": bson.A{from, time.Time{}}},
		"$$NOW",
		from,
	}}
	newEntry := bson.M{
		"from":   resolvedFrom,
		"to":     to,
		"reason": reason,
		"setAt":  "$$NOW",
	}
	pipeline := mongo.Pipeline{
		{{Key: "$set", Value: bson.M{
			"holds": bson.M{"$concatArrays": bson.A{
				bson.M{"$ifNull": bson.A{"$holds", bson.A{}}},
				bson.A{newEntry},
			}},
		}}},
	}
	_, err := s.holds.UpdateByID(ctx, tenant, pipeline, options.UpdateOne().SetUpsert(true))
	return err
}

// ClearActiveHolds stamps `clearedAt = $$NOW` on every currently-active
// hold on tenant via a single-doc `$map` aggregation update. Active is
// defined inline as `from ≤ $$NOW AND to > $$NOW AND clearedAt is not
// set` — matching the memory backend's filter. Expired or
// already-cleared entries are left alone.
func (s *Store) ClearActiveHolds(ctx context.Context, tenant string) error {
	pipeline := mongo.Pipeline{
		{{Key: "$set", Value: bson.M{
			"holds": bson.M{
				"$map": bson.M{
					"input": bson.M{"$ifNull": bson.A{"$holds", bson.A{}}},
					"as":    "h",
					"in": bson.M{"$cond": bson.A{
						// active iff: from ≤ $$NOW AND to > $$NOW AND clearedAt is missing/zero
						bson.M{"$and": bson.A{
							bson.M{"$lte": bson.A{"$$h.from", "$$NOW"}},
							bson.M{"$gt": bson.A{"$$h.to", "$$NOW"}},
							bson.M{"$not": bson.M{"$ifNull": bson.A{"$$h.clearedAt", false}}},
						}},
						bson.M{"$mergeObjects": bson.A{"$$h", bson.M{"clearedAt": "$$NOW"}}},
						"$$h",
					}},
				},
			},
		}}},
	}
	_, err := s.holds.UpdateByID(ctx, tenant, pipeline)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil // no holds doc → nothing to clear
	}
	return err
}

// MostRestrictiveActiveHold reads the tenant's holds doc, filters for
// currently-active entries, and returns the entry with the latest `to`
// (most-restrictive). Fails open on store error per the sendstate.Store
// contract — a missing doc or a backing-store error both yield
// `(false, Hold{}, …)` so a caller that treats `active=false` as "no
// hold" proceeds rather than blocking on a degraded backend.
//
// The filter happens client-side after a single doc read (rather than a
// `$filter`+`$sort`+`$first` aggregation) because the audit array is
// small (operational rate-limit events / operator actions), the
// returned bytes are bounded by the per-tenant doc, and the
// implementation stays straightforward.
func (s *Store) MostRestrictiveActiveHold(ctx context.Context, tenant string) (bool, protect.Hold, error) {
	var doc struct {
		Holds []holdEntryDoc `bson:"holds"`
	}
	if err := s.holds.FindOne(ctx, bson.M{"_id": tenant}).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return false, protect.Hold{}, nil
		}
		return false, protect.Hold{}, err
	}
	now := time.Now()
	var best protect.Hold
	active := false
	for _, h := range doc.Holds {
		if !h.ClearedAt.IsZero() {
			continue
		}
		if h.From.After(now) || !h.To.After(now) {
			continue
		}
		if !active || h.To.After(best.To) {
			best = h.hold()
			active = true
		}
	}
	if !active {
		return false, protect.Hold{}, nil
	}
	return true, best, nil
}

// reserveOutcomeDoc decodes the post-update entry doc with the two extra
// fields the CheckAndReserve pipeline stamps. Inherits all entryDoc shape
// via embedding.
type reserveOutcomeDoc struct {
	entryDoc       `bson:",inline"`
	ReserveOutcome string    `bson:"_reserveOutcome,omitempty"`
	ReserveReason  string    `bson:"_reserveReason,omitempty"`
	ReservedAt     time.Time `bson:"_reservedAt,omitempty"`
}

// composeDeferReasonExpr builds the chained $cond that returns the firing
// Coalescer's Name (string), or "" when none fire. Order follows the
// caller's attached order — the first Coalescer that defers wins.
//
// Every Coalescer.Strategy must map to a non-nil [mongoExpr]; the
// switch is exhaustive over the strategies coalesce ships, so a nil
// here is a "your build is incoherent" diagnostic rather than a runtime
// concern.
func composeDeferReasonExpr(coalescers []coalesce.Coalescer) (any, error) {
	// Build from the tail forward: each Coalescer's $cond wraps the
	// chain so far in its else-branch.
	expr := any("") // empty-string sentinel for "no Coalescer deferred"
	for i := len(coalescers) - 1; i >= 0; i-- {
		me := mongoExpr(coalescers[i])
		if me == nil {
			return nil, errors.New("mongodb: CheckAndReserve: Coalescer " + coalescers[i].Name() + " has no Mongo expression — unknown Strategy")
		}
		expr = bson.M{"$cond": bson.A{bson.M(me), coalescers[i].Name(), expr}}
	}
	return expr, nil
}

// mongoExpr returns the Mongo aggregation expression that evaluates to
// true iff c would defer at the server's `$$NOW`. Switches on
// [coalesce.Coalescer.Strategy] — the in-process backend uses the
// Go-side mirror, [coalesce.Coalescer.ShouldDefer]. Panics on an
// unknown Strategy, matching the coalesce package's switch policy —
// every [coalesce.Coalescer] reaching this point should originate from
// one of the package's New* constructors.
func mongoExpr(c coalesce.Coalescer) sendstate.MongoExpr {
	switch c.Strategy {
	case coalesce.StrategyLeadingEdge, coalesce.StrategyQuota:
		return sendstate.MongoExpr{
			"$gte": []any{
				countInTrailingWindow("$lastNSendTimes", c.Window),
				c.HardCap,
			},
		}
	case coalesce.StrategyTrailingEdge:
		return sendstate.MongoExpr{
			"$or": []any{
				map[string]any{"$gt": []any{
					countInTrailingWindow("$lastNSendTimes", c.Window), 0,
				}},
				map[string]any{"$gt": []any{
					countInTrailingWindow("$lastNDeferredTimes", c.Window), 0,
				}},
			},
		}
	default:
		panic(fmt.Sprintf("mongodb: unknown coalesce.Strategy %d — construct Coalescers via the coalesce package's New* constructors", c.Strategy))
	}
}

// countInTrailingWindow renders `$size($filter(... > $$NOW - windowMs))`
// — the server-side equivalent of [sendstate.Entry.CountSentInWindow]
// (or CountDeferredInWindow, depending on the field). `$ifNull` defaults
// a missing array field to `[]` so a first-time key (no document yet, or
// no sends recorded yet) counts as zero rather than erroring.
func countInTrailingWindow(field string, window time.Duration) any {
	return map[string]any{
		"$size": map[string]any{
			"$filter": map[string]any{
				"input": map[string]any{"$ifNull": []any{field, []any{}}},
				"cond": map[string]any{
					"$gt": []any{
						"$$this",
						map[string]any{"$subtract": []any{"$$NOW", window.Milliseconds()}},
					},
				},
			},
		},
	}
}

// bumpSentMetrics is the metrics-doc update for a Proceed CheckAndReserve.
// Mirrors RecordAsSent's pipeline-update over the same metrics fields,
// using the post-update entry's send-time list for accurate rolling-window
// peak counts.
func (s *Store) bumpSentMetrics(ctx context.Context, key, tenant, namespace string, now time.Time, ent sendstate.Entry) error {
	if s.maxSendTimes <= 1 {
		_, err := s.metrics.UpdateByID(ctx, key, bson.M{
			"$inc": bson.M{"totalSent": 1},
			"$set": bson.M{"lastSentAt": now, "tenant": tenant, "namespace": namespace},
			"$min": bson.M{"firstSentAt": now},
		}, options.UpdateOne().SetUpsert(true))
		return err
	}
	set := bson.M{
		"tenant":      tenant,
		"namespace":   namespace,
		"totalSent":   bson.M{"$add": bson.A{bson.M{"$ifNull": bson.A{"$totalSent", 0}}, 1}},
		"lastSentAt":  now,
		"firstSentAt": bson.M{"$min": bson.A{"$firstSentAt", now}},
	}
	if s.ttl >= time.Hour {
		sent1h := int64(ent.CountSentInWindow(now, time.Hour))
		set["peak1h"] = bson.M{"$max": bson.A{bson.M{"$ifNull": bson.A{"$peak1h", 0}}, sent1h}}
		set["peak1hAt"] = bson.M{"$cond": bson.A{
			bson.M{"$gt": bson.A{sent1h, bson.M{"$ifNull": bson.A{"$peak1h", 0}}}},
			now, "$peak1hAt",
		}}
	}
	if s.ttl >= 24*time.Hour {
		sent24h := int64(ent.CountSentInWindow(now, 24*time.Hour))
		set["peak24h"] = bson.M{"$max": bson.A{bson.M{"$ifNull": bson.A{"$peak24h", 0}}, sent24h}}
		set["peak24hAt"] = bson.M{"$cond": bson.A{
			bson.M{"$gt": bson.A{sent24h, bson.M{"$ifNull": bson.A{"$peak24h", 0}}}},
			now, "$peak24hAt",
		}}
	}
	_, err := s.metrics.UpdateByID(ctx, key, mongo.Pipeline{{{Key: "$set", Value: set}}}, options.UpdateOne().SetUpsert(true))
	return err
}

// bumpDeferredMetrics is the metrics-doc update for a Deferred CheckAndReserve.
// Mirrors RecordAsDeferred's bump.
func (s *Store) bumpDeferredMetrics(ctx context.Context, key, tenant, namespace string) error {
	now := time.Now()
	_, err := s.metrics.UpdateByID(ctx, key, bson.M{
		"$inc": bson.M{"totalDeferred": 1},
		"$set": bson.M{"lastDeferredAt": now, "tenant": tenant, "namespace": namespace},
		"$min": bson.M{"firstDeferredAt": now},
	}, options.UpdateOne().SetUpsert(true))
	return err
}

// ReadEntry returns the working Entry, filtering on expireAt > now so an
// expired-but-unswept document reads as the zero Entry. A miss, an
// expired record, or a driver error all return the zero Entry (the
// error is surfaced for logging; callers fail-open).
func (s *Store) ReadEntry(ctx context.Context, key string) (sendstate.Entry, error) {
	var doc entryDoc
	err := s.entries.FindOne(ctx, bson.M{
		"_id":      key,
		"expireAt": bson.M{"$gt": time.Now()},
	}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return sendstate.Entry{}, nil // miss or expired → zero (fail-open)
	}
	if err != nil {
		return sendstate.Entry{}, err
	}
	return doc.entry(), nil
}

// ReadMetrics returns the key's lifetime [sendstate.Metrics]. ok=false
// when the key has no metrics record.
func (s *Store) ReadMetrics(ctx context.Context, key string) (sendstate.Metrics, bool, error) {
	var doc metricsDoc
	err := s.metrics.FindOne(ctx, bson.M{"_id": key}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return sendstate.Metrics{}, false, nil
	}
	if err != nil {
		return sendstate.Metrics{}, false, err
	}
	return doc.metrics(), true, nil
}

// RangeDeferred visits pending entries — most recent deferral newer than
// most recent send — oldest-pending first, up to limit (<= 0 =
// unbounded), skipping TTL-expired records. Pending and order both come
// from the denormalized scalar tails. A doc that has only ever been
// deferred has no lastSentAt (null), which sorts below any date, so it
// reads as pending.
//
// When namespace is non-empty an equality predicate on namespace is added,
// scoping the sweep to that namespace — the {namespace, lastDeferredAt}
// compound index then serves the equality + sort + limit in one scan (limit
// applies within the namespace). When empty, the whole store is visited and
// the {lastDeferredAt: 1} index serves the sort. The $expr pending predicate
// is a cheap residual on the bounded scan either way.
func (s *Store) RangeDeferred(ctx context.Context, limit int, namespace string, fn func(key string, e sendstate.Entry) bool) error {
	filter := bson.M{
		"expireAt": bson.M{"$gt": time.Now()},                               // TTL-honoring
		"$expr":    bson.M{"$gt": bson.A{"$lastDeferredAt", "$lastSentAt"}}, // pending
	}
	if namespace != "" {
		filter["namespace"] = namespace // sweep-scoping: only this namespace's pending entries
	}
	opts := options.Find().SetSort(bson.D{{Key: "lastDeferredAt", Value: 1}}) // oldest first
	if limit > 0 {
		opts.SetLimit(int64(limit))
	}
	cur, err := s.entries.Find(ctx, filter, opts)
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var doc entryDoc
		if err := cur.Decode(&doc); err != nil {
			return err
		}
		if !fn(doc.Key, doc.entry()) {
			break
		}
	}
	return cur.Err()
}
