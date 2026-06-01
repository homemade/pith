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
//   - One read drives a Check: policies read only entries; metrics is
//     observability-only.
//
// Cross-instance correctness depends on the collections (or client)
// using majority write concern, so a RecordAsSent is visible to the
// next instance's ReadEntry; configure that on the *mongo.Database
// passed to [New] (a w:1 default can let a racing instance miss a
// just-recorded send).
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

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
// Construct with [New], call [Store.EnsureIndexes] once at startup,
// then pass the Store as the first argument to [pith/protect.New].
// Unlike the in-memory backend, the Protector does NOT auto-size
// this Store's send-timestamp bound: pass [WithMaxSendTimes] yourself
// (>= the largest attached Coalescer hardCap) or in-window
// timestamps will be dropped by $slice and caps will leak. See the
// package doc for the majority-write-concern requirement.
type Store struct {
	entries      *mongo.Collection
	metrics      *mongo.Collection
	ttl          time.Duration
	maxSendTimes int // bounds lastNSendTimes/lastNDeferredTimes; must be >= the largest Coalescer hardCap
}

// Option configures a [Store].
type Option func(*Store)

// WithMaxSendTimes bounds each key's lastNSendTimes list. Set it >= the
// largest attached Coalescer hardCap. Unlike the in-memory store,
// [pith/protect.New] does NOT auto-size this backend (it only sizes a
// store that opts in via a GrowMaxSendTimes method, which this one
// deliberately doesn't), so the caller is responsible: too small a
// bound drops in-window timestamps via $slice, making
// [sendstate.Entry.CountSentInWindow] undercount and a cap leak.
func WithMaxSendTimes(n int) Option { return func(s *Store) { s.maxSendTimes = n } }

// New returns a Mongo-backed Store over the given database. entryTTL is
// required (mirroring pith/sendstate/memory.New — pith holds no default TTL) and
// MUST be >= the largest Coalescer window in use; [pith/protect.New]
// validates that against the EntryTTL reported here. Panics if
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

// EntryTTL reports the configured TTL so [pith/protect.New] can validate
// it against the attached Coalescer windows.
func (s *Store) EntryTTL() time.Duration { return s.ttl }

// Config bundles the parameters [Open] needs to dial Mongo and build a
// Store. URI and Database are deployment config (typically read from
// the environment); EntryTTL, MaxSendTimes, and Timeout are program
// config (typically code constants).
//
// No JSON tags by design: callers serialize their own env-var shape
// and map fields onto Config. That keeps pith decoupled from the
// caller's env-var convention (flat vs composite JSON vs anything
// else).
type Config struct {
	// URI is the Mongo connection string. Required. SRV strings
	// (mongodb+srv://...) enable TLS by default; for Atlas this is
	// the normal format.
	URI string

	// Database is the database name within the cluster. Required.
	Database string

	// EntryTTL is the working-state TTL. Required and must be >= the
	// largest attached Coalescer window; [pith/protect.New] validates.
	EntryTTL time.Duration

	// MaxSendTimes bounds lastNSendTimes via $slice. Pass the largest
	// attached Coalescer hardCap; too small a value drops in-window
	// timestamps and caps will leak. Zero defers to the New default
	// (dedupe-only floor of 1) — only sensible for callers running
	// dedupe alone.
	MaxSendTimes int

	// Timeout caps every CRUD op (and EnsureIndexes) at this duration
	// via the v2 driver's client-level Timeout. On a timeout
	// [pith/protect.Protector.Check] fails open (DecisionProceed +
	// err), so a slow Mongo degrades to over-send rather than
	// blocking the gated path. Recommended: a tight bound (e.g.
	// 200ms). Zero disables it.
	Timeout time.Duration
}

// Open dials Mongo using cfg and returns a ready-to-use Store plus the
// underlying client (so callers can Disconnect at shutdown). The client
// is configured with majority write concern — required for
// cross-instance correctness; see the package doc — and the configured
// per-op Timeout if set. [Store.EnsureIndexes] is run before returning,
// so the returned Store has TTL eviction and the RangeDeferred sort
// index in place.
//
// v2's mongo.Connect is lazy (no I/O until first operation), so a
// connectivity failure typically surfaces from EnsureIndexes rather
// than from Connect. On any error Open returns nil Store; the client
// is returned where possible so the caller can Disconnect if it was
// constructed.
func Open(ctx context.Context, cfg Config) (*Store, *mongo.Client, error) {
	if cfg.URI == "" {
		return nil, nil, errors.New("mongodb.Open: Config.URI is required")
	}
	if cfg.Database == "" {
		return nil, nil, errors.New("mongodb.Open: Config.Database is required")
	}
	if cfg.EntryTTL <= 0 {
		return nil, nil, errors.New("mongodb.Open: Config.EntryTTL is required (> 0)")
	}

	clientOpts := options.Client().
		ApplyURI(cfg.URI).
		SetWriteConcern(writeconcern.Majority())
	if cfg.Timeout > 0 {
		clientOpts = clientOpts.SetTimeout(cfg.Timeout)
	}
	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return nil, nil, fmt.Errorf("mongodb.Open: connect: %w", err)
	}

	var storeOpts []Option
	if cfg.MaxSendTimes > 0 {
		storeOpts = append(storeOpts, WithMaxSendTimes(cfg.MaxSendTimes))
	}
	store := New(client.Database(cfg.Database), cfg.EntryTTL, storeOpts...)

	if err := store.EnsureIndexes(ctx); err != nil {
		// Return the client so the caller can Disconnect; the Store
		// is nil because callers can't usefully proceed without the
		// TTL index (entries would accumulate forever).
		return nil, client, fmt.Errorf("mongodb.Open: EnsureIndexes: %w", err)
	}
	return store, client, nil
}

// EnsureIndexes creates the TTL index on expireAt and the
// lastDeferredAt index that serves [Store.RangeDeferred]'s sort.
// Idempotent; call once at startup.
func (s *Store) EnsureIndexes(ctx context.Context) error {
	_, err := s.entries.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "expireAt", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(0),
	})
	if err != nil {
		return err
	}
	// Serves RangeDeferred's {lastDeferredAt: 1} sort+limit; the pending
	// $expr predicate is a cheap residual on the bounded scan.
	_, err = s.entries.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "lastDeferredAt", Value: 1}},
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
func (s *Store) RecordAsSent(ctx context.Context, key, contentHash string) error {
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
			"$set":  bson.M{"contentHash": contentHash, "lastSentAt": now, "expireAt": now.Add(s.ttl)},
			"$push": bson.M{"lastNSendTimes": bson.M{"$each": bson.A{now}, "$slice": -s.maxSendTimes}},
		}, options.UpdateOne().SetUpsert(true))
		if err != nil {
			return err
		}
		_, err = s.metrics.UpdateByID(ctx, key, bson.M{
			"$inc": bson.M{"totalSent": 1},
			"$set": bson.M{"lastSentAt": now},
			"$min": bson.M{"firstSentAt": now},
		}, options.UpdateOne().SetUpsert(true))
		return err
	}

	// Windowed mode: FindOneAndUpdate (not UpdateByID) so we read back the
	// post-$push, post-$slice send-time list to count the rolling windows
	// below. Same write and round-trip — it just returns the document.
	var doc entryDoc
	err := s.entries.FindOneAndUpdate(ctx, bson.M{"_id": key}, bson.M{
		"$set":  bson.M{"contentHash": contentHash, "lastSentAt": now, "expireAt": now.Add(s.ttl)},
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
	sent1h := int64(ent.CountSentInWindow(now, time.Hour))
	sent24h := int64(ent.CountSentInWindow(now, 24*time.Hour))

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
	_, err = s.metrics.UpdateByID(ctx, key, mongo.Pipeline{
		{{Key: "$set", Value: bson.M{
			"totalSent":   bson.M{"$add": bson.A{bson.M{"$ifNull": bson.A{"$totalSent", 0}}, 1}},
			"lastSentAt":  now,
			"firstSentAt": bson.M{"$min": bson.A{"$firstSentAt", now}},
			"peak1h":      bson.M{"$max": bson.A{bson.M{"$ifNull": bson.A{"$peak1h", 0}}, sent1h}},
			"peak1hAt": bson.M{"$cond": bson.A{
				bson.M{"$gt": bson.A{sent1h, bson.M{"$ifNull": bson.A{"$peak1h", 0}}}},
				now, "$peak1hAt",
			}},
			"peak24h": bson.M{"$max": bson.A{bson.M{"$ifNull": bson.A{"$peak24h", 0}}, sent24h}},
			"peak24hAt": bson.M{"$cond": bson.A{
				bson.M{"$gt": bson.A{sent24h, bson.M{"$ifNull": bson.A{"$peak24h", 0}}}},
				now, "$peak24hAt",
			}},
		}}},
	}, options.UpdateOne().SetUpsert(true))
	return err
}

// RecordAsDeferred stamps the breadcrumb, appends the deferral
// timestamp (and scalar lastDeferredAt tail), and refreshes the TTL —
// without touching contentHash, lastNSendTimes, or lastSentAt; then
// bumps the deferred-side counters.
func (s *Store) RecordAsDeferred(ctx context.Context, key string, messageRef []byte) error {
	now := time.Now()

	_, err := s.entries.UpdateByID(ctx, key, bson.M{
		"$set":  bson.M{"lastDeferredMessageRef": messageRef, "lastDeferredAt": now, "expireAt": now.Add(s.ttl)},
		"$push": bson.M{"lastNDeferredTimes": bson.M{"$each": bson.A{now}, "$slice": -s.maxSendTimes}},
	}, options.UpdateOne().SetUpsert(true))
	if err != nil {
		return err
	}

	// $min on firstDeferredAt — set-once semantics symmetric with
	// firstSentAt on RecordAsSent. See that method's comment.
	_, err = s.metrics.UpdateByID(ctx, key, bson.M{
		"$inc": bson.M{"totalDeferred": 1},
		"$set": bson.M{"lastDeferredAt": now},
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
// from the denormalized scalar tails: the {lastDeferredAt: 1} index
// serves the sort+limit and the $expr is a cheap residual on the bounded
// scan. A doc that has only ever been deferred has no lastSentAt (null),
// which sorts below any date, so it reads as pending.
func (s *Store) RangeDeferred(ctx context.Context, limit int, fn func(key string, e sendstate.Entry) bool) error {
	filter := bson.M{
		"expireAt": bson.M{"$gt": time.Now()},                               // TTL-honoring
		"$expr":    bson.M{"$gt": bson.A{"$lastDeferredAt", "$lastSentAt"}}, // pending
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
