# Mongo `sendstate.Store` — implementation sketch

A shared-backing [`sendstate.Store`](../sendstate/) for multi-instance use. It
mirrors `MemoryStore`'s two-store model as **two collections** with different
lifecycles:

- **`pith_entries`** — TTL'd working state (drives the policies). One document
  per key, expired by a TTL index.
- **`pith_metrics`** — permanent lifetime rollup (observability). Never expires.

The design decisions this implements (see the `sendstate` package doc):

- **Sliding TTL** via an explicit `expireAt` date + `expireAfterSeconds: 0`.
- **TTL-honoring reads** — every entry read filters `expireAt > now`, so an
  expired-but-not-yet-deleted doc reads as absent. TTL deletion is pure storage
  reclamation; it never affects an answer, so this backend matches `MemoryStore`
  regardless of when Mongo's background deleter runs.
- **One read drives a `Check`** — policies read only `pith_entries`; `pith_metrics`
  is observability-only.
- **`$max` peak merge** for the per-cap high-water marks.

## Collections & indexes

```js
// One-time setup (also done programmatically by EnsureIndexes below).
db.pith_entries.createIndex({ expireAt: 1 }, { expireAfterSeconds: 0 })
// _id is already unique-indexed; that covers every lookup. No index needed
// on pith_metrics beyond _id.
```

Document shapes:

```js
// pith_entries
{ _id: "act-1:contact-3",
  contentHash: "9f2c…",
  lastDeferredMessageRef: BinData(…),         // optional; latest replay breadcrumb
  lastNSendTimes: [ISODate, ISODate, …],      // bounded to the last N
  lastNDeferredTimes: [ISODate, ISODate, …],  // deferral cadence, bounded the same
  expireAt: ISODate }                         // now + ttl, refreshed every write

// pith_metrics
{ _id: "act-1:contact-3",
  totalSent: 312, totalDeferred: 9,
  lastSentAt: ISODate, lastDeferredAt: ISODate,
  peakSendsInWindow: { "at cap": 50, "leading-edge debounce window": 1, "burst": 5 } }
```

## BSON documents + conversion

`sendstate.Entry`/`Metrics` stay backend-agnostic (no bson tags), so the store
defines its own document structs and converts.

```go
package mongostore

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	// (mongo-driver v2: go.mongodb.org/mongo-driver/v2/... — same shapes)

	"github.com/homemade/pith/sendstate"
)

type entryDoc struct {
	Key                    string      `bson:"_id"`
	ContentHash            string      `bson:"contentHash,omitempty"`
	LastDeferredMessageRef []byte      `bson:"lastDeferredMessageRef,omitempty"`
	LastNSendTimes         []time.Time `bson:"lastNSendTimes,omitempty"`
	LastNDeferredTimes     []time.Time `bson:"lastNDeferredTimes,omitempty"`
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

type metricsDoc struct {
	Key               string            `bson:"_id"`
	TotalSent         uint64            `bson:"totalSent"`
	TotalDeferred     uint64            `bson:"totalDeferred"`
	LastSentAt        time.Time         `bson:"lastSentAt,omitempty"`
	LastDeferredAt    time.Time         `bson:"lastDeferredAt,omitempty"`
	PeakSendsInWindow map[string]uint64 `bson:"peakSendsInWindow,omitempty"`
}

func (d metricsDoc) metrics() sendstate.Metrics {
	return sendstate.Metrics{
		TotalSent:         d.TotalSent,
		TotalDeferred:     d.TotalDeferred,
		LastSentAt:        d.LastSentAt,
		LastDeferredAt:    d.LastDeferredAt,
		PeakSendsInWindow: d.PeakSendsInWindow,
	}
}
```

## Store

```go
// Store is a Mongo-backed sendstate.Store.
type Store struct {
	entries      *mongo.Collection
	metrics      *mongo.Collection
	ttl          time.Duration
	maxSendTimes int // bounds lastNSendTimes; must be >= the largest Coalescer hardCap
}

type Option func(*Store)

// WithMaxSendTimes bounds each key's lastNSendTimes list. Set it >= the largest
// attached Coalescer hardCap (protect.New only auto-sizes *MemoryStore, so for
// Mongo the caller is responsible — see Notes).
func WithMaxSendTimes(n int) Option { return func(s *Store) { s.maxSendTimes = n } }

// New requires entryTTL (mirroring sendstate.NewMemoryStore — pith holds no
// default TTL); it MUST be >= the largest Coalescer window. Panics if <= 0.
// Like MemoryStore, expose EntryTTL() so protect.New can validate it.
func New(db *mongo.Database, entryTTL time.Duration, opts ...Option) *Store {
	if entryTTL <= 0 {
		panic("mongostore: New requires a positive entryTTL")
	}
	s := &Store{
		entries: db.Collection("pith_entries"),
		metrics: db.Collection("pith_metrics"),
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

// EntryTTL reports the configured TTL so protect.New can validate it.
func (s *Store) EntryTTL() time.Duration { return s.ttl }

// EnsureIndexes creates the TTL index. Idempotent; call once at startup.
func (s *Store) EnsureIndexes(ctx context.Context) error {
	_, err := s.entries.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "expireAt", Value: 1}},
		Options: options.Index().SetExpireAfterSeconds(0),
	})
	return err
}
```

### Reads — TTL-honoring

```go
// ReadEntry returns the working Entry, filtering on expireAt > now so an
// expired-but-unswept doc reads as the zero Entry (matching MemoryStore).
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

// RangeDeferred visits pending entries (most recent deferral newer than
// most recent send), oldest-pending first, up to limit. The pending
// filter and sort key both need a scalar deferral timestamp on the
// entries doc — see Note 0 for how to represent "pending" cheaply.
// Sketch using denormalized scalar lastSentAt/lastDeferredAt fields:
func (s *Store) RangeDeferred(ctx context.Context, limit int, fn func(key string, e sendstate.Entry) bool) error {
	filter := bson.M{
		"expireAt": bson.M{"$gt": time.Now()},                  // TTL-honoring
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
```

(`lastSentAt`/`lastDeferredAt` here are scalar tails denormalized onto the
entries doc on each write — the array `$last` isn't directly sortable. See
Note 0.)

### Writes — one upsert per collection

```go
// RecordAsSent: append the send timestamp (bounded to the last N via $slice)
// and refresh the TTL; bump lifetime counters. Touches only send-side state —
// the deferred ref / lastNDeferredTimes are left intact (recency makes the
// deferral no longer pending). Two upserts — eventually consistent across
// collections (see Notes).
func (s *Store) RecordAsSent(ctx context.Context, key, contentHash string) error {
	now := time.Now()

	_, err := s.entries.UpdateByID(ctx, key, bson.M{
		"$set":  bson.M{"contentHash": contentHash, "expireAt": now.Add(s.ttl)},
		"$push": bson.M{"lastNSendTimes": bson.M{"$each": bson.A{now}, "$slice": -s.maxSendTimes}},
	}, options.Update().SetUpsert(true))
	if err != nil {
		return err
	}

	_, err = s.metrics.UpdateByID(ctx, key, bson.M{
		"$inc": bson.M{"totalSent": 1},
		"$set": bson.M{"lastSentAt": now},
		// lastDeferredAt is NOT cleared — it's a lifetime timestamp;
		// the newer lastSentAt simply supersedes it. totalDeferred and
		// peakSendsInWindow are likewise left untouched (preserved).
	}, options.Update().SetUpsert(true))
	return err
}

// RecordAsDeferred: stamp the breadcrumb, append the deferral timestamp, and
// refresh the TTL (does NOT touch contentHash or lastNSendTimes); bump the
// deferred-side counters.
func (s *Store) RecordAsDeferred(ctx context.Context, key string, messageRef []byte) error {
	now := time.Now()

	_, err := s.entries.UpdateByID(ctx, key, bson.M{
		"$set":  bson.M{"lastDeferredMessageRef": messageRef, "expireAt": now.Add(s.ttl)},
		"$push": bson.M{"lastNDeferredTimes": bson.M{"$each": bson.A{now}, "$slice": -s.maxSendTimes}},
	}, options.Update().SetUpsert(true))
	if err != nil {
		return err
	}

	_, err = s.metrics.UpdateByID(ctx, key, bson.M{
		"$inc": bson.M{"totalDeferred": 1},
		"$set": bson.M{"lastDeferredAt": now},
	}, options.Update().SetUpsert(true))
	return err
}

// RaisePeaks: one $max upsert raises every cap's stored peak to at least the
// given value. Lower values are ignored by $max; absent doc is created.
func (s *Store) RaisePeaks(ctx context.Context, key string, counts map[string]uint64) error {
	if len(counts) == 0 {
		return nil
	}
	maxes := bson.M{}
	for capName, n := range counts {
		maxes["peakSendsInWindow."+capName] = n
	}
	_, err := s.metrics.UpdateByID(ctx, key, bson.M{"$max": maxes},
		options.Update().SetUpsert(true))
	return err
}
```

## Notes / gotchas

0. **DECISION TO MAKE — `RangeDeferred`'s "pending" query + sort.** The sweep
   finds keys awaiting replay (most recent deferral newer than most recent
   send) and visits them **oldest-deferral first** (so a bounded sweep can't
   starve a long-waiting breadcrumb). Pith doesn't clear anything on send
   (recency decides), so there's no single `$exists` flag — and the sort needs
   a scalar deferral timestamp. The arrays (`lastNSendTimes`/`lastNDeferredTimes`)
   aren't directly filterable/sortable, so denormalize their tails onto
   `pith_entries`. Two shapes:
   - **Scalar `lastSentAt` + `lastDeferredAt` (+ `$expr`)** — written on every
     send/defer (the tails). Filter `{$expr: {$gt: ["$lastDeferredAt","$lastSentAt"]}}`,
     sort `{lastDeferredAt: 1}`, limit (the `RangeDeferred` sketch above).
     `$expr` can't use an index for the predicate, but the `{lastDeferredAt: 1}`
     index serves the sort+limit and the `$expr` is a cheap residual on a
     bounded scan. Touches only the relevant side on each write (no clear).
   - **A `pendingSince` marker** — set to the defer time on defer and **unset
     on send** (the one spot we'd reintroduce a send-time clear), giving an
     indexable `{pendingSince: {$exists: true}}` filter and `{pendingSince: 1}`
     sort. Cleaner query, at the cost of a send-side write to a deferred-side
     field.

   Either way, eligibility ("gone quiet" etc.) stays in Go via
   `Entry.CountDeferredInWindow`. The in-memory store needs none of this (its
   `RangeDeferred` `Range`s, compares tails, and sorts the pending slice).
   Resolve when wiring the Mongo sweep; it doesn't change the other methods.

1. **Carrying stale timestamps is harmless.** A write that upserts an
   expired-but-undeleted entry doc `$push`es onto the old `lastNSendTimes` and
   refreshes `expireAt`. Because `ttl` > any Coalescer window, those stale
   timestamps are always out-of-window, so `CountInWindow` never counts them —
   same property the in-memory store relies on. No need to reset on write.

2. **`maxSendTimes` is the caller's responsibility for Mongo.** `protect.New`
   only auto-sizes `*sendstate.MemoryStore`. For this store, pass
   `WithMaxSendTimes(largestHardCap)` (or larger). Too small → `$slice` drops
   in-window timestamps → `CountInWindow` undercounts → caps leak.

3. **Cap names become field paths.** `RaisePeaks` builds `"peakSendsInWindow."+capName`.
   Mongo field names cannot contain `.` or start with `$`. Cap reasons like
   `"at cap"`/`"leading-edge debounce window"` are fine; if you allow arbitrary custom
   reasons, sanitize them (or key by a slug) before they reach the store.

4. **Cross-collection consistency.** A send/deferral writes two collections in
   two ops, not atomically — `pith_metrics` may briefly lag `pith_entries`
   (the eventual-consistency the package doc already calls out). Policies only
   read `pith_entries`, so this never affects a decision. If you want
   atomicity, wrap both updates in a `session.WithTransaction` (needs a replica
   set); the extra round-trips aren't worth it for advisory counters.

5. **Write concern for cross-instance correctness.** For the gating guarantee
   to hold across instances, configure the collections (or client) with
   `majority` write concern so a `RecordAsSent` is visible to the next
   instance's `ReadEntry`. Default (`w:1`) can let a racing instance miss a
   just-recorded send.

6. **BSON time precision is milliseconds.** `lastNSendTimes` round-trips at ms
   precision (BSON has no ns). Irrelevant for second/minute/hour windows;
   only matters if a Coalescer window approaches single-digit ms.

7. **`ReadEntry` fail-open.** A driver error returns the zero `Entry` + the
   error; `protect.Check` proceeds (fail-open) and surfaces the error for
   logging — same contract as the interface.
```
