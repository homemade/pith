// Package sendstate is the shared per-key state that pith's policies
// consult to make their decisions. Callers (and the [pith/protect]
// orchestration layer) write here, and policies read over the same
// records.
//
// State is split across two stores — mirroring two backend
// collections — with different lifecycles:
//
//   - [Entry] — working state: ContentHash, LastDeferredMessageRef,
//     and the rolling list of send timestamps (LastNSendTimes). Read
//     via [Store.ReadEntry] and bounded by a TTL (set at store
//     construction): a key idle past the horizon reads as absent.
//     [Entry] carries the two
//     read primitives every policy needs — [Entry.Seen] (content
//     dedupe) and [Entry.CountInWindow] (the per-window send count
//     [pith/coalesce.Coalescer] thresholds against) — so a single
//     ReadEntry drives a whole [pith/protect.Protector.Check].
//   - [Metrics] — lifetime observability rollup: TotalSent,
//     TotalDeferred, LastSentAt, LastDeferredAt, and per-cap
//     PeakSendsInWindow high-water marks (raised via
//     [Store.RaisePeaks]). Read via [Store.ReadMetrics]. Never expires.
//
// [Store.RecordAsSent] sets ContentHash, appends a timestamp to
// LastNSendTimes, refreshes the Entry TTL, increments TotalSent and
// stamps LastSentAt. It touches only send-side state — neither the
// LastDeferredMessageRef breadcrumb, LastNDeferredTimes, nor
// LastDeferredAt is cleared. [Store.RecordAsDeferred] sets
// LastDeferredMessageRef, appends to LastNDeferredTimes, refreshes the
// Entry TTL, increments TotalDeferred and stamps LastDeferredAt (no
// LastNSendTimes append — deferrals don't feed the [Entry.CountInWindow]
// send count).
//
// A deferral is pending (awaiting a flush) exactly when the most recent
// deferral is newer than the most recent send (LastDeferredAt after
// LastSentAt, or the LastNDeferredTimes tail after the LastNSendTimes
// tail). A send makes the send side win by recency — nothing is cleared
// to mark a deferral resolved, and a breadcrumb left after a send is
// simply never read (it isn't pending).
//
// The window — "how many sends in the last W?" — is a read-side
// policy. [Entry.CountInWindow] takes both the reference "now" and
// the window per-call, so multiple Coalescers (each with their own
// window) share one read against one consistent now.
//
// The contract is record-on-success: callers RecordAsSent only
// after the gated operation has actually succeeded. RecordAsDeferred
// is called by [pith/protect.Protector.Check] itself when any
// attached Coalescer's ShouldDefer returns true.
//
// # TTL semantics
//
// The Entry store behaves like a Mongo TTL index using the
// expire-at-an-explicit-time idiom (an indexed date field with
// expireAfterSeconds: 0): each write sets the record's expiry to
// now + the store's TTL, and the record is gone once that time passes.
// The TTL is required at construction (see [NewMemoryStore]) — pith
// holds no default.
//
// Crucially, reads are TTL-honoring: [Store.ReadEntry] treats an
// expired record as absent regardless of whether the backend has
// physically removed it yet. (A Mongo backend mirrors this by adding
// an "expireAt > now" predicate to its read, since Mongo's TTL
// deleter is a lazy background thread.) Deletion is therefore pure
// storage reclamation, never a correctness mechanism, so both
// backends answer identically no matter when the sweep/deleter runs.
//
// The TTL MUST be >= the largest Coalescer window in use;
// [pith/protect.New] validates this. Given that, an expired record's
// timestamps are all older than
// any window, so neither a stale read (filtered out) nor a write that
// carries them forward can change a [Entry.CountInWindow] result.
//
// # Concurrent / cross-store consistency
//
// A send or deferral writes both stores (Entry and Metrics)
// non-atomically. If a fresh RecordAsDeferred races a RecordAsSent,
// their relative recency (and so which one "wins" the pending check)
// follows whichever timestamp landed later; the system is eventually
// consistent — the next event re-stamps. Counters and timestamps in
// Metrics may likewise lag the Entry store briefly.
package sendstate

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Entry is the per-key working state read by policies. It carries the
// read primitives ([Entry.Seen], [Entry.CountInWindow]) directly.
type Entry struct {
	// ContentHash is the fingerprint of the message body most
	// recently passed to [Store.RecordAsSent]. Callers choose the
	// hashing scheme; pith only compares strings.
	ContentHash string

	// LastDeferredMessageRef is the caller-supplied message ref from
	// the most recent [Store.RecordAsDeferred] — the breadcrumb a
	// consumer-side replay sweep re-derives from. Pith treats it as an
	// opaque byte slice. It is NOT cleared on a send: a send supersedes
	// a deferral by recency (see "pending" below), so a stale ref left
	// after a send is simply never read.
	LastDeferredMessageRef []byte

	// LastNSendTimes holds the most recent [Store.RecordAsSent]
	// timestamps for this key, in chronological order (oldest first).
	// The store appends on each send and bounds the list by count,
	// dropping the oldest past the cap (see [MemoryStore.MaxSendTimes]);
	// it is not time-pruned, so an entry within the cap may predate any
	// mechanism window. Treat as immutable from the caller's side (the
	// store copy-on-writes when mutating). Drives [Entry.CountInWindow],
	// which applies the window filter, and [Entry.Seen]'s "has ever
	// sent" guard.
	LastNSendTimes []time.Time

	// LastNDeferredTimes is the deferral-side mirror of LastNSendTimes:
	// the most recent [Store.RecordAsDeferred] timestamps (oldest
	// first), appended on each Coalescer-driven defer and count-bounded
	// the same way. Read via [Entry.CountDeferredInWindow]; not cleared
	// on a send. It lets a replay sweep express debounce eligibility on
	// the Entry alone — e.g. trailing-edge "gone quiet" is
	// CountDeferredInWindow(now, W) == 0, max-wait is >= K.
	//
	// A deferral is pending (awaiting replay) when the most recent
	// deferral is newer than the most recent send — derived from these
	// timestamps (equivalently [Metrics.LastDeferredAt] >
	// [Metrics.LastSentAt]); nothing is stored or cleared to mark it.
	LastNDeferredTimes []time.Time
}

// Seen reports whether the most recent successful send for this key
// carried the same contentHash — i.e. re-sending it now would be a
// no-op. This is the content-dedupe primitive: the store keeps only
// the last-sent hash (ContentHash), so Seen answers "is this identical
// to the immediately preceding send?", not "have I ever sent this".
// An Entry with no send recorded (empty LastNSendTimes — including the
// zero Entry a miss or expired TTL reads as) always returns false.
// Pure; no I/O.
func (e Entry) Seen(contentHash string) bool {
	if len(e.LastNSendTimes) == 0 {
		return false
	}
	return e.ContentHash == contentHash
}

// CountInWindow returns the number of send timestamps in
// LastNSendTimes within the trailing window ending at now. now is the
// caller's reference time (typically captured once per Check and
// shared across every policy). Pure; no I/O.
func (e Entry) CountInWindow(now time.Time, window time.Duration) int {
	return countInWindow(e.LastNSendTimes, now, window)
}

// CountDeferredInWindow is the deferral-side mirror of
// [Entry.CountInWindow]: the number of LastNDeferredTimes within the
// trailing window ending at now. A replay sweep uses it for debounce
// eligibility (e.g. == 0 means the burst has gone quiet). Pure; no I/O.
func (e Entry) CountDeferredInWindow(now time.Time, window time.Duration) int {
	return countInWindow(e.LastNDeferredTimes, now, window)
}

// countInWindow counts the tail of an append-only chronological
// timestamp list within [now-window, now].
func countInWindow(times []time.Time, now time.Time, window time.Duration) int {
	cutoff := now.Add(-window)
	n := 0
	for i := len(times) - 1; i >= 0; i-- {
		if times[i].Before(cutoff) {
			break
		}
		n++
	}
	return n
}

// Metrics is the lifetime observability rollup for a per-key record.
// It never expires.
type Metrics struct {
	// TotalSent is the lifetime count of [Store.RecordAsSent] calls
	// for this key.
	TotalSent uint64

	// TotalDeferred is the lifetime count of
	// [Store.RecordAsDeferred] calls for this key.
	TotalDeferred uint64

	// LastSentAt is the timestamp of the most recent
	// [Store.RecordAsSent]. Zero when no send has been observed.
	LastSentAt time.Time

	// LastDeferredAt is the timestamp of the most recent
	// [Store.RecordAsDeferred] for this key. Lifetime — never cleared,
	// so it records the last-ever deferral even after later successful
	// sends. Zero only if no deferral has ever been observed. A
	// deferral is pending iff LastDeferredAt is after LastSentAt.
	LastDeferredAt time.Time

	// PeakSendsInWindow is the high-water mark of in-window send count
	// per attached cap, keyed by the cap's reason ("at cap",
	// "leading-edge debounce window", a custom reason, …): the largest
	// [Entry.CountInWindow] the cap's window has reached. Maintained by
	// [Store.RaisePeaks]. Schemaless on purpose — caps are deployment
	// config, so a map (not fixed fields) lets caps come and go without
	// a migration, mapping 1:1 to a BSON sub-document.
	PeakSendsInWindow map[string]uint64
}

// Store is the shared per-key send-state store. [Store.ReadEntry]
// drives the policies ([Entry.Seen], [pith/coalesce.Coalescer.ShouldDefer]);
// [Store.ReadMetrics] serves observability. Writes come from
// [pith/protect.Protector.RecordAsSent] (on successful sends) and
// [pith/protect.Protector.Check] (on Coalescer-driven deferrals).
type Store interface {
	// RecordAsSent stores (key → contentHash) stamped at now, appends
	// a timestamp to the key's LastNSendTimes, refreshes the Entry TTL,
	// increments TotalSent, and stamps LastSentAt. It touches only
	// send-side state — the deferred breadcrumb, LastNDeferredTimes,
	// and LastDeferredAt are all left intact; the newer send timestamp
	// is what makes a prior deferral no longer pending.
	RecordAsSent(ctx context.Context, key, contentHash string) error

	// RecordAsDeferred sets LastDeferredMessageRef, appends a timestamp
	// to LastNDeferredTimes, refreshes the Entry TTL, increments
	// TotalDeferred, and stamps LastDeferredAt. Does not touch
	// ContentHash, LastSentAt, or LastNSendTimes.
	RecordAsDeferred(ctx context.Context, key string, messageRef []byte) error

	// ReadEntry returns the key's working [Entry]. A miss, a record
	// whose TTL has expired, or a backing-store failure (err != nil)
	// all return the zero Entry — which reads as "nothing sent" so
	// callers fail-open (proceed). Backends honor the TTL on read
	// (treat expired as absent) rather than relying on deletion timing.
	ReadEntry(ctx context.Context, key string) (Entry, error)

	// ReadMetrics returns the key's lifetime [Metrics]. ok=false when
	// the key has no metrics record (never seen). err != nil signals a
	// backing-store failure.
	ReadMetrics(ctx context.Context, key string) (Metrics, bool, error)

	// RaisePeaks max-merges counts into the key's
	// [Metrics.PeakSendsInWindow]: each cap's stored peak is kept at
	// the larger of the stored and given value. A best-effort
	// observability write (callers may ignore its error). Backends
	// implement it as a single max-merge upsert (Mongo: a $max update).
	RaisePeaks(ctx context.Context, key string, counts map[string]uint64) error

	// RangeDeferred drives a consumer-side replay sweep: it visits keys
	// with a pending deferral — those whose most recent deferral is
	// newer than their most recent send — calling fn with the key and
	// its [Entry] (the breadcrumb is Entry.LastDeferredMessageRef; the
	// cadence for eligibility is via [Entry.CountDeferredInWindow]).
	//
	// Records are visited oldest-pending first (smallest most-recent
	// deferral timestamp), so a bounded sweep can't starve a
	// long-waiting breadcrumb. TTL-expired records are skipped (they
	// read as absent). At most limit records are visited (limit <= 0
	// means no bound); fn returns false to stop early.
	//
	// fn does the consumer-specific work — re-derive the payload from
	// the ref, re-emit via Check, and on a successful send RecordAsSent
	// (recency then makes the key no longer pending). pith provides no
	// orchestration beyond this enumeration.
	RangeDeferred(ctx context.Context, limit int, fn func(key string, e Entry) bool) error
}

// MemoryStore is an in-process [Store] backed by two [sync.Map]s —
// the in-memory analog of the Entry and Metrics backend collections.
// It is best-effort within one process; records written in one
// process are invisible to others. Use a shared-backing implementation
// when cross-process coordination is required.
type MemoryStore struct {
	// MaxSendTimes caps how many of the most recent
	// [Store.RecordAsSent] timestamps each key's [Entry.LastNSendTimes]
	// retains; appending past the cap drops the oldest. It must be
	// >= the largest send-count any attached policy needs to observe
	// within its window (e.g. the largest Coalescer hardCap), or
	// [Entry.CountInWindow] will undercount. Zero selects
	// [defaultMaxSendTimes]. Set before first use; not safe to mutate
	// concurrently with RecordAsSent.
	MaxSendTimes int

	ttl     time.Duration // required entry TTL, set by NewMemoryStore
	entries sync.Map      // key: string → value: *entryRecord (TTL'd)
	metrics sync.Map      // key: string → value: *Metrics (permanent)
	ops     atomic.Uint64
}

// maxSendTimes is the effective cap, applying [defaultMaxSendTimes]
// when MaxSendTimes is unset (<= 0).
func (m *MemoryStore) maxSendTimes() int {
	if m.MaxSendTimes > 0 {
		return m.MaxSendTimes
	}
	return defaultMaxSendTimes
}

// entryRecord is the stored Entry plus its TTL field — the in-memory
// mirror of an Entry-collection document (Entry fields + expireAt).
type entryRecord struct {
	entry    Entry
	expireAt time.Time
}

// NewMemoryStore returns a MemoryStore whose entries expire entryTTL
// after their last write. entryTTL is required and has no default — it
// MUST be >= the largest Coalescer window the store is used with (see
// the package "TTL semantics" note); [pith/protect.New] validates that.
// Panics if entryTTL <= 0.
func NewMemoryStore(entryTTL time.Duration) *MemoryStore {
	if entryTTL <= 0 {
		panic("sendstate: NewMemoryStore requires a positive entryTTL")
	}
	return &MemoryStore{ttl: entryTTL}
}

// EntryTTL reports the configured entry TTL. [pith/protect.New] reads
// it to validate the TTL against the attached Coalescer windows.
func (m *MemoryStore) EntryTTL() time.Duration { return m.ttl }

// RecordAsSent appends a send timestamp (bounded to the most recent
// maxSendTimes() entries), refreshes the Entry TTL, and advances the
// lifetime metrics. It touches only send-side state — the deferred ref
// and LastNDeferredTimes are preserved; the newer send timestamp is
// what makes any prior deferral no longer pending. The documented race
// with concurrent RecordAsDeferred is accepted.
func (m *MemoryStore) RecordAsSent(_ context.Context, key, contentHash string) error {
	now := time.Now()
	expireAt := now.Add(m.ttl)

	// Entry store.
	for {
		v, ok := m.entries.Load(key)
		if !ok {
			fresh := &entryRecord{
				entry:    Entry{ContentHash: contentHash, LastNSendTimes: []time.Time{now}},
				expireAt: expireAt,
			}
			if _, loaded := m.entries.LoadOrStore(key, fresh); !loaded {
				break
			}
			continue
		}
		prev := v.(*entryRecord)
		// Copy-on-write the slice so any prior reader's Entry stays
		// immutable, then bound it to the most recent maxSendTimes().
		// Carrying forward an expired record's stale timestamps is
		// harmless: the TTL > any window, so they never count.
		times := append(append([]time.Time(nil), prev.entry.LastNSendTimes...), now)
		if n := m.maxSendTimes(); len(times) > n {
			times = append([]time.Time(nil), times[len(times)-n:]...)
		}
		// Deferred-side state is preserved untouched — a send supersedes
		// a pending deferral by recency (newer send timestamp), not by
		// clearing the ref or the deferral list.
		next := &entryRecord{
			entry: Entry{
				ContentHash:            contentHash,
				LastDeferredMessageRef: prev.entry.LastDeferredMessageRef,
				LastNSendTimes:         times,
				LastNDeferredTimes:     prev.entry.LastNDeferredTimes,
			},
			expireAt: expireAt,
		}
		if m.entries.CompareAndSwap(key, prev, next) {
			break
		}
	}

	// Metrics store.
	for {
		v, ok := m.metrics.Load(key)
		if !ok {
			fresh := &Metrics{TotalSent: 1, LastSentAt: now}
			if _, loaded := m.metrics.LoadOrStore(key, fresh); !loaded {
				break
			}
			continue
		}
		prev := v.(*Metrics)
		// next := *prev preserves TotalDeferred, PeakSendsInWindow, and
		// LastDeferredAt — the last-deferred timestamp is lifetime and
		// never cleared; a later send just makes LastSentAt exceed it.
		next := *prev
		next.TotalSent = prev.TotalSent + 1
		next.LastSentAt = now
		if m.metrics.CompareAndSwap(key, prev, &next) {
			break
		}
	}

	m.maybeSweep()
	return nil
}

// RecordAsDeferred sets LastDeferredMessageRef, appends a deferral
// timestamp to LastNDeferredTimes (bounded like the send list),
// refreshes the Entry TTL, and advances the deferred-side metrics.
// Preserves ContentHash and LastNSendTimes. CAS-loops so concurrent
// calls don't clobber each other's writes.
func (m *MemoryStore) RecordAsDeferred(_ context.Context, key string, messageRef []byte) error {
	now := time.Now()
	expireAt := now.Add(m.ttl)

	// Entry store.
	for {
		v, ok := m.entries.Load(key)
		if !ok {
			fresh := &entryRecord{
				entry: Entry{
					LastDeferredMessageRef: messageRef,
					LastNDeferredTimes:     []time.Time{now},
				},
				expireAt: expireAt,
			}
			if _, loaded := m.entries.LoadOrStore(key, fresh); !loaded {
				break
			}
			continue
		}
		prev := v.(*entryRecord)
		dtimes := append(append([]time.Time(nil), prev.entry.LastNDeferredTimes...), now)
		if n := m.maxSendTimes(); len(dtimes) > n {
			dtimes = append([]time.Time(nil), dtimes[len(dtimes)-n:]...)
		}
		next := &entryRecord{
			entry: Entry{
				ContentHash:            prev.entry.ContentHash,
				LastDeferredMessageRef: messageRef,
				LastNSendTimes:         prev.entry.LastNSendTimes,
				LastNDeferredTimes:     dtimes,
			},
			expireAt: expireAt,
		}
		if m.entries.CompareAndSwap(key, prev, next) {
			break
		}
	}

	// Metrics store.
	for {
		v, ok := m.metrics.Load(key)
		if !ok {
			fresh := &Metrics{TotalDeferred: 1, LastDeferredAt: now}
			if _, loaded := m.metrics.LoadOrStore(key, fresh); !loaded {
				break
			}
			continue
		}
		prev := v.(*Metrics)
		next := *prev
		next.TotalDeferred = prev.TotalDeferred + 1
		next.LastDeferredAt = now
		if m.metrics.CompareAndSwap(key, prev, &next) {
			break
		}
	}

	m.maybeSweep()
	return nil
}

// ReadEntry returns the key's [Entry], honoring the TTL: a record
// whose expireAt has passed reads as the zero Entry, as does a miss.
func (m *MemoryStore) ReadEntry(_ context.Context, key string) (Entry, error) {
	v, ok := m.entries.Load(key)
	if !ok {
		return Entry{}, nil
	}
	r := v.(*entryRecord)
	if !r.expireAt.After(time.Now()) {
		return Entry{}, nil // expired but not yet swept
	}
	return r.entry, nil
}

// ReadMetrics returns the key's lifetime [Metrics]. ok=false when the
// key has never been recorded.
func (m *MemoryStore) ReadMetrics(_ context.Context, key string) (Metrics, bool, error) {
	v, ok := m.metrics.Load(key)
	if !ok {
		return Metrics{}, false, nil
	}
	met := *v.(*Metrics)
	// Hand back a copy of the map so callers can't mutate stored state.
	if met.PeakSendsInWindow != nil {
		cp := make(map[string]uint64, len(met.PeakSendsInWindow))
		for k, n := range met.PeakSendsInWindow {
			cp[k] = n
		}
		met.PeakSendsInWindow = cp
	}
	return met, true, nil
}

// RaisePeaks max-merges counts into the key's PeakSendsInWindow,
// creating the metrics record if absent. CAS-loops and only writes
// when a value actually rises.
func (m *MemoryStore) RaisePeaks(_ context.Context, key string, counts map[string]uint64) error {
	if len(counts) == 0 {
		return nil
	}
	for {
		v, ok := m.metrics.Load(key)
		if !ok {
			peaks := make(map[string]uint64, len(counts))
			for k, n := range counts {
				peaks[k] = n
			}
			if _, loaded := m.metrics.LoadOrStore(key, &Metrics{PeakSendsInWindow: peaks}); !loaded {
				return nil
			}
			continue
		}
		prev := v.(*Metrics)
		merged := make(map[string]uint64, len(prev.PeakSendsInWindow)+len(counts))
		for k, n := range prev.PeakSendsInWindow {
			merged[k] = n
		}
		changed := false
		for k, n := range counts {
			if n > merged[k] {
				merged[k] = n
				changed = true
			}
		}
		if !changed {
			return nil
		}
		next := *prev
		next.PeakSendsInWindow = merged
		if m.metrics.CompareAndSwap(key, prev, &next) {
			return nil
		}
	}
}

// RangeDeferred visits pending entries (most recent deferral newer
// than most recent send) oldest-pending first, skipping TTL-expired
// records, up to limit (<= 0 = unbounded).
func (m *MemoryStore) RangeDeferred(_ context.Context, limit int, fn func(key string, e Entry) bool) error {
	now := time.Now()

	type pendingKey struct {
		key        string
		entry      Entry
		deferredAt time.Time
	}
	var pending []pendingKey

	m.entries.Range(func(k, v interface{}) bool {
		r := v.(*entryRecord)
		if !r.expireAt.After(now) {
			return true // expired; reads as absent
		}
		deferredAt := lastTime(r.entry.LastNDeferredTimes)
		if !deferredAt.After(lastTime(r.entry.LastNSendTimes)) {
			return true // not pending (no deferral, or a send superseded it)
		}
		pending = append(pending, pendingKey{key: k.(string), entry: r.entry, deferredAt: deferredAt})
		return true
	})

	// Oldest-pending first so a bounded sweep can't starve a
	// long-waiting breadcrumb.
	sort.Slice(pending, func(i, j int) bool {
		return pending[i].deferredAt.Before(pending[j].deferredAt)
	})

	for i, p := range pending {
		if limit > 0 && i >= limit {
			break
		}
		if !fn(p.key, p.entry) {
			break
		}
	}
	return nil
}

// lastTime returns the final element of a chronological timestamp list,
// or the zero time when empty.
func lastTime(ts []time.Time) time.Time {
	if len(ts) == 0 {
		return time.Time{}
	}
	return ts[len(ts)-1]
}

// defaultMaxSendTimes bounds each key's LastNSendTimes list when
// [MemoryStore.MaxSendTimes] is unset. Generously larger than any
// realistic policy send-count so the default doesn't undercount
// [Entry.CountInWindow].
const defaultMaxSendTimes = 256

// maybeSweep runs a full [MemoryStore.sweep] roughly once per 1000
// writes, amortizing the map scan rather than paying it on every call.
func (m *MemoryStore) maybeSweep() {
	if m.ops.Add(1)%1000 == 0 {
		m.sweep()
	}
}

// sweep is the in-memory equivalent of the Entry collection's Mongo
// TTL deleter: it removes entryRecords whose expireAt has passed,
// reclaiming the memory their LastNSendTimes lists hold. It is pure
// storage reclamation — [MemoryStore.ReadEntry] already treats expired
// records as absent, so sweep timing never affects answers. The
// metrics map is never swept (lifetime Metrics).
func (m *MemoryStore) sweep() {
	now := time.Now()
	m.entries.Range(func(k, v interface{}) bool {
		r := v.(*entryRecord)
		if !r.expireAt.After(now) {
			// CompareAndDelete so a concurrent write that refreshed the
			// TTL (replacing r) survives.
			m.entries.CompareAndDelete(k, r)
		}
		return true
	})
}
