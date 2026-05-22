// Package sendstate is the shared per-key state that pith's
// read-only mechanisms ([pith/dedupe.Deduper], [pith/coalesce.Coalescer])
// consult to make their decisions. Callers (and the [pith/protect]
// orchestration layer) write here, and the mechanisms layer
// policies over the same records.
//
// The model is one Entry per key, plus an internal rolling list of
// send timestamps and lifetime counters (exposed via [Store.Metrics]
// and [Store.CountInWindow]).
//
//   - Entry holds the most recent send's ContentHash + LastSentAt,
//     and the most recent deferral's LastDeferredAt +
//     LastDeferredMessageRef. Returned by [Store.Lookup].
//   - The timestamp list is internal to the implementation; it
//     drives [Store.CountInWindow], which Coalescers consult to
//     decide ShouldDefer.
//   - Lifetime counters (TotalSent, TotalDeferred) are exposed via
//     [Store.Metrics] for observability dashboards.
//
// [Store.RecordAsSent] sets ContentHash + LastSentAt, clears the
// deferred-side fields, appends a timestamp to the internal list,
// and increments TotalSent. [Store.RecordAsDeferred] sets
// LastDeferredAt + LastDeferredMessageRef and increments
// TotalDeferred (no timestamp list append — deferrals don't feed
// the ShouldDefer count, which tracks successful sends only).
//
// The window — "how many sends in the last W?" — is a read-side
// policy. CountInWindow accepts the window per-call so multiple
// Coalescers (each with their own window) can share one Store.
//
// The contract is record-on-success: callers RecordAsSent only
// after the gated operation has actually succeeded. RecordAsDeferred
// is called by [pith/protect.Protector.Check] itself when any
// attached Coalescer's ShouldDefer returns true — for the bundled
// debounce / at-cap mechanisms or any caller-attached cap.
//
// # Concurrent deferred-then-sent race
//
// RecordAsSent unconditionally clears LastDeferredAt and
// LastDeferredMessageRef. If a fresh RecordAsDeferred call races
// with a sweep's RecordAsSent — i.e. a new deferral lands between
// the sweep's Lookup and its RecordAsSent — the fresher message
// ref is lost. The system is eventually consistent: the next
// inbound deferred event will re-stamp, and the next sweep will
// pick it up.
package sendstate

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Entry is one slot's recorded state.
type Entry struct {
	// ContentHash is the fingerprint of the message body most
	// recently passed to [Store.RecordAsSent]. Callers choose the
	// hashing scheme; pith only compares strings.
	ContentHash string

	// LastSentAt is the timestamp of the most recent
	// [Store.RecordAsSent].
	LastSentAt time.Time

	// LastDeferredAt is the timestamp of the most recent
	// [Store.RecordAsDeferred]. Zero when no deferral has been
	// observed since the last send (or ever).
	LastDeferredAt time.Time

	// LastDeferredMessageRef is the caller-supplied message ref
	// from the most recent [Store.RecordAsDeferred]. Pith treats
	// this as an opaque byte slice. Cleared on [Store.RecordAsSent].
	LastDeferredMessageRef []byte
}

// Metrics is a per-key observability snapshot. Returned by
// [Store.Metrics].
type Metrics struct {
	// TotalSent is the lifetime count of [Store.RecordAsSent] calls
	// for this key.
	TotalSent uint64

	// TotalDeferred is the lifetime count of
	// [Store.RecordAsDeferred] calls for this key.
	TotalDeferred uint64

	// LastSentAt mirrors [Entry.LastSentAt] for convenience.
	LastSentAt time.Time

	// LastDeferredAt mirrors [Entry.LastDeferredAt] for convenience.
	LastDeferredAt time.Time
}

// Store is the shared per-key send-state store consulted by
// mechanism read methods (e.g. [pith/dedupe.Deduper.SeenInWindow],
// [pith/coalesce.Coalescer.ShouldDefer]) and written by
// [pith/protect.Protector.RecordAsSent] (on successful sends) and
// [pith/protect.Protector.Check] (on debounce / at-cap deferrals).
type Store interface {
	// RecordAsSent stores (key → contentHash) stamped at now,
	// appends a timestamp to the internal rolling list, increments
	// TotalSent, and clears LastDeferredAt + LastDeferredMessageRef.
	RecordAsSent(ctx context.Context, key, contentHash string) error

	// RecordAsDeferred stamps LastDeferredAt + LastDeferredMessageRef
	// and increments TotalDeferred. Does not touch ContentHash,
	// LastSentAt, or the send-timestamp list.
	RecordAsDeferred(ctx context.Context, key string, messageRef []byte) error

	// Lookup returns the entry for key. ok=false when no entry
	// exists. err != nil signals a backing-store failure; callers
	// must treat it as fail-open (proceed as if ok=false).
	Lookup(ctx context.Context, key string) (Entry, bool, error)

	// CountInWindow returns the number of [Store.RecordAsSent]
	// calls for key whose timestamp is within the trailing
	// window from now. Used by [pith/coalesce.Coalescer.ShouldDefer]
	// to apply per-key cap policies. err != nil signals a
	// backing-store failure; callers must treat it as fail-open.
	CountInWindow(ctx context.Context, key string, window time.Duration) (int, error)

	// Metrics returns the per-key observability snapshot. ok=false
	// when no entry exists for key.
	Metrics(ctx context.Context, key string) (Metrics, bool, error)
}

// MemoryStore is an in-process [Store] backed by a [sync.Map]. It
// is best-effort within one process; entries written in one
// process are invisible to others. Use a shared-backing
// implementation when cross-process coordination is required.
//
// Internally entries are stored as *entryData (Entry + send
// timestamps + lifetime counters); Lookup and Metrics return
// value-copy snapshots of the publicly relevant fields.
type MemoryStore struct {
	entries sync.Map // key: string → value: *entryData
	ops     atomic.Uint64
}

// entryData is the private full per-key record. Public API exposes
// it via [Entry] (subset) and [Metrics] (subset). The send
// timestamp list lives here because it drives CountInWindow but is
// not part of any consumer-facing struct.
type entryData struct {
	entry         Entry
	totalSent     uint64
	totalDeferred uint64
	sendTimes     []time.Time
}

// NewMemoryStore returns a ready-to-use MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

// RecordAsSent stores (key → contentHash) stamped at now, appends
// a timestamp to the rolling list, and increments TotalSent.
// Unconditional overwrite on the Entry fields — the documented race
// with concurrent RecordAsDeferred is accepted.
func (m *MemoryStore) RecordAsSent(_ context.Context, key, contentHash string) error {
	now := time.Now()
	for {
		v, ok := m.entries.Load(key)
		if !ok {
			fresh := &entryData{
				entry:     Entry{ContentHash: contentHash, LastSentAt: now},
				totalSent: 1,
				sendTimes: []time.Time{now},
			}
			if _, loaded := m.entries.LoadOrStore(key, fresh); !loaded {
				break
			}
			continue
		}
		prev := v.(*entryData)
		next := *prev
		next.entry = Entry{ContentHash: contentHash, LastSentAt: now}
		next.totalSent = prev.totalSent + 1
		// Copy-on-write the slice so the previous snapshot stays
		// immutable for any concurrent reader holding it.
		next.sendTimes = append(append([]time.Time(nil), prev.sendTimes...), now)
		if m.entries.CompareAndSwap(key, prev, &next) {
			break
		}
	}
	m.maybeSweep()
	return nil
}

// RecordAsDeferred stamps LastDeferredAt + LastDeferredMessageRef
// and increments TotalDeferred. Preserves ContentHash and
// LastSentAt. CAS-loops so concurrent calls don't clobber each
// other's writes.
func (m *MemoryStore) RecordAsDeferred(_ context.Context, key string, messageRef []byte) error {
	now := time.Now()
	for {
		v, ok := m.entries.Load(key)
		if !ok {
			fresh := &entryData{
				entry:         Entry{LastDeferredAt: now, LastDeferredMessageRef: messageRef},
				totalDeferred: 1,
			}
			if _, loaded := m.entries.LoadOrStore(key, fresh); !loaded {
				break
			}
			continue
		}
		prev := v.(*entryData)
		next := *prev
		next.entry.LastDeferredAt = now
		next.entry.LastDeferredMessageRef = messageRef
		next.totalDeferred = prev.totalDeferred + 1
		if m.entries.CompareAndSwap(key, prev, &next) {
			break
		}
	}
	m.maybeSweep()
	return nil
}

// Lookup returns a value copy of the entry for key.
func (m *MemoryStore) Lookup(_ context.Context, key string) (Entry, bool, error) {
	v, ok := m.entries.Load(key)
	if !ok {
		return Entry{}, false, nil
	}
	return v.(*entryData).entry, true, nil
}

// CountInWindow returns the number of recorded sends for key whose
// timestamp falls within the trailing window from now. Implemented
// as a linear scan of the rolling list; for in-memory use with a
// realistic cap (tens per key) this is cheap.
func (m *MemoryStore) CountInWindow(_ context.Context, key string, window time.Duration) (int, error) {
	v, ok := m.entries.Load(key)
	if !ok {
		return 0, nil
	}
	d := v.(*entryData)
	cutoff := time.Now().Add(-window)
	// sendTimes is append-only chronological; count from the tail.
	n := 0
	for i := len(d.sendTimes) - 1; i >= 0; i-- {
		if d.sendTimes[i].Before(cutoff) {
			break
		}
		n++
	}
	return n, nil
}

// Metrics returns the per-key observability snapshot.
func (m *MemoryStore) Metrics(_ context.Context, key string) (Metrics, bool, error) {
	v, ok := m.entries.Load(key)
	if !ok {
		return Metrics{}, false, nil
	}
	d := v.(*entryData)
	return Metrics{
		TotalSent:      d.totalSent,
		TotalDeferred:  d.totalDeferred,
		LastSentAt:     d.entry.LastSentAt,
		LastDeferredAt: d.entry.LastDeferredAt,
	}, true, nil
}

// memorySweepHorizon is how long the in-memory implementation
// keeps an entry around after its last write. Generously larger
// than any realistic mechanism window so legitimate hits aren't
// swept out from under callers.
const memorySweepHorizon = 48 * time.Hour

func (m *MemoryStore) maybeSweep() {
	if m.ops.Add(1)%1000 == 0 {
		m.sweep()
	}
}

// sweep removes entries whose most recent timestamp (the later of
// LastSentAt and LastDeferredAt) is older than [memorySweepHorizon].
// It also prunes old timestamps from each entry's send-time list so
// it doesn't grow unbounded.
func (m *MemoryStore) sweep() {
	cutoff := time.Now().Add(-memorySweepHorizon)
	m.entries.Range(func(k, v interface{}) bool {
		d := v.(*entryData)
		last := d.entry.LastSentAt
		if d.entry.LastDeferredAt.After(last) {
			last = d.entry.LastDeferredAt
		}
		if last.Before(cutoff) {
			m.entries.Delete(k)
			return true
		}
		// Prune old send timestamps. CAS-update so a concurrent
		// RecordAsSent doesn't overwrite a stale snapshot.
		i := 0
		for ; i < len(d.sendTimes); i++ {
			if !d.sendTimes[i].Before(cutoff) {
				break
			}
		}
		if i > 0 {
			next := *d
			next.sendTimes = append([]time.Time(nil), d.sendTimes[i:]...)
			m.entries.CompareAndSwap(k, d, &next)
		}
		return true
	})
}
