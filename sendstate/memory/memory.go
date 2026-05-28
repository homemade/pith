// Package memory provides an in-process [sendstate.Store] backed by two
// [sync.Map]s — the in-memory analog of the Entry and Metrics backend
// collections. It is best-effort within one process: records written in
// one process are invisible to others, so use a shared-backing store
// (see pith/sendstate/mongodb) when cross-process coordination is required.
package memory

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/homemade/pith/sendstate"
)

// Compile-time check that Store satisfies [sendstate.Store].
var _ sendstate.Store = (*Store)(nil)

// Store is an in-process [sendstate.Store] backed by two [sync.Map]s —
// the in-memory mirror of the Entry (TTL'd working state) and Metrics
// (permanent lifetime rollup) backend collections. It is best-effort
// within one process; for cross-process coordination use a
// shared-backing store (see [pith/sendstate/mongodb]).
//
// Construct with [New]; wire into a Protector by passing it as the
// first argument to [pith/protect.New]. The Protector auto-sizes
// [Store.MaxSendTimes] to the largest attached Coalescer cap via
// [Store.GrowMaxSendTimes], so the typical caller doesn't set it.
type Store struct {
	// MaxSendTimes caps how many of the most recent
	// [sendstate.Store.RecordAsSent] timestamps each key's
	// [sendstate.Entry.LastNSendTimes] retains; appending past the cap
	// drops the oldest. It must be >= the largest send-count any
	// attached policy needs to observe within its window (e.g. the
	// largest Coalescer hardCap), or [sendstate.Entry.CountSentInWindow]
	// will undercount. Zero selects [defaultMaxSendTimes].
	// [pith/protect.New] grows it to the largest attached cap via
	// [Store.GrowMaxSendTimes]. Set before first use; not safe to
	// mutate concurrently with RecordAsSent.
	MaxSendTimes int

	ttl     time.Duration // required entry TTL, set by New
	entries sync.Map      // key: string → value: *entryRecord (TTL'd)
	metrics sync.Map      // key: string → value: *sendstate.Metrics (permanent)
	ops     atomic.Uint64
}

// maxSendTimes is the effective cap, applying [defaultMaxSendTimes]
// when MaxSendTimes is unset (<= 0).
func (m *Store) maxSendTimes() int {
	if m.MaxSendTimes > 0 {
		return m.MaxSendTimes
	}
	return defaultMaxSendTimes
}

// entryRecord is the stored Entry plus its TTL field — the in-memory
// mirror of an Entry-collection document (Entry fields + expireAt).
type entryRecord struct {
	entry    sendstate.Entry
	expireAt time.Time
}

// New returns a Store whose entries expire entryTTL after their last
// write. entryTTL is required and has no default — it MUST be >= the
// largest Coalescer window the store is used with (see the sendstate
// package "TTL semantics" note); [pith/protect.New] validates that.
// Panics if entryTTL <= 0.
func New(entryTTL time.Duration) *Store {
	if entryTTL <= 0 {
		panic("memory: New requires a positive entryTTL")
	}
	return &Store{ttl: entryTTL}
}

// EntryTTL reports the configured entry TTL. [pith/protect.New] reads
// it to validate the TTL against the attached Coalescer windows.
func (m *Store) EntryTTL() time.Duration { return m.ttl }

// GrowMaxSendTimes raises [Store.MaxSendTimes] to at least n, never
// lowering a larger value the caller already set. [pith/protect.New]
// calls it (via a structural interface, so it needn't import this
// package) to size the send-timestamp list to the largest attached
// Coalescer cap. Set before first use.
func (m *Store) GrowMaxSendTimes(n int) {
	if n > m.MaxSendTimes {
		m.MaxSendTimes = n
	}
}

// RecordAsSent appends a send timestamp (bounded to the most recent
// maxSendTimes() entries), refreshes the Entry TTL, and advances the
// lifetime metrics. It touches only send-side state — the deferred
// ref and LastNDeferredTimes are preserved; the newer send timestamp
// is what makes any prior deferral no longer pending. The documented
// race with concurrent RecordAsDeferred is accepted.
//
// TODO(peaks): [sendstate.Metrics.PeakSendsInWindow] is intentionally
// not populated in the initial release. The Store interface comment
// describes the shape the recording methods will grow when we revisit.
func (m *Store) RecordAsSent(_ context.Context, key, contentHash string) error {
	now := time.Now()
	expireAt := now.Add(m.ttl)

	// Entry store.
	for {
		v, ok := m.entries.Load(key)
		if !ok {
			fresh := &entryRecord{
				entry:    sendstate.Entry{ContentHash: contentHash, LastNSendTimes: []time.Time{now}},
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
		// Deferred-side state is preserved untouched — a send
		// supersedes a pending deferral by recency (newer send
		// timestamp), not by clearing the ref or the deferral list.
		next := &entryRecord{
			entry: sendstate.Entry{
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
			fresh := &sendstate.Metrics{
				TotalSent:  1,
				LastSentAt: now,
			}
			if _, loaded := m.metrics.LoadOrStore(key, fresh); !loaded {
				break
			}
			continue
		}
		prev := v.(*sendstate.Metrics)
		// next := *prev preserves TotalDeferred and LastDeferredAt —
		// the last-deferred timestamp is lifetime and never cleared; a
		// later send just makes LastSentAt exceed it.
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
//
// TODO(peaks): see the matching note on RecordAsSent.
func (m *Store) RecordAsDeferred(_ context.Context, key string, messageRef []byte) error {
	now := time.Now()
	expireAt := now.Add(m.ttl)

	// Entry store.
	for {
		v, ok := m.entries.Load(key)
		if !ok {
			fresh := &entryRecord{
				entry: sendstate.Entry{
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
			entry: sendstate.Entry{
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
			fresh := &sendstate.Metrics{
				TotalDeferred:  1,
				LastDeferredAt: now,
			}
			if _, loaded := m.metrics.LoadOrStore(key, fresh); !loaded {
				break
			}
			continue
		}
		prev := v.(*sendstate.Metrics)
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

// ReadEntry returns the key's [sendstate.Entry], honoring the TTL: a
// record whose expireAt has passed reads as the zero Entry, as does a
// miss.
func (m *Store) ReadEntry(_ context.Context, key string) (sendstate.Entry, error) {
	v, ok := m.entries.Load(key)
	if !ok {
		return sendstate.Entry{}, nil
	}
	r := v.(*entryRecord)
	if !r.expireAt.After(time.Now()) {
		return sendstate.Entry{}, nil // expired but not yet swept
	}
	return r.entry, nil
}

// ReadMetrics returns the key's lifetime [sendstate.Metrics]. ok=false
// when the key has never been recorded.
func (m *Store) ReadMetrics(_ context.Context, key string) (sendstate.Metrics, bool, error) {
	v, ok := m.metrics.Load(key)
	if !ok {
		return sendstate.Metrics{}, false, nil
	}
	met := *v.(*sendstate.Metrics)
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

// RangeDeferred visits pending entries (most recent deferral newer
// than most recent send) oldest-pending first, skipping TTL-expired
// records, up to limit (<= 0 = unbounded).
func (m *Store) RangeDeferred(_ context.Context, limit int, fn func(key string, e sendstate.Entry) bool) error {
	now := time.Now()

	type pendingKey struct {
		key        string
		entry      sendstate.Entry
		deferredAt time.Time
	}
	var pending []pendingKey

	m.entries.Range(func(k, v interface{}) bool {
		r := v.(*entryRecord)
		if !r.expireAt.After(now) {
			return true // expired; reads as absent
		}
		deferredAt := r.entry.LastDeferredTime()
		if !deferredAt.After(r.entry.LastSentTime()) {
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

// defaultMaxSendTimes bounds each key's LastNSendTimes list when
// [Store.MaxSendTimes] is unset. Generously larger than any realistic
// policy send-count so the default doesn't undercount
// [sendstate.Entry.CountSentInWindow].
const defaultMaxSendTimes = 256

// maybeSweep runs a full [Store.sweep] roughly once per 1000 writes,
// amortizing the map scan rather than paying it on every call.
func (m *Store) maybeSweep() {
	if m.ops.Add(1)%1000 == 0 {
		m.sweep()
	}
}

// sweep is the in-memory equivalent of the Entry collection's Mongo
// TTL deleter: it removes entryRecords whose expireAt has passed,
// reclaiming the memory their LastNSendTimes lists hold. It is pure
// storage reclamation — [Store.ReadEntry] already treats expired
// records as absent, so sweep timing never affects answers. The
// metrics map is never swept (lifetime Metrics).
func (m *Store) sweep() {
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
