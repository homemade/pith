// Package memory provides an in-process [sendstate.Store] backed by two
// [sync.Map]s — the in-memory analog of the Entry and Metrics backend
// collections. It is best-effort within one process: records written in
// one process are invisible to others, so use a shared-backing store
// (see pith/sendstate/mongodb) when cross-process coordination is required.
package memory

import (
	"context"
	"errors"
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
// Typical callers don't construct a Store directly: the
// [pith/protect/memory.NewReadProtector] / [pith/protect/memory.NewWriteProtector]
// factories build one internally and auto-size [Store.MaxSendTimes] to the
// largest attached Coalescer cap via [Store.GrowMaxSendTimes]. Direct
// construction via [New] is supported for tests or callers that need raw
// store access.
type Store struct {
	// MaxSendTimes caps how many of the most recent
	// [sendstate.Store.RecordAsSent] timestamps each key's
	// [sendstate.Entry.LastNSendTimes] retains; appending past the cap
	// drops the oldest. It must be >= the largest send-count any
	// attached policy needs to observe within its window (e.g. the
	// largest Coalescer hardCap), or [sendstate.Entry.CountSentInWindow]
	// will undercount. Zero selects [defaultMaxSendTimes].
	// The [pith/protect/memory] factories grow it to the largest attached
	// cap via [Store.GrowMaxSendTimes]. Set before first use; not safe to
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
// mirror of an Entry-collection document (Entry fields + expireAt +
// tenant + namespace). namespace is the sweep-scoping token stamped by
// RecordAsDeferred and carried forward untouched by RecordAsSent;
// RangeDeferred filters on it. tenant is the optional outer scoping
// token, also stamped on every write — labelling only, not part of the
// sweep filter (see [sendstate.Metrics.Tenant]).
type entryRecord struct {
	entry     sendstate.Entry
	expireAt  time.Time
	tenant    string
	namespace string
}

// New returns a Store whose entries expire entryTTL after their last
// write. entryTTL is required and has no default — it MUST be >= the
// largest Coalescer window the store is used with (see the sendstate
// package "TTL semantics" note); the [pith/protect/memory] factories
// validate that against the attached Coalescers. Panics if entryTTL <= 0.
func New(entryTTL time.Duration) *Store {
	if entryTTL <= 0 {
		panic("memory: New requires a positive entryTTL")
	}
	return &Store{ttl: entryTTL}
}

// EntryTTL reports the configured entry TTL. The [pith/protect/memory]
// factories read it to validate the TTL against the attached Coalescer
// windows.
func (m *Store) EntryTTL() time.Duration { return m.ttl }

// GrowMaxSendTimes raises [Store.MaxSendTimes] to at least n, never
// lowering a larger value the caller already set. The
// [pith/protect/memory] factories call it (via a structural interface,
// so they needn't import this package) to size the send-timestamp list
// to the largest attached Coalescer cap. Set before first use.
func (m *Store) GrowMaxSendTimes(n int) {
	if n > m.MaxSendTimes {
		m.MaxSendTimes = n
	}
}

// RecordAsSent appends a send timestamp (bounded to the most recent
// maxSendTimes() entries), refreshes the Entry TTL, stamps the
// tenant + namespace, and advances the lifetime metrics. It touches
// only send-side state — the deferred ref and LastNDeferredTimes are
// preserved; the newer send timestamp is what makes any prior deferral
// no longer pending. The documented race with concurrent RecordAsDeferred
// is accepted.
func (m *Store) RecordAsSent(_ context.Context, key, tenant, namespace, contentHash string) error {
	now := time.Now()
	expireAt := now.Add(m.ttl)

	// Peaks are only meaningful when more than one send time is retained
	// (maxSendTimes > 1); at the dedupe-only floor of 1 the window count is a
	// constant 1, so skip the fold and leave the peak fields unset. The cap
	// rises above the floor only when a windowing policy is in use (see
	// GrowMaxSendTimes), which is exactly when the peaks become meaningful.
	//
	// A window's peak is also folded ONLY when EntryTTL covers it: below that
	// the entry expires after a gap shorter than the window, so the count is a
	// misleading lower bound that silently resets (e.g. a 2h-TTL store folds
	// peak1h but skips peak24h). Skipped fields stay unset — absent, not a
	// deceptive 0.
	//
	// Rolling-window send counts at this send (when tracking) are captured as
	// the Entry CAS commits below and folded into the peaks in the metrics
	// loop. A trailing-window count peaks at an arrival, so sampling here is
	// exact over the bounded send-time list.
	fold1h := m.maxSendTimes() > 1 && m.ttl >= time.Hour
	fold24h := m.maxSendTimes() > 1 && m.ttl >= 24*time.Hour
	trackPeaks := fold1h || fold24h
	var sent1h, sent24h int

	// Entry store.
	for {
		v, ok := m.entries.Load(key)
		if !ok {
			fresh := &entryRecord{
				entry:     sendstate.Entry{ContentHash: contentHash, LastNSendTimes: []time.Time{now}},
				expireAt:  expireAt,
				tenant:    tenant,
				namespace: namespace,
			}
			if _, loaded := m.entries.LoadOrStore(key, fresh); !loaded {
				if trackPeaks {
					sent1h, sent24h = 1, 1 // first send: in both windows
				}
				break
			}
			continue
		}
		prev := v.(*entryRecord)
		// Copy-on-write the slice so any prior reader's Entry stays
		// immutable, then bound it to the most recent maxSendTimes().
		// Carrying forward an expired record's stale timestamps is
		// harmless: a window is only folded when TTL >= that window, so
		// timestamps from a record older than the TTL fall outside every
		// folded window and never count.
		times := append(append([]time.Time(nil), prev.entry.LastNSendTimes...), now)
		if n := m.maxSendTimes(); len(times) > n {
			times = append([]time.Time(nil), times[len(times)-n:]...)
		}
		// Deferred-side state is preserved untouched — a send
		// supersedes a pending deferral by recency (newer send
		// timestamp), not by clearing the ref or the deferral list.
		// tenant + namespace are constant per key, so re-stamping the
		// passed values matches any prior deferral's.
		next := &entryRecord{
			entry: sendstate.Entry{
				ContentHash:            contentHash,
				LastDeferredMessageRef: prev.entry.LastDeferredMessageRef,
				LastNSendTimes:         times,
				LastNDeferredTimes:     prev.entry.LastNDeferredTimes,
			},
			expireAt:  expireAt,
			tenant:    tenant,
			namespace: namespace,
		}
		if m.entries.CompareAndSwap(key, prev, next) {
			if trackPeaks {
				e := sendstate.Entry{LastNSendTimes: times}
				sent1h = e.CountSentInWindow(now, time.Hour)
				sent24h = e.CountSentInWindow(now, 24*time.Hour)
			}
			break
		}
	}

	// Metrics store.
	for {
		v, ok := m.metrics.Load(key)
		if !ok {
			fresh := &sendstate.Metrics{
				Tenant:      tenant,
				Namespace:   namespace,
				TotalSent:   1,
				FirstSentAt: now,
				LastSentAt:  now,
			}
			if fold1h {
				fresh.Peak1h, fresh.Peak1hAt = uint64(sent1h), now
			}
			if fold24h {
				fresh.Peak24h, fresh.Peak24hAt = uint64(sent24h), now
			}
			if _, loaded := m.metrics.LoadOrStore(key, fresh); !loaded {
				break
			}
			continue
		}
		prev := v.(*sendstate.Metrics)
		// next := *prev preserves TotalDeferred, FirstDeferredAt, and
		// LastDeferredAt — the deferred-side timestamps are lifetime and
		// never cleared; a later send just makes LastSentAt exceed
		// LastDeferredAt.
		next := *prev
		next.Tenant = tenant
		next.Namespace = namespace
		next.TotalSent = prev.TotalSent + 1
		next.LastSentAt = now
		// FirstSentAt is set-once. The metrics doc could have been
		// created on a prior deferral (TotalSent == 0 path), so check
		// the field itself rather than assuming "fresh doc means first".
		if next.FirstSentAt.IsZero() {
			next.FirstSentAt = now
		}
		// Peak high-water marks: strict > stamps PeakedAt only when the peak
		// rises, marking the first time a level is reached (later ties don't
		// move it). Lifetime — never decreased.
		if fold1h {
			if c := uint64(sent1h); c > next.Peak1h {
				next.Peak1h, next.Peak1hAt = c, now
			}
		}
		if fold24h {
			if c := uint64(sent24h); c > next.Peak24h {
				next.Peak24h, next.Peak24hAt = c, now
			}
		}
		if m.metrics.CompareAndSwap(key, prev, &next) {
			break
		}
	}

	m.maybeSweep()
	return nil
}

// RecordAsDeferred sets LastDeferredMessageRef, appends a deferral
// timestamp to LastNDeferredTimes (bounded like the send list),
// refreshes the Entry TTL, stamps the tenant + sweep-scoping namespace,
// and advances the deferred-side metrics. Preserves ContentHash and
// LastNSendTimes. CAS-loops so concurrent calls don't clobber each
// other's writes.
func (m *Store) RecordAsDeferred(_ context.Context, key, tenant, namespace string, messageRef []byte) error {
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
				expireAt:  expireAt,
				tenant:    tenant,
				namespace: namespace,
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
			expireAt:  expireAt,
			tenant:    tenant,
			namespace: namespace,
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
				Tenant:          tenant,
				Namespace:       namespace,
				TotalDeferred:   1,
				FirstDeferredAt: now,
				LastDeferredAt:  now,
			}
			if _, loaded := m.metrics.LoadOrStore(key, fresh); !loaded {
				break
			}
			continue
		}
		prev := v.(*sendstate.Metrics)
		next := *prev
		next.Tenant = tenant
		next.Namespace = namespace
		next.TotalDeferred = prev.TotalDeferred + 1
		next.LastDeferredAt = now
		// FirstDeferredAt is set-once — symmetric with the send side
		// (see RecordAsSent). The metrics doc could have been created
		// on a prior send, so check the field rather than assuming.
		if next.FirstDeferredAt.IsZero() {
			next.FirstDeferredAt = now
		}
		if m.metrics.CompareAndSwap(key, prev, &next) {
			break
		}
	}

	m.maybeSweep()
	return nil
}

// CheckAndReserve atomically (within this process) evaluates the dedupe
// layer + every req.DeferEvals against the key's TTL-honored entry and
// either reserves a send-slot (push) or stamps a deferred breadcrumb,
// matching the [sendstate.Store.CheckAndReserve] contract. Implementation
// is a CAS loop on the entry record: one fresh ReadEntry-equivalent
// (TTL-filtering inside the loop) per iteration drives all policy
// evaluation; the swap commits the outcome.
//
// Cross-process correctness is best-effort, like every other write on
// this backend — concurrent processes each have their own Store, so the
// cap holds only within one process. Use [pith/sendstate/mongodb] for
// multi-instance coordination.
func (m *Store) CheckAndReserve(_ context.Context, req sendstate.ReserveRequest, messageRef []byte) (sendstate.ReserveResult, error) {
	if len(req.Coalescers) == 0 {
		return sendstate.ReserveResult{}, errors.New("memory: CheckAndReserve requires at least one Coalescer")
	}

	now := time.Now()
	expireAt := now.Add(m.ttl)

	// Peak-fold inputs are captured below on a successful Proceed CAS and
	// then folded by the metrics-CAS step. Same conditions as RecordAsSent
	// (cap above the dedupe floor; TTL covers the window).
	fold1h := m.maxSendTimes() > 1 && m.ttl >= time.Hour
	fold24h := m.maxSendTimes() > 1 && m.ttl >= 24*time.Hour
	trackPeaks := fold1h || fold24h
	var sent1h, sent24h int

	var result sendstate.ReserveResult

	// Entry CAS — policy evaluation + reserve / breadcrumb-stamp commit.
	for {
		v, loaded := m.entries.Load(req.Key)

		// prev tracks the record currently in the map (the CAS witness);
		// entry is the policy-input view, zeroed when prev is TTL-expired
		// so dedupe / Coalescers see "nothing recorded yet."
		var prev *entryRecord
		var entry sendstate.Entry
		if loaded {
			prev = v.(*entryRecord)
			if prev.expireAt.After(now) {
				entry = prev.entry
			}
		}

		// Layer 1: content dedupe (write side only — ContentHash != "").
		// Deduped is a drop, not a write — nothing to swap, return now.
		if req.ContentHash != "" && entry.Seen(req.ContentHash) {
			return sendstate.ReserveResult{Deduped: true, Reason: "duplicate content"}, nil
		}

		// Layer 2: Coalescers in attached order; first defer wins.
		var deferReason string
		for _, c := range req.Coalescers {
			if c.ShouldDefer(entry, now) {
				deferReason = c.Name()
				break
			}
		}

		if deferReason != "" {
			// Deferred: stamp the breadcrumb on the entry (mirrors
			// RecordAsDeferred — append to LastNDeferredTimes, set
			// LastDeferredMessageRef, refresh TTL).
			dtimes := append(append([]time.Time(nil), entry.LastNDeferredTimes...), now)
			if n := m.maxSendTimes(); len(dtimes) > n {
				dtimes = append([]time.Time(nil), dtimes[len(dtimes)-n:]...)
			}
			next := &entryRecord{
				entry: sendstate.Entry{
					ContentHash:            entry.ContentHash,
					LastDeferredMessageRef: messageRef,
					LastNSendTimes:         entry.LastNSendTimes,
					LastNDeferredTimes:     dtimes,
				},
				expireAt:  expireAt,
				tenant:    req.Tenant,
				namespace: req.Namespace,
			}
			if prev == nil {
				if _, lost := m.entries.LoadOrStore(req.Key, next); !lost {
					result.Deferred = true
					result.Reason = deferReason
					break
				}
				continue
			}
			if m.entries.CompareAndSwap(req.Key, prev, next) {
				result.Deferred = true
				result.Reason = deferReason
				break
			}
			continue
		}

		// Proceed: reserve a send-slot. The ContentHash IS updated here
		// (the reserve is the optimistic record); a subsequent ReleaseReservation
		// pops the timestamp by value but does not roll back ContentHash —
		// see the sendstate package doc for the trade-off.
		stimes := append(append([]time.Time(nil), entry.LastNSendTimes...), now)
		if n := m.maxSendTimes(); len(stimes) > n {
			stimes = append([]time.Time(nil), stimes[len(stimes)-n:]...)
		}
		next := &entryRecord{
			entry: sendstate.Entry{
				ContentHash:            req.ContentHash, // empty on read gates — preserved as empty
				LastDeferredMessageRef: entry.LastDeferredMessageRef,
				LastNSendTimes:         stimes,
				LastNDeferredTimes:     entry.LastNDeferredTimes,
			},
			expireAt:  expireAt,
			tenant:    req.Tenant,
			namespace: req.Namespace,
		}
		if prev == nil {
			if _, lost := m.entries.LoadOrStore(req.Key, next); !lost {
				if trackPeaks {
					e := sendstate.Entry{LastNSendTimes: stimes}
					sent1h = e.CountSentInWindow(now, time.Hour)
					sent24h = e.CountSentInWindow(now, 24*time.Hour)
				}
				result.ReservedAt = now
				break
			}
			continue
		}
		if m.entries.CompareAndSwap(req.Key, prev, next) {
			if trackPeaks {
				e := sendstate.Entry{LastNSendTimes: stimes}
				sent1h = e.CountSentInWindow(now, time.Hour)
				sent24h = e.CountSentInWindow(now, 24*time.Hour)
			}
			result.ReservedAt = now
			break
		}
	}

	// Metrics CAS — bump lifetime counters according to the outcome. Same
	// semantic split as RecordAsSent / RecordAsDeferred; we inline both
	// branches rather than calling those methods to avoid stamping a
	// second "now" or double-pushing.
	if result.Deferred {
		m.bumpDeferredMetrics(req.Key, req.Tenant, req.Namespace, now)
	} else {
		m.bumpSentMetrics(req.Key, req.Tenant, req.Namespace, now, fold1h, fold24h, sent1h, sent24h)
	}

	m.maybeSweep()
	return result, nil
}

// ReleaseReservation pops the send-timestamp at reservedAt from the key's
// LastNSendTimes — by value, so concurrent sibling reserves on the same
// key keep their slots. Lifetime metrics (TotalSent, peaks, LastSentAt)
// are *not* rolled back: the count tracks reserve attempts, and the peaks
// captured "level reached." See sendstate package doc for the trade-off.
//
// Best-effort — a missing record (TTL'd or never created) or a missing
// timestamp (already swept by maxSendTimes overflow) is a silent no-op.
func (m *Store) ReleaseReservation(_ context.Context, key string, reservedAt time.Time) error {
	for {
		v, ok := m.entries.Load(key)
		if !ok {
			return nil // gone — slot already absent
		}
		prev := v.(*entryRecord)

		// Find the most-recent occurrence of reservedAt. Walk from the tail
		// (newest first) because a freshly-failed reserve is overwhelmingly
		// near the tail.
		idx := -1
		for i := len(prev.entry.LastNSendTimes) - 1; i >= 0; i-- {
			if prev.entry.LastNSendTimes[i].Equal(reservedAt) {
				idx = i
				break
			}
		}
		if idx == -1 {
			return nil // not present — overflow popped it or a sibling beat us
		}

		stimes := make([]time.Time, 0, len(prev.entry.LastNSendTimes)-1)
		stimes = append(stimes, prev.entry.LastNSendTimes[:idx]...)
		stimes = append(stimes, prev.entry.LastNSendTimes[idx+1:]...)

		next := &entryRecord{
			entry: sendstate.Entry{
				ContentHash:            prev.entry.ContentHash,
				LastDeferredMessageRef: prev.entry.LastDeferredMessageRef,
				LastNSendTimes:         stimes,
				LastNDeferredTimes:     prev.entry.LastNDeferredTimes,
			},
			expireAt:  prev.expireAt,
			tenant:    prev.tenant,
			namespace: prev.namespace,
		}
		if m.entries.CompareAndSwap(key, prev, next) {
			return nil
		}
		// CAS conflict — concurrent write to this key; retry.
	}
}

// bumpSentMetrics is the metrics-CAS portion of RecordAsSent + CheckAndReserve's
// Proceed branch. Factored so both call sites stamp identical lifetime
// updates from the same now and pre-counted peak inputs.
func (m *Store) bumpSentMetrics(key, tenant, namespace string, now time.Time, fold1h, fold24h bool, sent1h, sent24h int) {
	for {
		v, loaded := m.metrics.Load(key)
		if !loaded {
			fresh := &sendstate.Metrics{
				Tenant:      tenant,
				Namespace:   namespace,
				TotalSent:   1,
				FirstSentAt: now,
				LastSentAt:  now,
			}
			if fold1h {
				fresh.Peak1h, fresh.Peak1hAt = uint64(sent1h), now
			}
			if fold24h {
				fresh.Peak24h, fresh.Peak24hAt = uint64(sent24h), now
			}
			if _, lost := m.metrics.LoadOrStore(key, fresh); !lost {
				return
			}
			continue
		}
		prev := v.(*sendstate.Metrics)
		next := *prev
		next.Tenant = tenant
		next.Namespace = namespace
		next.TotalSent = prev.TotalSent + 1
		next.LastSentAt = now
		if next.FirstSentAt.IsZero() {
			next.FirstSentAt = now
		}
		if fold1h {
			if c := uint64(sent1h); c > next.Peak1h {
				next.Peak1h, next.Peak1hAt = c, now
			}
		}
		if fold24h {
			if c := uint64(sent24h); c > next.Peak24h {
				next.Peak24h, next.Peak24hAt = c, now
			}
		}
		if m.metrics.CompareAndSwap(key, prev, &next) {
			return
		}
	}
}

// bumpDeferredMetrics is the metrics-CAS portion of RecordAsDeferred + the
// Deferred branch of CheckAndReserve. Same factoring rationale as
// bumpSentMetrics.
func (m *Store) bumpDeferredMetrics(key, tenant, namespace string, now time.Time) {
	for {
		v, loaded := m.metrics.Load(key)
		if !loaded {
			fresh := &sendstate.Metrics{
				Tenant:          tenant,
				Namespace:       namespace,
				TotalDeferred:   1,
				FirstDeferredAt: now,
				LastDeferredAt:  now,
			}
			if _, lost := m.metrics.LoadOrStore(key, fresh); !lost {
				return
			}
			continue
		}
		prev := v.(*sendstate.Metrics)
		next := *prev
		next.Tenant = tenant
		next.Namespace = namespace
		next.TotalDeferred = prev.TotalDeferred + 1
		next.LastDeferredAt = now
		if next.FirstDeferredAt.IsZero() {
			next.FirstDeferredAt = now
		}
		if m.metrics.CompareAndSwap(key, prev, &next) {
			return
		}
	}
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
	return *v.(*sendstate.Metrics), true, nil
}

// RangeDeferred visits pending entries (most recent deferral newer
// than most recent send) oldest-pending first, skipping TTL-expired
// records, up to limit (<= 0 = unbounded). When namespace is non-empty
// only entries stamped with it are visited (so limit applies within the
// namespace); "" visits every namespace.
func (m *Store) RangeDeferred(_ context.Context, limit int, namespace string, fn func(key string, e sendstate.Entry) bool) error {
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
		if namespace != "" && r.namespace != namespace {
			return true // out of the requested namespace's scope
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
