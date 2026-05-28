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
//     dedupe) and [Entry.CountSentInWindow] (the per-window send count
//     [pith/coalesce.Coalescer] thresholds against) — so a single
//     ReadEntry drives a whole [pith/protect.Protector.Check].
//   - [Metrics] — lifetime observability rollup: TotalSent,
//     TotalDeferred, LastSentAt, LastDeferredAt, and (when wired —
//     see TODO(peaks) on [Metrics.PeakSendsInWindow]) per-cap
//     PeakSendsInWindow high-water marks. Read via [Store.ReadMetrics].
//     Never expires.
//
// [Store.RecordAsSent] sets ContentHash, appends a timestamp to
// LastNSendTimes, refreshes the Entry TTL, increments TotalSent and
// stamps LastSentAt. It touches only send-side state — neither the
// LastDeferredMessageRef breadcrumb, LastNDeferredTimes, nor
// LastDeferredAt is cleared. [Store.RecordAsDeferred] sets
// LastDeferredMessageRef, appends to LastNDeferredTimes, refreshes the
// Entry TTL, increments TotalDeferred and stamps LastDeferredAt (no
// LastNSendTimes append — deferrals don't feed the [Entry.CountSentInWindow]
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
// policy. [Entry.CountSentInWindow] takes both the reference "now" and
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
// The TTL is required at construction (see [pith/sendstate/memory.New]) — pith
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
// carries them forward can change a [Entry.CountSentInWindow] result.
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
	"time"
)

// Entry is the per-key working state read by policies. It carries the
// read primitives directly: [Entry.Seen] (content dedupe),
// [Entry.CountSentInWindow] / [Entry.CountDeferredInWindow] (per-window
// counts that Coalescers and replay-eligibility predicates threshold
// against), and [Entry.LastSentTime] / [Entry.LastDeferredTime] (tail
// timestamps — convenient for the pending check
// `e.LastDeferredTime().After(e.LastSentTime())` and for surfacing
// "how long has this breadcrumb waited?" to consumers).
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
	// dropping the oldest past the cap
	// ([pith/sendstate/memory.Store.MaxSendTimes] for the in-memory
	// backend, [pith/sendstate/mongodb.WithMaxSendTimes] for the Mongo
	// backend); it is not time-pruned, so an entry within the cap may
	// predate any mechanism window. Treat as immutable from the
	// caller's side (the store copy-on-writes when mutating). Drives
	// [Entry.CountSentInWindow] (applies the window filter),
	// [Entry.LastSentTime] (most-recent tail), and [Entry.Seen]'s
	// "has ever sent" guard.
	LastNSendTimes []time.Time

	// LastNDeferredTimes is the deferral-side mirror of LastNSendTimes:
	// the most recent [Store.RecordAsDeferred] timestamps (oldest
	// first), appended on each Coalescer-driven defer and count-bounded
	// the same way. Read via [Entry.CountDeferredInWindow] (window
	// count) and [Entry.LastDeferredTime] (most-recent tail); not
	// cleared on a send. It lets a replay sweep express debounce
	// eligibility on the Entry alone — e.g. trailing-edge "gone quiet"
	// is CountDeferredInWindow(now, W) == 0, max-wait is >= K.
	//
	// A deferral is pending (awaiting replay) when the most recent
	// deferral is newer than the most recent send —
	// `e.LastDeferredTime().After(e.LastSentTime())`, equivalently
	// [Metrics.LastDeferredAt] > [Metrics.LastSentAt]. Nothing is
	// stored or cleared to mark it; recency alone resolves it.
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

// CountSentInWindow returns the number of send timestamps in
// LastNSendTimes within the trailing window ending at now. now is the
// caller's reference time (typically captured once per Check and
// shared across every policy). Pure; no I/O.
func (e Entry) CountSentInWindow(now time.Time, window time.Duration) int {
	return countInWindow(e.LastNSendTimes, now, window)
}

// CountDeferredInWindow is the deferral-side mirror of
// [Entry.CountSentInWindow]: the number of LastNDeferredTimes within the
// trailing window ending at now. A replay sweep uses it for debounce
// eligibility (e.g. == 0 means the burst has gone quiet). Pure; no I/O.
func (e Entry) CountDeferredInWindow(now time.Time, window time.Duration) int {
	return countInWindow(e.LastNDeferredTimes, now, window)
}

// LastSentTime returns the timestamp of the most recent
// [Store.RecordAsSent] for this key — the tail of LastNSendTimes — or
// the zero [time.Time] if no send has been recorded. Pure; no I/O.
func (e Entry) LastSentTime() time.Time { return lastTime(e.LastNSendTimes) }

// LastDeferredTime returns the timestamp of the most recent
// [Store.RecordAsDeferred] for this key — the tail of
// LastNDeferredTimes — or the zero [time.Time] if no deferral has been
// recorded. The deferral-pending check is
// `e.LastDeferredTime().After(e.LastSentTime())`. Pure; no I/O.
func (e Entry) LastDeferredTime() time.Time { return lastTime(e.LastNDeferredTimes) }

// lastTime returns the final element of a chronological timestamp list,
// or the zero time when empty.
func lastTime(ts []time.Time) time.Time {
	if len(ts) == 0 {
		return time.Time{}
	}
	return ts[len(ts)-1]
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
	// per attached cap, keyed by the cap's derived name — what
	// [pith/coalesce.Coalescer.CapPolicy] returns and what surfaces
	// as Outcome.Reason on a defer (e.g. "leading-edge debounce 10s",
	// "quota cap 50 per 24h", or a custom Coalescer's name). The
	// value is the largest [Entry.CountSentInWindow] the cap's window
	// has reached.
	//
	// TODO(peaks): not populated in the initial release — backends
	// leave this nil. The Store interface comment describes the
	// shape the recording methods will grow when we revisit. Map
	// (not fixed fields) on purpose: caps are deployment config, so
	// new ones come and go without a migration, mapping 1:1 to a
	// BSON sub-document.
	PeakSendsInWindow map[string]uint64
}

// Store is the shared per-key send-state store. [Store.ReadEntry]
// drives the policies ([Entry.Seen], [pith/coalesce.Coalescer.ShouldDefer]);
// [Store.ReadMetrics] serves observability. Writes come from
// [pith/protect.Protector.RecordAsSent] (on successful sends) and
// [pith/protect.Protector.Check] (on Coalescer-driven deferrals).
//
// TODO(peaks): peak observability ([Metrics.PeakSendsInWindow]) is
// intentionally NOT wired through Store in the initial release —
// backends leave the field unset. When we revisit, the natural shape
// is a peakBumps map[string]uint64 added to RecordAsSent and
// RecordAsDeferred so the Store can max-merge per-cap bumps atomically
// with the rest of the write; [pith/protect.Protector.Check] precomputes
// the bumps from the Entry it already reads.
type Store interface {
	// RecordAsSent stores (key → contentHash) stamped at now, appends
	// a timestamp to the key's LastNSendTimes, refreshes the Entry
	// TTL, increments TotalSent, and stamps LastSentAt. It touches
	// only send-side state — the deferred breadcrumb,
	// LastNDeferredTimes, and LastDeferredAt are all left intact; the
	// newer send timestamp is what makes a prior deferral no longer
	// pending.
	RecordAsSent(ctx context.Context, key, contentHash string) error

	// RecordAsDeferred sets LastDeferredMessageRef, appends a
	// timestamp to LastNDeferredTimes, refreshes the Entry TTL,
	// increments TotalDeferred, and stamps LastDeferredAt. Does not
	// touch ContentHash, LastSentAt, or LastNSendTimes.
	RecordAsDeferred(ctx context.Context, key string, messageRef []byte) error

	// ReadEntry returns the key's working [Entry]. Two distinct cases
	// both return the zero Entry, distinguishable by err:
	//
	//   - A miss or a record whose TTL has expired returns (zero
	//     [Entry], nil err). The zero Entry reads as "nothing sent",
	//     so policies naturally proceed.
	//   - A backing-store failure returns (zero [Entry], non-nil err).
	//     Callers fail-open (proceed) and surface the err for logging
	//     — see [pith/protect.Protector.Check].
	//
	// Backends honor the TTL on read (treat expired as absent) rather
	// than relying on deletion timing, so both backends answer
	// identically regardless of when the sweep/deleter runs.
	ReadEntry(ctx context.Context, key string) (Entry, error)

	// ReadMetrics returns the key's lifetime [Metrics]. ok=false when
	// the key has no metrics record (never seen). err != nil signals a
	// backing-store failure.
	ReadMetrics(ctx context.Context, key string) (Metrics, bool, error)

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
