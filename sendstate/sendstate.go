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
//     TotalDeferred, LastSentAt, LastDeferredAt. Read via
//     [Store.ReadMetrics]. Never expires.
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
//
// Paired with the Total* counters, FirstSentAt / FirstDeferredAt
// define an "activity span" for each side — useful for cheap
// per-key averages like sends-per-day:
//
//	if !met.FirstSentAt.IsZero() && met.TotalSent > 1 {
//	    span := met.LastSentAt.Sub(met.FirstSentAt)
//	    rate := float64(met.TotalSent) / span.Hours()
//	}
type Metrics struct {
	// Namespace is the sweep-scoping token this key belongs to (the
	// caller-defined value bound on the protector's namespace handle; ""
	// = the whole store). Stamped on every [Store.RecordAsSent] /
	// [Store.RecordAsDeferred], so it is present even for a send-only key.
	// Constant per key. Lets observability group/filter lifetime metrics by
	// namespace (e.g. per-tenant cap-pressure) without parsing the key.
	Namespace string

	// TotalSent is the lifetime count of [Store.RecordAsSent] calls
	// for this key.
	TotalSent uint64

	// TotalDeferred is the lifetime count of
	// [Store.RecordAsDeferred] calls for this key.
	TotalDeferred uint64

	// FirstSentAt is the timestamp of the *first* [Store.RecordAsSent]
	// ever observed for this key — set once and never updated. Zero
	// when no send has been observed. With LastSentAt it defines the
	// active-send window for this key.
	FirstSentAt time.Time

	// FirstDeferredAt is the timestamp of the *first*
	// [Store.RecordAsDeferred] ever observed for this key — set once
	// and never updated. Zero when no deferral has been observed.
	// With LastDeferredAt it defines the active-deferral window.
	FirstDeferredAt time.Time

	// LastSentAt is the timestamp of the most recent
	// [Store.RecordAsSent]. Zero when no send has been observed.
	LastSentAt time.Time

	// LastDeferredAt is the timestamp of the most recent
	// [Store.RecordAsDeferred] for this key. Lifetime — never cleared,
	// so it records the last-ever deferral even after later successful
	// sends. Zero only if no deferral has ever been observed. A
	// deferral is pending iff LastDeferredAt is after LastSentAt.
	LastDeferredAt time.Time

	// Peak1h is the high-water mark of sends within any rolling 1-hour
	// window for this key: the maximum [Entry.CountSentInWindow] over a 1h
	// window observed at a [Store.RecordAsSent]. Monotonic — only ever
	// rises, and never cleared, so it survives the expiry of the underlying
	// send timestamps. Zero when no send has been observed.
	//
	// Accuracy bound: the count is taken over the bounded send-time list, so
	// Peak1h is exact only while that list retains a full window of sends;
	// if the cap is smaller than the busiest window it observes, Peak1h is a
	// lower bound.
	Peak1h uint64

	// Peak1hAt is when Peak1h was first reached — the [Store.RecordAsSent]
	// timestamp whose 1h window count first exceeded the prior peak (stamped
	// on a strict increase, so it marks the first time the level was hit and
	// is not moved by later ties). Distinct from LastSentAt whenever the
	// peak predates the most recent send. Zero when no send has been
	// observed.
	Peak1hAt time.Time

	// Peak24h is the high-water mark of sends within any rolling 24-hour
	// window — the 24h analogue of Peak1h, with the same monotonicity and
	// accuracy bound.
	Peak24h uint64

	// Peak24hAt is when Peak24h was first reached — the 24h analogue of
	// Peak1hAt.
	Peak24hAt time.Time
}

// Store is the shared per-key send-state store. [Store.ReadEntry]
// drives the policies ([Entry.Seen], [pith/coalesce.Coalescer.ShouldDefer]);
// [Store.ReadMetrics] serves observability. Writes come from
// [pith/protect.Protector.RecordAsSent] (on successful sends) and
// [pith/protect.Protector.Check] (on Coalescer-driven deferrals).
type Store interface {
	// RecordAsSent stores (key → contentHash) stamped at now, appends
	// a timestamp to the key's LastNSendTimes, refreshes the Entry
	// TTL, increments TotalSent, and stamps LastSentAt. It touches
	// only send-side state — the deferred breadcrumb,
	// LastNDeferredTimes, and LastDeferredAt are all left intact; the
	// newer send timestamp is what makes a prior deferral no longer
	// pending. It also stamps namespace (the caller-defined token bound
	// on the protector's namespace handle; "" = the whole store) on both
	// the entry and the [Metrics] doc, so a send-only key — one never
	// deferred — still carries its namespace for per-namespace metrics
	// queries.
	RecordAsSent(ctx context.Context, key, namespace, contentHash string) error

	// RecordAsDeferred sets LastDeferredMessageRef, appends a
	// timestamp to LastNDeferredTimes, refreshes the Entry TTL,
	// increments TotalDeferred, and stamps LastDeferredAt. Does not
	// touch ContentHash, LastSentAt, or LastNSendTimes. It also stamps
	// namespace (the caller-defined sweep-scoping token bound on the
	// protector's namespace handle; "" = the whole store) on the entry —
	// so [Store.RangeDeferred] can filter to it — and on the [Metrics] doc.
	// The namespace is constant per key, so its value is stable across
	// sends and re-deferrals.
	RecordAsDeferred(ctx context.Context, key, namespace string, messageRef []byte) error

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
	// namespace scopes the sweep: when non-empty, only entries whose
	// stored namespace equals it are visited, so limit (and the
	// oldest-pending ordering) apply within that namespace rather than
	// across the whole store. Empty means visit every namespace. This is
	// what lets independent streams share one store yet be swept fairly —
	// without it, one namespace's oldest-pending backlog would consume the
	// limit budget of every sweep.
	//
	// fn does the consumer-specific work — re-derive the payload from
	// the ref, re-emit via Check, and on a successful send RecordAsSent
	// (recency then makes the key no longer pending). pith provides no
	// orchestration beyond this enumeration.
	RangeDeferred(ctx context.Context, limit int, namespace string, fn func(key string, e Entry) bool) error
}
