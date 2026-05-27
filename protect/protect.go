// Package protect composes pith's integration-guard policies —
// content dedupe and zero or more [coalesce.Coalescer] cap policies —
// behind a single facade so callers can apply them all with one
// Check call per gated operation.
//
// # The mechanism set
//
// Pith applies two kinds of policy, both read from a single
// [sendstate.Entry]:
//
//   - Dedupe — "is this the same ContentHash as the last successful
//     send for this key?" Always applied; a no-op re-send is always
//     redundant. It's the [sendstate.Entry.Seen] primitive — no
//     configuration, no window.
//
//   - Coalesce — "is this key at or above some hardCap within some
//     trailing window?" Zero or more Coalescers can be attached,
//     each with its own (hardCap, window). Examples:
//
//     [WithLeadingEdgeDebounce] 10s → hardCap=1, window=10s (leading-edge throttle)
//     [WithCap]      50, 24h     → hardCap=50, window=24h  (destination quota)
//     [WithCoalescer] 5, 1m, "burst" → hardCap=5,  window=1m   (custom burst cap)
//
// At Check time the Protector reads one [sendstate.Entry] from the
// store and evaluates every policy against it (anchored at a single
// now): dedupe first, then each Coalescer in attached order, returning
// the first DecisionDeferred. Outcome.Reason distinguishes which
// mechanism produced it.
//
// # Why the policies share a shape
//
// Every policy is a pure function over a [sendstate.Entry]
// ([sendstate.Entry.Seen], [coalesce.Coalescer.ShouldDefer]). The
// store owns send-history data; one read per Check feeds all policies,
// so backends that pay per round-trip (e.g. Mongo) incur a single
// document fetch regardless of how many Coalescers are attached.
// RecordAsSent + RecordAsDeferred are the only writes, and they
// live on the store, not on the policies.
//
// # Check / RecordAsSent contract
//
// Happy-path:
//
//	p := protect.New(
//	    protect.WithCap(50, 24*time.Hour),     // quota cap
//	    protect.WithLeadingEdgeDebounce(10*time.Second), // leading-edge throttle
//	)
//
//	out, err := p.Check(ctx, protect.Request{
//	    ContentHash: contentHash,
//	    TargetKey:   activityID + ":" + contactID,
//	    MessageRef:  ref,
//	})
//	switch out.Decision {
//	case protect.DecisionProceed:
//	    if err := svc.Send(...); err == nil {
//	        _ = p.RecordAsSent(ctx, req)
//	    }
//	case protect.DecisionDeferred:
//	    // out.Reason ∈ {"duplicate content", "leading-edge debounce window", "at cap", ...}
//	}
//
// Backing-store errors are fail-open: a non-nil error from Check
// carries Decision == DecisionProceed so callers over-send rather
// than dropping work.
//
// # Deferred breadcrumbs and consumer-side flush
//
// On every Coalescer-driven defer (not on dedupe defers — those
// are genuinely redundant), Check calls
// [sendstate.Store.RecordAsDeferred] to store the request's MessageRef
// in LastDeferredMessageRef and append to LastNDeferredTimes. A
// consumer-side sweep can scan the store for entries with a pending
// deferral (most recent deferral newer than the most recent send),
// gate eligibility on the deferral cadence ([sendstate.Entry.CountDeferredInWindow]
// — e.g. "gone quiet"), re-derive the upstream state from MessageRef,
// and re-emit via Check. A successful RecordAsSent at the end of the
// re-emit makes the send the most recent event, so the entry is no
// longer pending — nothing is cleared; recency alone resolves it (a
// stale breadcrumb is never read once not pending).
//
// # Backends
//
// Today: the in-memory implementations from [sendstate] and
// [coalesce] are wired up by default, suitable for tests, examples,
// and single-process deployments.
//
// IMPORTANT — single-process limitation: the in-memory backends
// are process-local. A shared-backing implementation (e.g. the
// planned Mongo backend) is required for multi-instance use.
//
// Swap the store via [WithSendStore]. Custom Coalescer
// implementations are pure policies over the [sendstate.Entry]
// Check reads from the shared store, so they need no store of their
// own — only the [sendstate.Store] backs the actual send history.
package protect

import (
	"context"
	"fmt"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/sendstate"
)

// namedCoalescer pairs a [coalesce.Coalescer] with the Reason
// string reported when it triggers a defer. cap and window are
// retained from the configuring [WithCap] / [WithLeadingEdgeDebounce] /
// [WithCoalescer] option so [New] can lazily construct the default
// Coalescer once the shared [sendstate.Store] is wired up; they
// are unused (zero) when [WithCoalescerImpl] supplies a pre-built
// Coalescer.
type namedCoalescer struct {
	reason string
	cap    int
	window time.Duration
	coalesce.Coalescer
}

// Protector composes pith's integration-guard mechanisms behind a
// single facade. Construct with [New].
type Protector struct {
	store      sendstate.Store
	coalescers []namedCoalescer
}

// Request is the input to [Protector.Check] and [Protector.RecordAsSent].
type Request struct {
	// TargetKey identifies the per-key slot used by dedupe and
	// every attached Coalescer (typically "{activity-id}:{contact-id}"
	// or similar). The shared key across all layers is what lets
	// them collapse to a single record per target.
	TargetKey string

	// ContentHash is a stable fingerprint of the message being
	// sent (typically a truncated cryptographic hash). It's
	// compared against the last successful send for TargetKey
	// (via [sendstate.Entry.Seen]) to skip re-sending identical
	// content.
	ContentHash string

	// MessageRef is caller-defined data stored in the sendstate
	// entry's LastDeferredMessageRef when Check returns
	// DecisionDeferred on a Coalescer branch. A sweep layer reads
	// it back to re-derive and re-emit. Typically a small
	// reference (e.g. an upstream event ID + context,
	// JSON-encoded). Ignored on the dedupe defer path (no
	// breadcrumb — the duplicate is genuinely redundant).
	MessageRef []byte
}

// Decision is the outcome of a [Protector.Check] call.
type Decision int

const (
	// DecisionProceed: caller should perform the gated operation,
	// then call [Protector.RecordAsSent] on success.
	DecisionProceed Decision = iota

	// DecisionDeferred: caller should drop the operation.
	// Outcome.Reason names which mechanism caused the defer:
	//
	//   "duplicate content" — dedupe matched.
	//   "leading-edge debounce window" — debounce Coalescer at cap (hardCap=1).
	//   "at cap"            — quota Coalescer at cap.
	//   (or a custom reason from a caller-attached Coalescer)
	//
	// On every Coalescer-driven defer, [Protector.Check] calls
	// [sendstate.Store.RecordAsDeferred] so a sweep layer can
	// later re-emit the cluster's final state.
	DecisionDeferred
)

// String returns the decision name (for logging).
func (d Decision) String() string {
	switch d {
	case DecisionProceed:
		return "Proceed"
	case DecisionDeferred:
		return "Deferred"
	default:
		return "Unknown"
	}
}

// Outcome reports the [Protector.Check] result.
type Outcome struct {
	Decision Decision

	// Reason is human-readable detail for logging.
	Reason string
}

// Option configures a [Protector].
type Option func(*config)

type config struct {
	store      sendstate.Store
	coalescers []namedCoalescer
}

// WithCap attaches a quota Coalescer with the given (hardCap,
// window). The hard cap is the per-target send count within the
// trailing window that triggers DecisionDeferred with Reason="at
// cap".
func WithCap(hardCap int, window time.Duration) Option {
	return func(c *config) {
		c.coalescers = append(c.coalescers, namedCoalescer{
			reason: "at cap",
			cap:    hardCap,
			window: window,
		})
	}
}

// WithLeadingEdgeDebounce attaches a cluster-collapse Coalescer
// (hardCap=1) over the given window. Returns DecisionDeferred with
// Reason="leading-edge debounce window" when triggered. The name is
// explicit about the strategy — a trailing-edge variant could be added
// alongside, with its own Reason.
//
// It is a leading-edge throttle: send the first event, then enforce a
// minimum spacing of window between sends. The window is anchored on
// the last successful send (deferred attempts don't reset it), so:
//
//	t=0   send (count in last 10s = 0)        → PROCEED, record t=0
//	t=5   another event (count = 1)           → DEFER "leading-edge debounce window"
//	t=8   another event (count = 1)           → DEFER
//	t=12  another event (t=0 aged out, count=0)→ PROCEED, record t=12
//
// A continuous stream therefore sends once per window, steadily — it
// does NOT wait for a quiet gap (that's a trailing-edge debounce, which
// pith does not do directly). To also emit the burst's *final* state,
// pair it with the deferred-breadcrumb sweep (see the package
// "Deferred breadcrumbs" note): the leading edge sends immediately, and
// a sweep flushes the last deferred state once the window settles.
//
// The window is source-driven (how long an inbound cluster of related
// events lasts — typically seconds), independent of [WithCap]'s
// destination-driven quota window.
func WithLeadingEdgeDebounce(window time.Duration) Option {
	return func(c *config) {
		c.coalescers = append(c.coalescers, namedCoalescer{
			reason: "leading-edge debounce window",
			cap:    1,
			window: window,
		})
	}
}

// WithCoalescer attaches a custom Coalescer cap with the supplied
// (hardCap, window) and a caller-chosen reason string used in
// Outcome.Reason when it triggers a defer. Useful for layered cap
// policies beyond debounce + quota (e.g. a burst cap of 5 per
// minute alongside a daily cap of 50).
func WithCoalescer(hardCap int, window time.Duration, reason string) Option {
	return func(c *config) {
		c.coalescers = append(c.coalescers, namedCoalescer{
			reason: reason,
			cap:    hardCap,
			window: window,
		})
	}
}

// WithCoalescerImpl attaches a pre-built [coalesce.Coalescer] with
// the supplied reason string. Useful when a caller wants to wrap
// (e.g. for tracing / metrics) a Coalescer. The Coalescer evaluates
// the snapshot Check reads from the shared store, so the impl needs
// no store of its own; it is responsible for its own (hardCap,
// window), which it must report via [coalesce.Coalescer.CapPolicy]
// so [New] can size store capacity. The reason string is what
// surfaces in Outcome.Reason on a defer.
func WithCoalescerImpl(c coalesce.Coalescer, reason string) Option {
	return func(cfg *config) {
		cfg.coalescers = append(cfg.coalescers, namedCoalescer{
			reason:    reason,
			Coalescer: c,
		})
	}
}

// WithSendStore swaps the default in-memory [sendstate.MemoryStore]
// for a custom backend.
func WithSendStore(s sendstate.Store) Option {
	return func(c *config) { c.store = s }
}

// New returns a Protector with the requested Coalescers attached.
// Content dedupe ([sendstate.Entry.Seen]) is always applied. A
// Protector with no Coalescer-attaching option still dedupes, but
// applies no cap.
//
// A store is required (via [WithSendStore]); New panics without one.
// For any store that reports its TTL (e.g. [sendstate.MemoryStore]),
// New panics unless that TTL is >= the largest attached Coalescer
// window — a shorter TTL would expire in-window send history and leak
// a cap. New also sizes [sendstate.MemoryStore.MaxSendTimes] to the
// largest attached cap (via [coalesce.Coalescer.CapPolicy]) so the
// send-timestamp list always holds enough history; it only grows the
// value, never shrinks one the caller set.
func New(opts ...Option) *Protector {
	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}

	if cfg.store == nil {
		panic("protect: New requires a store (use WithSendStore)")
	}
	// Wire each attached cap with the shared store.
	for i := range cfg.coalescers {
		if cfg.coalescers[i].Coalescer == nil {
			cfg.coalescers[i].Coalescer = coalesce.NewCoalescer(
				cfg.coalescers[i].cap, cfg.coalescers[i].window,
			)
		}
	}

	// The largest attached cap (hardCap, window) drives two store
	// invariants. CapPolicy covers every Coalescer, including pre-built
	// ones from [WithCoalescerImpl] whose values are otherwise opaque.
	maxCap := 0
	var maxWindow time.Duration
	for _, c := range cfg.coalescers {
		hardCap, window := c.CapPolicy()
		if hardCap > maxCap {
			maxCap = hardCap
		}
		if window > maxWindow {
			maxWindow = window
		}
	}

	// The store's TTL must cover the widest window, else expiry would
	// drop in-window history and leak the cap. Validate any store that
	// reports its TTL; custom backends without one self-certify.
	if t, ok := cfg.store.(interface{ EntryTTL() time.Duration }); ok {
		if ttl := t.EntryTTL(); ttl < maxWindow {
			panic(fmt.Sprintf("protect: store EntryTTL %s is shorter than the largest Coalescer window %s", ttl, maxWindow))
		}
	}

	// Size the in-memory list to the largest cap so
	// [sendstate.Entry.CountInWindow] can't undercount. Grow-only: a
	// larger value the caller set is preserved. Non-MemoryStore
	// backends manage their own bounding.
	if ms, ok := cfg.store.(*sendstate.MemoryStore); ok {
		if maxCap > ms.MaxSendTimes {
			ms.MaxSendTimes = maxCap
		}
	}

	return &Protector{
		store:      cfg.store,
		coalescers: cfg.coalescers,
	}
}

// SendStore returns the [sendstate.Store] the Protector writes to.
func (p *Protector) SendStore() sendstate.Store { return p.store }

// Check applies content dedupe and each attached Coalescer in order
// and returns the first DecisionDeferred. One [sendstate.Store.ReadEntry]
// read drives every policy — dedupe and each Coalescer evaluate
// against the same [sendstate.Entry], anchored at a single now, so
// backends that pay per round-trip (e.g. Mongo) incur a single fetch
// per Check. Coalescer counts advance only when [Protector.RecordAsSent]
// appends to the store on a successful send.
//
// On every Coalescer-driven defer, Check calls
// [sendstate.Store.RecordAsDeferred] so the deferred breadcrumb is
// available to a consumer-side sweep.
//
// Backing-store errors are fail-open: the returned err is non-nil
// but Decision is DecisionProceed. A failed RecordAsDeferred stamp
// still returns DecisionDeferred so the caller sees the policy
// outcome; the error is attached for logging.
func (p *Protector) Check(ctx context.Context, req Request) (Outcome, error) {
	now := time.Now()
	entry, err := p.store.ReadEntry(ctx, req.TargetKey)
	if err != nil {
		return Outcome{Decision: DecisionProceed}, err
	}

	// Layer 1: dedupe — identical content to the last send is
	// genuinely redundant; no breadcrumb stamped.
	if entry.Seen(req.ContentHash) {
		return Outcome{Decision: DecisionDeferred, Reason: "duplicate content"}, nil
	}

	// Layer 2: each attached Coalescer in order.
	for _, c := range p.coalescers {
		if c.ShouldDefer(entry, now) {
			recErr := p.store.RecordAsDeferred(ctx, req.TargetKey, req.MessageRef)
			return Outcome{Decision: DecisionDeferred, Reason: c.reason}, recErr
		}
	}

	// Proceeding: a send is expected to follow, so each cap's in-window
	// count will reach (what we just observed) + 1. Raise the per-cap
	// high-water marks to that without a re-read — the same read that
	// cleared every cap tells us the post-send count. Best-effort
	// observability: a failed raise never changes the decision.
	p.raisePeaks(ctx, req.TargetKey, entry, now)

	return Outcome{Decision: DecisionProceed}, nil
}

// raisePeaks bumps each attached cap's PeakSendsInWindow to the count
// it will reach once the in-flight send lands (observed + 1). The mark
// is optimistic — if the caller proceeds but the send fails (no
// RecordAsSent), it reads 1 high; acceptable for an advisory gauge.
func (p *Protector) raisePeaks(ctx context.Context, key string, entry sendstate.Entry, now time.Time) {
	if len(p.coalescers) == 0 {
		return
	}
	peaks := make(map[string]uint64, len(p.coalescers))
	for _, c := range p.coalescers {
		_, window := c.CapPolicy()
		if n := uint64(entry.CountInWindow(now, window)) + 1; n > peaks[c.reason] {
			peaks[c.reason] = n
		}
	}
	_ = p.store.RaisePeaks(ctx, key, peaks)
}

// RecordAsSent commits a successful send: writes (TargetKey →
// ContentHash) to [sendstate.Store], appending a timestamp to the
// internal rolling list and incrementing TotalSent. Subsequent
// dedupe and Coalescer reads consult that record on the next Check.
func (p *Protector) RecordAsSent(ctx context.Context, req Request) error {
	return p.store.RecordAsSent(ctx, req.TargetKey, req.ContentHash)
}

// ReadEntry returns the per-key working [sendstate.Entry] (zero when
// absent or TTL-expired). Thin pass-through to the underlying store;
// useful for consumer-side sweeps that need to read deferred
// breadcrumbs.
func (p *Protector) ReadEntry(ctx context.Context, key string) (sendstate.Entry, error) {
	return p.store.ReadEntry(ctx, key)
}

// RangeDeferredWithCapsClear drives a replay sweep over pending
// deferrals, but invokes fn only for entries whose attached Coalescer
// caps currently have room — skipping those still inside a cap window,
// where a re-emit would just defer again and waste the (typically
// expensive) re-derivation in fn.
//
// It examines the oldest limit pending entries from the store (oldest
// deferral first — see [sendstate.Store.RangeDeferred]); because oldest
// deferral correlates with windows having elapsed, the eligible entries
// sort to the front and the budget lands on them. fn is the consumer's
// re-derive + [Protector.Check] + [Protector.RecordAsSent]; it returns
// false to stop early.
//
// The gate covers only the caps (pure CountInWindow arithmetic, no
// I/O). Dedupe still applies in the full Check fn runs — if the
// re-derived content equals the last send, that Check defers and the
// breadcrumb is left pending (the rare revert case).
func (p *Protector) RangeDeferredWithCapsClear(ctx context.Context, limit int, fn func(key string, e sendstate.Entry) bool) error {
	return p.store.RangeDeferred(ctx, limit, func(key string, e sendstate.Entry) bool {
		if !p.capsClear(e, time.Now()) {
			return true // pending but a cap window hasn't elapsed — skip
		}
		return fn(key, e)
	})
}

// capsClear reports whether every attached Coalescer cap currently has
// room for the entry (none would defer at now) — i.e. the cap layers
// would let a re-emit through. Pure; no I/O. Does not consider dedupe.
func (p *Protector) capsClear(e sendstate.Entry, now time.Time) bool {
	for _, c := range p.coalescers {
		if c.ShouldDefer(e, now) {
			return false
		}
	}
	return true
}

// Metrics returns the per-key lifetime [sendstate.Metrics]. ok=false
// when the key has never been recorded. Thin pass-through to the
// underlying store for observability dashboards.
func (p *Protector) Metrics(ctx context.Context, key string) (sendstate.Metrics, bool, error) {
	return p.store.ReadMetrics(ctx, key)
}
