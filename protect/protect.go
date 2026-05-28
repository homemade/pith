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
//     trailing window?" Zero or more Coalescers can be attached via
//     [WithCoalescer], each with its own (hardCap, window) and a
//     name it derives from those bounds (surfaced as Outcome.Reason
//     on defer). Examples:
//
//     WithCoalescer([coalesce.NewLeadingEdgeDebounce] 10s)  → leading-edge throttle
//     WithCoalescer([coalesce.NewQuota] 50, 24h)            → destination quota
//     WithCoalescer([coalesce.NewQuota] 5, 1m)              → burst quota
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
//	p := protect.New(store,
//	    protect.WithCoalescer(coalesce.NewQuota(50, 24*time.Hour)),         // quota cap
//	    protect.WithCoalescer(coalesce.NewLeadingEdgeDebounce(10*time.Second)), // leading-edge throttle
//	)
//
//	out := p.Check(ctx, protect.Request{
//	    RequestMeta: protect.RequestMeta{
//	        TargetKey:  activityID + ":" + contactID,
//	        MessageRef: ref,
//	    },
//	    ContentHash: contentHash,
//	})
//	if out.Err != nil {
//	    log.Printf("pith.Check: %v", out.Err) // fail-open; Decision is still actionable
//	}
//	switch out.Decision {
//	case protect.DecisionProceed:
//	    if err := svc.Send(...); err == nil {
//	        _ = p.RecordAsSent(ctx, req)
//	    }
//	case protect.DecisionDeduped:
//	    // out.Reason is "duplicate content" — content identical to
//	    // the most recent successful send; drop and move on.
//	case protect.DecisionDeferred:
//	    // out.Reason is the Coalescer's derived name, e.g.
//	    // "quota cap 50 per 24h" or "leading-edge debounce 10s";
//	    // a breadcrumb is stamped for the consumer-side sweep.
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
// Pass the [sendstate.Store] backing the Protector as the first
// argument to [New] — the in-process [pith/sendstate/memory.Store]
// for tests/examples/single-process use, or a shared-backing
// implementation (e.g. [pith/sendstate/mongodb.Store]) for
// cross-instance coordination. Custom Coalescer implementations are
// pure policies over the [sendstate.Entry] Check reads from the
// shared store, so they need no store of their own — only the
// [sendstate.Store] backs the actual send history.
package protect

import (
	"context"
	"fmt"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/sendstate"
)

// Protector composes pith's integration-guard mechanisms behind a
// single facade. Construct with [New].
//
// Each attached cap is a [coalesce.Coalescer] carrying its own name
// (reported via [coalesce.Coalescer.CapPolicy]); that name is what
// Check surfaces in Outcome.Reason when the cap defers, so the
// Protector keeps no parallel naming of its own.
//
// # Facade discipline
//
// Protector exposes only the operations that change protection
// behaviour:
//
//   - [Protector.Check] — gate a candidate send.
//   - [Protector.RecordAsSent] — confirm a successful send.
//   - [Protector.ReplayCandidates] — ask for replay candidates
//     whose Coalescer caps now have room.
//
// Read-only access to per-key state — the working [sendstate.Entry]
// and the lifetime [sendstate.Metrics] — is deliberately NOT on this
// surface. Observability dashboards, admin endpoints, and tests that
// need to inspect what's in the store go through the
// [sendstate.Store] directly. Two distinct surfaces for two distinct
// concerns: protection logic on the Protector, state storage on the
// Store. Keeping reads off the Protector also keeps callers from
// reaching past the discipline (e.g. writing to the store outside
// the record-on-success contract).
type Protector struct {
	store      sendstate.Store
	coalescers []coalesce.Coalescer
}

// RequestMeta is the addressing half of a [Request]: the target slot
// plus the replay breadcrumb, carrying no content fingerprint. It's
// embedded by [Request] on the input side (with ContentHash for Check
// and RecordAsSent) and by [DeferredRequest] on the output side of a
// replay sweep (see [Protector.ReplayCandidates]) — at sweep
// time the payload hasn't been re-derived, so no ContentHash exists
// yet. A consumer re-derives the payload from MessageRef, hashes it,
// and embeds the RequestMeta in a full [Request] to re-emit via
// [Protector.Check].
type RequestMeta struct {
	// TargetKey identifies the per-key slot used by dedupe and
	// every attached Coalescer (typically "{activity-id}:{contact-id}"
	// or similar). The shared key across all layers is what lets
	// them collapse to a single record per target.
	TargetKey string

	// MessageRef is caller-defined data stored in the sendstate
	// entry's LastDeferredMessageRef when Check returns
	// DecisionDeferred on a Coalescer branch. A sweep layer reads
	// it back to re-derive and re-emit. Typically a small
	// reference (e.g. an upstream event ID + context,
	// JSON-encoded). Ignored on the dedupe defer path (no
	// breadcrumb — the duplicate is genuinely redundant).
	MessageRef []byte
}

// Request is the input to [Protector.Check] and [Protector.RecordAsSent].
type Request struct {
	RequestMeta

	// ContentHash is a stable fingerprint of the message being
	// sent (typically a truncated cryptographic hash). It's
	// compared against the last successful send for TargetKey
	// (via [sendstate.Entry.Seen]) to skip re-sending identical
	// content.
	ContentHash string
}

// DeferredRequest is the unit yielded by [Protector.ReplayCandidates]:
// a pending deferral whose attached Coalescer caps currently have room to
// re-emit. It embeds [RequestMeta] (target key + breadcrumb — what the
// consumer re-derives from and re-emits via [Protector.Check]) and adds
// the timestamp of the most recent deferral, so the consumer can reason
// about age without a second read.
type DeferredRequest struct {
	RequestMeta

	// DeferredAt is the timestamp of the most recent deferral on this
	// key (the tail of [sendstate.Entry.LastNDeferredTimes], via
	// [sendstate.Entry.LastDeferredTime]). Useful for prioritising or
	// observability — e.g. logging how long a breadcrumb has waited
	// before replay.
	DeferredAt time.Time
}

// Decision is the outcome of a [Protector.Check] call.
type Decision int

const (
	// DecisionProceed: caller should perform the gated operation,
	// then call [Protector.RecordAsSent] on success.
	DecisionProceed Decision = iota

	// DecisionDeduped: caller should drop the operation — the
	// content fingerprint is identical to the most recent
	// successful send for this key, so re-sending is genuinely
	// redundant. Outcome.Reason is the fixed string "duplicate
	// content". No breadcrumb is stamped on the store: there's
	// nothing to replay (the duplicate is already at the
	// destination), so this defer is silent on the consumer-side
	// sweep — by design, separate from DecisionDeferred so
	// observability can distinguish the two suppression modes.
	DecisionDeduped

	// DecisionDeferred: caller should drop the operation — an
	// attached Coalescer cap pushed back. Outcome.Reason names
	// which Coalescer caused the defer (its derived name from
	// [coalesce.Coalescer.CapPolicy]), e.g. "leading-edge debounce
	// 10s" or "quota cap 50 per 24h". On every such defer,
	// [Protector.Check] calls [sendstate.Store.RecordAsDeferred]
	// so a consumer-side sweep can later re-emit once the cap
	// window clears.
	DecisionDeferred
)

// String returns the decision name (for logging).
func (d Decision) String() string {
	switch d {
	case DecisionProceed:
		return "Proceed"
	case DecisionDeduped:
		return "Deduped"
	case DecisionDeferred:
		return "Deferred"
	default:
		return "Unknown"
	}
}

// Outcome reports the [Protector.Check] result. It's the single value
// Check returns — Decision is always actionable, and Err carries any
// backing-store failure that happened along the way (so a caller can
// log it without losing the policy outcome).
type Outcome struct {
	// Decision is the policy outcome the caller should act on:
	// DecisionProceed (perform the operation), DecisionDeduped
	// (drop — identical content to the last send), or
	// DecisionDeferred (drop — a Coalescer cap fired). Always
	// meaningful, even when Err is non-nil.
	Decision Decision

	// Reason is human-readable detail for logging. Empty on a
	// proceed; "duplicate content" on a DecisionDeduped; the
	// Coalescer's derived name on a DecisionDeferred.
	Reason string

	// Err is the backing-store error encountered while making the
	// decision, or nil. Two cases produce a non-nil Err:
	//
	//   - ReadEntry failed: Check fails open with Decision =
	//     DecisionProceed so the caller doesn't drop work.
	//   - A Coalescer fired and RecordAsDeferred (the breadcrumb
	//     write) failed: Decision = DecisionDeferred so the caller
	//     still sees the policy outcome; Err is attached for
	//     logging. Recovery is best-effort — the next event
	//     re-stamps.
	//
	// Callers typically: act on Decision, and log Err if non-nil.
	Err error
}

// Option configures a [Protector].
type Option func(*config)

type config struct {
	coalescers []coalesce.Coalescer
}

// WithCoalescer attaches a cap [coalesce.Coalescer] — build one with
// [coalesce.NewLeadingEdgeDebounce] (cluster collapse) or
// [coalesce.NewQuota] (destination quota), or supply a custom impl
// (e.g. wrapped for tracing / metrics).
//
// The Coalescer evaluates the snapshot Check reads from the shared
// store, so it needs no store of its own; it is responsible for its
// own name and (hardCap, window), which it reports via
// [coalesce.Coalescer.CapPolicy]. [New] uses that to size store
// capacity, and Check surfaces the name in Outcome.Reason on a defer
// (e.g. "quota cap 50 per 24h", "leading-edge debounce 10s").
//
// Caps with distinct bounds derive distinct names, so layered caps
// (e.g. a burst quota of 5 per minute alongside a daily quota of 100
// per 24h) can be attached together; [New] panics if two attached
// caps derive the same name.
func WithCoalescer(c coalesce.Coalescer) Option {
	return func(cfg *config) {
		cfg.coalescers = append(cfg.coalescers, c)
	}
}

// New returns a Protector backed by store and with the requested
// Coalescers attached. Content dedupe ([sendstate.Entry.Seen]) is
// always applied; a Protector with no Coalescer-attaching option
// still dedupes, but applies no cap.
//
// store is required and must be non-nil — pass the in-process
// [pith/sendstate/memory.Store] for single-process use, or a
// shared-backing store (e.g. [pith/sendstate/mongodb.Store]) for
// cross-instance coordination. For any store that reports its TTL
// (e.g. [pith/sendstate/memory.Store]), New panics unless that TTL
// is >= the largest attached Coalescer window — a shorter TTL would
// expire in-window send history and leak a cap. New also sizes a
// self-sizing store's send-timestamp list to the largest attached
// cap (via [coalesce.Coalescer.CapPolicy]) so the send-timestamp
// list always holds enough history; it only grows the value, never
// shrinks one the caller set.
func New(store sendstate.Store, opts ...Option) *Protector {
	if store == nil {
		panic("protect: New requires a non-nil store")
	}
	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}

	// The largest attached cap (hardCap, window) drives two store
	// invariants. CapPolicy covers every Coalescer, including pre-built
	// ones from a custom [WithCoalescer] whose values are otherwise opaque.
	// The name is what Check reports in Outcome.Reason on a defer, so it
	// must be unique across attached caps — two caps sharing a name would
	// produce an ambiguous Reason. (When peak observability is wired back
	// in — see the TODO(peaks) on Check — the same name will key the
	// per-cap PeakSendsInWindow entries, reinforcing the uniqueness rule.)
	maxCap := 0
	var maxWindow time.Duration
	seen := make(map[string]struct{}, len(cfg.coalescers))
	for _, c := range cfg.coalescers {
		name, hardCap, window := c.CapPolicy()
		if _, dup := seen[name]; dup {
			panic(fmt.Sprintf("protect: duplicate Coalescer name %q — attached caps must be unique", name))
		}
		seen[name] = struct{}{}
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
	if t, ok := store.(interface{ EntryTTL() time.Duration }); ok {
		if ttl := t.EntryTTL(); ttl < maxWindow {
			panic(fmt.Sprintf("protect: store EntryTTL %s is shorter than the largest Coalescer window %s", ttl, maxWindow))
		}
	}

	// Size the send-timestamp list to the largest cap so
	// [sendstate.Entry.CountSentInWindow] can't undercount. Grow-only: a
	// larger value the caller set is preserved. Detected structurally so
	// protect stays backend-agnostic — only the in-memory store
	// (pith/sendstate/memory) self-sizes; other backends (e.g. pith/sendstate/mongodb)
	// manage their own bounding and don't implement this.
	if sz, ok := store.(interface{ GrowMaxSendTimes(int) }); ok {
		sz.GrowMaxSendTimes(maxCap)
	}

	return &Protector{
		store:      store,
		coalescers: cfg.coalescers,
	}
}

// Check applies content dedupe and each attached Coalescer in order
// and returns the first suppression. One [sendstate.Store.ReadEntry]
// read drives every policy — dedupe and each Coalescer evaluate
// against the same [sendstate.Entry], anchored at a single now, so
// backends that pay per round-trip (e.g. Mongo) incur a single fetch
// per Check. Coalescer counts advance only when [Protector.RecordAsSent]
// appends to the store on a successful send.
//
// Three outcomes:
//
//   - DecisionProceed: caller should perform the gated operation, then
//     call [Protector.RecordAsSent] on success. Check itself makes no
//     store write on this path.
//   - DecisionDeduped: dedupe matched the most recent send's
//     ContentHash. No store write — the duplicate is genuinely
//     redundant, so there's nothing to replay.
//   - DecisionDeferred: a Coalescer cap fired. Check calls
//     [sendstate.Store.RecordAsDeferred] to stamp the deferred
//     breadcrumb for a consumer-side sweep.
//
// Backing-store errors are fail-open and surface via [Outcome.Err]:
// a ReadEntry failure yields (DecisionProceed, Reason: "", Err: err);
// a failed RecordAsDeferred stamp still yields DecisionDeferred so
// the caller sees the policy outcome, with the error attached for
// logging. Either way, callers act on Decision and log Err if set.
func (p *Protector) Check(ctx context.Context, req Request) Outcome {
	now := time.Now()
	entry, err := p.store.ReadEntry(ctx, req.TargetKey)
	if err != nil {
		return Outcome{Decision: DecisionProceed, Err: err}
	}

	// Layer 1: dedupe — identical content to the last send is
	// genuinely redundant; no breadcrumb stamped, and the dedicated
	// DecisionDeduped outcome lets observers distinguish "duplicate
	// suppressed" from "Coalescer cap fired".
	if entry.Seen(req.ContentHash) {
		return Outcome{Decision: DecisionDeduped, Reason: "duplicate content"}
	}

	// Layer 2: each attached Coalescer in order. The Reason surfaced
	// on a defer is the Coalescer's own name.
	//
	// TODO(peaks): [sendstate.Metrics.PeakSendsInWindow] is intentionally
	// not populated in the initial release. When we revisit, the firing
	// cap's `CountSentInWindow(now, window) + 1` is the natural bump to
	// thread into RecordAsDeferred (and a per-cap bump map into
	// RecordAsSent on the proceed path), so the Store can max-merge peaks
	// atomically with the rest of the write.
	for _, c := range p.coalescers {
		if c.ShouldDefer(entry, now) {
			capName, _, _ := c.CapPolicy()
			recErr := p.store.RecordAsDeferred(ctx, req.TargetKey, req.MessageRef)
			return Outcome{Decision: DecisionDeferred, Reason: capName, Err: recErr}
		}
	}

	return Outcome{Decision: DecisionProceed}
}

// RecordAsSent commits a successful send: writes (TargetKey →
// ContentHash) to [sendstate.Store], appending a timestamp to the
// internal rolling list and incrementing TotalSent.
//
// TODO(peaks): [sendstate.Metrics.PeakSendsInWindow] is intentionally
// not populated in the initial release. When we revisit, this method's
// signature is the natural place to thread the per-cap bumps Check
// precomputed (see the matching TODO in Check), so the Store can fold
// the peak update into the metrics write.
func (p *Protector) RecordAsSent(ctx context.Context, req Request) error {
	return p.store.RecordAsSent(ctx, req.TargetKey, req.ContentHash)
}

// ReplayCandidates collects pending deferrals that are ready to
// replay — those whose attached Coalescer caps currently have room — and
// returns them as [DeferredRequest] (the embedded [RequestMeta] carries
// the target key + breadcrumb for the consumer to re-derive and re-emit;
// DeferredAt carries the timestamp of the most recent deferral). Entries
// still inside a cap window are skipped: a re-emit would just defer again
// and waste the (typically expensive) re-derivation.
//
// It examines the oldest limit pending entries from the store (oldest
// deferral first — see [sendstate.Store.RangeDeferred]). limit bounds the
// entries examined, not the slice returned, so an all-within-window
// prefix can yield an empty result even when eligible entries sit further
// back; because oldest deferral correlates with windows having elapsed,
// the eligible entries sort to the front and the budget lands on them.
// limit <= 0 means no bound.
//
// The gate covers only the caps (pure CountSentInWindow arithmetic, no I/O).
// Dedupe is not applied here — it still runs when the consumer re-emits
// each [DeferredRequest]'s [RequestMeta] via [Protector.Check]: if the
// re-derived content equals the last send, that Check defers and the
// breadcrumb is left pending (the rare revert case).
func (p *Protector) ReplayCandidates(ctx context.Context, limit int) ([]DeferredRequest, error) {
	var out []DeferredRequest
	err := p.store.RangeDeferred(ctx, limit, func(key string, e sendstate.Entry) bool {
		if p.capsClear(e, time.Now()) {
			out = append(out, DeferredRequest{
				RequestMeta: RequestMeta{TargetKey: key, MessageRef: e.LastDeferredMessageRef},
				DeferredAt:  e.LastDeferredTime(),
			})
		}
		return true
	})
	return out, err
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

