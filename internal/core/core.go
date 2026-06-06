// Package core is the implementation of pith's protection facade.
//
// Internal-only: external callers see this behaviour through the
// [pith/protect.ReadProtector] and [pith/protect.WriteProtector]
// interfaces and construct via the factory subpackages
// [pith/protect/memory] and [pith/protect/mongodb]. The split keeps the
// public surface minimal — there is no public way to construct a gate
// around a caller-supplied [sendstate.Store], so the supported backends
// stay the two pith ships. The Go internal-package rule makes this
// structural, not a doc convention.
//
// The architectural notes that users read — the read/write split, the
// Check/RecordAsSent contract, deferred breadcrumbs, replay — live on
// [pith/protect] (where users find them).
package core

import (
	"context"
	"fmt"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/sendstate"
)

// RequestMeta is the addressing primitive: the target slot plus the
// replay breadcrumb, carrying no content fingerprint. It is the complete
// request for a read gate ([ReadGate]); it is the addressing half of a
// write gate ([WriteGate]) call, which adds a contentHash argument; and
// it is embedded by [DeferredRequest] on the output side of a replay
// sweep, where the payload hasn't been re-derived yet.
type RequestMeta struct {
	// TargetKey identifies the per-key slot used by dedupe and every
	// attached Coalescer (typically "{activity-id}:{contact-id}" or
	// similar). The shared key across all layers is what lets them
	// collapse to a single record per target.
	TargetKey string

	// MessageRef is caller-defined data stored in the sendstate entry's
	// LastDeferredMessageRef when a Check returns DecisionDeferred (write or
	// read gate). A sweep layer reads it back to re-derive and re-emit.
	// Typically a small reference (e.g. an upstream event ID + context,
	// JSON-encoded). A write sweep re-emits the re-derived payload; a read
	// sweep re-derives the target and re-fetches current state.
	MessageRef []byte
}

// DeferredRequest is the unit yielded by [WriteGate.ReplayCandidates] /
// [ReadGate.ReplayCandidates]: a pending deferral whose attached Coalescer
// caps currently have room to re-emit. It embeds [RequestMeta] (target key
// + breadcrumb — what the consumer re-derives from and re-emits via Check)
// and adds the timestamp of the most recent deferral, so the consumer can
// reason about age without a second read.
type DeferredRequest struct {
	RequestMeta

	// DeferredAt is the timestamp of the most recent deferral on this
	// key (the tail of [sendstate.Entry.LastNDeferredTimes], via
	// [sendstate.Entry.LastDeferredTime]).
	DeferredAt time.Time
}

// Decision is the outcome of a Check call.
type Decision int

const (
	// DecisionProceed: caller should perform the gated operation, then
	// call RecordAsSent on success.
	DecisionProceed Decision = iota

	// DecisionDeduped: caller should drop the operation — the content
	// fingerprint is identical to the most recent successful send for
	// this key, so re-sending is genuinely redundant. Write gates only
	// (a read gate has no content to dedupe). Reason is "duplicate
	// content"; no breadcrumb is stamped — the duplicate is already at
	// the destination, so there is nothing to replay.
	DecisionDeduped

	// DecisionDeferred: a Coalescer cap pushed back. Reason names the
	// Coalescer. Check stamps a deferred breadcrumb
	// ([sendstate.Store.RecordAsDeferred]) so a consumer-side sweep can
	// re-emit once the cap window clears — the deferred operation is one
	// the caller still intends to perform (a write to retry, or a read to
	// re-take against current state).
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

// Outcome reports a Check result. Decision is always actionable; Err
// carries any backing-store failure encountered along the way (so a
// caller can log it without losing the policy outcome).
type Outcome struct {
	// Decision is the policy outcome the caller should act on. Always
	// meaningful, even when Err is non-nil.
	Decision Decision

	// Reason is human-readable detail for logging. Empty on a proceed;
	// "duplicate content" on a DecisionDeduped; the Coalescer's derived
	// name on a DecisionDeferred.
	Reason string

	// Err is the backing-store error encountered while making the
	// decision, or nil. A ReadEntry failure fails open (Decision =
	// DecisionProceed); a failed RecordAsDeferred stamp still yields
	// DecisionDeferred with the error attached. Callers act on Decision
	// and log Err if set.
	Err error
}

// gate holds the shared protection policy for one store. The `dedupe`
// flag is the only read-vs-write difference; the typed shells [ReadGate]
// and [WriteGate] wrap a *gate and expose the right method set and Check
// signature for each. Both stamp a breadcrumb on a Coalescer cap and offer
// replay — a debounced read is re-taken against current state, not lost.
type gate struct {
	store      sendstate.Store
	coalescers []coalesce.Coalescer

	// dedupe runs the Layer-1 content check (Seen) when true. Write
	// gates set it; read gates don't (no payload to fingerprint).
	dedupe bool
}

// ReadGate is the concrete read-side gate: coalescers only, no dedupe. A
// Coalescer cap DEFERS (breadcrumb + replay), like the write gate — so a
// debounced read is re-taken against current state by a consumer sweep, not
// lost. It satisfies [pith/protect.ReadProtector]. Construct via the factory
// subpackages.
type ReadGate struct{ *gate }

// Check gates a candidate read. Returns DecisionProceed or, on a Coalescer
// cap, DecisionDeferred (a breadcrumb is stamped for the replay sweep). Never
// DecisionDeduped (no dedupe layer).
func (r ReadGate) Check(ctx context.Context, meta RequestMeta) Outcome {
	return r.check(ctx, meta, "")
}

// RecordAsSent commits a performed read, advancing the Coalescer counts.
func (r ReadGate) RecordAsSent(ctx context.Context, meta RequestMeta) error {
	return r.record(ctx, meta, "")
}

// ReplayCandidates collects pending deferred reads whose Coalescer caps now
// have room. See [gate.replay] for the full contract.
func (r ReadGate) ReplayCandidates(ctx context.Context, limit int) ([]DeferredRequest, error) {
	return r.replay(ctx, limit)
}

// WriteGate is the concrete write-side gate: content dedupe + coalescers,
// Coalescer caps DEFER (breadcrumb + replay). It satisfies
// [pith/protect.WriteProtector]. Construct via the factory subpackages.
type WriteGate struct{ *gate }

// Check gates a candidate write. Returns DecisionProceed, DecisionDeduped
// (identical content), or DecisionDeferred (a Coalescer cap fired — a
// breadcrumb is stamped for the replay sweep).
func (w WriteGate) Check(ctx context.Context, meta RequestMeta, contentHash string) Outcome {
	return w.check(ctx, meta, contentHash)
}

// RecordAsSent commits a successful write (TargetKey → contentHash).
func (w WriteGate) RecordAsSent(ctx context.Context, meta RequestMeta, contentHash string) error {
	return w.record(ctx, meta, contentHash)
}

// ReplayCandidates collects pending deferrals whose Coalescer caps now
// have room. See [gate.replay] for the full contract.
func (w WriteGate) ReplayCandidates(ctx context.Context, limit int) ([]DeferredRequest, error) {
	return w.replay(ctx, limit)
}

// NewRead builds a read gate over store with the given Coalescers (at
// least one is required; see [newGate]). Internal — called by the factory
// subpackages.
func NewRead(store sendstate.Store, coalescers ...coalesce.Coalescer) ReadGate {
	return ReadGate{newGate(store, false, coalescers)}
}

// NewWrite builds a write gate over store with the given Coalescers (at
// least one is required; see [newGate]). Internal — called by the factory
// subpackages.
func NewWrite(store sendstate.Store, coalescers ...coalesce.Coalescer) WriteGate {
	return WriteGate{newGate(store, true, coalescers)}
}

// LargestHardCap returns the largest hardCap among coalescers, or 0 when
// none are given. The mongo factory uses it to derive MaxSendTimes
// without first constructing a gate.
func LargestHardCap(coalescers ...coalesce.Coalescer) int {
	largest := 0
	for _, c := range coalescers {
		if _, hardCap, _ := c.CapPolicy(); hardCap > largest {
			largest = hardCap
		}
	}
	return largest
}

// newGate validates the Coalescer set and store, sizes a self-sizing
// store, and returns the gate. store must be non-nil and at least one
// Coalescer must be attached. The largest attached cap (hardCap, window)
// drives two store invariants:
//
//   - a store that reports its TTL must have EntryTTL >= the largest
//     Coalescer window, else expiry would drop in-window history and leak
//     the cap (panic on violation);
//   - a self-sizing store (one exposing GrowMaxSendTimes) is grown to the
//     largest hardCap so the send-timestamp list can't undercount.
//
// Coalescer names (from [coalesce.Coalescer.CapPolicy]) must be unique —
// Check surfaces the name in Outcome.Reason, so two caps sharing a name
// would be ambiguous (panic on collision).
func newGate(store sendstate.Store, dedupe bool, coalescers []coalesce.Coalescer) *gate {
	if store == nil {
		panic("protect: a gate requires a non-nil store")
	}
	if len(coalescers) == 0 {
		panic("protect: a gate requires at least one Coalescer")
	}

	maxCap := 0
	var maxWindow time.Duration
	seen := make(map[string]struct{}, len(coalescers))
	for _, c := range coalescers {
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

	if t, ok := store.(interface{ EntryTTL() time.Duration }); ok {
		if ttl := t.EntryTTL(); ttl < maxWindow {
			panic(fmt.Sprintf("protect: store EntryTTL %s is shorter than the largest Coalescer window %s", ttl, maxWindow))
		}
	}
	if sz, ok := store.(interface{ GrowMaxSendTimes(int) }); ok {
		sz.GrowMaxSendTimes(maxCap)
	}

	return &gate{store: store, coalescers: coalescers, dedupe: dedupe}
}

// check applies dedupe (write gates only) and each attached Coalescer in
// order and returns the first suppression. One [sendstate.Store.ReadEntry]
// read drives every policy — dedupe and each Coalescer evaluate against
// the same [sendstate.Entry], anchored at a single now. Coalescer counts
// advance only when record appends to the store on a successful send.
//
//   - DecisionProceed: perform the operation, then call record on success.
//     No store write on this path.
//   - DecisionDeduped: dedupe matched the last send's contentHash (write
//     gates only). No store write — nothing to replay.
//   - DecisionDeferred: a Coalescer cap fired. Check stamps a deferred
//     breadcrumb for the consumer sweep and returns DecisionDeferred — read
//     and write gates alike.
//
// Backing-store errors are fail-open via [Outcome.Err]: a ReadEntry
// failure yields (DecisionProceed, Err); a failed RecordAsDeferred stamp
// still yields DecisionDeferred with the error attached.
func (g *gate) check(ctx context.Context, meta RequestMeta, contentHash string) Outcome {
	now := time.Now()
	entry, err := g.store.ReadEntry(ctx, meta.TargetKey)
	if err != nil {
		return Outcome{Decision: DecisionProceed, Err: err}
	}

	// Layer 1: content dedupe (write gates only).
	if g.dedupe && entry.Seen(contentHash) {
		return Outcome{Decision: DecisionDeduped, Reason: "duplicate content"}
	}

	// Layer 2: each attached Coalescer in order.
	for _, c := range g.coalescers {
		if c.ShouldDefer(entry, now) {
			capName, _, _ := c.CapPolicy()
			recErr := g.store.RecordAsDeferred(ctx, meta.TargetKey, meta.MessageRef)
			return Outcome{Decision: DecisionDeferred, Reason: capName, Err: recErr}
		}
	}

	return Outcome{Decision: DecisionProceed}
}

// record commits a successful send: writes (TargetKey → contentHash) to
// the store, appending a timestamp to the rolling send list and
// incrementing TotalSent. Read gates pass an empty contentHash.
func (g *gate) record(ctx context.Context, meta RequestMeta, contentHash string) error {
	return g.store.RecordAsSent(ctx, meta.TargetKey, contentHash)
}

// replay collects pending deferrals ready to re-emit — those whose
// attached Coalescer caps currently have room — as [DeferredRequest]
// (the embedded [RequestMeta] carries the target key + breadcrumb;
// DeferredAt carries the most recent deferral time). Entries still inside
// a cap window are skipped: a re-emit would just defer again and waste
// the (typically expensive) re-derivation.
//
// It examines the oldest limit pending entries from the store (oldest
// deferral first — see [sendstate.Store.RangeDeferred]). limit bounds the
// entries examined, not the slice returned; limit <= 0 means no bound.
//
// The gate covers only the caps (pure CountSentInWindow /
// CountDeferredInWindow arithmetic, no I/O). Dedupe is not applied here —
// on a write gate it still runs when the consumer re-emits each candidate
// via [WriteGate.Check]; a read gate ([ReadGate.Check]) has no dedupe and
// re-takes the read against current state.
func (g *gate) replay(ctx context.Context, limit int) ([]DeferredRequest, error) {
	var out []DeferredRequest
	err := g.store.RangeDeferred(ctx, limit, func(key string, e sendstate.Entry) bool {
		if g.capsClear(e, time.Now()) {
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
// room for the entry (none would defer at now). Pure; no I/O. Does not
// consider dedupe.
func (g *gate) capsClear(e sendstate.Entry, now time.Time) bool {
	for _, c := range g.coalescers {
		if c.ShouldDefer(e, now) {
			return false
		}
	}
	return true
}
