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
// # Construction
//
// Construct a Protector via one of the factory subpackages — the
// public API does not expose a way to wire in a caller-supplied
// [sendstate.Store], so the supported backends are the two pith ships:
//
//   - [pith/protect/memory.New] — in-process, single-Lambda
//     dev / test use.
//   - [pith/protect/mongodb.New] — Atlas-backed for
//     cross-instance coordination; derives MaxSendTimes from the
//     attached Coalescers' largest hardCap so the storage-side bound
//     can't be forgotten.
//
// # Check / RecordAsSent contract
//
// Happy-path (against the Mongo factory):
//
//	import (
//	    "github.com/homemade/pith/coalesce"
//	    "github.com/homemade/pith/protect"
//	    pmongo "github.com/homemade/pith/protect/mongodb"
//	)
//
//	p, client, err := pmongo.New(ctx, pmongo.Config{
//	    URI: ..., Database: ..., EntryTTL: 48*time.Hour,
//	},
//	    protect.WithCoalescer(coalesce.NewQuota(50, 24*time.Hour)),
//	    protect.WithCoalescer(coalesce.NewLeadingEdgeDebounce(10*time.Second)),
//	)
//	if err != nil { ... }
//	defer client.Disconnect(ctx)
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
// Pith ships two backends, both wired up by their own factory
// subpackage — see "Construction" above. There is no public way to
// supply a custom [sendstate.Store]; the constructor that does so
// lives in pith/internal/core and is reachable only from inside the
// pith module. If you need a different backend, fork; the supported
// surface is intentionally narrow.
package protect
