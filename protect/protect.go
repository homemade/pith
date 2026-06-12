// Package protect composes pith's integration-guard policies behind two
// capability-typed facades — [ReadProtector] and [WriteProtector] — so a
// caller applies the right policy set with one Check per gated operation.
//
// # Read vs write
//
// The two gates model the two kinds of operation pith guards:
//
//   - [WriteProtector] — a content-bearing operation (a send / merge /
//     PATCH). Applies content dedupe AND one or more coalesce caps. A
//     capped Check is DEFERRED: the write is an action you still intend
//     to perform, so a breadcrumb is stashed and ReplayCandidates yields
//     it for re-emission once the cap window clears. Its Check takes a
//     contentHash.
//
//   - [ReadProtector] — a content-free operation (a read, poll, inbound
//     event, fire-and-forget trigger). Applies coalesce caps only — there
//     is no payload to fingerprint, so no dedupe. A capped Check is
//     DEFERRED: re-taking the read against current state is still an action
//     you intend to perform, so a breadcrumb is stashed and
//     ReplayCandidates yields it for a sweep to re-read once the cap clears.
//     Its Check takes no hash.
//
// Both gates defer (not drop) a capped Check, because a cap suppresses
// whatever arrived at the wrong moment — which may be the final, changed
// state — so it must be replayed, not lost. Only dedupe is safe to drop
// without replay (the duplicate is already at the destination); that is why
// dedupe belongs to the write side alone. A write replays the stashed
// payload; a read replays the *act of reading*, re-fetching current state —
// re-reading after the burst settles captures exactly the final value.
//
// # Limitation
//
// The two gates cover the two combinations that arise in practice:
// content-bearing + dedupe (write) and content-free, no-dedupe (read) —
// both replay a capped Check. An operation that is content-bearing but must
// be re-emitted *without* dedupe (replay every cap-suppressed payload, even
// an unchanged one) fits neither cleanly. No such case exists today; if one
// arises it wants a third gate (or decoupling the dedupe decision from
// content presence). These are behavioural bundles, not an HTTP-method
// taxonomy.
//
// # The mechanism set
//
// All policies read a single [sendstate.Entry] per Check:
//
//   - Dedupe — "is this the same ContentHash as the last successful
//     send for this key?" Write gates only. It's the
//     [sendstate.Entry.Seen] primitive — no configuration, no window.
//
//   - Coalesce — "is this key at or above some hardCap within some
//     trailing window?" One or more [coalesce.Coalescer]s are attached
//     at construction, each with its own (hardCap, window) and a name it
//     derives from those bounds (surfaced as Outcome.Reason on a cap),
//     e.g. [coalesce.NewLeadingEdgeDebounce] (cluster collapse) or
//     [coalesce.NewQuota] (destination quota). At least one is required;
//     layered caps (e.g. a 5-per-minute burst alongside a 100-per-24h
//     quota) can be attached together, and the factories panic if two
//     attached caps derive the same name.
//
// At Check time the gate reads one Entry and evaluates every policy
// against it, anchored at a single now: dedupe first (write gates), then
// each Coalescer in attached order, returning the first suppression.
// Outcome.Reason distinguishes which mechanism produced it. One read per
// Check feeds all policies, so backends that pay per round-trip (e.g.
// Mongo) incur a single document fetch regardless of how many Coalescers
// are attached. RecordAsSent + RecordAsDeferred are the only writes.
//
// # Outcomes
//
// A read gate's Check returns DecisionProceed or DecisionDeferred. A write
// gate's returns DecisionProceed, DecisionDeduped, or DecisionDeferred.
// DecisionDeferred means the same thing on both gates — a cap fired, a
// breadcrumb is stashed, and the request will replay once the cap clears;
// only the write gate adds DecisionDeduped (an identical payload, dropped
// silently — nothing to replay).
//
// # Scope
//
// A replay sweep ([ReadNamespace.ReplayCandidates] /
// [WriteNamespace.ReplayCandidates]) enumerates the WHOLE store within the
// handle's namespace; TargetKey addresses Check / RecordAsSent, not the sweep.
// There are three scope tools, for three different needs:
//
//   - SEPARATE STORES (a distinct Database in the [pith/protect/mongodb]
//     factory) for streams that replay through DIFFERENT consumers. Sharing a
//     store here lets one stream's sweep drain and resolve the other's pending
//     deferrals — re-deriving and re-emitting them through the wrong consumer.
//     Never split such streams by key prefix; the collection boundary is the
//     only isolation that keeps their replay logic apart.
//
//   - NAMESPACES (pick one with Namespace(ns)) for many streams with the SAME
//     consumer that share a store but must be swept fairly. A namespace-scoped
//     sweep applies limit and the oldest-first ordering within the namespace,
//     so one namespace's backlog can't head-of-line-block another's. It
//     narrows only along the namespace axis; it is not a substitute for
//     separate stores across consumers. The "" namespace is the whole store.
//
//   - TENANTS (chain with Tenant(t).Namespace(ns)) as an optional OUTER scope
//     above the namespace. Tenant is a labelling field — stamped on every
//     Entry / Metrics write and queryable / indexable for observability or
//     per-tenant aggregation — but it does NOT scope the replay sweep or
//     isolate TargetKeys. If two tenants must use the same namespace name and
//     need distinct keys, include the tenant in TargetKey explicitly; pith
//     does not derive keys from Tenant. Tenant("") is the untenanted handle,
//     equivalent to calling Namespace directly on the root protector.
//
// # Construction
//
// Construct via a factory subpackage — the public API exposes no way to
// wire in a caller-supplied [sendstate.Store], so the supported backends
// are the two pith ships:
//
//   - [pith/protect/memory] — in-process, single-instance dev / test use.
//   - [pith/protect/mongodb] — Atlas-backed for cross-instance
//     coordination; derives MaxSendTimes from the attached Coalescers'
//     largest hardCap so the storage-side bound can't be forgotten.
//
// Each backend exposes NewReadProtector and NewWriteProtector, both
// taking at least one Coalescer (a (first, rest...) signature, so the
// requirement is compile-time).
//
// # Check / RecordAsSent contract
//
// Write happy-path (against the Mongo factory):
//
//	p, client, err := pmongo.NewWriteProtector(ctx, pmongo.Config{
//	    URI: ..., Database: ..., EntryTTL: 48*time.Hour,
//	},
//	    coalesce.NewQuota(50, 24*time.Hour),
//	    coalesce.NewLeadingEdgeDebounce(10*time.Second),
//	)
//	if err != nil { ... }
//	defer client.Disconnect(ctx)
//
//	// Scope: outer Tenant for observability, inner Namespace for the sweep.
//	// Drop Tenant if you don't need a labelled outer scope; use Namespace("")
//	// for the whole-store namespace.
//	w := p.Tenant(orgID).Namespace(campaignID)
//	meta := protect.RequestMeta{TargetKey: activityID + ":" + contactID, MessageRef: ref}
//	out := w.Check(ctx, meta, contentHash)
//	if out.Err != nil {
//	    log.Printf("pith.Check: %v", out.Err) // fail-open; Decision is still actionable
//	}
//	switch out.Decision {
//	case protect.DecisionProceed:
//	    if err := svc.Send(...); err == nil {
//	        _ = w.RecordAsSent(ctx, meta, contentHash) // record-on-success
//	    }
//	case protect.DecisionDeduped:
//	    // identical content to the last send; drop.
//	case protect.DecisionDeferred:
//	    // a Coalescer cap fired; a breadcrumb is stamped for the sweep.
//	}
//
// A read gate is the same shape without the hash and with no Deduped arm —
// a capped read defers (and a sweep replays it) rather than dropping:
//
//	r := readProtector.Tenant(orgID).Namespace(campaignID) // or just .Namespace(...)
//	out := r.Check(ctx, meta)
//	switch out.Decision {
//	case protect.DecisionProceed:
//	    if doRead() { _ = r.RecordAsSent(ctx, meta) }
//	case protect.DecisionDeferred:
//	    // a Coalescer cap fired; a breadcrumb is stamped — a sweep
//	    // (ReplayCandidates) will re-take this read once the cap clears.
//	}
//
// Backing-store errors are fail-open: a non-nil Outcome.Err carries
// Decision == DecisionProceed so callers over-send rather than dropping
// work. RecordAsSent is record-on-success — a failed operation leaves the
// slot unrecorded so a retry isn't suppressed.
//
// # Deferred breadcrumbs and the consumer-side sweep
//
// On every DecisionDeferred, Check calls [sendstate.Store.RecordAsDeferred]
// to store the request's MessageRef and append to LastNDeferredTimes. A
// consumer-side sweep scans for entries with a pending deferral (most
// recent deferral newer than the most recent send), re-derives from
// MessageRef, and re-emits via Check; [WriteNamespace.ReplayCandidates] /
// [ReadNamespace.ReplayCandidates] yield the ones whose caps now have room.
// A write sweep re-emits the stashed payload; a read sweep re-fetches
// current state. A successful RecordAsSent makes the send the most recent
// event, so the entry is no longer pending — recency alone resolves it.
//
// Both gates run this sweep — the read gate is replay-capable, the same
// machinery as the write gate minus dedupe. pith fires no timers itself:
// the replay/trailing fire happens when a consumer sweep next runs after the
// cap clears — under continuous traffic (a sweep at each request tail) within
// a request or two of going quiet; if all traffic stops, at the next request
// or a cron sweep. A caller that genuinely wants a fire-and-forget poll
// throttle can simply not run a sweep — the deferred entries then TTL out.
package protect
