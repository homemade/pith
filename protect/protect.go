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
//     DROPPED: the call is skippable, so it is simply suppressed — no
//     breadcrumb, nothing to replay. Its Check takes no hash.
//
// The names track the replay axis: a write is retryable (deferred and
// replayed), a read is skippable (dropped). The same axis is why dedupe
// belongs only to the write side — a duplicate content suppression is
// never replayable, the duplicate is already at the destination.
//
// # Limitation
//
// The two gates cover the two combinations that arise in practice:
// content-bearing + retryable (write) and content-free + skippable (read).
// An operation that is content-free but must NOT be skipped — a bodiless
// mutation you still need to retry, e.g. a DELETE or fire-once trigger —
// fits neither: a WriteProtector would dedupe on an empty hash, a
// ReadProtector would drop it. No such case exists today; if one arises it
// wants a third gate (or decoupling the replay decision from content
// presence). These are behavioural bundles, not an HTTP-method taxonomy.
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
// A read gate's Check returns DecisionProceed or DecisionDropped. A write
// gate's returns DecisionProceed, DecisionDeduped, or DecisionDeferred.
// Dropped and Deferred are distinct constants because their downstream
// meaning differs — a dropped read is gone; a deferred write is stashed
// and will replay.
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
//	meta := protect.RequestMeta{TargetKey: activityID + ":" + contactID, MessageRef: ref}
//	out := p.Check(ctx, meta, contentHash)
//	if out.Err != nil {
//	    log.Printf("pith.Check: %v", out.Err) // fail-open; Decision is still actionable
//	}
//	switch out.Decision {
//	case protect.DecisionProceed:
//	    if err := svc.Send(...); err == nil {
//	        _ = p.RecordAsSent(ctx, meta, contentHash) // record-on-success
//	    }
//	case protect.DecisionDeduped:
//	    // identical content to the last send; drop.
//	case protect.DecisionDeferred:
//	    // a Coalescer cap fired; a breadcrumb is stamped for the sweep.
//	}
//
// A read gate is the same shape without the hash and with no Deduped /
// Deferred arm:
//
//	out := r.Check(ctx, meta)
//	switch out.Decision {
//	case protect.DecisionProceed:
//	    if doRead() { _ = r.RecordAsSent(ctx, meta) }
//	case protect.DecisionDropped:
//	    // too-frequent; skip this read.
//	}
//
// Backing-store errors are fail-open: a non-nil Outcome.Err carries
// Decision == DecisionProceed so callers over-send rather than dropping
// work. RecordAsSent is record-on-success — a failed operation leaves the
// slot unrecorded so a retry isn't suppressed.
//
// # Deferred breadcrumbs and the consumer-side sweep (write gates)
//
// On every DecisionDeferred, Check calls [sendstate.Store.RecordAsDeferred]
// to store the request's MessageRef and append to LastNDeferredTimes. A
// consumer-side sweep scans for entries with a pending deferral (most
// recent deferral newer than the most recent send), re-derives the
// payload from MessageRef, and re-emits via Check;
// [WriteProtector.ReplayCandidates] yields the ones whose caps now have
// room. A successful RecordAsSent makes the send the most recent event,
// so the entry is no longer pending — recency alone resolves it. Read
// gates stamp no breadcrumb (DecisionDropped), so they have no sweep.
package protect
