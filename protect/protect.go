// Package protect composes pith's integration-guard policies behind two
// capability-typed facades — [ReadProtector] and [WriteProtector] — so a
// caller applies the right policy set with one CheckAndReserve per gated
// operation.
//
// # Read vs write
//
// The two gates model the two kinds of operation pith guards:
//
//   - [WriteProtector] — a content-bearing operation (a send / merge /
//     PATCH). Applies content dedupe AND one or more coalesce caps. A
//     capped CheckAndReserve is DEFERRED: the write is an action you still
//     intend to perform, so a breadcrumb is stashed and ReplayCandidates
//     yields it for re-emission once the cap window clears. Its
//     CheckAndReserve takes a contentHash.
//
//   - [ReadProtector] — a content-free operation (a read, poll, inbound
//     event, fire-and-forget trigger). Applies coalesce caps only — there
//     is no payload to fingerprint, so no dedupe. A capped CheckAndReserve
//     is DEFERRED: re-taking the read against current state is still an
//     action you intend to perform, so a breadcrumb is stashed and
//     ReplayCandidates yields it for a sweep to re-read once the cap clears.
//     Its CheckAndReserve takes no hash.
//
// Both gates defer (not drop) a capped CheckAndReserve, because a cap
// suppresses whatever arrived at the wrong moment — which may be the final,
// changed state — so it must be replayed, not lost. Only dedupe is safe to
// drop without replay (the duplicate is already at the destination); that
// is why dedupe belongs to the write side alone. A write replays the stashed
// payload; a read replays the *act of reading*, re-fetching current state —
// re-reading after the burst settles captures exactly the final value.
//
// # Limitation
//
// The two gates cover the two combinations that arise in practice:
// content-bearing + dedupe (write) and content-free, no-dedupe (read) —
// both replay a capped CheckAndReserve. An operation that is content-bearing
// but must be re-emitted *without* dedupe (replay every cap-suppressed
// payload, even an unchanged one) fits neither cleanly. No such case exists
// today; if one arises it wants a third gate (or decoupling the dedupe
// decision from content presence). These are behavioural bundles, not an
// HTTP-method taxonomy.
//
// # The mechanism set
//
// CheckAndReserve evaluates every policy atomically against the per-key
// [sendstate.Entry] in a single backing-store op:
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
// CheckAndReserve evaluates every policy anchored at a single now: dedupe
// first (write gates), then each Coalescer in attached order, returning the
// first suppression. Outcome.Reason distinguishes which mechanism produced
// it. On Proceed it atomically reserves a send-slot (pushes the timestamp
// onto the entry's send-time list) and returns a [ReleaseFunc] the caller
// invokes on op failure to pop the reservation by value. The read + decide
// + write all run as one op, so backends that pay per round-trip (e.g.
// Mongo) incur a single op per gated call regardless of how many Coalescers
// are attached, and the per-key cap holds strictly even under concurrent
// callers (no TOCTOU between deciding and reserving).
//
// # Outcomes
//
// A read gate's CheckAndReserve returns DecisionProceed or DecisionDeferred.
// A write gate's returns DecisionProceed, DecisionDeduped, or
// DecisionDeferred. DecisionDeferred means the same thing on both gates — a
// cap fired, a breadcrumb is stashed, and the request will replay once the
// cap clears; only the write gate adds DecisionDeduped (an identical
// payload, dropped silently — nothing to replay).
//
// # Scope
//
// A replay sweep ([ReadNamespace.ReplayCandidates] /
// [WriteNamespace.ReplayCandidates]) enumerates the WHOLE store within the
// handle's namespace; TargetKey addresses CheckAndReserve, not the sweep.
// There are three scope tools, for three different needs:
//
//   - SEPARATE STORES (a distinct Database in the [pith/protect/mongodb]
//     factory) for streams that replay through DIFFERENT consumers. Sharing a
//     store here lets one stream's sweep drain and resolve the other's pending
//     deferrals — re-deriving and re-emitting them through the wrong consumer.
//     Never split such streams by key prefix; the collection boundary is the
//     only isolation that keeps their replay logic apart.
//
//   - NAMESPACES (pick one with .Tenant(t).Namespace(ns), where t may be "")
//     for many streams with the SAME consumer that share a store but must be
//     swept fairly. A namespace-scoped sweep applies limit and the oldest-first
//     ordering within the namespace, so one namespace's backlog can't
//     head-of-line-block another's. It narrows only along the namespace axis;
//     it is not a substitute for separate stores across consumers. The ""
//     namespace is the whole store.
//
//   - TENANTS (chain with Tenant(t).Namespace(ns)) as an optional OUTER scope
//     above the namespace. Tenant doubles as label and hold scope: it is
//     stamped on every Entry / Metrics write (queryable / indexable for
//     observability or per-tenant aggregation) AND it gates a hold primitive
//     — [WriteTenant.PlaceOnHold] / [ReadTenant.PlaceOnHold] /
//     [WriteTenant.HasActiveHold] / [ReadTenant.HasActiveHold] /
//     [WriteTenant.ClearActiveHolds] / [ReadTenant.ClearActiveHolds] — for
//     tenant-wide ops suppression (honouring a downstream rate-limit
//     response, an operator maintenance pause, etc.). Tenant does NOT scope
//     the replay sweep or isolate TargetKeys: if two tenants must use the
//     same namespace name and need distinct keys, include the tenant in
//     TargetKey explicitly; pith does not derive keys from Tenant. Tenant("")
//     is the untenanted handle — the "no outer scope" sentinel of the chain.
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
// # CheckAndReserve / replay contract
//
// Write happy-path (against the Mongo factory). The client is the caller's —
// open it with mongo.Connect (configured with majority write concern; see
// [pith/sendstate/mongodb]), share it across pith stores or other libraries,
// and Disconnect at shutdown:
//
//	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetWriteConcern(writeconcern.Majority()))
//	if err != nil { ... }
//	defer client.Disconnect(ctx)
//
//	p, err := pmongo.NewWriteProtector(ctx, client, pmongo.Config{
//	    Database: ..., EntryTTL: 48*time.Hour,
//	},
//	    coalesce.NewQuota(50, 24*time.Hour),
//	    coalesce.NewLeadingEdgeDebounce(10*time.Second),
//	)
//	if err != nil { ... }
//
//	// Scope: outer Tenant for observability + holds, inner Namespace for
//	// the sweep. Use Tenant("") for the untenanted handle and Namespace("")
//	// for the whole-store namespace.
//	w := p.Tenant(orgID).Namespace(campaignID)
//	meta := protect.RequestMeta{TargetKey: activityID + ":" + contactID, MessageRef: ref}
//	out, release := w.CheckAndReserve(ctx, meta, contentHash)
//	if out.Err != nil {
//	    log.Printf("pith.CheckAndReserve: %v", out.Err) // fail-closed; Decision is Deferred
//	}
//	switch out.Decision {
//	case protect.DecisionProceed:
//	    // A slot is already reserved on the entry. Perform the op.
//	    if err := svc.Send(...); err != nil {
//	        _ = release(ctx) // pop the reservation by value
//	    }
//	    // Success: the reservation IS the canonical record-of-send;
//	    // no follow-up call needed.
//	case protect.DecisionDeduped:
//	    // identical content to the last send; drop.
//	case protect.DecisionDeferred:
//	    // a Coalescer cap fired; a breadcrumb is stamped for the sweep.
//	}
//
// A read gate is the same shape without the hash and with no Deduped arm —
// a capped read defers (and a sweep replays it) rather than dropping:
//
//	r := readProtector.Tenant(orgID).Namespace(campaignID) // Tenant("") for untenanted
//	out, release := r.CheckAndReserve(ctx, meta)
//	switch out.Decision {
//	case protect.DecisionProceed:
//	    if err := doRead(); err != nil {
//	        // The read gate's record-on-proceed semantic deliberately
//	        // consumes the slot even on fetch failure (the debounce
//	        // window throttles retries too). Most read callers discard
//	        // release for that reason; invoke it only if you want a
//	        // failed fetch to leave the slot unconsumed.
//	        _ = release
//	    }
//	case protect.DecisionDeferred:
//	    // a Coalescer cap fired; a breadcrumb is stamped — a sweep
//	    // (ReplayCandidates) will re-take this read once the cap clears.
//	}
//
// Backing-store errors are fail-closed: a non-nil Outcome.Err carries
// Decision == DecisionDeferred so callers defer-and-replay rather than risk
// an unintended overshoot (the cap stays strict even when the backing store
// flaps). The replay sweep re-drives deferred entries on a subsequent
// invocation. A caller that prefers fail-open semantics (over-send rather
// than drop a legitimate op) overrides at the call site by treating
// out.Err as a proceed.
//
// # Deferred breadcrumbs and the consumer-side sweep
//
// On every DecisionDeferred, CheckAndReserve stores the request's MessageRef
// and appends to LastNDeferredTimes as part of its atomic op. A consumer-side
// sweep scans for entries with a pending deferral (most recent deferral newer
// than the most recent send), re-derives from MessageRef, and re-emits via
// CheckAndReserve; [WriteNamespace.ReplayCandidates] /
// [ReadNamespace.ReplayCandidates] yield the ones whose caps now have room.
// A write sweep re-emits the stashed payload; a read sweep re-fetches
// current state. A successful CheckAndReserve reservation makes the send
// the most recent event, so the entry is no longer pending — recency alone
// resolves it.
//
// Both gates run this sweep — the read gate is replay-capable, the same
// machinery as the write gate minus dedupe. pith fires no timers itself:
// the replay/trailing fire happens when a consumer sweep next runs after the
// cap clears — under continuous traffic (a sweep at each request tail) within
// a request or two of going quiet; if all traffic stops, at the next request
// or a cron sweep. A caller that genuinely wants a fire-and-forget poll
// throttle can simply not run a sweep — the deferred entries then TTL out.
package protect
