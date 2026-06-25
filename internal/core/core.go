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
// The public value types and gate interfaces are defined in [pith/protect]
// (a leaf package); core imports them and supplies the only implementation —
// the concrete gate shells plus the blessed NewRead / NewWrite constructors.
// The architectural notes that users read — the read/write split, the
// CheckAndReserve / replay contract, deferred breadcrumbs, tenant holds —
// live on [pith/protect] (where users find them).
package core

import (
	"context"
	"fmt"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/protect"
	"github.com/homemade/pith/sendstate"
)

// gate holds the shared protection policy for one store. The `dedupe`
// flag is the only read-vs-write difference. The base shells [ReadGate] /
// [WriteGate] wrap a *gate and only mint namespace handles; the gating
// methods live on the scoped shells [ReadNamespaceGate] / [WriteNamespaceGate]
// (a *scopedGate), which bind the gate to a namespace. Both stamp a breadcrumb
// on a Coalescer cap and offer replay — a debounced read is re-taken against
// current state, not lost.
type gate struct {
	store      sendstate.Store
	coalescers []coalesce.Coalescer

	// dedupe runs the Layer-1 content check (Seen) when true. Write
	// gates set it; read gates don't (no payload to fingerprint).
	dedupe bool
}

// scopedGate binds a *gate to a (tenant, namespace) pair. The gating methods
// (CheckAndReserve / record / replay) live here, so the scope is fixed once
// on the handle and can't diverge between the CheckAndReserve that defers
// and the sweep that drains. "" tenant is untenanted; "" namespace is the
// whole store.
type scopedGate struct {
	*gate
	tenant    string
	namespace string
}

// ReadGate is the root read-side protector: coalescers only, no dedupe. It is
// a factory whose only method is [ReadGate.Tenant], which mints a
// [ReadTenantGate]; that handle in turn mints the namespace-scoped
// [ReadNamespaceGate] where the gating methods live. Satisfies
// [pith/protect.ReadProtector]. Construct via the factory subpackages.
type ReadGate struct {
	*gate
}

// Tenant returns a [ReadTenantGate] bound to t as the outer scope. The
// receiver is unaffected — two tenants can be served from the same root
// protector by holding two separate tenant-bound handles. Tenant("") returns
// the untenanted handle.
func (r ReadGate) Tenant(t string) protect.ReadTenant {
	return ReadTenantGate{gate: r.gate, tenant: t}
}

// ReadTenantGate is a [ReadGate] bound to one tenant — the middle step in the
// Tenant → Namespace chain. Mints a [ReadNamespaceGate] via
// [ReadTenantGate.Namespace]; that scoped handle stamps the bound tenant
// alongside the namespace on every write it commits. Satisfies
// [pith/protect.ReadTenant].
type ReadTenantGate struct {
	*gate
	tenant string // "" = untenanted
}

// Namespace returns a read handle scoped to ns ("" = the whole store). Like
// selecting a Mongo collection off a database: all gating happens through the
// returned handle, which stamps this ReadTenantGate's tenant alongside the
// namespace on every write it commits.
func (r ReadTenantGate) Namespace(ns string) protect.ReadNamespace {
	return ReadNamespaceGate{&scopedGate{gate: r.gate, tenant: r.tenant, namespace: ns}}
}

// PlaceOnHold appends a hold to the bound tenant's audit log. See
// [protect.ReadTenant.PlaceOnHold] for the contract.
func (r ReadTenantGate) PlaceOnHold(ctx context.Context, from, to time.Time, reason string) error {
	return r.store.PlaceHold(ctx, r.tenant, from, to, reason)
}

// ClearActiveHolds stamps `ClearedAt = now` on every currently-active
// hold on the bound tenant. See [protect.ReadTenant.ClearActiveHolds].
func (r ReadTenantGate) ClearActiveHolds(ctx context.Context) error {
	return r.store.ClearActiveHolds(ctx, r.tenant)
}

// HasActiveHold reports whether the bound tenant has any currently-active
// hold. Fails open on store error. See [protect.ReadTenant.HasActiveHold].
func (r ReadTenantGate) HasActiveHold(ctx context.Context) (bool, protect.Hold, error) {
	return r.store.MostRestrictiveActiveHold(ctx, r.tenant)
}

// ReadNamespaceGate is a [ReadGate] scoped to one namespace:
// CheckAndReserve / RecordAsSent / ReplayCandidates all operate within
// it. Satisfies [pith/protect.ReadNamespace].
type ReadNamespaceGate struct{ *scopedGate }

// RecordAsSent commits a performed read, advancing the Coalescer counts.
func (r ReadNamespaceGate) RecordAsSent(ctx context.Context, meta protect.RequestMeta) error {
	return r.record(ctx, meta, "")
}

// RecordAsDeferred stamps a deferred breadcrumb on the entry. See
// [protect.ReadNamespace.RecordAsDeferred] for the contract.
func (r ReadNamespaceGate) RecordAsDeferred(ctx context.Context, meta protect.RequestMeta) error {
	return r.store.RecordAsDeferred(ctx, meta.TargetKey, r.tenant, r.namespace, meta.MessageRef)
}

// ReplayCandidates collects pending deferred reads in this namespace whose
// Coalescer caps now have room. See [scopedGate.replay] for the full contract.
func (r ReadNamespaceGate) ReplayCandidates(ctx context.Context, limit int) ([]protect.DeferredRequest, error) {
	return r.replay(ctx, limit)
}

// CheckAndReserve evaluates every attached Coalescer and — on a Proceed
// — atomically reserves a send-slot. See
// [protect.ReadNamespace.CheckAndReserve] for the contract — two
// outcomes (Proceed / Deferred), fail-closed on store error, ReleaseFunc
// rolls back a Proceed reservation on op failure.
func (r ReadNamespaceGate) CheckAndReserve(ctx context.Context, meta protect.RequestMeta) (protect.Outcome, protect.ReleaseFunc) {
	return r.checkAndReserve(ctx, meta, "")
}

// WriteGate is the root write-side protector: content dedupe + coalescers. It
// is a factory whose only method is [WriteGate.Tenant], which mints a
// [WriteTenantGate]; that handle in turn mints the namespace-scoped
// [WriteNamespaceGate] where the gating methods live. Satisfies
// [pith/protect.WriteProtector]. Construct via the factory subpackages.
type WriteGate struct {
	*gate
}

// Tenant returns a [WriteTenantGate] bound to t as the outer scope. The
// receiver is unaffected. Tenant("") returns the untenanted handle.
func (w WriteGate) Tenant(t string) protect.WriteTenant {
	return WriteTenantGate{gate: w.gate, tenant: t}
}

// WriteTenantGate is a [WriteGate] bound to one tenant — the middle step in
// the Tenant → Namespace chain. Mints a [WriteNamespaceGate] via
// [WriteTenantGate.Namespace]; that scoped handle stamps the bound tenant
// alongside the namespace on every write it commits. Satisfies
// [pith/protect.WriteTenant].
type WriteTenantGate struct {
	*gate
	tenant string // "" = untenanted
}

// PlaceOnHold appends a hold to the bound tenant's audit log. See
// [protect.WriteTenant.PlaceOnHold] for the contract.
func (w WriteTenantGate) PlaceOnHold(ctx context.Context, from, to time.Time, reason string) error {
	return w.store.PlaceHold(ctx, w.tenant, from, to, reason)
}

// ClearActiveHolds stamps `ClearedAt = now` on every currently-active
// hold on the bound tenant. See [protect.WriteTenant.ClearActiveHolds].
func (w WriteTenantGate) ClearActiveHolds(ctx context.Context) error {
	return w.store.ClearActiveHolds(ctx, w.tenant)
}

// HasActiveHold reports whether the bound tenant has any currently-active
// hold. Fails open on store error. See [protect.WriteTenant.HasActiveHold].
func (w WriteTenantGate) HasActiveHold(ctx context.Context) (bool, protect.Hold, error) {
	return w.store.MostRestrictiveActiveHold(ctx, w.tenant)
}

// Namespace returns a write handle scoped to ns ("" = the whole store).
// Stamps this WriteTenantGate's tenant alongside the namespace on every write
// it commits.
func (w WriteTenantGate) Namespace(ns string) protect.WriteNamespace {
	return WriteNamespaceGate{&scopedGate{gate: w.gate, tenant: w.tenant, namespace: ns}}
}

// WriteNamespaceGate is a [WriteGate] scoped to one namespace:
// CheckAndReserve / RecordAsSent / ReplayCandidates all operate within
// it. Satisfies [pith/protect.WriteNamespace].
type WriteNamespaceGate struct{ *scopedGate }

// RecordAsSent commits a successful write (TargetKey → contentHash).
func (w WriteNamespaceGate) RecordAsSent(ctx context.Context, meta protect.RequestMeta, contentHash string) error {
	return w.record(ctx, meta, contentHash)
}

// RecordAsDeferred stamps a deferred breadcrumb on the entry. See
// [protect.WriteNamespace.RecordAsDeferred] for the contract.
func (w WriteNamespaceGate) RecordAsDeferred(ctx context.Context, meta protect.RequestMeta) error {
	return w.store.RecordAsDeferred(ctx, meta.TargetKey, w.tenant, w.namespace, meta.MessageRef)
}

// ReplayCandidates collects pending deferrals in this namespace whose Coalescer
// caps now have room. See [scopedGate.replay] for the full contract.
func (w WriteNamespaceGate) ReplayCandidates(ctx context.Context, limit int) ([]protect.DeferredRequest, error) {
	return w.replay(ctx, limit)
}

// CheckAndReserve evaluates dedupe and every attached Coalescer and — on
// a Proceed — atomically reserves a send-slot. See
// [protect.WriteNamespace.CheckAndReserve] for the contract — three
// outcomes (Proceed / Deduped / Deferred), fail-closed on store error,
// ReleaseFunc rolls back a Proceed reservation on op failure.
func (w WriteNamespaceGate) CheckAndReserve(ctx context.Context, meta protect.RequestMeta, contentHash string) (protect.Outcome, protect.ReleaseFunc) {
	return w.checkAndReserve(ctx, meta, contentHash)
}

// NewRead builds a root read gate over store with the given Coalescers (at
// least one is required; see [newGate]). Internal — called by the factory
// subpackages. The returned gate is the root of the Tenant → Namespace
// chain; callers reach a [ReadNamespaceGate] via [ReadGate.Tenant] →
// [ReadTenantGate.Namespace].
func NewRead(store sendstate.Store, coalescers ...coalesce.Coalescer) ReadGate {
	return ReadGate{gate: newGate(store, false, coalescers)}
}

// NewWrite builds a root write gate over store with the given Coalescers (at
// least one is required; see [newGate]). Internal — called by the factory
// subpackages. The returned gate is the root of the Tenant → Namespace
// chain; callers reach a [WriteNamespaceGate] via [WriteGate.Tenant] →
// [WriteTenantGate.Namespace].
func NewWrite(store sendstate.Store, coalescers ...coalesce.Coalescer) WriteGate {
	return WriteGate{gate: newGate(store, true, coalescers)}
}

// LargestHardCap returns the largest hardCap among coalescers, or 0 when
// none are given. The mongo factory uses it to derive MaxSendTimes
// without first constructing a gate.
func LargestHardCap(coalescers ...coalesce.Coalescer) int {
	largest := 0
	for _, c := range coalescers {
		if c.HardCap > largest {
			largest = c.HardCap
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
// Coalescer names (from [coalesce.Coalescer.Name]) must be unique —
// CheckAndReserve surfaces the name in Outcome.Reason, so two caps sharing
// a name would be ambiguous (panic on collision).
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
		name := c.Name()
		if _, dup := seen[name]; dup {
			panic(fmt.Sprintf("protect: duplicate Coalescer name %q — attached caps must be unique", name))
		}
		seen[name] = struct{}{}
		if c.HardCap > maxCap {
			maxCap = c.HardCap
		}
		if c.Window > maxWindow {
			maxWindow = c.Window
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

// record commits a successful send: writes (TargetKey → contentHash) to
// the store, appending a timestamp to the rolling send list and
// incrementing TotalSent. Read gates pass an empty contentHash. It stamps this
// handle's tenant + namespace on the entry + metrics so a send-only key
// (never deferred) still carries both scopes.
func (g *scopedGate) record(ctx context.Context, meta protect.RequestMeta, contentHash string) error {
	return g.store.RecordAsSent(ctx, meta.TargetKey, g.tenant, g.namespace, contentHash)
}

// replay collects pending deferrals ready to re-emit — those whose
// attached Coalescer caps currently have room — as [protect.DeferredRequest]
// (the embedded [protect.RequestMeta] carries the target key + breadcrumb;
// DeferredAt carries the most recent deferral time). Entries still inside
// a cap window are skipped: a re-emit would just defer again and waste
// the (typically expensive) re-derivation.
//
// It examines the oldest limit pending entries in this handle's namespace
// (oldest deferral first — see [sendstate.Store.RangeDeferred]). limit bounds
// the entries examined, not the slice returned; limit <= 0 means no bound.
// Because the scan is namespace-scoped, limit applies within the namespace —
// independent streams sharing a store are swept fairly (one namespace's backlog
// can't starve another's). The "" namespace sweeps the whole store.
//
// The gate covers only the caps (pure CountSentInWindow /
// CountDeferredInWindow arithmetic, no I/O). Dedupe is not applied here —
// on a write gate it still runs when the consumer re-emits each candidate
// via [WriteNamespaceGate.CheckAndReserve]; a read gate
// ([ReadNamespaceGate.CheckAndReserve]) has no dedupe and re-takes the
// read against current state.
func (g *scopedGate) replay(ctx context.Context, limit int) ([]protect.DeferredRequest, error) {
	var out []protect.DeferredRequest
	err := g.store.RangeDeferred(ctx, limit, g.namespace, func(key string, e sendstate.Entry) bool {
		if g.capsClear(e, time.Now()) {
			out = append(out, protect.DeferredRequest{
				RequestMeta: protect.RequestMeta{TargetKey: key, MessageRef: e.LastDeferredMessageRef},
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

// checkAndReserve drives the atomic Store.CheckAndReserve primitive
// through the gate's coalescer set. The path:
//
//  1. Build a [sendstate.ReserveRequest] from this handle's
//     tenant / namespace + the caller's meta + contentHash + the
//     gate's [coalesce.Coalescer] slice (the store switches on each
//     Coalescer's Strategy internally — Go-side for the in-process
//     backend, Mongo aggregation for the Atlas backend).
//  2. Call [sendstate.Store.CheckAndReserve], translate the
//     [sendstate.ReserveResult] into a [protect.Outcome], and on a
//     Proceed return a [protect.ReleaseFunc] that closes over the
//     reserved timestamp.
//
// Fail-policy: a store error returns DecisionDeferred + non-nil
// Outcome.Err (fail-closed — replay sweep re-drives). Distinct from
// [scopedGate.check], which fails open. The polarity matters because
// the reserve path exists to enforce a cap; failing open here would let
// a store outage breach the very cap CheckAndReserve closes the TOCTOU on.
//
// Read gates pass an empty contentHash — the store's dedupe layer is
// then wired off and Deduped is unreachable.
func (g *scopedGate) checkAndReserve(ctx context.Context, meta protect.RequestMeta, contentHash string) (protect.Outcome, protect.ReleaseFunc) {
	req := sendstate.ReserveRequest{
		Key:         meta.TargetKey,
		Tenant:      g.tenant,
		Namespace:   g.namespace,
		ContentHash: contentHash,
		Coalescers:  g.coalescers,
	}
	res, err := g.store.CheckAndReserve(ctx, req, meta.MessageRef)
	if err != nil {
		// Fail-closed: even if the store erred mid-pipeline, surface
		// Deferred so the replay sweep re-drives the request.
		reason := res.Reason
		if reason == "" {
			reason = "store error"
		}
		return protect.Outcome{Decision: protect.DecisionDeferred, Reason: reason, Err: err}, nil
	}
	switch {
	case res.Deduped:
		return protect.Outcome{Decision: protect.DecisionDeduped, Reason: res.Reason}, nil
	case res.Deferred:
		return protect.Outcome{Decision: protect.DecisionDeferred, Reason: res.Reason}, nil
	}
	// Proceed: capture the reservedAt for the release closure. The store
	// reference is captured too — same lifetime as the gate.
	store := g.store
	key := meta.TargetKey
	reservedAt := res.ReservedAt
	return protect.Outcome{Decision: protect.DecisionProceed}, func(ctx context.Context) error {
		return store.ReleaseReservation(ctx, key, reservedAt)
	}
}
