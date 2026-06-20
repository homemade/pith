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
// Check/RecordAsSent contract, deferred breadcrumbs, replay — live on
// [pith/protect] (where users find them).
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
// (check / record / replay) live here, so the scope is fixed once on the
// handle and can't diverge between the Check that defers and the sweep that
// drains. "" tenant is untenanted; "" namespace is the whole store.
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

// ReadNamespaceGate is a [ReadGate] scoped to one namespace: Check / RecordAsSent
// / ReplayCandidates all operate within it. Satisfies [pith/protect.ReadNamespace].
type ReadNamespaceGate struct{ *scopedGate }

// Check gates a candidate read. Returns DecisionProceed or, on a Coalescer
// cap, DecisionDeferred (a breadcrumb is stamped for the replay sweep). Never
// DecisionDeduped (no dedupe layer).
func (r ReadNamespaceGate) Check(ctx context.Context, meta protect.RequestMeta) protect.Outcome {
	return r.check(ctx, meta, "")
}

// RecordAsSent commits a performed read, advancing the Coalescer counts.
func (r ReadNamespaceGate) RecordAsSent(ctx context.Context, meta protect.RequestMeta) error {
	return r.record(ctx, meta, "")
}

// ReplayCandidates collects pending deferred reads in this namespace whose
// Coalescer caps now have room. See [scopedGate.replay] for the full contract.
func (r ReadNamespaceGate) ReplayCandidates(ctx context.Context, limit int) ([]protect.DeferredRequest, error) {
	return r.replay(ctx, limit)
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

// Namespace returns a write handle scoped to ns ("" = the whole store).
// Stamps this WriteTenantGate's tenant alongside the namespace on every write
// it commits.
func (w WriteTenantGate) Namespace(ns string) protect.WriteNamespace {
	return WriteNamespaceGate{&scopedGate{gate: w.gate, tenant: w.tenant, namespace: ns}}
}

// WriteNamespaceGate is a [WriteGate] scoped to one namespace: Check /
// RecordAsSent / ReplayCandidates all operate within it. Satisfies
// [pith/protect.WriteNamespace].
type WriteNamespaceGate struct{ *scopedGate }

// Check gates a candidate write. Returns DecisionProceed, DecisionDeduped
// (identical content), or DecisionDeferred (a Coalescer cap fired — a
// breadcrumb is stamped for the replay sweep).
func (w WriteNamespaceGate) Check(ctx context.Context, meta protect.RequestMeta, contentHash string) protect.Outcome {
	return w.check(ctx, meta, contentHash)
}

// RecordAsSent commits a successful write (TargetKey → contentHash).
func (w WriteNamespaceGate) RecordAsSent(ctx context.Context, meta protect.RequestMeta, contentHash string) error {
	return w.record(ctx, meta, contentHash)
}

// ReplayCandidates collects pending deferrals in this namespace whose Coalescer
// caps now have room. See [scopedGate.replay] for the full contract.
func (w WriteNamespaceGate) ReplayCandidates(ctx context.Context, limit int) ([]protect.DeferredRequest, error) {
	return w.replay(ctx, limit)
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
// Backing-store errors are fail-open via [protect.Outcome.Err]: a ReadEntry
// failure yields (DecisionProceed, Err); a failed RecordAsDeferred stamp
// still yields DecisionDeferred with the error attached.
func (g *scopedGate) check(ctx context.Context, meta protect.RequestMeta, contentHash string) protect.Outcome {
	now := time.Now()
	entry, err := g.store.ReadEntry(ctx, meta.TargetKey)
	if err != nil {
		return protect.Outcome{Decision: protect.DecisionProceed, Err: err}
	}

	// Layer 1: content dedupe (write gates only).
	if g.dedupe && entry.Seen(contentHash) {
		return protect.Outcome{Decision: protect.DecisionDeduped, Reason: "duplicate content"}
	}

	// Layer 2: each attached Coalescer in order. The deferral stamps this
	// handle's tenant + namespace so the replay sweep can scope to namespace
	// and observability can group by tenant.
	for _, c := range g.coalescers {
		if c.ShouldDefer(entry, now) {
			capName, _, _ := c.CapPolicy()
			recErr := g.store.RecordAsDeferred(ctx, meta.TargetKey, g.tenant, g.namespace, meta.MessageRef)
			return protect.Outcome{Decision: protect.DecisionDeferred, Reason: capName, Err: recErr}
		}
	}

	return protect.Outcome{Decision: protect.DecisionProceed}
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
// via [WriteNamespaceGate.Check]; a read gate ([ReadNamespaceGate.Check]) has
// no dedupe and re-takes the read against current state.
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
