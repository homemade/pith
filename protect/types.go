package protect

import (
	"context"
	"time"
)

// This file is the definition site of pith's public protection surface — the
// request/outcome value types and the four gate interfaces. They live here (a
// leaf package that imports nothing else in pith) so the public names users see
// in godoc and compiler errors are protect.X, not an internal/core alias.
//
// internal/core imports these and supplies the only implementation: the
// concrete gate shells plus the blessed constructors (NewRead / NewWrite),
// which the factory subpackages [pith/protect/memory] and [pith/protect/mongodb]
// wrap. The internal/ split keeps gate construction unreachable from outside the
// module, so the supported backends stay the two pith ships.

// RequestMeta is the addressing primitive: the target slot plus the replay
// breadcrumb, carrying no content fingerprint. It is the complete request for a
// read gate ([ReadNamespace]); it is the addressing half of a write gate
// ([WriteNamespace]) call, which adds a contentHash argument; and it is embedded
// by [DeferredRequest] on the output side of a replay sweep, where the payload
// hasn't been re-derived yet.
type RequestMeta struct {
	// TargetKey identifies the per-key slot used by dedupe and every
	// attached Coalescer (typically "{activity-id}:{contact-id}" or
	// similar). The shared key across all layers is what lets them
	// collapse to a single record per target.
	TargetKey string

	// MessageRef is caller-defined data stored in the sendstate entry's
	// LastDeferredMessageRef when a CheckAndReserve returns
	// DecisionDeferred (write or read gate). A sweep layer reads it back
	// to re-derive and re-emit. Typically a small reference (e.g. an
	// upstream event ID + context, JSON-encoded). A write sweep re-emits
	// the re-derived payload; a read sweep re-derives the target and
	// re-fetches current state.
	MessageRef []byte
}

// DeferredRequest is the unit yielded by [WriteNamespace.ReplayCandidates] /
// [ReadNamespace.ReplayCandidates]: a pending deferral whose attached Coalescer
// caps currently have room to re-emit. It embeds [RequestMeta] (target key +
// breadcrumb — what the consumer re-derives from and re-emits via
// CheckAndReserve) and adds the timestamp of the most recent deferral, so the
// consumer can reason about age without a second read.
type DeferredRequest struct {
	RequestMeta

	// DeferredAt is the timestamp of the most recent deferral on this key
	// (the tail of the sendstate entry's deferral history).
	DeferredAt time.Time
}

// Decision is the outcome of a CheckAndReserve call.
type Decision int

const (
	// DecisionProceed: caller should perform the gated operation.
	// CheckAndReserve has atomically reserved the send-slot already, so a
	// successful op needs no follow-up — the reserve IS the record of the
	// send. A failed op calls the returned [ReleaseFunc] to pop the
	// reservation by value.
	DecisionProceed Decision = iota

	// DecisionDeduped: caller should drop the operation — the content
	// fingerprint is identical to the most recent successful send for
	// this key, so re-sending is genuinely redundant. Write gates only
	// (a read gate has no content to dedupe). Reason is "duplicate
	// content"; no breadcrumb is stamped — the duplicate is already at
	// the destination, so there is nothing to replay.
	DecisionDeduped

	// DecisionDeferred: a Coalescer cap pushed back. Reason names the
	// Coalescer. CheckAndReserve stamps a deferred breadcrumb so a
	// consumer-side sweep can re-emit once the cap window clears — the
	// deferred operation is one the caller still intends to perform (a
	// write to retry, or a read to re-take against current state).
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

// ReleaseFunc rolls back the optimistic reserve made by a Proceed
// [WriteNamespace.CheckAndReserve] / [ReadNamespace.CheckAndReserve] when
// the gated operation subsequently fails. It pops the reserved
// send-timestamp from the entry's send-time list (by value, so concurrent
// sibling reserves aren't clobbered) but does not roll back lifetime
// metrics — see the sendstate package doc. Best-effort: a backing-store
// error is returned for logging but the slot leaks until the trailing
// window slides it out.
//
// nil on non-Proceed outcomes (Deduped / Deferred): there is no
// reservation to release. Callers should pattern-match on Outcome.Decision
// and only invoke the release on the Proceed branch's op-failure path.
type ReleaseFunc func(ctx context.Context) error

// Outcome reports a CheckAndReserve result. Decision is always actionable;
// Err carries any backing-store failure encountered along the way (so a
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
	// decision, or nil. A failed deferral stamp still yields
	// DecisionDeferred with the error attached. Callers act on Decision
	// and log Err if set.
	Err error
}

// Hold is one entry in the per-tenant holds audit log. The lifecycle
// has three states:
//
//   - **Active**: `From ≤ now < To AND ClearedAt is zero`. Gated ops on
//     the tenant are suppressed.
//   - **Naturally expired**: `now ≥ To AND ClearedAt is zero`. The
//     window passed without a manual clear; entry remains in the audit
//     array.
//   - **Explicitly cleared**: `ClearedAt is non-zero`, set by an
//     operator-driven unpause via [WriteTenant.ClearActiveHolds] /
//     [ReadTenant.ClearActiveHolds]. The original window is preserved
//     alongside the clear timestamp for audit.
//
// The holds log is append-only with no TTL by design — every hold ever
// placed is a permanent audit record. [WriteTenant.HasActiveHold] /
// [ReadTenant.HasActiveHold] return only currently-active entries (the
// most-restrictive one); the other states are visible only via direct
// read of the holds collection / map.
type Hold struct {
	// From is when the hold takes effect. A zero time passed to
	// PlaceOnHold is treated as "now".
	From time.Time

	// To is when the hold naturally expires (exclusive — active while
	// now < To).
	To time.Time

	// Reason is a human-readable label set by PlaceOnHold; surfaced as
	// the hold-suppression reason at consult sites.
	Reason string

	// SetAt is the server-side timestamp at which the hold was placed,
	// stamped by PlaceOnHold. Differentiates stacked holds with
	// identical (From, To, Reason).
	SetAt time.Time

	// ClearedAt is zero on currently-active and naturally-expired
	// entries; non-zero on entries explicitly cleared via
	// ClearActiveHolds. Always zero on the Hold value returned by
	// HasActiveHold (which filters cleared entries out).
	ClearedAt time.Time
}

// ReadProtector is the root read-side gate for content-free operations — a
// read, poll, inbound event, or fire-and-forget trigger — with one or more
// coalesce cap policies and no content dedupe. It is a factory only: bind a
// tenant with [ReadProtector.Tenant] to get a [ReadTenant], then pick a
// namespace with [ReadTenant.Namespace] to get a [ReadNamespace], where the
// gating happens (like selecting a Mongo collection off a database). Tenant("")
// is the "no outer scope" sentinel and Namespace("") is the whole-store
// namespace — the chain is the single entry point even when you don't scope
// either layer. Construct via [pith/protect/memory.NewReadProtector] or
// [pith/protect/mongodb.NewReadProtector].
//
// The tenant is a labelling field for observability and per-tenant queries —
// it does not isolate TargetKeys or scope the
// [ReadNamespace.ReplayCandidates] sweep (still namespace-scoped).
type ReadProtector interface {
	// Tenant returns a [ReadTenant] bound to t as the outer scope. Calling
	// [ReadTenant.Namespace] on the returned handle mints a [ReadNamespace]
	// that stamps t on every entry / Metrics doc it writes. The original
	// ReadProtector is unaffected — Tenant produces a fresh handle rather
	// than mutating shared state, so two tenants can be served from the
	// same root protector.
	//
	// Tenant("") is the untenanted handle: the empty string is the "no
	// outer scope" sentinel, consistent with how Namespace("") is the
	// whole-store namespace.
	Tenant(t string) ReadTenant
}

// ReadTenant is a [ReadProtector] bound to one tenant — the middle step in
// the Tenant → Namespace chain. Pick a namespace with [ReadTenant.Namespace]
// to get a [ReadNamespace], where CheckAndReserve / RecordAsSent /
// ReplayCandidates run. Namespace handles minted from this ReadTenant stamp
// the bound tenant on every entry / Metrics doc they write.
//
// Tenant-scoped hold operations ([ReadTenant.PlaceOnHold] /
// [ReadTenant.ClearActiveHolds] / [ReadTenant.HasActiveHold]) live here
// rather than on the namespace handle because a hold suppresses every
// gated op for the bound tenant regardless of namespace — typical use is
// honouring a downstream rate-limit response, where a single PlaceOnHold
// at the tenant scope blocks every pipeline under that tenant (so a 429
// on one pipeline doesn't keep firing on its siblings).
type ReadTenant interface {
	// Namespace returns a [ReadNamespace] scoped to ns ("" = the whole
	// store). The returned handle is where gating happens —
	// CheckAndReserve / RecordAsSent / ReplayCandidates all operate
	// within ns and stamp the tenant bound on this ReadTenant alongside
	// the namespace on every write they commit.
	Namespace(ns string) ReadNamespace

	// PlaceOnHold appends a hold entry to the tenant's audit log,
	// suppressing gated ops on this tenant from `from` (inclusive) until
	// `to` (exclusive). `from.IsZero()` is treated as "now" by the
	// storage layer. The append is atomic — concurrent PlaceOnHold calls
	// each contribute their own entry; no caller-supplied id is needed.
	// `reason` is surfaced via [HasActiveHold]'s returned [Hold.Reason]
	// and intended as a short human-readable label.
	PlaceOnHold(ctx context.Context, from, to time.Time, reason string) error

	// ClearActiveHolds atomically stamps `ClearedAt = now` on every
	// currently-active hold on this tenant (entries where
	// `From ≤ now < To AND ClearedAt is zero`). Expired or
	// already-cleared entries are left alone. The cleared entries remain
	// in the audit array — the holds log is append-only — so the audit
	// trail preserves both the original window and the manual clearance.
	// The operator escape hatch and intended manual-unpause path.
	ClearActiveHolds(ctx context.Context) error

	// HasActiveHold reports whether this tenant has any currently-active
	// hold. When `active` is true, `hold` is the most-restrictive active
	// entry (the one with the latest `To` among active entries); when
	// `active` is false, `hold` is the zero [Hold] value. Fails open on a
	// store error — `(false, Hold{}, err)` — so a caller that treats
	// `active=false` as "no hold" proceeds rather than blocking on a
	// degraded backend (matching the existing read-gate fail-open policy).
	HasActiveHold(ctx context.Context) (active bool, hold Hold, err error)
}

// ReadNamespace is a [ReadProtector] scoped to one namespace — where reads
// are gated (CheckAndReserve) and swept (ReplayCandidates, within the
// namespace). A capped CheckAndReserve is DEFERRED: a breadcrumb is
// stashed and a consumer sweep re-takes the read against current state
// once the cap clears (the final state is never lost — a dropped
// cap-suppression would lose it). Pair it with a
// [pith/coalesce.NewTrailingEdgeDebounce] so a sustained burst collapses
// to a single final read after quiet.
type ReadNamespace interface {
	RecordAsSent(ctx context.Context, meta RequestMeta) error
	ReplayCandidates(ctx context.Context, limit int) ([]DeferredRequest, error)

	// RecordAsDeferred stamps a deferred breadcrumb on the entry,
	// making it replay-eligible via [ReplayCandidates]. Mirror of
	// [WriteNamespace.RecordAsDeferred] for the read gate; see that
	// method's godoc for the use-cases.
	RecordAsDeferred(ctx context.Context, meta RequestMeta) error

	// CheckAndReserve evaluates every attached Coalescer and — on a
	// Proceed — atomically reserves a send-slot for the caller before they
	// perform the gated read. The returned ReleaseFunc rolls the
	// reservation back on op failure.
	//
	// Two outcomes (DecisionProceed / DecisionDeferred) — never
	// DecisionDeduped, because the read gate has no content to fingerprint.
	// On a Deferred outcome the deferred breadcrumb is stamped: a
	// consumer-side sweep (ReplayCandidates) re-takes the read once the
	// cap clears.
	//
	// Fail-policy: a backing-store error returns DecisionDeferred + non-nil
	// Outcome.Err (fail-closed — the replay sweep re-drives). Callers
	// that prefer fail-open semantics (over-read rather than drop a
	// legitimate fetch) override at the call site by treating Err as a
	// proceed.
	//
	// ReleaseFunc is nil on Deferred. On Proceed it is non-nil; the caller
	// invokes it only when the gated read subsequently fails — a successful
	// read leaves the reserve in place as the canonical "record of read."
	CheckAndReserve(ctx context.Context, meta RequestMeta) (Outcome, ReleaseFunc)
}

// WriteProtector is the root write-side gate for content-bearing operations —
// a send/merge/PATCH — with content dedupe plus one or more coalesce cap
// policies. Like [ReadProtector] it is a factory only: bind a tenant with
// [WriteProtector.Tenant] to get a [WriteTenant], then pick a namespace with
// [WriteTenant.Namespace] to get a [WriteNamespace], where the gating happens.
// Construct via [pith/protect/memory.NewWriteProtector] or
// [pith/protect/mongodb.NewWriteProtector]. See [ReadProtector] for the
// labelling-vs-isolation contract — it applies identically here.
type WriteProtector interface {
	// Tenant returns a [WriteTenant] bound to t as the outer scope. Calling
	// [WriteTenant.Namespace] on the returned handle mints a [WriteNamespace]
	// that stamps t on every entry / Metrics doc it writes. The original
	// WriteProtector is unaffected — Tenant produces a fresh handle rather
	// than mutating shared state.
	//
	// Tenant("") is the untenanted handle.
	Tenant(t string) WriteTenant
}

// WriteTenant is a [WriteProtector] bound to one tenant — the middle step in
// the Tenant → Namespace chain. Pick a namespace with [WriteTenant.Namespace]
// to get a [WriteNamespace]. Namespace handles minted from this WriteTenant
// stamp the bound tenant on every entry / Metrics doc they write.
//
// Tenant-scoped hold operations ([WriteTenant.PlaceOnHold] /
// [WriteTenant.ClearActiveHolds] / [WriteTenant.HasActiveHold]) live here
// rather than on the namespace handle because a hold suppresses every
// gated op for the bound tenant regardless of namespace — typical use is
// honouring a downstream rate-limit response, where a single PlaceOnHold
// at the tenant scope blocks every pipeline under that tenant (so a 429
// on one pipeline doesn't keep firing on its siblings).
type WriteTenant interface {
	// Namespace returns a [WriteNamespace] scoped to ns ("" = the whole
	// store). The returned handle is where gating happens and stamps the
	// tenant bound on this WriteTenant alongside the namespace on every
	// write it commits.
	Namespace(ns string) WriteNamespace

	// PlaceOnHold appends a hold entry to the tenant's audit log,
	// suppressing gated ops on this tenant from `from` (inclusive) until
	// `to` (exclusive). `from.IsZero()` is treated as "now" by the
	// storage layer. The append is atomic — concurrent PlaceOnHold calls
	// each contribute their own entry; no caller-supplied id is needed.
	// `reason` is surfaced via [HasActiveHold]'s returned [Hold.Reason]
	// and intended as a short human-readable label.
	PlaceOnHold(ctx context.Context, from, to time.Time, reason string) error

	// ClearActiveHolds atomically stamps `ClearedAt = now` on every
	// currently-active hold on this tenant (entries where
	// `From ≤ now < To AND ClearedAt is zero`). Expired or
	// already-cleared entries are left alone. The cleared entries remain
	// in the audit array — the holds log is append-only — so the audit
	// trail preserves both the original window and the manual clearance.
	// The operator escape hatch and intended manual-unpause path.
	ClearActiveHolds(ctx context.Context) error

	// HasActiveHold reports whether this tenant has any currently-active
	// hold. When `active` is true, `hold` is the most-restrictive active
	// entry (the one with the latest `To` among active entries); when
	// `active` is false, `hold` is the zero [Hold] value. Fails open on
	// a store error — `(false, Hold{}, err)` — so a caller treating
	// `active=false` as "no hold" proceeds rather than blocking on a
	// degraded backend (a missed hold-read costs at most one extra round
	// of unsuppressed traffic before the next 429 re-arms).
	HasActiveHold(ctx context.Context) (active bool, hold Hold, err error)
}

// WriteNamespace is a [WriteProtector] scoped to one namespace — where writes
// are gated (CheckAndReserve) and swept (ReplayCandidates). A capped
// CheckAndReserve is DEFERRED: the write is an action you still intend to
// perform, so a breadcrumb is stashed for re-emission once the cap window
// clears. A DecisionDeduped (identical content to the last send) is dropped,
// not replayed.
type WriteNamespace interface {
	RecordAsSent(ctx context.Context, meta RequestMeta, contentHash string) error
	ReplayCandidates(ctx context.Context, limit int) ([]DeferredRequest, error)

	// RecordAsDeferred stamps a deferred breadcrumb on the entry —
	// LastDeferredMessageRef + LastNDeferredTimes — making the entry
	// replay-eligible via [ReplayCandidates]. CheckAndReserve already
	// stamps the breadcrumb internally when its policy chain defers,
	// so this method exists for the cases where the protect-layer
	// caller wants to defer EXPLICITLY (independent of the
	// CheckAndReserve outcome):
	//
	//   - A wire-side rate-limit error after a Proceed reservation:
	//     the caller calls the release closure to pop the
	//     timestamp, then RecordAsDeferred to stamp the breadcrumb;
	//     the replay sweep then re-drives the request once the
	//     tenant's hold clears.
	//   - A pre-CheckAndReserve tenant-level hold: every kept activity
	//     gets a breadcrumb stamped so replay picks them up once the
	//     hold clears.
	//
	// Best-effort: a store error is returned for logging; the entry
	// just isn't replay-eligible.
	RecordAsDeferred(ctx context.Context, meta RequestMeta) error

	// CheckAndReserve evaluates dedupe and every attached Coalescer and
	// — on a Proceed — atomically reserves a send-slot for the caller
	// before they perform the gated write. The returned ReleaseFunc
	// rolls the reservation back on op failure.
	//
	// Three outcomes (DecisionProceed / DecisionDeduped / DecisionDeferred).
	// On a Deduped outcome the caller drops the operation; on a Deferred
	// outcome the deferred breadcrumb is stamped, and a consumer-side
	// sweep (ReplayCandidates) re-emits once the cap clears.
	//
	// Fail-policy: a backing-store error returns DecisionDeferred + non-nil
	// Outcome.Err (fail-closed — the replay sweep re-drives). Cap
	// discipline holds even when the backing store flaps.
	//
	// ReleaseFunc is nil on Deduped / Deferred. On Proceed it is non-nil;
	// the caller invokes it only on op failure — a successful op leaves the
	// reserve as the canonical "record of send."
	CheckAndReserve(ctx context.Context, meta RequestMeta, contentHash string) (Outcome, ReleaseFunc)
}
