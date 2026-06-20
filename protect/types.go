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
	// LastDeferredMessageRef when a Check returns DecisionDeferred (write or
	// read gate). A sweep layer reads it back to re-derive and re-emit.
	// Typically a small reference (e.g. an upstream event ID + context,
	// JSON-encoded). A write sweep re-emits the re-derived payload; a read
	// sweep re-derives the target and re-fetches current state.
	MessageRef []byte
}

// DeferredRequest is the unit yielded by [WriteNamespace.ReplayCandidates] /
// [ReadNamespace.ReplayCandidates]: a pending deferral whose attached Coalescer
// caps currently have room to re-emit. It embeds [RequestMeta] (target key +
// breadcrumb — what the consumer re-derives from and re-emits via Check) and
// adds the timestamp of the most recent deferral, so the consumer can reason
// about age without a second read.
type DeferredRequest struct {
	RequestMeta

	// DeferredAt is the timestamp of the most recent deferral on this key
	// (the tail of the sendstate entry's deferral history).
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
	// Coalescer. Check stamps a deferred breadcrumb so a consumer-side sweep
	// can re-emit once the cap window clears — the deferred operation is one
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

// Outcome reports a Check result. Decision is always actionable; Err carries any
// backing-store failure encountered along the way (so a caller can log it
// without losing the policy outcome).
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
	// DecisionProceed); a failed deferral stamp still yields
	// DecisionDeferred with the error attached. Callers act on Decision
	// and log Err if set.
	Err error
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
// to get a [ReadNamespace], where Check / RecordAsSent / ReplayCandidates run.
// Namespace handles minted from this ReadTenant stamp the bound tenant on
// every entry / Metrics doc they write.
type ReadTenant interface {
	// Namespace returns a [ReadNamespace] scoped to ns ("" = the whole
	// store). The returned handle is where gating happens — Check /
	// RecordAsSent / ReplayCandidates all operate within ns and stamp the
	// tenant bound on this ReadTenant alongside the namespace on every
	// write they commit.
	Namespace(ns string) ReadNamespace
}

// ReadNamespace is a [ReadProtector] scoped to one namespace — where reads are
// gated (Check / RecordAsSent) and swept (ReplayCandidates, within the
// namespace). A capped Check is DEFERRED: a breadcrumb is stashed and a consumer
// sweep re-takes the read against current state once the cap clears (the final
// state is never lost — a dropped cap-suppression would lose it). Pair it with a
// [pith/coalesce.NewTrailingEdgeDebounce] so a sustained burst collapses to a
// single final read after quiet.
type ReadNamespace interface {
	Check(ctx context.Context, meta RequestMeta) Outcome
	RecordAsSent(ctx context.Context, meta RequestMeta) error
	ReplayCandidates(ctx context.Context, limit int) ([]DeferredRequest, error)
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
type WriteTenant interface {
	// Namespace returns a [WriteNamespace] scoped to ns ("" = the whole
	// store). The returned handle is where gating happens and stamps the
	// tenant bound on this WriteTenant alongside the namespace on every
	// write it commits.
	Namespace(ns string) WriteNamespace
}

// WriteNamespace is a [WriteProtector] scoped to one namespace — where writes
// are gated (Check / RecordAsSent) and swept (ReplayCandidates). A capped Check
// is DEFERRED: the write is an action you still intend to perform, so a
// breadcrumb is stashed for re-emission once the cap window clears. A
// DecisionDeduped (identical content to the last send) is dropped, not
// replayed.
type WriteNamespace interface {
	Check(ctx context.Context, meta RequestMeta, contentHash string) Outcome
	RecordAsSent(ctx context.Context, meta RequestMeta, contentHash string) error
	ReplayCandidates(ctx context.Context, limit int) ([]DeferredRequest, error)
}
