package protect

import (
	"context"

	"github.com/homemade/pith/internal/core"
)

// The value types below are aliased from pith/internal/core — the real
// implementation lives there. Type aliases preserve identity, so
// protect.RequestMeta and core.RequestMeta are the same type. The
// ReadProtector / WriteProtector interfaces are declared here (not
// aliased) and are satisfied structurally by core's gate shells, which
// the factory subpackages return. The split exists so external code
// can't construct a gate around a caller-supplied sendstate.Store — see
// the package doc on protect.go.

// RequestMeta is the addressing primitive (target slot + replay
// breadcrumb). It is the complete request for a [ReadProtector], and the
// addressing half of a [WriteProtector] call (which adds a contentHash
// argument). See [core.RequestMeta].
type RequestMeta = core.RequestMeta

// DeferredRequest is the unit yielded by [WriteProtector.ReplayCandidates].
// See [core.DeferredRequest].
type DeferredRequest = core.DeferredRequest

// Decision is the outcome of a Check call. See [core.Decision].
type Decision = core.Decision

// The Decision constants, re-exported so callers don't import core.
// Aliased types preserve constant identity.
//
//   - DecisionProceed — perform the operation, then RecordAsSent on success.
//   - DecisionDeduped — drop; identical content to the last send (write only).
//   - DecisionDeferred — a cap fired; stashed for replay (read or write).
const (
	DecisionProceed  = core.DecisionProceed
	DecisionDeduped  = core.DecisionDeduped
	DecisionDeferred = core.DecisionDeferred
)

// Outcome reports a Check result. See [core.Outcome].
type Outcome = core.Outcome

// ReadProtector guards a content-free operation — a read, poll, inbound
// event, or fire-and-forget trigger — with one or more coalesce cap
// policies and no content dedupe. A capped Check is DEFERRED: a breadcrumb
// is stashed and [ReadProtector.ReplayCandidates] yields it so a consumer
// sweep re-takes the read against current state once the cap clears (the
// final state is never lost — a dropped cap-suppression would lose it).
// Pair it with a [pith/coalesce.NewTrailingEdgeDebounce] so a sustained
// burst collapses to a single final read after quiet. A caller wanting a
// pure poll throttle can ignore ReplayCandidates / run no sweep; deferred
// entries then simply TTL out. Construct via
// [pith/protect/memory.NewReadProtector] or
// [pith/protect/mongodb.NewReadProtector].
type ReadProtector interface {
	// Check gates a candidate read. Returns DecisionProceed or, on a
	// Coalescer cap, DecisionDeferred (a breadcrumb is stamped for the
	// replay sweep). Never DecisionDeduped. Fails open (DecisionProceed)
	// with Outcome.Err set on a backing-store read error.
	Check(ctx context.Context, meta RequestMeta) Outcome

	// RecordAsSent commits a performed read, advancing the cap counts.
	// Call only after the read succeeded.
	RecordAsSent(ctx context.Context, meta RequestMeta) error

	// ReplayCandidates collects pending deferred reads whose Coalescer caps
	// now have room, oldest deferral first; limit bounds the entries
	// examined (<= 0 means no bound). The consumer re-derives each from
	// MessageRef and re-takes the read via Check.
	//
	// Replay scope: a sweep enumerates the WHOLE store and cannot be scoped
	// to a subset of keys (TargetKey addresses Check/RecordAsSent, not the
	// sweep). So independently-replayed streams must live in SEPARATE stores
	// (a distinct Database in the Mongo factory), never one store split by
	// key prefix — otherwise one stream's sweep drains and resolves another's
	// pending entries, re-running them through the wrong consumer.
	ReplayCandidates(ctx context.Context, limit int) ([]DeferredRequest, error)
}

// WriteProtector guards a content-bearing operation — a send/merge/PATCH
// — with content dedupe plus one or more coalesce cap policies. A capped
// Check is DEFERRED: the write is an action you still intend to perform,
// so a breadcrumb is stashed and [WriteProtector.ReplayCandidates] yields
// it for re-emission once the cap window clears. Construct via
// [pith/protect/memory.NewWriteProtector] or
// [pith/protect/mongodb.NewWriteProtector].
type WriteProtector interface {
	// Check gates a candidate write. Returns DecisionProceed,
	// DecisionDeduped (contentHash identical to the last send), or
	// DecisionDeferred (a Coalescer cap fired — a breadcrumb is stamped
	// for the replay sweep). Fails open (DecisionProceed) with
	// Outcome.Err set on a backing-store read error.
	Check(ctx context.Context, meta RequestMeta, contentHash string) Outcome

	// RecordAsSent commits a successful write (TargetKey → contentHash).
	// Call only after the send succeeded — record-on-success leaves a
	// failed send unrecorded so a retry with identical content is not
	// suppressed.
	RecordAsSent(ctx context.Context, meta RequestMeta, contentHash string) error

	// ReplayCandidates collects pending deferrals whose Coalescer caps now
	// have room, oldest deferral first; limit bounds the entries examined
	// (<= 0 means no bound). The consumer re-derives each from MessageRef
	// and re-emits via Check.
	//
	// Replay scope: a sweep enumerates the WHOLE store and cannot be scoped
	// to a subset of keys (TargetKey addresses Check/RecordAsSent, not the
	// sweep). So independently-replayed streams must live in SEPARATE stores
	// (a distinct Database in the Mongo factory), never one store split by
	// key prefix — otherwise one stream's sweep drains and resolves another's
	// pending entries, re-running them through the wrong consumer.
	ReplayCandidates(ctx context.Context, limit int) ([]DeferredRequest, error)
}
