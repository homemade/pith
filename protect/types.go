package protect

import (
	"github.com/homemade/pith/internal/core"
)

// The types below are aliased from pith/internal/core — the real
// implementation lives there. Type aliases preserve identity, so
// protect.RequestMeta and core.RequestMeta are the same type. The gate
// interfaces (ReadProtector / ReadNamespace / WriteProtector / WriteNamespace)
// are aliased too: aliasing (not re-declaring) keeps them identical to the core
// types, which is what lets core's concrete ReadGate.Namespace return a
// ReadNamespace and satisfy the public surface (Go requires identical, not
// merely structurally-equal, return types for interface satisfaction). The
// factory subpackages return core's gate shells. The internal/ split exists so
// external code can't construct a gate around a caller-supplied sendstate.Store
// — see the package doc on protect.go.

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

// ReadProtector is the base read-side gate for content-free operations — a
// read, poll, inbound event, or fire-and-forget trigger — with one or more
// coalesce cap policies and no content dedupe. It is a factory only: pick a
// namespace with [ReadProtector.Namespace] to get a [ReadNamespace], where the
// gating happens (like selecting a Mongo collection off a database). "" is the
// whole-store namespace — the single entry point even when you don't scope.
// Construct via [pith/protect/memory.NewReadProtector] or
// [pith/protect/mongodb.NewReadProtector]. See [core.ReadProtector].
type ReadProtector = core.ReadProtector

// ReadNamespace is a [ReadProtector] scoped to one namespace — where reads are
// gated (Check / RecordAsSent) and swept (ReplayCandidates, within the
// namespace). A capped Check is DEFERRED: a breadcrumb is stashed and a consumer
// sweep re-takes the read against current state once the cap clears (the final
// state is never lost — a dropped cap-suppression would lose it). Pair it with a
// [pith/coalesce.NewTrailingEdgeDebounce] so a sustained burst collapses to a
// single final read after quiet. See [core.ReadNamespace].
type ReadNamespace = core.ReadNamespace

// WriteProtector is the base write-side gate for content-bearing operations — a
// send/merge/PATCH — with content dedupe plus one or more coalesce cap policies.
// Like [ReadProtector] it is a factory only: pick a namespace with
// [WriteProtector.Namespace] to get a [WriteNamespace]. Construct via
// [pith/protect/memory.NewWriteProtector] or [pith/protect/mongodb.NewWriteProtector].
// See [core.WriteProtector].
type WriteProtector = core.WriteProtector

// WriteNamespace is a [WriteProtector] scoped to one namespace — where writes
// are gated (Check / RecordAsSent) and swept (ReplayCandidates). A capped Check
// is DEFERRED: the write is an action you still intend to perform, so a
// breadcrumb is stashed for re-emission once the cap window clears. A
// DecisionDeduped (identical content to the last send) is dropped, not
// replayed. See [core.WriteNamespace].
type WriteNamespace = core.WriteNamespace
