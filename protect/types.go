package protect

import (
	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/internal/core"
)

// Protector, Request, Outcome, etc. are aliased from pith/internal/core
// — the real implementation lives there. Type aliases preserve
// identity, so *protect.Protector and *core.Protector are the same
// type at the language level; callers receive *protect.Protector from
// the factory subpackages and can use it normally. The split exists
// so external code can't construct a Protector around a caller-supplied
// sendstate.Store — see the package doc on protect.go.

// Protector composes pith's integration-guard mechanisms. See
// [core.Protector] for the operation set.
type Protector = core.Protector

// RequestMeta is the addressing half of a [Request]. See
// [core.RequestMeta].
type RequestMeta = core.RequestMeta

// Request is the input to [Protector.Check] / [Protector.RecordAsSent].
// See [core.Request].
type Request = core.Request

// DeferredRequest is the unit yielded by [Protector.ReplayCandidates].
// See [core.DeferredRequest].
type DeferredRequest = core.DeferredRequest

// Decision is the outcome of a [Protector.Check] call. See
// [core.Decision].
type Decision = core.Decision

// DecisionProceed / DecisionDeduped / DecisionDeferred re-export the
// constants from core so callers don't need to import core. Aliased
// types preserve constant identity.
const (
	DecisionProceed  = core.DecisionProceed
	DecisionDeduped  = core.DecisionDeduped
	DecisionDeferred = core.DecisionDeferred
)

// Outcome reports the [Protector.Check] result. See [core.Outcome].
type Outcome = core.Outcome

// Option configures a Protector. See [core.Option]. The factory
// subpackages accept []Option in their New signatures.
type Option = core.Option

// WithCoalescer attaches a cap [coalesce.Coalescer]. See
// [core.WithCoalescer].
func WithCoalescer(c coalesce.Coalescer) Option {
	return core.WithCoalescer(c)
}

// Inspect applies opts to a throwaway config and returns the
// Coalescers that would be attached. See [core.Inspect].
func Inspect(opts ...Option) []coalesce.Coalescer {
	return core.Inspect(opts...)
}
