package core

import "github.com/homemade/pith/coalesce"

// Inspect applies opts to a throwaway config and returns the Coalescers
// that would be attached to a Protector built with the same opts.
//
// Provided so backend wrapper packages (pith/protect/mongodb,
// pith/protect/memory) can introspect cap policies for sizing decisions
// without first constructing a Protector — e.g. the mongo wrapper
// derives MaxSendTimes from the largest attached hardCap so callers
// can't forget the storage-side bound. The returned slice is a copy
// (mutating it doesn't affect anything).
//
// Order matches Check's evaluation order (opts applied left-to-right).
// Returns nil when opts attaches no Coalescers.
func Inspect(opts ...Option) []coalesce.Coalescer {
	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}
	if len(cfg.coalescers) == 0 {
		return nil
	}
	out := make([]coalesce.Coalescer, len(cfg.coalescers))
	copy(out, cfg.coalescers)
	return out
}
