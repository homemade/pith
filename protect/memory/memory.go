// Package memory wires a [pith/protect.Protector] to the in-process
// [pith/sendstate/memory.Store] backend.
//
// Provided for symmetry with [pith/protect/mongodb.New] — the
// memory backend already auto-sizes itself through the core
// constructor's structural GrowMaxSendTimes check, so this factory is
// a one-line wrapper. Importing it (instead of constructing core
// directly, which isn't possible from outside the pith module
// anyway) lets callers use a single, consistent "construct a Protector
// for backend X" pattern across backends.
package memory

import (
	"time"

	"github.com/homemade/pith/internal/core"
	"github.com/homemade/pith/protect"
	"github.com/homemade/pith/sendstate/memory"
)

// New returns a Protector backed by the in-process memory
// store with the given entry TTL and any Coalescer-attaching options.
//
// entryTTL is required and must be >= the largest attached Coalescer
// window; the core constructor validates that.
func New(entryTTL time.Duration, opts ...protect.Option) *protect.Protector {
	return core.New(memory.New(entryTTL), opts...)
}
