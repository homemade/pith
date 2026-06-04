// Package memory wires a [pith/protect.ReadProtector] /
// [pith/protect.WriteProtector] to the in-process
// [pith/sendstate/memory.Store] backend.
//
// Provided for symmetry with [pith/protect/mongodb] — the memory backend
// already auto-sizes itself through the core constructor's structural
// GrowMaxSendTimes check, so these factories are one-line wrappers.
// Importing them (instead of constructing core directly, which isn't
// possible from outside the pith module anyway) lets callers use a
// single, consistent "construct a protector for backend X" pattern.
//
// Both constructors require at least one Coalescer (the (first, rest...)
// shape). entryTTL is required and must be >= the largest attached
// Coalescer window; the core constructor validates that.
package memory

import (
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/internal/core"
	"github.com/homemade/pith/protect"
	"github.com/homemade/pith/sendstate/memory"
)

// NewReadProtector returns a [pith/protect.ReadProtector] backed by the
// in-process memory store: coalesce caps only, capped Checks DROP.
func NewReadProtector(entryTTL time.Duration, first coalesce.Coalescer, rest ...coalesce.Coalescer) protect.ReadProtector {
	return core.NewRead(memory.New(entryTTL), append([]coalesce.Coalescer{first}, rest...)...)
}

// NewWriteProtector returns a [pith/protect.WriteProtector] backed by the
// in-process memory store: content dedupe + coalesce caps, capped Checks
// DEFER (replayable).
func NewWriteProtector(entryTTL time.Duration, first coalesce.Coalescer, rest ...coalesce.Coalescer) protect.WriteProtector {
	return core.NewWrite(memory.New(entryTTL), append([]coalesce.Coalescer{first}, rest...)...)
}
