// Package coalesce provides a per-key cap policy: "at most hardCap
// successful sends per rolling window." Used by [pith/protect] as
// the underlying mechanism for both "debounce" (hardCap=1 — a
// leading-edge throttle: send the first, then enforce a minimum
// spacing of window between sends) and destination-side quota
// (hardCap=N) — same shape, different parameters.
//
// Coalescer is a read-only policy: a pure function over a
// pre-fetched [pith/sendstate.Entry]. It answers "does this entry
// hold hardCap or more successful sends within the trailing window?"
// The caller owns the entry read — typically one shared read fed to
// dedupe and every attached Coalescer (see
// [pith/protect.Protector.Check]) — and writes (recording a
// successful send) go through [pith/sendstate.Store.RecordAsSent].
//
// The (hardCap, window) pair is supplied once at construction and
// applied to every read. It's a deployment setting, not a per-call
// decision. Multiple Coalescer instances can be configured, each
// with its own (hardCap, window).
package coalesce

import (
	"time"

	"github.com/homemade/pith/sendstate"
)

// Coalescer reports whether an entry is at or above its configured
// cap within the trailing window the Coalescer was constructed with.
type Coalescer interface {
	// ShouldDefer returns true when entry holds hardCap or more send
	// timestamps within the Coalescer's trailing window ending at now:
	//
	//   true  → at or above cap → defer.
	//   false → below cap → proceed; caller calls
	//                       [pith/sendstate.Store.RecordAsSent] on
	//                       success.
	//
	// now is the caller's reference time, shared across every policy
	// in a single Check. The zero Entry (a miss or expired TTL) has an
	// empty timestamp list, so the count is zero and the result is
	// false. Pure function; no I/O.
	ShouldDefer(entry sendstate.Entry, now time.Time) bool

	// CapPolicy returns the (hardCap, window) this Coalescer applies
	// on every read: it defers once a key reaches hardCap successful
	// sends within the trailing window. Exposed so a composition
	// layer (e.g. [pith/protect]) can size the shared store to hold
	// enough send timestamps for the largest attached cap — see
	// [pith/sendstate.MemoryStore.MaxSendTimes]. Custom
	// implementations must report the bounds they actually enforce.
	CapPolicy() (hardCap int, window time.Duration)
}

// coalescer is the default [Coalescer]: a pure (hardCap, window)
// policy evaluated against a [pith/sendstate.Entry].
type coalescer struct {
	hardCap int
	window  time.Duration
}

// NewCoalescer returns a Coalescer applying (hardCap, window) to
// every [Coalescer.ShouldDefer] call. The caller supplies the entry
// and reference time per read (typically one shared read across all
// mechanisms — see [pith/protect]).
func NewCoalescer(hardCap int, window time.Duration) Coalescer {
	return &coalescer{hardCap: hardCap, window: window}
}

func (c *coalescer) ShouldDefer(entry sendstate.Entry, now time.Time) bool {
	return entry.CountInWindow(now, c.window) >= c.hardCap
}

func (c *coalescer) CapPolicy() (hardCap int, window time.Duration) {
	return c.hardCap, c.window
}
