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
// The (hardCap, window) pair is supplied once at construction —
// [NewLeadingEdgeDebounce] for a debounce, [NewQuota] for a quota —
// and applied to every read. It's a deployment setting, not a
// per-call decision. Each Coalescer derives a name from its bounds
// (reported via [Coalescer.CapPolicy]) that identifies the cap to a
// composition layer; quotas with distinct bounds therefore carry
// distinct names without a caller-supplied label.
package coalesce

import (
	"fmt"
	"strings"
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

	// CapPolicy returns the name and (hardCap, window) this Coalescer
	// applies on every read: it defers once a key reaches hardCap
	// successful sends within the trailing window. Exposed so a
	// composition layer (e.g. [pith/protect]) can identify which cap
	// fired and size the shared store to hold enough send timestamps
	// for the largest attached cap — see
	// [pith/sendstate.MemoryStore.MaxSendTimes]. Custom
	// implementations must report a name and the bounds they actually
	// enforce.
	CapPolicy() (name string, hardCap int, window time.Duration)
}

// coalescer is the default [Coalescer]: a pure (hardCap, window)
// policy evaluated against a [pith/sendstate.Entry].
type coalescer struct {
	name    string
	hardCap int
	window  time.Duration
}

// NewLeadingEdgeDebounce returns a leading-edge debounce Coalescer:
// hardCap=1 over window, so the first send proceeds and further sends
// are deferred until window elapses. Its name is derived from window
// (e.g. "leading-edge debounce 30ms") and reported via
// [Coalescer.CapPolicy] — a composition layer surfaces it to identify
// which cap fired. The window alone makes the name unique among
// debounces; see [pith/protect] for the cross-cap uniqueness check.
func NewLeadingEdgeDebounce(window time.Duration) Coalescer {
	return &coalescer{
		name:    fmt.Sprintf("leading-edge debounce %s", humanizeDuration(window)),
		hardCap: 1,
		window:  window,
	}
}

// NewQuota returns a quota Coalescer: it defers once a key reaches
// hardCap successful sends within the trailing window. Its name is
// derived from (hardCap, window) (e.g. "quota cap 100 per 24h") and
// reported via [Coalescer.CapPolicy], so layered quotas with distinct
// bounds (e.g. a burst quota alongside a daily quota) carry distinct
// names without the caller supplying one. Quotas with identical bounds
// derive the same name; a composition layer that attaches several must
// keep them unique (see [pith/protect]).
func NewQuota(hardCap int, window time.Duration) Coalescer {
	return &coalescer{
		name:    fmt.Sprintf("quota cap %d per %s", hardCap, humanizeDuration(window)),
		hardCap: hardCap,
		window:  window,
	}
}

// humanizeDuration renders d without the trailing zero components that
// [time.Duration.String] emits for whole hours and minutes ("24h0m0s"
// → "24h", "1m0s" → "1m", "1h0m30s" → "1h30s"), while leaving
// sub-second and fractional durations to Duration.String, which
// already renders them minimally ("30ms", "1.5s"). Used to build
// readable, still-unique Coalescer names from their bounds.
func humanizeDuration(d time.Duration) string {
	if d < time.Second {
		return d.String()
	}
	var b strings.Builder
	if h := d / time.Hour; h > 0 {
		fmt.Fprintf(&b, "%dh", h)
		d -= h * time.Hour
	}
	if m := d / time.Minute; m > 0 {
		fmt.Fprintf(&b, "%dm", m)
		d -= m * time.Minute
	}
	if d > 0 {
		// Remaining sub-minute part, including any fractional seconds.
		b.WriteString(d.String())
	}
	return b.String()
}

func (c *coalescer) ShouldDefer(entry sendstate.Entry, now time.Time) bool {
	return entry.CountSentInWindow(now, c.window) >= c.hardCap
}

func (c *coalescer) CapPolicy() (name string, hardCap int, window time.Duration) {
	return c.name, c.hardCap, c.window
}
