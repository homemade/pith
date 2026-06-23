// Package coalesce defines the per-key cap policy types: a [Coalescer]
// is "at most HardCap successful sends per rolling Window" plus a defer
// [Strategy] that selects how the cap is interpreted.
//
// Three [Strategy] values cover every shape pith ships: leading-edge
// debounce (HardCap=1, first proceeds then defer for the window),
// destination-side quota (HardCap=N over Window), and trailing-edge
// debounce (defer while any recent send OR deferral). The last one
// additionally consults the deferral cadence
// ([sendstate.Entry.CountDeferredInWindow]), so a sustained burst stays
// deferred until quiet and then re-fires once — meaningful only on a
// gate that stamps deferrals and is swept (see [pith/protect.ReadNamespace]).
//
// Coalescer is pure value-data: storage backends own the actual
// evaluation. [Coalescer.ShouldDefer] is the policy logic in Go (used by
// the in-process backend); the Mongo backend translates the same
// policy into a `findOneAndUpdate` aggregation expression internally
// (see pith/sendstate/mongodb). The (HardCap, Window, Strategy) triple
// is fully sufficient for both: this package needs no knowledge of
// either backend.
//
// Each Coalescer derives a name from its Strategy + bounds (see [Coalescer.Name])
// that identifies the cap to a composition layer; quotas with distinct
// bounds therefore carry distinct names without a caller-supplied label.
package coalesce

import (
	"fmt"
	"strings"
	"time"
)

// Activity is the per-key view a [Coalescer] reads from to decide.
// Implementations supply the trailing-window counts the policy switches
// over; the in-process and Mongo backends both surface this view from
// their respective state. [pith/sendstate.Entry] satisfies it
// structurally — this package keeps zero imports of any pith package
// so the policy layer is fully backend-agnostic.
type Activity interface {
	// CountSentInWindow returns the number of recorded sends in the
	// trailing window ending at now.
	CountSentInWindow(now time.Time, window time.Duration) int

	// CountDeferredInWindow returns the number of recorded deferrals
	// in the trailing window ending at now (used by the trailing-edge
	// debounce strategy; ignored by leading-edge and quota).
	CountDeferredInWindow(now time.Time, window time.Duration) int
}

// Strategy selects how a [Coalescer]'s (HardCap, Window) policy is
// interpreted. The three valid values map to the three policy shapes
// pith ships; storage backends switch on Strategy to evaluate.
type Strategy int

const (
	// StrategyInvalid is the zero value, reserved as a sentinel so a
	// zero-value [Coalescer] (forgotten constructor call) is detectable
	// — every Strategy-switching method panics on this value with a
	// pointer back to the package's New* constructors. Not a legal
	// Strategy for any constructed Coalescer.
	StrategyInvalid Strategy = iota

	// StrategyLeadingEdge defers once the trailing-window send count
	// reaches HardCap — the first HardCap sends in any Window proceed,
	// subsequent ones defer until the window slides. HardCap=1 yields
	// a classical leading-edge debounce.
	StrategyLeadingEdge

	// StrategyQuota has the same Go-side and Mongo-side evaluation as
	// [StrategyLeadingEdge] (count vs HardCap over Window). It exists as
	// a distinct Strategy so [Coalescer.Name] can return a
	// quota-flavoured string for observability — "quota cap 50 per 24h"
	// reads differently from "leading-edge debounce 24h" even though
	// the cap arithmetic is identical.
	StrategyQuota

	// StrategyTrailingEdge defers while the key has any send OR any
	// deferral within Window. Paired with a gate that stamps deferrals
	// and a consumer-side replay sweep, this yields a leading fire
	// (the first event proceeds) plus a single trailing fire (the
	// deferred burst re-emits once after quiet, reading the final
	// state), collapsing everything between. HardCap is conventionally 1
	// (the trailing-edge variant doesn't honour a higher cap — it gates
	// on activity presence, not count).
	StrategyTrailingEdge
)

// Coalescer is a per-key cap policy: "defer when this Strategy's rule
// fires over (HardCap, Window)". A pure value type — storage backends
// own evaluation. Construct via one of the package's New* constructors;
// the zero value is not a meaningful policy.
type Coalescer struct {
	Strategy Strategy
	HardCap  int
	Window   time.Duration
}

// Name returns the derived deferral reason string used by the protector
// layer as [Outcome.Reason] on a Deferred outcome. Pure function of
// (Strategy, HardCap, Window); two Coalescers with identical fields
// produce identical names, so a composition layer attaching multiple
// caps must keep their bounds distinct to keep names unique. Panics on
// an unknown Strategy — every [Coalescer] should originate from one of
// the package's New* constructors, which is enforced at the call site
// by every method that switches on Strategy.
func (c Coalescer) Name() string {
	switch c.Strategy {
	case StrategyLeadingEdge:
		return fmt.Sprintf("leading-edge debounce %s", humanizeDuration(c.Window))
	case StrategyQuota:
		return fmt.Sprintf("quota cap %d per %s", c.HardCap, humanizeDuration(c.Window))
	case StrategyTrailingEdge:
		return fmt.Sprintf("trailing-edge debounce %s", humanizeDuration(c.Window))
	default:
		panic(fmt.Sprintf("coalesce: unknown Strategy %d — construct Coalescers via the package's New* constructors", c.Strategy))
	}
}

// ShouldDefer reports whether this Coalescer would defer for activity
// at now. Pure; no I/O. The zero [Activity] (a miss or expired TTL —
// both counts return zero) yields false. Used by the in-process backend
// ([pith/sendstate/memory]); the Mongo backend evaluates the same
// policy as a server-side aggregation expression inside its
// [Store.CheckAndReserve] pipeline. Panics on an unknown Strategy
// (see [Coalescer.Name]).
func (c Coalescer) ShouldDefer(activity Activity, now time.Time) bool {
	switch c.Strategy {
	case StrategyLeadingEdge, StrategyQuota:
		return activity.CountSentInWindow(now, c.Window) >= c.HardCap
	case StrategyTrailingEdge:
		return activity.CountSentInWindow(now, c.Window) > 0 ||
			activity.CountDeferredInWindow(now, c.Window) > 0
	default:
		panic(fmt.Sprintf("coalesce: unknown Strategy %d — construct Coalescers via the package's New* constructors", c.Strategy))
	}
}

// NewLeadingEdgeDebounce returns a leading-edge debounce Coalescer
// (HardCap=1 over window): the first send proceeds and further sends
// are deferred until window elapses. Its name is derived from window
// (e.g. "leading-edge debounce 30ms"); the window alone makes the name
// unique among debounces.
//
// Prefer it on a write gate: its content dedupe trims the repeated
// payloads a sustained burst would otherwise re-emit once per window,
// and the throttled change-history it leaves is useful there. On a
// content-free read gate, use [NewTrailingEdgeDebounce] — with no dedupe
// to trim the repeats, a leading-edge debounce would re-fetch every
// window through a flood.
func NewLeadingEdgeDebounce(window time.Duration) Coalescer {
	return Coalescer{Strategy: StrategyLeadingEdge, HardCap: 1, Window: window}
}

// NewQuota returns a quota Coalescer: it defers once a key reaches
// hardCap successful sends within the trailing window. Its name is
// derived from (hardCap, window) (e.g. "quota cap 100 per 24h"), so
// layered quotas with distinct bounds (e.g. a burst quota alongside a
// daily quota) carry distinct names without the caller supplying one.
// Quotas with identical bounds derive the same name; a composition
// layer that attaches several must keep them unique (see [pith/protect]).
func NewQuota(hardCap int, window time.Duration) Coalescer {
	return Coalescer{Strategy: StrategyQuota, HardCap: hardCap, Window: window}
}

// NewTrailingEdgeDebounce returns a leading+trailing debounce Coalescer
// over window. It defers while the key has been active — a recent send
// OR a recent deferral — within the window, and clears once it's been
// quiet for window. Paired with a gate that stamps deferrals and a
// consumer-side replay sweep, that yields a leading fire (the first
// event proceeds) plus a single trailing fire (the deferred burst
// re-emits once after quiet, reading the final state), collapsing
// everything between. Its name (e.g. "trailing-edge debounce 20s") is
// returned via [Coalescer.Name].
//
// Prefer it on a content-free read gate, which has no dedupe to trim
// repeats: a leading-edge debounce there would re-fetch roughly once
// per window through a sustained burst, whereas trailing-edge collapses
// the burst to a single final read after quiet. For a write gate, use
// [NewLeadingEdgeDebounce] — its dedupe trims the repeated payloads
// and the throttled change-history is useful there.
func NewTrailingEdgeDebounce(window time.Duration) Coalescer {
	return Coalescer{Strategy: StrategyTrailingEdge, HardCap: 1, Window: window}
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
		b.WriteString(d.String())
	}
	return b.String()
}
