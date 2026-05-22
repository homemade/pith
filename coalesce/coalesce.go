// Package coalesce provides a per-key cap policy: "at most hardCap
// successful sends per rolling window." Used by [pith/protect] as
// the underlying mechanism for both cluster-collapse "debounce"
// (hardCap=1) and destination-side quota (hardCap=N) — same shape,
// different parameters.
//
// Coalescer is a read-only policy layer over a
// [pith/sendstate.Store]. The store owns the per-key send-timestamp
// list; coalesce answers "is key K at or above hardCap within the
// trailing window?" by calling [pith/sendstate.Store.CountInWindow].
// Writes (recording a successful send) go through the store
// directly via [pith/protect.Protector.RecordAsSent] or
// [pith/sendstate.Store.RecordAsSent].
//
// The (hardCap, window) pair is supplied once at construction and
// applied to every [Coalescer.ShouldDefer] call. It's a deployment
// setting, not a per-call decision. Multiple Coalescer instances
// can be configured over the same store, each with its own
// (hardCap, window).
package coalesce

import (
	"context"
	"time"

	"github.com/homemade/pith/sendstate"
)

// Coalescer reports whether the key is at or above its configured
// cap within the trailing window the Coalescer was constructed with.
type Coalescer interface {
	// ShouldDefer reports true when the [pith/sendstate.Store]
	// holds hardCap or more send records for key within the
	// trailing window.
	//
	//   true  → at or above cap → defer.
	//   false → below cap → proceed; caller calls
	//                       [pith/sendstate.Store.RecordAsSent] on
	//                       success.
	//
	// Backing-store failures must be treated as fail-open by
	// callers (return false), so a sendstate outage degrades to
	// "operation proceeds" rather than dropping legitimate work.
	ShouldDefer(ctx context.Context, key string) (bool, error)
}

// storeCoalescer is the default [Coalescer]: a thin policy over a
// [pith/sendstate.Store] with a fixed (hardCap, window).
type storeCoalescer struct {
	store   sendstate.Store
	hardCap int
	window  time.Duration
}

// NewCoalescer returns a Coalescer backed by the supplied
// [pith/sendstate.Store], applying (hardCap, window) on every
// ShouldDefer call. Use this constructor when the same store is
// shared with other mechanisms (e.g. [pith/dedupe] under
// [pith/protect.Protector]); each layer's policy then reads from
// one record per key.
func NewCoalescer(store sendstate.Store, hardCap int, window time.Duration) Coalescer {
	return &storeCoalescer{store: store, hardCap: hardCap, window: window}
}

// NewMemoryCoalescer returns a Coalescer backed by a private
// in-process [pith/sendstate.MemoryStore]. Convenient for
// standalone use (tests, examples, single-process deployments
// where coalesce is the only mechanism reading the store).
func NewMemoryCoalescer(hardCap int, window time.Duration) Coalescer {
	return NewCoalescer(sendstate.NewMemoryStore(), hardCap, window)
}

func (c *storeCoalescer) ShouldDefer(ctx context.Context, key string) (bool, error) {
	n, err := c.store.CountInWindow(ctx, key, c.window)
	if err != nil {
		return false, err
	}
	return n >= c.hardCap, nil
}
