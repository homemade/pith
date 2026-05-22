// Package dedupe provides short-window suppression of repeated
// operations: for a given key, the same content sent again within
// the window is reported as suppressed.
//
// dedupe is a read-only policy layer over a [sendstate.Store]. The
// store owns the "(key → most-recent content-hash, last-sent-at)"
// record; dedupe answers "is contentHash X for key K within the
// trailing window?" by consulting the store. Writes — recording a
// successful send — go through the store directly (typically via
// [pith/protect.Protector.RecordAsSent] or [sendstate.Store.RecordAsSent]
// for standalone use).
//
// The window is supplied once at construction (see [NewDeduper]) and
// applied to every [Deduper.SeenInWindow] call. It's a deployment
// setting, not a per-call decision; callers that need different
// windows over the same store should construct multiple Deduper
// instances.
//
// Common patterns for (key, contentHash):
//
//   - Per-target dedupe — key = "{resource}:{target}", contentHash =
//     hash of the message. Same content for the same target is
//     suppressed; the same content for a different target proceeds.
//   - Cross-target dedupe — key = "" or a fixed scope, contentHash =
//     hash of the message. Same content anywhere within the scope
//     is suppressed.
//   - Pure equality — key = the content itself, contentHash = "".
//     The classic "have I seen this token before" pattern.
//
// See [Example_contentHashKey].
package dedupe

import (
	"context"
	"time"

	"github.com/homemade/pith/sendstate"
)

// Deduper reports whether a recent send for the given key carried
// the same content-hash as the supplied one, within the trailing
// window the Deduper was constructed with.
type Deduper interface {
	// SeenInWindow reports true when the [sendstate.Store] holds a
	// record for key whose ContentHash equals the supplied
	// contentHash AND whose LastSentAt is within the Deduper's
	// trailing window.
	//
	//   true  → recent match → suppress.
	//   false → no record, outside window, or different content →
	//                    proceed and the caller records on success.
	//
	// Backing-store failures must be treated as fail-open by callers
	// (return false), so a sendstate outage degrades to "operation
	// proceeds" rather than dropping legitimate work.
	SeenInWindow(ctx context.Context, key, contentHash string) (bool, error)
}

// storeDeduper is the default [Deduper] implementation: a thin
// policy over a [sendstate.Store] with a fixed window.
type storeDeduper struct {
	store  sendstate.Store
	window time.Duration
}

// NewDeduper returns a Deduper backed by the supplied
// [sendstate.Store], applying window on every SeenInWindow call.
// Use this constructor when the same store is shared with other
// mechanisms (e.g. [pith/coalesce] under [pith/protect.Protector]);
// each layer's policy then reads from one record per key.
func NewDeduper(store sendstate.Store, window time.Duration) Deduper {
	return &storeDeduper{store: store, window: window}
}

// NewMemoryDeduper returns a Deduper backed by a private in-process
// [sendstate.MemoryStore] with the supplied window. Convenient for
// standalone use (tests, examples, single-process deployments where
// dedupe is the only mechanism reading the store).
func NewMemoryDeduper(window time.Duration) Deduper {
	return NewDeduper(sendstate.NewMemoryStore(), window)
}

func (d *storeDeduper) SeenInWindow(ctx context.Context, key, contentHash string) (bool, error) {
	e, ok, err := d.store.Lookup(ctx, key)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if !time.Now().Before(e.LastSentAt.Add(d.window)) {
		return false, nil
	}
	return e.ContentHash == contentHash, nil
}
