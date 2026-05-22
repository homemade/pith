package protect_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/dedupe"
	"github.com/homemade/pith/protect"
	"github.com/homemade/pith/sendstate"
	"github.com/joineduptech/doc/sequencerec"
)

// Recording wrappers around each mechanism so calls from
// Protector.Check / RecordAsSent show up in the diagram as their
// outermost arrows. The inner CountInWindow reads driven by
// Coalescer.ShouldDefer are not recorded — the diagram stays at
// the mechanism-call abstraction.

type recordingDeduper struct {
	inner dedupe.Deduper
	rec   *sequencerec.Recorder
}

func (r *recordingDeduper) SeenInWindow(ctx context.Context, key, contentHash string) (bool, error) {
	seen, err := r.inner.SeenInWindow(ctx, key, contentHash)
	r.rec.Record("Dedupe", "SeenInWindow", []any{key, contentHash}, []any{seen, err})
	return seen, err
}

// recordingCap wraps a coalesce.Coalescer and tags its diagram
// participant with a caller-supplied name (e.g. "Debounce",
// "Quota") so each attached cap shows up as its own lifeline.
type recordingCap struct {
	inner coalesce.Coalescer
	rec   *sequencerec.Recorder
	name  string
}

func (r *recordingCap) ShouldDefer(ctx context.Context, key string) (bool, error) {
	d, err := r.inner.ShouldDefer(ctx, key)
	r.rec.Record(r.name, "ShouldDefer", []any{key}, []any{d, err})
	return d, err
}

// recordingSendStore wraps a [sendstate.Store] so the write surface
// used by Protector.Check / RecordAsSent surfaces in the diagram.
// CountInWindow reads driven by ShouldDefer are intentionally not
// recorded here — they'd push arrows ahead of their parent
// ShouldDefer call (sequencerec records on return); leaving them
// silent keeps the diagram at the mechanism-call abstraction.
type recordingSendStore struct {
	inner sendstate.Store
	rec   *sequencerec.Recorder
}

func (r *recordingSendStore) RecordAsSent(ctx context.Context, key, contentHash string) error {
	err := r.inner.RecordAsSent(ctx, key, contentHash)
	r.rec.Record("SendState", "RecordAsSent", []any{key, contentHash}, []any{err})
	return err
}

func (r *recordingSendStore) RecordAsDeferred(ctx context.Context, key string, messageRef []byte) error {
	err := r.inner.RecordAsDeferred(ctx, key, messageRef)
	r.rec.Record("SendState", "RecordAsDeferred", []any{key, fmt.Sprintf("<%d bytes>", len(messageRef))}, []any{err})
	return err
}

func (r *recordingSendStore) Lookup(ctx context.Context, key string) (sendstate.Entry, bool, error) {
	return r.inner.Lookup(ctx, key)
}

func (r *recordingSendStore) CountInWindow(ctx context.Context, key string, window time.Duration) (int, error) {
	return r.inner.CountInWindow(ctx, key, window)
}

func (r *recordingSendStore) Metrics(ctx context.Context, key string) (sendstate.Metrics, bool, error) {
	return r.inner.Metrics(ctx, key)
}

// TestProtectorScenarios exercises each [protect.Protector.Check]
// decision branch and emits a Mermaid sequence diagram next to this
// file (sequence_test.md).
func TestProtectorScenarios(t *testing.T) {
	rec := sequencerec.New()
	ctx := context.Background()

	const capWindow = 24 * time.Hour
	// Short debounce window so the at-cap scenario below can wait
	// it out and exercise the quota Coalescer's ShouldDefer arrow
	// distinctly. Realistic deployments use seconds-to-minutes.
	const debounceWindow = 30 * time.Millisecond
	const hardCap = 2

	innerStore := sendstate.NewMemoryStore()
	recStore := &recordingSendStore{inner: innerStore, rec: rec}
	innerDedupe := dedupe.NewDeduper(recStore, capWindow)
	innerDebounce := coalesce.NewCoalescer(recStore, 1, debounceWindow)
	innerQuota := coalesce.NewCoalescer(recStore, hardCap, capWindow)

	// Wire debounce *before* quota so the diagram evaluates the
	// short-window cap first (cheaper / source-driven check).
	p := protect.New(
		protect.WithSendStore(recStore),
		// Dedupe needs the cap window to be configured; supply it
		// without attaching the default quota Coalescer (we attach a
		// wrapped one below via WithCoalescerImpl). Using WithCap
		// here would double-attach.
		protect.WithDeduperImpl(&recordingDeduper{inner: innerDedupe, rec: rec}),
		protect.WithCoalescerImpl(
			&recordingCap{inner: innerDebounce, rec: rec, name: "Debounce"},
			"debounce window",
		),
		protect.WithCoalescerImpl(
			&recordingCap{inner: innerQuota, rec: rec, name: "Quota"},
			"at cap",
		),
	)

	rec.Run(t, "first send proceeds and is recorded", func(t *testing.T) {
		req := protect.Request{
			ContentHash: "hash-A",
			TargetKey:   "act-1:contact-1",
			MessageRef:  []byte("activity-A"),
		}
		rec.Note("Check(content=hash-A, target=act-1:contact-1)")
		out, err := p.Check(ctx, req)
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if out.Decision != protect.DecisionProceed {
			t.Fatalf("want Proceed, got %s", out.Decision)
		}
		rec.Note(fmt.Sprintf("→ %s", out.Decision))

		rec.Note("send to downstream succeeded → RecordAsSent")
		if err := p.RecordAsSent(ctx, req); err != nil {
			t.Fatalf("RecordAsSent: %v", err)
		}
	})

	rec.Run(t, "duplicate content to the same target is deferred", func(t *testing.T) {
		req := protect.Request{
			ContentHash: "hash-A",           // same content as scenario 1
			TargetKey:   "act-1:contact-1", // same target as scenario 1
			MessageRef:  []byte("activity-A-dup"),
		}
		rec.Note("Check(content=hash-A, target=act-1:contact-1)")
		out, err := p.Check(ctx, req)
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if out.Decision != protect.DecisionDeferred {
			t.Fatalf("want Deferred, got %s", out.Decision)
		}
		rec.Note(fmt.Sprintf("→ %s (%s)", out.Decision, out.Reason))
	})

	rec.Run(t, "same-target follow-up within debounce window is deferred", func(t *testing.T) {
		req := protect.Request{
			ContentHash: "hash-B",           // new content
			TargetKey:   "act-1:contact-1", // same target as scenario 1
			MessageRef:  []byte("activity-B"),
		}
		rec.Note("Check(content=hash-B, target=act-1:contact-1) — 1 send within debounce window")
		out, err := p.Check(ctx, req)
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if out.Decision != protect.DecisionDeferred {
			t.Fatalf("want Deferred, got %s", out.Decision)
		}
		if out.Reason != "debounce window" {
			t.Fatalf("want Reason=\"debounce window\", got %q", out.Reason)
		}
		rec.Note(fmt.Sprintf("→ %s (%s) — deferred breadcrumb stamped", out.Decision, out.Reason))
	})

	rec.Run(t, "deferred at quota cap with breadcrumb stamped", func(t *testing.T) {
		// Pre-populate the quota counter for contact-3 directly on
		// the raw store so the setup steps don't show in the
		// diagram. Then sleep long enough that the setup sends fall
		// outside the debounce window — the debounce ShouldDefer
		// will return false (no recent send), so the diagram shows
		// debounce checking and clearing before quota trips.
		_ = innerStore.RecordAsSent(ctx, "act-1:contact-3", "setup-1")
		_ = innerStore.RecordAsSent(ctx, "act-1:contact-3", "setup-2")
		time.Sleep(2 * debounceWindow)

		req := protect.Request{
			ContentHash: "hash-C",
			TargetKey:   "act-1:contact-3",
			MessageRef:  []byte("activity-C"),
		}
		rec.Note("Check(content=hash-C, target=act-1:contact-3) — quota at hardCap=2, debounce window expired")
		out, err := p.Check(ctx, req)
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if out.Decision != protect.DecisionDeferred {
			t.Fatalf("want Deferred, got %s", out.Decision)
		}
		if out.Reason != "at cap" {
			t.Fatalf("want Reason=\"at cap\", got %q", out.Reason)
		}
		rec.Note(fmt.Sprintf("→ %s (%s) — deferred breadcrumb stamped", out.Decision, out.Reason))
	})

	rec.Run(t, "below-cap send proceeds; RecordAsSent appends to sendstate", func(t *testing.T) {
		req := protect.Request{
			ContentHash: "hash-D",
			TargetKey:   "act-1:contact-4",
			MessageRef:  []byte("activity-D"),
		}
		rec.Note("Check(content=hash-D, target=act-1:contact-4) — counts start at 0")
		out, err := p.Check(ctx, req)
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if out.Decision != protect.DecisionProceed {
			t.Fatalf("want Proceed, got %s", out.Decision)
		}
		rec.Note(fmt.Sprintf("→ %s", out.Decision))
		if err := p.RecordAsSent(ctx, req); err != nil {
			t.Fatalf("RecordAsSent: %v", err)
		}
	})

	t.Cleanup(func() { rec.WriteMermaid(t) })
}
