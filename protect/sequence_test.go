package protect_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/protect"
	"github.com/homemade/pith/sendstate"
	"github.com/joineduptech/doc/sequencerec"
)

// Recording wrappers around each mechanism so calls from
// Protector.Check / RecordAsSent show up in the diagram as their
// outermost arrows. Protector.Check fetches one Entry up-front and
// feeds it to every policy — the diagram shows that as a single
// ReadEntry arrow followed by pure-function policy arrows that don't
// go back to the store.

// recordingCap wraps a coalesce.Coalescer and tags its diagram
// participant with a caller-supplied name (e.g. "Debounce",
// "Quota") so each attached cap shows up as its own lifeline.
type recordingCap struct {
	inner coalesce.Coalescer
	rec   *sequencerec.Recorder
	name  string
}

func (r *recordingCap) ShouldDefer(entry sendstate.Entry, now time.Time) bool {
	d := r.inner.ShouldDefer(entry, now)
	r.rec.Record(r.name, "ShouldDefer", nil, []any{d})
	return d
}

func (r *recordingCap) CapPolicy() (hardCap int, window time.Duration) {
	return r.inner.CapPolicy()
}

// recordingSendStore wraps a [sendstate.Store] so the read + write
// surface used by Protector.Check / RecordAsSent surfaces in the
// diagram. ReadEntry is recorded explicitly — it's the single read
// per Check that drives every downstream policy.
type recordingSendStore struct {
	inner sendstate.Store
	rec   *sequencerec.Recorder
}

func (r *recordingSendStore) ReadEntry(ctx context.Context, key string) (sendstate.Entry, error) {
	entry, err := r.inner.ReadEntry(ctx, key)
	r.rec.Record("SendState", "ReadEntry", []any{key}, []any{"entry", err})
	return entry, err
}

func (r *recordingSendStore) ReadMetrics(ctx context.Context, key string) (sendstate.Metrics, bool, error) {
	return r.inner.ReadMetrics(ctx, key)
}

func (r *recordingSendStore) RaisePeaks(ctx context.Context, key string, counts map[string]uint64) error {
	err := r.inner.RaisePeaks(ctx, key, counts)
	r.rec.Record("SendState", "RaisePeaks", []any{key, fmt.Sprintf("%v", counts)}, []any{err})
	return err
}

func (r *recordingSendStore) RangeDeferred(ctx context.Context, limit int, fn func(key string, e sendstate.Entry) bool) error {
	return r.inner.RangeDeferred(ctx, limit, fn)
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

// TestProtectorScenarios exercises each [protect.Protector.Check]
// decision branch and emits a Mermaid sequence diagram next to this
// file (sequence_test.md).
func TestProtectorScenarios(t *testing.T) {
	rec := sequencerec.New()
	ctx := context.Background()

	const capWindow = 24 * time.Hour
	// Short debounce window so the at-cap scenario below can wait
	// it out and exercise the quota Coalescer's ShouldDefer
	// arrow distinctly. Realistic deployments use seconds-to-minutes.
	const debounceWindow = 30 * time.Millisecond
	const hardCap = 2

	innerStore := sendstate.NewMemoryStore(capWindow)
	recStore := &recordingSendStore{inner: innerStore, rec: rec}
	innerDebounce := coalesce.NewCoalescer(1, debounceWindow)
	innerQuota := coalesce.NewCoalescer(hardCap, capWindow)

	// Content dedupe is always applied via sendstate.Entry.Seen
	// (no Coalescer to wrap), so it shows in the diagram only as the
	// Check outcome, not a separate participant. Wire debounce
	// *before* quota so the diagram evaluates the short-window cap
	// first (cheaper / source-driven check).
	p := protect.New(
		protect.WithSendStore(recStore),
		protect.WithCoalescerImpl(
			&recordingCap{inner: innerDebounce, rec: rec, name: "Debounce"},
			"leading-edge debounce window",
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
			ContentHash: "hash-A",          // same content as scenario 1
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
			ContentHash: "hash-B",          // new content
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
		if out.Reason != "leading-edge debounce window" {
			t.Fatalf("want Reason=\"leading-edge debounce window\", got %q", out.Reason)
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
