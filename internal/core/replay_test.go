package core_test

import (
	"context"
	"testing"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/internal/core"
	"github.com/homemade/pith/sendstate/memory"
)

// ReplayCandidates skips a pending entry while its cap window is still
// open, then returns it once the window elapses.
func TestWriteGate_ReplayCandidates(t *testing.T) {
	ctx := context.Background()
	const debounce = 30 * time.Millisecond
	w := core.NewWrite(
		memory.New(time.Hour),
		coalesce.NewLeadingEdgeDebounce(debounce),
	)
	meta := core.RequestMeta{TargetKey: "k1", MessageRef: []byte("ref")}

	// First send proceeds and is recorded.
	if out := w.Check(ctx, meta, "h1"); out.Decision != core.DecisionProceed {
		t.Fatalf("first Check should proceed, got %s", out.Decision)
	}
	_ = w.RecordAsSent(ctx, meta, "h1")

	// Second (new content) within the window → debounce defers → pending.
	if out := w.Check(ctx, meta, "h2"); out.Decision != core.DecisionDeferred {
		t.Fatalf("second Check should defer (debounce), got %s", out.Decision)
	}

	// Within the window: caps not clear → not returned.
	metas, err := w.ReplayCandidates(ctx, 0)
	if err != nil {
		t.Fatalf("ReplayCandidates: %v", err)
	}
	if len(metas) != 0 {
		t.Fatalf("pending entry inside the cap window should be skipped, got %v", metas)
	}

	// After the window elapses: caps clear → returned.
	time.Sleep(2 * debounce)
	metas, err = w.ReplayCandidates(ctx, 0)
	if err != nil {
		t.Fatalf("ReplayCandidates: %v", err)
	}
	if len(metas) != 1 || metas[0].TargetKey != "k1" {
		t.Fatalf("after window, should return k1, got %v", metas)
	}
	if string(metas[0].MessageRef) != "ref" {
		t.Fatalf("expected breadcrumb on returned entry, got %q", metas[0].MessageRef)
	}
}
