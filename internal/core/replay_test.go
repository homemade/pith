package core_test

import (
	"context"
	"testing"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/internal/core"
	"github.com/homemade/pith/protect"
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
	).Tenant("").Namespace("") // untenanted, whole-store; gating happens on the handle
	meta := protect.RequestMeta{TargetKey: "k1", MessageRef: []byte("ref")}

	// First send proceeds and is reserved (CheckAndReserve atomically
	// records on Proceed — no separate RecordAsSent needed).
	if out, _ := w.CheckAndReserve(ctx, meta, "h1"); out.Decision != protect.DecisionProceed {
		t.Fatalf("first CheckAndReserve should proceed, got %s", out.Decision)
	}

	// Second (new content) within the window → debounce defers → pending.
	if out, _ := w.CheckAndReserve(ctx, meta, "h2"); out.Decision != protect.DecisionDeferred {
		t.Fatalf("second CheckAndReserve should defer (debounce), got %s", out.Decision)
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
