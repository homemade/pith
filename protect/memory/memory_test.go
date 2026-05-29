package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/protect"
	memprotect "github.com/homemade/pith/protect/memory"
)

// The factory is a thin wrapper, but pinning the happy path here keeps
// it observable: a refactor that broke the wiring (e.g. forgot to apply
// opts) would surface a Check that returns the wrong Decision.
func TestNew_BuildsAFunctioningProtector(t *testing.T) {
	const debounce = 30 * time.Millisecond
	p := memprotect.New(
		time.Hour,
		protect.WithCoalescer(coalesce.NewLeadingEdgeDebounce(debounce)),
	)
	ctx := context.Background()

	// First Check proceeds (no prior send).
	req := protect.Request{
		RequestMeta: protect.RequestMeta{TargetKey: "k1"},
		ContentHash: "h1",
	}
	if out := p.Check(ctx, req); out.Decision != protect.DecisionProceed {
		t.Fatalf("first Check = %s, want Proceed", out.Decision)
	}
	if err := p.RecordAsSent(ctx, req); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}

	// Second Check (different content) within the window → debounce defers.
	req2 := protect.Request{
		RequestMeta: protect.RequestMeta{TargetKey: "k1"},
		ContentHash: "h2",
	}
	if out := p.Check(ctx, req2); out.Decision != protect.DecisionDeferred {
		t.Fatalf("second Check = %s, want Deferred (debounce)", out.Decision)
	}
}

// Memory-backed Protector works fine with no Coalescers attached —
// dedupe-only behaviour, useful for tests and local dev. Pins this
// because protect.New is happy with zero opts; the wrapper must
// faithfully pass that through.
func TestNew_DedupeOnlyWithoutCoalescers(t *testing.T) {
	p := memprotect.New(time.Hour)
	ctx := context.Background()

	req := protect.Request{
		RequestMeta: protect.RequestMeta{TargetKey: "k1"},
		ContentHash: "h1",
	}
	if out := p.Check(ctx, req); out.Decision != protect.DecisionProceed {
		t.Fatalf("Check = %s, want Proceed", out.Decision)
	}
	if err := p.RecordAsSent(ctx, req); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}

	// Identical content → dedupe suppresses.
	if out := p.Check(ctx, req); out.Decision != protect.DecisionDeduped {
		t.Fatalf("repeat Check = %s, want Deduped", out.Decision)
	}
}
