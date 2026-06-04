package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/protect"
	memprotect "github.com/homemade/pith/protect/memory"
)

// The factories are thin wrappers, but pinning the happy paths here keeps
// the wiring observable: a refactor that broke it would surface a Check
// returning the wrong Decision.

// A write protector dedupes identical content and defers a same-target
// follow-up within the debounce window.
func TestNewWriteProtector(t *testing.T) {
	const debounce = 30 * time.Millisecond
	p := memprotect.NewWriteProtector(
		time.Hour,
		coalesce.NewLeadingEdgeDebounce(debounce),
	)
	ctx := context.Background()
	meta := protect.RequestMeta{TargetKey: "k1"}

	// First Check proceeds (no prior send).
	if out := p.Check(ctx, meta, "h1"); out.Decision != protect.DecisionProceed {
		t.Fatalf("first Check = %s, want Proceed", out.Decision)
	}
	if err := p.RecordAsSent(ctx, meta, "h1"); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}

	// Identical content → dedupe suppresses.
	if out := p.Check(ctx, meta, "h1"); out.Decision != protect.DecisionDeduped {
		t.Fatalf("repeat Check = %s, want Deduped", out.Decision)
	}

	// New content within the window → debounce defers.
	if out := p.Check(ctx, meta, "h2"); out.Decision != protect.DecisionDeferred {
		t.Fatalf("new-content Check = %s, want Deferred (debounce)", out.Decision)
	}
}

// A read protector has no dedupe and DROPS (not defers) a too-frequent
// read — and takes no contentHash.
func TestNewReadProtector(t *testing.T) {
	const debounce = 30 * time.Millisecond
	p := memprotect.NewReadProtector(
		time.Hour,
		coalesce.NewLeadingEdgeDebounce(debounce),
	)
	ctx := context.Background()
	meta := protect.RequestMeta{TargetKey: "k1"}

	// First Check proceeds.
	if out := p.Check(ctx, meta); out.Decision != protect.DecisionProceed {
		t.Fatalf("first Check = %s, want Proceed", out.Decision)
	}
	if err := p.RecordAsSent(ctx, meta); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}

	// Within the window → dropped (a read is skippable, not replayable).
	if out := p.Check(ctx, meta); out.Decision != protect.DecisionDropped {
		t.Fatalf("repeat Check = %s, want Dropped", out.Decision)
	}
}
