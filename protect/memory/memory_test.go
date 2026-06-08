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
	w := p.Namespace("") // whole-store namespace; gating happens on the handle
	meta := protect.RequestMeta{TargetKey: "k1"}

	// First Check proceeds (no prior send).
	if out := w.Check(ctx, meta, "h1"); out.Decision != protect.DecisionProceed {
		t.Fatalf("first Check = %s, want Proceed", out.Decision)
	}
	if err := w.RecordAsSent(ctx, meta, "h1"); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}

	// Identical content → dedupe suppresses.
	if out := w.Check(ctx, meta, "h1"); out.Decision != protect.DecisionDeduped {
		t.Fatalf("repeat Check = %s, want Deduped", out.Decision)
	}

	// New content within the window → debounce defers.
	if out := w.Check(ctx, meta, "h2"); out.Decision != protect.DecisionDeferred {
		t.Fatalf("new-content Check = %s, want Deferred (debounce)", out.Decision)
	}
}

// A read protector has no dedupe and DEFERS (not drops) a too-frequent
// read — stamping a breadcrumb so a sweep can re-take it — and takes no
// contentHash.
func TestNewReadProtector(t *testing.T) {
	const debounce = 30 * time.Millisecond
	p := memprotect.NewReadProtector(
		time.Hour,
		coalesce.NewLeadingEdgeDebounce(debounce),
	)
	ctx := context.Background()
	r := p.Namespace("") // whole-store namespace; gating happens on the handle
	meta := protect.RequestMeta{TargetKey: "k1", MessageRef: []byte("ref-1")}

	// First Check proceeds.
	if out := r.Check(ctx, meta); out.Decision != protect.DecisionProceed {
		t.Fatalf("first Check = %s, want Proceed", out.Decision)
	}
	if err := r.RecordAsSent(ctx, meta); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}

	// Within the window → deferred (a cap suppression is replayable, not lost).
	if out := r.Check(ctx, meta); out.Decision != protect.DecisionDeferred {
		t.Fatalf("repeat Check = %s, want Deferred", out.Decision)
	}

	// The deferral left a breadcrumb; once the window clears it is a replay
	// candidate (re-derived from MessageRef, re-read against current state).
	time.Sleep(2 * debounce)
	cands, err := r.ReplayCandidates(ctx, 0)
	if err != nil {
		t.Fatalf("ReplayCandidates: %v", err)
	}
	if len(cands) != 1 || string(cands[0].MessageRef) != "ref-1" {
		t.Fatalf("ReplayCandidates = %+v, want 1 candidate with MessageRef ref-1", cands)
	}
}
