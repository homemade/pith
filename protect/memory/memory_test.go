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
	w := p.Tenant("").Namespace("") // untenanted, whole-store; gating happens on the handle
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
	r := p.Tenant("").Namespace("") // untenanted, whole-store; gating happens on the handle
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

// TestTenantChain verifies the chained Tenant().Namespace() API. Three handles
// minted from the same root protector — two tenanted and one untenanted — each
// compose Check + RecordAsSent + dedupe normally, confirming the chain is just
// scoping plumbing on top of the existing gating methods.
func TestTenantChain(t *testing.T) {
	const debounce = 30 * time.Millisecond
	p := memprotect.NewWriteProtector(
		time.Hour,
		coalesce.NewLeadingEdgeDebounce(debounce),
	)
	ctx := context.Background()

	tA := p.Tenant("tenant-A").Namespace("ns")
	tB := p.Tenant("tenant-B").Namespace("ns")
	untenanted := p.Tenant("").Namespace("ns")

	for _, h := range []struct {
		name string
		w    protect.WriteNamespace
	}{
		{"tenant-A/ns", tA},
		{"tenant-B/ns", tB},
		{"untenanted/ns", untenanted},
	} {
		meta := protect.RequestMeta{TargetKey: "k1-" + h.name}
		if out := h.w.Check(ctx, meta, "h"); out.Decision != protect.DecisionProceed {
			t.Fatalf("%s: first Check = %s, want Proceed", h.name, out.Decision)
		}
		if err := h.w.RecordAsSent(ctx, meta, "h"); err != nil {
			t.Fatalf("%s: RecordAsSent: %v", h.name, err)
		}
		// Identical content on the same handle dedupes — confirms the chained
		// handle composes Check + RecordAsSent normally.
		if out := h.w.Check(ctx, meta, "h"); out.Decision != protect.DecisionDeduped {
			t.Fatalf("%s: repeat Check = %s, want Deduped", h.name, out.Decision)
		}
	}
}

// TestTenantEmpty confirms Tenant("") yields an untenanted handle — the
// "no outer scope" sentinel of the chain, where the resulting handle stamps
// no tenant on its writes.
func TestTenantEmpty(t *testing.T) {
	p := memprotect.NewWriteProtector(time.Hour, coalesce.NewLeadingEdgeDebounce(30*time.Millisecond))
	ctx := context.Background()

	w := p.Tenant("").Namespace("ns")
	meta := protect.RequestMeta{TargetKey: "k1"}

	if out := w.Check(ctx, meta, "h"); out.Decision != protect.DecisionProceed {
		t.Fatalf("Check = %s, want Proceed", out.Decision)
	}
	if err := w.RecordAsSent(ctx, meta, "h"); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}
}

// TestTenantReadProtector mirrors TestTenantChain for the read side: chained
// Tenant().Namespace() handles compose Check / RecordAsSent / ReplayCandidates.
func TestTenantReadProtector(t *testing.T) {
	const debounce = 30 * time.Millisecond
	p := memprotect.NewReadProtector(
		time.Hour,
		coalesce.NewLeadingEdgeDebounce(debounce),
	)
	ctx := context.Background()

	r := p.Tenant("tenant-A").Namespace("ns")
	meta := protect.RequestMeta{TargetKey: "k1", MessageRef: []byte("ref-1")}

	if out := r.Check(ctx, meta); out.Decision != protect.DecisionProceed {
		t.Fatalf("first Check = %s, want Proceed", out.Decision)
	}
	if err := r.RecordAsSent(ctx, meta); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}
	if out := r.Check(ctx, meta); out.Decision != protect.DecisionDeferred {
		t.Fatalf("repeat Check = %s, want Deferred", out.Decision)
	}
}
