package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/protect"
	memprotect "github.com/homemade/pith/protect/memory"
)

// Factory + tenant-chain smoke tests. The CheckAndReserve outcomes themselves
// are pinned in checkandreserve_test.go; this file covers the protector wiring
// and the Tenant().Namespace() chain composition.

// TestTenantChain verifies the chained Tenant().Namespace() API. Three handles
// minted from the same root protector — two tenanted and one untenanted —
// each compose CheckAndReserve + dedupe normally, confirming the chain is just
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
		out, release := h.w.CheckAndReserve(ctx, meta, "h")
		if out.Decision != protect.DecisionProceed {
			t.Fatalf("%s: first CheckAndReserve = %s, want Proceed", h.name, out.Decision)
		}
		if release == nil {
			t.Fatalf("%s: first CheckAndReserve release is nil", h.name)
		}
		// Identical content on the same handle dedupes — confirms the chained
		// handle composes CheckAndReserve normally.
		out2, release2 := h.w.CheckAndReserve(ctx, meta, "h")
		if out2.Decision != protect.DecisionDeduped {
			t.Fatalf("%s: repeat CheckAndReserve = %s, want Deduped", h.name, out2.Decision)
		}
		if release2 != nil {
			t.Fatalf("%s: dedupe CheckAndReserve release is non-nil; nothing reserved", h.name)
		}
	}
}

// TestTenantReadProtector mirrors TestTenantChain for the read side: chained
// Tenant().Namespace() handles compose CheckAndReserve / ReplayCandidates.
func TestTenantReadProtector(t *testing.T) {
	const debounce = 30 * time.Millisecond
	p := memprotect.NewReadProtector(
		time.Hour,
		coalesce.NewLeadingEdgeDebounce(debounce),
	)
	ctx := context.Background()

	r := p.Tenant("tenant-A").Namespace("ns")
	meta := protect.RequestMeta{TargetKey: "k1", MessageRef: []byte("ref-1")}

	out, release := r.CheckAndReserve(ctx, meta)
	if out.Decision != protect.DecisionProceed {
		t.Fatalf("first CheckAndReserve = %s, want Proceed", out.Decision)
	}
	if release == nil {
		t.Fatal("first CheckAndReserve release is nil")
	}
	// Within the window → deferred (a cap suppression is replayable, not lost).
	out2, _ := r.CheckAndReserve(ctx, meta)
	if out2.Decision != protect.DecisionDeferred {
		t.Fatalf("repeat CheckAndReserve = %s, want Deferred", out2.Decision)
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
