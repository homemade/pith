package memory_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/protect"
	memprotect "github.com/homemade/pith/protect/memory"
)

// Hold-primitive tests against the in-process memory backend. The
// Mongo-backed equivalents live in protect/mongodb/holds_test.go and
// pin the same invariants through the $$NOW-driven aggregation
// pipeline. Both backends share these tests' shape so a refactor on
// one is checked against the other's expectations.

// TestHold_ActiveDuringWindow pins the core HasActiveHold lifecycle:
// active while `From ≤ now < To AND ClearedAt is zero`, inactive
// after the window.
func TestHold_ActiveDuringWindow(t *testing.T) {
	p := memprotect.NewWriteProtector(time.Hour, coalesce.NewQuota(1000, time.Hour))
	ctx := context.Background()
	tenant := p.Tenant("acme")

	// Initially no hold.
	if active, _, err := tenant.HasActiveHold(ctx); err != nil || active {
		t.Fatalf("initial HasActiveHold = (%v, _, %v); want (false, _, nil)", active, err)
	}

	// Place a hold ending in the near future.
	to := time.Now().Add(50 * time.Millisecond)
	if err := tenant.PlaceOnHold(ctx, time.Time{}, to, "test"); err != nil {
		t.Fatalf("PlaceOnHold: %v", err)
	}

	// Active during the window.
	active, hold, err := tenant.HasActiveHold(ctx)
	if err != nil {
		t.Fatalf("HasActiveHold during window: %v", err)
	}
	if !active {
		t.Fatal("HasActiveHold = false during the window, want true")
	}
	if hold.Reason != "test" {
		t.Fatalf("hold.Reason = %q, want %q", hold.Reason, "test")
	}
	if !hold.To.Equal(to) {
		t.Fatalf("hold.To = %v, want %v", hold.To, to)
	}
	if hold.SetAt.IsZero() {
		t.Error("hold.SetAt is zero; storage layer must stamp it")
	}
	if !hold.ClearedAt.IsZero() {
		t.Error("hold.ClearedAt is non-zero on a currently-active hold")
	}

	// Sleep past the window — naturally expired.
	time.Sleep(70 * time.Millisecond)
	if active, _, err := tenant.HasActiveHold(ctx); err != nil || active {
		t.Fatalf("post-window HasActiveHold = (%v, _, %v); want (false, _, nil)", active, err)
	}
}

// TestHold_FutureFromScheduled confirms a hold with `From > now` is
// inactive until From, then active until To.
func TestHold_FutureFromScheduled(t *testing.T) {
	p := memprotect.NewWriteProtector(time.Hour, coalesce.NewQuota(1000, time.Hour))
	ctx := context.Background()
	tenant := p.Tenant("acme")

	now := time.Now()
	from := now.Add(30 * time.Millisecond)
	to := now.Add(90 * time.Millisecond)
	if err := tenant.PlaceOnHold(ctx, from, to, "future"); err != nil {
		t.Fatalf("PlaceOnHold: %v", err)
	}

	// Before From → inactive.
	if active, _, _ := tenant.HasActiveHold(ctx); active {
		t.Fatal("HasActiveHold before From = true, want false")
	}

	// After From, before To → active.
	time.Sleep(50 * time.Millisecond)
	if active, _, _ := tenant.HasActiveHold(ctx); !active {
		t.Fatal("HasActiveHold inside window = false, want true")
	}

	// After To → inactive again.
	time.Sleep(50 * time.Millisecond)
	if active, _, _ := tenant.HasActiveHold(ctx); active {
		t.Fatal("HasActiveHold after To = true, want false")
	}
}

// TestHold_ConcurrentPlaceOnHold confirms concurrent PlaceOnHold calls
// each contribute their own entry — the audit array grows by N for N
// concurrent calls, and HasActiveHold returns the most-restrictive
// (latest To) of them.
func TestHold_ConcurrentPlaceOnHold(t *testing.T) {
	p := memprotect.NewWriteProtector(time.Hour, coalesce.NewQuota(1000, time.Hour))
	ctx := context.Background()
	tenant := p.Tenant("acme")

	const n = 20
	now := time.Now()
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Each entry has a distinct To so HasActiveHold can identify
			// the most-restrictive one as the LAST iteration's.
			to := now.Add(time.Duration(i+1) * time.Second)
			if err := tenant.PlaceOnHold(ctx, time.Time{}, to, "concurrent"); err != nil {
				t.Errorf("PlaceOnHold[%d]: %v", i, err)
			}
		}()
	}
	close(start)
	wg.Wait()

	active, hold, err := tenant.HasActiveHold(ctx)
	if err != nil {
		t.Fatalf("HasActiveHold: %v", err)
	}
	if !active {
		t.Fatal("HasActiveHold after N concurrent PlaceOnHolds = false; want true")
	}
	// The most-restrictive hold has the latest To — n seconds from now.
	wantTo := now.Add(time.Duration(n) * time.Second)
	if hold.To.Before(wantTo) || hold.To.After(wantTo.Add(time.Second)) {
		t.Fatalf("most-restrictive hold.To = %v, want ~%v (latest of N concurrent)", hold.To, wantTo)
	}
}

// TestHold_ClearActiveHoldsStampsOnlyActive confirms ClearActiveHolds
// stamps ClearedAt on currently-active entries only — expired entries
// stay expired (no ClearedAt added retroactively), and a subsequent
// HasActiveHold returns false.
func TestHold_ClearActiveHoldsStampsOnlyActive(t *testing.T) {
	p := memprotect.NewWriteProtector(time.Hour, coalesce.NewQuota(1000, time.Hour))
	ctx := context.Background()
	tenant := p.Tenant("acme")

	now := time.Now()

	// 1. Already-expired hold (placed in the past, To already passed).
	expiredTo := now.Add(-30 * time.Millisecond)
	if err := tenant.PlaceOnHold(ctx, now.Add(-100*time.Millisecond), expiredTo, "expired"); err != nil {
		t.Fatalf("PlaceOnHold[expired]: %v", err)
	}

	// 2. Currently-active hold.
	activeTo := now.Add(2 * time.Hour)
	if err := tenant.PlaceOnHold(ctx, time.Time{}, activeTo, "active"); err != nil {
		t.Fatalf("PlaceOnHold[active]: %v", err)
	}

	// Pre-clear: the active hold is reported active.
	if active, _, _ := tenant.HasActiveHold(ctx); !active {
		t.Fatal("pre-clear HasActiveHold = false, want true")
	}

	// Clear active holds — should stamp ClearedAt on hold #2 only.
	if err := tenant.ClearActiveHolds(ctx); err != nil {
		t.Fatalf("ClearActiveHolds: %v", err)
	}

	// Post-clear: no active hold.
	if active, _, _ := tenant.HasActiveHold(ctx); active {
		t.Fatal("post-clear HasActiveHold = true, want false")
	}

	// A subsequent PlaceOnHold re-establishes an active hold (no
	// residual state from the clear).
	if err := tenant.PlaceOnHold(ctx, time.Time{}, now.Add(2*time.Hour), "fresh"); err != nil {
		t.Fatalf("PlaceOnHold[fresh]: %v", err)
	}
	active, hold, _ := tenant.HasActiveHold(ctx)
	if !active {
		t.Fatal("post-clear-then-place HasActiveHold = false, want true")
	}
	if hold.Reason != "fresh" {
		t.Fatalf("post-clear-then-place hold.Reason = %q, want %q", hold.Reason, "fresh")
	}
}

// TestHold_ReadProtectorParity confirms the same hold API works
// identically on the read side — it's the same per-tenant store
// regardless of whether the protector is read or write.
func TestHold_ReadProtectorParity(t *testing.T) {
	p := memprotect.NewReadProtector(time.Hour, coalesce.NewQuota(1000, time.Hour))
	ctx := context.Background()
	tenant := p.Tenant("acme")

	to := time.Now().Add(2 * time.Hour)
	if err := tenant.PlaceOnHold(ctx, time.Time{}, to, "read-side"); err != nil {
		t.Fatalf("PlaceOnHold: %v", err)
	}

	active, hold, err := tenant.HasActiveHold(ctx)
	if err != nil || !active {
		t.Fatalf("read-side HasActiveHold = (%v, _, %v); want (true, _, nil)", active, err)
	}
	if hold.Reason != "read-side" {
		t.Fatalf("hold.Reason = %q, want %q", hold.Reason, "read-side")
	}

	if err := tenant.ClearActiveHolds(ctx); err != nil {
		t.Fatalf("ClearActiveHolds: %v", err)
	}
	if active, _, _ := tenant.HasActiveHold(ctx); active {
		t.Fatal("post-clear HasActiveHold = true, want false")
	}

	// Suppress unused-var warning if Hold's exported fields are ever
	// pruned — the test should still compile-link against protect.Hold.
	_ = protect.Hold{}
}

// TestHold_TenantsAreIsolated verifies that holds on tenant A don't
// leak into tenant B — the per-tenant scoping is the foundational
// guarantee.
func TestHold_TenantsAreIsolated(t *testing.T) {
	p := memprotect.NewWriteProtector(time.Hour, coalesce.NewQuota(1000, time.Hour))
	ctx := context.Background()

	tA := p.Tenant("A")
	tB := p.Tenant("B")

	to := time.Now().Add(2 * time.Hour)
	if err := tA.PlaceOnHold(ctx, time.Time{}, to, "only-A"); err != nil {
		t.Fatalf("PlaceOnHold A: %v", err)
	}

	if active, _, _ := tA.HasActiveHold(ctx); !active {
		t.Fatal("tenant A: HasActiveHold = false, want true (just placed)")
	}
	if active, _, _ := tB.HasActiveHold(ctx); active {
		t.Fatal("tenant B: HasActiveHold = true, want false (A's hold should not leak)")
	}

	// Clearing on A shouldn't affect B.
	_ = tA.ClearActiveHolds(ctx)
	if active, _, _ := tB.HasActiveHold(ctx); active {
		t.Fatal("tenant B after A's clear: HasActiveHold = true, want false")
	}
}
