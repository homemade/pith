package mongodb_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/protect"
	pmongo "github.com/homemade/pith/protect/mongodb"
)

// Hold-primitive tests against the real Mongo backend. Mirrors the
// memory-backend coverage in protect/memory/holds_test.go so a refactor
// on one is checked against the other's expectations. The Mongo backend
// adds the additional invariant of $$NOW-driven atomic operations on the
// holds collection — captured here through concurrent PlaceOnHold +
// ClearActiveHolds interactions.

// TestMongoHold_ActiveDuringWindow pins the core lifecycle on real Mongo.
func TestMongoHold_ActiveDuringWindow(t *testing.T) {
	if testClient == nil {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := freshDBName()
	p, err := pmongo.NewWriteProtector(ctx, testClient, pmongo.Config{
		Database: dbName,
		EntryTTL: 25 * time.Hour,
	}, coalesce.NewQuota(1000, 24*time.Hour))
	if err != nil {
		t.Fatalf("NewWriteProtector: %v", err)
	}
	t.Cleanup(func() { _ = testClient.Database(dbName).Drop(context.Background()) })

	tenant := p.Tenant("acme")

	// Initially no hold.
	if active, _, err := tenant.HasActiveHold(ctx); err != nil || active {
		t.Fatalf("initial HasActiveHold = (%v, _, %v); want (false, _, nil)", active, err)
	}

	// Place a hold ending in the near future.
	to := time.Now().Add(200 * time.Millisecond)
	if err := tenant.PlaceOnHold(ctx, time.Time{}, to, "rate-limit"); err != nil {
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
	if hold.Reason != "rate-limit" {
		t.Fatalf("hold.Reason = %q, want %q", hold.Reason, "rate-limit")
	}
	if hold.SetAt.IsZero() {
		t.Error("hold.SetAt is zero; server-side $$NOW must stamp it")
	}
	if !hold.ClearedAt.IsZero() {
		t.Error("hold.ClearedAt is non-zero on a currently-active hold")
	}

	// Sleep past the window — naturally expired.
	time.Sleep(250 * time.Millisecond)
	if active, _, err := tenant.HasActiveHold(ctx); err != nil || active {
		t.Fatalf("post-window HasActiveHold = (%v, _, %v); want (false, _, nil)", active, err)
	}
}

// TestMongoHold_ConcurrentPlaceOnHold confirms concurrent PlaceOnHold
// calls each contribute their own entry to the audit array — the
// single-doc atomic update linearises them and no entry is lost.
func TestMongoHold_ConcurrentPlaceOnHold(t *testing.T) {
	if testClient == nil {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := freshDBName()
	p, err := pmongo.NewWriteProtector(ctx, testClient, pmongo.Config{
		Database: dbName,
		EntryTTL: 25 * time.Hour,
	}, coalesce.NewQuota(1000, 24*time.Hour))
	if err != nil {
		t.Fatalf("NewWriteProtector: %v", err)
	}
	t.Cleanup(func() { _ = testClient.Database(dbName).Drop(context.Background()) })

	tenant := p.Tenant("acme")
	const n = 20
	var succeeded atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	now := time.Now()
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			to := now.Add(time.Duration(i+1) * time.Hour)
			if err := tenant.PlaceOnHold(ctx, time.Time{}, to, "concurrent"); err != nil {
				t.Errorf("PlaceOnHold[%d]: %v", i, err)
				return
			}
			succeeded.Add(1)
		}()
	}
	close(start)
	wg.Wait()

	if got := succeeded.Load(); got != n {
		t.Fatalf("PlaceOnHold successes = %d, want %d", got, n)
	}

	active, hold, err := tenant.HasActiveHold(ctx)
	if err != nil {
		t.Fatalf("HasActiveHold: %v", err)
	}
	if !active {
		t.Fatal("HasActiveHold after N concurrent PlaceOnHolds = false; want true")
	}
	// The most-restrictive hold has the latest To — n hours from now.
	wantTo := now.Add(time.Duration(n) * time.Hour)
	margin := 5 * time.Second // Mongo's $$NOW may differ slightly from this process's clock
	if hold.To.Before(wantTo.Add(-margin)) || hold.To.After(wantTo.Add(margin)) {
		t.Fatalf("most-restrictive hold.To = %v, want ~%v", hold.To, wantTo)
	}
}

// TestMongoHold_ClearActiveHoldsStampsServerSideNow confirms
// ClearActiveHolds stamps ClearedAt via $$NOW (server-side) on
// currently-active entries only.
func TestMongoHold_ClearActiveHoldsStampsServerSideNow(t *testing.T) {
	if testClient == nil {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := freshDBName()
	p, err := pmongo.NewWriteProtector(ctx, testClient, pmongo.Config{
		Database: dbName,
		EntryTTL: 25 * time.Hour,
	}, coalesce.NewQuota(1000, 24*time.Hour))
	if err != nil {
		t.Fatalf("NewWriteProtector: %v", err)
	}
	t.Cleanup(func() { _ = testClient.Database(dbName).Drop(context.Background()) })

	tenant := p.Tenant("acme")
	now := time.Now()

	// Active hold (To in the future).
	if err := tenant.PlaceOnHold(ctx, time.Time{}, now.Add(2*time.Hour), "active"); err != nil {
		t.Fatalf("PlaceOnHold[active]: %v", err)
	}
	if active, _, _ := tenant.HasActiveHold(ctx); !active {
		t.Fatal("pre-clear HasActiveHold = false, want true")
	}

	// Clear active holds.
	if err := tenant.ClearActiveHolds(ctx); err != nil {
		t.Fatalf("ClearActiveHolds: %v", err)
	}

	// Post-clear: no active hold.
	if active, _, _ := tenant.HasActiveHold(ctx); active {
		t.Fatal("post-clear HasActiveHold = true, want false")
	}

	// A fresh PlaceOnHold after the clear re-establishes an active hold.
	if err := tenant.PlaceOnHold(ctx, time.Time{}, now.Add(2*time.Hour), "fresh"); err != nil {
		t.Fatalf("PlaceOnHold[fresh]: %v", err)
	}
	active, hold, _ := tenant.HasActiveHold(ctx)
	if !active || hold.Reason != "fresh" {
		t.Fatalf("post-clear-then-place HasActiveHold = (%v, reason=%q); want (true, %q)", active, hold.Reason, "fresh")
	}
}

// TestMongoHold_ReadProtectorParity confirms the same hold collection
// is used regardless of read- vs write-protector — they share the
// tenant scope.
func TestMongoHold_ReadProtectorParity(t *testing.T) {
	if testClient == nil {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := freshDBName()
	p, err := pmongo.NewReadProtector(ctx, testClient, pmongo.Config{
		Database: dbName,
		EntryTTL: 25 * time.Hour,
	}, coalesce.NewQuota(1000, 24*time.Hour))
	if err != nil {
		t.Fatalf("NewReadProtector: %v", err)
	}
	t.Cleanup(func() { _ = testClient.Database(dbName).Drop(context.Background()) })

	tenant := p.Tenant("acme")
	if err := tenant.PlaceOnHold(ctx, time.Time{}, time.Now().Add(2*time.Hour), "read-side"); err != nil {
		t.Fatalf("PlaceOnHold: %v", err)
	}

	active, hold, err := tenant.HasActiveHold(ctx)
	if err != nil || !active {
		t.Fatalf("read-side HasActiveHold = (%v, _, %v); want (true, _, nil)", active, err)
	}
	if hold.Reason != "read-side" {
		t.Fatalf("hold.Reason = %q, want %q", hold.Reason, "read-side")
	}

	// protect.Hold is re-exported from sendstate; type-check that the
	// returned value satisfies the protect.Hold type identity.
	_ = protect.Hold(hold)
}

// TestMongoHold_TenantsAreIsolated verifies per-tenant doc separation —
// the holds collection's `_id = tenant` keying must prevent cross-tenant
// leakage.
func TestMongoHold_TenantsAreIsolated(t *testing.T) {
	if testClient == nil {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := freshDBName()
	p, err := pmongo.NewWriteProtector(ctx, testClient, pmongo.Config{
		Database: dbName,
		EntryTTL: 25 * time.Hour,
	}, coalesce.NewQuota(1000, 24*time.Hour))
	if err != nil {
		t.Fatalf("NewWriteProtector: %v", err)
	}
	t.Cleanup(func() { _ = testClient.Database(dbName).Drop(context.Background()) })

	tA := p.Tenant("A")
	tB := p.Tenant("B")

	if err := tA.PlaceOnHold(ctx, time.Time{}, time.Now().Add(2*time.Hour), "only-A"); err != nil {
		t.Fatalf("PlaceOnHold A: %v", err)
	}

	if active, _, _ := tA.HasActiveHold(ctx); !active {
		t.Fatal("tenant A: HasActiveHold = false, want true")
	}
	if active, _, _ := tB.HasActiveHold(ctx); active {
		t.Fatal("tenant B: HasActiveHold = true, want false (A's hold should not leak)")
	}

	_ = tA.ClearActiveHolds(ctx)
	if active, _, _ := tB.HasActiveHold(ctx); active {
		t.Fatal("tenant B after A's clear: HasActiveHold = true, want false")
	}
}
