package protect_test

import (
	"testing"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/protect"
	"github.com/homemade/pith/sendstate/memory"
)

// New sizes the memory store's send-timestamp list to the largest
// attached Coalescer cap so CountSentInWindow can't undercount.
func TestNew_SizesMaxSendTimesToLargestCap(t *testing.T) {
	store := memory.New(24 * time.Hour)
	_ = protect.New(
		store,
		protect.WithCoalescer(coalesce.NewLeadingEdgeDebounce(10*time.Second)), // hardCap 1
		protect.WithCoalescer(coalesce.NewQuota(50, 24*time.Hour)),             // hardCap 50
		protect.WithCoalescer(coalesce.NewQuota(5, time.Minute)),               // hardCap 5 (burst)
	)
	if store.MaxSendTimes != 50 {
		t.Fatalf("MaxSendTimes = %d, want 50 (largest attached cap)", store.MaxSendTimes)
	}
}

// Grow-only: a caller who pre-sizes their own store above the
// largest cap keeps that value (e.g. to cover a custom WithCoalescer).
func TestNew_PreservesLargerManualCap(t *testing.T) {
	store := memory.New(24 * time.Hour)
	store.MaxSendTimes = 1000
	_ = protect.New(
		store,
		protect.WithCoalescer(coalesce.NewQuota(50, 24*time.Hour)),
	)
	if store.MaxSendTimes != 1000 {
		t.Fatalf("MaxSendTimes = %d, want 1000 (grow-only must not shrink)", store.MaxSendTimes)
	}
}
