package protect_test

import (
	"testing"
	"time"

	"github.com/homemade/pith/protect"
	"github.com/homemade/pith/sendstate"
)

// New sizes the MemoryStore's send-timestamp list to the largest
// attached Coalescer cap so CountInWindow can't undercount.
func TestNew_SizesMaxSendTimesToLargestCap(t *testing.T) {
	p := protect.New(
		protect.WithSendStore(sendstate.NewMemoryStore(24*time.Hour)),
		protect.WithLeadingEdgeDebounce(10*time.Second), // hardCap 1
		protect.WithCap(50, 24*time.Hour),               // hardCap 50
		protect.WithCoalescer(5, time.Minute, "burst"),  // hardCap 5
	)
	ms, ok := p.SendStore().(*sendstate.MemoryStore)
	if !ok {
		t.Fatalf("default store is not *sendstate.MemoryStore")
	}
	if ms.MaxSendTimes != 50 {
		t.Fatalf("MaxSendTimes = %d, want 50 (largest attached cap)", ms.MaxSendTimes)
	}
}

// Grow-only: a caller who pre-sizes their own store above the
// largest cap keeps that value (e.g. to cover a WithCoalescerImpl).
func TestNew_PreservesLargerManualCap(t *testing.T) {
	store := sendstate.NewMemoryStore(24 * time.Hour)
	store.MaxSendTimes = 1000
	p := protect.New(
		protect.WithSendStore(store),
		protect.WithCap(50, 24*time.Hour),
	)
	ms := p.SendStore().(*sendstate.MemoryStore)
	if ms.MaxSendTimes != 1000 {
		t.Fatalf("MaxSendTimes = %d, want 1000 (grow-only must not shrink)", ms.MaxSendTimes)
	}
}
