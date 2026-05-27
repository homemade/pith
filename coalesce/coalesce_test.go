package coalesce

import (
	"testing"
	"time"
)

func TestStoreCoalescer_CapPolicy(t *testing.T) {
	c := NewCoalescer(50, 24*time.Hour)
	hardCap, window := c.CapPolicy()
	if hardCap != 50 {
		t.Fatalf("CapPolicy hardCap = %d, want 50", hardCap)
	}
	if window != 24*time.Hour {
		t.Fatalf("CapPolicy window = %v, want 24h", window)
	}
}
