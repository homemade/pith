package coalesce

import (
	"testing"
	"time"
)

func TestStoreCoalescer_CapPolicy(t *testing.T) {
	c := NewQuota(50, 24*time.Hour)
	name, hardCap, window := c.CapPolicy()
	if want := "quota cap 50 per 24h"; name != want {
		t.Fatalf("CapPolicy name = %q, want %q", name, want)
	}
	if hardCap != 50 {
		t.Fatalf("CapPolicy hardCap = %d, want 50", hardCap)
	}
	if window != 24*time.Hour {
		t.Fatalf("CapPolicy window = %v, want 24h", window)
	}
}

func TestHumanizeDuration(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{24 * time.Hour, "24h"},
		{time.Hour, "1h"},
		{90 * time.Minute, "1h30m"},
		{time.Hour + 30*time.Second, "1h30s"}, // zero-minute component dropped
		{time.Minute, "1m"},
		{90 * time.Second, "1m30s"},
		{10 * time.Second, "10s"},
		{1500 * time.Millisecond, "1.5s"},
		{30 * time.Millisecond, "30ms"},
		{0, "0s"},
	}
	for _, c := range cases {
		if got := humanizeDuration(c.d); got != c.want {
			t.Errorf("humanizeDuration(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}

func TestLeadingEdgeDebounce_CapPolicy(t *testing.T) {
	c := NewLeadingEdgeDebounce(30 * time.Millisecond)
	name, hardCap, window := c.CapPolicy()
	if want := "leading-edge debounce 30ms"; name != want {
		t.Fatalf("CapPolicy name = %q, want %q", name, want)
	}
	if hardCap != 1 {
		t.Fatalf("CapPolicy hardCap = %d, want 1", hardCap)
	}
	if window != 30*time.Millisecond {
		t.Fatalf("CapPolicy window = %v, want 30ms", window)
	}
}
