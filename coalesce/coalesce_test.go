package coalesce

import (
	"testing"
	"time"

	"github.com/homemade/pith/sendstate"
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

func TestTrailingEdgeDebounce_CapPolicy(t *testing.T) {
	c := NewTrailingEdgeDebounce(20 * time.Second)
	name, hardCap, window := c.CapPolicy()
	if want := "trailing-edge debounce 20s"; name != want {
		t.Fatalf("CapPolicy name = %q, want %q", name, want)
	}
	// hardCap is 1, like a leading-edge debounce, so a shared store sizes
	// MaxSendTimes the same way.
	if hardCap != 1 {
		t.Fatalf("CapPolicy hardCap = %d, want 1", hardCap)
	}
	if window != 20*time.Second {
		t.Fatalf("CapPolicy window = %v, want 20s", window)
	}
}

// The trailing-edge debounce is leading + trailing: the first event fires
// (empty entry → proceed), a recent send OR a recent deferral within the
// window keeps the key deferred (the burst middle), and once both go quiet
// it fires once more — the trailing/final read.
func TestTrailingEdgeDebounce_ShouldDefer(t *testing.T) {
	const w = 20 * time.Second
	c := NewTrailingEdgeDebounce(w)
	now := time.Unix(1_700_000_000, 0)

	// Leading fire: a never-seen key (zero entry) proceeds.
	if c.ShouldDefer(sendstate.Entry{}, now) {
		t.Fatal("empty entry should proceed (leading fire), got defer")
	}

	// A recent send within the window → defer (leading-edge behaviour).
	recentSend := sendstate.Entry{LastNSendTimes: []time.Time{now.Add(-5 * time.Second)}}
	if !c.ShouldDefer(recentSend, now) {
		t.Fatal("recent send within window should defer, got proceed")
	}

	// No recent send but a recent *deferral* → still defer: a sustained
	// burst stays collapsed (this is what leading-edge alone would miss).
	recentDefer := sendstate.Entry{
		LastNSendTimes:     []time.Time{now.Add(-2 * w)},
		LastNDeferredTimes: []time.Time{now.Add(-3 * time.Second)},
	}
	if !c.ShouldDefer(recentDefer, now) {
		t.Fatal("recent deferral within window should defer, got proceed")
	}

	// Gone quiet: both the last send and the last deferral are older than
	// the window → the trailing fire proceeds (re-reads the final state).
	quiet := sendstate.Entry{
		LastNSendTimes:     []time.Time{now.Add(-2 * w)},
		LastNDeferredTimes: []time.Time{now.Add(-2 * w)},
	}
	if c.ShouldDefer(quiet, now) {
		t.Fatal("quiet key (send & deferral both past window) should proceed (trailing fire), got defer")
	}
}
