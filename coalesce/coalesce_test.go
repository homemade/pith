package coalesce

import (
	"testing"
	"time"
)

// fakeActivity is a minimal [Activity] for ShouldDefer tests — counts
// timestamps in the trailing window inline so the test doesn't need to
// import any other pith package (keeping coalesce's test surface
// dependency-free).
type fakeActivity struct {
	sent     []time.Time
	deferred []time.Time
}

func (f fakeActivity) CountSentInWindow(now time.Time, window time.Duration) int {
	return countInWindow(f.sent, now, window)
}

func (f fakeActivity) CountDeferredInWindow(now time.Time, window time.Duration) int {
	return countInWindow(f.deferred, now, window)
}

func countInWindow(ts []time.Time, now time.Time, window time.Duration) int {
	cutoff := now.Add(-window)
	n := 0
	for _, t := range ts {
		if t.After(cutoff) {
			n++
		}
	}
	return n
}

func TestQuota_Fields(t *testing.T) {
	c := NewQuota(50, 24*time.Hour)
	if c.Strategy != StrategyQuota {
		t.Fatalf("Strategy = %v, want StrategyQuota", c.Strategy)
	}
	if c.HardCap != 50 {
		t.Fatalf("HardCap = %d, want 50", c.HardCap)
	}
	if c.Window != 24*time.Hour {
		t.Fatalf("Window = %v, want 24h", c.Window)
	}
	if want := "quota cap 50 per 24h"; c.Name() != want {
		t.Fatalf("Name = %q, want %q", c.Name(), want)
	}
}

func TestLeadingEdgeDebounce_Fields(t *testing.T) {
	c := NewLeadingEdgeDebounce(30 * time.Millisecond)
	if c.Strategy != StrategyLeadingEdge {
		t.Fatalf("Strategy = %v, want StrategyLeadingEdge", c.Strategy)
	}
	if c.HardCap != 1 {
		t.Fatalf("HardCap = %d, want 1", c.HardCap)
	}
	if c.Window != 30*time.Millisecond {
		t.Fatalf("Window = %v, want 30ms", c.Window)
	}
	if want := "leading-edge debounce 30ms"; c.Name() != want {
		t.Fatalf("Name = %q, want %q", c.Name(), want)
	}
}

func TestTrailingEdgeDebounce_Fields(t *testing.T) {
	c := NewTrailingEdgeDebounce(20 * time.Second)
	if c.Strategy != StrategyTrailingEdge {
		t.Fatalf("Strategy = %v, want StrategyTrailingEdge", c.Strategy)
	}
	// HardCap is 1, like a leading-edge debounce, so a shared store sizes
	// MaxSendTimes the same way.
	if c.HardCap != 1 {
		t.Fatalf("HardCap = %d, want 1", c.HardCap)
	}
	if c.Window != 20*time.Second {
		t.Fatalf("Window = %v, want 20s", c.Window)
	}
	if want := "trailing-edge debounce 20s"; c.Name() != want {
		t.Fatalf("Name = %q, want %q", c.Name(), want)
	}
}

// An unknown Strategy panics on Name() and ShouldDefer() — every
// Coalescer reaching the gate should originate from one of the New*
// constructors. Two failure modes covered:
//
//   - Coalescer{} (zero-value) → Strategy == StrategyInvalid (0).
//   - Literal-constructed Coalescer with an out-of-range value.
//
// Pins the contract so a future "default = leading-edge" silent
// regression is visible.
func TestUnknownStrategy_PanicsOnNameAndShouldDefer(t *testing.T) {
	cases := []struct {
		name string
		c    Coalescer
	}{
		{"zero-value", Coalescer{}},
		{"out-of-range", Coalescer{Strategy: Strategy(99), HardCap: 1, Window: time.Second}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			func() {
				defer func() {
					if recover() == nil {
						t.Error("Name on unknown Strategy did not panic")
					}
				}()
				_ = tc.c.Name()
			}()

			func() {
				defer func() {
					if recover() == nil {
						t.Error("ShouldDefer on unknown Strategy did not panic")
					}
				}()
				_ = tc.c.ShouldDefer(fakeActivity{}, time.Now())
			}()
		})
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

// The trailing-edge debounce is leading + trailing: the first event fires
// (empty activity → proceed), a recent send OR a recent deferral within
// the window keeps the key deferred (the burst middle), and once both go
// quiet it fires once more — the trailing/final read.
func TestTrailingEdgeDebounce_ShouldDefer(t *testing.T) {
	const w = 20 * time.Second
	c := NewTrailingEdgeDebounce(w)
	now := time.Unix(1_700_000_000, 0)

	// Leading fire: a never-seen key (zero activity) proceeds.
	if c.ShouldDefer(fakeActivity{}, now) {
		t.Fatal("empty activity should proceed (leading fire), got defer")
	}

	// A recent send within the window → defer (leading-edge behaviour).
	recentSend := fakeActivity{sent: []time.Time{now.Add(-5 * time.Second)}}
	if !c.ShouldDefer(recentSend, now) {
		t.Fatal("recent send within window should defer, got proceed")
	}

	// No recent send but a recent *deferral* → still defer: a sustained
	// burst stays collapsed (this is what leading-edge alone would miss).
	recentDefer := fakeActivity{
		sent:     []time.Time{now.Add(-2 * w)},
		deferred: []time.Time{now.Add(-3 * time.Second)},
	}
	if !c.ShouldDefer(recentDefer, now) {
		t.Fatal("recent deferral within window should defer, got proceed")
	}

	// Gone quiet: both the last send and the last deferral are older than
	// the window → the trailing fire proceeds (re-reads the final state).
	quiet := fakeActivity{
		sent:     []time.Time{now.Add(-2 * w)},
		deferred: []time.Time{now.Add(-2 * w)},
	}
	if c.ShouldDefer(quiet, now) {
		t.Fatal("quiet key (send & deferral both past window) should proceed (trailing fire), got defer")
	}
}
