package protect_test

import (
	"context"
	"testing"
	"time"

	"github.com/homemade/pith/protect"
	"github.com/homemade/pith/sendstate"
)

// RangeDeferredWithCapsClear skips a pending entry while its cap window
// is still open, then yields it once the window elapses.
func TestProtector_RangeDeferredWithCapsClear(t *testing.T) {
	ctx := context.Background()
	const debounce = 30 * time.Millisecond
	p := protect.New(
		protect.WithSendStore(sendstate.NewMemoryStore(time.Hour)),
		protect.WithLeadingEdgeDebounce(debounce),
	)
	req := func(h string) protect.Request {
		return protect.Request{TargetKey: "k1", ContentHash: h, MessageRef: []byte("ref")}
	}

	// First send proceeds and is recorded.
	if out, _ := p.Check(ctx, req("h1")); out.Decision != protect.DecisionProceed {
		t.Fatalf("first Check should proceed, got %s", out.Decision)
	}
	_ = p.RecordAsSent(ctx, req("h1"))

	// Second (new content) within the window → debounce defers → pending.
	if out, _ := p.Check(ctx, req("h2")); out.Decision != protect.DecisionDeferred {
		t.Fatalf("second Check should defer (debounce), got %s", out.Decision)
	}

	// Within the window: caps not clear → not yielded.
	var seen []string
	_ = p.RangeDeferredWithCapsClear(ctx, 0, func(key string, _ sendstate.Entry) bool {
		seen = append(seen, key)
		return true
	})
	if len(seen) != 0 {
		t.Fatalf("pending entry inside the cap window should be skipped, got %v", seen)
	}

	// After the window elapses: caps clear → yielded.
	time.Sleep(2 * debounce)
	seen = nil
	_ = p.RangeDeferredWithCapsClear(ctx, 0, func(key string, e sendstate.Entry) bool {
		seen = append(seen, key)
		if string(e.LastDeferredMessageRef) != "ref" {
			t.Fatalf("expected breadcrumb on yielded entry, got %q", e.LastDeferredMessageRef)
		}
		return true
	})
	if len(seen) != 1 || seen[0] != "k1" {
		t.Fatalf("after window, should yield k1, got %v", seen)
	}
}

// Each proceed raises every attached cap's high-water mark to the
// post-send in-window count, maxing out at the cap that eventually
// defers.
func TestProtector_PeakSendsInWindow(t *testing.T) {
	ctx := context.Background()
	p := protect.New(
		protect.WithSendStore(sendstate.NewMemoryStore(time.Hour)),
		protect.WithCap(3, time.Hour),                  // "at cap", hardCap 3
		protect.WithCoalescer(100, time.Hour, "daily"), // never defers here
	)

	// Three sends with distinct content (so dedupe doesn't suppress).
	for _, h := range []string{"h1", "h2", "h3"} {
		req := protect.Request{TargetKey: "k1", ContentHash: h}
		out, err := p.Check(ctx, req)
		if err != nil {
			t.Fatalf("Check(%s): %v", h, err)
		}
		if out.Decision != protect.DecisionProceed {
			t.Fatalf("Check(%s) = %s, want Proceed", h, out.Decision)
		}
		if err := p.RecordAsSent(ctx, req); err != nil {
			t.Fatalf("RecordAsSent(%s): %v", h, err)
		}
	}

	// Fourth attempt is at the quota cap → deferred, peak already at 3.
	out, _ := p.Check(ctx, protect.Request{TargetKey: "k1", ContentHash: "h4"})
	if out.Decision != protect.DecisionDeferred || out.Reason != "at cap" {
		t.Fatalf("4th Check = %s/%q, want Deferred/\"at cap\"", out.Decision, out.Reason)
	}

	met, ok, err := p.Metrics(ctx, "k1")
	if err != nil || !ok {
		t.Fatalf("Metrics: ok=%v err=%v", ok, err)
	}
	if met.PeakSendsInWindow["at cap"] != 3 {
		t.Fatalf(`peak "at cap" = %d, want 3`, met.PeakSendsInWindow["at cap"])
	}
	if met.PeakSendsInWindow["daily"] != 3 {
		t.Fatalf(`peak "daily" = %d, want 3`, met.PeakSendsInWindow["daily"])
	}
	if met.TotalSent != 3 {
		t.Fatalf("TotalSent = %d, want 3", met.TotalSent)
	}
}
