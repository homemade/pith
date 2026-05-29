package core_test

import (
	"testing"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/internal/core"
)

// Inspect returns the Coalescers attached via opts, in order, without
// constructing a Protector. The wrapper packages
// (pith/protect/{memory,mongodb}) rely on this to derive sizing
// decisions from the same opts they're about to hand to core.New.
func TestInspect_ReturnsAttachedCoalescersInOrder(t *testing.T) {
	debounce := coalesce.NewLeadingEdgeDebounce(10 * time.Second)
	quotaA := coalesce.NewQuota(50, 24*time.Hour)
	quotaB := coalesce.NewQuota(5, time.Minute)

	got := core.Inspect(
		core.WithCoalescer(debounce),
		core.WithCoalescer(quotaA),
		core.WithCoalescer(quotaB),
	)

	if len(got) != 3 {
		t.Fatalf("Inspect length = %d, want 3", len(got))
	}
	// Names are the documented public way to identify Coalescers.
	wantNames := []string{
		"leading-edge debounce 10s",
		"quota cap 50 per 24h",
		"quota cap 5 per 1m",
	}
	for i, c := range got {
		name, _, _ := c.CapPolicy()
		if name != wantNames[i] {
			t.Fatalf("Inspect[%d] name = %q, want %q", i, name, wantNames[i])
		}
	}
}

// No opts → nil result. Lets backend wrappers cleanly treat "no
// Coalescers attached" as a zero-default case without needing a length
// check before iterating.
func TestInspect_NoOptsReturnsNil(t *testing.T) {
	if got := core.Inspect(); got != nil {
		t.Fatalf("Inspect() with no opts = %v, want nil", got)
	}
}

// The returned slice is a copy — mutating it doesn't leak into a
// Protector built from the same opts. Stops a backend wrapper from
// accidentally corrupting state by reordering or zeroing the result.
func TestInspect_ReturnsCopy(t *testing.T) {
	debounce := coalesce.NewLeadingEdgeDebounce(10 * time.Second)
	opts := []core.Option{core.WithCoalescer(debounce)}

	got := core.Inspect(opts...)
	got[0] = nil // clobber

	again := core.Inspect(opts...)
	if again[0] == nil {
		t.Fatalf("Inspect must return a defensive copy; got shared backing array")
	}
}
