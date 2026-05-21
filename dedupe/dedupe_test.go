package dedupe

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMemoryDeduper_SeenAndRecord(t *testing.T) {
	d := NewMemoryDeduper()
	ctx := context.Background()
	key := "dedupe:webhook:c1:t1:abc123"

	seen, err := d.SeenInWindow(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error on first SeenInWindow: %v", err)
	}
	if seen {
		t.Fatalf("expected miss on first lookup")
	}

	if err := d.RecordSent(ctx, key, time.Hour); err != nil {
		t.Fatalf("unexpected error on RecordSent: %v", err)
	}

	seen, err = d.SeenInWindow(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error on second SeenInWindow: %v", err)
	}
	if !seen {
		t.Fatalf("expected hit after RecordSent")
	}
}

func TestMemoryDeduper_TTLExpiry(t *testing.T) {
	d := NewMemoryDeduper()
	ctx := context.Background()
	key := "dedupe:webhook:c1:t1:short"

	if err := d.RecordSent(ctx, key, 10*time.Millisecond); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	time.Sleep(25 * time.Millisecond)

	seen, err := d.SeenInWindow(ctx, key)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if seen {
		t.Fatalf("expected expired entry to miss")
	}
}

func TestMemoryDeduper_RecordRefreshesExpiry(t *testing.T) {
	d := NewMemoryDeduper()
	ctx := context.Background()
	key := "dedupe:webhook:c1:t1:refresh"

	_ = d.RecordSent(ctx, key, 10*time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	_ = d.RecordSent(ctx, key, time.Hour)
	time.Sleep(10 * time.Millisecond)

	seen, _ := d.SeenInWindow(ctx, key)
	if !seen {
		t.Fatalf("second RecordSent should refresh expiry")
	}
}

func TestMemoryDeduper_Concurrent(t *testing.T) {
	d := NewMemoryDeduper()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "dedupe:webhook:c1:t1:k" + string(rune('a'+i%26))
			_ = d.RecordSent(ctx, key, time.Hour)
			_, _ = d.SeenInWindow(ctx, key)
		}(i)
	}
	wg.Wait()
}
