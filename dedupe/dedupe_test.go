package dedupe_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/homemade/pith/dedupe"
	"github.com/homemade/pith/sendstate"
)

func TestDeduper_SeenAfterRecord(t *testing.T) {
	s := sendstate.NewMemoryStore()
	d := dedupe.NewDeduper(s, time.Hour)
	ctx := context.Background()
	key := "c1:t1"
	content := "abc123"

	seen, err := d.SeenInWindow(ctx, key, content)
	if err != nil {
		t.Fatalf("unexpected error on first SeenInWindow: %v", err)
	}
	if seen {
		t.Fatalf("expected miss on first lookup")
	}

	if err := s.RecordAsSent(ctx, key, content); err != nil {
		t.Fatalf("unexpected error on Record: %v", err)
	}

	seen, err = d.SeenInWindow(ctx, key, content)
	if err != nil {
		t.Fatalf("unexpected error on second SeenInWindow: %v", err)
	}
	if !seen {
		t.Fatalf("expected hit after Record")
	}
}

func TestDeduper_SameKeyDifferentContentMisses(t *testing.T) {
	s := sendstate.NewMemoryStore()
	d := dedupe.NewDeduper(s, time.Hour)
	ctx := context.Background()
	key := "c1:t1"

	if err := s.RecordAsSent(ctx, key, "hash-A"); err != nil {
		t.Fatalf("Record: %v", err)
	}

	seen, err := d.SeenInWindow(ctx, key, "hash-B")
	if err != nil {
		t.Fatalf("SeenInWindow: %v", err)
	}
	if seen {
		t.Fatalf("different content for the same key should miss")
	}
}

func TestDeduper_WindowExpiry(t *testing.T) {
	s := sendstate.NewMemoryStore()
	d := dedupe.NewDeduper(s, 10*time.Millisecond)
	ctx := context.Background()
	key := "c1:t1"
	content := "short"

	if err := s.RecordAsSent(ctx, key, content); err != nil {
		t.Fatalf("Record: %v", err)
	}
	time.Sleep(25 * time.Millisecond)

	seen, err := d.SeenInWindow(ctx, key, content)
	if err != nil {
		t.Fatalf("SeenInWindow: %v", err)
	}
	if seen {
		t.Fatalf("expected entry older than window to miss")
	}
}

func TestDeduper_RecordRefreshesLastSentAt(t *testing.T) {
	s := sendstate.NewMemoryStore()
	d := dedupe.NewDeduper(s, 10*time.Millisecond)
	ctx := context.Background()
	key := "c1:t1"
	content := "refresh"

	_ = s.RecordAsSent(ctx, key, content)
	time.Sleep(15 * time.Millisecond)
	_ = s.RecordAsSent(ctx, key, content)
	time.Sleep(5 * time.Millisecond)

	// 20ms passed since the first Record, but only 5ms since the
	// second. A 10ms window should still hit because LastSentAt was
	// refreshed.
	seen, _ := d.SeenInWindow(ctx, key, content)
	if !seen {
		t.Fatalf("second Record should refresh LastSentAt")
	}
}

func TestNewMemoryDeduper_StandaloneConvenience(t *testing.T) {
	d := dedupe.NewMemoryDeduper(time.Hour)
	seen, err := d.SeenInWindow(context.Background(), "k", "c")
	if err != nil {
		t.Fatalf("SeenInWindow: %v", err)
	}
	if seen {
		t.Fatalf("fresh deduper should miss")
	}
}

func TestDeduper_Concurrent(t *testing.T) {
	s := sendstate.NewMemoryStore()
	d := dedupe.NewDeduper(s, time.Hour)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "c1:t1:k" + string(rune('a'+i%26))
			_ = s.RecordAsSent(ctx, key, "content")
			_, _ = d.SeenInWindow(ctx, key, "content")
		}(i)
	}
	wg.Wait()
}
