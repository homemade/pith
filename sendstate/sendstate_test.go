package sendstate

import (
	"bytes"
	"context"
	"sync"
	"testing"
)

func TestMemoryStore_RecordAsSentThenLookup(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	if err := s.RecordAsSent(ctx, "k1", "hash-A"); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}
	e, ok, err := s.Lookup(ctx, "k1")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if !ok {
		t.Fatalf("expected hit")
	}
	if e.ContentHash != "hash-A" {
		t.Fatalf("ContentHash = %q, want hash-A", e.ContentHash)
	}
	if e.LastSentAt.IsZero() {
		t.Fatalf("LastSentAt should be set")
	}
	if !e.LastDeferredAt.IsZero() {
		t.Fatalf("LastDeferredAt should be zero after RecordAsSent")
	}
	if e.LastDeferredMessageRef != nil {
		t.Fatalf("LastDeferredMessageRef should be nil after RecordAsSent")
	}
}

func TestMemoryStore_LookupMiss(t *testing.T) {
	s := NewMemoryStore()
	_, ok, err := s.Lookup(context.Background(), "missing")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if ok {
		t.Fatalf("expected miss on unknown key")
	}
}

func TestMemoryStore_RecordAsSentOverwrites(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	_ = s.RecordAsSent(ctx, "k1", "hash-A")
	_ = s.RecordAsSent(ctx, "k1", "hash-B")

	e, _, _ := s.Lookup(ctx, "k1")
	if e.ContentHash != "hash-B" {
		t.Fatalf("ContentHash = %q, want hash-B", e.ContentHash)
	}
}

func TestMemoryStore_RecordAsDeferred(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	// RecordAsDeferred on an unknown key creates an entry with only
	// LastDeferredAt + LastDeferredMessageRef set.
	if err := s.RecordAsDeferred(ctx, "k1", []byte("ref-A")); err != nil {
		t.Fatalf("RecordAsDeferred: %v", err)
	}
	e, ok, err := s.Lookup(ctx, "k1")
	if err != nil || !ok {
		t.Fatalf("Lookup: ok=%v err=%v", ok, err)
	}
	if e.LastDeferredAt.IsZero() {
		t.Fatalf("LastDeferredAt should be set")
	}
	if !bytes.Equal(e.LastDeferredMessageRef, []byte("ref-A")) {
		t.Fatalf("LastDeferredMessageRef = %q, want ref-A", e.LastDeferredMessageRef)
	}
	if !e.LastSentAt.IsZero() || e.ContentHash != "" {
		t.Fatalf("Send-side fields should be untouched: %+v", e)
	}

	// RecordAsSent then RecordAsDeferred: send-side preserved,
	// deferred-side stamped.
	if err := s.RecordAsSent(ctx, "k2", "hash-A"); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}
	if err := s.RecordAsDeferred(ctx, "k2", []byte("ref-B")); err != nil {
		t.Fatalf("RecordAsDeferred: %v", err)
	}
	e2, _, _ := s.Lookup(ctx, "k2")
	if e2.ContentHash != "hash-A" {
		t.Fatalf("ContentHash lost: %q", e2.ContentHash)
	}
	if e2.LastSentAt.IsZero() {
		t.Fatalf("LastSentAt should be preserved by RecordAsDeferred")
	}
	if !e2.LastDeferredAt.After(e2.LastSentAt) {
		t.Fatalf("LastDeferredAt should be after LastSentAt; got sent=%v deferred=%v", e2.LastSentAt, e2.LastDeferredAt)
	}
	if !bytes.Equal(e2.LastDeferredMessageRef, []byte("ref-B")) {
		t.Fatalf("LastDeferredMessageRef = %q, want ref-B", e2.LastDeferredMessageRef)
	}

	// Subsequent RecordAsSent clears deferred-side fields.
	if err := s.RecordAsSent(ctx, "k2", "hash-B"); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}
	e3, _, _ := s.Lookup(ctx, "k2")
	if !e3.LastDeferredAt.IsZero() {
		t.Fatalf("LastDeferredAt should be reset by RecordAsSent; got %v", e3.LastDeferredAt)
	}
	if e3.LastDeferredMessageRef != nil {
		t.Fatalf("LastDeferredMessageRef should be cleared by RecordAsSent; got %q", e3.LastDeferredMessageRef)
	}
}

func TestMemoryStore_Concurrent(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "k" + string(rune('a'+i%26))
			_ = s.RecordAsSent(ctx, key, "content")
			_ = s.RecordAsDeferred(ctx, key, []byte("ref"))
			_, _, _ = s.Lookup(ctx, key)
		}(i)
	}
	wg.Wait()
}
