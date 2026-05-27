package sendstate

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"
)

// testTTL is generously larger than any window/sleep used in these
// tests, so entries don't expire mid-test unless deliberately backdated.
const testTTL = time.Hour

func TestMemoryStore_RecordAsSentThenRead(t *testing.T) {
	s := NewMemoryStore(testTTL)
	ctx := context.Background()

	if err := s.RecordAsSent(ctx, "k1", "hash-A"); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}

	entry, err := s.ReadEntry(ctx, "k1")
	if err != nil {
		t.Fatalf("ReadEntry: %v", err)
	}
	if entry.ContentHash != "hash-A" {
		t.Fatalf("Entry.ContentHash = %q, want hash-A", entry.ContentHash)
	}
	if len(entry.LastNSendTimes) != 1 {
		t.Fatalf("Entry.LastNSendTimes len = %d, want 1", len(entry.LastNSendTimes))
	}
	if entry.LastDeferredMessageRef != nil {
		t.Fatalf("LastDeferredMessageRef should be nil after RecordAsSent")
	}

	met, ok, err := s.ReadMetrics(ctx, "k1")
	if err != nil || !ok {
		t.Fatalf("ReadMetrics: ok=%v err=%v", ok, err)
	}
	if met.TotalSent != 1 {
		t.Fatalf("Metrics.TotalSent = %d, want 1", met.TotalSent)
	}
	if met.LastSentAt.IsZero() {
		t.Fatalf("Metrics.LastSentAt should be set")
	}
	if !met.LastDeferredAt.IsZero() {
		t.Fatalf("Metrics.LastDeferredAt should be zero after RecordAsSent")
	}
}

func TestMemoryStore_ReadMiss(t *testing.T) {
	s := NewMemoryStore(testTTL)
	ctx := context.Background()

	entry, err := s.ReadEntry(ctx, "missing")
	if err != nil {
		t.Fatalf("ReadEntry: %v", err)
	}
	if entry.ContentHash != "" || len(entry.LastNSendTimes) != 0 {
		t.Fatalf("miss should return zero Entry, got %+v", entry)
	}
	if _, ok, _ := s.ReadMetrics(ctx, "missing"); ok {
		t.Fatalf("ReadMetrics should miss on unknown key")
	}
}

func TestMemoryStore_RecordAsSentOverwrites(t *testing.T) {
	s := NewMemoryStore(testTTL)
	ctx := context.Background()

	_ = s.RecordAsSent(ctx, "k1", "hash-A")
	_ = s.RecordAsSent(ctx, "k1", "hash-B")

	entry, _ := s.ReadEntry(ctx, "k1")
	if entry.ContentHash != "hash-B" {
		t.Fatalf("ContentHash = %q, want hash-B", entry.ContentHash)
	}
	if len(entry.LastNSendTimes) != 2 {
		t.Fatalf("LastNSendTimes len = %d, want 2", len(entry.LastNSendTimes))
	}
	met, _, _ := s.ReadMetrics(ctx, "k1")
	if met.TotalSent != 2 {
		t.Fatalf("TotalSent = %d, want 2", met.TotalSent)
	}
}

func TestMemoryStore_RecordAsDeferred(t *testing.T) {
	s := NewMemoryStore(testTTL)
	ctx := context.Background()

	if err := s.RecordAsDeferred(ctx, "k1", []byte("ref-A")); err != nil {
		t.Fatalf("RecordAsDeferred: %v", err)
	}
	entry, _ := s.ReadEntry(ctx, "k1")
	if !bytes.Equal(entry.LastDeferredMessageRef, []byte("ref-A")) {
		t.Fatalf("LastDeferredMessageRef = %q, want ref-A", entry.LastDeferredMessageRef)
	}
	if entry.ContentHash != "" || len(entry.LastNSendTimes) != 0 {
		t.Fatalf("send-side Entry fields should be untouched: %+v", entry)
	}
	met, _, _ := s.ReadMetrics(ctx, "k1")
	if met.LastDeferredAt.IsZero() || !met.LastSentAt.IsZero() {
		t.Fatalf("only deferred-side metrics should be set: %+v", met)
	}

	_ = s.RecordAsSent(ctx, "k2", "hash-A")
	_ = s.RecordAsDeferred(ctx, "k2", []byte("ref-B"))

	entry2, _ := s.ReadEntry(ctx, "k2")
	if entry2.ContentHash != "hash-A" {
		t.Fatalf("ContentHash lost: %q", entry2.ContentHash)
	}
	if !bytes.Equal(entry2.LastDeferredMessageRef, []byte("ref-B")) {
		t.Fatalf("LastDeferredMessageRef = %q, want ref-B", entry2.LastDeferredMessageRef)
	}
	met2, _, _ := s.ReadMetrics(ctx, "k2")
	if met2.LastSentAt.IsZero() {
		t.Fatalf("LastSentAt should be preserved by RecordAsDeferred")
	}
	if !met2.LastDeferredAt.After(met2.LastSentAt) {
		t.Fatalf("LastDeferredAt should be after LastSentAt")
	}

	// Subsequent RecordAsSent does NOT clear the breadcrumb — it just
	// makes the send the most recent event, so the deferral is no longer
	// pending (LastSentAt > LastDeferredAt). The stale ref is harmless.
	_ = s.RecordAsSent(ctx, "k2", "hash-B")
	entry3, _ := s.ReadEntry(ctx, "k2")
	if !bytes.Equal(entry3.LastDeferredMessageRef, []byte("ref-B")) {
		t.Fatalf("ref should be preserved (not cleared) by RecordAsSent, got %q", entry3.LastDeferredMessageRef)
	}
	met3, _, _ := s.ReadMetrics(ctx, "k2")
	if met3.LastDeferredAt.IsZero() {
		t.Fatalf("LastDeferredAt should be preserved by RecordAsSent")
	}
	if !met3.LastSentAt.After(met3.LastDeferredAt) {
		t.Fatalf("send should supersede the deferral by recency (no longer pending)")
	}
}

func TestEntry_CountDeferredInWindow(t *testing.T) {
	s := NewMemoryStore(testTTL)
	ctx := context.Background()

	entry, _ := s.ReadEntry(ctx, "missing")
	if n := entry.CountDeferredInWindow(time.Now(), time.Hour); n != 0 {
		t.Fatalf("CountDeferredInWindow on miss = %d, want 0", n)
	}

	// Two deferrals; a send in between must NOT clear the deferral list.
	_ = s.RecordAsDeferred(ctx, "k1", []byte("r1"))
	_ = s.RecordAsSent(ctx, "k1", "h")
	_ = s.RecordAsDeferred(ctx, "k1", []byte("r2"))

	entry, _ = s.ReadEntry(ctx, "k1")
	now := time.Now()
	if n := entry.CountDeferredInWindow(now, time.Hour); n != 2 {
		t.Fatalf("CountDeferredInWindow = %d, want 2 (send must not clear deferrals)", n)
	}
	// Sends stay on their own list.
	if n := entry.CountInWindow(now, time.Hour); n != 1 {
		t.Fatalf("CountInWindow = %d, want 1", n)
	}
	// Tight window: trailing-edge "gone quiet" reads zero.
	if n := entry.CountDeferredInWindow(now, 0); n != 0 {
		t.Fatalf("CountDeferredInWindow(0) = %d, want 0", n)
	}
}

func TestEntry_Seen(t *testing.T) {
	s := NewMemoryStore(testTTL)
	ctx := context.Background()

	entry, _ := s.ReadEntry(ctx, "k1")
	if entry.Seen("") || entry.Seen("hash-A") {
		t.Fatalf("fresh entry should not report Seen")
	}

	_ = s.RecordAsSent(ctx, "k1", "hash-A")
	entry, _ = s.ReadEntry(ctx, "k1")
	if !entry.Seen("hash-A") {
		t.Fatalf("same content should be Seen after a send")
	}
	if entry.Seen("hash-B") {
		t.Fatalf("different content should not be Seen")
	}

	_ = s.RecordAsDeferred(ctx, "k2", []byte("ref"))
	entry, _ = s.ReadEntry(ctx, "k2")
	if entry.Seen("") {
		t.Fatalf("deferral-only entry should not report Seen")
	}
}

func TestEntry_CountInWindow(t *testing.T) {
	s := NewMemoryStore(testTTL)
	ctx := context.Background()

	entry, _ := s.ReadEntry(ctx, "missing")
	if n := entry.CountInWindow(time.Now(), time.Hour); n != 0 {
		t.Fatalf("CountInWindow on miss = %d, want 0", n)
	}

	for i := 0; i < 3; i++ {
		_ = s.RecordAsSent(ctx, "k1", "h")
	}
	entry, _ = s.ReadEntry(ctx, "k1")
	now := time.Now()
	if n := entry.CountInWindow(now, time.Hour); n != 3 {
		t.Fatalf("CountInWindow = %d, want 3", n)
	}
	if n := entry.CountInWindow(now, 0); n != 0 {
		t.Fatalf("CountInWindow(0) = %d, want 0", n)
	}
}

func TestEntry_CountInWindowExpiry(t *testing.T) {
	s := NewMemoryStore(testTTL)
	ctx := context.Background()

	_ = s.RecordAsSent(ctx, "k1", "h")
	time.Sleep(25 * time.Millisecond)
	_ = s.RecordAsSent(ctx, "k1", "h")

	entry, _ := s.ReadEntry(ctx, "k1")
	now := time.Now()
	if n := entry.CountInWindow(now, 10*time.Millisecond); n != 1 {
		t.Fatalf("CountInWindow(10ms) = %d, want 1", n)
	}
	if n := entry.CountInWindow(now, 50*time.Millisecond); n != 2 {
		t.Fatalf("CountInWindow(50ms) = %d, want 2", n)
	}
}

func TestMemoryStore_LastNSendTimesCap(t *testing.T) {
	s := NewMemoryStore(testTTL)
	s.MaxSendTimes = 3
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		_ = s.RecordAsSent(ctx, "k1", "h")
	}

	entry, _ := s.ReadEntry(ctx, "k1")
	if got := len(entry.LastNSendTimes); got != 3 {
		t.Fatalf("LastNSendTimes len = %d, want 3 (capped)", got)
	}
	for i := 1; i < len(entry.LastNSendTimes); i++ {
		if entry.LastNSendTimes[i].Before(entry.LastNSendTimes[i-1]) {
			t.Fatalf("LastNSendTimes not chronological: %v", entry.LastNSendTimes)
		}
	}
	met, _, _ := s.ReadMetrics(ctx, "k1")
	if met.TotalSent != 10 {
		t.Fatalf("TotalSent = %d, want 10 (cap doesn't affect lifetime count)", met.TotalSent)
	}
}

func TestMemoryStore_ReadEntryHonorsTTL(t *testing.T) {
	s := NewMemoryStore(testTTL)
	ctx := context.Background()
	_ = s.RecordAsSent(ctx, "k1", "hash-A")

	// Backdate the record's expiry so it's expired but not yet swept.
	v, _ := s.entries.Load("k1")
	r := v.(*entryRecord)
	s.entries.Store("k1", &entryRecord{entry: r.entry, expireAt: time.Now().Add(-time.Minute)})

	entry, _ := s.ReadEntry(ctx, "k1")
	if entry.ContentHash != "" || len(entry.LastNSendTimes) != 0 {
		t.Fatalf("expired entry should read as zero, got %+v", entry)
	}
	met, ok, _ := s.ReadMetrics(ctx, "k1")
	if !ok || met.TotalSent != 1 {
		t.Fatalf("Metrics should survive Entry TTL: ok=%v TotalSent=%d", ok, met.TotalSent)
	}
}

func TestMemoryStore_SweepDeletesExpiredEntriesKeepsMetrics(t *testing.T) {
	s := NewMemoryStore(testTTL)
	ctx := context.Background()
	_ = s.RecordAsSent(ctx, "k1", "hash-A")

	v, _ := s.entries.Load("k1")
	r := v.(*entryRecord)
	s.entries.Store("k1", &entryRecord{entry: r.entry, expireAt: time.Now().Add(-time.Minute)})

	s.sweep()

	if _, ok := s.entries.Load("k1"); ok {
		t.Fatalf("sweep should delete the expired entry record")
	}
	if _, ok := s.metrics.Load("k1"); !ok {
		t.Fatalf("sweep must not touch the metrics record")
	}

	_ = s.RecordAsSent(ctx, "k1", "hash-B")
	met, _, _ := s.ReadMetrics(ctx, "k1")
	if met.TotalSent != 2 {
		t.Fatalf("TotalSent should continue from retained metrics = 2, got %d", met.TotalSent)
	}
}

func TestMemoryStore_RaisePeaks(t *testing.T) {
	s := NewMemoryStore(testTTL)
	ctx := context.Background()

	_ = s.RaisePeaks(ctx, "k1", map[string]uint64{"at cap": 3, "burst": 2})
	met, ok, _ := s.ReadMetrics(ctx, "k1")
	if !ok || met.PeakSendsInWindow["at cap"] != 3 || met.PeakSendsInWindow["burst"] != 2 {
		t.Fatalf("after first raise: %+v (ok=%v)", met.PeakSendsInWindow, ok)
	}

	_ = s.RaisePeaks(ctx, "k1", map[string]uint64{"at cap": 5, "burst": 1, "daily": 10})
	met, _, _ = s.ReadMetrics(ctx, "k1")
	if met.PeakSendsInWindow["at cap"] != 5 {
		t.Fatalf(`"at cap" should rise to 5, got %d`, met.PeakSendsInWindow["at cap"])
	}
	if met.PeakSendsInWindow["burst"] != 2 {
		t.Fatalf(`"burst" should stay 2 (lower ignored), got %d`, met.PeakSendsInWindow["burst"])
	}
	if met.PeakSendsInWindow["daily"] != 10 {
		t.Fatalf(`"daily" should be added as 10, got %d`, met.PeakSendsInWindow["daily"])
	}

	_ = s.RecordAsSent(ctx, "k1", "h")
	met, _, _ = s.ReadMetrics(ctx, "k1")
	if met.TotalSent != 1 {
		t.Fatalf("TotalSent = %d, want 1", met.TotalSent)
	}
	if met.PeakSendsInWindow["at cap"] != 5 {
		t.Fatalf("RecordAsSent must preserve peaks, got %+v", met.PeakSendsInWindow)
	}

	met.PeakSendsInWindow["at cap"] = 999
	again, _, _ := s.ReadMetrics(ctx, "k1")
	if again.PeakSendsInWindow["at cap"] != 5 {
		t.Fatalf("stored peak mutated through returned map: %d", again.PeakSendsInWindow["at cap"])
	}
}

func TestMemoryStore_RangeDeferred(t *testing.T) {
	s := NewMemoryStore(testTTL)
	ctx := context.Background()

	// k-not-pending: deferral then a later send → superseded (excluded).
	_ = s.RecordAsDeferred(ctx, "k-not-pending", []byte("x"))
	_ = s.RecordAsSent(ctx, "k-not-pending", "h")

	// k-sent-only: only a send, never deferred (excluded).
	_ = s.RecordAsSent(ctx, "k-sent-only", "h")

	// Three pending keys, deferred in order so oldest-first is k1, k2, k3.
	_ = s.RecordAsDeferred(ctx, "k1", []byte("r1"))
	time.Sleep(2 * time.Millisecond)
	_ = s.RecordAsDeferred(ctx, "k2", []byte("r2"))
	time.Sleep(2 * time.Millisecond)
	_ = s.RecordAsDeferred(ctx, "k3", []byte("r3"))

	// Unbounded: only the pending keys, oldest-first.
	var got []string
	_ = s.RangeDeferred(ctx, 0, func(key string, e Entry) bool {
		got = append(got, key)
		if string(e.LastDeferredMessageRef) == "" {
			t.Fatalf("%s yielded without a breadcrumb", key)
		}
		return true
	})
	want := []string{"k1", "k2", "k3"}
	if len(got) != 3 || got[0] != "k1" || got[1] != "k2" || got[2] != "k3" {
		t.Fatalf("RangeDeferred order = %v, want %v (oldest-pending first, no non-pending keys)", got, want)
	}

	// Bounded: limit picks the oldest N.
	var bounded []string
	_ = s.RangeDeferred(ctx, 2, func(key string, _ Entry) bool {
		bounded = append(bounded, key)
		return true
	})
	if len(bounded) != 2 || bounded[0] != "k1" || bounded[1] != "k2" {
		t.Fatalf("RangeDeferred(limit=2) = %v, want [k1 k2]", bounded)
	}

	// fn returning false stops early.
	count := 0
	_ = s.RangeDeferred(ctx, 0, func(string, Entry) bool {
		count++
		return false
	})
	if count != 1 {
		t.Fatalf("fn returning false should stop after 1, visited %d", count)
	}

	// A successful send on k2 makes it no longer pending.
	_ = s.RecordAsSent(ctx, "k2", "h")
	got = nil
	_ = s.RangeDeferred(ctx, 0, func(key string, _ Entry) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 2 || got[0] != "k1" || got[1] != "k3" {
		t.Fatalf("after send on k2, pending = %v, want [k1 k3]", got)
	}
}

func TestMemoryStore_Concurrent(t *testing.T) {
	s := NewMemoryStore(testTTL)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "k" + string(rune('a'+i%26))
			_ = s.RecordAsSent(ctx, key, "content")
			_ = s.RecordAsDeferred(ctx, key, []byte("ref"))
			_, _ = s.ReadEntry(ctx, key)
			_, _, _ = s.ReadMetrics(ctx, key)
		}(i)
	}
	wg.Wait()
}
