package memory

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/homemade/pith/sendstate"
)

// testTTL is generously larger than any window/sleep used in these
// tests (including the 24h peak window, so both Peak1h and Peak24h fold —
// see RecordAsSent's TTL>=window guard), so entries don't expire mid-test
// unless deliberately backdated.
const testTTL = 25 * time.Hour

func TestMemoryStore_RecordAsSentThenRead(t *testing.T) {
	s := New(testTTL)
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
	s := New(testTTL)
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
	s := New(testTTL)
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
	s := New(testTTL)
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

// FirstSentAt / FirstDeferredAt have set-once semantics: written on
// the first observation, preserved on every subsequent write. Pins
// each side independently and a cross-side sequence (defer-then-send
// must populate both, with FirstSentAt taken from the send not the
// metrics-doc-creation moment).
func TestMemoryStore_FirstSentAtAndFirstDeferredAt(t *testing.T) {
	s := New(testTTL)
	ctx := context.Background()

	// Send side: FirstSentAt set on first record, unchanged on second.
	_ = s.RecordAsSent(ctx, "k1", "h1")
	met, _, _ := s.ReadMetrics(ctx, "k1")
	first := met.FirstSentAt
	if first.IsZero() {
		t.Fatalf("FirstSentAt should be set after first send")
	}
	if !met.FirstSentAt.Equal(met.LastSentAt) {
		t.Fatalf("after first send FirstSentAt and LastSentAt should match: first=%v last=%v", met.FirstSentAt, met.LastSentAt)
	}

	time.Sleep(5 * time.Millisecond)
	_ = s.RecordAsSent(ctx, "k1", "h2")
	met, _, _ = s.ReadMetrics(ctx, "k1")
	if !met.FirstSentAt.Equal(first) {
		t.Fatalf("FirstSentAt should be unchanged on subsequent send, got %v want %v", met.FirstSentAt, first)
	}
	if !met.LastSentAt.After(first) {
		t.Fatalf("LastSentAt should advance past FirstSentAt: first=%v last=%v", first, met.LastSentAt)
	}

	// Defer side: FirstDeferredAt set on first record, unchanged on second.
	_ = s.RecordAsDeferred(ctx, "k2", []byte("ref-1"))
	met, _, _ = s.ReadMetrics(ctx, "k2")
	firstD := met.FirstDeferredAt
	if firstD.IsZero() {
		t.Fatalf("FirstDeferredAt should be set after first deferral")
	}

	time.Sleep(5 * time.Millisecond)
	_ = s.RecordAsDeferred(ctx, "k2", []byte("ref-2"))
	met, _, _ = s.ReadMetrics(ctx, "k2")
	if !met.FirstDeferredAt.Equal(firstD) {
		t.Fatalf("FirstDeferredAt should be unchanged on subsequent deferral, got %v want %v", met.FirstDeferredAt, firstD)
	}

	// Cross-side: defer creates the metrics doc, then a send happens.
	// FirstSentAt must be set from the send (not the prior deferral),
	// FirstDeferredAt must remain the deferral's timestamp.
	_ = s.RecordAsDeferred(ctx, "k3", []byte("ref"))
	met, _, _ = s.ReadMetrics(ctx, "k3")
	deferAt := met.FirstDeferredAt
	if !met.FirstSentAt.IsZero() {
		t.Fatalf("FirstSentAt should be zero when only deferrals have been recorded, got %v", met.FirstSentAt)
	}

	time.Sleep(5 * time.Millisecond)
	_ = s.RecordAsSent(ctx, "k3", "h")
	met, _, _ = s.ReadMetrics(ctx, "k3")
	if met.FirstSentAt.IsZero() {
		t.Fatalf("FirstSentAt should be set after the first send (even when the metrics doc was already created by a prior defer)")
	}
	if !met.FirstSentAt.After(deferAt) {
		t.Fatalf("FirstSentAt should reflect the send's timestamp, not the doc-creation moment: firstSent=%v deferAt=%v", met.FirstSentAt, deferAt)
	}
	if !met.FirstDeferredAt.Equal(deferAt) {
		t.Fatalf("FirstDeferredAt must be preserved across a later send: got %v want %v", met.FirstDeferredAt, deferAt)
	}
}

func TestEntry_CountDeferredInWindow(t *testing.T) {
	s := New(testTTL)
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
	if n := entry.CountSentInWindow(now, time.Hour); n != 1 {
		t.Fatalf("CountSentInWindow = %d, want 1", n)
	}
	// Tight window: trailing-edge "gone quiet" reads zero.
	if n := entry.CountDeferredInWindow(now, 0); n != 0 {
		t.Fatalf("CountDeferredInWindow(0) = %d, want 0", n)
	}
}

func TestEntry_Seen(t *testing.T) {
	s := New(testTTL)
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

func TestEntry_CountSentInWindow(t *testing.T) {
	s := New(testTTL)
	ctx := context.Background()

	entry, _ := s.ReadEntry(ctx, "missing")
	if n := entry.CountSentInWindow(time.Now(), time.Hour); n != 0 {
		t.Fatalf("CountSentInWindow on miss = %d, want 0", n)
	}

	for i := 0; i < 3; i++ {
		_ = s.RecordAsSent(ctx, "k1", "h")
	}
	entry, _ = s.ReadEntry(ctx, "k1")
	now := time.Now()
	if n := entry.CountSentInWindow(now, time.Hour); n != 3 {
		t.Fatalf("CountSentInWindow = %d, want 3", n)
	}
	if n := entry.CountSentInWindow(now, 0); n != 0 {
		t.Fatalf("CountSentInWindow(0) = %d, want 0", n)
	}
}

func TestEntry_CountSentInWindowExpiry(t *testing.T) {
	s := New(testTTL)
	ctx := context.Background()

	_ = s.RecordAsSent(ctx, "k1", "h")
	time.Sleep(25 * time.Millisecond)
	_ = s.RecordAsSent(ctx, "k1", "h")

	entry, _ := s.ReadEntry(ctx, "k1")
	now := time.Now()
	if n := entry.CountSentInWindow(now, 10*time.Millisecond); n != 1 {
		t.Fatalf("CountSentInWindow(10ms) = %d, want 1", n)
	}
	if n := entry.CountSentInWindow(now, 50*time.Millisecond); n != 2 {
		t.Fatalf("CountSentInWindow(50ms) = %d, want 2", n)
	}
}

func TestMemoryStore_LastNSendTimesCap(t *testing.T) {
	s := New(testTTL)
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
	s := New(testTTL)
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
	s := New(testTTL)
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

func TestMemoryStore_RangeDeferred(t *testing.T) {
	s := New(testTTL)
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
	_ = s.RangeDeferred(ctx, 0, func(key string, e sendstate.Entry) bool {
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
	_ = s.RangeDeferred(ctx, 2, func(key string, _ sendstate.Entry) bool {
		bounded = append(bounded, key)
		return true
	})
	if len(bounded) != 2 || bounded[0] != "k1" || bounded[1] != "k2" {
		t.Fatalf("RangeDeferred(limit=2) = %v, want [k1 k2]", bounded)
	}

	// fn returning false stops early.
	count := 0
	_ = s.RangeDeferred(ctx, 0, func(string, sendstate.Entry) bool {
		count++
		return false
	})
	if count != 1 {
		t.Fatalf("fn returning false should stop after 1, visited %d", count)
	}

	// A successful send on k2 makes it no longer pending.
	_ = s.RecordAsSent(ctx, "k2", "h")
	got = nil
	_ = s.RangeDeferred(ctx, 0, func(key string, _ sendstate.Entry) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 2 || got[0] != "k1" || got[1] != "k3" {
		t.Fatalf("after send on k2, pending = %v, want [k1 k3]", got)
	}
}

func TestMemoryStore_Concurrent(t *testing.T) {
	s := New(testTTL)
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

// TestMemoryStore_PeakHighWaterMarks: rapid sends all fall inside both
// rolling windows, so each peak tracks the running send count, and the
// first send seeds PeakedAt == LastSentAt.
func TestMemoryStore_PeakHighWaterMarks(t *testing.T) {
	s := New(testTTL)
	ctx := context.Background()

	// First send: peak is 1 in both windows, stamped at the send.
	_ = s.RecordAsSent(ctx, "k1", "h")
	met, _, _ := s.ReadMetrics(ctx, "k1")
	if met.Peak1h != 1 || met.Peak24h != 1 {
		t.Fatalf("after 1 send: Peak1h=%d Peak24h=%d, want 1/1", met.Peak1h, met.Peak24h)
	}
	if met.Peak1hAt.IsZero() || !met.Peak1hAt.Equal(met.LastSentAt) {
		t.Fatalf("after 1 send: Peak1hAt=%v should equal LastSentAt=%v", met.Peak1hAt, met.LastSentAt)
	}
	if !met.Peak24hAt.Equal(met.LastSentAt) {
		t.Fatalf("after 1 send: Peak24hAt=%v should equal LastSentAt=%v", met.Peak24hAt, met.LastSentAt)
	}

	// Four more rapid sends — all in window, so both peaks reach 5.
	for range 4 {
		_ = s.RecordAsSent(ctx, "k1", "h")
	}
	met, _, _ = s.ReadMetrics(ctx, "k1")
	if met.TotalSent != 5 {
		t.Fatalf("TotalSent=%d, want 5", met.TotalSent)
	}
	if met.Peak1h != 5 || met.Peak24h != 5 {
		t.Fatalf("after 5 sends: Peak1h=%d Peak24h=%d, want 5/5", met.Peak1h, met.Peak24h)
	}
}

// TestMemoryStore_PeakFreezesWhenWindowCountPlateaus is the distinguishing
// test: with the send-time list capped, the in-window count plateaus, so
// the peak stops rising and PeakedAt freezes at the last rise — diverging
// from LastSentAt, which keeps advancing. This is the whole point of a
// dedicated PeakedAt over reusing LastSentAt.
func TestMemoryStore_PeakFreezesWhenWindowCountPlateaus(t *testing.T) {
	s := New(testTTL)
	s.MaxSendTimes = 3 // count saturates at 3
	ctx := context.Background()

	// 2ms gaps so timestamps stay strictly ordered (also after the
	// millisecond truncation the Mongo mirror applies).
	for range 6 {
		_ = s.RecordAsSent(ctx, "k1", "h")
		time.Sleep(2 * time.Millisecond)
	}

	met, _, _ := s.ReadMetrics(ctx, "k1")
	if met.TotalSent != 6 {
		t.Fatalf("TotalSent=%d, want 6", met.TotalSent)
	}
	if met.Peak1h != 3 || met.Peak24h != 3 {
		t.Fatalf("Peak1h=%d Peak24h=%d, want 3/3 (capped count)", met.Peak1h, met.Peak24h)
	}
	// PeakedAt froze at the 3rd send; LastSentAt is the 6th. They must differ.
	if !met.Peak1hAt.Before(met.LastSentAt) {
		t.Fatalf("Peak1hAt=%v should be before LastSentAt=%v (peak froze, sends continued)", met.Peak1hAt, met.LastSentAt)
	}
	// Both windows plateaued on the same send, so their PeakedAt agree.
	if !met.Peak1hAt.Equal(met.Peak24hAt) {
		t.Fatalf("Peak1hAt=%v and Peak24hAt=%v should match (same plateau send)", met.Peak1hAt, met.Peak24hAt)
	}
}

// TestMemoryStore_PeaksSkippedAtDedupeFloor: at maxSendTimes == 1 the window
// count is a constant 1, so peak tracking is skipped and the fields stay
// unset — while the rest of the metrics still advance.
func TestMemoryStore_PeaksSkippedAtDedupeFloor(t *testing.T) {
	s := New(testTTL)
	s.MaxSendTimes = 1 // dedupe-only floor
	ctx := context.Background()

	for range 5 {
		_ = s.RecordAsSent(ctx, "k1", "h")
	}

	met, _, _ := s.ReadMetrics(ctx, "k1")
	if met.TotalSent != 5 || met.LastSentAt.IsZero() {
		t.Fatalf("non-peak metrics should still advance: TotalSent=%d LastSentAt=%v", met.TotalSent, met.LastSentAt)
	}
	if met.Peak1h != 0 || met.Peak24h != 0 {
		t.Fatalf("Peak1h=%d Peak24h=%d, want 0/0 (skipped at floor)", met.Peak1h, met.Peak24h)
	}
	if !met.Peak1hAt.IsZero() || !met.Peak24hAt.IsZero() {
		t.Fatalf("PeakedAt should be zero at floor, got %v / %v", met.Peak1hAt, met.Peak24hAt)
	}
}

// TestMemoryStore_PeakSkippedWhenTTLBelowWindow: a window's peak is only
// folded when the Entry TTL covers it. A 2h-TTL store (e.g. the Raisely
// write-back gate) retains less than 24h of send history, so Peak24h would
// be a misleading lower bound that silently resets on idle gaps — it must
// stay unset (absent, distinguishable from a real 0), while Peak1h still
// folds because TTL >= 1h.
func TestMemoryStore_PeakSkippedWhenTTLBelowWindow(t *testing.T) {
	s := New(2 * time.Hour) // covers the 1h window, not the 24h one
	s.MaxSendTimes = 5      // above the dedupe floor, so peaks are eligible
	ctx := context.Background()

	for range 3 {
		_ = s.RecordAsSent(ctx, "k1", "h")
	}

	met, _, _ := s.ReadMetrics(ctx, "k1")
	if met.TotalSent != 3 {
		t.Fatalf("TotalSent=%d, want 3", met.TotalSent)
	}
	if met.Peak1h != 3 || met.Peak1hAt.IsZero() {
		t.Fatalf("Peak1h should fold (TTL >= 1h): Peak1h=%d Peak1hAt=%v", met.Peak1h, met.Peak1hAt)
	}
	if met.Peak24h != 0 || !met.Peak24hAt.IsZero() {
		t.Fatalf("Peak24h must be unset (TTL < 24h): Peak24h=%d Peak24hAt=%v", met.Peak24h, met.Peak24hAt)
	}
}
