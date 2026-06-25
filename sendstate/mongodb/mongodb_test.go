package mongodb

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"

	"github.com/homemade/pith/sendstate"
)

// testClient is the shared driver client for the package's container,
// or nil when Docker is unavailable (every test then skips).
var (
	testClient *mongo.Client
	dbCounter  atomic.Uint64
)

func TestMain(m *testing.M) { os.Exit(run(m)) }

// run spins up one MongoDB container for the whole package and returns
// the test exit code. It's split out of TestMain so its defers (container
// + client teardown) actually run — os.Exit in TestMain would skip them.
//
// A standalone mongod is enough — the store uses no transactions, and
// $slice/$expr plus TTL-honoring reads all work without a replica set.
// If the container can't start (no Docker), testClient stays nil and
// every test t.Skip()s rather than fails.
func run(m *testing.M) int {
	ctx := context.Background()

	container, err := tcmongo.Run(ctx, "mongo:7")
	if err != nil {
		fmt.Printf("mongodb tests skipped: cannot start container: %v\n", err)
		return m.Run() // testClient == nil → every test t.Skip()s
	}
	defer func() { _ = container.Terminate(ctx) }()

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		fmt.Printf("mongodb tests skipped: connection string: %v\n", err)
		return m.Run()
	}

	// Majority write concern is what the store doc recommends for
	// cross-instance correctness; exercise that configuration here.
	// (v2's ApplyURI accepts the module's trailing-slash connection
	// string as-is — no need to trim it.)
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetWriteConcern(writeconcern.Majority()))
	if err != nil {
		fmt.Printf("mongodb tests skipped: connect: %v\n", err)
		return m.Run()
	}
	defer func() { _ = client.Disconnect(ctx) }()

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		fmt.Printf("mongodb tests skipped: ping: %v\n", err)
		return m.Run()
	}
	testClient = client
	return m.Run()
}

// newStore returns a Store over a fresh, isolated database (so each
// test's entries/metrics collections are independent), with indexes
// ensured. Skips the test when no container is available.
func newStore(t *testing.T, ttl time.Duration, opts ...Option) *Store {
	t.Helper()
	if testClient == nil {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	db := testClient.Database(fmt.Sprintf("pithtest_%d", dbCounter.Add(1)))
	t.Cleanup(func() { _ = db.Drop(context.Background()) })
	s := New(db, ttl, opts...)
	if err := s.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	return s
}

func TestStore_RecordAsSentThenRead(t *testing.T) {
	s := newStore(t, time.Hour)
	ctx := context.Background()

	if err := s.RecordAsSent(ctx, "k1", "", "", "hash-A"); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}

	entry, err := s.ReadEntry(ctx, "k1")
	if err != nil {
		t.Fatalf("ReadEntry: %v", err)
	}
	if entry.ContentHash != "hash-A" {
		t.Fatalf("ContentHash = %q, want hash-A", entry.ContentHash)
	}
	if len(entry.LastNSendTimes) != 1 {
		t.Fatalf("LastNSendTimes len = %d, want 1", len(entry.LastNSendTimes))
	}
	if entry.LastDeferredMessageRef != nil {
		t.Fatalf("LastDeferredMessageRef should be nil after RecordAsSent")
	}

	met, ok, err := s.ReadMetrics(ctx, "k1")
	if err != nil || !ok {
		t.Fatalf("ReadMetrics: ok=%v err=%v", ok, err)
	}
	if met.TotalSent != 1 {
		t.Fatalf("TotalSent = %d, want 1", met.TotalSent)
	}
	if met.LastSentAt.IsZero() {
		t.Fatalf("LastSentAt should be set")
	}
	if !met.LastDeferredAt.IsZero() {
		t.Fatalf("LastDeferredAt should be zero after RecordAsSent")
	}
}

func TestStore_ReadMiss(t *testing.T) {
	s := newStore(t, time.Hour)
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

func TestStore_Seen(t *testing.T) {
	// Pass a cap (as a protect-driven deployment would) so the send list
	// retains both writes; the default maxSendTimes is 1 (dedupe-only
	// floor), under which $slice would keep only the most recent.
	s := newStore(t, time.Hour, WithMaxSendTimes(10))
	ctx := context.Background()

	entry, _ := s.ReadEntry(ctx, "k1")
	if entry.Seen("hash-A") {
		t.Fatalf("fresh entry should not report Seen")
	}

	_ = s.RecordAsSent(ctx, "k1", "", "", "hash-A")
	_ = s.RecordAsSent(ctx, "k1", "", "", "hash-B")

	entry, _ = s.ReadEntry(ctx, "k1")
	if entry.ContentHash != "hash-B" {
		t.Fatalf("ContentHash = %q, want hash-B (last write wins)", entry.ContentHash)
	}
	if !entry.Seen("hash-B") || entry.Seen("hash-A") {
		t.Fatalf("Seen should match only the most recent content")
	}
	if len(entry.LastNSendTimes) != 2 {
		t.Fatalf("LastNSendTimes len = %d, want 2", len(entry.LastNSendTimes))
	}
	met, _, _ := s.ReadMetrics(ctx, "k1")
	if met.TotalSent != 2 {
		t.Fatalf("TotalSent = %d, want 2", met.TotalSent)
	}
}

func TestStore_RecordAsDeferred(t *testing.T) {
	s := newStore(t, time.Hour)
	ctx := context.Background()

	// Send then defer: the deferral is now pending and the breadcrumb
	// is stored; send-side state is preserved.
	_ = s.RecordAsSent(ctx, "k1", "", "", "hash-A")
	if err := s.RecordAsDeferred(ctx, "k1", "", "", []byte("ref-A")); err != nil {
		t.Fatalf("RecordAsDeferred: %v", err)
	}
	entry, _ := s.ReadEntry(ctx, "k1")
	if entry.ContentHash != "hash-A" {
		t.Fatalf("ContentHash lost by RecordAsDeferred: %q", entry.ContentHash)
	}
	if !bytes.Equal(entry.LastDeferredMessageRef, []byte("ref-A")) {
		t.Fatalf("LastDeferredMessageRef = %q, want ref-A", entry.LastDeferredMessageRef)
	}
	met, _, _ := s.ReadMetrics(ctx, "k1")
	if met.LastSentAt.IsZero() || !met.LastDeferredAt.After(met.LastSentAt) {
		t.Fatalf("LastDeferredAt should be after LastSentAt: %+v", met)
	}

	// A later send supersedes the deferral by recency — the breadcrumb
	// is NOT cleared (just no longer pending).
	_ = s.RecordAsSent(ctx, "k1", "", "", "hash-B")
	entry, _ = s.ReadEntry(ctx, "k1")
	if !bytes.Equal(entry.LastDeferredMessageRef, []byte("ref-A")) {
		t.Fatalf("ref should be preserved (not cleared) by RecordAsSent, got %q", entry.LastDeferredMessageRef)
	}
	met, _, _ = s.ReadMetrics(ctx, "k1")
	if !met.LastSentAt.After(met.LastDeferredAt) {
		t.Fatalf("send should supersede the deferral by recency")
	}
}

// FirstSentAt / FirstDeferredAt have set-once semantics, implemented
// via Mongo's $min operator (writes new value if missing, otherwise
// keeps the smaller — and since clock-monotonic now is always >= the
// stored value, the stored one always wins on subsequent writes).
// Mirrors the memory backend's test.
func TestStore_FirstSentAtAndFirstDeferredAt(t *testing.T) {
	s := newStore(t, time.Hour, WithMaxSendTimes(10))
	ctx := context.Background()

	// Send side.
	_ = s.RecordAsSent(ctx, "k1", "", "", "h1")
	met, _, _ := s.ReadMetrics(ctx, "k1")
	first := met.FirstSentAt
	if first.IsZero() {
		t.Fatalf("FirstSentAt should be set after first send")
	}

	time.Sleep(10 * time.Millisecond)
	_ = s.RecordAsSent(ctx, "k1", "", "", "h2")
	met, _, _ = s.ReadMetrics(ctx, "k1")
	// Mongo stores timestamps at millisecond precision, so use ~Equal rather
	// than strict equality after a round-trip.
	if !met.FirstSentAt.Equal(first) {
		t.Fatalf("FirstSentAt should be unchanged ($min preserves the smaller stored value), got %v want %v", met.FirstSentAt, first)
	}
	if !met.LastSentAt.After(first) {
		t.Fatalf("LastSentAt should advance past FirstSentAt: first=%v last=%v", first, met.LastSentAt)
	}

	// Defer side.
	_ = s.RecordAsDeferred(ctx, "k2", "", "", []byte("ref-1"))
	met, _, _ = s.ReadMetrics(ctx, "k2")
	firstD := met.FirstDeferredAt
	if firstD.IsZero() {
		t.Fatalf("FirstDeferredAt should be set after first deferral")
	}

	time.Sleep(10 * time.Millisecond)
	_ = s.RecordAsDeferred(ctx, "k2", "", "", []byte("ref-2"))
	met, _, _ = s.ReadMetrics(ctx, "k2")
	if !met.FirstDeferredAt.Equal(firstD) {
		t.Fatalf("FirstDeferredAt should be unchanged on subsequent deferral, got %v want %v", met.FirstDeferredAt, firstD)
	}

	// Cross-side: deferral creates the metrics doc, then a send fires.
	// FirstSentAt must reflect the send's timestamp (set via $min on a
	// previously-missing field — the prior defer didn't write it).
	_ = s.RecordAsDeferred(ctx, "k3", "", "", []byte("ref"))
	met, _, _ = s.ReadMetrics(ctx, "k3")
	deferAt := met.FirstDeferredAt
	if !met.FirstSentAt.IsZero() {
		t.Fatalf("FirstSentAt should be zero when only deferrals have been recorded, got %v", met.FirstSentAt)
	}

	time.Sleep(10 * time.Millisecond)
	_ = s.RecordAsSent(ctx, "k3", "", "", "h")
	met, _, _ = s.ReadMetrics(ctx, "k3")
	if met.FirstSentAt.IsZero() {
		t.Fatalf("FirstSentAt should be set after the first send")
	}
	if !met.FirstSentAt.After(deferAt) {
		t.Fatalf("FirstSentAt should reflect the send's timestamp, not the metrics-doc creation moment: firstSent=%v deferAt=%v", met.FirstSentAt, deferAt)
	}
	if !met.FirstDeferredAt.Equal(deferAt) {
		t.Fatalf("FirstDeferredAt must be preserved across a later send: got %v want %v", met.FirstDeferredAt, deferAt)
	}
}

func TestStore_LastNSendTimesCap(t *testing.T) {
	s := newStore(t, time.Hour, WithMaxSendTimes(3))
	ctx := context.Background()

	for range 10 {
		_ = s.RecordAsSent(ctx, "k1", "", "", "h")
	}

	entry, _ := s.ReadEntry(ctx, "k1")
	if got := len(entry.LastNSendTimes); got != 3 {
		t.Fatalf("LastNSendTimes len = %d, want 3 ($slice cap)", got)
	}
	if n := entry.CountSentInWindow(time.Now(), time.Hour); n != 3 {
		t.Fatalf("CountSentInWindow = %d, want 3", n)
	}
	met, _, _ := s.ReadMetrics(ctx, "k1")
	if met.TotalSent != 10 {
		t.Fatalf("TotalSent = %d, want 10 (cap doesn't affect lifetime count)", met.TotalSent)
	}
}

func TestStore_ReadEntryHonorsTTL(t *testing.T) {
	// Tiny TTL so the written record's expireAt passes during the test;
	// ReadEntry filters expireAt > now, so it reads as absent even
	// before Mongo's background deleter runs.
	s := newStore(t, 50*time.Millisecond)
	ctx := context.Background()
	_ = s.RecordAsSent(ctx, "k1", "", "", "hash-A")

	time.Sleep(150 * time.Millisecond)

	entry, err := s.ReadEntry(ctx, "k1")
	if err != nil {
		t.Fatalf("ReadEntry: %v", err)
	}
	if entry.ContentHash != "" || len(entry.LastNSendTimes) != 0 {
		t.Fatalf("expired entry should read as zero, got %+v", entry)
	}
	// Metrics survive the Entry TTL (separate, never-expiring collection).
	met, ok, _ := s.ReadMetrics(ctx, "k1")
	if !ok || met.TotalSent != 1 {
		t.Fatalf("Metrics should survive Entry TTL: ok=%v TotalSent=%d", ok, met.TotalSent)
	}
}

func TestStore_RangeDeferred(t *testing.T) {
	s := newStore(t, time.Hour)
	ctx := context.Background()

	// Not pending: deferral then a later send (superseded).
	_ = s.RecordAsDeferred(ctx, "k-superseded", "", "", []byte("x"))
	_ = s.RecordAsSent(ctx, "k-superseded", "", "", "h")
	// Not pending: only ever sent.
	_ = s.RecordAsSent(ctx, "k-sent-only", "", "", "h")

	// Three pending, deferred oldest→newest.
	_ = s.RecordAsDeferred(ctx, "k1", "", "", []byte("r1"))
	time.Sleep(5 * time.Millisecond)
	_ = s.RecordAsDeferred(ctx, "k2", "", "", []byte("r2"))
	time.Sleep(5 * time.Millisecond)
	_ = s.RecordAsDeferred(ctx, "k3", "", "", []byte("r3"))

	var got []string
	if err := s.RangeDeferred(ctx, 0, "", func(key string, e sendstate.Entry) bool {
		got = append(got, key)
		if len(e.LastDeferredMessageRef) == 0 {
			t.Errorf("%s yielded without a breadcrumb", key)
		}
		return true
	}); err != nil {
		t.Fatalf("RangeDeferred: %v", err)
	}
	if len(got) != 3 || got[0] != "k1" || got[1] != "k2" || got[2] != "k3" {
		t.Fatalf("RangeDeferred = %v, want [k1 k2 k3] (oldest-pending first, no non-pending keys)", got)
	}

	// Bounded picks the oldest N.
	var bounded []string
	_ = s.RangeDeferred(ctx, 2, "", func(key string, _ sendstate.Entry) bool {
		bounded = append(bounded, key)
		return true
	})
	if len(bounded) != 2 || bounded[0] != "k1" || bounded[1] != "k2" {
		t.Fatalf("RangeDeferred(limit=2) = %v, want [k1 k2]", bounded)
	}

	// A send on k2 makes it no longer pending.
	_ = s.RecordAsSent(ctx, "k2", "", "", "h")
	got = nil
	_ = s.RangeDeferred(ctx, 0, "", func(key string, _ sendstate.Entry) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 2 || got[0] != "k1" || got[1] != "k3" {
		t.Fatalf("after send on k2, pending = %v, want [k1 k3]", got)
	}
}

// A namespace-scoped RangeDeferred visits only its own namespace (served by the
// {namespace, lastDeferredAt} index), with limit applied within it; "" spans all.
func TestStore_RangeDeferred_Namespace(t *testing.T) {
	s := newStore(t, time.Hour)
	ctx := context.Background()

	// tenant-a deferred oldest, so a namespace-blind oldest-first sweep would
	// surface them before tenant-b's b1.
	_ = s.RecordAsDeferred(ctx, "a1", "", "tenant-a", []byte("ra1"))
	time.Sleep(5 * time.Millisecond)
	_ = s.RecordAsDeferred(ctx, "a2", "", "tenant-a", []byte("ra2"))
	time.Sleep(5 * time.Millisecond)
	_ = s.RecordAsDeferred(ctx, "a3", "", "tenant-a", []byte("ra3"))
	time.Sleep(5 * time.Millisecond)
	_ = s.RecordAsDeferred(ctx, "b1", "", "tenant-b", []byte("rb1"))
	time.Sleep(5 * time.Millisecond)
	_ = s.RecordAsDeferred(ctx, "u1", "", "", []byte("ru1"))

	collect := func(limit int, namespace string) []string {
		var ks []string
		_ = s.RangeDeferred(ctx, limit, namespace, func(k string, _ sendstate.Entry) bool {
			ks = append(ks, k)
			return true
		})
		return ks
	}

	// tenant-b's sweep sees only b1, despite tenant-a's older deferrals.
	if b := collect(0, "tenant-b"); len(b) != 1 || b[0] != "b1" {
		t.Fatalf("namespace tenant-b = %v, want [b1] (not blocked by tenant-a)", b)
	}
	// tenant-a oldest-first, limit applies within the namespace.
	if a := collect(2, "tenant-a"); len(a) != 2 || a[0] != "a1" || a[1] != "a2" {
		t.Fatalf("namespace tenant-a (limit 2) = %v, want [a1 a2]", a)
	}
	// Unfiltered spans every namespace.
	all := map[string]bool{}
	for _, k := range collect(0, "") {
		all[k] = true
	}
	for _, k := range []string{"a1", "a2", "a3", "b1", "u1"} {
		if !all[k] {
			t.Fatalf("unfiltered sweep missing %s: %v", k, all)
		}
	}
}

func TestStore_EnsureIndexesIdempotent(t *testing.T) {
	s := newStore(t, time.Hour) // newStore already called EnsureIndexes once
	if err := s.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("second EnsureIndexes should be idempotent: %v", err)
	}
}

// TestOpen_HappyPath exercises the full convenience constructor: it
// dials Mongo from a Config, builds the Store with majority WC, and
// runs EnsureIndexes — all in one call. Verifies the Store is
// operational end-to-end and that both expected indexes are present
// on the entries collection.
func TestEnsureIndexes_CreatesTTLAndSortIndexes(t *testing.T) {
	if testClient == nil {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := fmt.Sprintf("pithtest_indexes_%d", dbCounter.Add(1))

	store := New(testClient.Database(dbName), time.Hour, WithMaxSendTimes(50))
	if err := store.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	t.Cleanup(func() { _ = testClient.Database(dbName).Drop(context.Background()) })

	// Store is functional end-to-end.
	if err := store.RecordAsSent(ctx, "k1", "", "", "hash-A"); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}
	entry, err := store.ReadEntry(ctx, "k1")
	if err != nil {
		t.Fatalf("ReadEntry: %v", err)
	}
	if entry.ContentHash != "hash-A" {
		t.Fatalf("ContentHash = %q, want hash-A", entry.ContentHash)
	}

	// Both indexes on the entries collection exist — the TTL expireAt index
	// and the RangeDeferred sort index.
	cur, err := testClient.Database(dbName).Collection("entries").Indexes().List(ctx)
	if err != nil {
		t.Fatalf("Indexes().List: %v", err)
	}
	defer cur.Close(ctx)
	have := map[string]bool{}
	for cur.Next(ctx) {
		var idx bson.M
		if err := cur.Decode(&idx); err != nil {
			t.Fatalf("decode index: %v", err)
		}
		if name, _ := idx["name"].(string); name != "" {
			have[name] = true
		}
	}
	if !have["expireAt_1"] {
		t.Errorf("expireAt TTL index missing; have %v", have)
	}
	if !have["lastDeferredAt_1"] {
		t.Errorf("lastDeferredAt index missing; have %v", have)
	}
}

// TestStore_PeakHighWaterMarks mirrors the memory backend: rapid sends all
// fall inside both rolling windows, so each peak tracks the running count
// and the first send seeds PeakedAt == LastSentAt. Exercises the $set
// pipeline ($max magnitude + $cond-on-rise PeakedAt) against real Mongo.
func TestStore_PeakHighWaterMarks(t *testing.T) {
	// Default maxSendTimes is the dedupe-only floor of 1; production sizes
	// it to the largest Coalescer cap via protect. Set a realistic cap so
	// the in-window count can exceed 1. TTL exceeds the 24h window so both
	// peaks fold (see RecordAsSent's TTL>=window guard).
	s := newStore(t, 25*time.Hour, WithMaxSendTimes(50))
	ctx := context.Background()

	_ = s.RecordAsSent(ctx, "k1", "", "", "h")
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

	for range 4 {
		_ = s.RecordAsSent(ctx, "k1", "", "", "h")
	}
	met, _, _ = s.ReadMetrics(ctx, "k1")
	if met.TotalSent != 5 {
		t.Fatalf("TotalSent=%d, want 5", met.TotalSent)
	}
	if met.Peak1h != 5 || met.Peak24h != 5 {
		t.Fatalf("after 5 sends: Peak1h=%d Peak24h=%d, want 5/5", met.Peak1h, met.Peak24h)
	}
}

// TestStore_PeakFreezesWhenWindowCountPlateaus mirrors the memory backend's
// distinguishing test: with lastNSendTimes $slice-capped, the in-window
// count plateaus, so the peak stops rising and PeakedAt freezes at the last
// rise — diverging from LastSentAt. Verifies the strict $gt in the pipeline.
func TestStore_PeakFreezesWhenWindowCountPlateaus(t *testing.T) {
	s := newStore(t, 25*time.Hour, WithMaxSendTimes(3)) // TTL > 24h so both peaks fold
	ctx := context.Background()

	// 2ms gaps keep timestamps strictly ordered after BSON's millisecond
	// truncation.
	for range 6 {
		_ = s.RecordAsSent(ctx, "k1", "", "", "h")
		time.Sleep(2 * time.Millisecond)
	}

	met, _, _ := s.ReadMetrics(ctx, "k1")
	if met.TotalSent != 6 {
		t.Fatalf("TotalSent=%d, want 6", met.TotalSent)
	}
	if met.Peak1h != 3 || met.Peak24h != 3 {
		t.Fatalf("Peak1h=%d Peak24h=%d, want 3/3 (capped count)", met.Peak1h, met.Peak24h)
	}
	if !met.Peak1hAt.Before(met.LastSentAt) {
		t.Fatalf("Peak1hAt=%v should be before LastSentAt=%v (peak froze, sends continued)", met.Peak1hAt, met.LastSentAt)
	}
	if !met.Peak1hAt.Equal(met.Peak24hAt) {
		t.Fatalf("Peak1hAt=%v and Peak24hAt=%v should match (same plateau send)", met.Peak1hAt, met.Peak24hAt)
	}
}

// TestStore_PeaksSkippedAtDedupeFloor: the default maxSendTimes is the
// dedupe-only floor of 1, so RecordAsSent takes the cheap path (plain
// UpdateByID + classic-operator metrics, no read-back, no pipeline) and
// leaves the peak fields unset — while the rest of the metrics still advance.
func TestStore_PeaksSkippedAtDedupeFloor(t *testing.T) {
	s := newStore(t, time.Hour) // no WithMaxSendTimes → floor of 1
	ctx := context.Background()

	for range 5 {
		_ = s.RecordAsSent(ctx, "k1", "", "", "h")
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

// TestStore_PeakSkippedWhenTTLBelowWindow mirrors the memory backend: a
// window's peak is only folded when EntryTTL covers it. A 2h-TTL store
// retains under 24h of send history, so the $set pipeline omits
// peak24h/peak24hAt entirely — they stay unset (absent, not a deceptive
// 0) — while peak1h still folds because TTL >= 1h.
func TestStore_PeakSkippedWhenTTLBelowWindow(t *testing.T) {
	s := newStore(t, 2*time.Hour, WithMaxSendTimes(5)) // covers 1h, not 24h
	ctx := context.Background()

	for range 3 {
		_ = s.RecordAsSent(ctx, "k1", "", "", "h")
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

// TestStore_TenantStamping: when the caller passes a non-empty tenant, it is
// mirrored onto the Metrics doc alongside Namespace; an empty tenant leaves
// the field as the zero value (omitempty → absent in Mongo, decodes back to
// the zero string).
func TestStore_TenantStamping(t *testing.T) {
	s := newStore(t, time.Hour)
	ctx := context.Background()

	// Tenanted send + deferral: both populate Tenant on Metrics.
	if err := s.RecordAsSent(ctx, "k1", "tenant-A", "ns-X", "h"); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}
	if err := s.RecordAsDeferred(ctx, "k1", "tenant-A", "ns-X", []byte("ref")); err != nil {
		t.Fatalf("RecordAsDeferred: %v", err)
	}
	met, ok, err := s.ReadMetrics(ctx, "k1")
	if err != nil || !ok {
		t.Fatalf("ReadMetrics(k1): ok=%v err=%v", ok, err)
	}
	if met.Tenant != "tenant-A" {
		t.Errorf("k1 Tenant = %q, want %q", met.Tenant, "tenant-A")
	}
	if met.Namespace != "ns-X" {
		t.Errorf("k1 Namespace = %q, want %q", met.Namespace, "ns-X")
	}

	// Untenanted send: Tenant stays empty.
	if err := s.RecordAsSent(ctx, "k2", "", "ns-Y", "h"); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}
	met2, ok, err := s.ReadMetrics(ctx, "k2")
	if err != nil || !ok {
		t.Fatalf("ReadMetrics(k2): ok=%v err=%v", ok, err)
	}
	if met2.Tenant != "" {
		t.Errorf("k2 Tenant = %q, want empty", met2.Tenant)
	}
	if met2.Namespace != "ns-Y" {
		t.Errorf("k2 Namespace = %q, want %q", met2.Namespace, "ns-Y")
	}
}
