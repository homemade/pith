package mongodb

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
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
// or nil when Docker is unavailable (every test then skips). testURI
// captures the same container's connection string so [TestOpen] can
// build its own client via Open without reaching through testClient.
var (
	testClient *mongo.Client
	testURI    string
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
	testURI = uri
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

	if err := s.RecordAsSent(ctx, "k1", "hash-A"); err != nil {
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

	_ = s.RecordAsSent(ctx, "k1", "hash-A")
	_ = s.RecordAsSent(ctx, "k1", "hash-B")

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
	_ = s.RecordAsSent(ctx, "k1", "hash-A")
	if err := s.RecordAsDeferred(ctx, "k1", []byte("ref-A")); err != nil {
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
	_ = s.RecordAsSent(ctx, "k1", "hash-B")
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
	_ = s.RecordAsSent(ctx, "k1", "h1")
	met, _, _ := s.ReadMetrics(ctx, "k1")
	first := met.FirstSentAt
	if first.IsZero() {
		t.Fatalf("FirstSentAt should be set after first send")
	}

	time.Sleep(10 * time.Millisecond)
	_ = s.RecordAsSent(ctx, "k1", "h2")
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
	_ = s.RecordAsDeferred(ctx, "k2", []byte("ref-1"))
	met, _, _ = s.ReadMetrics(ctx, "k2")
	firstD := met.FirstDeferredAt
	if firstD.IsZero() {
		t.Fatalf("FirstDeferredAt should be set after first deferral")
	}

	time.Sleep(10 * time.Millisecond)
	_ = s.RecordAsDeferred(ctx, "k2", []byte("ref-2"))
	met, _, _ = s.ReadMetrics(ctx, "k2")
	if !met.FirstDeferredAt.Equal(firstD) {
		t.Fatalf("FirstDeferredAt should be unchanged on subsequent deferral, got %v want %v", met.FirstDeferredAt, firstD)
	}

	// Cross-side: deferral creates the metrics doc, then a send fires.
	// FirstSentAt must reflect the send's timestamp (set via $min on a
	// previously-missing field — the prior defer didn't write it).
	_ = s.RecordAsDeferred(ctx, "k3", []byte("ref"))
	met, _, _ = s.ReadMetrics(ctx, "k3")
	deferAt := met.FirstDeferredAt
	if !met.FirstSentAt.IsZero() {
		t.Fatalf("FirstSentAt should be zero when only deferrals have been recorded, got %v", met.FirstSentAt)
	}

	time.Sleep(10 * time.Millisecond)
	_ = s.RecordAsSent(ctx, "k3", "h")
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
		_ = s.RecordAsSent(ctx, "k1", "h")
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
	_ = s.RecordAsSent(ctx, "k1", "hash-A")

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
	_ = s.RecordAsDeferred(ctx, "k-superseded", []byte("x"))
	_ = s.RecordAsSent(ctx, "k-superseded", "h")
	// Not pending: only ever sent.
	_ = s.RecordAsSent(ctx, "k-sent-only", "h")

	// Three pending, deferred oldest→newest.
	_ = s.RecordAsDeferred(ctx, "k1", []byte("r1"))
	time.Sleep(5 * time.Millisecond)
	_ = s.RecordAsDeferred(ctx, "k2", []byte("r2"))
	time.Sleep(5 * time.Millisecond)
	_ = s.RecordAsDeferred(ctx, "k3", []byte("r3"))

	var got []string
	if err := s.RangeDeferred(ctx, 0, func(key string, e sendstate.Entry) bool {
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
	_ = s.RangeDeferred(ctx, 2, func(key string, _ sendstate.Entry) bool {
		bounded = append(bounded, key)
		return true
	})
	if len(bounded) != 2 || bounded[0] != "k1" || bounded[1] != "k2" {
		t.Fatalf("RangeDeferred(limit=2) = %v, want [k1 k2]", bounded)
	}

	// A send on k2 makes it no longer pending.
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
func TestOpen_HappyPath(t *testing.T) {
	if testURI == "" {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := fmt.Sprintf("pithtest_open_%d", dbCounter.Add(1))

	store, client, err := Open(ctx, Config{
		URI:          testURI,
		Database:     dbName,
		EntryTTL:     time.Hour,
		MaxSendTimes: 50,
		Timeout:      200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		// Drop the test DB via the package's shared client (the one
		// Open dialed is being Disconnected on the next line).
		_ = testClient.Database(dbName).Drop(context.Background())
		_ = client.Disconnect(context.Background())
	})

	// Store is functional end-to-end.
	if err := store.RecordAsSent(ctx, "k1", "hash-A"); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}
	entry, err := store.ReadEntry(ctx, "k1")
	if err != nil {
		t.Fatalf("ReadEntry: %v", err)
	}
	if entry.ContentHash != "hash-A" {
		t.Fatalf("ContentHash = %q, want hash-A", entry.ContentHash)
	}

	// EnsureIndexes ran inside Open: both indexes on the entries
	// collection exist (the TTL expireAt index and the RangeDeferred
	// sort index).
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

// Config validation runs before any I/O — these tests don't need
// Docker and run unconditionally.

func TestOpen_RequiresURI(t *testing.T) {
	_, _, err := Open(context.Background(), Config{Database: "x", EntryTTL: time.Hour})
	if err == nil || !strings.Contains(err.Error(), "URI") {
		t.Fatalf("err = %v, want a URI-required error", err)
	}
}

func TestOpen_RequiresDatabase(t *testing.T) {
	_, _, err := Open(context.Background(), Config{URI: "mongodb://x", EntryTTL: time.Hour})
	if err == nil || !strings.Contains(err.Error(), "Database") {
		t.Fatalf("err = %v, want a Database-required error", err)
	}
}

func TestOpen_RequiresPositiveEntryTTL(t *testing.T) {
	_, _, err := Open(context.Background(), Config{URI: "mongodb://x", Database: "x"})
	if err == nil || !strings.Contains(err.Error(), "EntryTTL") {
		t.Fatalf("err = %v, want an EntryTTL-required error", err)
	}
}
