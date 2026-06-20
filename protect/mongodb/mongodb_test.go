package mongodb_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	driver "go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/protect"
	pmongo "github.com/homemade/pith/protect/mongodb"
)

// Mirrors pith/sendstate/mongodb's container-per-package pattern so these
// thin wrappers get honest end-to-end coverage rather than mocking
// around the very behaviour we're trying to verify. The client opened in
// TestMain is shared across tests — production callers do the same (one
// process-wide client, many stores).
var (
	testClient *driver.Client
	dbCounter  atomic.Uint64
)

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	ctx := context.Background()
	container, err := tcmongo.Run(ctx, "mongo:7")
	if err != nil {
		fmt.Printf("protect/mongodb tests skipped: cannot start container: %v\n", err)
		return m.Run() // testClient == nil → integration tests t.Skip()
	}
	defer func() { _ = container.Terminate(ctx) }()

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		fmt.Printf("protect/mongodb tests skipped: connection string: %v\n", err)
		return m.Run()
	}

	// Sanity ping to confirm the cluster is reachable before any test
	// reaches it — matches the readiness check in sendstate/mongodb's
	// TestMain. Majority write concern is required for cross-instance
	// correctness (see pith/sendstate/mongodb package doc).
	client, err := driver.Connect(options.Client().ApplyURI(uri).SetWriteConcern(writeconcern.Majority()))
	if err != nil {
		fmt.Printf("protect/mongodb tests skipped: connect: %v\n", err)
		return m.Run()
	}
	defer func() { _ = client.Disconnect(ctx) }()

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		fmt.Printf("protect/mongodb tests skipped: ping: %v\n", err)
		return m.Run()
	}
	testClient = client
	return m.Run()
}

// freshDBName returns an isolated database name per test so concurrent
// or sequential tests don't share state.
func freshDBName() string {
	return fmt.Sprintf("pithtest_pmongo_%d", dbCounter.Add(1))
}

// The below-derived error fires *before* any Mongo I/O, but the new
// caller-owned-client shape requires a non-nil client + a configured Database
// to reach the cap check. Skip when the container isn't available rather
// than reordering validation.
func TestNewWriteProtector_ErrorsOnMaxSendTimesBelowDerived(t *testing.T) {
	if testClient == nil {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	_, err := pmongo.NewWriteProtector(context.Background(), testClient, pmongo.Config{
		Database:     "ignored",
		EntryTTL:     25 * time.Hour, // covers the 24h Quota window so the below-derived check (not the TTL guard) is what fires
		MaxSendTimes: 10,             // below the quota's hardCap of 50
	}, coalesce.NewQuota(50, 24*time.Hour))
	if err == nil {
		t.Fatalf("expected an error when MaxSendTimes < largest hardCap, got nil")
	}
	// The error mentions both numbers so the diagnosis is self-contained.
	msg := err.Error()
	if !strings.Contains(msg, "(10)") || !strings.Contains(msg, "(50)") {
		t.Fatalf("error %q should mention both MaxSendTimes (10) and the derived value (50)", msg)
	}
}

// Happy path: cfg.MaxSendTimes unset, the wrapper derives it from the
// attached Coalescers, the resulting WriteProtector is functional.
// Exercises the actual derivation logic against a real mongo backend.
func TestNewWriteProtector_AutoDerivesMaxSendTimesFromCoalescers(t *testing.T) {
	if testClient == nil {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := freshDBName()

	p, err := pmongo.NewWriteProtector(ctx, testClient, pmongo.Config{
		Database: dbName,
		EntryTTL: 25 * time.Hour, // must cover the 24h Quota window (core validates this)
		// MaxSendTimes deliberately omitted — wrapper should derive it
		// from the attached caps' largest hardCap (50).
	},
		coalesce.NewLeadingEdgeDebounce(10*time.Second),
		coalesce.NewQuota(50, 24*time.Hour),
	)
	if err != nil {
		t.Fatalf("NewWriteProtector: %v", err)
	}
	t.Cleanup(func() {
		_ = testClient.Database(dbName).Drop(context.Background())
	})

	// Functional check: a Check + RecordAsSent round-trip works.
	w := p.Tenant("").Namespace("") // untenanted, whole-store; gating happens on the handle
	meta := protect.RequestMeta{TargetKey: "k1"}
	if out := w.Check(ctx, meta, "h1"); out.Decision != protect.DecisionProceed || out.Err != nil {
		t.Fatalf("first Check = %s, err=%v; want Proceed", out.Decision, out.Err)
	}
	if err := w.RecordAsSent(ctx, meta, "h1"); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}

	// Identical content → dedupe trips. Confirms the protector and store
	// are wired up end-to-end.
	if out := w.Check(ctx, meta, "h1"); out.Decision != protect.DecisionDeduped {
		t.Fatalf("repeat Check = %s, want Deduped", out.Decision)
	}
}

// A read protector built against a real mongo backend has no dedupe and
// DEFERS a capped read — stamping a breadcrumb that becomes a replay
// candidate once the cap clears.
func TestNewReadProtector_DefersAtCap(t *testing.T) {
	if testClient == nil {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := freshDBName()

	r, err := pmongo.NewReadProtector(ctx, testClient, pmongo.Config{
		Database: dbName,
		EntryTTL: 25 * time.Hour,
	}, coalesce.NewQuota(1, 24*time.Hour))
	if err != nil {
		t.Fatalf("NewReadProtector: %v", err)
	}
	t.Cleanup(func() {
		_ = testClient.Database(dbName).Drop(context.Background())
	})

	rn := r.Tenant("").Namespace("") // untenanted, whole-store; gating happens on the handle
	meta := protect.RequestMeta{TargetKey: "r1", MessageRef: []byte("ref-1")}
	if out := rn.Check(ctx, meta); out.Decision != protect.DecisionProceed || out.Err != nil {
		t.Fatalf("first Check = %s, err=%v; want Proceed", out.Decision, out.Err)
	}
	if err := rn.RecordAsSent(ctx, meta); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}
	// Quota of 1 is now reached → the next read is DEFERRED (breadcrumb
	// stamped), not dropped.
	if out := rn.Check(ctx, meta); out.Decision != protect.DecisionDeferred {
		t.Fatalf("over-cap Check = %s, want Deferred", out.Decision)
	}
	// The deferral is not yet a replay candidate (the 24h quota window has
	// not cleared) — but the breadcrumb is recorded.
	cands, err := rn.ReplayCandidates(ctx, 0)
	if err != nil {
		t.Fatalf("ReplayCandidates: %v", err)
	}
	if len(cands) != 0 {
		t.Fatalf("ReplayCandidates = %+v, want none while the quota window is full", cands)
	}
}

// Explicit MaxSendTimes override above the derived value is respected
// — a caller wanting extra headroom (e.g. for future replay-bounded
// scans) can set it without the wrapper second-guessing.
func TestNewWriteProtector_RespectsExplicitMaxSendTimesAboveDerived(t *testing.T) {
	if testClient == nil {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := freshDBName()

	p, err := pmongo.NewWriteProtector(ctx, testClient, pmongo.Config{
		Database:     dbName,
		EntryTTL:     25 * time.Hour, // covers the 24h Quota window (core validates this)
		MaxSendTimes: 200,            // > derived (50) — should be respected.
	}, coalesce.NewQuota(50, 24*time.Hour))
	if err != nil {
		t.Fatalf("NewWriteProtector: %v", err)
	}
	t.Cleanup(func() {
		_ = testClient.Database(dbName).Drop(context.Background())
	})
	if p == nil {
		t.Fatalf("NewWriteProtector returned a nil WriteProtector with no error")
	}
}
