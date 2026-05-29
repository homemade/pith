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

// Mirrors pith/sendstate/mongodb's container-per-package pattern so this
// thin wrapper gets honest end-to-end coverage rather than mocking
// around the very behaviour we're trying to verify.
var (
	testURI   string
	dbCounter atomic.Uint64
)

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	ctx := context.Background()
	container, err := tcmongo.Run(ctx, "mongo:7")
	if err != nil {
		fmt.Printf("protect/mongodb tests skipped: cannot start container: %v\n", err)
		return m.Run() // testURI == "" → integration tests t.Skip()
	}
	defer func() { _ = container.Terminate(ctx) }()

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		fmt.Printf("protect/mongodb tests skipped: connection string: %v\n", err)
		return m.Run()
	}

	// Sanity ping to confirm the cluster is reachable before any test
	// reaches it — matches the readiness check in sendstate/mongodb's
	// TestMain.
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
	testURI = uri
	return m.Run()
}

// freshDBName returns an isolated database name per test so concurrent
// or sequential tests don't share state.
func freshDBName() string {
	return fmt.Sprintf("pithtest_pmongo_%d", dbCounter.Add(1))
}

// The below-derived error fires *before* any Mongo I/O — exercised
// without a container so this test runs everywhere. Surfaces the
// misconfiguration at construction instead of letting a too-small
// MaxSendTimes silently leak the cap in production.
func TestNew_ErrorsOnMaxSendTimesBelowDerived(t *testing.T) {
	_, _, err := pmongo.New(context.Background(), pmongo.Config{
		URI:          "mongodb://ignored",
		Database:     "ignored",
		EntryTTL:     time.Hour,
		MaxSendTimes: 10, // below the quota's hardCap of 50
	}, protect.WithCoalescer(coalesce.NewQuota(50, 24*time.Hour)))
	if err == nil {
		t.Fatalf("expected an error when MaxSendTimes < largest hardCap, got nil")
	}
	// The error mentions both numbers so the diagnosis is self-contained.
	msg := err.Error()
	if !strings.Contains(msg, "(10)") || !strings.Contains(msg, "(50)") {
		t.Fatalf("error %q should mention both MaxSendTimes (10) and the derived value (50)", msg)
	}
}

// No Coalescers attached → derived value is 0 → the wrapper doesn't
// touch Config.MaxSendTimes → the underlying mongostore.New default
// (dedupe-only floor of 1) applies. Pure-logic test, no Mongo needed
// to exercise the validation path (Open is reached only if the
// validation passes; here we deliberately don't reach Open by passing
// a URI that would fail).
func TestNew_ErrorsOnly_NoCoalescers_RespectsExplicitMaxSendTimes(t *testing.T) {
	// With no Coalescers, derived = 0. cfg.MaxSendTimes = 1 is >= 0 so
	// validation passes; Open then fails on the bad URI (which is fine —
	// we just want to confirm we get *past* the wrapper's validation).
	_, _, err := pmongo.New(context.Background(), pmongo.Config{
		URI:          "mongodb://nonexistent.invalid:27017",
		Database:     "ignored",
		EntryTTL:     time.Hour,
		MaxSendTimes: 1,
		Timeout:      50 * time.Millisecond, // fail Open fast
	})
	if err == nil {
		t.Fatalf("expected an Open error against the invalid host, got nil")
	}
	// Must NOT be our wrapper's "below derived" error.
	if strings.Contains(err.Error(), "would silently leak") {
		t.Fatalf("error came from the wrapper's validation, not Open: %v", err)
	}
}

// Happy path: cfg.MaxSendTimes unset, the wrapper derives it from the
// attached Coalescers, the resulting Protector is functional.
// Exercises the actual derivation logic against a real mongo backend.
func TestNew_AutoDerivesMaxSendTimesFromCoalescers(t *testing.T) {
	if testURI == "" {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := freshDBName()

	p, client, err := pmongo.New(ctx, pmongo.Config{
		URI:      testURI,
		Database: dbName,
		EntryTTL: time.Hour,
		Timeout:  200 * time.Millisecond,
		// MaxSendTimes deliberately omitted — wrapper should derive it
		// from the attached caps' largest hardCap (50).
	},
		protect.WithCoalescer(coalesce.NewLeadingEdgeDebounce(10*time.Second)),
		protect.WithCoalescer(coalesce.NewQuota(50, 24*time.Hour)),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Database(dbName).Drop(context.Background())
		_ = client.Disconnect(context.Background())
	})

	// Functional check: a Check + RecordAsSent round-trip works.
	req := protect.Request{
		RequestMeta: protect.RequestMeta{TargetKey: "k1"},
		ContentHash: "h1",
	}
	if out := p.Check(ctx, req); out.Decision != protect.DecisionProceed || out.Err != nil {
		t.Fatalf("first Check = %s, err=%v; want Proceed", out.Decision, out.Err)
	}
	if err := p.RecordAsSent(ctx, req); err != nil {
		t.Fatalf("RecordAsSent: %v", err)
	}

	// Identical content → dedupe trips. Confirms the protector and
	// store are wired up end-to-end (a misconfigured MaxSendTimes
	// wouldn't surface here, but a broken construction would).
	if out := p.Check(ctx, req); out.Decision != protect.DecisionDeduped {
		t.Fatalf("repeat Check = %s, want Deduped", out.Decision)
	}
}

// Explicit MaxSendTimes override above the derived value is respected
// — a caller wanting extra headroom (e.g. for future replay-bounded
// scans) can set it without the wrapper second-guessing.
func TestNew_RespectsExplicitMaxSendTimesAboveDerived(t *testing.T) {
	if testURI == "" {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := freshDBName()

	p, client, err := pmongo.New(ctx, pmongo.Config{
		URI:          testURI,
		Database:     dbName,
		EntryTTL:     time.Hour,
		Timeout:      200 * time.Millisecond,
		MaxSendTimes: 200, // > derived (50) — should be respected.
	}, protect.WithCoalescer(coalesce.NewQuota(50, 24*time.Hour)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Database(dbName).Drop(context.Background())
		_ = client.Disconnect(context.Background())
	})
	// Just confirming construction succeeded — the override behaviour
	// is "use what the caller said, don't error." A functional probe is
	// covered by the auto-derive test.
	if p == nil {
		t.Fatalf("New returned a nil Protector with no error")
	}
}
