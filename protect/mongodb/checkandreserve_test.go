package mongodb_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/protect"
	pmongo "github.com/homemade/pith/protect/mongodb"
)

// TestMongoWriteCheckAndReserveOutcomes exercises the three branches the
// write-side gate produces (Proceed / Deduped / Deferred) end-to-end
// through the real findOneAndUpdate aggregation pipeline. Same shape as
// the memory variant, but on Mongo so the pipeline's $cond / $filter
// expressions are observed against an actual server.
func TestMongoWriteCheckAndReserveOutcomes(t *testing.T) {
	if testClient == nil {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := freshDBName()

	p, err := pmongo.NewWriteProtector(ctx, testClient, pmongo.Config{
		Database: dbName,
		EntryTTL: 25 * time.Hour,
	}, coalesce.NewLeadingEdgeDebounce(time.Hour))
	if err != nil {
		t.Fatalf("NewWriteProtector: %v", err)
	}
	t.Cleanup(func() { _ = testClient.Database(dbName).Drop(context.Background()) })

	w := p.Tenant("").Namespace("")
	meta := protect.RequestMeta{TargetKey: "k1"}

	out, release := w.CheckAndReserve(ctx, meta, "h1")
	if out.Decision != protect.DecisionProceed || out.Err != nil {
		t.Fatalf("first CheckAndReserve = %s (err=%v), want Proceed", out.Decision, out.Err)
	}
	if release == nil {
		t.Fatal("first CheckAndReserve: release is nil")
	}

	// Identical content → Deduped (pipeline's $eq fires).
	out2, release2 := w.CheckAndReserve(ctx, meta, "h1")
	if out2.Decision != protect.DecisionDeduped {
		t.Fatalf("dedupe CheckAndReserve = %s (err=%v), want Deduped", out2.Decision, out2.Err)
	}
	if release2 != nil {
		t.Fatal("dedupe CheckAndReserve: release is non-nil")
	}

	// New content inside the debounce window → Deferred. The pipeline's
	// $size($filter > now-window) reaches hardCap=1.
	out3, release3 := w.CheckAndReserve(ctx, meta, "h2")
	if out3.Decision != protect.DecisionDeferred {
		t.Fatalf("debounced CheckAndReserve = %s, want Deferred", out3.Decision)
	}
	if release3 != nil {
		t.Fatal("debounced CheckAndReserve: release is non-nil")
	}
}

// TestMongoReadCheckAndReserveNeverDedupes confirms the read-side
// variant skips dedupe end-to-end on Mongo (no contentHash in the
// pipeline, so the $eq sub-expression is constant-false).
func TestMongoReadCheckAndReserveNeverDedupes(t *testing.T) {
	if testClient == nil {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := freshDBName()

	p, err := pmongo.NewReadProtector(ctx, testClient, pmongo.Config{
		Database: dbName,
		EntryTTL: 25 * time.Hour,
	}, coalesce.NewLeadingEdgeDebounce(time.Hour))
	if err != nil {
		t.Fatalf("NewReadProtector: %v", err)
	}
	t.Cleanup(func() { _ = testClient.Database(dbName).Drop(context.Background()) })

	r := p.Tenant("").Namespace("")
	meta := protect.RequestMeta{TargetKey: "kr", MessageRef: []byte("ref-r")}

	if out, release := r.CheckAndReserve(ctx, meta); out.Decision != protect.DecisionProceed || release == nil {
		t.Fatalf("first CheckAndReserve = %s, release nil? %v", out.Decision, release == nil)
	}
	// Second reserve inside the window → Deferred (debounce). Not Deduped.
	out2, _ := r.CheckAndReserve(ctx, meta)
	if out2.Decision != protect.DecisionDeferred {
		t.Fatalf("second CheckAndReserve = %s, want Deferred (read gate never dedupes)", out2.Decision)
	}
}

// TestMongoReleaseByValueSpartsSiblingReserve is the headline release-by-
// value test against real Mongo: a $pull-by-value pop must not clobber a
// sibling reserve's slot. With hardCap=2, both reserves coexist; releasing
// one must free exactly one slot.
func TestMongoReleaseByValueSpartsSiblingReserve(t *testing.T) {
	if testClient == nil {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := freshDBName()

	p, err := pmongo.NewWriteProtector(ctx, testClient, pmongo.Config{
		Database: dbName,
		EntryTTL: 25 * time.Hour,
	}, coalesce.NewQuota(2, 24*time.Hour))
	if err != nil {
		t.Fatalf("NewWriteProtector: %v", err)
	}
	t.Cleanup(func() { _ = testClient.Database(dbName).Drop(context.Background()) })

	w := p.Tenant("").Namespace("")
	metaA := protect.RequestMeta{TargetKey: "k-shared"}
	metaB := protect.RequestMeta{TargetKey: "k-shared"}

	outA, releaseA := w.CheckAndReserve(ctx, metaA, "hA")
	if outA.Decision != protect.DecisionProceed {
		t.Fatalf("reserve A = %s, want Proceed", outA.Decision)
	}
	// Spacing: Mongo $$NOW resolves at millisecond resolution; sleep
	// enough for the second reserve's timestamp to differ so $pull
	// matching one doesn't pop both.
	time.Sleep(5 * time.Millisecond)
	outB, releaseB := w.CheckAndReserve(ctx, metaB, "hB")
	if outB.Decision != protect.DecisionProceed {
		t.Fatalf("reserve B = %s, want Proceed", outB.Decision)
	}
	_ = releaseB

	// Both reserves hold the cap. New attempt should defer.
	if out := w.Check(ctx, metaA, "hC"); out.Decision != protect.DecisionDeferred {
		t.Fatalf("at-cap Check = %s, want Deferred", out.Decision)
	}

	// Release A by value — B's slot must survive.
	if err := releaseA(ctx); err != nil {
		t.Fatalf("releaseA: %v", err)
	}
	outC, _ := w.CheckAndReserve(ctx, metaA, "hC")
	if outC.Decision != protect.DecisionProceed {
		t.Fatalf("post-releaseA CheckAndReserve = %s, want Proceed (A's slot freed)", outC.Decision)
	}
	// And B's slot is still there — cap is back at 2.
	if out := w.Check(ctx, metaA, "hD"); out.Decision != protect.DecisionDeferred {
		t.Fatalf("after C reserves, Check = %s, want Deferred (B's slot intact)", out.Decision)
	}
}

// TestMongoConcurrentReservesCannotOvershoot is the headline atomicity
// test against real Atlas: hardCap reservers race against a hardCap=K
// quota; exactly K must Proceed, the rest Defer. This is the property
// the findOneAndUpdate aggregation pipeline exists to guarantee — the
// pre-update document state drives every $cond, so concurrent reserves
// serialise on the single-document write lock and the second one's
// $size already includes the first one's just-pushed timestamp.
func TestMongoConcurrentReservesCannotOvershoot(t *testing.T) {
	if testClient == nil {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := freshDBName()

	const hardCap = 5
	const reservers = 30

	p, err := pmongo.NewWriteProtector(ctx, testClient, pmongo.Config{
		Database: dbName,
		EntryTTL: 25 * time.Hour,
	}, coalesce.NewQuota(hardCap, 24*time.Hour))
	if err != nil {
		t.Fatalf("NewWriteProtector: %v", err)
	}
	t.Cleanup(func() { _ = testClient.Database(dbName).Drop(context.Background()) })

	w := p.Tenant("").Namespace("")
	meta := protect.RequestMeta{TargetKey: "k-race"}

	var proceeds, defers, other atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < reservers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Distinct contentHash per reserver so dedupe doesn't fire;
			// only the quota cap is exercised.
			content := "h-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
			out, _ := w.CheckAndReserve(ctx, meta, content)
			switch out.Decision {
			case protect.DecisionProceed:
				proceeds.Add(1)
			case protect.DecisionDeferred:
				defers.Add(1)
			default:
				other.Add(1)
				t.Errorf("unexpected outcome %s (err=%v)", out.Decision, out.Err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := proceeds.Load(); got != hardCap {
		t.Errorf("Proceeds = %d, want exactly %d (atomicity property)", got, hardCap)
	}
	if got := defers.Load(); got != reservers-hardCap {
		t.Errorf("Defers = %d, want %d", got, reservers-hardCap)
	}
	if got := other.Load(); got != 0 {
		t.Errorf("Other = %d, want 0", got)
	}
}

// TestMongoReleaseClearsDedupeForIdenticalRetry pins the wire-failure
// recovery contract against real Mongo: after a Proceed reserves a slot
// for some content hash and the gated op subsequently fails, calling the
// release closure must leave the dedupe gate inert so an identical retry
// can proceed. The aggregation pipeline's dedupedExpr therefore gates on
// BOTH contentHash equality AND lastNSendTimes being non-empty — mirroring
// the memory backend's [sendstate.Entry.Seen] len-guard. Without that
// guard the dedupe gate fires on the retry (contentHash is preserved
// across a $pull-by-value release) and the work is lost. raisortto's
// webhook handler depends on this — Raisely re-delivers identical
// webhooks on a 5xx; the retry must not be dedupe-suppressed when the
// original send failed.
func TestMongoReleaseClearsDedupeForIdenticalRetry(t *testing.T) {
	if testClient == nil {
		t.Skip("no MongoDB container (Docker unavailable)")
	}
	ctx := context.Background()
	dbName := freshDBName()

	// Wide quota so dedupe is the only suppressor under test.
	p, err := pmongo.NewWriteProtector(ctx, testClient, pmongo.Config{
		Database: dbName,
		EntryTTL: 25 * time.Hour,
	}, coalesce.NewQuota(1000, 24*time.Hour))
	if err != nil {
		t.Fatalf("NewWriteProtector: %v", err)
	}
	t.Cleanup(func() { _ = testClient.Database(dbName).Drop(context.Background()) })

	w := p.Tenant("").Namespace("")
	meta := protect.RequestMeta{TargetKey: "k-retry"}

	// First reserve proceeds; the aggregation pipeline stamps
	// contentHash=h1 and pushes $$NOW onto lastNSendTimes.
	out, release := w.CheckAndReserve(ctx, meta, "h1")
	if out.Decision != protect.DecisionProceed {
		t.Fatalf("first CheckAndReserve = %s, want Proceed", out.Decision)
	}
	if release == nil {
		t.Fatal("first CheckAndReserve: release is nil")
	}

	// Simulate a wire-failure rollback. The $pull pops the timestamp
	// from lastNSendTimes; contentHash on the doc is preserved.
	if err := release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Raisely's identical retry: same target, same content. Must proceed
	// — the dedupedExpr's lastNSendTimes-size guard keeps the gate inert
	// despite the preserved contentHash.
	out2, release2 := w.CheckAndReserve(ctx, meta, "h1")
	if out2.Decision != protect.DecisionProceed {
		t.Fatalf("post-release identical retry = %s, want Proceed (record-on-success invariant)", out2.Decision)
	}
	if release2 == nil {
		t.Fatal("post-release retry: release is nil")
	}

	// Now an actual cascade duplicate AFTER a successful proceed IS
	// suppressed — the dedupedExpr fires because contentHash matches AND
	// lastNSendTimes has the post-retry timestamp.
	out3, _ := w.CheckAndReserve(ctx, meta, "h1")
	if out3.Decision != protect.DecisionDeduped {
		t.Fatalf("post-success identical retry = %s, want Deduped", out3.Decision)
	}
}

// (Custom-Coalescer rejection test removed: under the data-only
// Coalescer design every coalesce.Coalescer value is server-side
// evaluable by construction — the Strategy enum is exhaustive and
// sendstate/mongodb's switch covers every variant — so there's no
// rejection path left to exercise.)
