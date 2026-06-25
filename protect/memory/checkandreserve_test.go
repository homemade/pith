package memory_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/protect"
	memprotect "github.com/homemade/pith/protect/memory"
)

// TestWriteCheckAndReserveOutcomes exercises the three branches the
// write-side gate produces (Proceed / Deduped / Deferred), plus the
// existence (or absence) of a ReleaseFunc on each branch. The legacy
// Check path is identical-shape so the assertions here pin only the
// CheckAndReserve-specific behaviour.
func TestWriteCheckAndReserveOutcomes(t *testing.T) {
	const debounce = 30 * time.Millisecond
	p := memprotect.NewWriteProtector(
		time.Hour,
		coalesce.NewLeadingEdgeDebounce(debounce),
	)
	ctx := context.Background()
	w := p.Tenant("").Namespace("")
	meta := protect.RequestMeta{TargetKey: "k1"}

	// First reserve: Proceed + ReleaseFunc non-nil so the caller can
	// roll back on op failure.
	out, release := w.CheckAndReserve(ctx, meta, "h1")
	if out.Decision != protect.DecisionProceed {
		t.Fatalf("first CheckAndReserve = %s, want Proceed", out.Decision)
	}
	if release == nil {
		t.Fatal("first CheckAndReserve: release is nil; expected a non-nil ReleaseFunc on Proceed")
	}

	// Same content again → dedupe fires; no reservation made; no release.
	out2, release2 := w.CheckAndReserve(ctx, meta, "h1")
	if out2.Decision != protect.DecisionDeduped {
		t.Fatalf("dedupe CheckAndReserve = %s, want Deduped", out2.Decision)
	}
	if release2 != nil {
		t.Fatal("dedupe CheckAndReserve: release is non-nil; nothing was reserved")
	}

	// New content inside the debounce window → defer; breadcrumb is
	// stamped on the entry (a sweep will replay later). No release.
	meta2 := protect.RequestMeta{TargetKey: "k1", MessageRef: []byte("ref-debounced")}
	out3, release3 := w.CheckAndReserve(ctx, meta2, "h2")
	if out3.Decision != protect.DecisionDeferred {
		t.Fatalf("debounced CheckAndReserve = %s, want Deferred", out3.Decision)
	}
	if release3 != nil {
		t.Fatal("deferred CheckAndReserve: release is non-nil; nothing was reserved")
	}

	// And the deferred breadcrumb shows up as a replay candidate once
	// the window passes — same as the legacy Check defer path.
	time.Sleep(2 * debounce)
	cands, err := w.ReplayCandidates(ctx, 0)
	if err != nil {
		t.Fatalf("ReplayCandidates: %v", err)
	}
	if len(cands) != 1 || string(cands[0].MessageRef) != "ref-debounced" {
		t.Fatalf("ReplayCandidates = %+v, want 1 candidate with MessageRef ref-debounced", cands)
	}
}

// TestReadCheckAndReserveNeverDedupes confirms the read-side variant
// returns only Proceed or Deferred — never DecisionDeduped — even when
// the same target key is reserved repeatedly. The read gate has no
// content to fingerprint, so dedupe doesn't apply.
func TestReadCheckAndReserveNeverDedupes(t *testing.T) {
	const debounce = 30 * time.Millisecond
	p := memprotect.NewReadProtector(
		time.Hour,
		coalesce.NewLeadingEdgeDebounce(debounce),
	)
	ctx := context.Background()
	r := p.Tenant("").Namespace("")
	meta := protect.RequestMeta{TargetKey: "k-read", MessageRef: []byte("ref-r")}

	out, release := r.CheckAndReserve(ctx, meta)
	if out.Decision != protect.DecisionProceed {
		t.Fatalf("first CheckAndReserve = %s, want Proceed", out.Decision)
	}
	if release == nil {
		t.Fatal("first CheckAndReserve: release is nil; expected a non-nil ReleaseFunc on Proceed")
	}

	// Second reserve inside the window → Deferred (debounce fires).
	// Crucially: it does NOT come back Deduped, even though every input
	// is identical to the first call. Dedupe is write-only.
	out2, _ := r.CheckAndReserve(ctx, meta)
	if out2.Decision != protect.DecisionDeferred {
		t.Fatalf("second CheckAndReserve = %s, want Deferred (read gates never dedupe)", out2.Decision)
	}
}

// TestReleaseRestoresSlot verifies the failure-rollback path: after a
// Proceed reserves a slot, calling the returned ReleaseFunc pops the
// reserved send-time so a subsequent reserve sees a vacant cap again.
// Without the release, the second reserve would defer (cap=1 leading-edge
// debounce is held by the first); with the release the second proceeds.
func TestReleaseRestoresSlot(t *testing.T) {
	const debounce = time.Hour // long enough that timing alone can't clear it
	p := memprotect.NewWriteProtector(
		time.Hour,
		coalesce.NewLeadingEdgeDebounce(debounce),
	)
	ctx := context.Background()
	w := p.Tenant("").Namespace("")
	meta := protect.RequestMeta{TargetKey: "k-rel"}

	out, release := w.CheckAndReserve(ctx, meta, "h1")
	if out.Decision != protect.DecisionProceed || release == nil {
		t.Fatalf("first CheckAndReserve = %s (release nil? %v); want Proceed + non-nil release", out.Decision, release == nil)
	}

	// Without releasing first, a follow-up reserve with NEW content
	// would debounce (leading-edge cap=1 in this window). The reserve
	// would stamp a deferred breadcrumb, but that's a no-op on top of
	// the already-deferred state; we only assert the Decision.
	if out, _ := w.CheckAndReserve(ctx, meta, "h2"); out.Decision != protect.DecisionDeferred {
		t.Fatalf("pre-release CheckAndReserve = %s, want Deferred (cap held by reserve)", out.Decision)
	}

	// Release rolls the reservation back. The next reserve should proceed.
	if err := release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	out2, release2 := w.CheckAndReserve(ctx, meta, "h2")
	if out2.Decision != protect.DecisionProceed {
		t.Fatalf("post-release CheckAndReserve = %s, want Proceed", out2.Decision)
	}
	if release2 == nil {
		t.Fatal("post-release CheckAndReserve: release is nil")
	}
}

// TestReleaseByValueSpartsSiblingReserve confirms that ReleaseReservation
// pops by value (not position) — releasing one reserve does not clobber
// a sibling reserve that pushed afterwards. This is the property that
// makes optimistic-reserve-then-release safe under concurrency.
func TestReleaseByValueSpartsSiblingReserve(t *testing.T) {
	// hardCap=2 so two reserves can coexist; a third would defer.
	p := memprotect.NewWriteProtector(
		time.Hour,
		coalesce.NewQuota(2, time.Hour),
	)
	ctx := context.Background()
	w := p.Tenant("").Namespace("")

	metaA := protect.RequestMeta{TargetKey: "k-shared"}
	metaB := protect.RequestMeta{TargetKey: "k-shared"}

	outA, releaseA := w.CheckAndReserve(ctx, metaA, "hA")
	if outA.Decision != protect.DecisionProceed || releaseA == nil {
		t.Fatalf("reserve A = %s, want Proceed", outA.Decision)
	}
	// Sleep a nanosecond so B's reservedAt isn't equal to A's (otherwise
	// the value-match would pop both on either release).
	time.Sleep(time.Microsecond)
	outB, releaseB := w.CheckAndReserve(ctx, metaB, "hB")
	if outB.Decision != protect.DecisionProceed || releaseB == nil {
		t.Fatalf("reserve B = %s, want Proceed", outB.Decision)
	}

	// Both reserves at cap. A new attempt should defer (cap=2 reached).
	metaC := protect.RequestMeta{TargetKey: "k-shared"}
	if out, _ := w.CheckAndReserve(ctx, metaC, "hC"); out.Decision != protect.DecisionDeferred {
		t.Fatalf("at-cap CheckAndReserve = %s, want Deferred", out.Decision)
	}

	// Release ONLY A — by value, so B's slot survives. A new reserve
	// should now find one slot open (B still occupies one of the two).
	if err := releaseA(ctx); err != nil {
		t.Fatalf("releaseA: %v", err)
	}
	outC, releaseC := w.CheckAndReserve(ctx, metaC, "hC")
	if outC.Decision != protect.DecisionProceed {
		t.Fatalf("post-releaseA CheckAndReserve = %s, want Proceed (A's slot freed)", outC.Decision)
	}
	if releaseC == nil {
		t.Fatal("post-releaseA CheckAndReserve: release is nil")
	}

	// And critically, the next attempt should AGAIN defer — B's slot
	// is intact, so cap is back at 2.
	if out, _ := w.CheckAndReserve(ctx, metaC, "hD"); out.Decision != protect.DecisionDeferred {
		t.Fatalf("after C reserves, CheckAndReserve = %s, want Deferred (B still holds a slot)", out.Decision)
	}
}

// TestConcurrentReservesCannotOvershoot is the headline atomicity test:
// hardCap reservers race against a quota=hardCap cap, and exactly hardCap
// of them must Proceed (the rest Defer). The in-process backend's CAS
// loop is what closes the TOCTOU window; this test would fail against the
// pre-CheckAndReserve read-only Check / record-after-send pattern.
func TestConcurrentReservesCannotOvershoot(t *testing.T) {
	const hardCap = 5
	const reservers = 30
	p := memprotect.NewWriteProtector(
		time.Hour,
		coalesce.NewQuota(hardCap, time.Hour),
	)
	ctx := context.Background()
	w := p.Tenant("").Namespace("")
	meta := protect.RequestMeta{TargetKey: "k-race"}

	var proceeds, defers atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < reservers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Distinct content per reserver so dedupe doesn't fire — only
			// the quota cap is being exercised.
			content := "h" + string(rune('a'+i%26)) + string(rune('0'+i/26))
			out, _ := w.CheckAndReserve(ctx, meta, content)
			switch out.Decision {
			case protect.DecisionProceed:
				proceeds.Add(1)
			case protect.DecisionDeferred:
				defers.Add(1)
			default:
				t.Errorf("unexpected outcome %s", out.Decision)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := proceeds.Load(); got != hardCap {
		t.Errorf("Proceeds = %d, want exactly %d (atomicity)", got, hardCap)
	}
	if got := defers.Load(); got != reservers-hardCap {
		t.Errorf("Defers = %d, want %d", got, reservers-hardCap)
	}
}

// TestReleaseClearsDedupeForIdenticalRetry verifies the wire-failure
// recovery contract: after a Proceed reserves a slot for some content
// hash and the gated op subsequently fails, calling the release closure
// must leave the dedupe gate inert so an identical retry can proceed.
//
// This is the "record-on-success" invariant that consumers driven by an
// upstream that re-delivers on transient failure (a typical webhook
// source) depend on — an identical retry after a failed send must not
// be dedupe-suppressed. The memory backend gets this for free via
// [sendstate.Entry.Seen]'s len(LastNSendTimes) == 0 → false guard; this
// test pins that semantic so a future refactor can't silently regress
// it. The mirroring Mongo test lives in
// protect/mongodb/checkandreserve_test.go.
func TestReleaseClearsDedupeForIdenticalRetry(t *testing.T) {
	// Wide quota so dedupe is the only suppressor under test.
	p := memprotect.NewWriteProtector(
		time.Hour,
		coalesce.NewQuota(1000, time.Hour),
	)
	ctx := context.Background()
	w := p.Tenant("").Namespace("")
	meta := protect.RequestMeta{TargetKey: "k-retry"}

	// First reserve proceeds and stamps contentHash=h1.
	out, release := w.CheckAndReserve(ctx, meta, "h1")
	if out.Decision != protect.DecisionProceed {
		t.Fatalf("first CheckAndReserve = %s, want Proceed", out.Decision)
	}
	if release == nil {
		t.Fatal("first CheckAndReserve: release is nil")
	}

	// Simulate a wire-failure rollback. contentHash stays as h1 on the
	// entry; the dedupe gate must still be inert because LastNSendTimes
	// is now empty.
	if err := release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Upstream's identical retry: same target, same content. Must proceed
	// — a "duplicate" verdict here would lose the work the original wire
	// call failed to deliver.
	out2, release2 := w.CheckAndReserve(ctx, meta, "h1")
	if out2.Decision != protect.DecisionProceed {
		t.Fatalf("post-release identical retry = %s, want Proceed (record-on-success invariant)", out2.Decision)
	}
	if release2 == nil {
		t.Fatal("post-release retry: release is nil")
	}

	// Now an actual cascade duplicate AFTER a successful proceed IS
	// suppressed — Seen() returns true because LastNSendTimes is non-empty.
	out3, _ := w.CheckAndReserve(ctx, meta, "h1")
	if out3.Decision != protect.DecisionDeduped {
		t.Fatalf("post-success identical retry = %s, want Deduped", out3.Decision)
	}
}

// (TestExistingCheckPathUnchanged was a Phase 1 regression guard pinning
// the legacy Check + RecordAsSent flow alongside CheckAndReserve. Phase
// 2a removed Check from the public namespace interfaces, so the guard
// has no method to exercise; CheckAndReserve outcomes are pinned by
// TestWriteCheckAndReserveOutcomes earlier in this file.)
