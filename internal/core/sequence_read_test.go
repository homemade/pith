package core_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/internal/core"
	"github.com/homemade/pith/protect"
	"github.com/homemade/pith/sendstate"
	"github.com/homemade/pith/sendstate/memory"
	"github.com/joineduptech/doc/sequencerec"
)

// This file documents the read gate (the write gate's machinery minus
// content dedupe). recordingReadProtector drives a [core.ReadGate] through
// its public surface (NewRead / Check / RecordAsSent / ReplayCandidates); the
// collaborator wrappers (recordingSendStore, recordingCap) and outcomeLabel
// are shared with sequence_write_test.go. The diagram is emitted to
// sequence_read_test.md (WriteMermaid keys the filename on the source file
// that calls New, so it sits beside the write-gate diagram, not on top of it).

// recordingReadProtector wraps a [core.ReadGate] so the public calls a client
// makes show up as the outermost Client->Protector arrows. Mirrors
// recordingProtector (write gate) without the contentHash — a read has no
// payload to fingerprint.
type recordingReadProtector struct {
	inner protect.ReadNamespace
	rec   *sequencerec.Recorder
}

func (r *recordingReadProtector) Check(ctx context.Context, meta protect.RequestMeta) protect.Outcome {
	r.rec.Enter("Client", "Protector", "Check", nil)
	out := r.inner.Check(ctx, meta)
	r.rec.Exit([]any{outcomeLabel(out)})
	return out
}

func (r *recordingReadProtector) RecordAsSent(ctx context.Context, meta protect.RequestMeta) error {
	r.rec.Enter("Client", "Protector", "RecordAsSent", nil)
	err := r.inner.RecordAsSent(ctx, meta)
	r.rec.Exit([]any{err})
	return err
}

func (r *recordingReadProtector) ReplayCandidates(ctx context.Context, limit int) ([]protect.DeferredRequest, error) {
	r.rec.Enter("Client", "Protector", "ReplayCandidates", []any{limit})
	ready, err := r.inner.ReplayCandidates(ctx, limit)
	r.rec.Exit([]any{fmt.Sprintf("%d DeferredRequest", len(ready))})
	return ready, err
}

// TestReadGateScenarios exercises each [core.ReadGate] Check decision branch —
// the write gate's machinery minus content dedupe — paired with a
// trailing-edge debounce, and emits a Mermaid sequence diagram next to this
// file (sequence_read_test.md). The read gate DEFERS (not drops) a capped
// read, stamps a breadcrumb, and replays it once the burst goes quiet, so the
// final state is read — exactly what a dropped cap-suppression would lose.
func TestReadGateScenarios(t *testing.T) {
	rec := sequencerec.New()
	rec.SetActor("Client")
	ctx := context.Background()

	// A short trailing-edge window so the scenarios can wait it out
	// deterministically; the store TTL is far larger so a deferred entry
	// survives until the sweep re-takes it (a window-sized TTL could expire
	// the breadcrumb at the very moment the window clears). Realistic
	// deployments use seconds-to-minutes windows.
	const debounceWindow = 100 * time.Millisecond
	const entryTTL = 24 * time.Hour

	// Track the collaborator interfaces so a method added to either fails
	// this test until a scenario exercises it (or it's allowlisted in
	// AssertCovered below). The facade ReadGate is deliberately not tracked —
	// the diagram documents a chosen subset of its surface.
	rec.Track("Store", reflect.TypeOf((*sendstate.Store)(nil)).Elem())
	rec.Track("Debounce", reflect.TypeOf((*coalesce.Coalescer)(nil)).Elem())

	// The cap is a coalesce.Coalescer instance, not a distinct type — label
	// the lifeline UML role:type form so the diagram doesn't imply a type the
	// package lacks. Arrows and coverage still key on "Debounce".
	rec.Describe("Debounce", "debounce : Coalescer")

	innerStore := memory.New(entryTTL)
	recStore := &recordingSendStore{inner: innerStore, rec: rec}
	innerDebounce := coalesce.NewTrailingEdgeDebounce(debounceWindow)

	// Quiet mirror of the gate's capsClear over the unwrapped coalescer —
	// used by the replay-sweep scenarios to label each swept entry's verdict
	// without doubling its ShouldDefer arrows in the diagram.
	recStore.capsClear = func(e sendstate.Entry) bool {
		return !innerDebounce.ShouldDefer(e, time.Now())
	}

	// Fold construction into the first scenario's diagram section so the
	// wiring shows once, ahead of the first Check.
	const scenario1 = "first read proceeds and is recorded"
	rec.SetScope(scenario1)
	rec.Enter("Client", "Protector", "NewRead", []any{
		"store",
		"Trailing 100ms",
	})
	// A read gate has no content dedupe (no payload to fingerprint), so Check
	// takes no hash and there is no Deduped branch — a capped read defers and
	// is replayable.
	p := core.NewRead(
		recStore,
		&recordingCap{inner: innerDebounce, rec: rec, name: "Debounce"},
	)
	rec.Exit([]any{"core.ReadGate"})
	pr := &recordingReadProtector{inner: p.Namespace(""), rec: rec}

	rec.Run(t, scenario1, func(t *testing.T) {
		meta := protect.RequestMeta{TargetKey: "campaign-1:webhooks:profile-1", MessageRef: []byte("profile-1@v1")}
		rec.Note("Check(target=campaign-1:webhooks:profile-1) — leading fire, no recent activity")
		out := pr.Check(ctx, meta)
		if out.Err != nil {
			t.Fatalf("Check: %v", out.Err)
		}
		if out.Decision != protect.DecisionProceed {
			t.Fatalf("want Proceed, got %s", out.Decision)
		}
		rec.Note(fmt.Sprintf("→ %s", out.Decision))

		rec.Note("read current state succeeded → RecordAsSent")
		if err := pr.RecordAsSent(ctx, meta); err != nil {
			t.Fatalf("RecordAsSent: %v", err)
		}
	})

	rec.Run(t, "same-target read within the window is deferred (breadcrumb stamped)", func(t *testing.T) {
		meta := protect.RequestMeta{TargetKey: "campaign-1:webhooks:profile-1", MessageRef: []byte("profile-1@v2")}
		rec.Note("Check(target=campaign-1:webhooks:profile-1) — burst: a send is within the window")
		out := pr.Check(ctx, meta)
		if out.Err != nil {
			t.Fatalf("Check: %v", out.Err)
		}
		if out.Decision != protect.DecisionDeferred {
			t.Fatalf("want Deferred, got %s", out.Decision)
		}
		if want := "trailing-edge debounce 100ms"; out.Reason != want {
			t.Fatalf("want Reason=%q, got %q", want, out.Reason)
		}
		rec.Note(fmt.Sprintf("→ %s (%s) — deferred breadcrumb stamped (MessageRef profile-1@v2)", out.Decision, out.Reason))
	})

	rec.Run(t, "replay sweep while the burst is active returns nothing", func(t *testing.T) {
		// The key was deferred a moment ago, so the trailing-edge window has
		// not cleared — the sweep skips it. Re-reading now would only defer
		// again; trailing-edge holds the burst until it goes quiet.
		rec.Note("ReplayCandidates(limit=10) — burst still active")
		ready, err := pr.ReplayCandidates(ctx, 10)
		if err != nil {
			t.Fatalf("ReplayCandidates: %v", err)
		}
		if len(ready) != 0 {
			t.Fatalf("want 0 replay-ready while the window is open, got %d: %+v", len(ready), ready)
		}
		rec.Note("→ 0 ready (window open)")
	})

	rec.Run(t, "after quiet, the sweep yields the final read", func(t *testing.T) {
		// Wait out the trailing-edge window: the last send and the last
		// deferral both age past it, so the key goes quiet and its caps
		// clear. The sweep now yields the deferred breadcrumb; the consumer
		// re-derives from MessageRef and re-reads current state — the
		// trailing/final fire, capturing the latest value.
		time.Sleep(2 * debounceWindow)
		rec.Note("ReplayCandidates(limit=10) — window cleared (gone quiet)")
		ready, err := pr.ReplayCandidates(ctx, 10)
		if err != nil {
			t.Fatalf("ReplayCandidates: %v", err)
		}
		if len(ready) != 1 {
			t.Fatalf("want 1 replay-ready after quiet, got %d: %+v", len(ready), ready)
		}
		if ready[0].TargetKey != "campaign-1:webhooks:profile-1" {
			t.Fatalf("want campaign-1:webhooks:profile-1, got %q", ready[0].TargetKey)
		}
		if string(ready[0].MessageRef) != "profile-1@v2" {
			t.Fatalf("want MessageRef=profile-1@v2, got %q", string(ready[0].MessageRef))
		}
		rec.Note(fmt.Sprintf("→ %d ready: %s (MessageRef %q) — consumer re-reads current state",
			len(ready), ready[0].TargetKey, string(ready[0].MessageRef)))
	})

	t.Cleanup(func() {
		rec.WriteMermaid(t)
		// CapPolicy is infrastructure NewRead() uses to size the store — not
		// part of the per-Check story. ReadMetrics is not reachable through
		// any gate surface and no scenario reads it here.
		rec.AssertCovered(t,
			"Debounce.CapPolicy",
			"Store.ReadMetrics",
		)
		// Explicit guard: the cap policy must stay observable as its own
		// lifeline, evaluating an Entry via ShouldDefer.
		if !rec.Recorded("Debounce", "ShouldDefer") {
			t.Errorf("diagram must show Debounce.ShouldDefer — the cap is no longer observable")
		}
	})
}
