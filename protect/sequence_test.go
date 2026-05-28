package protect_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/protect"
	"github.com/homemade/pith/sendstate"
	"github.com/homemade/pith/sendstate/memory"
	"github.com/joineduptech/doc/sequencerec"
)

// The diagram is recorded at two levels. A Client lifeline drives the
// Protector through its public surface (New / Check / RecordAsSent /
// ReplayCandidates), recorded by recordingProtector. The Protector
// in turn drives its collaborators (Store, Debounce, Quota),
// recorded by the wrappers below — every one of their arrows has
// source "Protector", so they nest inside the Protector's activation
// bar. Protector.Check fetches one Entry up-front and feeds it to every
// policy — the diagram shows that as a single ReadEntry arrow followed
// by pure-function policy arrows that don't go back to the store.

// recordingProtector wraps a [protect.Protector] so the public calls a
// client makes show up as the outermost Client->Protector arrows. Each
// call is bracketed with Enter/Exit so the Protector's activation bar
// spans the collaborator calls it makes inside.
type recordingProtector struct {
	inner *protect.Protector
	rec   *sequencerec.Recorder
}

func (w *recordingProtector) Check(ctx context.Context, req protect.Request) protect.Outcome {
	w.rec.Enter("Client", "Protector", "Check", nil)
	out := w.inner.Check(ctx, req)
	w.rec.Exit([]any{outcomeLabel(out)})
	return out
}

func (w *recordingProtector) RecordAsSent(ctx context.Context, req protect.Request) error {
	w.rec.Enter("Client", "Protector", "RecordAsSent", nil)
	err := w.inner.RecordAsSent(ctx, req)
	w.rec.Exit([]any{err})
	return err
}

func (w *recordingProtector) ReplayCandidates(ctx context.Context, limit int) ([]protect.DeferredRequest, error) {
	w.rec.Enter("Client", "Protector", "ReplayCandidates", []any{limit})
	ready, err := w.inner.ReplayCandidates(ctx, limit)
	w.rec.Exit([]any{fmt.Sprintf("%d DeferredRequest", len(ready))})
	return ready, err
}

// outcomeLabel renders a Check outcome as a single return value,
// folding the deferral Reason in so the diagram shows which mechanism
// fired without a separate arrow.
func outcomeLabel(out protect.Outcome) string {
	if out.Reason != "" {
		return fmt.Sprintf("%s (%s)", out.Decision, out.Reason)
	}
	return out.Decision.String()
}

// recordingCap wraps a coalesce.Coalescer and tags its diagram
// participant with a caller-supplied name (e.g. "Debounce",
// "Quota") so each attached cap shows up as its own lifeline. Its
// arrows originate from the Protector, which is what calls ShouldDefer.
type recordingCap struct {
	inner coalesce.Coalescer
	rec   *sequencerec.Recorder
	name  string
}

func (r *recordingCap) ShouldDefer(entry sendstate.Entry, now time.Time) bool {
	d := r.inner.ShouldDefer(entry, now)
	r.rec.RecordCall("Protector", r.name, "ShouldDefer", nil, []any{d})
	return d
}

func (r *recordingCap) CapPolicy() (name string, hardCap int, window time.Duration) {
	return r.inner.CapPolicy()
}

// recordingSendStore wraps a [sendstate.Store] so the read + write
// surface the Protector uses surfaces in the diagram as Protector->Store
// arrows. ReadEntry is recorded explicitly — it's the single read per
// Check that drives every downstream policy.
type recordingSendStore struct {
	inner sendstate.Store
	rec   *sequencerec.Recorder

	// capsClear, when set, lets RangeDeferred annotate each swept
	// entry with the replay verdict Protector.capsClear reaches for
	// it. It mirrors that check using the *unwrapped* coalescers, so
	// the annotation doesn't emit a second round of ShouldDefer
	// arrows on top of the ones the real sweep already records.
	capsClear func(e sendstate.Entry) bool
}

func (r *recordingSendStore) ReadEntry(ctx context.Context, key string) (sendstate.Entry, error) {
	entry, err := r.inner.ReadEntry(ctx, key)
	r.rec.RecordCall("Protector", "Store", "ReadEntry", []any{key}, []any{"entry", err})
	return entry, err
}

func (r *recordingSendStore) ReadMetrics(ctx context.Context, key string) (sendstate.Metrics, bool, error) {
	met, ok, err := r.inner.ReadMetrics(ctx, key)
	r.rec.RecordCall("Protector", "Store", "ReadMetrics", []any{key},
		[]any{fmt.Sprintf("TotalSent=%d", met.TotalSent), ok, err})
	return met, ok, err
}

func (r *recordingSendStore) RangeDeferred(ctx context.Context, limit int, fn func(key string, e sendstate.Entry) bool) error {
	// Enter before delegating so the scan's activation bar spans the
	// per-entry cap checks the callback drives; Exit after so the
	// return arrow follows them. MemoryStore.RangeDeferred never
	// errors, so the recorded return is nil.
	r.rec.Enter("Protector", "Store", "RangeDeferred", []any{limit})
	err := r.inner.RangeDeferred(ctx, limit, func(key string, e sendstate.Entry) bool {
		r.rec.NoteOver("Protector", fmt.Sprintf("examine %s", key))
		keep := fn(key, e) // runs Protector.capsClear → records the ShouldDefer arrows
		if r.capsClear != nil {
			if r.capsClear(e) {
				r.rec.NoteOver("Protector", "caps clear → replay candidate")
			} else {
				r.rec.NoteOver("Protector", "caps not clear → skip")
			}
		}
		return keep
	})
	r.rec.Exit([]any{err})
	return err
}

func (r *recordingSendStore) RecordAsSent(ctx context.Context, key, contentHash string) error {
	err := r.inner.RecordAsSent(ctx, key, contentHash)
	r.rec.RecordCall("Protector", "Store", "RecordAsSent", []any{key, contentHash}, []any{err})
	return err
}

func (r *recordingSendStore) RecordAsDeferred(ctx context.Context, key string, messageRef []byte) error {
	err := r.inner.RecordAsDeferred(ctx, key, messageRef)
	r.rec.RecordCall("Protector", "Store", "RecordAsDeferred", []any{key, fmt.Sprintf("<%d bytes>", len(messageRef))}, []any{err})
	return err
}

// TestProtectorScenarios exercises each [protect.Protector.Check]
// decision branch and emits a Mermaid sequence diagram next to this
// file (sequence_test.md).
func TestProtectorScenarios(t *testing.T) {
	rec := sequencerec.New()
	rec.SetActor("Client")
	ctx := context.Background()

	const capWindow = 24 * time.Hour
	// Short debounce window so the at-cap scenario below can wait
	// it out and exercise the quota Coalescer's ShouldDefer
	// arrow distinctly. Realistic deployments use seconds-to-minutes.
	const debounceWindow = 30 * time.Millisecond
	const hardCap = 2

	// Track the observed interfaces so a method added to either surface
	// fails this test until a scenario exercises it (or it's allowlisted
	// in AssertCovered below). The facade *Protector is deliberately not
	// tracked — the diagram documents a chosen subset of its surface.
	rec.Track("Store", reflect.TypeOf((*sendstate.Store)(nil)).Elem())
	rec.Track("Debounce", reflect.TypeOf((*coalesce.Coalescer)(nil)).Elem())
	rec.Track("Quota", reflect.TypeOf((*coalesce.Coalescer)(nil)).Elem())

	// Both caps are instances of the one coalesce.Coalescer type, not
	// distinct types — label the lifelines UML role:type form so the
	// diagram doesn't imply Debounce/Quota types the package lacks.
	// Arrows and coverage still key on "Debounce" / "Quota".
	rec.Describe("Debounce", "debounce : Coalescer")
	rec.Describe("Quota", "quota : Coalescer")

	innerStore := memory.New(capWindow)
	recStore := &recordingSendStore{inner: innerStore, rec: rec}
	innerDebounce := coalesce.NewLeadingEdgeDebounce(debounceWindow)
	innerQuota := coalesce.NewQuota(hardCap, capWindow)

	// Quiet mirror of Protector.capsClear over the unwrapped
	// coalescers — used only by the replay-sweep scenario to label
	// each swept entry's verdict without doubling its ShouldDefer
	// arrows in the diagram.
	recStore.capsClear = func(e sendstate.Entry) bool {
		now := time.Now()
		return !innerDebounce.ShouldDefer(e, now) && !innerQuota.ShouldDefer(e, now)
	}

	// Fold construction into the first scenario's diagram section so
	// the wiring shows once, ahead of the first Check. SetScope tags
	// the New() arrow with the same name the first rec.Run uses, so
	// they render together.
	const scenario1 = "first send proceeds and is recorded"
	rec.SetScope(scenario1)
	rec.Enter("Client", "Protector", "New", []any{
		"store",
		"WithCoalescer(Debounce 1/30ms)",
		"WithCoalescer(Quota 2/24h)",
	})

	// Content dedupe is always applied via sendstate.Entry.Seen
	// (no Coalescer to wrap), so it shows in the diagram only as the
	// Check outcome, not a separate participant. Wire debounce
	// *before* quota so the diagram evaluates the short-window cap
	// first (cheaper / source-driven check).
	p := protect.New(
		recStore,
		protect.WithCoalescer(
			&recordingCap{inner: innerDebounce, rec: rec, name: "Debounce"},
		),
		protect.WithCoalescer(
			&recordingCap{inner: innerQuota, rec: rec, name: "Quota"},
		),
	)
	rec.Exit([]any{"*Protector"})
	pr := &recordingProtector{inner: p, rec: rec}

	rec.Run(t, scenario1, func(t *testing.T) {
		req := protect.Request{
			RequestMeta: protect.RequestMeta{
				TargetKey:  "act-1:contact-1",
				MessageRef: []byte("activity-A"),
			},
			ContentHash: "hash-A",
		}
		rec.Note("Check(content=hash-A, target=act-1:contact-1)")
		out := pr.Check(ctx, req)
		if out.Err != nil {
			t.Fatalf("Check: %v", out.Err)
		}
		if out.Decision != protect.DecisionProceed {
			t.Fatalf("want Proceed, got %s", out.Decision)
		}
		rec.Note(fmt.Sprintf("→ %s", out.Decision))

		rec.Note("send to downstream succeeded → RecordAsSent")
		if err := pr.RecordAsSent(ctx, req); err != nil {
			t.Fatalf("RecordAsSent: %v", err)
		}

		// Verify the recorded send is reflected as TotalSent=1 for the
		// target. Reads directly from the raw store — not via a Protector
		// API (none exposes Metrics anymore), so this read does NOT show
		// up as a Protector→Store arrow in the diagram.
		met, ok, err := innerStore.ReadMetrics(ctx, req.TargetKey)
		if err != nil {
			t.Fatalf("ReadMetrics: %v", err)
		}
		if !ok {
			t.Fatalf("ReadMetrics: want ok=true after a recorded send")
		}
		if met.TotalSent != 1 {
			t.Fatalf("TotalSent = %d, want 1 after first send", met.TotalSent)
		}
	})

	rec.Run(t, "duplicate content to the same target is deduped", func(t *testing.T) {
		req := protect.Request{
			RequestMeta: protect.RequestMeta{
				TargetKey:  "act-1:contact-1", // same target as scenario 1
				MessageRef: []byte("activity-A-dup"),
			},
			ContentHash: "hash-A", // same content as scenario 1
		}
		rec.Note("Check(content=hash-A, target=act-1:contact-1)")
		out := pr.Check(ctx, req)
		if out.Err != nil {
			t.Fatalf("Check: %v", out.Err)
		}
		if out.Decision != protect.DecisionDeduped {
			t.Fatalf("want Deduped, got %s", out.Decision)
		}
		rec.Note(fmt.Sprintf("→ %s (%s) — no breadcrumb stamped", out.Decision, out.Reason))
	})

	rec.Run(t, "same-target follow-up within debounce window is deferred", func(t *testing.T) {
		req := protect.Request{
			RequestMeta: protect.RequestMeta{
				TargetKey:  "act-1:contact-1", // same target as scenario 1
				MessageRef: []byte("activity-B"),
			},
			ContentHash: "hash-B", // new content
		}
		rec.Note("Check(content=hash-B, target=act-1:contact-1) — 1 send within debounce window")
		out := pr.Check(ctx, req)
		if out.Err != nil {
			t.Fatalf("Check: %v", out.Err)
		}
		if out.Decision != protect.DecisionDeferred {
			t.Fatalf("want Deferred, got %s", out.Decision)
		}
		if want := "leading-edge debounce 30ms"; out.Reason != want {
			t.Fatalf("want Reason=%q, got %q", want, out.Reason)
		}
		rec.Note(fmt.Sprintf("→ %s (%s) — deferred breadcrumb stamped", out.Decision, out.Reason))
	})

	rec.Run(t, "deferred at quota cap with breadcrumb stamped", func(t *testing.T) {
		// Pre-populate the quota counter for contact-3 directly on
		// the raw store so the setup steps don't show in the
		// diagram. Then sleep long enough that the setup sends fall
		// outside the debounce window — the debounce ShouldDefer
		// will return false (no recent send), so the diagram shows
		// debounce checking and clearing before quota trips.
		_ = innerStore.RecordAsSent(ctx, "act-1:contact-3", "setup-1")
		_ = innerStore.RecordAsSent(ctx, "act-1:contact-3", "setup-2")
		time.Sleep(2 * debounceWindow)

		req := protect.Request{
			RequestMeta: protect.RequestMeta{
				TargetKey:  "act-1:contact-3",
				MessageRef: []byte("activity-C"),
			},
			ContentHash: "hash-C",
		}
		rec.Note("Check(content=hash-C, target=act-1:contact-3) — quota at hardCap=2, debounce window expired")
		out := pr.Check(ctx, req)
		if out.Err != nil {
			t.Fatalf("Check: %v", out.Err)
		}
		if out.Decision != protect.DecisionDeferred {
			t.Fatalf("want Deferred, got %s", out.Decision)
		}
		if want := "quota cap 2 per 24h"; out.Reason != want {
			t.Fatalf("want Reason=%q, got %q", want, out.Reason)
		}
		rec.Note(fmt.Sprintf("→ %s (%s) — deferred breadcrumb stamped", out.Decision, out.Reason))
	})

	rec.Run(t, "below-cap send proceeds; RecordAsSent appends to sendstate", func(t *testing.T) {
		req := protect.Request{
			RequestMeta: protect.RequestMeta{
				TargetKey:  "act-1:contact-4",
				MessageRef: []byte("activity-D"),
			},
			ContentHash: "hash-D",
		}
		rec.Note("Check(content=hash-D, target=act-1:contact-4) — counts start at 0")
		out := pr.Check(ctx, req)
		if out.Err != nil {
			t.Fatalf("Check: %v", out.Err)
		}
		if out.Decision != protect.DecisionProceed {
			t.Fatalf("want Proceed, got %s", out.Decision)
		}
		rec.Note(fmt.Sprintf("→ %s", out.Decision))
		if err := pr.RecordAsSent(ctx, req); err != nil {
			t.Fatalf("RecordAsSent: %v", err)
		}
	})

	rec.Run(t, "replay sweep returns deferrals whose caps have cleared", func(t *testing.T) {
		// Scenarios 3 and 4 stamped deferred breadcrumbs on contact-1
		// (debounce window) and contact-3 (quota cap). By now the debounce
		// window has long since elapsed — scenario 4 slept past it — so
		// contact-1's caps clear and it's replay-ready. contact-3 is still
		// inside the 24h quota window, so the sweep skips it: re-emitting
		// would only defer again and waste the re-derivation. The sweep
		// visits oldest-deferral first (contact-1, then contact-3).
		rec.Note("ReplayCandidates(limit=10) — replay sweep")
		ready, err := pr.ReplayCandidates(ctx, 10)
		if err != nil {
			t.Fatalf("ReplayCandidates: %v", err)
		}
		if len(ready) != 1 {
			t.Fatalf("want 1 replay-ready entry, got %d: %+v", len(ready), ready)
		}
		if ready[0].TargetKey != "act-1:contact-1" {
			t.Fatalf("want act-1:contact-1, got %q", ready[0].TargetKey)
		}
		if string(ready[0].MessageRef) != "activity-B" {
			t.Fatalf("want MessageRef=activity-B, got %q", string(ready[0].MessageRef))
		}
		rec.Note(fmt.Sprintf("→ %d ready: %s (MessageRef %q)",
			len(ready), ready[0].TargetKey, string(ready[0].MessageRef)))
	})

	t.Cleanup(func() {
		rec.WriteMermaid(t)
		// CapPolicy is infrastructure New() uses to size the store —
		// not part of the per-Check story this diagram tells.
		// ReadMetrics is no longer reachable through any Protector
		// surface (Protector.Metrics was removed); scenario 1 reads
		// it directly off the raw store as a test verification step,
		// which intentionally bypasses the recording wrapper so it
		// doesn't show up as a Protector→Store arrow in the diagram.
		rec.AssertCovered(t,
			"Debounce.CapPolicy",
			"Quota.CapPolicy",
			"Store.ReadMetrics",
		)
		// Explicit guard: both cap policies must stay observable as
		// their own lifelines, evaluating an Entry via ShouldDefer.
		// AssertCovered would also trip if these went dark, but only
		// incidentally (the tracked names would record nothing); this
		// states the requirement directly so a refactor that stops
		// wrapping the caps fails with a reason, not a coverage riddle.
		for _, cap := range []string{"Debounce", "Quota"} {
			if !rec.Recorded(cap, "ShouldDefer") {
				t.Errorf("diagram must show %s.ShouldDefer — the %s cap is no longer observable", cap, cap)
			}
		}
	})
}
