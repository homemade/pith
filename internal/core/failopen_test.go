package core_test

import (
	"context"
	"errors"
	"testing"

	"github.com/homemade/pith/internal/core"
	"github.com/homemade/pith/sendstate"
)

// failingStore is a [sendstate.Store] stub that returns a configured
// error from ReadEntry (and benign behavior from the rest). It's used
// to exercise [core.Protector.Check]'s fail-open contract — the
// other methods are inert so the test pins exactly the error path
// under test.
type failingStore struct {
	readErr error
}

func (f *failingStore) ReadEntry(context.Context, string) (sendstate.Entry, error) {
	return sendstate.Entry{}, f.readErr
}
func (f *failingStore) ReadMetrics(context.Context, string) (sendstate.Metrics, bool, error) {
	return sendstate.Metrics{}, false, nil
}
func (f *failingStore) RecordAsSent(context.Context, string, string) error {
	return nil
}
func (f *failingStore) RecordAsDeferred(context.Context, string, []byte) error {
	return nil
}
func (f *failingStore) RangeDeferred(context.Context, int, func(string, sendstate.Entry) bool) error {
	return nil
}

// Backing-store errors on the ReadEntry that drives Check are
// fail-open: the returned err is non-nil but Decision is
// DecisionProceed, so callers over-send rather than dropping work.
// The contract is documented at the top of protect.go ("Backing-store
// errors are fail-open: a non-nil error from Check carries Decision
// == DecisionProceed").
func TestProtector_Check_FailsOpenOnReadEntryError(t *testing.T) {
	wantErr := errors.New("simulated store outage")
	p := core.New(&failingStore{readErr: wantErr})

	out := p.Check(context.Background(), core.Request{
		RequestMeta: core.RequestMeta{TargetKey: "k1"},
		ContentHash: "h1",
	})

	if !errors.Is(out.Err, wantErr) {
		t.Fatalf("Outcome.Err = %v, want to wrap %v", out.Err, wantErr)
	}
	if out.Decision != core.DecisionProceed {
		t.Fatalf("Outcome.Decision = %s, want DecisionProceed (fail-open)", out.Decision)
	}
	if out.Reason != "" {
		t.Fatalf("Outcome.Reason = %q, want empty (no policy fired before the error)", out.Reason)
	}
}
