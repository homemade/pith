package core_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/internal/core"
	"github.com/homemade/pith/protect"
	"github.com/homemade/pith/sendstate"
)

// failingStore is a [sendstate.Store] stub used to exercise the gate's
// error-path contracts: CheckAndReserve's fail-closed. Configure the
// error you want to inject via the matching field; un-set fields stay
// benign.
type failingStore struct {
	reserveErr error
}

func (f *failingStore) ReadEntry(context.Context, string) (sendstate.Entry, error) {
	return sendstate.Entry{}, nil
}
func (f *failingStore) ReadMetrics(context.Context, string) (sendstate.Metrics, bool, error) {
	return sendstate.Metrics{}, false, nil
}
func (f *failingStore) RecordAsSent(context.Context, string, string, string, string) error {
	return nil
}
func (f *failingStore) RecordAsDeferred(context.Context, string, string, string, []byte) error {
	return nil
}
func (f *failingStore) RangeDeferred(context.Context, int, string, func(string, sendstate.Entry) bool) error {
	return nil
}
func (f *failingStore) CheckAndReserve(context.Context, sendstate.ReserveRequest, []byte) (sendstate.ReserveResult, error) {
	if f.reserveErr != nil {
		// Mirror the production backends: on error, fail-closed result
		// + the err. The gate respects whichever value the store returns
		// in res.Deferred; making the stub set it here means the gate's
		// own fail-closed path doesn't depend on the stub's helpfulness.
		return sendstate.ReserveResult{Deferred: true, Reason: "store error"}, f.reserveErr
	}
	return sendstate.ReserveResult{}, nil
}
func (f *failingStore) ReleaseReservation(context.Context, string, time.Time) error {
	return nil
}
func (f *failingStore) PlaceHold(context.Context, string, time.Time, time.Time, string) error {
	return nil
}
func (f *failingStore) ClearActiveHolds(context.Context, string) error {
	return nil
}
func (f *failingStore) MostRestrictiveActiveHold(context.Context, string) (bool, protect.Hold, error) {
	return false, protect.Hold{}, nil
}

// Backing-store errors on the atomic CheckAndReserve are fail-CLOSED —
// the polarity opposite of Check above. The polarity matters because
// CheckAndReserve exists to enforce a hard cap; a fail-open on store
// error would let a Mongo blip breach the very cap CheckAndReserve was
// built to defend. Fail-closed surfaces Deferred + Err so the replay
// sweep re-drives the request once the store recovers.
func TestGate_CheckAndReserve_FailsClosedOnStoreError(t *testing.T) {
	wantErr := errors.New("simulated reserve outage")
	w := core.NewWrite(&failingStore{reserveErr: wantErr}, coalesce.NewQuota(1, time.Hour))

	out, release := w.Tenant("").Namespace("").CheckAndReserve(context.Background(), protect.RequestMeta{TargetKey: "k1"}, "h1")

	if !errors.Is(out.Err, wantErr) {
		t.Fatalf("Outcome.Err = %v, want to wrap %v", out.Err, wantErr)
	}
	if out.Decision != protect.DecisionDeferred {
		t.Fatalf("Outcome.Decision = %s, want DecisionDeferred (fail-closed)", out.Decision)
	}
	if release != nil {
		t.Fatal("Outcome release is non-nil on fail-closed Deferred — nothing was reserved, nothing to release")
	}
}
