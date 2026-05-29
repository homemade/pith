// Package mongodb wires a [pith/protect.Protector] to the Atlas-backed
// [pith/sendstate/mongodb.Store] backend, handling the MaxSendTimes
// contract automatically.
//
// Without this wrapper, callers would need to remember to pass
// Config.MaxSendTimes >= the largest attached Coalescer hardCap when
// constructing the mongo store; the storage layer otherwise drops
// in-window send timestamps via $slice and the cap leaks silently
// (the contract pith/sendstate/mongodb godoc on WithMaxSendTimes flags
// as the caller's responsibility). This package's New closes
// that gap by deriving the value from the Coalescers attached via
// opts — the same opts that drive Check's policy evaluation.
package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/homemade/pith/internal/core"
	"github.com/homemade/pith/protect"
	mongostore "github.com/homemade/pith/sendstate/mongodb"
)

// Config re-exports [pith/sendstate/mongodb.Config] so callers don't
// need a separate import. Identical shape and semantics.
type Config = mongostore.Config

// New returns a Protector backed by an Atlas-backed mongo
// store, plus the underlying *mongo.Client (so callers can Disconnect
// at shutdown), or an error.
//
// MaxSendTimes handling — the reason this wrapper exists:
//
//   - cfg.MaxSendTimes == 0: derived from the largest hardCap among
//     Coalescers attached via opts. The common case: caller doesn't
//     set MaxSendTimes at all and the wrapper sizes correctly.
//   - cfg.MaxSendTimes >= derived: respected — a caller wanting
//     extra headroom (e.g. for replay-bounded scans) can set a larger
//     value explicitly.
//   - cfg.MaxSendTimes < derived: returns an error. A smaller bound
//     would silently leak the cap, so we surface the misconfiguration
//     at construction instead of letting it manifest as overshooting
//     quota in production.
//
// All other Config fields (URI, Database, EntryTTL, Timeout) follow
// [pith/sendstate/mongodb.Open]'s semantics — see its godoc.
func New(ctx context.Context, cfg Config, opts ...protect.Option) (*protect.Protector, *mongo.Client, error) {
	derived := largestHardCap(opts)
	switch {
	case cfg.MaxSendTimes == 0:
		cfg.MaxSendTimes = derived
	case cfg.MaxSendTimes < derived:
		return nil, nil, fmt.Errorf("mongodb.New: Config.MaxSendTimes (%d) is below the largest attached Coalescer hardCap (%d) — would silently leak the cap via $slice", cfg.MaxSendTimes, derived)
	}
	store, client, err := mongostore.Open(ctx, cfg)
	if err != nil {
		// On EnsureIndexes failure mongostore.Open returns a non-nil
		// client so the caller can Disconnect; surface that here too.
		return nil, client, err
	}
	return core.New(store, opts...), client, nil
}

// largestHardCap returns the largest hardCap among the Coalescers
// attached via opts, or 0 when no Coalescers are attached. Uses
// core.Inspect to avoid constructing a throwaway Protector.
func largestHardCap(opts []protect.Option) int {
	largest := 0
	for _, c := range core.Inspect(opts...) {
		_, hardCap, _ := c.CapPolicy()
		if hardCap > largest {
			largest = hardCap
		}
	}
	return largest
}
