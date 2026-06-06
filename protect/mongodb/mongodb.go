// Package mongodb wires a [pith/protect.ReadProtector] /
// [pith/protect.WriteProtector] to the Atlas-backed
// [pith/sendstate/mongodb.Store] backend, handling the MaxSendTimes
// contract automatically.
//
// Without this wrapper, callers would need to remember to pass
// Config.MaxSendTimes >= the largest attached Coalescer hardCap when
// constructing the mongo store; the storage layer otherwise drops
// in-window send timestamps via $slice and the cap leaks silently (the
// contract pith/sendstate/mongodb godoc on WithMaxSendTimes flags as the
// caller's responsibility). These constructors close that gap by
// deriving the value from the attached Coalescers — the same ones that
// drive Check's policy evaluation.
//
// Both constructors require at least one Coalescer (the (first, rest...)
// shape).
package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/internal/core"
	"github.com/homemade/pith/protect"
	mongostore "github.com/homemade/pith/sendstate/mongodb"
)

// Config re-exports [pith/sendstate/mongodb.Config] so callers don't
// need a separate import. Identical shape and semantics.
type Config = mongostore.Config

// NewReadProtector returns a [pith/protect.ReadProtector] backed by an
// Atlas mongo store (coalesce caps only, no content dedupe; capped Checks
// DEFER and are replayable via ReplayCandidates), plus the underlying
// *mongo.Client (so callers can Disconnect at shutdown), or an error.
// MaxSendTimes is handled as described on [NewWriteProtector].
func NewReadProtector(ctx context.Context, cfg Config, first coalesce.Coalescer, rest ...coalesce.Coalescer) (protect.ReadProtector, *mongo.Client, error) {
	coalescers := append([]coalesce.Coalescer{first}, rest...)
	store, client, err := open(ctx, cfg, coalescers)
	if err != nil {
		return nil, client, err
	}
	return core.NewRead(store, coalescers...), client, nil
}

// NewWriteProtector returns a [pith/protect.WriteProtector] backed by an
// Atlas mongo store (content dedupe + coalesce caps, capped Checks DEFER),
// plus the underlying *mongo.Client, or an error.
//
// MaxSendTimes handling — the reason these wrappers exist:
//
//   - cfg.MaxSendTimes == 0: derived from the largest hardCap among the
//     attached Coalescers. The common case.
//   - cfg.MaxSendTimes >= derived: respected — a caller wanting extra
//     headroom (e.g. for replay-bounded scans) can set a larger value.
//   - cfg.MaxSendTimes < derived: returns an error. A smaller bound would
//     silently leak the cap via $slice, so we surface the misconfiguration
//     at construction.
//
// All other Config fields (URI, Database, EntryTTL, Timeout) follow
// [pith/sendstate/mongodb.Open]'s semantics — see its godoc.
func NewWriteProtector(ctx context.Context, cfg Config, first coalesce.Coalescer, rest ...coalesce.Coalescer) (protect.WriteProtector, *mongo.Client, error) {
	coalescers := append([]coalesce.Coalescer{first}, rest...)
	store, client, err := open(ctx, cfg, coalescers)
	if err != nil {
		return nil, client, err
	}
	return core.NewWrite(store, coalescers...), client, nil
}

// open sizes MaxSendTimes from the coalescers and opens the store. On
// EnsureIndexes failure mongostore.Open returns a non-nil client so the
// caller can Disconnect; that client is surfaced here too.
func open(ctx context.Context, cfg Config, coalescers []coalesce.Coalescer) (*mongostore.Store, *mongo.Client, error) {
	derived := core.LargestHardCap(coalescers...)
	switch {
	case cfg.MaxSendTimes == 0:
		cfg.MaxSendTimes = derived
	case cfg.MaxSendTimes < derived:
		return nil, nil, fmt.Errorf("mongodb: Config.MaxSendTimes (%d) is below the largest attached Coalescer hardCap (%d) — would silently leak the cap via $slice", cfg.MaxSendTimes, derived)
	}
	return mongostore.Open(ctx, cfg)
}
