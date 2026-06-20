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
// Both constructors require a caller-owned [*mongo.Client] (configured
// with majority write concern; see [pith/sendstate/mongodb]) and at least
// one Coalescer (the (first, rest...) shape). The client is the caller's
// to share across pith stores or other libraries and to Disconnect at
// shutdown.
package mongodb

import (
	"context"
	"errors"
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
// DEFER and are replayable via ReplayCandidates) on the caller-owned
// client, or an error. MaxSendTimes is handled as described on
// [NewWriteProtector].
func NewReadProtector(ctx context.Context, client *mongo.Client, cfg Config, first coalesce.Coalescer, rest ...coalesce.Coalescer) (protect.ReadProtector, error) {
	coalescers := append([]coalesce.Coalescer{first}, rest...)
	store, err := open(ctx, client, cfg, coalescers)
	if err != nil {
		return nil, err
	}
	return core.NewRead(store, coalescers...), nil
}

// NewWriteProtector returns a [pith/protect.WriteProtector] backed by an
// Atlas mongo store (content dedupe + coalesce caps, capped Checks DEFER)
// on the caller-owned client, or an error.
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
// Other Config fields (Database, EntryTTL) are validated here; see
// [pith/sendstate/mongodb.Config] for their semantics. The client itself is
// the caller's — open it with [mongo.Connect] configured for majority write
// concern (see the [pith/sendstate/mongodb] package doc) and Disconnect at
// shutdown.
func NewWriteProtector(ctx context.Context, client *mongo.Client, cfg Config, first coalesce.Coalescer, rest ...coalesce.Coalescer) (protect.WriteProtector, error) {
	coalescers := append([]coalesce.Coalescer{first}, rest...)
	store, err := open(ctx, client, cfg, coalescers)
	if err != nil {
		return nil, err
	}
	return core.NewWrite(store, coalescers...), nil
}

// open sizes MaxSendTimes from the coalescers, builds the store over the
// caller's client, and ensures the indexes are in place.
func open(ctx context.Context, client *mongo.Client, cfg Config, coalescers []coalesce.Coalescer) (*mongostore.Store, error) {
	if client == nil {
		return nil, errors.New("mongodb: client is required")
	}
	if cfg.Database == "" {
		return nil, errors.New("mongodb: Config.Database is required")
	}
	if cfg.EntryTTL <= 0 {
		return nil, errors.New("mongodb: Config.EntryTTL is required (> 0)")
	}

	derived := core.LargestHardCap(coalescers...)
	switch {
	case cfg.MaxSendTimes == 0:
		cfg.MaxSendTimes = derived
	case cfg.MaxSendTimes < derived:
		return nil, fmt.Errorf("mongodb: Config.MaxSendTimes (%d) is below the largest attached Coalescer hardCap (%d) — would silently leak the cap via $slice", cfg.MaxSendTimes, derived)
	}

	var storeOpts []mongostore.Option
	if cfg.MaxSendTimes > 0 {
		storeOpts = append(storeOpts, mongostore.WithMaxSendTimes(cfg.MaxSendTimes))
	}
	store := mongostore.New(client.Database(cfg.Database), cfg.EntryTTL, storeOpts...)
	if err := store.EnsureIndexes(ctx); err != nil {
		return nil, fmt.Errorf("mongodb: EnsureIndexes: %w", err)
	}
	return store, nil
}
