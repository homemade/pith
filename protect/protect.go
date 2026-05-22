// Package protect composes pith's integration-guard primitives —
// [dedupe.Deduper] and zero or more [coalesce.Coalescer] cap
// policies — behind a single facade so callers can apply them all
// with one Check call per gated operation.
//
// # The mechanism set
//
// Pith offers two policy types, both read-only over a shared
// [sendstate.Store]:
//
//   - Dedupe — "have I sent this exact ContentHash for this key
//     within the dedupe window?" Active when [WithCap] is supplied
//     (which also sets the dedupe window); otherwise a no-op
//     fallback that always returns false.
//   - Coalesce — "is this key at or above some hardCap within some
//     trailing window?" Zero or more Coalescers can be attached,
//     each with its own (hardCap, window). Examples:
//
//	    [WithDebounce] 10s         → hardCap=1,  window=10s  (cluster collapse)
//	    [WithCap]      50, 24h     → hardCap=50, window=24h  (destination quota)
//	    [WithCoalescer] 5, 1m, "burst" → hardCap=5,  window=1m   (custom burst cap)
//
// At Check time the Protector evaluates dedupe first, then each
// Coalescer in attached order, and returns the first
// DecisionDeferred. Outcome.Reason distinguishes which mechanism
// produced it.
//
// # Why the policies share a shape
//
// Both mechanism types are single-method reads over the shared
// [sendstate.Store]. The store owns send-history data (timestamps
// + lifetime counters); each policy reads a snapshot to decide.
// RecordAsSent + RecordAsDeferred are the only writes, and they
// live on the store, not on the mechanisms.
//
// # Check / RecordAsSent contract
//
// Happy-path:
//
//	p := protect.New(
//	    protect.WithCap(50, 24*time.Hour),     // dedupe window + quota cap
//	    protect.WithDebounce(10*time.Second),  // cluster collapse
//	)
//
//	out, err := p.Check(ctx, protect.Request{
//	    ContentHash: contentHash,
//	    TargetKey:   activityID + ":" + contactID,
//	    MessageRef:  ref,
//	})
//	switch out.Decision {
//	case protect.DecisionProceed:
//	    if err := svc.Send(...); err == nil {
//	        _ = p.RecordAsSent(ctx, req)
//	    }
//	case protect.DecisionDeferred:
//	    // out.Reason ∈ {"duplicate content", "debounce window", "at cap", ...}
//	}
//
// Backing-store errors are fail-open: a non-nil error from Check
// carries Decision == DecisionProceed so callers over-send rather
// than dropping work.
//
// # Deferred breadcrumbs and consumer-side flush
//
// On every Coalescer-driven defer (not on dedupe defers — those
// are genuinely redundant), Check calls
// [sendstate.Store.RecordAsDeferred] to stamp LastDeferredAt and
// store the request's MessageRef in LastDeferredMessageRef. A
// consumer-side sweep can scan the store for entries where
// LastDeferredAt > LastSentAt and (now - LastDeferredAt) exceeds
// the relevant window, then re-derive the current upstream state
// from MessageRef and re-emit via Check. A successful RecordAsSent
// at the end of the re-emit clears the deferred fields.
//
// # Backends
//
// Today: the in-memory implementations from [sendstate], [dedupe],
// and [coalesce] are wired up by default, suitable for tests,
// examples, and single-process deployments.
//
// IMPORTANT — single-process limitation: the in-memory backends
// are process-local. A shared-backing implementation (e.g. the
// planned Mongo backend) is required for multi-instance use.
//
// Swap individual backends via [WithSendStore] and [WithDeduperImpl].
// Custom Deduper / Coalescer implementations passed via the
// per-mechanism options MUST read from the same [sendstate.Store]
// the Protector writes to; otherwise their reads will not see
// RecordAsSent's writes.
package protect

import (
	"context"
	"time"

	"github.com/homemade/pith/coalesce"
	"github.com/homemade/pith/dedupe"
	"github.com/homemade/pith/sendstate"
)

// namedCoalescer pairs a [coalesce.Coalescer] with the Reason
// string reported when it triggers a defer. cap and window are
// retained from the configuring [WithCap] / [WithDebounce] /
// [WithCoalescer] option so [New] can lazily construct the default
// Coalescer once the shared [sendstate.Store] is wired up; they
// are unused (zero) when [WithCoalescerImpl] supplies a pre-built
// Coalescer.
type namedCoalescer struct {
	reason string
	cap    int
	window time.Duration
	coalesce.Coalescer
}

// Protector composes pith's integration-guard mechanisms behind a
// single facade. Construct with [New].
type Protector struct {
	store      sendstate.Store
	dedupe     dedupe.Deduper
	coalescers []namedCoalescer
}

// Request is the input to [Protector.Check] and [Protector.RecordAsSent].
type Request struct {
	// TargetKey identifies the per-key slot used by dedupe and
	// every attached Coalescer (typically "{activity-id}:{contact-id}"
	// or similar). The shared key across all layers is what lets
	// them collapse to a single record per target.
	TargetKey string

	// ContentHash is a stable fingerprint of the message being
	// sent (typically a truncated cryptographic hash). It's
	// compared by dedupe to skip repeat sends of the same
	// content-hash for the same TargetKey within the dedupe
	// window.
	ContentHash string

	// MessageRef is caller-defined data stored in the sendstate
	// entry's LastDeferredMessageRef when Check returns
	// DecisionDeferred on a Coalescer branch. A sweep layer reads
	// it back to re-derive and re-emit. Typically a small
	// reference (e.g. an upstream event ID + context,
	// JSON-encoded). Ignored on the dedupe defer path (no
	// breadcrumb — the duplicate is genuinely redundant).
	MessageRef []byte
}

// Decision is the outcome of a [Protector.Check] call.
type Decision int

const (
	// DecisionProceed: caller should perform the gated operation,
	// then call [Protector.RecordAsSent] on success.
	DecisionProceed Decision = iota

	// DecisionDeferred: caller should drop the operation.
	// Outcome.Reason names which mechanism caused the defer:
	//
	//   "duplicate content" — dedupe matched.
	//   "debounce window"   — debounce Coalescer at cap (hardCap=1).
	//   "at cap"            — quota Coalescer at cap.
	//   (or a custom reason from a caller-attached Coalescer)
	//
	// On every Coalescer-driven defer, [Protector.Check] calls
	// [sendstate.Store.RecordAsDeferred] so a sweep layer can
	// later re-emit the cluster's final state.
	DecisionDeferred
)

// String returns the decision name (for logging).
func (d Decision) String() string {
	switch d {
	case DecisionProceed:
		return "Proceed"
	case DecisionDeferred:
		return "Deferred"
	default:
		return "Unknown"
	}
}

// Outcome reports the [Protector.Check] result.
type Outcome struct {
	Decision Decision

	// Reason is human-readable detail for logging.
	Reason string
}

// Option configures a [Protector].
type Option func(*config)

type config struct {
	store         sendstate.Store
	dedupe        dedupe.Deduper
	coalescers    []namedCoalescer
	capWindow     time.Duration
	capConfigured bool
}

// WithCap configures the dedupe window and attaches a quota
// Coalescer with the given (hardCap, window=capWindow). The window
// is also used by dedupe as the SeenInWindow trailing window. The
// hard cap is the per-target event count that triggers
// DecisionDeferred with Reason="at cap".
func WithCap(hardCap int, window time.Duration) Option {
	return func(c *config) {
		c.capWindow = window
		c.capConfigured = true
		c.coalescers = append(c.coalescers, namedCoalescer{
			reason: "at cap",
			cap:    hardCap,
			window: window,
		})
	}
}

// WithDebounce attaches a cluster-collapse Coalescer (hardCap=1)
// over the given window. Independent of [WithCap]'s window — debounce
// is source-driven (how long an inbound cluster of duplicate /
// related events lasts), not destination-driven. Returns
// DecisionDeferred with Reason="debounce window" when triggered.
func WithDebounce(window time.Duration) Option {
	return func(c *config) {
		c.coalescers = append(c.coalescers, namedCoalescer{
			reason: "debounce window",
			cap:    1,
			window: window,
		})
	}
}

// WithCoalescer attaches a custom Coalescer cap with the supplied
// (hardCap, window) and a caller-chosen reason string used in
// Outcome.Reason when it triggers a defer. Useful for layered cap
// policies beyond debounce + quota (e.g. a burst cap of 5 per
// minute alongside a daily cap of 50).
func WithCoalescer(hardCap int, window time.Duration, reason string) Option {
	return func(c *config) {
		c.coalescers = append(c.coalescers, namedCoalescer{
			reason: reason,
			cap:    hardCap,
			window: window,
		})
	}
}

// WithCoalescerImpl attaches a pre-built [coalesce.Coalescer] with
// the supplied reason string. Useful when a caller wants to wrap
// (e.g. for tracing / metrics) a Coalescer that's already
// constructed against the protector's shared [sendstate.Store].
// The custom impl is responsible for its own (hardCap, window);
// the reason string is what surfaces in Outcome.Reason on a defer.
func WithCoalescerImpl(c coalesce.Coalescer, reason string) Option {
	return func(cfg *config) {
		cfg.coalescers = append(cfg.coalescers, namedCoalescer{
			reason:    reason,
			Coalescer: c,
		})
	}
}

// WithSendStore swaps the default in-memory [sendstate.MemoryStore]
// for a custom backend.
func WithSendStore(s sendstate.Store) Option {
	return func(c *config) { c.store = s }
}

// WithDeduperImpl swaps the default [dedupe.Deduper] for a custom
// implementation. The custom impl must read from the same
// [sendstate.Store] supplied via [WithSendStore]; otherwise its
// SeenInWindow will not see writes from [Protector.RecordAsSent].
func WithDeduperImpl(d dedupe.Deduper) Option {
	return func(c *config) { c.dedupe = d }
}

// New returns a Protector with the requested mechanisms enabled.
// A Protector with neither [WithCap] nor any Coalescer-attaching
// option is legal but not useful (dedupe falls back to a no-op);
// the standalone packages are the right entry point if you want
// only one mechanism.
func New(opts ...Option) *Protector {
	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}

	if cfg.store == nil {
		cfg.store = sendstate.NewMemoryStore()
	}
	if cfg.dedupe == nil {
		if cfg.capConfigured {
			cfg.dedupe = dedupe.NewDeduper(cfg.store, cfg.capWindow)
		} else {
			cfg.dedupe = noopDeduper{}
		}
	}
	// Wire each attached cap with the shared store.
	for i := range cfg.coalescers {
		if cfg.coalescers[i].Coalescer == nil {
			cfg.coalescers[i].Coalescer = coalesce.NewCoalescer(
				cfg.store, cfg.coalescers[i].cap, cfg.coalescers[i].window,
			)
		}
	}
	return &Protector{
		store:      cfg.store,
		dedupe:     cfg.dedupe,
		coalescers: cfg.coalescers,
	}
}

// SendStore returns the [sendstate.Store] the Protector writes to.
func (p *Protector) SendStore() sendstate.Store { return p.store }

// Dedupe returns the dedupe mechanism, or a no-op if not enabled.
func (p *Protector) Dedupe() dedupe.Deduper { return p.dedupe }

// Check applies dedupe and each attached Coalescer in order and
// returns the first DecisionDeferred. Pure read on the cap side;
// Coalescer counts advance only when [Protector.RecordAsSent]
// appends to the [sendstate.Store] on a successful send.
//
// On every Coalescer-driven defer, Check calls
// [sendstate.Store.RecordAsDeferred] so the deferred breadcrumb is
// available to a consumer-side sweep.
//
// Backing-store errors are fail-open: the returned err is non-nil
// but Decision is DecisionProceed. A failed RecordAsDeferred stamp
// still returns DecisionDeferred so the caller sees the policy
// outcome; the error is attached for logging.
func (p *Protector) Check(ctx context.Context, req Request) (Outcome, error) {
	// Layer 1: dedupe — same content within window is genuinely
	// redundant; no breadcrumb stamped.
	seen, err := p.dedupe.SeenInWindow(ctx, req.TargetKey, req.ContentHash)
	if err != nil {
		return Outcome{Decision: DecisionProceed}, err
	}
	if seen {
		return Outcome{Decision: DecisionDeferred, Reason: "duplicate content"}, nil
	}

	// Layer 2: each attached Coalescer in order.
	for _, c := range p.coalescers {
		defer_, err := c.ShouldDefer(ctx, req.TargetKey)
		if err != nil {
			return Outcome{Decision: DecisionProceed}, err
		}
		if defer_ {
			recErr := p.store.RecordAsDeferred(ctx, req.TargetKey, req.MessageRef)
			return Outcome{Decision: DecisionDeferred, Reason: c.reason}, recErr
		}
	}

	return Outcome{Decision: DecisionProceed}, nil
}

// RecordAsSent commits a successful send: writes (TargetKey →
// ContentHash) to [sendstate.Store], appending a timestamp to the
// internal rolling list and incrementing TotalSent. Subsequent
// dedupe and Coalescer reads consult that record on the next Check.
func (p *Protector) RecordAsSent(ctx context.Context, req Request) error {
	return p.store.RecordAsSent(ctx, req.TargetKey, req.ContentHash)
}

// Metrics returns the per-key observability snapshot. ok=false
// when no entry exists for key.
func (p *Protector) Metrics(ctx context.Context, key string) (sendstate.Metrics, bool, error) {
	return p.store.Metrics(ctx, key)
}

// --- no-op fallbacks ---

type noopDeduper struct{}

func (noopDeduper) SeenInWindow(_ context.Context, _, _ string) (bool, error) {
	return false, nil
}
