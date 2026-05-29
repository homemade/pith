# pith

Protect Integration THresholds with Go: dedupe + per-key cap policies (debounce, quota, custom).

## Packages

- [`pith/sendstate`](sendstate/) — shared per-key state split across two stores (mirroring two backend collections): the TTL'd **`Entry`** (content-hash, last-deferred message-ref, rolling send-timestamp list) read via `ReadEntry`, and the permanent **`Metrics`** rollup (lifetime counters + last-sent/deferred times) read via `ReadMetrics`. `Entry` carries the read primitives directly: `Seen(contentHash)` (content dedupe — is this identical to the last send?), `CountSentInWindow(now, window)` (per-window send count), and its deferral mirror `CountDeferredInWindow(now, window)` (deferral cadence, for replay-sweep eligibility — e.g. trailing-edge "gone quiet" == 0). The `Entry` store behaves like a Mongo TTL index (`expireAt` + `expireAfterSeconds: 0`) with TTL-honoring reads, so expiry never affects answers. A send touches only send-side state — a deferral is "pending" purely by being more recent than the last send (nothing is cleared)
- [`pith/coalesce`](coalesce/) — per-key cap policy ("at most hardCap successful sends per window"); one-method read-only policy (`ShouldDefer`), a pure function over a `sendstate.Entry` (via `Entry.CountSentInWindow`). Multiple Coalescers can be attached at different `(hardCap, window)` pairs
- [`pith/protect`](protect/) — composition layer; `Check` always applies content dedupe (`Entry.Seen`) then each attached Coalescer in order, returns `DecisionDeduped` on a content match or `DecisionDeferred` on the first Coalescer hit, and stamps a deferred-breadcrumb on sendstate for any Coalescer-driven defer. `RecordAsSent` updates the `Entry` + `Metrics` stores. Read-only access to per-key state is deliberately off the Protector facade — observability dashboards and tests read `sendstate.Entry` / `sendstate.Metrics` via `sendstate.Store` directly. `ReplayCandidates(ctx, limit)` drives the consumer-side replay sweep: it walks the oldest pending deferrals and returns `[]DeferredRequest` (target key + breadcrumb + deferral timestamp) for entries whose cap windows have elapsed, skipping ones that would just defer again. The consumer re-derives the upstream state from each breadcrumb and re-emits via `Check` — dedupe and caps are re-applied, so a content-identical revert (rare) can still defer.

## Backends

`sendstate.Store` ships with two implementations; one is required as the
first argument to [`protect.New`](protect/) (the type system enforces it;
`New` also panics on an explicitly nil store).

### Memory — [`pith/sendstate/memory`](sendstate/memory/)

Process-local `sync.Map`-backed store for tests, examples, and single-process
use. Best-effort within one process — records written in one process are
invisible to others.

```go
import (
    "github.com/homemade/pith/protect"
    "github.com/homemade/pith/sendstate/memory"
)

p := protect.New(memory.New(entryTTL),
    protect.WithCoalescer(/* … */),
)
```

`protect.New` auto-sizes the memory store's `MaxSendTimes` (the bound on the
rolling send-timestamp list) to the largest attached Coalescer cap, so callers
don't normally set it.

### Mongo — [`pith/sendstate/mongodb`](sendstate/mongodb/)

Shared-backing Mongo store for multi-instance / cross-container deployments.
Two collections: `entries` (TTL'd working state — one document per key,
deleted by a TTL index on `expireAt`) and `metrics` (permanent lifetime
rollup — never expires). Reads honor the TTL via an `expireAt > now`
predicate, so answers don't depend on when Mongo's background TTL deleter
runs.

`Open` is the convenience constructor — it dials with majority write concern
(required for cross-instance correctness), applies a per-op `Timeout`, builds
the Store, and runs `EnsureIndexes` to create the `expireAt` TTL index plus
the `lastDeferredAt` index that serves `RangeDeferred`'s sort:

```go
import (
    "github.com/homemade/pith/protect"
    "github.com/homemade/pith/sendstate/mongodb"
)

store, client, err := mongodb.Open(ctx, mongodb.Config{
    URI:          "mongodb+srv://user:pw@cluster.example.com",
    Database:     "pith",
    EntryTTL:     48 * time.Hour,
    MaxSendTimes: 50,                       // largest attached Coalescer hardCap
    Timeout:      200 * time.Millisecond,   // per-op; pith.Check fails open on overshoot
})
if err != nil {
    return fmt.Errorf("mongodb.Open: %w", err)
}
defer client.Disconnect(ctx)

p := protect.New(store,
    protect.WithCoalescer(/* … */),
)
```

Unlike the memory backend, `protect.New` does **not** auto-size the Mongo
store's send-timestamp bound — pass `Config.MaxSendTimes` (or
`mongodb.WithMaxSendTimes(n)` if using `New` directly) as the largest attached
Coalescer hardCap, or in-window timestamps will be dropped by `$slice` and
caps will leak.

For tests or custom client configuration, use `mongodb.New(db, entryTTL, opts...)`
with a `*mongo.Database` you've built yourself; that's the lower-level
constructor `Open` wraps.

### Backend-error behaviour

Backing-store errors from `ReadEntry` (the single read every `Check` makes)
are **fail-open**: `Check` returns `Outcome{Decision: DecisionProceed, Err: err}`
so callers over-send rather than dropping work — the error is surfaced via
`Outcome.Err` for logging. Combined with the Mongo store's `Timeout`, a slow
or unreachable backend degrades to over-send and bounded latency, never to
dropped sends.

## Documentation

Documentation effort is focused on **godoc**. The canonical, rendered reference — including runnable `Example_*` functions — is at [pkg.go.dev/github.com/homemade/pith](https://pkg.go.dev/github.com/homemade/pith), or locally via `go doc github.com/homemade/pith/<package>`. This README covers repo-level concerns only (packages, versioning); for package APIs, types, and usage patterns, look there first.

## Versioning

This repo follows [Semantic Versioning 2.0.0](https://semver.org/). Git tags use the form `vMAJOR.MINOR.PATCH`:

- **MAJOR** — incompatible API changes
- **MINOR** — backwards-compatible additions
- **PATCH** — backwards-compatible fixes

While the version is below `v1.0.0` the API is **not yet stable** — minor-version bumps may include breaking changes (per Go module convention for pre-1.0 modules). `v1.0.0` will signal a commitment to API stability.

Consumers pin via Go modules:

```
require github.com/homemade/pith v0.1.0
```
