# pith

Protect Integration THresholds with Go: per-key cap policies (debounce, quota, custom), with content dedupe for write paths.

## Packages

- [`pith/sendstate`](sendstate/) — shared per-key state split across two stores (mirroring two backend collections): the TTL'd **`Entry`** (content-hash, last-deferred message-ref, rolling send-timestamp list) read via `ReadEntry`, and the permanent **`Metrics`** rollup (lifetime counters + last-sent/deferred times) read via `ReadMetrics`. `Entry` carries the read primitives directly: `Seen(contentHash)` (content dedupe — is this identical to the last send?), `CountSentInWindow(now, window)` (per-window send count), and its deferral mirror `CountDeferredInWindow(now, window)` (deferral cadence, for replay-sweep eligibility — e.g. trailing-edge "gone quiet" == 0). The `Entry` store behaves like a Mongo TTL index (`expireAt` + `expireAfterSeconds: 0`) with TTL-honoring reads, so expiry never affects answers. A send touches only send-side state — a deferral is "pending" purely by being more recent than the last send (nothing is cleared)
- [`pith/coalesce`](coalesce/) — per-key cap policy ("at most hardCap successful sends per window"); one-method read-only policy (`ShouldDefer`), a pure function over a `sendstate.Entry` (via `Entry.CountSentInWindow`). Multiple Coalescers can be attached at different `(hardCap, window)` pairs
- [`pith/protect`](protect/) — composition layer exposing **two capability-typed gates**, each a factory: a [`WriteProtector`](protect/) / [`ReadProtector`](protect/) only mints namespace-scoped handles via `Namespace(ns)` (`""` = the whole store), and the gating happens on the returned handle — mirroring a Mongo `Database`→`Collection`. A [`WriteNamespace`](protect/) guards a content-bearing operation (send / merge / PATCH): `Check(meta, contentHash)` applies content dedupe (`Entry.Seen`) then each attached Coalescer, returning `DecisionDeduped` on a content match or `DecisionDeferred` on a cap — a deferred write stamps a breadcrumb and is replayable via `ReplayCandidates`. A [`ReadNamespace`](protect/) guards a content-free operation (read / poll / event / trigger): `Check(meta)` applies coalesce caps only (no dedupe, no hash), returning `DecisionDeferred` on a cap — like a write, a capped read stamps a breadcrumb and is replayable via `ReplayCandidates`, except the replay re-fetches *current* state rather than re-emitting a payload (re-reading after the burst settles captures the final value). Both gates defer (not drop) a cap, because a cap can suppress the final, changed state; only dedupe is safe to drop without replay (the duplicate is already at the destination), which is why dedupe is write-only. Pair a read gate with `NewTrailingEdgeDebounce` so a sustained burst collapses to one final read. The replay sweep is **namespace-scoped** — `ReplayCandidates` applies its limit within the handle's namespace, so independently-replayed streams sharing a store are swept fairly and one namespace's backlog can't head-of-line-block another's; streams replayed by *different* consumers still need separate stores. An optional outer **tenant** scope chains above the namespace (`p.Tenant(t).Namespace(ns)`): it is a labelling field stamped on every `Entry` / `Metrics` write for observability and per-tenant queries, and does not affect sweep scoping or `TargetKey` isolation. Read-only access to per-key state is deliberately off the gate facades — observability and tests read `sendstate.Entry` / `sendstate.Metrics` via `sendstate.Store` directly. See the [godoc](https://pkg.go.dev/github.com/homemade/pith/protect) for the full mechanism set and the Check / RecordAsSent / replay contract.

## Backends

`sendstate.Store` ships with two implementations; one backs every gate.
The gate facades are constructed via the factory subpackages
([`protect/memory`](protect/memory/), [`protect/mongodb`](protect/mongodb/)) —
there is no public way to wrap a caller-supplied store. Each backend exposes
`NewReadProtector` and `NewWriteProtector`, both requiring at least one
Coalescer (a `(first, rest ...Coalescer)` signature).

### Memory — [`pith/sendstate/memory`](sendstate/memory/)

Process-local `sync.Map`-backed store for tests, examples, and single-process
use. Best-effort within one process — records written in one process are
invisible to others.

```go
import (
    "github.com/homemade/pith/coalesce"
    memprotect "github.com/homemade/pith/protect/memory"
)

p := memprotect.NewWriteProtector(entryTTL,
    coalesce.NewQuota(50, 24*time.Hour),
    coalesce.NewLeadingEdgeDebounce(10*time.Second),
)
```

The constructors auto-size the memory store's `MaxSendTimes` (the bound on the
rolling send-timestamp list) to the largest attached Coalescer cap, so callers
don't normally set it.

### Mongo — [`pith/sendstate/mongodb`](sendstate/mongodb/)

Shared-backing Mongo store for multi-instance / cross-container deployments.
Two collections: `entries` (TTL'd working state — one document per key,
deleted by a TTL index on `expireAt`) and `metrics` (permanent lifetime
rollup — never expires). Reads honor the TTL via an `expireAt > now`
predicate, so answers don't depend on when Mongo's background TTL deleter
runs.

The `protect/mongodb` constructors dial with majority write concern (required
for cross-instance correctness), apply a per-op `Timeout`, build the store, run
`EnsureIndexes`, and derive the store's `MaxSendTimes` from the attached
Coalescers (so the storage-side bound can't be forgotten):

```go
import (
    "github.com/homemade/pith/coalesce"
    pmongo "github.com/homemade/pith/protect/mongodb"
)

p, client, err := pmongo.NewWriteProtector(ctx, pmongo.Config{
    URI:      "mongodb+srv://user:pw@cluster.example.com",
    Database: "pith",
    EntryTTL: 48 * time.Hour,
    Timeout:  200 * time.Millisecond, // per-op; Check fails open on overshoot
    // MaxSendTimes omitted — derived from the largest attached Coalescer cap.
}, coalesce.NewQuota(50, 24*time.Hour))
if err != nil {
    return fmt.Errorf("pmongo.NewWriteProtector: %w", err)
}
defer client.Disconnect(ctx)
```

Set `Config.MaxSendTimes` explicitly only to request *more* headroom than the
derived value; a value below it is rejected at construction (it would drop
in-window timestamps via `$slice` and leak the cap). The lower-level
`sendstate/mongodb.Open` / `New` remain available for direct store access.

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

While pith has a single in-lockstep consumer it ships breaking changes as
**minor** bumps (rather than a MAJOR), to avoid the Go `/v2` module-path
suffix and a matching import rewrite for no real benefit at one internal
client; the consumer is updated in the same release. Revisit if pith gains
external consumers.

Consumers pin via Go modules:

```
require github.com/homemade/pith v1.6.0
```
