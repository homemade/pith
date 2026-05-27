# pith

Protect Integration THresholds with Go: dedupe + per-key cap policies (debounce, quota, custom).

## Packages

- [`pith/sendstate`](sendstate/) — shared per-key state split across two stores (mirroring two backend collections): the TTL'd **`Entry`** (content-hash, last-deferred message-ref, rolling send-timestamp list) read via `ReadEntry`, and the permanent **`Metrics`** rollup (lifetime counters + last-sent/deferred times) read via `ReadMetrics`. `Entry` carries the read primitives directly: `Seen(contentHash)` (content dedupe — is this identical to the last send?), `CountInWindow(now, window)` (per-window send count), and its deferral mirror `CountDeferredInWindow(now, window)` (deferral cadence, for replay-sweep eligibility — e.g. trailing-edge "gone quiet" == 0). The `Entry` store behaves like a Mongo TTL index (`expireAt` + `expireAfterSeconds: 0`) with TTL-honoring reads, so expiry never affects answers. A send touches only send-side state — a deferral is "pending" purely by being more recent than the last send (nothing is cleared)
- [`pith/coalesce`](coalesce/) — per-key cap policy ("at most hardCap successful sends per window"); one-method read-only policy (`ShouldDefer`), a pure function over a `sendstate.Entry` (via `Entry.CountInWindow`). Multiple Coalescers can be attached at different `(hardCap, window)` pairs
- [`pith/protect`](protect/) — composition layer; `Check` always applies content dedupe (`Entry.Seen`) then each attached Coalescer in order, returns `DecisionDeferred` on the first hit, and stamps a deferred-breadcrumb on sendstate for any Coalescer-driven defer. `RecordAsSent` updates the `Entry` + `Metrics` stores. On every proceed it raises each cap's high-water mark (post-send in-window count). `Metrics(ctx, key)` exposes per-key observability (TotalSent, TotalDeferred, LastSentAt, LastDeferredAt, and per-cap `PeakSendsInWindow`). `RangeDeferredWithCapsClear(ctx, limit, fn)` drives a consumer-side replay sweep over pending deferrals (oldest-first), invoking `fn` only for entries whose cap windows have elapsed (skipping ones that would just defer again), to be re-derived and re-emitted via `Check`

## Backends

Today the in-memory implementations from `sendstate` and `coalesce` are wired up
by default — process-local, for tests/examples/single-process use. A shared
backend is required for multi-instance deployments; see
[docs/mongo-store.md](docs/mongo-store.md) for a full Mongo `sendstate.Store`
implementation sketch (two TTL'd/permanent collections, `$max` peak merges,
TTL-honoring reads).

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
