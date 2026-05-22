# pith

Protect Integration THresholds with Go: dedupe + per-key cap policies (debounce, quota, custom).

## Packages

- [`pith/sendstate`](sendstate/) — shared per-key record (content-hash, last sent, last deferred, last deferred message-ref) plus internal rolling send-timestamp list and lifetime counters; the read + write surface all mechanisms layer over
- [`pith/dedupe`](dedupe/) — short-window content-hash dedupe by caller-supplied string key; one-method read-only policy (`SeenInWindow`) over `sendstate.Store`
- [`pith/coalesce`](coalesce/) — per-key cap policy ("at most hardCap successful sends per window"); one-method read-only policy (`ShouldDefer`) over `sendstate.Store.CountInWindow`. Multiple Coalescers can be attached at different `(hardCap, window)` pairs
- [`pith/protect`](protect/) — composition layer; `Check` applies dedupe + each attached Coalescer in order, returns `DecisionDeferred` on the first hit, and stamps a deferred-breadcrumb on sendstate for any Coalescer-driven defer. `RecordAsSent` is a single sendstate write that updates the timestamp list + lifetime counters. `Metrics(ctx, key)` exposes per-key observability (TotalSent, TotalDeferred, LastSentAt, LastDeferredAt)

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
