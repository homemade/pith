# pith

Integration guards for Go: dedupe, debounce, quota cap, ...

## Packages

- [`pith/dedupe`](dedupe/) — short-window deduplication of repeated operations by caller-supplied string key

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
