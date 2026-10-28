# EventFabric is published as ESM only, needs Node 24.15 or later, and CommonJS consumers load it with `require(esm)`

EventFabric ships one ESM build (`"type": "module"`). Existing Applications on the old stack are CommonJS and load it with plain `require()`. This relies on `require(esm)`, which became stable in Node 24.15, so `engines.node` is `>=24.15`. To keep `require()` working, the module graph has no top-level `await` and no default exports. A CI smoke test `require()`s every `exports` entry from a CommonJS fixture, on Node 24.15 and 26.

## Considered Options

- **Dual ESM + CommonJS output.** Rejected: it brings the dual package hazard (two copies of module-level registries and error classes, so `instanceof` checks fail), and it doubles the build and test surface. It would only buy support for Node 18, which is past end of life.
- **CommonJS only.** Rejected: it ties the framework to the older module format. It remains the fallback (same `tsc`, `"type": "commonjs"`, no code changes).
- **A lower floor (`>=22.12`).** Rejected: `require(esm)` is still experimental there, and Node 22 reaches end of life in April 2027.

## Consequences

- A CommonJS Application must run Node ≥ 24.15 everywhere, including local development.
- A dependency that adds top-level `await` breaks every CommonJS consumer; the smoke test catches it.
