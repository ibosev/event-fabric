# Readmodel stores commit documents and Checkpoint atomically, and lay out relationally from a declared schema

Readmodels sit behind one Readmodel store interface, so an Application can choose Mongo, Postgres or another database per Readmodel group without changing its Readmodels. A store qualifies only if it can commit a batch of document writes together with the group's Checkpoint, checked against the Lease's epoch, in one transaction. This keeps read-your-writes exact (a group Checkpoint ≥ P means its documents reflect every Event up to P), and Readmodel documents carry no per-document Position (the old stack's `_oblak`). Each Readmodel declares a store-neutral schema. The Postgres store turns it into typed columns, prefixed columns for nested objects, array columns and child tables, instead of a single JSONB document per row.

## Considered Options

- **Checkpoint always in Postgres, with a per-document Position or epoch in other stores.** Rejected: every store would need idempotent replays and a document field, and a Checkpoint in another database cannot say exactly what a Readmodel shows.
- **One JSONB document per row in Postgres.** Rejected by the maintainer: Readmodels already read and write individual fields through `vm.get`/`vm.set`, and Postgres tables should follow relational conventions. JSONB remains an opt-in for fields marked `json`.
- **Infer the layout from `initialState`.** Rejected: `null` and `[]` carry no type.

## Consequences

- A Mongo store requires a replica set, because it uses multi-document transactions.
- Every migrated Readmodel needs a schema. A codemod drafts them and a person reviews them. Both stores validate writes against it.
- A schema change runs a hand-written migration for that exact step, or else rebuilds the group into a new generation (shadow and swap).
