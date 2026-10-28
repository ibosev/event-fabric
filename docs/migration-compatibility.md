# Compatibility rehearsal with synthetic examples

These examples use invented billing data. They demonstrate EventFabric's contracts and the evidence a migration needs. They contain no source Application code, database contents, customer names or deployment counts.

Run the [executable rehearsal](../examples/compatibility/rehearsal.mjs) from the repository root with Node >=24.15:

```sh
npm ci
npm run build
node examples/compatibility/rehearsal.mjs
```

It uses the real kernel, Event import, Readmodel rebuild and repository interfaces with the supplied in-memory stores. It creates no external effects or persistent database resources. The synthetic amounts are whole numbers chosen for clear expectations; this is not a currency conversion or monetary rounding implementation.

## What the example checks

Two Invoice Streams contain three Events: issue Invoice A for EUR 100 / USD 108, record a payment of EUR 25 / USD 27, and issue Invoice B for EUR 40 / USD 44. The hand-worked final balances are EUR 75 / USD 81 and EUR 40 / USD 44. Expected states and documents are literals independent of the handlers under test.

The example asserts:

- Mapping preserves source input, Event ids and ordering, and translates explicitly zero-based Revisions to 1-based Revisions. Every mapper review must be resolved.
- Import/read preserves every envelope and payload field under the declared JSON contract. Source Positions are replaced; an undefined optional property is absent on the JSON wire.
- Both Streams fold to expected State, all three accepted Commands reproduce their expected Event name/payload/metadata, and `api.invoices` rebuilds every expected document.
- A payment above the remaining balance is rejected; an accepted zero-payment Command records no Events; resending a newly accepted Command returns the same Events with `duplicate: true`.
- Queries by balance, child payment id and tenant return expected results. A repository tenant filter demonstrates query semantics; Gateway authorization is a separate contract test.
- The rebuilt group's Checkpoint reaches the imported head and the Outbox stays empty.
- An invented saved document with `status: "open"` differs from the expected replay's `status: "partial"`. The fixture passes because it detects that discrepancy; saved-document parity remains false for that deliberately stale input.

The output reports `syntheticFixturePassed: true` and **`applicationReady: false`**, with explicit omitted areas. It also reports `partialAuditPassed: true`: a second audit intentionally checks one Stream, skips every Command and supplies no Readmodel expectation. That is an executable demonstration of why the existing audit boolean is insufficient for readiness.

## Classify behavior before translating it

Record each real call site in an inventory with its source locator, classification, target behavior, example/test id and any unresolved decision. These rows are generic patterns, not claims about which patterns an Application uses.

| Classification | Synthetic example | Evidence required |
| --- | --- | --- |
| Unchanged | `invoiceIssued` still means the same business fact | Same qualified Event name, payload, ordering and State after import; declare retired Event names too |
| Mechanical | Replace a known Event-name expression with a qualified string | Exact reviewed symbol mapping, compiled declarations and all consumers updated |
| Mechanical | Task service access becomes `ctx.services.mailer` | Run the migrated Task through the actual CLI context and verify cleanup |
| Mechanical | An old enricher becomes `legacyEnricher(…)` | Payload and metadata of every applied Event match, including enrichers reading the State or changing the payload; later Steps and Rules see the enriched Event |
| Mechanical | `aggregate.apply.issued(payload, meta)` becomes `…(payload, legacyMetadata(meta, command.metadata))` | Nested and colliding metadata keys keep the Command's values |
| Mechanical | An old `query` map entry moves into `identity` through `legacyQueryIdentity` | The Reaction updates the same documents; absent documents are not created |
| Mechanical | A Saga Event without an Identity gets `(event) => event.id` | One instance per such Event, as before; continuation Events still reach their instance |
| Mechanical | A QueryBuilder chain becomes `find(...legacyQuery({ … }))` | Same documents, order, count and default size of 100; result consumers unchanged |
| Manual | A property-path Identity becomes `event => event.payload.invoiceId` | Check target ids, absent/existing documents, predicates and every handler |
| Mechanical | Grouped Reaction keys with their own Identities or markers move into `reactionSets` | Each set updates the documents its Identity selects and keeps its markers; check documents two sets select, which now fold in declaration order |
| Manual | A mutable customer lookup moves out of a Reaction | Record necessary facts in Events; give Reactions only deterministic helpers |
| Mechanical | `readmodels.list(m => m.g.m, { query, transforms })` becomes `legacyList('g', 'm', { query, transforms })` | `{ total, data }`, totals beyond the default page and the tenant filter on both; clients of error bodies, negative paging and text search |
| Mechanical | An old REST descriptor with `id` and `payload` callbacks becomes `command(req => CommandInput, { respondWith })` | Check the outcome body and status each client reads, the returned document Identity and tenant scope |
| Intentionally removed | Aggregate `disablePersistence` | Declaration must fail; record the caller's replacement workflow |
| Intentionally removed | WebSocket client protocol or targeted document rebuild | Document HTTP/SSE reconnect/refetch or whole-group rebuild and test callers |
| Unused | A particular native Mongo aggregation API | Mark unused only after an Application inventory establishes no reachable call sites; absence from this example is insufficient |

An available equivalent does not prove source compatibility. An empty `migrateSource().reviews` list means no known unsupported pattern was found. Execute the transformed code and inspect its results.

## Readmodel schema and parity

A schema with `balance: { EUR: number, USD: number }` gives Postgres typed `balance_EUR` and `balance_USD` columns. An array of payment objects uses a child table; scalar arrays use Postgres arrays. Dynamic dictionaries can opt into `"x-store": "json"`. These are schema decisions, not columns created whenever a new value arrives. The repository reconstructs the nested document when reading. [ADR-0009](adr/0009-readmodel-stores-atomic-checkpoint-relational-layout.md) specifies the layout.

Review schemas beyond the initial state: historical fields, nullable values, empty arrays, date values, mixed representations, declared currency keys and indexes. Use `DateTime()` for a date-time field that reads back as `Date`. Unknown historical fields need an explicit schema or a reported refusal; they must not disappear merely to make parity pass.

Keep three distinct comparisons:

1. **Import fidelity:** every source Event versus the imported log, with reviewed envelope/BSON mappings, exact identity and per-Stream Revision/order checks.
2. **Replay parity:** the new Readmodel versus an independent baseline. For this example that baseline is hand-worked; a migration normally needs an independently executed old projection or an independently verified fixture.
3. **Saved-document parity and correctness:** the new projection versus the saved source documents, plus business consistency checks. Matching an old replay does not establish that either replay is financially correct or that the saved projection agrees.

Document every normalization: optional absence to NULL, Date representation, replacement of old processing guards, or an intentional deterministic clock policy. Do not remove a business field from comparison because it differs. Count missing, unexpected and changed document ids separately; retain unknown saved fields and all differences in a private audit. Report justified exceptions separately from exact equality.

For Postgres evidence, additionally inspect the active generation's actual tables, column types, child counts/order, primary/foreign keys and required indexes. Repeat queries and parity checks after reconnecting and restarting the disposable database. The in-memory example does not claim those checks ran.

## Coverage manifest and readiness

Create the expected manifest independently of the selected test cases. Compare exact id sets, including duplicates and unexpected ids, rather than counts alone. The executable example has an explicit manifest for its two Streams, three Events, three accepted Commands, one Readmodel and five additional cases.

A complete Application manifest needs these dimensions:

| Dimension | Expected inventory | Evidence to attach |
| --- | --- | --- |
| Source | Source revision/file hashes, configuration/deployment fingerprint, store heads and ordered log fingerprint | Capture provenance and verify the source remained stable |
| Events/Streams | Every Event id and qualified name, every full Stream triple and Revision range | Full-content import verification and independent final State expectations |
| Commands | Every Command contract plus accepted, rejected, zero-Event, duplicate/conflict and unknown-outcome cases | Exact before-state, expected outcome, Events, metadata and effect intent; per-case skip reason |
| Readmodels | Every group/model, expected document ids including empty models, required queries/indexes | Full document comparison, schema review and complete Checkpoints |
| Sagas | Every group including empty groups, resolved instance ids and pending work | Import at the known head, then next-Event State and outgoing-effect comparison |
| Workers | Every Message contract, provider intent and Idempotency key rule | Captured requests, retries, permanent failure, crash/restart and cancellation behavior |
| Gateways/clients | Every route/client contract and access rule | HTTP outcomes, response shape, headers, scope, read-your-writes and SSE reconnect/refetch |
| Composition | Every declaration, service/factory, dependency edge, Task and lifecycle owner | Init order, failure cleanup, request-state isolation and package/entrypoint checks |
| Operations | Selected adapters, workload and required operator records | Restart/recovery, measured latency/rebuild time, logging/alerts and rollback assumptions |

For every omitted case record `id`, `reason`, `owner` and disposition (`pending` or explicitly `not-applicable` with evidence). An explanation of a difference is not proof that the difference is acceptable. Carry a decision reference and preserve the original expected/actual values. No overall success while an applicable case is pending or a difference is unapproved.

Use separate report fields:

```json
{
  "provenance": "synthetic",
  "comparisonsPassed": true,
  "fixtureCoverageComplete": true,
  "savedDocumentParity": false,
  "applicationCoverageComplete": false,
  "applicationReady": false,
  "missingEvidence": ["Saga continuation", "HTTP/SSE contracts", "storage restart"]
}
```

This is a suggested audit-report contract, not a new EventFabric API. The current `auditReplay()` sets `passed` when at least one case was checked and every recorded difference has an explanation. It permits skipped Commands, omitted Readmodels and explained differences. It compares Command Event names/payloads/metadata; it does not provide complete rejection, duplicate-delivery or effect-intent assertions. The fixture CLI reads only Events, States and Commands. Retain and resolve mapper reviews before calling it, and provide explicit before-state fixtures for rejected/zero-Event Commands rather than inferring their history from an empty Event-id list.

## Synthetic cases to extend the rehearsal

These are follow-on cases, not checks claimed by the executable example. Use the public interfaces and real disposable adapters where durability is part of the claim; replace only external providers with recording fakes.

| Area | Invented scenario | Required observation |
| --- | --- | --- |
| Saga continuation | Import `saga:reminders` for `invoice-a` with EUR 75 outstanding at head H; append a next payment Event | Expected State and exactly the intended Command/Message; no history effects, and every empty group also initialized at H |
| Pending work | A reminder has a due time and replace key; another timer was retired | Every row mapped or explicitly retired; no timeout kept as inert JSON |
| Worker intent | A receipt provider accepts a request and its acknowledgement is lost | Retry uses the same recorded Idempotency key and request; record failure/retry intent without contacting a real provider |
| Gateway | Two tenants request the same Invoice id, then retry a payment | Scope on one/list/count/stream/Notifications; accepted/rejected/failed/unknown/behind/duplicate behavior and headers |
| Client | An SSE connection drops after a committed payment | Reconnect/refetch converges on the current document; heartbeat and shutdown cleanup work |
| Services | `pool → settingsRepository → pricingService`, plus a pure money formatter | Inventory factories and edges, initialize before use, dispose in reverse ownership order on success/failure; pass tenant context as an argument |
| CRUD/send access | A Task reads settings; a service records a Command through an injected sender | Translate callable/argument-taking CRUD forms deliberately; list every send closure and make its readiness/lifetime explicit |
| Readmodel services | A Reaction formerly read mutable settings | Persist the decision inputs in the Event and configure a narrow `readmodelServices`; prove changes to external settings cannot change replay |
| Logging | A Consumer halts, a Lease changes owner, the Outbox retries and a pool saturates | Capture level/fields, match operator queries/alerts, and avoid payload/credential logging |

EventFabric's current logging contract includes `component`, debug timing/lag/pool summaries and Lease records. The [README Operations section](../README.md#operations) describes it. Which records an operator actually relies on requires operator evidence; a synthetic logger assertion cannot establish that dependency. Service request/tenant state, dependency counts and unused APIs likewise need an Application inventory before they can be claimed complete.

The final rehearsal checklist is: freeze/capture the expected manifest; resolve translations and deliberate changes; import into fresh storage; verify every Event and State; compare accepted and non-accepted Command/effect cases; rebuild and compare every model; continue imported Sagas without replay; exercise Workers and client contracts; verify restart, latency, logging and rollback assumptions; then evaluate all readiness fields. This guide establishes the reusable checks and examples. It does not authorize a live migration.
