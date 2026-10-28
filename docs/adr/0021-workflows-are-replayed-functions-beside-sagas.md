# Workflows are replayed functions beside Sagas

Some processes read better as one function that waits, than as Reactions over hand-kept state. EventFabric adds a **Workflow**: a consumer kind beside Readmodels, Sagas and Workers, whose instance is one `async` function run from start to end. Every durable call in the body (`waitFor` an Event or Signal, `sleep`, `ask` a Worker, `read` a Readmodel, `tell`, `queue`, `now`, `random`, `set`) is a named step recorded in a journal; after a wake or a restart the body runs again from the top and finished steps return their recorded result. The journal, the instance's state and result live in their own table, committed with the Outbox and the Checkpoint in one Lease-fenced transaction, exactly as Saga state is. Workflows are native: they reuse Partitions, Leases, the Outbox, Saga Message delivery and Deadlines, and need no second service. Sagas stay as they are for Applications that prefer Reactions; the two share Signals' delivery path and the Worker reply.

## Considered Options

- **A Temporal-backed bridge.** A mature engine for a fraction of the effort, but a second service to run, process state in two systems, and deduplication in both directions; EventFabric's promise is one Postgres.
- **Step history as Domain Events in the Event log**, as Axoniq Framework 5.4 does. Rejected for the reasons ADR-0020 rejected wake-up Events: the log is immutable, Readmodels replay it, and every Partition scans it.
- **Workflow-as-code compiled onto Sagas as a hidden third authoring form.** Rejected: a Workflow has its own lifecycle (status, result, restart), its own failure mode (drift) and its own determinism contract, which Saga Reactions deliberately do not have.
- **A replayed function with a journal of its own** (chosen).

## Consequences

- A step is identified by its name plus its occurrence count in the run, not its position, so independent branches may be reordered and loops need no numbering. Steps that send or read also record their arguments. A run that, once replay has finished, holds a new step while a finished step was never reached, or replays a step with different arguments, has **drifted**: the instance is parked, its wakes dead-lettered, and an operator resumes it after deploying matching code. The Partition is not halted.
- The body must be deterministic: it awaits only `ctx` calls, takes time and randomness from `ctx`, and does I/O only through `ask` and `read`. The `determinism` lint covers Workflow bodies; a `ctx` call after the run was judged suspended is a runtime error. Saga Reactions remain free of this rule because nothing replays them. No sandbox replaces `Date` or `Math.random`.
- An instance pins the definition version it started under; an Application registers old versions until their instances drain. `restart(input)` ends the run and begins a fresh journal under the same id, so long-lived loops keep the journal bounded.
- `ask` is at-least-once through the Outbox, as every Worker Message is. An at-most-once option for Workers without idempotency keys is a later opt-in.
- Queries read the committed state, status and result from the store; nothing runs the body to answer one. Updates, Signals that return a result to their sender, are declared in the glossary but not built.
- Names are EventFabric's own: `waitFor`, `sleep`, `ask`, `restart`, `set`, `startWorkflow`, `sendSignal`, `queryWorkflow`. Temporal's and Axon's names are references, not the interface.
