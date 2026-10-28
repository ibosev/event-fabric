# Saga Messages are System Events outside the Event log

EventFabric is event sourced and event driven at once. Domain Events are the facts Aggregates record in the Event log; System Events are delivered to handlers but never recorded there. A Saga Message is a System Event addressed to one Saga instance, delivered now or at a due time; a Deadline is a named Saga Message an instance sends itself. Neither has a Stream, Revision or Position, and Readmodels, rebuilds and Command waits never see them. This follows Axon's `DeadlineManager`, whose `DeadlineMessage` is an `EventMessage` that is explicitly not stored.

The owner of the instance's Partition (`fnv1a(id) % N`, ADR-0016) handles a Saga Message only when its Checkpoint has reached the log head as it stood when the message fell due (or, for an immediate message, when it was sent), so the handler sees every Domain Event recorded before. Handling commits the instance's state, its Outbox effects (ADR-0011), the message's consumption and its Deadline changes in one Lease-fenced transaction; the Checkpoint does not move.

## Considered Options

- **Stored wake-up Events on stateless Aggregates.** No new runtime concept, and existing Saga Reactions stay as they are, but every wake-up becomes a permanent Domain Event that exists only to reach one Saga instance, and Readmodels replay it.
- **Guarded Commands to the owning Stream** (the named Schedules of ADR-0011). Right for decisions an Aggregate owns, and kept for them; but existing reminders decide on Saga state, so each would need a new Aggregate and changes to domain code.
- **Saga Messages outside the log** (chosen). A new delivery path and its own storage, in exchange for wake-ups that reach exactly one instance against its state and leave no history.

## Consequences

- At most one Deadline per (Saga, instance, name) is pending; setting it again replaces it and destroying the instance cancels it, both in the handler's transaction. A replaced or cancelled Deadline that is already in flight is a no-op, by its generation.
- Saga Messages are never early. After downtime they catch up, and lateness is reported from the original due time. The goal is 100 ms from due time to handler start in healthy operation: the Partition owner keeps a timer for its earliest due time, other senders nudge it over the transport, and a slow poll covers lost nudges and Lease handovers.
- A lagging Partition delays its Saga Messages. That is the price of handling them against state that has seen every earlier Domain Event.
- A failing handler retries with Reaction backoff and dead-letters after 5 attempts, without halting the Saga's Event consumption.
- A Saga Message to a missing instance is dropped, unless its handler is declared to start one.
- Sagas, Workers and Tasks may send Saga Messages; a Gateway may only through an explicit Access-map entry.
- Sagas still cannot be rebuilt, so nothing ever replays Saga Messages, and a rebuild of Readmodels never sees them.
