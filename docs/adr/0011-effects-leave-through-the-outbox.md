# Effects leave only through a transactional Outbox; external calls run in Workers, never in Reactions

Every Command or queued Message that a Command handler (`tell()`) or a Saga Reaction sends is written to an Outbox table in the same Postgres transaction as the Events, or as the Saga state and its epoch-checked Checkpoint. EventFabric sends Outbox rows after the commit and a leased dispatcher retries what is left, so delivery is at least once and nothing is sent for a transaction that rolled back. Saga state therefore lives in Postgres. Reactions do not call external systems. They record a queued Message and a Worker performs the call, passing an Idempotency key derived from the Consumer group and the Event id. A Lease epoch can fence only a store that checks it, and an external system cannot, so this is the honest guarantee.

## Considered Options

- **Send after commit, no Outbox.** Rejected: a crash between commit and send loses Commands silently (the old stack's Saga `_commands` are never re-sent, and `destroy()` deletes them).
- **External calls inside Reactions, with an Idempotency key.** Rejected: a Saga runs on one Partition (ADR-0007), so a slow provider stalls every later Event, and the call happens before the commit. In the old stack it caused duplicate emails and webhooks.
- **Saga state in a Readmodel-style store of choice.** Rejected: the Outbox would have to live in each store, with one dispatcher per store.
- **A table of effects already performed.** Rejected: a crash between the call and the row write still repeats the effect.

## Consequences

- Sagas that call external systems move those calls into Workers, and the Worker runtime enters milestone 1.
- Delayed work is an Outbox row with `notBefore` and a replace key; BullMQ and Redis are no longer needed for it.
- Every external effect may repeat. Providers without idempotency support can send a duplicate after a crash.

## Named Schedules

A named Schedule retains the Outbox delivery path and adds a current attempt owned by the target Stream. The Stream's append, Schedule changes and dispatch admission share a lock, including when a Command records no Events. An attempt guard at acceptance prevents an already travelling or running obsolete Command from recording effects; deleting an Outbox row alone cannot provide that guarantee. Schedule changes occur in Command handling, so replaying Events does not recreate them.

Hard to reverse:

- Outbox migration 5 adds the Schedule table and durable cancellation results, and a scheduled Command carries a `schedule` envelope field (name and original due time). Only `app.schedule` sets it: `createCommand`, and so every public sender, refuses it.
- Replicas from before this version ignore the field and the attempt guard, so every Command and Outbox replica upgrades together. Rolling back once Schedules exist would let an obsolete attempt run as an ordinary Command.
