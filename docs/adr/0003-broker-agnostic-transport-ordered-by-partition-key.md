# Broker-agnostic transport, ordered by Partition key

EventFabric must scale horizontally on more than one message broker: Kafka and RabbitMQ today, possibly others later. So ordering cannot rely on one broker's features. Every message carries a Partition key. EventFabric guarantees that messages with the same key are processed in order, by one consumer at a time, on every supported broker, including while instances join, leave or rebalance. Concurrency and ordering correctness is a property of EventFabric, not of whichever broker is configured. A broker adapter only has to provide what that guarantee needs.

## Consequences

- Broker-specific routing features (RabbitMQ topic exchanges as the primary routing mechanism, Kafka-only semantics) cannot leak into the interface Applications see.
- Rebalancing needs explicit ownership handover. A Partition's new owner must not start before the old owner has stopped.
- ADR-0007 limits this, in milestone 1, to Commands: Readmodel groups and Sagas each run on one Partition and read from Postgres.
- Commands get a narrower guarantee. The Stream's Revision is their fence, so one consumer at a time per Partition is best-effort, for fewer conflicts, not for correctness. Command Partitions have no Lease, and Commands are not deduplicated (a redelivered Command is re-validated against the current state). The guarantee above still holds in full for Consumer groups, which keep a Lease and an epoch-checked Checkpoint.
- [ADR-0015](0015-command-partitions-spread-by-consumer-priority.md) spreads Command Partitions across replicas on RabbitMQ through consumer priority on quorum queues. Lease-based assignment remains the broker-agnostic path.
- A creation Command with no Stream id yet is partitioned by its Command id; otherwise every creation of an Aggregate type would share one Partition.
