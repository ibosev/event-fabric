# Command Partitions are spread across replicas by consumer priority on quorum queues

Every replica subscribes to all 128 Command queues of each Context it handles. Each queue is a RabbitMQ quorum queue with single active consumer. A replica subscribes with consumer priority `score(instance, context, p)`: the first 31 bits of SHA-256 over its random instance id, the Context and the Partition (rendezvous hashing). RabbitMQ makes the highest-priority live consumer active. So the 128 Partitions spread evenly over the replicas, a joining replica takes over only its own share, and a leaving replica's Partitions go to the next-highest priority without any Lease timeout. A higher-priority consumer takes a queue only after the current one has acknowledged its in-flight Command, so a join never runs one Stream on two replicas at once. Requires RabbitMQ 4.0 or newer.

Before this, Command queues were classic queues. Classic queues ignore consumer priority under single active consumer, so the replica that subscribed first was active on all 128 queues and the others only stood by.

## Considered Options

- **Lease-based Partition assignment in Postgres.** Replicas claim a fair share through Leases with epochs and subscribe only to those queues. Works on any broker and could spread Consumer groups too, but costs a rebalance loop and leaves no handler for up to one Lease duration after a crash. Kept as the path for a second broker.
- **Consistent-hash exchange, `x-modulus-hash` or super streams.** They change how keys reach lanes, not which replica owns a lane. Adding lanes moves keys and can reorder messages already queued. Super streams also lack per-message requeue and a reply path.
- **FNV-1a as the score.** Rejected after measuring it: it mixes similar keys too weakly, and three replicas got splits as bad as 89/0/39.

## Consequences

- A broker that already holds classic Command queues from an earlier build refuses the redeclare (`inequivalent arg 'x-queue-type'`). Delete those queues, or start from an empty broker.
- Quorum queues replicate and keep a delivery limit (20 by default in RabbitMQ 4.x); a Command redelivered more often is dropped by the broker. Handler failures are replies, not redeliveries, so only crashes count toward it.
- The instance id is random per process by default, so a restarted replica gets a new share. `createRabbitMQTransport({ instance })` pins it.
- On a fresh broker, a replica that subscribes while another one is still creating the same quorum queue can be refused (`noproc`). RabbitMQ closes the connection and the process exits; under a restart policy the next start finds the queue ready. The transport does not reconnect by itself.
- Cross-Aggregate order (an Invoice Command before its Customer exists) is unaffected.
