# The transport carries Commands, notifications and Messages, never Events

EventFabric's transport has three lanes:

- a Command lane, request and reply, where the sender awaits accepted, rejected, failed or unknown;
- a broadcast lane, delivered at most once, for live notifications and broadcast Messages;
- a queue lane, delivered at least once, for queued Messages.

Events never travel on it as the source of truth. Consumer groups read them from Postgres (ADR-0007), and only a relay copies them onto the broadcast lane for live feeds. The design follows Axon Framework's awaitable Commands and subscription queries, and the first adapters are in-memory and RabbitMQ.

## Considered Options

- **No broker in milestone 1.** Gateways would run Commands in the request, and a Postgres queue would hold the durable ones. Rejected: the maintainer wants Commands, live notifications and platform Messages on a broker, as in Axon and as Applications on the old stack already run.
- **Gateways tailing Postgres for live feeds.** Rejected for a broker pub/sub channel, which also carries Readmodel-change notifications.
- **Command-id dedupe in a `commands` table.** Rejected: the table grows with every Command, and re-validating a redelivered Command against the current state is the standard pattern. Commands whose duplicates matter carry a business id that a Precondition checks.

## Consequences

- A Command can run twice on redelivery. An Application must guard Commands that append, such as payments or tags, with a business id.
- Notifications and broadcast Messages can be lost. Subscribers re-read state when they reconnect.
- The sender gets "unknown" when its deadline passes. To learn the outcome, it looks for its Command id in the Stream's Events, or resends.
