# Event consumers run on one Partition, in Position order, read from Postgres

Every Readmodel group and every Saga processes Events on a single Partition in global Position order, reading the Event log directly from Postgres. ADR-0003's per-key Partitioning across brokers applies, in milestone 1, to Commands only. Partitioning Event consumers by Stream was rejected because Readmodels and Sagas routinely combine Events from several Streams into one document or Saga instance, and would race or silently lose updates; partitioning by Identity was rejected because a query Identity cannot pick a Partition before the Reaction runs. We trade horizontal scaling within one Consumer group for correctness; groups still run in parallel to each other.

## Consequences

- A broker gains Event consumers nothing at one Partition (Kafka keeps order only within a topic partition), so Events are not published to a broker in milestone 1. Exporting Events to external systems would be a separate feature.
- Scaling one hot Consumer group later means an opt-in Partitioning by Stream or Saga instance id, with rules about which Reactions qualify.
- Superseded in part by ADR-0016: a group may run as several Partitions, owned by document or Saga instance, each still in global Position order.
