# Readmodel groups and Sagas are partitioned by document and Saga instance

A Readmodel group or Saga may declare `partitions: N`. Every Partition advances through the whole Event log in Position order, as the single Partition of ADR-0007 does, and resolves Identities the same way, query Identities included. It then applies only the changes to documents it owns, `fnv1a("model/id") % N`, or to Saga instances it owns, `fnv1a(id) % N`. Every document and Saga instance has exactly one owner, and that owner sees every Event that touches it in global Position order. So no Event is missed or reordered for any document or Saga instance, which is the guarantee one Partition gives. Each Partition is its own Lease, epoch, Checkpoint and halt, and replicas share the Leases out.

Advancing through the whole history does not require materializing every payload: Readmodel Partitions select the union of qualified Event names in their Reactions and commit safe progress through excluded Events. Every Partition still sees every relevant Event before filtering by document ownership. The Event store proves a complete visible prefix, and a partial fold cannot advance past unfinished relevant Events. Sagas retain their existing reader.

ADR-0007 rejected partitioning by Identity because "a query Identity cannot pick a Partition before the Reaction runs". Here nothing picks a Partition before: every Partition resolves the Identity, then keeps what it owns. Partitioning by Stream stays rejected, because documents and Saga instances combine several Streams.

## Rules that follow

- **No order across documents.** Two Partitions can be at different Positions at the same moment, so a Reaction of a partitioned group may not read the group's own documents. It is refused at runtime. Cross-group reads wait for the lowest Checkpoint of the other group's Partitions, as before.
- **Read-your-writes** (`minPosition`, `respondWith`) waits until every Partition has passed the Position.
- **Changing N is a drain, then a split.** A `layout:<group>` row records N (1 when absent). Partition 0's holder takes every old and new Partition's Lease. It then runs each old Partition up to P, the newest Event visible below the snapshot's `xmin`, so nothing can commit at or before P any more. Finally it starts every new Partition at P and records N. Each key's old owner handled every Event up to P and its new owner handles every Event after P. Partitions 1..N-1 wait until the layout says N. A group that never ran starts every Partition at the beginning.
- **A Saga's Commands to one Stream leave the Outbox in the order they were recorded**, across transactions: a row waits while an earlier row for the same target Stream is due or retrying. Delayed rows not yet due don't hold later rows back. Without this, Saga Partitions committing in separate transactions let a credit overtake its charge (a 60-account test run refused 11 Commands).

## Considered Options

- **A Partition key declared per group** (`partitionBy(event)`). It needs a correct key per group, and a wrong one silently races two Partitions on one document. Ownership by document needs no declaration and cannot race.
- **Each Partition reading only its own Events.** That needs the key before the Identity is known (ADR-0007's objection). Reading the whole log costs N reads per Event, which measured small next to the Reactions and the commits.
- **Rebuild on any change of N.** That would work for Readmodels but not for Sagas, which cannot be rebuilt.

## Consequences

- Each Partition keeps its Checkpoint in its own record and commits under its own lock. The group's lock, which took 50% of Postgres statement time with 4 Partitions, is now taken exclusively only by whole-group writes: layout resets, rebuilds, swaps and fences of Partition 0.
- A layout switch fences every Partition of both layouts before it drains. Its reset checks, in one store transaction, that each Partition is still fenced by the coordinator, and the layout is published by compare-and-set against the recorded count and every coordinator Lease. A switch that fails at any step leaves the old layout running, and the next holder of Partition 0 starts it again. Whole-group transitions write every Partition record they invalidate, so Mongo conflicts with batches in flight, and live batches commit only against the generation they were folded against.
- Live subscriptions keep one cursor per document, because Partitions commit different documents at equal or decreasing Positions. A Saga's Idempotency keys include the instance id, so they don't depend on the layout. Groups with one Partition are not affected.
- Code from before this ADR ignores the layout, so change N only after every replica runs this version.
- Partition records are named `<group>#<partition>`, so group and Saga names cannot contain `#`. Registration moves Partition metadata of the earlier format into these records.
- Outbox migration 3 adds a stored `target` column and index.
