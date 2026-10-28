# Position is the writing transaction's id plus a sequence

An Event's Position is the pair (the id of the Postgres transaction that wrote it, a sequence number), and readers only read Events below the oldest still-running transaction. A plain sequence was rejected because sequence values are handed out at insert, not at commit, so a reader can permanently skip an Event that commits late. That is the bug in the old stack's counter, which also ran outside the commit. Gap detection (Marten) and one writer at a time (Message DB) were rejected as fragile and as a global bottleneck respectively.

## Consequences

- A long-running or idle transaction anywhere on the database cluster pauses every reader until it ends. EventFabric's database roles set `idle_in_transaction_session_timeout`, reader lag is exposed as a metric, and heavy reporting jobs belong on another cluster.
- Requires Postgres 13 or newer.
- Positions are ordered by transaction start, not commit time, and are not contiguous.

## Stream order without a Stream lock

Within one Stream, Positions ascend with Revisions, so a consumer meets a Stream's Events in Revision order. An append without Schedule changes guarantees this without a Stream lock. One statement checks the expected Revision, records the Command id and inserts the Events, and it is the transaction's first write. Its transaction id is therefore assigned after its snapshot saw the Stream's previous Revision commit. A concurrent writer that the snapshot missed fails on the unique `(Stream, Revision)` index and gets a Revision conflict. Any new write in an append must come after that statement. An append that changes a Schedule still takes the Stream lock before its first write, because a scheduled Command's Admission takes the same lock.
