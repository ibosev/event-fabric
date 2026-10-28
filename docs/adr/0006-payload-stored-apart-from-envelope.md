# Event payloads are stored apart from the envelope

Each Event is split across two tables: `events` holds the envelope and metadata, `event_payloads` holds the payload. This costs a join on every read. It is done from day one, even though erasure is not in milestone 1, so a later "erase subject" operation can overwrite payloads without touching Revisions, Positions or Checkpoints, and without breaking ADR-0005's immutable log for everything else. Splitting the table after production data exists would mean migrating the whole log.

## Consequences

- Metadata stays in `events` and is never erased, so Applications must not put personal data in metadata.
- Readers and the import API write and read both tables in one transaction.
