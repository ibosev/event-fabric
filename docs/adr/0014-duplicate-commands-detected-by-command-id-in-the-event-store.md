# Duplicate Commands are detected by Command id in the Event store, kept forever

A Command id is accepted at most once. When the Event store appends a Command's Events and Outbox rows, it also records the Command id with a fingerprint (name, Stream, payload), in the same transaction and under a unique constraint. A Duplicate Command, meaning one whose id is already recorded, is not run again: it returns the first accepted outcome. If the fingerprint differs, the Command is rejected with `conflict`. Rejected and failed Commands leave no record. Records never expire. HTTP clients get de-duplication through `Idempotency-Key`: the Gateway turns the key, scoped by the Application, into the Command id. One rule covers both client retries and the Outbox re-sending a Command after an `unknown` outcome, because a re-sent Command keeps its id.

## Considered Options

- **A separate key store with a TTL, such as Redis.** Rejected. A crash between the commit and marking the key complete leaves duplicates blocked until the TTL expires and then lets them run again. The store fails open when it is down. It is extra infrastructure, and it needs an in-flight window to handle concurrent duplicates.
- **Two mechanisms: client keys over HTTP, plus de-duplication of told Commands by id.** Rejected. It would mean two stores and two sets of conflict rules for one guarantee.
- **A retention window, such as 24 hours.** Rejected. One row per accepted Command grows at the same pace as the Event log, and an expiry would reopen duplicates for late Outbox re-sends and dead-letter replays.

## Consequences

- Concurrent duplicates may both run their handlers. That is harmless, because handlers have no external effects (ADR-0011): the loser's append fails on the unique record, and it returns the winner's outcome.
- A replayed HTTP response shows the Readmodel as it is now, not a cached copy of the first response.
- A client that reuses a key for a different request, however much later, gets `conflict`.

## Named Schedule attempts

A scheduled Command's fingerprint also includes its framework-owned Schedule name and original due time. Ordinary Command fingerprints retain their existing representation. Cancelled or superseded attempts record durable acceptance with no Events or effects, including after manual dead-letter replay; accepted cancellation Commands retain their original per-name cancellation results. This keeps a duplicate cancellation from cancelling a replacement and keeps obsolete work harmless across process restarts. Base envelope validation remains mandatory; an obsolete attempt bypasses current business schemas so a schema change cannot revive it as a failure.
