# Clean-room TypeScript rewrite in a separate repo

EventFabric is rebuilt from scratch in TypeScript, in its own repository. Existing Applications run the old oblak stack (cqrs-domain, cqrs-saga, cqrs-eventdenormalizer, eventstore, kellner, zaek) as a vendored, renamed copy. Its core is callback-based and untested, and its Postgres path marks Events dispatched before they reach the broker and assigns Positions outside the commit transaction. Fixing that in place would mean rewriting most of it without tests to lean on, so it is reference material only. So are its tests and its modernization docs, whose claims did not always match the code.

## Considered Options

- **Evolve the vendored copy.** Rejected: the parts that need to change (command kernel, event store, transport) are the parts with no tests.
- **New folder inside an existing Application.** Rejected: keeps the framework coupled to one Application's config, tooling and release cycle.
