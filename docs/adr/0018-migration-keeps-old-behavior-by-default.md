# Migration keeps the old behavior by default; new semantics are adopted per call site

Where the old stack and EventFabric give the same declaration different meanings, a migrated Application keeps the old meaning. The codemods do this through today's API, wherever it can express the old behavior. Where it cannot, they leave the source unchanged and list it. The improved semantics stay the defaults for code written for EventFabric, and the Application adopts them one call site at a time once its tests cover the difference.

Old behavior is kept in one of two places:

- **In the runtime, only where the old form is unambiguous and the new framework has no conflicting meaning for it.** An enricher that sets `event.metadata` fields and returns nothing keeps them. The Reaction markers `settings.autoCreate`, `settings.shouldHandleEvent` and Saga `shouldHandle` keep their old meaning, and `only.if(predicate)` is their new spelling. A false predicate skips the whole Reaction, so nothing is created. A Readmodel's `reactionSets` hold the old grouped keys that had their own Identity or markers: one Event, several document selections on one model.
- **At the call site, through a named `event-fabric/compat` helper,** where the new framework gives the same form another meaning: `legacyMetadata` (deep metadata merge, Command values winning), `legacyQuery`, `legacyOne` and `legacyStream` (QueryBuilder filters, defaults and result shapes), `legacyQueryIdentity` (old `query` maps), `legacyEnricher` (an enricher that runs inside each `apply` on the State before that Event and may change the whole Event) and `legacyList` (the old list route's query string and `{ total, data }`). Codemods also emit plain code where that is enough: `(event) => event.id` for a Saga Event without an Identity, removal of the `initialState` the old Saga builder ignored, removal of the old `only.ifNotExists()` from Reactions.

Security fixes are not compatibility requirements. Access maps are enforced on every read and Notification, and the migration never recreates the old stack's unenforced Command rules or dropped tenant filters.

## Considered Options

- **Adopt the new semantics and list every difference** (the first migration approach). It produced no silent rewrites, but every inventoried call site became manual work, each a chance to change behavior while porting.
- **A compatibility mode switch per Application.** One flag would change the meaning of the same declaration depending on configuration, and new code in a migrated Application could never use the new semantics.
- **Change EventFabric's own semantics back to the old ones.** Shallow Event-wins metadata, portable queries, the Stream-id Saga default and frozen Events are deliberate; reverting them would reintroduce the problems they fix for every Application.

## Consequences

- Migrated code shows where it keeps old behavior: each helper call is greppable, and removing it is the decision to adopt the new semantics there.
- The old Reaction markers stay accepted by the runtime. `only.ifNotExists()` keeps its new meaning, so the codemod removes the old one from Reactions instead of renaming it.
- Where the old order was undefined, the migrated code picks one and documents it: Reaction sets that select the same document fold it in declaration order, where the old stack loaded and saved it once per group.
- `legacyList` keeps the old request contract but not its failure modes: errors answer before any byte is sent, negative paging is refused, and the old schema option, which serialized each document as `{}`, is not translated.
