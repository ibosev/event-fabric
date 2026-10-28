# Keep the authoring interface; allow mechanical codemods

EventFabric keeps the shape Applications already author: Aggregates as `{ commands, events, initialState, idGenerator }`, the `only.*` Preconditions and Rules, Readmodels and Sagas as `{ identity, reactions }`, the `getDomain()` / `getReadmodels()` / `getServices()` accessors and the REST helpers. `getCrud()` is not kept: CRUD models are Application code, reached through an Application service after a prefix-only codemod. Applications may be migrated with mechanical codemods where the old stack leaked its internals (raw Mongo collections via `.base()`, Mongo-filter Event store queries, `_runtime` access from plugins), so EventFabric does not have to keep those leaks.

## Considered Options

- **Zero changes to existing Applications.** Rejected: it would force the Mongo driver and the old runtime internals into EventFabric's interface permanently.
- **Free redesign of the authoring interface.** Rejected: existing Applications would have to migrate every Aggregate, Readmodel, Saga and `only.*` use by hand.
