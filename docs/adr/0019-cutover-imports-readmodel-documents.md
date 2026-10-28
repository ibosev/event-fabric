# A cutover imports Readmodel documents instead of rebuilding them

At cutover, an Application can take over the Readmodel documents the old stack already folded, instead of rebuilding every group from the imported Event log. `importReadmodels(group, documents, { position })` loads them into a pending generation whose Checkpoint is the imported log head. Any later Events are folded as a rebuild would fold them, and the generation is swapped in. The old store is read, never written, so rollback stays the old stack's restart.

An import is correct only when the documents equal what the group's Reactions produce from the log up to that Position. EventFabric cannot check that. The Application proves it once: a rehearsal rebuilds every group from the imported log and compares every document with the old store, with zero unexplained differences. Importing is then much faster than a sequential rebuild.

## Considered Options

- **Rebuild every group at cutover.** It needs no trust in the old documents, but its time grows with the log: a large log means hours of rebuilding while the system is offline.
- **Adopt the old collections in place.** It copies nothing, but EventFabric would need external collection names, and it would write to the old store, so rollback would lose the old state.
- **Import into a new generation** (chosen). It costs one pass over the documents, and leaves the old store untouched.

## Consequences

- The new store keeps whatever the old documents held, including differences a rebuild would remove (e.g. documents the old rebuild never deleted). A later rebuild, after a schema or Reaction change, produces EventFabric's own result. The parity comparison lists those differences in advance.
- Readmodel stores implement the optional `load` method: it writes only into a pending generation without a Checkpoint, under the group's epoch. Stores built on `durableReadmodelStore` have it.
- Rebuild speed still matters after cutover, for schema changes, new Readmodels and Reaction fixes. Those rebuild online, into a pending generation, while live processing continues.
