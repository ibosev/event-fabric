import { AsyncLocalStorage } from "node:async_hooks";
import { comparePositions } from "../position.js";
import {
  copyDocument,
  queryDocuments,
  textFields,
} from "../readmodel-store/query.js";
import {
  indexDocument,
  indexableClauses,
  lookups,
  pathIndex,
  union,
  type PathIndex,
} from "./filter-index.js";
import { normalizeDocument, resolveSchema } from "../readmodel-store/schema.js";
import type { Filter, ReadmodelStore } from "../readmodel-store/types.js";
import type { JsonObject, Position, Schema } from "../types.js";
import type { ReadOptions } from "./readmodels.js";
import type { ReadmodelGroup } from "./types.js";

type Entry = {
  scope: string;
  /** For the group's own documents, `null` records a document known to be absent. */
  value: any;
  /** One of the rebuilt group's own documents; only losing one of these ends a complete scope. */
  own: boolean;
  /** A folded change, normalized on its first read. */
  folded: boolean;
  /** For another group's read: that group's Checkpoint when it was read. */
  stamp?: Position;
  bytes: number;
};
/** What a complete scope knows besides its entries: every document id per model, and its indexes. */
type Complete = {
  ids: Map<string, Set<string>>;
  indexes: Map<string, Map<string, PathIndex>>;
  indexBytes: number;
};
// Measured: about 250 bytes per keyed document and indexed path.
const indexBytesPerDocument = 250;

export const defaultRebuildCacheBytes = 256 * 2 ** 20;
/**
 * About the length of `value` as JSON, 16 times faster than `JSON.stringify` on plain documents:
 * numbers count 8 and escapes are not counted. Too deep, as a cycle is, counts as never fitting.
 */
export function jsonLength(value: unknown, depth = 0): number {
  if (value === null || value === undefined) return 4;
  if (typeof value === "string") return value.length + 2;
  if (typeof value === "number") return 8;
  if (typeof value === "boolean") return 5;
  if (typeof value !== "object") return 4;
  if (depth > 200) return Infinity;
  if (value instanceof Date) return 26;
  const prototype = Object.getPrototypeOf(value);
  // Anything else, such as a driver's document with `toJSON`, is measured as JSON is written.
  if (
    (prototype !== Object.prototype &&
      prototype !== Array.prototype &&
      prototype !== null) ||
    typeof (value as { toJSON?: unknown }).toJSON === "function"
  )
    try {
      return (JSON.stringify(value) ?? "null").length;
    } catch {
      return Infinity;
    }
  let length = 2;
  if (Array.isArray(value))
    for (const item of value) length += jsonLength(item, depth + 1) + 1;
  else
    for (const key of Object.keys(value))
      length +=
        key.length +
        4 +
        jsonLength((value as Record<string, unknown>)[key], depth + 1);
  return length;
}
/**
 * What rebuilds have read and folded, least recently used first, within `maxBytes` for the whole
 * process. Each rebuild has its own scope, freed when it ends.
 *
 * A rebuild is the only writer of its generation, so its own documents stay exact until it ends; an
 * evicted one is read from the store again. A scope that began on an empty generation is complete
 * until it loses one: every document it lacks is then absent, so reads by `_id` need no store at all.
 */
export function rebuildCache(maxBytes: number) {
  const entries = new Map<string, Entry>(),
    complete = new Map<string, Complete>();
  let bytes = 0;
  const remove = (key: string) => {
    const entry = entries.get(key);
    if (!entry) return;
    entries.delete(key);
    bytes -= entry.bytes;
  };
  // A scope that lost one of its documents also loses its indexes, which then could miss one.
  const incomplete = (scope: string) => {
    bytes -= complete.get(scope)?.indexBytes ?? 0;
    complete.delete(scope);
  };
  const trim = () => {
    for (const [oldest, entry] of entries) {
      if (bytes <= maxBytes) break;
      if (entry.own) incomplete(entry.scope);
      remove(oldest);
    }
  };
  return {
    /** Starts a scope; `empty` when its generation has no documents yet. */
    begin(scope: string, empty: boolean) {
      this.drop(scope);
      if (empty)
        complete.set(scope, {
          ids: new Map(),
          indexes: new Map(),
          indexBytes: 0,
        });
    },
    /** Ends a scope and frees its entries. */
    drop(scope: string) {
      incomplete(scope);
      for (const [key, entry] of [...entries])
        if (entry.scope === scope) remove(key);
    },
    complete: (scope: string) => complete.has(scope),
    /** A complete scope's ids and indexes of `model`. */
    model(scope: string, model: string) {
      const state = complete.get(scope);
      if (!state) return;
      let indexes = state.indexes.get(model);
      if (!indexes) state.indexes.set(model, (indexes = new Map()));
      return { ids: state.ids.get(model) ?? new Set<string>(), indexes };
    },
    /** Counts a change in an index's size against the cap, evicting entries if needed. */
    account(scope: string, delta: number) {
      const state = complete.get(scope);
      if (!state) return;
      state.indexBytes += delta;
      bytes += delta;
      trim();
    },
    set(
      scope: string,
      key: string,
      value: unknown,
      {
        own = false,
        folded = false,
        stamp,
        document,
      }: Partial<Entry> & {
        /** A document's model and id, which a complete scope tracks for its indexes. */
        document?: { model: string; id: string };
      } = {},
    ) {
      remove(key);
      const state = document && complete.get(scope);
      if (state) {
        let ids = state.ids.get(document!.model);
        if (!ids) state.ids.set(document!.model, (ids = new Set()));
        if (value) ids.add(document!.id);
        else ids.delete(document!.id);
        for (const index of state.indexes.get(document!.model)?.values() ?? [])
          index.dirty.add(document!.id);
      }
      // Heap use measured about 2.5 times the JSON size of typical documents.
      const size = 3 * (key.length + jsonLength(value)) + 100;
      if (size > maxBytes) {
        if (own) incomplete(scope);
        return;
      }
      entries.set(key, { scope, value, own, folded, stamp, bytes: size });
      bytes += size;
      trim();
    },
    get(key: string) {
      const entry = entries.get(key);
      if (entry) {
        entries.delete(key);
        entries.set(key, entry);
      }
      return entry;
    },
    get bytes() {
      return bytes;
    },
  };
}
export type RebuildCache = ReturnType<typeof rebuildCache>;

/** The ids a filter selects when it selects documents by `_id` alone. */
const idsOf = (filter: Filter): string[] | undefined => {
  const keys = Object.keys(filter);
  if (keys.length !== 1 || keys[0] !== "_id") return;
  const id = (filter as any)._id;
  if (typeof id === "string") return [id];
  if (!id || typeof id !== "object" || Array.isArray(id)) return;
  const [operator, ...rest] = Object.keys(id);
  const ids = id[operator!];
  if (
    rest.length ||
    (operator !== "in" && operator !== "$in") ||
    !Array.isArray(ids) ||
    !ids.every((value) => typeof value === "string")
  )
    return;
  return ids;
};

/**
 * One rebuild batch's view of its group's store: reads of its generation by `_id` come from `cache`
 * where they can, and every document it reads is kept there; the caller keeps what it folds.
 * `uncommitted` holds the keys of the previous batch, still committing: the store still has their
 * earlier state, so reading one must not replace the entry its fold recorded.
 */
export function cachedReads(
  cache: RebuildCache,
  scope: string,
  name: string,
  group: ReadmodelGroup,
  schemas: Record<string, Schema> | undefined,
  generation: string,
  uncommitted: Set<string>,
): ReadmodelGroup {
  const { store } = group;
  const key = (model: string, id: string) =>
    scope + JSON.stringify([model, id]);
  const keep = (model: string, id: string, document: JsonObject | null) => {
    const at = key(model, id);
    if (!uncommitted.has(at))
      cache.set(scope, at, document && copyDocument(document), {
        own: true,
        document: { model, id },
      });
  };
  const resolved = new Map<string, ReturnType<typeof resolveSchema>>();
  const read = async (model: string, id: string) => {
    const entry = cache.get(key(model, id));
    if (entry?.folded && entry.value) {
      let schema = resolved.get(model);
      if (!schema)
        resolved.set(
          model,
          (schema = resolveSchema(group.models[model]!.schema, {
            ...schemas,
            ...group.schemas,
          })),
        );
      entry.value = await normalizeDocument(
        schema,
        entry.value,
        id,
        false,
        undefined,
        false,
      );
      entry.folded = false;
    }
    return entry;
  };
  // Keys the documents changed since an index was last used; one update at a time per index.
  const updating = new WeakMap<PathIndex, Promise<void>>();
  const refresh = async (model: string, index: PathIndex) => {
    for (;;) {
      const running = updating.get(index);
      if (running) await running;
      else if (index.dirty.size) {
        const ids = [...index.dirty];
        index.dirty.clear();
        const update = (async () => {
          const found = await Promise.all(ids.map((id) => read(model, id)));
          ids.forEach((id, i) =>
            indexDocument(index, id, found[i]?.value ?? null),
          );
        })();
        updating.set(index, update);
        try {
          await update;
        } finally {
          updating.delete(index);
        }
      } else return;
    }
  };
  /*
   * A filter on the scope's own generation answered from memory: while the scope is
   * complete the cache holds every document, so an index on one of the filter's equality or `in`
   * clauses gives a superset of the matches, and the matcher picks them out in the store's `_id`
   * order. Undefined when the filter has no such clause or the scope stops being complete.
   */
  const indexed = async (
    model: string,
    filter: Filter,
    options: Parameters<ReadmodelStore["find"]>[3] = {},
  ) => {
    const clauses = indexableClauses(filter);
    let best: { sets: Set<string>[]; size: number } | undefined;
    for (const { path, values } of clauses) {
      const known = cache.model(scope, model);
      if (!known) return;
      let index = known.indexes.get(path);
      if (!index) {
        known.indexes.set(path, (index = pathIndex(path)));
        for (const id of known.ids) index.dirty.add(id);
      }
      const before = index.keysOf.size;
      try {
        await refresh(model, index);
      } catch (error) {
        known.indexes.delete(path);
        throw error;
      }
      cache.account(
        scope,
        (index.keysOf.size - before) * indexBytesPerDocument,
      );
      // Only the clause with the fewest candidates is copied into a set.
      const sets = lookups(index, values),
        size = sets?.reduce((n, ids) => n + ids.size, 0);
      if (sets && (!best || size! < best.size)) best = { sets, size: size! };
    }
    if (!best) return;
    const documents: JsonObject[] = [];
    for (const id of union(best.sets)) {
      const entry = await read(model, id);
      if (!entry) return;
      if (entry.value) documents.push(entry.value);
    }
    if (!cache.complete(scope)) return;
    return queryDocuments(
      documents,
      filter,
      { ...options, sort: options.sort ?? {} },
      textFields(group.models[model]!),
    );
  };
  const find: ReadmodelStore["find"] = async (
    group_,
    model,
    filter = {},
    options = {},
  ) => {
    const ids = idsOf(filter);
    const own = group_ === name && options.generation === generation;
    if (
      own &&
      !options.q &&
      !ids &&
      group.models[model] &&
      cache.complete(scope)
    ) {
      const found = await indexed(model, filter, options);
      if (found) return found;
    }
    if (!own || options.q || !ids || !group.models[model]) {
      const found = await store.find(group_, model, filter, options);
      if (own && !options.q)
        for (const document of found)
          keep(model, String(document._id), document);
      return found;
    }
    const documents: JsonObject[] = [],
      missing: string[] = [];
    for (const id of new Set(ids)) {
      const entry = await read(model, id);
      if (entry) {
        if (entry.value) documents.push(entry.value);
      } else if (!cache.complete(scope)) missing.push(id);
    }
    if (missing.length) {
      const found = new Map(
        (
          await store.find(
            name,
            model,
            { _id: { in: missing } },
            { generation },
          )
        ).map((document) => [String(document._id), document]),
      );
      for (const id of missing) {
        const document = found.get(id) ?? null;
        keep(model, id, document);
        if (document) documents.push(document);
      }
    }
    // Copies: a fold changes the documents it is given. In `_id` order, as the store returns them.
    return queryDocuments(
      documents,
      filter,
      { ...options, sort: options.sort ?? {} },
      textFields(group.models[model]!),
    );
  };
  const findOne: ReadmodelStore["findOne"] = async (
    group_,
    model,
    filter,
    options,
  ) => (await find(group_, model, filter, { ...options, size: 1 }))[0] ?? null;
  return {
    ...group,
    store: new Proxy(store, {
      get: (target, property) =>
        property === "find"
          ? find
          : property === "findOne"
            ? findOne
            : typeof (target as any)[property] === "function"
              ? (target as any)[property].bind(target)
              : (target as any)[property],
    }),
  };
}

/** Whether `JSON.stringify` keeps everything that distinguishes `value`, so it can be a key. */
const keyable = (value: unknown): boolean =>
  value === null ||
  ["string", "number", "boolean"].includes(typeof value) ||
  value instanceof Date ||
  (Array.isArray(value) && value.every(keyable)) ||
  (typeof value === "object" &&
    Object.getPrototypeOf(value) === Object.prototype &&
    Object.values(value).every(keyable));

/**
 * Repositories of the groups a rebuild reads, for one batch whose last Event is at `through`. Each
 * result is kept with the read group's Checkpoint and reused while that Checkpoint is at or after
 * `through`: before each batch the rebuild waits for the groups it reads to reach its last Event, and
 * a Reaction reads their current state, so a kept result is never older than one read again.
 */
export function otherGroupReads(
  cache: RebuildCache,
  scope: string,
  readmodels: any,
  groups: Record<string, string[]>,
  through: Position,
) {
  const kept = async <T>(
    key: unknown[],
    load: () => Promise<{ value: T; stamp: Position | null }>,
  ): Promise<T> => {
    if (!keyable(key)) return (await load()).value;
    const at = scope + JSON.stringify(["read", ...key]);
    const entry = cache.get(at);
    if (entry && comparePositions(entry.stamp!, through) >= 0)
      return copyDocument(entry.value);
    const { value, stamp } = await load();
    if (stamp && comparePositions(stamp, through) >= 0)
      cache.set(scope, at, copyDocument(value), { stamp });
    return value;
  };
  const repository = (group: string, model: string) => {
    const live = readmodels.repository(group, model);
    // Reads that wait for a Position keep their own meaning.
    const query =
      (method: "find" | "findOne" | "count") =>
      (filter: Filter = {}, options?: ReadOptions) =>
        options?.minPosition
          ? live[method](filter, options)
          : kept([group, model, method, filter, options ?? null], async () => {
              // The Checkpoint is read first, so the result is at least as new as it.
              const { position } = await readmodels.status(group);
              return {
                value: await live[method](filter, options),
                stamp: position,
              };
            });
    const read = (id: string, options?: ReadOptions) =>
      options?.minPosition
        ? live(id, options)
        : kept([group, model, "read", id], async () => {
            const result = await live(id, options);
            return { value: result, stamp: result.position };
          });
    return Object.assign(read, {
      find: query("find"),
      findOne: query("findOne"),
      count: query("count"),
      stream: live.stream,
      native: live.native,
    });
  };
  return Object.fromEntries(
    Object.entries(groups).map(([group, models]) => [
      group,
      Object.fromEntries(
        models.map((model) => [model, repository(group, model)]),
      ),
    ]),
  );
}

const memoScope = new AsyncLocalStorage<{
  cache: RebuildCache;
  scope: string;
}>();
/** Runs a rebuild batch's fold with its scope, so `rebuildMemo` keeps values in that rebuild. */
export const withRebuildMemo = <T>(
  cache: RebuildCache,
  scope: string,
  fold: () => Promise<T>,
) => memoScope.run({ cache, scope }, fold);

/**
 * Inside a Readmodel rebuild, keeps what `load` returns under `key` until the rebuild ends, within
 * the rebuild cache's memory; anywhere else it calls `load` every time. For data a rebuild may
 * treat as fixed, such as an Application's CRUD records: the Application owns that choice. Callers
 * share the kept value, so they must not change it. A key that is not plain JSON is never kept.
 */
export async function rebuildMemo<T>(
  key: unknown,
  load: () => T | Promise<T>,
): Promise<T> {
  const context = memoScope.getStore();
  if (!context || !keyable(key)) return load();
  const at = context.scope + JSON.stringify(["memo", key]);
  const entry = context.cache.get(at);
  if (entry) return entry.value;
  const value = await load();
  context.cache.set(context.scope, at, value);
  return value;
}
