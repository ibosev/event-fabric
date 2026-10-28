import { pathValues } from "../readmodel-store/query.js";
import type { Filter } from "../readmodel-store/types.js";

/*
 * A hash index of one model's documents at one path, so a rebuild whose cache holds every document
 * answers equality and `in` filters from memory. It only narrows the candidates: the
 * matcher re-checks every one, so it must return a superset of what `matches` accepts for a clause.
 */
export type PathIndex = {
  path: string[];
  values: Map<unknown, Set<string>>;
  /** Stored Dates by time: the matcher compares a Date with anything through `new Date()`. */
  dates: Map<number, Set<string>>;
  /** Documents with an object or nested array at the path, which the index cannot key: always candidates. */
  always: Set<string>;
  keysOf: Map<string, { values: Set<unknown>; dates: Set<number> }>;
  /** Strings, numbers and booleans, which a Date condition can equal after conversion. */
  scalars: number;
  /** Documents changed since they were keyed. */
  dirty: Set<string>;
};

export const pathIndex = (path: string): PathIndex => ({
  path: path.split("."),
  values: new Map(),
  dates: new Map(),
  always: new Set(),
  keysOf: new Map(),
  scalars: 0,
  dirty: new Set(),
});

const scalar = (value: unknown) =>
  ["string", "number", "boolean"].includes(typeof value);
const add = <K>(map: Map<K, Set<string>>, key: K, id: string) => {
  let ids = map.get(key);
  if (!ids) map.set(key, (ids = new Set()));
  ids.add(id);
};
const take = <K>(map: Map<K, Set<string>>, key: K, id: string) => {
  const ids = map.get(key);
  ids?.delete(id);
  if (ids && !ids.size) map.delete(key);
};

/** Keys a normalized document, or removes it when `document` is null. */
export function indexDocument(
  index: PathIndex,
  id: string,
  document: Record<string, unknown> | null,
) {
  const old = index.keysOf.get(id);
  if (old) {
    for (const value of old.values) {
      take(index.values, value, id);
      if (scalar(value)) index.scalars--;
    }
    for (const time of old.dates) take(index.dates, time, id);
    index.always.delete(id);
    index.keysOf.delete(id);
  }
  if (!document) return;
  const keys = { values: new Set<unknown>(), dates: new Set<number>() };
  // As `contains`: a value at the path that is an array matches by its elements.
  for (const value of pathValues(document, index.path))
    for (const item of Array.isArray(value) ? value : [value])
      if (item instanceof Date) keys.dates.add(item.valueOf());
      else if (item !== null && typeof item === "object") index.always.add(id);
      // `equal` takes an absent value for `null`.
      else keys.values.add(item ?? null);
  for (const value of keys.values) {
    add(index.values, value, id);
    if (scalar(value)) index.scalars++;
  }
  for (const time of keys.dates) add(index.dates, time, id);
  index.keysOf.set(id, keys);
}

const keyed = (value: unknown) =>
  value === null ||
  value === undefined ||
  value instanceof Date ||
  scalar(value);
/**
 * The values a top-level filter clause selects by, or undefined when the index cannot narrow it:
 * negative operators (an empty array satisfies them), whole-object or whole-array equality, and
 * every other operator.
 */
export function clauseValues(condition: unknown): unknown[] | undefined {
  if (keyed(condition)) return [condition];
  if (typeof condition !== "object" || Array.isArray(condition)) return;
  const operators = Object.keys(condition!).map((key) =>
    key.replace(/^\$/, ""),
  );
  if (operators.some((key) => key === "ne" || key === "nin")) return;
  const at = (operator: string) =>
    Object.hasOwn(condition!, operator)
      ? (condition as any)[operator]
      : (condition as any)[`$${operator}`];
  if (operators.includes("eq") && keyed(at("eq"))) return [at("eq")];
  const values = at("in");
  if (operators.includes("in") && Array.isArray(values) && values.every(keyed))
    return values;
}

/**
 * The id sets whose union holds every document whose keys can equal one of `values`, or undefined
 * when a Date condition cannot use the index. Their sizes estimate the clause's cost before any copy.
 */
export function lookups(
  index: PathIndex,
  values: unknown[],
): Set<string>[] | undefined {
  const sets = [index.always];
  const merge = (ids?: Set<string>) => ids && sets.push(ids);
  for (const value of values)
    if (value instanceof Date) {
      // A stored string or number can convert to the same time, and no hash finds it.
      if (index.scalars) return;
      merge(index.dates.get(value.valueOf()));
      if (value.valueOf() === 0) merge(index.values.get(null));
    } else {
      merge(index.values.get(value ?? null));
      if (index.dates.size)
        merge(index.dates.get(new Date(value as any).valueOf()));
    }
  return sets;
}
export const union = (sets: Set<string>[]) => {
  const found = new Set<string>();
  for (const ids of sets) for (const id of ids) found.add(id);
  return found;
};
/** The ids whose keys can equal one of `values`, or undefined when a Date condition cannot use the index. */
export const candidates = (index: PathIndex, values: unknown[]) => {
  const sets = lookups(index, values);
  return sets && union(sets);
};

/** The top-level path clauses of a filter that an index can narrow, with their values. */
export const indexableClauses = (filter: Filter) =>
  Object.entries(filter).flatMap(([path, condition]) => {
    if (["oneOf", "$or", "and", "$and"].includes(path)) return [];
    const values = clauseValues(condition);
    return values ? [{ path, values }] : [];
  });

/**
 * Indexes over documents a caller holds in memory, such as a batch's staged changes: `touch` marks
 * a document changed, and an index re-keys it from `document` on its next use.
 */
export function memoryIndexes(
  document: (model: string, id: string) => Record<string, unknown> | null,
) {
  const indexes = new Map<string, Map<string, PathIndex>>(),
    ids = new Map<string, Set<string>>();
  return {
    touch(model: string, id: string) {
      let known = ids.get(model);
      if (!known) ids.set(model, (known = new Set()));
      known.add(id);
      for (const index of indexes.get(model)?.values() ?? [])
        index.dirty.add(id);
    },
    /** A superset of the ids `filter` selects, or undefined when no clause of it can narrow. */
    candidates(model: string, filter: Filter) {
      let best: { sets: Set<string>[]; size: number } | undefined;
      for (const { path, values } of indexableClauses(filter)) {
        let byPath = indexes.get(model);
        if (!byPath) indexes.set(model, (byPath = new Map()));
        let index = byPath.get(path);
        if (!index) {
          byPath.set(path, (index = pathIndex(path)));
          for (const id of ids.get(model) ?? []) index.dirty.add(id);
        }
        for (const id of index.dirty)
          indexDocument(index, id, document(model, id));
        index.dirty.clear();
        const sets = lookups(index, values),
          size = sets?.reduce((n, found) => n + found.size, 0);
        if (sets && (!best || size! < best.size)) best = { sets, size: size! };
      }
      return best && union(best.sets);
    },
  };
}
