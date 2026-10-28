import type { JsonObject } from "../types.js";
import { durableReadmodelStore } from "../readmodel-store/durable.js";
import type {
  ReadmodelTransaction,
  StoredGeneration,
  StoredGroup,
} from "../readmodel-store/durable.js";
import {
  compareValues,
  matches,
  pathValues,
} from "../readmodel-store/query.js";
import type { ReadmodelStore } from "../readmodel-store/types.js";

// Group and generation records round-trip through JSON, as the durable adapters store them.
const json = <T>(value: T): T =>
  value == null ? value : JSON.parse(JSON.stringify(value));
type GroupState = {
  /** The group's record and its Partitions' records, by name. */
  records: Record<string, StoredGroup>;
  generations: Map<
    string,
    {
      generation: StoredGeneration;
      models: Map<string, Map<string, JsonObject>>;
    }
  >;
};

/**
 * Readmodel store over Maps, behind the same durable rules as Postgres and Mongo.
 * A migration's native handle is the generation's Map of model name to documents by id.
 */
export function createMemoryReadmodelStore(): ReadmodelStore<
  Map<string, JsonObject> | undefined
> {
  const state = new Map<string, GroupState>();
  let writing = Promise.resolve();
  const transaction = (current: GroupState): ReadmodelTransaction => {
    const entry = (gen: StoredGeneration) => {
      const value = current.generations.get(gen.id);
      if (!value) throw new Error("Unknown Readmodel generation");
      return value;
    };
    const documents = (gen: StoredGeneration, model: string) => {
      const docs = entry(gen).models.get(model);
      if (!docs) throw new Error(`Unknown Readmodel ${model}`);
      return docs;
    };
    const key = (document: JsonObject, order: [string, 1 | -1][]) =>
      order.map(([path]) => pathValues(document, path.split("."))[0] ?? null);
    const compare = (a: any[], b: any[], order: [string, 1 | -1][]) => {
      for (const [i, [, direction]] of order.entries()) {
        const result = compareValues(a[i], b[i]);
        if (result) return result * direction;
      }
      return 0;
    };
    return {
      async getGroup(name) {
        return json(current.records[name] ?? null);
      },
      async listPartitions(group) {
        return Object.entries(current.records)
          .filter(([name]) => name.startsWith(`${group}#`))
          .map(([, record]) => json(record) as any);
      },
      async putGroup(group) {
        current.records[group.name] = json(group);
      },
      async getGeneration(_group, id) {
        return json(current.generations.get(id)?.generation ?? null);
      },
      async putGeneration(generation) {
        entry(generation).generation = json(generation);
      },
      async create(generation) {
        current.generations.set(generation.id, {
          generation: json(generation),
          models: new Map(
            Object.keys(generation.definitions).map((model) => [
              model,
              new Map(),
            ]),
          ),
        });
      },
      async drop(generation) {
        current.generations.delete(generation.id);
      },
      async read(gen, model, filter = {}) {
        return [...documents(gen, model).values()].filter((document) =>
          matches(document, filter),
        );
      },
      async page(gen, model, filter, order, after, limit) {
        return [...documents(gen, model).values()]
          .filter(
            (document) =>
              matches(document, filter) &&
              (!after || compare(key(document, order), after, order) > 0),
          )
          .sort((a, b) => compare(key(a, order), key(b, order), order))
          .slice(0, limit);
      },
      async write(gen, model, changes) {
        for (const [id, document] of changes)
          if (document === null) documents(gen, model).delete(id);
          else documents(gen, model).set(id, structuredClone(document));
      },
      native(gen, model) {
        return model ? entry(gen).models.get(model) : entry(gen).models;
      },
    };
  };
  return durableReadmodelStore({
    label: "memory",
    async transaction(group, fn, options) {
      const live = state.get(group) ?? { records: {}, generations: new Map() };
      if (options?.readOnly) return fn(transaction(live));
      const run = writing.then(async () => {
        // shortcut: each write copies the whole group; fine for tests and development, not for large Readmodels.
        const copy = structuredClone(state.get(group) ?? live);
        const result = await fn(transaction(copy));
        state.set(group, copy);
        return result;
      });
      writing = run.then(
        () => {},
        () => {},
      );
      return run;
    },
    async close() {},
  }) as ReadmodelStore<Map<string, JsonObject> | undefined>;
}
