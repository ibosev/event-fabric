import { PassThrough, Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createHash, randomUUID } from "node:crypto";
import { comparePositions } from "../position.js";
import {
  ForbiddenError,
  GenerationBehindError,
  StaleEpochError,
  StaleGenerationError,
} from "../errors.js";
import type { JsonObject, Position, Validate } from "../types.js";
import { normalizeDocument, prepareDefinitions, scalarPath } from "./schema.js";
import {
  copyDocument,
  exactFilter,
  pathValues,
  queryDocuments,
  textFields,
} from "./query.js";
import type {
  Filter,
  QueryOptions,
  ReadmodelChange,
  NativeOptions,
  ReadmodelDefinitions,
  ReadmodelStore,
} from "./types.js";
export type StoredGroup = {
  name: string;
  epoch: number;
  active: string;
  pending?: string;
  commitToken?: string;
  desired: ReadmodelDefinitions;
  hash: string;
  validate: boolean;
};
export type StoredGeneration = {
  group: string;
  id: string;
  definitions: ReadmodelDefinitions;
  hash: string;
  checkpoint: Position | null;
  reconstruction?: string;
  /** Bumped by `resetPartitions`, which moves every Partition back to `checkpoint`. */
  version?: number;
  /**
   * Each model's collection or table, recorded by the store when it creates the generation.
   * Generations created before readable names have none and use hashed names.
   */
  names?: Record<string, string>;
  /** Mongo: the group's database holding `names`. */
  database?: string;
};
/**
 * One Partition of a partitioned group, stored as group record `<group>#<partition>`.
 * Partitions write only their own record and documents, so they commit in parallel.
 */
export type StoredPartition = {
  name: string;
  /** Lease epoch of Partitions 1..N-1; Partition 0 is fenced by the group's `epoch`. */
  epoch: number;
  /** The Checkpoint holds while `generation` and `version` match the generation's. */
  generation?: string;
  version?: number;
  checkpoint?: Position;
  commitToken?: string;
};
/** One fold worker's progress in a sliced rebuild, stored as group record `<group>@<generation>#<slice>`. */
type StoredSlice = { name: string; count: number; checkpoint?: Position };
const sliceName = (group: string, generation: string, slice: number) =>
  `${group}@${generation}#${slice}`;
/** Partition records not stored yet. */
const unsaved = new WeakSet<StoredPartition>();
const partitionOf = async (
  tx: ReadmodelTransaction,
  group: string,
  partition: number,
): Promise<StoredPartition> => {
  const name = `${group}#${partition}`;
  const stored = (await tx.getGroup(name)) as unknown as StoredPartition | null;
  if (stored) return stored;
  const created = { name, epoch: 0 };
  unsaved.add(created);
  return created;
};
/**
 * Every write of a Partition record changes its `commitToken`, so on Mongo it conflicts with
 * any other transaction writing that record. Creating a record also writes the group record,
 * so a whole-group transition that lists the records cannot miss one created meanwhile.
 */
const putPartition = async (
  tx: ReadmodelTransaction,
  g: StoredGroup,
  part: StoredPartition,
) => {
  part.commitToken = randomUUID();
  await tx.putGroup(part as unknown as StoredGroup);
  if (unsaved.delete(part)) {
    g.commitToken = randomUUID();
    await tx.putGroup(g);
  }
};
/** Partition metadata in its older form, inside the group and generation records. */
type Legacy = {
  epochs?: Record<string, number>;
  checkpoints?: Record<string, Position>;
};
/** Moves Partition epochs and Checkpoints from the group and generation records into Partition records. */
const migrateLegacy = async (
  tx: ReadmodelTransaction,
  g: StoredGroup & Legacy,
  gen: StoredGeneration & Legacy,
) => {
  if (!g.epochs && !gen.checkpoints) return;
  const partitions = new Set([
    ...Object.keys(g.epochs ?? {}),
    ...Object.keys(gen.checkpoints ?? {}),
  ]);
  for (const p of partitions) {
    const part = await partitionOf(tx, g.name, Number(p));
    // Partition 0's epoch stays in the group record; the others keep theirs.
    if (p !== "0") part.epoch = Math.max(part.epoch, g.epochs?.[p] ?? 0);
    const checkpoint = gen.checkpoints?.[p];
    if (checkpoint)
      Object.assign(part, {
        generation: gen.id,
        version: gen.version ?? 0,
        checkpoint,
      });
    await putPartition(tx, g, part);
  }
  delete g.epochs;
  delete gen.checkpoints;
  await tx.putGeneration(gen);
  await tx.putGroup(g);
};
const checkpointOf = (gen: StoredGeneration, part?: StoredPartition) =>
  part?.checkpoint &&
  part.generation === gen.id &&
  part.version === (gen.version ?? 0)
    ? part.checkpoint
    : gen.checkpoint;
const epochOf = (g: StoredGroup, part?: StoredPartition) =>
  part && !part.name.endsWith("#0") ? part.epoch : g.epoch;
const freeze = <T>(value: T): T => {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
};
/**
 * Every transaction reads its group and generation records, whose definitions can be large; a store reads
 * them without the definitions and takes these by hash. Equal hashes mean equal definitions, so each is
 * decoded once per process, shared by every transaction and frozen.
 */
export const definitionsByHash = () => {
  const cache = new Map<string, ReadmodelDefinitions>();
  return async <T extends StoredGroup | StoredGeneration>(
    /** The record read without its definitions. */
    record: (Omit<T, "desired" | "definitions"> & { hash?: string }) | null,
    field: "desired" | "definitions",
    /** The whole record, read in the same transaction. */
    load: () => Promise<T | null>,
  ): Promise<T | null> => {
    // Partition and slice records carry no definitions.
    if (!record?.hash) return record as T | null;
    const definitions = cache.get(record.hash);
    if (definitions) return { ...record, [field]: definitions } as unknown as T;
    const full = await load();
    if (full)
      cache.set(full.hash, freeze((full as Record<string, any>)[field]));
    return full;
  };
};
/** What a native handle needs to join other groups' models: their active generations and the caller's scope. */
export type NativeJoins = NativeOptions & {
  generation(group: string): Promise<StoredGeneration>;
};
export interface ReadmodelTransaction {
  /** The native predicate's equality on a json field matches the value or an element of its array, as `matches` does (`exactFilter`'s `jsonEquality`). */
  jsonEquality?: boolean;
  /** `read` and `page` return documents built for this call, which the caller may keep and change. */
  ownsDocuments?: boolean;
  getGroup(name: string): Promise<StoredGroup | null>;
  /** Every stored Partition record (`<group>#<partition>`) of `group`. */
  listPartitions(group: string): Promise<StoredPartition[]>;
  putGroup(group: StoredGroup): Promise<void>;
  getGeneration(group: string, id: string): Promise<StoredGeneration | null>;
  putGeneration(generation: StoredGeneration): Promise<void>;
  create(generation: StoredGeneration): Promise<void>;
  drop(generation: StoredGeneration): Promise<void>;
  read(
    generation: StoredGeneration,
    model: string,
    filter?: Filter,
    options?: QueryOptions,
  ): Promise<JsonObject[]>;
  /** Counts matches in the database; only called for a filter `exactFilter` accepts. */
  count?(
    generation: StoredGeneration,
    model: string,
    filter: Filter,
  ): Promise<number>;
  /** Candidates (a superset of matches) ordered by `order`, strictly after the `after` key values. */
  page(
    generation: StoredGeneration,
    model: string,
    filter: Filter,
    order: [string, 1 | -1][],
    after: any[] | undefined,
    limit: number,
  ): Promise<JsonObject[]>;
  /** Replaces these documents of one model, each id at most once; `null` deletes. */
  write(
    generation: StoredGeneration,
    model: string,
    documents: [id: string, document: JsonObject | null][],
  ): Promise<void>;
  native(
    generation: StoredGeneration,
    model?: string,
    external?: boolean,
    joins?: NativeJoins,
  ): any;
  /** A read-only native handle limited to `filter`, which `exactFilter` accepts; absent where the store cannot. */
  scopedNative?(
    generation: StoredGeneration,
    model: string,
    filter: Filter,
    joins?: NativeJoins,
  ): any;
}
export interface ReadmodelBackend {
  transaction<T>(
    group: string,
    fn: (transaction: ReadmodelTransaction) => Promise<T>,
    options?: {
      readOnly?: boolean;
      schemaChange?: boolean;
      /** Writes of one Partition or slice only: they exclude that one's and the whole group's other writes, not the others'. */
      partition?: number | string;
    },
  ): Promise<T>;
  close(): Promise<void>;
  /** Partition writes exclude whole-group writes by lock, not by write conflicts, so rebuild slices may commit in parallel. */
  partitionLocks?: boolean;
  /** Where the store keeps its data, e.g. `postgres schema billing_readmodels`; part of `storeId()`. */
  label: string;
}
export const shortHash = (value: string, length = 8) =>
  createHash("sha256").update(value).digest("hex").slice(0, length);
/**
 * A readable storage name: characters `unsafe` matches become `_`. A name longer than `max`, or
 * one `used` already holds, keeps a prefix and ends in a hash of `seed`. Adds the name to `used`.
 */
export function readableName(
  name: string,
  max: number,
  seed: string,
  used: Set<string>,
  unsafe = /[^A-Za-z0-9_-]/g,
): string {
  let result = name.replace(unsafe, "_");
  if (result.length > max || used.has(result))
    result = `${result.slice(0, max - 9)}_${shortHash(seed)}`;
  used.add(result);
  return result;
}
/** The generation's tag in its storage names: the first 8 hex characters of its id. */
export const generationTag = (generation: StoredGeneration) =>
  generation.id.replaceAll("-", "").slice(0, 8);
/** The record holding a store's id; group names cannot contain `#`. */
const storeRecord = "#store";
const streamPage = 100,
  findPage = 1000;
const assertReconstruction = (gen: StoredGeneration, expected?: string) => {
  if (expected !== undefined && gen.reconstruction !== expected)
    throw new Error(
      "Readmodel rebuild definition changed; discard the pending generation and rebuild",
    );
};
const setEpoch = async (
  tx: ReadmodelTransaction,
  g: StoredGroup,
  part: StoredPartition,
  epoch: number,
) => {
  if (part.name.endsWith("#0")) {
    g.epoch = epoch;
    await tx.putGroup(g);
  } else part.epoch = epoch;
  // Batches write only their Partition's record; writing it here makes Mongo abort a stale one in flight.
  await putPartition(tx, g, part);
};
/** Fences of Partition 0 write the group's epoch, so they take the whole group's lock. */
const fenceLock = (partition?: number) => (partition ? partition : undefined);
export function durableReadmodelStore<TNative = unknown>(
  backend: ReadmodelBackend,
): ReadmodelStore<TNative> {
  const use = async <T>(
    group: string,
    generation: string | undefined,
    fn: (
      tx: ReadmodelTransaction,
      g: StoredGroup,
      gen: StoredGeneration,
      part?: StoredPartition,
    ) => Promise<T>,
    {
      record,
      ...options
    }: {
      readOnly?: boolean;
      schemaChange?: boolean;
      partition?: number | string;
      /** Loads this Partition's record. */
      record?: number;
    } = {},
  ): Promise<T> =>
    backend.transaction(
      group,
      async (tx) => {
        const g = await tx.getGroup(group);
        if (!g) throw new Error(`Unknown Readmodel group ${group}`);
        const gen = await tx.getGeneration(group, generation ?? g.active);
        if (!gen) throw new Error("Unknown Readmodel generation");
        // Registration migrates the old format, so it reappearing means an earlier version still writes.
        if (
          !options.readOnly &&
          ((g as Legacy).epochs ||
            (gen.id === g.active && (gen as Legacy).checkpoints))
        )
          throw new Error(
            `Readmodel group ${group} has Partition metadata written by an earlier EventFabric version; stop every instance of it before running this one`,
          );
        return fn(
          tx,
          g,
          gen,
          record === undefined
            ? undefined
            : await partitionOf(tx, group, record),
        );
      },
      options,
    );
  /*
   * Reads of a named generation (a fold's) reuse its record, skipping the group and generation lookups:
   * its names are fixed at creation and its definitions change only through registration's migration
   * or a drop, both cleared here. After another instance drops it, the fold's commit fails; after it
   * migrates it, this instance keeps reading with the definitions it registered.
   */
  const generations = new Map<string, StoredGeneration>();
  /** Each group's active generation record, as the group record named it when it was read. */
  const actives = new Map<string, { key: string; gen: StoredGeneration }>();
  const forget = (group: string, generation: string) =>
    generations.delete(JSON.stringify([group, generation]));
  const read = <T>(
    group: string,
    generation: string | undefined,
    fn: (tx: ReadmodelTransaction, gen: StoredGeneration) => Promise<T>,
  ): Promise<T> => {
    if (generation === undefined)
      return backend.transaction(
        group,
        async (tx) => {
          const g = await tx.getGroup(group);
          if (!g) throw new Error(`Unknown Readmodel group ${group}`);
          // A swap changes `active`; a registration, and with it any in-place migration, writes `hash`.
          const key = JSON.stringify([g.active, g.hash]);
          let known = actives.get(group);
          if (known?.key !== key) {
            const gen = await tx.getGeneration(group, g.active);
            if (!gen) throw new Error("Unknown Readmodel generation");
            actives.set(group, (known = { key, gen }));
          }
          return fn(tx, known.gen);
        },
        { readOnly: true },
      );
    const key = JSON.stringify([group, generation]);
    return backend.transaction(
      group,
      async (tx) => {
        let gen = generations.get(key);
        if (!gen) {
          gen = (await tx.getGeneration(group, generation)) ?? undefined;
          if (!gen) throw new Error("Unknown Readmodel generation");
          generations.set(key, gen);
        }
        return fn(tx, gen);
      },
      { readOnly: true },
    );
  };
  /** Normalizes and validates `changes` against the generation's schemas, then writes them model by model. */
  const write = async (
    tx: ReadmodelTransaction,
    name: string,
    g: StoredGroup,
    gen: StoredGeneration,
    changes: ReadmodelChange[],
  ) => {
    const prepared = await Promise.all(
      changes.map(async (c) => {
        const definition = gen.definitions[c.model];
        if (!definition) throw new Error(`Unknown Readmodel ${c.model}`);
        return {
          ...c,
          document:
            c.document === null
              ? null
              : await normalizeDocument(
                  definition.schema as any,
                  c.document,
                  c.id,
                  g.validate,
                  validationPorts.get(name),
                ),
        };
      }),
    );
    const byModel = new Map<string, [string, JsonObject | null][]>();
    for (const c of prepared) {
      let documents = byModel.get(c.model);
      if (!documents) byModel.set(c.model, (documents = []));
      documents.push([c.id, c.document]);
    }
    for (const [model, documents] of byModel)
      await tx.write(gen, model, documents);
  };
  const definitionFor = (
    gen: StoredGeneration,
    model: string,
    options: QueryOptions,
  ) => {
    const definition = gen.definitions[model];
    if (!definition) throw new Error(`Unknown Readmodel ${model}`);
    if (options.q && !textFields(definition).length)
      throw new Error("Readmodel has no declared text index");
    return definition;
  };
  const paging = ({ from = 0, size = Infinity }: QueryOptions) => {
    if (
      !Number.isInteger(from) ||
      from < 0 ||
      !(size === Infinity || Number.isInteger(size)) ||
      size < 0
    )
      throw new Error("Readmodel paging requires non-negative integers");
    return { from, size };
  };
  /**
   * The matches in `options` order, after `from` and up to `size`. Pages through the store's
   * ordered candidates, so memory holds one page, not every match.
   */
  async function* matching(
    tx: ReadmodelTransaction,
    gen: StoredGeneration,
    model: string,
    filter: Filter,
    options: QueryOptions,
    pageSize: number,
  ): AsyncGenerator<JsonObject> {
    const { from, size } = paging(options);
    const definition = definitionFor(gen, model, options);
    const fields = textFields(definition);
    // Keyset order: the requested sort, then _id, matching queryDocuments' tie-break.
    const order: [string, 1 | -1][] = [];
    for (const [path, direction] of Object.entries(options.sort ?? {})) {
      order.push([path, direction]);
      if (path === "_id") break;
    }
    if (order.at(-1)?.[0] !== "_id") order.push(["_id", 1]);
    // shortcut: text ranking and non-scalar sorts materialize matches; page them when these queries get large.
    if (
      (options.q && !options.sort) ||
      !order.every(
        ([path]) =>
          path === "_id" || scalarPath(definition.schema as any, path),
      )
    ) {
      yield* queryDocuments(
        await tx.read(gen, model, filter, options),
        filter,
        options,
        fields,
      );
      return;
    }
    // The store selected exactly the matches, so only a copy remains, and only for a store that shares its documents.
    const exact =
      !options.q &&
      exactFilter(filter, definition.schema as any, {
        jsonEquality: tx.jsonEquality,
      });
    let after: any[] | undefined,
      skip = from,
      left = size;
    while (left > 0) {
      const page = await tx.page(gen, model, filter, order, after, pageSize);
      for (const doc of exact
        ? tx.ownsDocuments
          ? page
          : copyDocument(page)
        : queryDocuments(page, filter, { q: options.q }, fields)) {
        if (skip > 0) skip--;
        else if (left-- > 0) yield doc;
        else return;
      }
      if (page.length < pageSize) return;
      const last = page.at(-1)!;
      after = order.map(
        ([path]) => pathValues(last, path.split("."))[0] ?? null,
      );
    }
  }
  const validationPorts = new Map<string, Validate>();
  const store: ReadmodelStore<TNative> = {
    async register(name, definitions, options = {}) {
      if (name.includes("#"))
        throw new Error(`Readmodel group name ${name} must not contain "#"`);
      const prepared = prepareDefinitions(definitions, options);
      if (options.validator) validationPorts.set(name, options.validator);
      else validationPorts.delete(name);
      return backend.transaction(
        name,
        async (tx) => {
          let g = await tx.getGroup(name);
          if (!g) {
            g = {
              name,
              epoch: 0,
              active: randomUUID(),
              desired: prepared.definitions,
              hash: prepared.hash,
              validate: options.validate !== false,
            };
            const gen = {
              group: name,
              id: g.active,
              definitions: prepared.definitions,
              hash: prepared.hash,
              checkpoint: null,
            };
            await tx.create(gen);
            await tx.putGeneration(gen);
          }
          const gen = (await tx.getGeneration(name, g.active))!;
          await migrateLegacy(tx, g, gen);
          if (gen.hash !== prepared.hash) {
            const migration = options.migrations?.find(
              (m) => m.from === gen.hash && m.to === prepared.hash,
            );
            if (migration) {
              await migration.up(tx.native(gen));
              gen.hash = prepared.hash;
              gen.definitions = prepared.definitions;
              await tx.putGeneration(gen);
              forget(name, gen.id);
            }
          }
          g.desired = prepared.definitions;
          g.hash = prepared.hash;
          g.validate = options.validate !== false;
          await tx.putGroup(g);
          return {
            hash: prepared.hash,
            rebuildRequired: gen.hash !== prepared.hash,
          };
        },
        { schemaChange: true },
      );
    },
    async fence(name, epoch, partition) {
      await use(
        name,
        undefined,
        async (tx, g, _gen, part) => {
          if (!Number.isSafeInteger(epoch) || epoch < epochOf(g, part))
            throw new StaleEpochError();
          await setEpoch(tx, g, part!, epoch);
        },
        { record: partition ?? 0, partition: fenceLock(partition) },
      );
    },
    async prepareSkip(name, epoch, position, partition) {
      await use(
        name,
        undefined,
        async (tx, g, gen, part) => {
          const current = epochOf(g, part);
          if (current !== epoch && current !== epoch + 1)
            throw new StaleEpochError();
          const checkpoint = checkpointOf(gen, part);
          if (checkpoint && comparePositions(checkpoint, position) >= 0)
            throw new Error("Readmodel already processed that Event");
          await setEpoch(tx, g, part!, epoch + 1);
        },
        { record: partition ?? 0, partition: fenceLock(partition) },
      );
    },
    async checkpoint(name, options) {
      return use(
        name,
        options?.generation,
        async (tx, _g, gen) => {
          if (!options?.partitions)
            return options?.partition === undefined
              ? gen.checkpoint
              : checkpointOf(
                  gen,
                  await partitionOf(tx, name, options.partition),
                );
          const records = new Map(
            (await tx.listPartitions(name)).map((r) => [r.name, r]),
          );
          let lowest: Position | null = null;
          for (let p = 0; p < options.partitions; p++) {
            const checkpoint = checkpointOf(gen, records.get(`${name}#${p}`));
            if (!checkpoint) return null;
            if (!lowest || comparePositions(checkpoint, lowest) < 0)
              lowest = checkpoint;
          }
          return lowest;
        },
        { readOnly: true },
      );
    },
    async load(name, epoch, generation, changes, options) {
      await use(name, generation, async (tx, g, gen) => {
        if (gen.id === g.active || gen.checkpoint)
          throw new Error(
            "A Readmodel import loads documents only into a pending generation without a Checkpoint",
          );
        assertReconstruction(gen, options?.reconstruction);
        if (epochOf(g) !== epoch) throw new StaleEpochError();
        await write(tx, name, g, gen, changes);
      });
    },
    async batch(name, epoch, position, changes, options) {
      const partition = options?.partition,
        slice = options?.slice;
      if (
        slice !== undefined &&
        (!backend.partitionLocks || !options?.generation)
      )
        throw new Error(
          "Rebuild slices need a generation and a store with Partition locks",
        );
      await use(
        name,
        options?.generation,
        async (tx, g, gen, part) => {
          if (options?.requireActive && gen.id !== g.active)
            throw new StaleGenerationError();
          assertReconstruction(gen, options?.reconstruction);
          if (epochOf(g, part) !== epoch) throw new StaleEpochError();
          const sliced =
            slice === undefined
              ? undefined
              : ((await tx.getGroup(
                  sliceName(name, gen.id, slice),
                )) as unknown as StoredSlice | null);
          if (sliced === null)
            throw new Error(`Unknown rebuild slice ${slice}`);
          const checkpoint = sliced
            ? sliced.checkpoint
            : checkpointOf(gen, part);
          if (checkpoint && comparePositions(position, checkpoint) <= 0) return;
          await write(tx, name, g, gen, changes);
          if (sliced) {
            sliced.checkpoint = position;
            await tx.putGroup(sliced as unknown as StoredGroup);
            return;
          }
          if (part) {
            Object.assign(part, {
              generation: gen.id,
              version: gen.version ?? 0,
              checkpoint: position,
            });
            await putPartition(tx, g, part);
            return;
          }
          gen.checkpoint = position;
          await tx.putGeneration(gen);
          g.commitToken = randomUUID();
          await tx.putGroup(g);
        },
        slice === undefined
          ? { record: partition, partition }
          : { partition: `${options!.generation}@${slice}` },
      );
    },
    ...(backend.partitionLocks && {
      async slices(name, generation, count) {
        return use(
          name,
          generation,
          async (tx, _g, gen) => {
            const record = async (slice: number) =>
              (await tx.getGroup(
                sliceName(name, gen.id, slice),
              )) as unknown as StoredSlice | null;
            const first = await record(0);
            if (!first) {
              if (count === undefined) return null;
              if (gen.checkpoint)
                throw new Error(
                  "Only a rebuild without a Checkpoint can start slices",
                );
              // shortcut: slice records outlive their generation; a few rows per sliced rebuild.
              for (let slice = 0; slice < count; slice++)
                await tx.putGroup({
                  name: sliceName(name, gen.id, slice),
                  count,
                } as unknown as StoredGroup);
              return Array<Position | null>(count).fill(null);
            }
            if (count !== undefined && first.count !== count)
              throw new Error(
                `This rebuild runs in ${first.count} slices; discard the pending generation to rebuild with ${count}`,
              );
            const records = [first];
            for (let slice = 1; slice < first.count; slice++)
              records.push((await record(slice))!);
            return records.map((stored) => stored.checkpoint ?? null);
          },
          count === undefined ? { readOnly: true } : {},
        );
      },
    }),
    async resetPartitions(name, epochs, position) {
      await use(name, undefined, async (tx, g, gen) => {
        const parts = [];
        for (const [p, epoch] of epochs.entries()) {
          const part = await partitionOf(tx, name, p);
          if (epochOf(g, part) !== epoch) throw new StaleEpochError();
          parts.push(part);
        }
        gen.checkpoint = position;
        gen.version = (gen.version ?? 0) + 1;
        await tx.putGeneration(gen);
        g.commitToken = randomUUID();
        await tx.putGroup(g);
        for (const part of parts) await putPartition(tx, g, part);
      });
    },
    async find(name, model, filter = {}, options = {}) {
      return read(name, options.generation, async (tx, gen) => {
        const page = Math.min(
          findPage,
          Math.max(
            streamPage,
            (options.from ?? 0) + (options.size ?? findPage),
          ),
        );
        const found: JsonObject[] = [];
        for await (const doc of matching(tx, gen, model, filter, options, page))
          found.push(doc);
        return found;
      });
    },
    async findOne(name, model, filter = {}, options = {}) {
      return (
        (await store.find(name, model, filter, { ...options, size: 1 }))[0] ??
        null
      );
    },
    async count(name, model, filter = {}, options = {}) {
      return read(name, options.generation, async (tx, gen) => {
        const definition = definitionFor(gen, model, options);
        if (
          !options.q &&
          tx.count &&
          exactFilter(filter, definition.schema as any, { jsonEquality: tx.jsonEquality })
        )
          return tx.count(gen, model, filter);
        let count = 0;
        for await (const _ of matching(
          tx,
          gen,
          model,
          filter,
          { q: options.q },
          findPage,
        ))
          count++;
        return count;
      });
    },
    async *stream(name, model, filter = {}, options = {}) {
      paging(options);
      if (options.size === 0) return;
      const output = new PassThrough({ objectMode: true, highWaterMark: 1 });
      let failure: any;
      const work = read(name, options.generation, async (tx, gen) => {
        // Backpressure keeps one page in memory while the transaction holds one snapshot.
        await pipeline(
          Readable.from(matching(tx, gen, model, filter, options, streamPage), {
            highWaterMark: 1,
          }),
          output,
        );
      }).catch((error) => {
        failure = error;
        output.destroy(error);
      });
      try {
        yield* output;
      } finally {
        const cancelled = !output.readableEnded;
        output.destroy();
        await work;
        if (
          failure &&
          !(
            cancelled &&
            ["ERR_STREAM_PREMATURE_CLOSE", "ABORT_ERR"].includes(failure.code)
          )
        )
          throw failure;
      }
    },
    async pendingGeneration(name) {
      return use(name, undefined, async (_tx, g) => g.pending ?? null, {
        readOnly: true,
      });
    },
    async activeGeneration(name) {
      return use(name, undefined, async (_tx, g) => g.active, {
        readOnly: true,
      });
    },
    async createGeneration(name, reconstruction) {
      return use(name, undefined, async (tx, g) => {
        const gen = {
          group: name,
          id: randomUUID(),
          definitions: g.desired,
          hash: g.hash,
          checkpoint: null,
          reconstruction,
        };
        await tx.create(gen);
        await tx.putGeneration(gen);
        g.pending = gen.id;
        await tx.putGroup(g);
        return gen.id;
      });
    },
    async swapGeneration(name, id, minimum, reconstruction) {
      await use(name, id, async (tx, g, gen) => {
        assertReconstruction(gen, reconstruction);
        if (gen.hash !== g.hash)
          throw new Error(
            "Readmodel rebuild definition changed; discard the pending generation and rebuild",
          );
        const live = (await tx.getGeneration(name, g.active))!;
        const parts = await tx.listPartitions(name);
        // The shadow must cover the furthest Partition of the live generation, not only its Checkpoint.
        let floor: Position | null = null;
        for (const position of [
          minimum ?? null,
          live.checkpoint,
          ...parts.map((part) => checkpointOf(live, part)),
        ])
          if (position && (!floor || comparePositions(position, floor) > 0))
            floor = position;
        if (
          floor &&
          (!gen.checkpoint || comparePositions(gen.checkpoint, floor) < 0)
        )
          throw new GenerationBehindError();
        g.active = id;
        if (g.pending === id) delete g.pending;
        await tx.putGroup(g);
        // On Mongo, every Partition's batch in flight now conflicts with the swap.
        for (const part of parts) await putPartition(tx, g, part);
      });
    },
    async dropGeneration(name, id) {
      await use(
        name,
        id,
        async (tx, g, gen) => {
          if (g.active === id)
            throw new Error("Cannot drop active Readmodel generation");
          await tx.drop(gen);
          forget(name, id);
          if (g.pending === id) {
            delete g.pending;
            await tx.putGroup(g);
          }
        },
        { schemaChange: true },
      );
    },
    async native(name, model, scope, options) {
      const joins: NativeJoins = {
        ...options,
        generation: (group) =>
          use(group, undefined, async (_tx, _g, gen) => gen, {
            readOnly: true,
          }),
      };
      return use(
        name,
        undefined,
        async (tx, _g, gen) => {
          if (!scope) return tx.native(gen, model, true, joins);
          // A scope the native predicate cannot select exactly would leak or hide documents.
          if (
            !tx.scopedNative ||
            !exactFilter(scope, gen.definitions[model]!.schema as any, { jsonEquality: tx.jsonEquality })
          )
            throw new ForbiddenError();
          // A scoped handle joins only where the caller says which documents of the joined model it may see.
          return tx.scopedNative(
            gen,
            model,
            scope,
            options?.joinScope && joins,
          );
        },
        { readOnly: true },
      );
    },
    async storeId() {
      const id = await backend.transaction(storeRecord, async (tx) => {
        const stored = (await tx.getGroup(storeRecord)) as unknown as {
          id: string;
        } | null;
        if (stored) return stored.id;
        const created = { name: storeRecord, id: randomUUID() };
        await tx.putGroup(created as unknown as StoredGroup);
        return created.id;
      });
      return `${backend.label} (${id.slice(0, 8)})`;
    },
    close: () => backend.close(),
  };
  return store;
}
