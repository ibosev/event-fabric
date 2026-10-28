import { Readable } from "node:stream";
import type { MongoClient, Collection, ClientSession, Db } from "mongodb";
import type { JsonObject } from "../types.js";
import { ForbiddenError } from "../errors.js";
import {
  definitionsByHash,
  durableReadmodelStore,
  generationTag,
  readableName,
  shortHash,
} from "../readmodel-store/durable.js";
import type {
  NativeJoins,
  ReadmodelTransaction,
  StoredGeneration,
} from "../readmodel-store/durable.js";
import { exactFilter, mongoSelector } from "../readmodel-store/query.js";
import { lazyCursor } from "../internal.js";
import { normalizeDocument } from "../readmodel-store/schema.js";
import type { ReadmodelStore } from "../readmodel-store/types.js";
export interface MongoReadmodelStore extends ReadmodelStore<
  Collection<JsonObject>
> {
  native(group: string, model: string): Promise<Collection<JsonObject>>;
}
export type MongoReadmodelOptions = {
  client: MongoClient;
  /** Holds the group and generation records; each group's documents live in `<database>-<group>-readmodels`. */
  database: string;
  prefix?: string;
  /**
   * Groups that share a database: group → name, kept in `<database>-<name>-readmodels` (default: the group's own
   * name). A native pipeline's `$lookup`, `$graphLookup` and `$unionWith` name a model of its group, or of
   * another group in the same database, as the collection. A changed name applies from the group's next generation.
   */
  databases?: Record<string, string>;
};
/** Mongo's snapshot history window has passed: the read's snapshot is no longer available. */
const SnapshotTooOld = 239;
// Position of the options argument for driver methods a migration is likely to call.
const optionsAt: Record<string, number> = {
  find: 1,
  findOne: 1,
  countDocuments: 1,
  aggregate: 1,
  distinct: 2,
  insertOne: 1,
  insertMany: 1,
  updateOne: 2,
  updateMany: 2,
  replaceOne: 2,
  deleteOne: 1,
  deleteMany: 1,
  bulkWrite: 1,
  findOneAndUpdate: 2,
  findOneAndReplace: 2,
  findOneAndDelete: 1,
  createIndex: 1,
  createIndexes: 1,
  dropIndex: 1,
  dropIndexes: 0,
  createCollection: 1,
  command: 1,
};
// Stages that read or write other collections, or the server's state, would escape a scope.
const escaping = new Set([
  "$lookup",
  "$graphLookup",
  "$unionWith",
  "$out",
  "$merge",
  "$documents",
  "$collStats",
  "$indexStats",
  "$currentOp",
  "$listSessions",
  "$listLocalSessions",
  "$changeStream",
  "$planCacheStats",
]);
// Join stages and the field naming the joined collection.
const joinFields: Record<string, string> = {
  $lookup: "from",
  $graphLookup: "from",
  $unionWith: "coll",
};
const checkPipeline = (pipeline: unknown[], joins = false) => {
  for (const stage of pipeline)
    for (const [name, value] of Object.entries(stage ?? {})) {
      if (escaping.has(name) && !(joins && name in joinFields))
        throw new ForbiddenError();
      if (name === "$facet")
        for (const inner of Object.values(value ?? {}))
          checkPipeline(inner as unknown[], joins);
    }
};
const hasJoin = (pipeline: unknown[]): boolean =>
  pipeline.some((stage) =>
    Object.entries(stage ?? {}).some(
      ([name, value]) =>
        name in joinFields ||
        (name === "$facet" &&
          Object.values(value ?? {}).some((inner) => hasJoin(inner as any))),
    ),
  );
/** A joined model's collection and the selector its documents are limited to. */
type JoinTarget = { name: string; selector: Record<string, any> };
const limited = (selector: Record<string, any>, inner?: unknown[]) =>
  Object.keys(selector).length
    ? [{ $match: selector }, ...(inner ?? [])]
    : inner;
/** `pipeline` with each join stage naming its model's collection and limited to the caller's documents of it. */
const joinedPipeline = (
  pipeline: any[],
  target: (name: string, foreignField?: string) => Promise<JoinTarget>,
): Promise<any[]> =>
  Promise.all(
    pipeline.map(async (stage) => {
      const [name] = Object.keys(stage ?? {});
      const value = stage?.[name!];
      if (name === "$facet")
        return {
          $facet: Object.fromEntries(
            await Promise.all(
              Object.entries(value ?? {}).map(async ([key, inner]) => [
                key,
                await joinedPipeline(inner as any[], target),
              ]),
            ),
          ),
        };
      if (name === "$lookup" || name === "$unionWith") {
        const join = typeof value === "string" ? { coll: value } : value;
        const field = joinFields[name]!;
        const { name: collection, selector } = await target(
          join[field],
          join.foreignField,
        );
        const pipeline = limited(
          selector,
          join.pipeline && (await joinedPipeline(join.pipeline, target)),
        );
        return {
          [name]: {
            ...join,
            [field]: collection,
            ...(pipeline && { pipeline }),
          },
        };
      }
      if (name === "$graphLookup") {
        const { name: from, selector } = await target(
          value.from,
          value.connectToField,
        );
        const restrict = value.restrictSearchWithMatch;
        return {
          $graphLookup: {
            ...value,
            from,
            ...(Object.keys(selector).length && {
              restrictSearchWithMatch: restrict
                ? { $and: [selector, restrict] }
                : selector,
            }),
          },
        };
      }
      return stage;
    }),
  );
const reportedJoins = new Set<string>();
// A join's field without an index scans the joined collection once per joined document.
const unindexed = (target: string, field: string) => {
  if (reportedJoins.has(`${target}.${field}`)) return;
  reportedJoins.add(`${target}.${field}`);
  process.emitWarning(
    `Readmodel ${target} has no index on ${field}, which a native pipeline joins on`,
    "EventFabricWarning",
  );
};
// Only these cursor methods reach a scoped cursor: the driver's cursor exposes its filter and pipeline.
const cursorMethods = [
  "toArray",
  "next",
  "tryNext",
  "hasNext",
  "close",
  "rewind",
  "forEach",
  "count",
  "explain",
  "limit",
  "skip",
  "sort",
  "project",
  "batchSize",
  "maxTimeMS",
  "comment",
  "hint",
  "collation",
  "allowDiskUse",
  "map",
  "match",
  "group",
  "unwind",
  "addStage",
  "clone",
];
const guarded = (cursor: any): any => {
  const wrapper: Record<string | symbol, unknown> = {
    [Symbol.asyncIterator]: () => cursor[Symbol.asyncIterator](),
    // The driver's stream holds the cursor.
    stream: () => Readable.from(cursor),
  };
  for (const name of cursorMethods)
    if (typeof cursor[name] === "function")
      wrapper[name] = (...args: unknown[]) => {
        if (name === "addStage") checkPipeline(args);
        const result = cursor[name](...args);
        if (result === cursor) return wrapper;
        return name === "clone" ? guarded(result) : result;
      };
  return Object.freeze(wrapper);
};
/** A read-only view of a collection that sees only the documents `selector` matches. */
function scopedCollection(
  collection: Collection<any>,
  selector: Record<string, any>,
  target?: (name: string, foreignField?: string) => Promise<JoinTarget>,
) {
  const within = (query: Record<string, any> = {}) => ({
    $and: [selector, query],
  });
  return {
    collectionName: collection.collectionName,
    find: (query?: Record<string, any>, options?: any) =>
      guarded(collection.find(within(query), options)),
    findOne: (query?: Record<string, any>, options?: any) =>
      collection.findOne(within(query), options),
    countDocuments: (query?: Record<string, any>, options?: any) =>
      collection.countDocuments(within(query), options),
    distinct: (key: string, query?: Record<string, any>, options?: any) =>
      collection.distinct(key, within(query), options ?? {}),
    aggregate: (pipeline: Record<string, any>[] = [], options?: any) => {
      checkPipeline(pipeline, !!target);
      if (!target || !hasJoin(pipeline))
        return guarded(
          collection.aggregate([{ $match: selector }, ...pipeline], options),
        );
      return lazyCursor(async () =>
        guarded(
          collection.aggregate(
            [{ $match: selector }, ...(await joinedPipeline(pipeline, target))],
            options,
          ),
        ),
      );
    },
  };
}
/** Injects the transaction's session into driver calls, so migration code that omits `session` still runs inside the transaction. */
function sessionBound<T extends object>(target: T, session: ClientSession): T {
  return new Proxy(target, {
    get(object, key, receiver) {
      const value = Reflect.get(object, key, receiver);
      if (typeof value !== "function" || typeof key !== "string") return value;
      if (key === "collection")
        return (...args: any[]) =>
          sessionBound(value.apply(object, args), session);
      const index = optionsAt[key];
      if (index === undefined) return value;
      return (...args: any[]) => {
        while (args.length < index) args.push(key === "aggregate" ? [] : {});
        args[index] = { ...args[index], session };
        return value.apply(object, args);
      };
    },
  });
}
export function createMongoReadmodelStore(
  options: MongoReadmodelOptions,
): MongoReadmodelStore {
  const db: Db = options.client.db(options.database),
    prefix = options.prefix ?? "event_fabric",
    groups: Collection<any> = db.collection(`${prefix}_groups`),
    generations: Collection<any> = db.collection(`${prefix}_generations`);
  // Older generations keep hashed names in `database`.
  const name = (gen: StoredGeneration, model: string) =>
    gen.names?.[model] ??
    `${prefix}_rm_${shortHash(`${gen.group}.${gen.id}.${model}`, 32)}`;
  // A store copied under another `database` keeps its original's names in each generation record.
  const own = `${options.database}-`
    .replace(/[^A-Za-z0-9_-]/g, "_")
    .slice(0, 54);
  const database = (gen: StoredGeneration) => {
    if (!gen.database) return db;
    if (!gen.database.startsWith(own))
      throw new Error(
        `Readmodel group ${gen.group} has its documents in database ${gen.database}, outside this store's database ${options.database}: repoint a copied store's generation records`,
      );
    return options.client.db(gen.database);
  };
  const databaseKey = (group: string) => options.databases?.[group] ?? group;
  /**
   * Resolves a join's collection name as the old stack did within one database: a model of the reading
   * group, else the one model of that name among the other groups in its database.
   */
  const joinTarget =
    (gen: StoredGeneration, joins: NativeJoins) =>
    async (name: string, field?: string): Promise<JoinTarget> => {
      const others = Object.keys(options.databases ?? {}).filter(
        (group) =>
          group !== gen.group && databaseKey(group) === databaseKey(gen.group),
      );
      let found: StoredGeneration | undefined = gen.definitions[name]
        ? gen
        : undefined;
      if (!found)
        for (const group of others) {
          const other = await joins.generation(group);
          if (!other.definitions[name]) continue;
          if (found)
            throw new Error(
              `Readmodel ${name} joined from ${gen.group} is in both ${found.group} and ${group}`,
            );
          found = other;
        }
      if (!found)
        throw new Error(
          `No Readmodel ${name} to join from ${gen.group} in its database`,
        );
      if (database(found).databaseName !== database(gen).databaseName)
        throw new Error(
          `Readmodel group ${found.group} is not in ${gen.group}'s database yet: rebuild it after changing \`databases\``,
        );
      const definition = found.definitions[name]!;
      const filter = (await joins.joinScope?.(found.group, name)) ?? {};
      if (
        Object.keys(filter).length &&
        !exactFilter(filter, definition.schema as any, { jsonEquality: true })
      )
        throw new ForbiddenError();
      if (
        field &&
        field !== "_id" &&
        !(definition.indexes ?? []).some(
          ({ fields }) =>
            (Array.isArray(fields) ? fields : Object.keys(fields))[0] === field,
        )
      )
        unindexed(`${found.group}.${name}`, field);
      return {
        name: collection(found, name).collectionName,
        selector: Object.keys(filter).length
          ? mongoSelector(filter, definition.schema as any)
          : {},
      };
    };
  /** The model's collection, its `aggregate` joining by model name. */
  const joining = (
    gen: StoredGeneration,
    c: Collection<any>,
    joins?: NativeJoins,
  ) =>
    joins
      ? new Proxy(c, {
          get(target, key) {
            const value = Reflect.get(target, key, target);
            if (key !== "aggregate")
              return typeof value === "function" ? value.bind(target) : value;
            return (pipeline: any[] = [], options?: any) =>
              hasJoin(pipeline)
                ? lazyCursor(async () =>
                    target.aggregate(
                      await joinedPipeline(pipeline, joinTarget(gen, joins)),
                      options,
                    ),
                  )
                : target.aggregate(pipeline, options);
          },
        })
      : c;
  const collection = (
    gen: StoredGeneration,
    model: string,
  ): Collection<any> => {
    if (!gen.definitions[model]) throw new Error(`Unknown Readmodel ${model}`);
    return database(gen).collection(name(gen, model));
  };
  /** `<database>-<group or its `databases` name>-readmodels`, and `<model>_<generation tag>` for each model. */
  const assignNames = (gen: StoredGeneration) => {
    // Mongo database names stay under 64 bytes; collections leave room for the namespace's 255.
    gen.database = readableName(
      `${options.database}-${databaseKey(gen.group)}-readmodels`,
      63,
      `${options.database}.${databaseKey(gen.group)}`,
      new Set(),
    );
    const used = new Set<string>();
    gen.names = Object.fromEntries(
      Object.keys(gen.definitions).map((model) => [
        model,
        `${readableName(model, 120, `${gen.group}.${gen.id}.${model}`, used)}_${generationTag(gen)}`,
      ]),
    );
  };
  const withDefinitions = definitionsByHash();
  let initialized: Promise<void> | undefined;
  const initialize = () =>
    (initialized ??= (async () => {
      await options.client.connect();
      const hello = await db.admin().command({ hello: 1 });
      if (!hello.setName)
        throw new Error("Mongo Readmodel store requires a replica set");
      for (const c of [groups, generations])
        try {
          await db.createCollection(c.collectionName);
        } catch (e: any) {
          if (e.code !== 48) throw e;
        }
    })());
  return durableReadmodelStore<Collection<JsonObject>>({
    label: `mongo database ${options.database}`,
    async transaction(group, fn, { readOnly = false } = {}) {
      await initialize();
      // Reads use a snapshot session, not a transaction: a transaction aborts after 60 s.
      const session: ClientSession = options.client.startSession(
        readOnly ? { snapshot: true } : {},
      );
      let drops: [database: Db, name: string][] = [],
        committed = false;
      try {
        const run = async () => {
          drops = [];
          const tx: ReadmodelTransaction = {
            jsonEquality: true,
            ownsDocuments: true,
            async getGroup(g) {
              const doc = await groups.findOne(
                { _id: g },
                { session, projection: { "value.desired": 0 } },
              );
              return withDefinitions(
                doc?.value ?? null,
                "desired",
                async () =>
                  (await groups.findOne({ _id: g }, { session }))?.value,
              );
            },
            async listPartitions(g) {
              const prefix = `${g}#`.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
              return (
                await groups
                  .find({ _id: { $regex: `^${prefix}` } }, { session })
                  .toArray()
              ).map((doc) => doc.value);
            },
            async putGroup(g) {
              await groups.replaceOne(
                { _id: g.name },
                { _id: g.name, value: g },
                { upsert: true, session },
              );
            },
            async getGeneration(g, id) {
              const doc = await generations.findOne(
                { _id: `${g}:${id}` },
                { session, projection: { "value.definitions": 0 } },
              );
              return withDefinitions(
                doc?.value ?? null,
                "definitions",
                async () =>
                  (
                    await generations.findOne(
                      { _id: `${g}:${id}` },
                      { session },
                    )
                  )?.value,
              );
            },
            async putGeneration(gen) {
              await generations.replaceOne(
                { _id: `${gen.group}:${gen.id}` },
                { _id: `${gen.group}:${gen.id}`, value: gen },
                { upsert: true, session },
              );
            },
            async create(gen) {
              assignNames(gen);
              for (const model of Object.keys(gen.definitions)) {
                const c = collection(gen, model);
                // Fails when the name is taken, rather than sharing another generation's collection.
                await database(gen).createCollection(c.collectionName, {
                  session,
                });
                for (const index of gen.definitions[model]!.indexes ?? []) {
                  const fields = Array.isArray(index.fields)
                    ? Object.fromEntries(
                        index.fields.map((f) => [f, index.text ? "text" : 1]),
                      )
                    : Object.fromEntries(
                        Object.entries(index.fields).map(([f, d]) => [
                          f,
                          index.text ? "text" : d,
                        ]),
                      );
                  await c.createIndex(fields as any, {
                    session,
                    ...(index.sparse === undefined
                      ? {}
                      : { sparse: index.sparse }),
                  });
                }
              }
            },
            async drop(gen) {
              // Deleting millions of documents in the transaction exhausts the WiredTiger cache.
              // shortcut: a crash between commit and the drops leaves the collections behind; they end in the generation's tag.
              for (const model of Object.keys(gen.definitions))
                drops.push([database(gen), name(gen, model)]);
              await generations.deleteOne(
                { _id: `${gen.group}:${gen.id}` },
                { session },
              );
            },
            async read(gen, model, filter = {}, options = {}) {
              const selector = mongoSelector(
                filter,
                gen.definitions[model]!.schema as any,
              );
              // Mongo text tokenization would drop portable substring matches.
              const docs = await collection(gen, model)
                .find(selector, { session })
                .toArray();
              return Promise.all(
                docs.map((doc) =>
                  normalizeDocument(
                    gen.definitions[model]!.schema as any,
                    doc,
                    String(doc._id),
                    false,
                    undefined,
                    false,
                  ),
                ),
              );
            },
            async page(gen, model, filter, order, after, limit) {
              const schema = gen.definitions[model]!.schema as any;
              // Ascending sorts put null first and descending sorts put it last, as compareValues does.
              const later = after?.flatMap((value, i) => {
                const [path, direction] = order[i]!;
                const strictly =
                  direction === 1
                    ? value === null
                      ? { [path]: { $ne: null } }
                      : { [path]: { $gt: value } }
                    : value === null
                      ? null
                      : {
                          $or: [{ [path]: { $lt: value } }, { [path]: null }],
                        };
                return strictly
                  ? [
                      {
                        $and: [
                          ...order
                            .slice(0, i)
                            .map(([p], j) => ({ [p]: after[j] })),
                          strictly,
                        ],
                      },
                    ]
                  : [];
              });
              const docs = await collection(gen, model)
                .find(
                  {
                    $and: [
                      mongoSelector(filter, schema),
                      later ? { $or: later } : {},
                    ],
                  },
                  { session },
                )
                .sort(Object.fromEntries(order))
                .limit(limit)
                .toArray();
              return Promise.all(
                docs.map((doc) =>
                  normalizeDocument(
                    schema,
                    doc,
                    String(doc._id),
                    false,
                    undefined,
                    false,
                  ),
                ),
              );
            },
            async count(gen, model, filter) {
              return collection(gen, model).countDocuments(
                mongoSelector(filter, gen.definitions[model]!.schema as any),
                { session },
              );
            },
            async write(gen, model, documents) {
              await collection(gen, model).bulkWrite(
                documents.map(([id, document]) =>
                  document === null
                    ? { deleteOne: { filter: { _id: id } } }
                    : {
                        replaceOne: {
                          filter: { _id: id },
                          replacement: document,
                          upsert: true,
                        },
                      },
                ),
                { session },
              );
            },
            scopedNative(gen, model, filter, joins) {
              return scopedCollection(
                collection(gen, model),
                mongoSelector(filter, gen.definitions[model]!.schema as any),
                joins && joinTarget(gen, joins),
              );
            },
            native(gen, model, _external, joins) {
              return model
                ? joining(gen, collection(gen, model), joins)
                : {
                    session,
                    collection: (m: string) =>
                      sessionBound(collection(gen, m), session),
                    database: sessionBound(database(gen), session),
                  };
            },
          };
          return fn(tx);
        };
        let result;
        if (readOnly)
          // Retrying after a stream has yielded would repeat documents to its caller.
          try {
            result = await run();
          } catch (error: any) {
            if (error?.code !== SnapshotTooOld) throw error;
            throw new Error(
              "Mongo Readmodel read outlived the server's snapshot history (minSnapshotHistoryWindowInSeconds, default 300 s); raise it for longer streams",
              { cause: error },
            );
          }
        else
          result = await session.withTransaction(run, {
            readConcern: { level: "local" },
            writeConcern: { w: "majority" },
          });
        committed = true;
        return result;
      } finally {
        await session.endSession();
        if (committed)
          for (const [database, collectionName] of drops)
            await database.dropCollection(collectionName);
      }
    },
    async close() {},
  });
}
