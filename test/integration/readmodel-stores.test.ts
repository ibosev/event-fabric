import { createHash } from "node:crypto";
import { Pool } from "pg";
import { MongoClient } from "mongodb";
import { afterAll, expect, test } from "vitest";
import { createPostgresReadmodelStore } from "../../src/adapters/postgres-readmodel.js";
import { createMongoReadmodelStore } from "../../src/adapters/mongo-readmodel.js";
import { createMemoryReadmodelStore } from "../../src/adapters/memory-readmodel.js";
import { dropGroupDatabases } from "./mongo.js";
import {
  ForbiddenError,
  GenerationBehindError,
  StaleEpochError,
  StaleGenerationError,
} from "../../src/errors.js";
const postgresUrl =
  process.env.EVENTFABRIC_POSTGRES_URL ??
  "postgres://eventfabric:eventfabric@127.0.0.1:55439/eventfabric";
const mongoUrl =
  process.env.EVENTFABRIC_MONGO_URL ??
  "mongodb://127.0.0.1:57019/?replicaSet=rs0&directConnection=true";
const pool = new Pool({ connectionString: postgresUrl });
const client = new MongoClient(mongoUrl);
afterAll(async () => {
  await dropGroupDatabases(client);
  await pool.end();
  await client.close();
});
const schema = {
  type: "object",
  properties: {
    name: { type: "string" },
    dates: { type: "array", items: { type: "string", format: "date-time" } },
    when: { type: ["string", "null"], format: "date-time" },
    owner: {
      type: ["object", "null"],
      properties: { account: { type: "string" } },
    },
    tags: { type: "array", items: { type: "string" } },
    items: {
      type: "array",
      items: {
        type: "object",
        properties: {
          sku: { type: "string" },
          cost: { type: "number" },
          dates: {
            type: "array",
            items: { type: "string", format: "date-time" },
          },
          parts: {
            type: "array",
            items: { type: "object", properties: { n: { type: "integer" } } },
          },
        },
      },
    },
    extra: { "x-store": "json", type: "object" },
  },
};
const p = (n: number) => ({ transactionId: String(n), sequence: "1" });
for (const adapter of ["memory", "postgres", "mongo"])
  test(`${adapter}: reconstruction guards survive adapter restart and fence both commits and publication`, async () => {
    const open = () =>
      adapter === "memory"
        ? createMemoryReadmodelStore()
        : adapter === "postgres"
          ? createPostgresReadmodelStore({ pool, schema: "readmodel_contract" })
          : createMongoReadmodelStore({
              client,
              database: "readmodel_contract",
            });
    const group = `reconstruction_${adapter}_${Date.now()}`,
      store = open();
    await store.register(group, {
      total: {
        schema: { type: "object", properties: { total: { type: "number" } } },
      },
    });
    await store.fence(group, 1);
    const active = await store.activeGeneration(group);
    const generation = await store.createGeneration(group, "definition-one");
    const options = { generation, reconstruction: "definition-one" };
    await store.batch(
      group,
      1,
      p(1),
      [{ model: "total", id: "one", document: { total: 10 } }],
      options,
    );
    const resumed = adapter === "memory" ? store : open();
    await expect(
      resumed.batch(group, 1, p(2), [], {
        ...options,
        reconstruction: "changed",
      }),
    ).rejects.toThrow(/rebuild definition changed/i);
    await expect(
      resumed.swapGeneration(group, generation, p(1), "changed"),
    ).rejects.toThrow(/rebuild definition changed/i);
    expect(await resumed.activeGeneration(group)).toBe(active);
    expect(await resumed.checkpoint(group, { generation })).toEqual(p(1));
    await resumed.register(group, {
      total: {
        schema: {
          type: "object",
          properties: { total: { type: "number" }, label: { type: "string" } },
        },
      },
    });
    await expect(
      resumed.swapGeneration(group, generation, p(1), "definition-one"),
    ).rejects.toThrow(/rebuild definition changed/i);
    await resumed.register(group, {
      total: {
        schema: { type: "object", properties: { total: { type: "number" } } },
      },
    });
    await resumed.batch(group, 1, p(2), [], options);
    await resumed.swapGeneration(group, generation, p(2), "definition-one");
    expect(await resumed.findOne(group, "total", { _id: "one" })).toEqual({
      _id: "one",
      total: 10,
    });
    await resumed.dropGeneration(group, active);
  });
for (const adapter of ["memory", "postgres", "mongo"])
  test(`${adapter}: shared Readmodel query, rollback, generation and schema contract`, async () => {
    const group = `contract_${adapter}_${Date.now()}`;
    const store =
      adapter === "memory"
        ? createMemoryReadmodelStore()
        : adapter === "postgres"
          ? createPostgresReadmodelStore({ pool, schema: "readmodel_contract" })
          : createMongoReadmodelStore({
              client,
              database: "readmodel_contract",
            });
    const registered = await store.register(group, {
      people: { schema, indexes: [{ fields: ["name"], text: true }] },
    });
    expect(registered.rebuildRequired).toBe(false);
    await store.fence(group, 1);
    const doc = {
      name: "Ada Lovelace",
      dates: [new Date("2026-01-02")],
      when: new Date("2026-01-02"),
      owner: { account: "a" },
      tags: ["a", "b"],
      items: [
        {
          sku: "book",
          cost: 12.5,
          dates: [new Date("2026-01-02")],
          parts: [{ n: 2 }, { n: 3 }],
        },
      ],
      extra: { z: 1, a: 2, dynamic: { foo: true } },
    };
    await store.batch(group, 1, p(1), [
      { model: "people", id: "one", document: doc },
    ]);
    expect(
      await store.findOne(group, "people", {
        tags: "a",
        items: { elemMatch: { cost: { gte: 12 } } },
      }),
    ).toEqual({ _id: "one", ...doc });
    expect(
      await store.count(group, "people", { "items.parts.n": { in: [3] } }),
    ).toBe(1);
    expect(
      await store.find(group, "people", {}, { q: "Lovelace" }),
    ).toHaveLength(1);
    expect(
      await store.count(group, "people", {
        extra: { dynamic: { foo: true }, a: 2, z: 1 },
      }),
    ).toBe(1);
    expect(
      await store.count(group, "people", {
        extra: { eq: { dynamic: { foo: true }, a: 2, z: 1 } },
      }),
    ).toBe(1);
    for (const [q, count] of [
      ["Love", 1],
      ['"Ada Love"', 1],
      ["Love -Grace", 1],
      ["Love -Ada", 0],
      ['Love -"Ada Love"', 0],
      ['Love -"Ada Grace"', 1],
    ] as const)
      expect(await store.find(group, "people", {}, { q }), q).toHaveLength(
        count,
      );
    for (const path of ["dates", "items.dates"])
      for (const condition of [
        "2026-01-02T00:00:00.000Z",
        { in: ["2026-01-02T00:00:00.000Z"] },
        { gt: "2026-01-01T00:00:00.000Z" },
      ])
        expect(
          await store.count(group, "people", { [path]: condition }),
          JSON.stringify({ [path]: condition }),
        ).toBe(1);
    await expect(
      store.batch(group, 1, p(2), [
        { model: "people", id: "one", document: { ...doc, name: "bad" } },
        { model: "people", id: "two", document: { unknown: true } },
      ]),
    ).rejects.toThrow();
    expect((await store.findOne(group, "people", { _id: "one" }))?.name).toBe(
      "Ada Lovelace",
    );
    expect(await store.checkpoint(group)).toEqual(p(1));
    await store.fence(group, 2);
    await expect(store.batch(group, 1, p(2), [])).rejects.toThrow(/epoch/i);
    const gen = await store.createGeneration(group);
    await store.batch(
      group,
      2,
      p(2),
      [{ model: "people", id: "two", document: { ...doc, name: "Grace" } }],
      { generation: gen },
    );
    expect(await store.count(group, "people")).toBe(1);
    expect(await store.findOne(group, "people", { _id: "two" })).toBeNull();
    await store.swapGeneration(group, gen);
    expect((await store.findOne(group, "people", {}))?._id).toBe("two");
    expect(await store.checkpoint(group)).toEqual(p(2));
    await store.register(group, { people: { schema } }, { validate: false });
    await expect(
      store.batch(group, 2, p(3), [
        { model: "people", id: "two", document: { oops: true } },
      ]),
    ).rejects.toThrow(/undeclared/i);
    await store.close();
  }, 30000);
for (const adapter of ["postgres", "mongo"])
  test(`${adapter}: concurrent owners cannot commit after a newer epoch or regress Checkpoint`, async () => {
    const group = `race_${adapter}_${Date.now()}`;
    const make = () =>
      adapter === "postgres"
        ? createPostgresReadmodelStore({ pool, schema: "readmodel_contract" })
        : createMongoReadmodelStore({ client, database: "readmodel_contract" });
    const a = make(),
      b = make();
    const definition = {
      m: { schema: { type: "object", properties: { n: { type: "integer" } } } },
    };
    await a.register(group, definition);
    await b.register(group, definition);
    await a.fence(group, 1);
    await Promise.all([
      a
        .batch(group, 1, p(1), [{ model: "m", id: "x", document: { n: 1 } }])
        .catch(() => {}),
      b.fence(group, 2),
    ]);
    await expect(
      a.batch(group, 1, p(3), [{ model: "m", id: "x", document: { n: 99 } }]),
    ).rejects.toThrow(/epoch/i);
    await b.batch(group, 2, p(2), [
      { model: "m", id: "x", document: { n: 2 } },
    ]);
    await b.batch(group, 2, p(1), [
      { model: "m", id: "x", document: { n: 1 } },
    ]);
    expect((await a.findOne(group, "m"))?.n).toBe(2);
    expect(await a.checkpoint(group)).toEqual(p(2));
    const oldHash = (await a.register(group, definition)).hash;
    const next = {
      m: {
        schema: {
          type: "object",
          properties: {
            n: { type: "integer" },
            label: { type: ["string", "null"] },
          },
        },
      },
    };
    const result = await a.register(group, next);
    expect(result.rebuildRequired).toBe(true);
    await expect(
      a.register(group, next, {
        migrations: [
          {
            from: oldHash,
            to: result.hash,
            async up() {
              throw new Error("migration failure");
            },
          },
        ],
      }),
    ).rejects.toThrow("migration failure");
    expect((await a.findOne(group, "m"))?.n).toBe(2);
    await a.close();
    await b.close();
  }, 30000);
for (const adapter of ["memory", "postgres", "mongo"])
  test(`${adapter}: each Partition has its own fence and Checkpoint, concurrent Partition batches keep both, and the group Checkpoint is the lowest`, async () => {
    const group = `partitions_${adapter}_${Date.now()}`;
    const store =
      adapter === "memory"
        ? createMemoryReadmodelStore()
        : adapter === "postgres"
          ? createPostgresReadmodelStore({ pool, schema: "readmodel_contract" })
          : createMongoReadmodelStore({
              client,
              database: "readmodel_contract",
            });
    await store.register(group, {
      m: { schema: { type: "object", properties: { n: { type: "integer" } } } },
    });
    await store.batch(group, 0, p(5), []);
    expect(await store.checkpoint(group, { partition: 1 })).toEqual(p(5));
    await store.fence(group, 3, 0);
    await store.fence(group, 1, 1);
    await expect(store.fence(group, 0, 1)).rejects.toThrow(StaleEpochError);
    await Promise.all(
      [0, 1].map(async (partition) => {
        const epoch = partition ? 1 : 3;
        for (let n = 6; n <= 15; n++)
          await store.batch(
            group,
            epoch,
            p(n * 2 + partition),
            [{ model: "m", id: `${partition}-${n}`, document: { n } }],
            { partition },
          );
      }),
    );
    expect(await store.checkpoint(group, { partition: 0 })).toEqual(p(30));
    expect(await store.checkpoint(group, { partition: 1 })).toEqual(p(31));
    expect(await store.checkpoint(group, { partitions: 2 })).toEqual(p(30));
    // Partition 2 never ran: it is still where the group was laid out.
    expect(await store.checkpoint(group, { partitions: 3 })).toEqual(p(5));
    expect(await store.count(group, "m")).toBe(20);
    await expect(
      store.batch(group, 1, p(40), [], { partition: 0 }),
    ).rejects.toThrow(StaleEpochError);
    // A batch without a Partition moves only the group's own Checkpoint.
    await store.batch(group, 3, p(45), []);
    expect(await store.checkpoint(group, { partition: 1 })).toEqual(p(31));
    // A reset needs every Partition's current fence, and moves every Partition.
    await expect(store.resetPartitions(group, [3, 0], p(50))).rejects.toThrow(
      StaleEpochError,
    );
    await store.resetPartitions(group, [3, 1], p(50));
    expect(await store.checkpoint(group, { partition: 1 })).toEqual(p(50));
    expect(await store.checkpoint(group, { partitions: 4 })).toEqual(p(50));
    await store.close();
  }, 30000);
test("Postgres native handle remains usable after its lookup transaction", async () => {
  const store = createPostgresReadmodelStore({
      pool,
      schema: "readmodel_contract",
    }),
    group = `native_${Date.now()}`;
  await store.register(group, {
    m: { schema: { type: "object", properties: { n: { type: "integer" } } } },
  });
  const native = await store.native(group, "m");
  expect(
    (await native.query(`SELECT count(*) FROM ${native.table}`)).rows[0].count,
  ).toBe("0");
  // Postgres cannot scope its native handle.
  await expect(store.native(group, "m", { n: { eq: 1 } })).rejects.toThrow(
    ForbiddenError,
  );
});
test("Mongo scoped native handle reads only its scope and refuses other collections", async () => {
  const store = createMongoReadmodelStore({
      client,
      database: "readmodel_contract",
    }),
    group = `scoped_${Date.now()}`;
  await store.register(group, { m: { schema } });
  await store.fence(group, 1);
  await store.batch(group, 1, p(1), [
    { model: "m", id: "a", document: { name: "a", owner: { account: "x" } } },
    { model: "m", id: "b", document: { name: "b", owner: { account: "y" } } },
  ]);
  const native = await store.native(group, "m", {
    "owner.account": { eq: "x" },
  });
  expect(await native.find({}).sort({ _id: 1 }).toArray()).toMatchObject([
    { _id: "a" },
  ]);
  expect(await native.findOne({ name: "b" })).toBeNull();
  expect(await native.countDocuments()).toBe(1);
  expect(await native.distinct("name")).toEqual(["a"]);
  expect(
    await native
      .aggregate([{ $group: { _id: null, n: { $sum: 1 } } }])
      .toArray(),
  ).toEqual([{ _id: null, n: 1 }]);
  expect(native.insertOne).toBeUndefined();
  expect(() =>
    native.aggregate([
      { $facet: { other: [{ $unionWith: { coll: "other" } }] } },
    ]),
  ).toThrow(ForbiddenError);
  // Cursors cannot replace the scope or add a stage that leaves the collection.
  expect(native.find({}).filter).toBeUndefined();
  expect(native.find({}).clone().filter).toBeUndefined();
  expect(native.aggregate().lookup).toBeUndefined();
  expect(native.aggregate().pipeline).toBeUndefined();
  expect(await native.find({}).stream().toArray()).toHaveLength(1);
  expect(() =>
    native.aggregate().addStage({ $unionWith: { coll: "other" } }),
  ).toThrow(ForbiddenError);
  expect(
    await native.aggregate().match({ name: "b" }).limit(1).toArray(),
  ).toEqual([]);
  // A scope the native predicate cannot select exactly (array membership) is refused.
  await expect(store.native(group, "m", { tags: { eq: "x" } })).rejects.toThrow(
    ForbiddenError,
  );
  // A json field holding an id or a list of ids: Mongo's equality selects what `matches` does.
  const owners = `${group}_json`;
  await store.register(owners, {
    m: {
      schema: {
        type: "object",
        properties: {
          owner: { type: "object", properties: { account: { "x-store": "json" } } },
        },
      },
    },
  });
  await store.fence(owners, 1);
  await store.batch(owners, 1, p(1), [
    { model: "m", id: "a", document: { owner: { account: "x" } } },
    { model: "m", id: "b", document: { owner: { account: ["y", "x"] } } },
    { model: "m", id: "c", document: { owner: { account: ["y", null] } } },
  ]);
  const scoped = await store.native(owners, "m", { "owner.account": { eq: "x" } });
  expect(await scoped.find({}).sort({ _id: 1 }).toArray()).toMatchObject([
    { _id: "a" },
    { _id: "b" },
  ]);
  expect(await store.count(owners, "m", { "owner.account": "x" })).toBe(2);
  // A date-like string would equal a stored Date in `matches` only.
  await expect(
    store.native(owners, "m", { "owner.account": { eq: "2026-01-02" } }),
  ).rejects.toThrow(ForbiddenError);
});
test("Mongo native pipelines join models of groups that share a database by model name, within the caller's scope", async () => {
  const suffix = Date.now(),
    files = `files_${suffix}`,
    balances = `balances_${suffix}`,
    alone = `alone_${suffix}`;
  const store = createMongoReadmodelStore({
    client,
    database: "readmodel_contract",
    databases: { [files]: `shared_${suffix}`, [balances]: `shared_${suffix}` },
  });
  const owned = {
    type: "object",
    properties: {
      name: { type: "string" },
      file: { type: ["string", "null"] },
      owner: { type: "object", properties: { account: { type: "string" } } },
    },
  };
  await store.register(files, { file: { schema: owned } });
  await store.register(balances, {
    balance: { schema: owned, indexes: [{ fields: ["file"] }] },
    note: { schema: owned },
  });
  await store.register(alone, { other: { schema: owned } });
  for (const group of [files, balances]) await store.fence(group, 1);
  await store.batch(files, 1, p(1), [
    { model: "file", id: "f", document: { name: "f", owner: { account: "x" } } },
  ]);
  await store.batch(balances, 1, p(1), [
    { model: "balance", id: "b1", document: { name: "b1", file: "f", owner: { account: "x" } } },
    { model: "balance", id: "b2", document: { name: "b2", file: "f", owner: { account: "y" } } },
    { model: "note", id: "n", document: { name: "n", file: "f", owner: { account: "x" } } },
  ]);
  const lookup = (from: string, foreignField = "file") => [
    { $lookup: { from, localField: "_id", foreignField, as: "joined" } },
    { $project: { names: "$joined.name" } },
  ];
  // As the old stack read within one database: the model name is the collection.
  const whole = await store.native(files, "file");
  expect(await whole.aggregate(lookup("balance")).toArray()).toEqual([
    { _id: "f", names: ["b1", "b2"] },
  ]);
  // The caller's rule for the joined model limits it; a model without a rule is refused.
  const joinScope = async (_group: string, model: string) => {
    if (model === "note") throw new ForbiddenError();
    return { "owner.account": { eq: "x" } };
  };
  const scoped = await store.native(files, "file", { "owner.account": { eq: "x" } }, { joinScope });
  expect(await scoped.aggregate(lookup("balance")).toArray()).toEqual([
    { _id: "f", names: ["b1"] },
  ]);
  await expect(scoped.aggregate(lookup("note")).toArray()).rejects.toThrow(ForbiddenError);
  const unscoped = await store.native(files, "file", undefined, { joinScope });
  expect(await unscoped.aggregate(lookup("balance")).toArray()).toEqual([
    { _id: "f", names: ["b1"] },
  ]);
  // Models outside the group's database, and unknown names, are not joined.
  await expect(whole.aggregate(lookup("other")).toArray()).rejects.toThrow("No Readmodel other");
  // A join on a field without an index is reported once.
  const warnings: string[] = [];
  const listen = (warning: Error) => warnings.push(warning.message);
  process.on("warning", listen);
  try {
    await whole.aggregate(lookup("note")).toArray();
    await new Promise((resolve) => setImmediate(resolve));
  } finally {
    process.off("warning", listen);
  }
  expect(warnings).toEqual([
    `Readmodel ${balances}.note has no index on file, which a native pipeline joins on`,
  ]);
});
for (const adapter of ["postgres", "mongo"])
  test(`${adapter}: failed schema migration rolls back writes and successful migration updates the hash`, async () => {
    const store =
        adapter === "postgres"
          ? createPostgresReadmodelStore({ pool, schema: "readmodel_contract" })
          : createMongoReadmodelStore({
              client,
              database: "readmodel_contract",
            }),
      group = `migration_${adapter}_${Date.now()}`;
    const original = {
      m: { schema: { type: "object", properties: { n: { type: "integer" } } } },
    };
    const oldHash = (await store.register(group, original)).hash;
    await store.fence(group, 1);
    await store.batch(group, 1, p(1), [
      { model: "m", id: "x", document: { n: 1 } },
    ]);
    const next = {
      m: {
        schema: {
          type: "object",
          properties: {
            n: { type: "integer" },
            label: { type: ["string", "null"] },
          },
        },
      },
    };
    const nextHash = (await store.register(group, next)).hash;
    await expect(
      store.register(group, next, {
        migrations: [
          {
            from: oldHash,
            to: nextHash,
            async up(native) {
              if (adapter === "postgres")
                await native.query(`UPDATE ${native.tableFor("m")} SET n=99`);
              else
                await native
                  .collection("m")
                  .updateOne(
                    { _id: "x" },
                    { $set: { n: 99 } },
                    { session: native.session },
                  );
              throw new Error("abort migration");
            },
          },
        ],
      }),
    ).rejects.toThrow("abort migration");
    expect((await store.findOne(group, "m"))?.n).toBe(1);
    // A read of a named generation keeps its record; the migration and the drop below must replace it.
    const old = await store.activeGeneration(group);
    expect((await store.findOne(group, "m", {}, { generation: old }))?.n).toBe(
      1,
    );
    expect(
      (
        await store.register(group, next, {
          migrations: [
            {
              from: oldHash,
              to: nextHash,
              async up(native) {
                if (adapter === "postgres")
                  await native.query(
                    `ALTER TABLE ${native.tableFor("m")} ADD COLUMN label text`,
                  );
                else
                  await native
                    .collection("m")
                    .updateMany(
                      {},
                      { $set: { label: null } },
                      { session: native.session },
                    );
              },
            },
          ],
        })
      ).rebuildRequired,
    ).toBe(false);
    expect(await store.findOne(group, "m", {}, { generation: old })).toEqual({
      _id: "x",
      n: 1,
      label: null,
    });
    const generation = await store.createGeneration(group);
    await store.batch(group, 1, p(2), [], { generation });
    await store.swapGeneration(group, generation);
    await store.dropGeneration(group, old);
    await expect(
      store.find(group, "m", {}, { generation: old }),
    ).rejects.toThrow(/generation/i);
  }, 30000);
for (const adapter of ["memory", "postgres", "mongo"])
  test(`${adapter}: portable filter operators, paging and stream share one meaning`, async () => {
    const store =
        adapter === "memory"
          ? createMemoryReadmodelStore()
          : adapter === "postgres"
            ? createPostgresReadmodelStore({
                pool,
                schema: "readmodel_contract",
              })
            : createMongoReadmodelStore({
                client,
                database: "readmodel_contract",
              }),
      group = `queries_${adapter}_${Date.now()}`;
    const nested = {
      type: ["object", "null"],
      properties: {
        inner: {
          type: ["object", "null"],
          properties: { value: { type: "integer" } },
        },
        items: {
          type: "array",
          items: { type: "object", properties: { n: { type: "integer" } } },
        },
      },
    };
    await store.register(group, {
      m: {
        schema: {
          type: "object",
          properties: {
            name: { type: "string" },
            score: { type: "number" },
            at: { type: "string", format: "date-time" },
            missing: { type: ["string", "null"] },
            tags: { type: "array", items: { type: "integer" } },
            nested,
          },
        },
      },
    });
    await store.fence(group, 1);
    await store.batch(group, 1, p(1), [
      {
        model: "m",
        id: "a",
        document: {
          name: "Ada",
          score: 10,
          at: "2026-01-01T00:00:00Z",
          tags: [1, 2],
          nested: null,
        },
      },
      {
        model: "m",
        id: "b",
        document: {
          name: "Grace",
          score: 20,
          at: new Date("2026-02-01"),
          tags: [3],
          nested: { inner: { value: 3 }, items: [{ n: 4 }] },
        },
      },
    ]);
    const cases: [any, string[]][] = [
      [{ score: { gt: 10 } }, ["b"]],
      [{ score: { oneOf: [{ gt: 15 }, { lt: 0 }] } }, ["b"]],
      [{ score: { gte: 10, lte: 20 } }, ["a", "b"]],
      [{ score: { lt: 20 } }, ["a"]],
      [{ name: { ne: "Ada" } }, ["b"]],
      [{ tags: { nin: [2] } }, ["b"]],
      [{ tags: { in: [2] } }, ["a"]],
      [{ tags: { elemMatch: { gt: 2 } } }, ["b"]],
      [{ oneOf: [{ name: "Ada" }, { score: 20 }] }, ["a", "b"]],
      [{ missing: { exists: false } }, ["a", "b"]],
      [{ "nested.inner.value": { exists: true } }, ["b"]],
      [{ "nested.items.n": { ne: 4 } }, ["a"]],
      [{ at: { gt: "2026-01-15T00:00:00.000Z" } }, ["b"]],
    ];
    for (const [filter, ids] of cases)
      expect(
        (await store.find(group, "m", filter, { sort: { _id: 1 } })).map(
          (d) => d._id,
        ),
        JSON.stringify(filter),
      ).toEqual(ids);
    expect(
      (
        await store.find(
          group,
          "m",
          {},
          { sort: { score: -1 }, from: 1, size: 1 },
        )
      ).map((d) => d._id),
    ).toEqual(["a"]);
    const streamed = [];
    for await (const doc of store.stream(
      group,
      "m",
      {},
      { sort: { score: -1 }, size: 1 },
    ))
      streamed.push(doc._id);
    expect(streamed).toEqual(["b"]);
    expect((await store.findOne(group, "m", { _id: "a" }))?.nested).toBeNull();
  }, 30000);

for (const adapter of ["postgres", "mongo"])
  test(`${adapter}: custom async validation and operator skip fence are durable`, async () => {
    const { createValidator } = await import("../../src/validation.js");
    const store =
      adapter === "postgres"
        ? createPostgresReadmodelStore({ pool, schema: "readmodel_contract" })
        : createMongoReadmodelStore({ client, database: "readmodel_contract" });
    const group = `validation_skip_${adapter}_${Date.now()}`;
    const validator = createValidator({
      formats: {
        "allowed-code": {
          async: true,
          validate: async (value: string) => value === "valid",
        },
      },
    });
    await store.register(
      group,
      {
        m: {
          schema: {
            $async: true,
            type: "object",
            required: ["code"],
            properties: {
              code: {
                type: "string",
                format: "allowed-code",
                default: "valid",
                transform: ["trim"],
              },
            },
          },
        },
      },
      { validator },
    );
    await store.fence(group, 1);
    await expect(
      store.batch(group, 1, p(1), [
        { model: "m", id: "x", document: { code: "invalid" } },
      ]),
    ).rejects.toThrow(/format/i);
    await expect(
      store.batch(group, 1, p(1), [{ model: "m", id: "x", document: {} }]),
    ).rejects.toThrow(/required|mutate/i);
    await store.batch(group, 1, p(1), [
      { model: "m", id: "x", document: { code: "valid" } },
    ]);
    await store.prepareSkip(group, 1, p(2));
    await expect(
      store.batch(group, 1, p(2), [
        { model: "m", id: "x", document: { code: "invalid" } },
      ]),
    ).rejects.toThrow(/epoch/i);
    await store.batch(group, 2, p(2), []);
    expect(await store.checkpoint(group)).toEqual(p(2));
    expect(await store.findOne(group, "m")).toEqual({
      _id: "x",
      code: "valid",
    });
    await expect(store.prepareSkip(group, 2, p(2))).rejects.toThrow(
      /already processed/i,
    );
    await store.close();
  }, 30000);

for (const adapter of ["memory", "postgres", "mongo"])
  test(`${adapter}: empty boolean filters preserve portable true and false`, async () => {
    const store =
      adapter === "memory"
        ? createMemoryReadmodelStore()
        : adapter === "postgres"
          ? createPostgresReadmodelStore({ pool, schema: "readmodel_contract" })
          : createMongoReadmodelStore({
              client,
              database: "readmodel_contract",
            });
    const group = `empty_${adapter}_${crypto.randomUUID()}`;
    await store.register(group, {
      m: {
        schema: { type: "object", properties: { value: { type: "integer" } } },
      },
    });
    await store.fence(group, 1);
    await store.batch(group, 1, p(1), [
      { model: "m", id: "one", document: { value: 1 } },
    ]);
    for (const filter of [{ oneOf: [] }, { $or: [] }, { value: { oneOf: [] } }])
      expect(await store.find(group, "m", filter)).toEqual([]);
    for (const filter of [{ and: [] }, { $and: [] }, { oneOf: [{ and: [] }] }])
      expect(await store.find(group, "m", filter)).toEqual([
        { _id: "one", value: 1 },
      ]);
    expect(await store.find(group, "m", { value: {} })).toEqual([]);
  });

for (const adapter of ["memory", "postgres", "mongo"])
  test(`${adapter}: null equality includes null array members and JSON null`, async () => {
    const store =
      adapter === "memory"
        ? createMemoryReadmodelStore()
        : adapter === "postgres"
          ? createPostgresReadmodelStore({ pool, schema: "readmodel_contract" })
          : createMongoReadmodelStore({
              client,
              database: "readmodel_contract",
            });
    const group = `null_${adapter}_${crypto.randomUUID()}`;
    await store.register(group, {
      m: {
        schema: {
          type: "object",
          properties: {
            tags: {
              type: ["array", "null"],
              items: { type: ["string", "null"] },
            },
            extra: { type: ["object", "null"], "x-store": "json" },
          },
        },
      },
    });
    await store.fence(group, 1);
    await store.batch(group, 1, p(1), [
      {
        model: "m",
        id: "member",
        document: { tags: [null, "a"], extra: null },
      },
      { model: "m", id: "whole", document: { tags: null, extra: null } },
      {
        model: "m",
        id: "empty",
        document: { tags: [], extra: { label: "x" } },
      },
    ]);
    for (const field of ["tags", "extra"]) {
      for (const condition of [
        null,
        { eq: null },
        { in: [null] },
        { oneOf: [null] },
      ])
        expect(
          (
            await store.find(
              group,
              "m",
              { [field]: condition },
              { sort: { _id: 1 } },
            )
          ).map((doc) => doc._id),
        ).toEqual(["member", "whole"]);
      for (const condition of [{ ne: null }, { nin: [null] }])
        expect(
          (await store.find(group, "m", { [field]: condition })).map(
            (doc) => doc._id,
          ),
        ).toEqual(["empty"]);
    }
  });

for (const adapter of ["memory", "postgres", "mongo"])
  test(`${adapter}: nullable object array items keep their value and order recursively`, async () => {
    const store =
      adapter === "memory"
        ? createMemoryReadmodelStore()
        : adapter === "postgres"
          ? createPostgresReadmodelStore({ pool, schema: "readmodel_contract" })
          : createMongoReadmodelStore({
              client,
              database: "readmodel_contract",
            });
    const group = `nullable_items_${adapter}_${crypto.randomUUID()}`;
    const definition = {
      schema: {
        type: "object",
        properties: {
          items: {
            type: "array",
            items: {
              type: ["object", "null"],
              properties: {
                x: { type: ["string", "null"] },
                children: {
                  type: ["array", "null"],
                  items: {
                    type: ["object", "null"],
                    properties: { n: { type: "integer" } },
                  },
                },
              },
            },
          },
        },
      },
    };
    await store.register(group, { m: definition });
    await store.fence(group, 1);
    const document = {
      items: [
        null,
        { x: null, children: null },
        { x: "b", children: [{ n: 1 }, null, { n: 2 }] },
        null,
      ],
    };
    await store.batch(group, 1, p(1), [{ model: "m", id: "one", document }]);
    expect(await store.findOne(group, "m", { _id: "one" })).toEqual({
      _id: "one",
      ...document,
    });
    if (adapter === "postgres") {
      const reopened = createPostgresReadmodelStore({
        pool,
        schema: "readmodel_contract",
      });
      await reopened.register(group, { m: definition });
      expect(await reopened.findOne(group, "m", { _id: "one" })).toEqual({
        _id: "one",
        ...document,
      });
    }
  });

for (const adapter of ["memory", "postgres", "mongo"])
  test(`${adapter}: one batch replaces, deletes and loads many documents with object arrays`, async () => {
    const store =
      adapter === "memory"
        ? createMemoryReadmodelStore()
        : adapter === "postgres"
          ? createPostgresReadmodelStore({ pool, schema: "readmodel_contract" })
          : createMongoReadmodelStore({
              client,
              database: "readmodel_contract",
            });
    const group = `batched_items_${adapter}_${crypto.randomUUID()}`;
    const item = {
      type: ["object", "null"],
      properties: {
        x: { type: ["string", "null"] },
        children: {
          type: ["array", "null"],
          items: {
            type: ["object", "null"],
            properties: { n: { type: "integer" } },
          },
        },
      },
    };
    const definition = {
      schema: {
        type: "object",
        properties: { items: { type: "array", items: item } },
      },
    };
    await store.register(group, { m: definition, other: definition });
    await store.fence(group, 1);
    const many = Array.from({ length: 30_000 }, (_, i) =>
      i % 7 ? { x: `${i}`, children: [{ n: i }, null] } : null,
    );
    await store.batch(group, 1, p(1), [
      { model: "m", id: "a", document: { items: [{ x: "old", children: [{ n: 0 }] }] } },
      { model: "m", id: "b", document: { items: [{ x: "gone", children: null }] } },
      { model: "m", id: "c", document: { items: [] } },
      { model: "other", id: "a", document: { items: [null] } },
    ]);
    const documents = {
      a: { items: [null, { x: "new", children: [null, { n: 1 }] }] },
      c: { items: many },
      d: { items: [{ x: null, children: null }] },
    };
    await store.batch(group, 1, p(2), [
      { model: "m", id: "a", document: documents.a },
      { model: "m", id: "b", document: null },
      { model: "m", id: "c", document: documents.c },
      { model: "m", id: "d", document: documents.d },
    ]);
    expect(await store.find(group, "m", {}, { sort: { _id: 1 } })).toEqual(
      Object.entries(documents).map(([_id, document]) => ({ _id, ...document })),
    );
    expect(await store.find(group, "other")).toEqual([
      { _id: "a", items: [null] },
    ]);
  });

test("Postgres upgrades existing child tables without changing their objects", async () => {
  const schema = `legacy_children_${Date.now()}`;
  const store = createPostgresReadmodelStore({ pool, schema });
  const models = {
    m: {
      schema: {
        type: "object",
        properties: {
          items: {
            type: "array",
            items: {
              type: ["object", "null"],
              properties: {
                x: {
                  type: ["string", "null"],
                },
              },
            },
          },
        },
      },
    },
  };
  await store.register("g", models);
  await store.fence("g", 1);
  await store.batch("g", 1, p(1), [
    { model: "m", id: "one", document: { items: [{ x: null }] } },
  ]);
  const native = await store.native("g", "m");
  const children = await native.query(
    "SELECT conrelid::regclass::text AS name FROM pg_constraint WHERE confrelid=$1::regclass AND contype='f'",
    [native.table],
  );
  for (const row of children.rows)
    await native.query(`ALTER TABLE ${row.name} DROP COLUMN "_item_null"`);
  const reopened = createPostgresReadmodelStore({ pool, schema });
  await reopened.register("g", models);
  expect(await reopened.findOne("g", "m", { _id: "one" })).toEqual({
    _id: "one",
    items: [{ x: null }],
  });
  await reopened.batch("g", 1, p(2), [
    { model: "m", id: "one", document: { items: [null, { x: null }] } },
  ]);
  expect(await reopened.findOne("g", "m", { _id: "one" })).toEqual({
    _id: "one",
    items: [null, { x: null }],
  });
});

test("Postgres indexes new and existing nested child tables across concurrent starts", async () => {
  const namespace = `child_indexes_${Date.now()}`;
  const models = { people: { schema } };
  const store = createPostgresReadmodelStore({ pool, schema: namespace });
  try {
    await store.register("g", models);
    await store.fence("g", 1);
    await store.batch("g", 1, p(1), [
      {
        model: "people",
        id: "one",
        document: {
          items: [
            { sku: "first", parts: [{ n: 2 }, { n: 1 }] },
            { sku: "second" },
          ],
        },
      },
    ]);
    const indexes = async () =>
      (
        await pool.query(
          `SELECT c.relname, i.indisvalid, pg_get_indexdef(i.indexrelid) AS definition
       FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
       LEFT JOIN pg_class idx ON idx.relnamespace=n.oid AND idx.relname=c.relname||'_parent_ordinal'
       LEFT JOIN pg_index i ON i.indexrelid=idx.oid
       WHERE n.nspname=$1 AND c.relkind='r'
       AND EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attname='_parent')`,
          [namespace],
        )
      ).rows;
    const children = await indexes();
    expect(children).toHaveLength(2);
    for (const child of children) {
      expect(child.indisvalid).toBe(true);
      expect(child.definition).toContain("(_parent, _ordinal)");
      await pool.query(
        `DROP INDEX "${namespace}"."${child.relname}_parent_ordinal"`,
      );
    }
    const reopened = [1, 2].map(() =>
      createPostgresReadmodelStore({ pool, schema: namespace }),
    );
    await Promise.all(reopened.map((s) => s.register("g", models)));
    for (const child of await indexes()) expect(child.indisvalid).toBe(true);
    expect(
      await reopened[0]!.findOne("g", "people", { _id: "one" }),
    ).toMatchObject({
      items: [{ sku: "first", parts: [{ n: 2 }, { n: 1 }] }, { sku: "second" }],
    });
  } finally {
    await pool.query(`DROP SCHEMA "${namespace}" CASCADE`);
  }
});

const storeFor = (adapter: string) =>
  adapter === "memory"
    ? createMemoryReadmodelStore()
    : adapter === "postgres"
      ? createPostgresReadmodelStore({ pool, schema: "readmodel_contract" })
      : createMongoReadmodelStore({ client, database: "readmodel_contract" });
const auditModel = {
  schema: {
    type: "object",
    required: ["name", "at"],
    properties: {
      name: { type: "string" },
      at: { type: "string", format: "date-time" },
      flag: { type: ["boolean", "null"] },
      labels: { "x-store": "json", type: "array", items: { type: "string" } },
      anything: { "x-store": "json" },
      items: {
        type: "array",
        items: {
          type: "object",
          required: ["sku"],
          properties: { sku: { type: "string" } },
        },
      },
    },
  },
  indexes: [{ fields: ["items.sku"] }, { fields: { name: 1 as const } }],
};
for (const adapter of ["memory", "postgres", "mongo"])
  test(`${adapter}: sorting, json filters, collation, text index and required fields agree`, async () => {
    const store = storeFor(adapter),
      group = `audit_${adapter}_${crypto.randomUUID()}`;
    await store.register(group, { m: auditModel });
    await store.fence(group, 1);
    const at = new Date("2026-03-01");
    const docs: [string, string, any][] = [
      ["c", "Z", ["a", "b"]],
      ["a", "a", "a"],
      ["b", "B", { k: "a" }],
    ];
    await store.batch(
      group,
      1,
      p(1),
      docs.map(([id, name, anything]) => ({
        model: "m",
        id,
        document: {
          name,
          at,
          flag: null,
          labels: id === "b" ? [] : ["a"],
          anything,
          items: [{ sku: `sku-${id}` }],
        },
      })),
    );
    const ids = async (filter: any, options: any = { sort: { _id: 1 } }) =>
      (await store.find(group, "m", filter, options)).map((d) => d._id);
    expect(await ids({}, { sort: { at: 1 } })).toEqual(["a", "b", "c"]);
    expect(await ids({}, { sort: { at: -1, flag: 1 } })).toEqual([
      "a",
      "b",
      "c",
    ]);
    expect(await ids({ labels: "a" })).toEqual(["a", "c"]);
    expect(await ids({ labels: { in: ["a"] } })).toEqual(["a", "c"]);
    expect(await ids({ anything: "a" })).toEqual(["a", "c"]);
    expect(await ids({ anything: { k: "a" } })).toEqual(["b"]);
    expect(await ids({ name: { gt: "Z" } })).toEqual(["a"]);
    expect(await ids({ name: { lt: "Z" } })).toEqual(["b"]);
    expect(await ids({ "items.sku": "sku-b" })).toEqual(["b"]);
    const streamed = [];
    for await (const doc of store.stream(group, "m", {}, { sort: { name: 1 } }))
      streamed.push(doc._id);
    expect(streamed).toEqual(["b", "c", "a"]);
    await expect(store.find(group, "m", {}, { q: "anything" })).rejects.toThrow(
      /no declared text index/,
    );
    await expect(
      store.count(group, "m", {}, { q: "anything" }),
    ).rejects.toThrow(/no declared text index/);
    await store.register(group, { m: auditModel }, { validate: false });
    await expect(
      store.batch(group, 1, p(2), [
        { model: "m", id: "x", document: { name: null, at } },
      ]),
    ).rejects.toThrow(/name is required/);
    if (adapter === "postgres") {
      const native = await store.native(group, "m");
      const columns = await native.query(
        `SELECT column_name, is_nullable FROM information_schema.columns
         WHERE table_schema=$1 AND table_name=$2 AND column_name IN ('name','at','flag','labels')
         ORDER BY column_name`,
        ["readmodel_contract", native.table.split(".")[1]!.replaceAll('"', "")],
      );
      expect(columns.rows).toEqual([
        { column_name: "at", is_nullable: "NO" },
        { column_name: "flag", is_nullable: "YES" },
        { column_name: "labels", is_nullable: "YES" },
        { column_name: "name", is_nullable: "NO" },
      ]);
    }
    await store.close();
  }, 30000);

for (const adapter of ["memory", "postgres", "mongo"])
  test(`${adapter}: schemas without a relational mapping are refused at registration`, async () => {
    const store = storeFor(adapter),
      group = `refused_${adapter}_${crypto.randomUUID()}`;
    const register = (properties: any, indexes?: any[]) =>
      store.register(group, {
        m: { schema: { type: "object", properties }, indexes },
      });
    const grid = {
      type: "array",
      items: { type: "array", items: { type: "string" } },
    };
    await expect(register({ grid })).rejects.toThrow(/x-store: json/);
    await expect(
      register({
        grid: {
          type: "array",
          items: {
            type: "array",
            items: { type: "object", properties: { n: { type: "integer" } } },
          },
        },
      }),
    ).rejects.toThrow(/x-store: json/);
    const items = {
      type: "array",
      items: { type: "object", properties: { sku: { type: "string" } } },
    };
    await expect(
      register({ items, name: { type: "string" } }, [
        { fields: ["items.sku", "name"] },
      ]),
    ).rejects.toThrow(/index/i);
    await expect(register({ items }, [{ fields: ["items"] }])).rejects.toThrow(
      /index/i,
    );
    await expect(
      register({ extra: { "x-store": "json", type: "object" } }, [
        { fields: ["extra.a"] },
      ]),
    ).rejects.toThrow(/index/i);
    await expect(
      register(
        {
          items: {
            type: "array",
            items: {
              "x-store": "json",
              type: "object",
              properties: { sku: { type: "string" } },
            },
          },
        },
        [{ fields: ["items.sku"] }],
      ),
    ).rejects.toThrow(/index/i);
    await expect(register({}, [{ fields: ["missing"] }])).rejects.toThrow(
      /index/i,
    );
    const ok = await register({ grid: { ...grid, "x-store": "json" }, items }, [
      { fields: ["items.sku"] },
      { fields: ["items.sku"], text: true },
    ]);
    expect(ok.rebuildRequired).toBe(false);
    await store.fence(group, 1);
    await store.batch(group, 1, p(1), [
      {
        model: "m",
        id: "x",
        document: { grid: [["a"], ["b", "c"]], items: [{ sku: "Book" }] },
      },
    ]);
    expect(
      (await store.find(group, "m", { grid: { exists: true } }, { q: "boo" }))
        .length,
    ).toBe(1);
    await store.close();
  }, 30000);

for (const adapter of ["postgres", "mongo"])
  test(`${adapter}: stream pages through many documents in find order`, async () => {
    const store = storeFor(adapter),
      group = `stream_${adapter}_${crypto.randomUUID()}`;
    await store.register(group, {
      m: {
        schema: {
          type: "object",
          properties: {
            n: { type: ["integer", "null"] },
            at: { type: "string", format: "date-time" },
          },
        },
      },
    });
    await store.fence(group, 1);
    await store.batch(
      group,
      1,
      p(1),
      Array.from({ length: 250 }, (_, i) => ({
        model: "m",
        id: `d${String(i).padStart(3, "0")}`,
        document: {
          n: i % 7 === 0 ? null : i % 5,
          at: new Date(Date.UTC(2026, 0, 1 + (i % 3))),
        },
      })),
    );
    for (const options of [
      { sort: { n: -1 as const, at: 1 as const } },
      { sort: { at: -1 as const }, from: 30, size: 150 },
      {},
    ]) {
      const streamed = [];
      for await (const doc of store.stream(
        group,
        "m",
        { n: { ne: 3 } },
        options,
      ))
        streamed.push(doc._id);
      expect(streamed).toEqual(
        (await store.find(group, "m", { n: { ne: 3 } }, options)).map(
          (d) => d._id,
        ),
      );
    }
    await store.close();
  }, 30000);

for (const adapter of ["memory", "postgres", "mongo"])
  test(`${adapter}: Unicode ranges and streamed sorts agree with binary database order`, async () => {
    const store = storeFor(adapter),
      group = `unicode_${adapter}_${crypto.randomUUID()}`;
    await store.register(group, {
      m: {
        schema: { type: "object", properties: { name: { type: "string" } } },
      },
    });
    await store.fence(group, 1);
    await store.batch(group, 1, p(1), [
      { model: "m", id: "astral", document: { name: "\u{10000}" } },
      { model: "m", id: "bmp", document: { name: "\ue000" } },
    ]);
    const options = { sort: { name: 1 as const } };
    expect(
      (await store.find(group, "m", {}, options)).map((d) => d._id),
    ).toEqual(["bmp", "astral"]);
    expect(
      (await Array.fromAsync(store.stream(group, "m", {}, options))).map(
        (d) => d._id,
      ),
    ).toEqual(["bmp", "astral"]);
    expect(
      (await store.find(group, "m", { name: { gt: "\ue000" } })).map(
        (d) => d._id,
      ),
    ).toEqual(["astral"]);
  });

test("Postgres streamed text search preserves find ranking before applying size", async () => {
  const store = storeFor("postgres"),
    group = `text_stream_${crypto.randomUUID()}`;
  await store.register(group, {
    m: {
      schema: { type: "object", properties: { title: { type: "string" } } },
      indexes: [{ fields: ["title"], text: true }],
    },
  });
  await store.fence(group, 1);
  await store.batch(group, 1, p(1), [
    { model: "m", id: "a", document: { title: "notebook" } },
    { model: "m", id: "z", document: { title: "book book" } },
  ]);
  const options = { q: "book", size: 1 };
  expect((await store.find(group, "m", {}, options)).map((d) => d._id)).toEqual(
    ["z"],
  );
  expect(
    (await Array.fromAsync(store.stream(group, "m", {}, options))).map(
      (d) => d._id,
    ),
  ).toEqual(["z"]);
});

for (const adapter of ["postgres", "mongo"])
  test(`${adapter}: a paged stream retains its snapshot while documents change`, async () => {
    const store = storeFor(adapter),
      group = `snapshot_${adapter}_${crypto.randomUUID()}`;
    await store.register(group, {
      m: { schema: { type: "object", properties: { n: { type: "integer" } } } },
    });
    await store.fence(group, 1);
    await store.batch(
      group,
      1,
      p(1),
      Array.from({ length: 150 }, (_, n) => ({
        model: "m",
        id: String(n),
        document: { n },
      })),
    );
    const rows = store.stream(group, "m", {}, { sort: { n: 1 } });
    try {
      expect((await rows.next()).value?.n).toBe(0);
      await store.batch(group, 1, p(2), [
        { model: "m", id: "0", document: { n: 200 } },
      ]);
      const rest = await Array.fromAsync(rows);
      expect(rest).toHaveLength(149);
      expect(rest.at(-1)?.n).toBe(149);
    } finally {
      await rows.return(undefined);
    }
  });

for (const adapter of ["postgres", "mongo"])
  test(`${adapter}: an open stream survives generation swap and closes on early return`, async () => {
    const store = storeFor(adapter),
      group = `swap_stream_${adapter}_${crypto.randomUUID()}`;
    await store.register(group, {
      m: { schema: { type: "object", properties: { n: { type: "integer" } } } },
    });
    await store.fence(group, 1);
    await store.batch(
      group,
      1,
      p(1),
      Array.from({ length: 150 }, (_, n) => ({
        model: "m",
        id: String(n),
        document: { n },
      })),
    );
    const old = await store.activeGeneration(group);
    const next = await store.createGeneration(group);
    await store.batch(
      group,
      1,
      p(2),
      [{ model: "m", id: "new", document: { n: 999 } }],
      { generation: next },
    );
    const rows = store.stream(group, "m", {}, { sort: { n: 1 } });
    let dropped: Promise<void> | undefined;
    try {
      expect((await rows.next()).value?.n).toBe(0);
      await store.swapGeneration(group, next);
      dropped = store.dropGeneration(group, old);
      const rest = await Array.fromAsync(rows);
      expect(rest).toHaveLength(149);
      expect(rest.at(-1)?.n).toBe(149);
    } finally {
      await rows.return(undefined);
      await dropped;
    }
    const short = store.stream(group, "m");
    expect((await short.next()).value?._id).toBe("new");
    await short.return(undefined);
    expect((await store.findOne(group, "m"))?._id).toBe("new");
  });

for (const adapter of ["memory", "postgres", "mongo"])
  test(`${adapter}: JSON-marked array items preserve nested arrays and scalar values`, async () => {
    const store = storeFor(adapter),
      group = `json_items_${adapter}_${crypto.randomUUID()}`;
    await store.register(group, {
      m: {
        schema: {
          type: "object",
          properties: {
            items: { type: "array", items: { "x-store": "json" } },
            grid: {
              type: "array",
              items: { type: "array", "x-store": "json" },
            },
          },
        },
      },
    });
    await store.fence(group, 1);
    const document = {
      items: ["text", { key: "value" }, null, 2],
      grid: [["a"], ["b", "c"]],
    };
    await store.batch(group, 1, p(1), [{ model: "m", id: "one", document }]);
    expect(await store.findOne(group, "m")).toEqual({
      _id: "one",
      ...document,
    });
    expect(await store.count(group, "m", { items: "text" })).toBe(1);
  });

test("Postgres reads do not wait for the group's write lock", async () => {
  const store = createPostgresReadmodelStore({
      pool,
      schema: "readmodel_contract",
    }),
    group = `lock_${crypto.randomUUID()}`;
  await store.register(group, {
    m: { schema: { type: "object", properties: { n: { type: "integer" } } } },
  });
  const holder = await pool.connect();
  try {
    await holder.query("BEGIN");
    await holder.query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", [
      `event-fabric-readmodels:readmodel_contract:${group}`,
    ]);
    const timeout = new Promise((_, reject) =>
      setTimeout(() => reject(new Error("read blocked by write lock")), 3000),
    );
    await Promise.race([
      Promise.all([
        store.find(group, "m"),
        store.count(group, "m"),
        store.checkpoint(group),
        (async () => {
          for await (const _ of store.stream(group, "m"));
        })(),
      ]),
      timeout,
    ]);
  } finally {
    await holder.query("ROLLBACK");
    holder.release();
  }
});

test("Mongo migrations that ignore the session still roll back", async () => {
  const store = storeFor("mongo"),
    group = `migration_session_${crypto.randomUUID()}`;
  const original = {
    m: { schema: { type: "object", properties: { n: { type: "integer" } } } },
  };
  const from = (await store.register(group, original)).hash;
  await store.fence(group, 1);
  await store.batch(group, 1, p(1), [
    { model: "m", id: "x", document: { n: 1 } },
  ]);
  const next = {
    m: {
      schema: {
        type: "object",
        properties: { n: { type: "integer" }, s: { type: ["string", "null"] } },
      },
    },
  };
  const to = (await store.register(group, next)).hash;
  await expect(
    store.register(group, next, {
      migrations: [
        {
          from,
          to,
          async up(native) {
            await native
              .collection("m")
              .updateOne({ _id: "x" }, { $set: { n: 2 } });
            await native.database
              .collection(native.collection("m").collectionName)
              .insertOne({ _id: "y", n: 3 });
            throw new Error("abort migration");
          },
        },
      ],
    }),
  ).rejects.toThrow("abort migration");
  expect(await store.find(group, "m")).toEqual([{ _id: "x", n: 1 }]);
});

// Store contracts that once ran on memory only.
const peopleModel = {
  people: {
    schema: {
      type: "object",
      properties: {
        name: { type: "string" },
        created: { type: ["string", "null"], format: "date-time" },
        tags: { type: "array", items: { type: "string" } },
        items: {
          type: "array",
          items: {
            type: "object",
            properties: { sku: { type: "string" }, cost: { type: "number" } },
          },
        },
      },
    },
  },
};
for (const adapter of ["memory", "postgres", "mongo"]) {
  const unique = (name: string) => `${name}_${adapter}_${crypto.randomUUID()}`;
  // How an absent optional field reads: Postgres columns cannot tell it from `null`.
  const absent = adapter === "postgres" ? null : undefined;

  test(`${adapter}: atomically fences changes and Checkpoint and applies portable queries`, async () => {
    const store = storeFor(adapter),
      group = unique("fences");
    await store.register(group, peopleModel);
    await store.fence(group, 1);
    await store.batch(group, 1, p(1), [
      {
        model: "people",
        id: "one",
        document: {
          name: "Ada",
          created: "2026-01-01T00:00:00.000Z",
          tags: ["a"],
          items: [{ sku: "book", cost: 12 }],
        },
      },
    ]);
    expect(
      await store.findOne(group, "people", {
        tags: "a",
        items: { elemMatch: { cost: { gt: 10 } } },
      }),
    ).toEqual({
      _id: "one",
      name: "Ada",
      created: new Date("2026-01-01"),
      tags: ["a"],
      items: [{ sku: "book", cost: 12 }],
    });
    await store.fence(group, 2);
    await expect(
      store.batch(group, 1, p(2), [
        { model: "people", id: "one", document: { name: "wrong" } },
      ]),
    ).rejects.toBeInstanceOf(StaleEpochError);
    expect(await store.checkpoint(group)).toEqual(p(1));
    expect((await store.findOne(group, "people", { _id: "one" }))?.name).toBe(
      "Ada",
    );
    await expect(
      store.batch(group, 2, p(2), [
        {
          model: "people",
          id: "one",
          document: { name: "wrong", undeclared: true },
        },
      ]),
    ).rejects.toThrow(/undeclared/i);
    expect(await store.checkpoint(group)).toEqual(p(1));
    await store.close();
  });

  test(`${adapter}: schema startup rejects cycles and documents remain closed with validation disabled`, async () => {
    const store = storeFor(adapter),
      group = unique("closed");
    await expect(
      store.register(
        unique("cyclic"),
        { m: { schema: { $ref: "a" } } },
        {
          schemas: {
            a: { type: "object", properties: { loop: { $ref: "a" } } },
          },
        },
      ),
    ).rejects.toThrow(/cyclic/i);
    await store.register(
      group,
      {
        m: {
          schema: {
            type: "object",
            properties: {
              nested: {
                type: ["object", "null"],
                properties: { known: { type: "string" } },
              },
              value: { type: "integer" },
              optional: { type: "string" },
            },
          },
        },
      },
      { validate: false },
    );
    await store.fence(group, 1);
    await expect(
      store.batch(group, 1, p(1), [
        { model: "m", id: "x", document: { nested: { unknown: 1 } } },
      ]),
    ).rejects.toThrow(/undeclared/i);
    await expect(
      store.batch(group, 1, p(1), [
        {
          model: "m",
          id: "x",
          document: { value: Number.MAX_SAFE_INTEGER + 1 },
        },
      ]),
    ).rejects.toThrow(/safe integer/i);
    await store.batch(group, 1, p(1), [
      { model: "m", id: "x", document: { value: 1 } },
    ]);
    expect(await store.findOne(group, "m")).toEqual({
      _id: "x",
      nested: absent,
      value: 1,
      optional: absent,
    });
    await store.close();
  });

  test(`${adapter}: prototype property names do not bypass closed schemas`, async () => {
    const store = storeFor(adapter),
      group = unique("proto");
    await store.register(
      group,
      {
        m: {
          schema: {
            type: "object",
            properties: { value: { type: "string" } },
          },
        },
      },
      { validate: false },
    );
    await store.fence(group, 1);
    await expect(
      store.batch(group, 1, p(1), [
        { model: "m", id: "x", document: JSON.parse('{"toString":"hidden"}') },
      ]),
    ).rejects.toThrow(/undeclared/i);
    await store.close();
  });

  test(`${adapter}: normalized optional NULLs round-trip without relaxing required fields`, async () => {
    const store = storeFor(adapter),
      group = unique("nulls");
    await store.register(group, {
      m: {
        schema: {
          type: "object",
          required: ["required"],
          properties: {
            required: { type: "string" },
            optional: { type: "string" },
          },
        },
      },
    });
    await store.fence(group, 1);
    await store.batch(group, 1, p(1), [
      { model: "m", id: "x", document: { required: "yes" } },
      { model: "m", id: "y", document: { required: "yes", optional: null } },
    ]);
    const doc = (await store.findOne(group, "m", { _id: "x" }))!;
    expect(doc.optional).toBe(absent);
    expect(Object.hasOwn(doc, "optional")).toBe(absent === null);
    expect((await store.findOne(group, "m", { _id: "y" }))!.optional).toBeNull();
    expect(
      (await store.find(group, "m", { optional: null })).map((d) => d._id),
    ).toEqual(["x", "y"]);
    await store.batch(group, 1, p(2), [{ model: "m", id: "x", document: doc }]);
    await expect(
      store.batch(group, 1, p(3), [
        { model: "m", id: "x", document: { required: null } },
      ]),
    ).rejects.toThrow(/invalid/i);
    await store.close();
  });

  test(`${adapter}: equal dates sort by _id and unmappable layouts are refused`, async () => {
    const store = storeFor(adapter),
      group = unique("dates");
    await expect(
      store.register(unique("grid"), {
        m: {
          schema: {
            type: "object",
            properties: {
              grid: {
                type: "array",
                items: { type: "array", items: { type: "number" } },
              },
            },
          },
        },
      }),
    ).rejects.toThrow(/x-store: json/);
    await store.register(group, {
      m: {
        schema: {
          type: "object",
          properties: { at: { type: "string", format: "date-time" } },
        },
      },
    });
    await store.fence(group, 1);
    const at = "2026-01-01T00:00:00.000Z";
    await store.batch(
      group,
      1,
      p(1),
      ["c", "a", "b"].map((id) => ({ model: "m", id, document: { at } })),
    );
    expect(
      (await store.find(group, "m", {}, { sort: { at: -1 } })).map(
        (d) => d._id,
      ),
    ).toEqual(["a", "b", "c"]);
    await store.close();
  });

  test(`${adapter}: schema builders' nullable anyOf, const unions and bare enums resolve to one-type columns`, async () => {
    const { resolveSchema } =
      await import("../../src/readmodel-store/schema.js");
    const { default: Type } = await import("typebox");
    const typed = Type.Object({
      receipt: Type.Union([Type.String({ minLength: 1 }), Type.Null()]),
      status: Type.Enum(["open", "settled"]),
      kind: Type.Union([Type.Literal("a"), Type.Literal("b"), Type.Null()]),
      count: Type.Union([Type.Integer(), Type.Null()], { description: "n" }),
      money: { anyOf: [{ $ref: "/Money" }, { type: "null" }] },
    });
    const schemas = { "/Money": { type: "number" } };
    expect(resolveSchema(typed, schemas).properties).toEqual({
      receipt: { type: ["string", "null"], minLength: 1 },
      status: { type: "string", enum: ["open", "settled"] },
      kind: { type: ["string", "null"], enum: ["a", "b", null] },
      count: { type: ["integer", "null"], description: "n" },
      money: { type: ["number", "null"] },
    });
    const store = storeFor(adapter),
      group = unique("builders");
    await expect(
      store.register(unique("anyof"), {
        m: {
          schema: {
            type: "object",
            properties: {
              either: { anyOf: [{ type: "string" }, { type: "number" }] },
            },
          },
        },
      }),
    ).rejects.toThrow(/anyOf requires x-store: json/);
    await store.register(group, { m: { schema: typed } }, { schemas });
    await store.fence(group, 1);
    const full = {
      receipt: "r-1",
      status: "settled",
      kind: "b",
      count: 3,
      money: 9.5,
    };
    await store.batch(group, 1, p(1), [
      { model: "m", id: "full", document: full },
      {
        model: "m",
        id: "empty",
        document: {
          receipt: null,
          status: "open",
          kind: null,
          count: null,
          money: null,
        },
      },
    ]);
    expect(await store.findOne(group, "m", { _id: "full" })).toEqual({
      _id: "full",
      ...full,
    });
    expect(await store.count(group, "m", { kind: null })).toBe(1);
    expect(await store.count(group, "m", { status: { in: ["open"] } })).toBe(1);
    await store.close();
  });

  test(`${adapter}: streams more than one page with a sort, from and size`, async () => {
    const store = storeFor(adapter),
      group = unique("paging");
    await store.register(group, {
      m: {
        schema: {
          type: "object",
          properties: { rank: { type: "integer" }, name: { type: "string" } },
        },
      },
    });
    await store.fence(group, 1);
    // 250 documents over three pages; rank repeats so _id breaks ties across page boundaries.
    const ids = Array.from({ length: 250 }, (_, i) =>
      String(i).padStart(3, "0"),
    );
    await store.batch(
      group,
      1,
      p(1),
      ids.map((id) => ({
        model: "m",
        id,
        document: { rank: Number(id) % 7, name: `n${id}` },
      })),
    );
    const expected = [...ids].sort(
      (a, b) =>
        (Number(b) % 7) - (Number(a) % 7) || (a < b ? -1 : a > b ? 1 : 0),
    );
    const options = { sort: { rank: -1 as const }, from: 30, size: 180 };
    const streamed = (
      await Array.fromAsync(store.stream(group, "m", {}, options))
    ).map((d) => d._id);
    expect(streamed).toEqual(expected.slice(30, 210));
    expect(
      (await store.find(group, "m", {}, options)).map((d) => d._id),
    ).toEqual(streamed);
    await store.close();
  }, 30000);
}
for (const adapter of ["postgres", "mongo"])
  test(`${adapter}: registration moves Partition metadata of the earlier format into Partition records, keeping Checkpoints and epochs`, async () => {
    const group = `legacy_${adapter}_${Date.now()}`;
    const store =
      adapter === "postgres"
        ? createPostgresReadmodelStore({ pool, schema: "readmodel_contract" })
        : createMongoReadmodelStore({ client, database: "readmodel_contract" });
    const definitions = {
      m: { schema: { type: "object", properties: { n: { type: "integer" } } } },
    };
    await store.register(group, definitions);
    await store.fence(group, 1);
    await store.batch(group, 1, p(5), [
      { model: "m", id: "x", document: { n: 10 } },
    ]);
    // The earlier format: Partition epochs on the group, Checkpoints on the generation.
    const legacy = async () => {
      const checkpoints = { 0: p(10), 1: p(10) };
      if (adapter === "postgres") {
        await pool.query(
          `UPDATE readmodel_contract.ef_rm_groups SET data=data||$1 WHERE name=$2`,
          [{ epochs: { 1: 7 } }, group],
        );
        await pool.query(
          `UPDATE readmodel_contract.ef_rm_generations SET data=(data-'version')||$1 WHERE group_name=$2`,
          [{ checkpoints }, group],
        );
      } else {
        const db = client.db("readmodel_contract");
        await db
          .collection<any>("event_fabric_groups")
          .updateOne({ _id: group }, { $set: { "value.epochs": { 1: 7 } } });
        await db.collection<any>("event_fabric_generations").updateMany(
          { "value.group": group },
          {
            $set: { "value.checkpoints": checkpoints },
            $unset: { "value.version": "" },
          },
        );
      }
    };
    await legacy();
    expect((await store.register(group, definitions)).rebuildRequired).toBe(
      false,
    );
    expect(await store.checkpoint(group, { partition: 0 })).toEqual(p(10));
    expect(await store.checkpoint(group, { partition: 1 })).toEqual(p(10));
    expect(await store.checkpoint(group, { partitions: 2 })).toEqual(p(10));
    await expect(store.fence(group, 6, 1)).rejects.toThrow(StaleEpochError);
    await store.fence(group, 7, 1);
    // Already applied: replaying Partition 1 up to P10 changes nothing.
    await store.batch(
      group,
      7,
      p(10),
      [{ model: "m", id: "x", document: { n: 99 } }],
      { partition: 1 },
    );
    expect((await store.findOne(group, "m"))?.n).toBe(10);
    // An instance of the earlier version writing again is refused, not replayed into.
    await legacy();
    await expect(
      store.batch(group, 7, p(11), [], { partition: 1 }),
    ).rejects.toThrow(/earlier EventFabric version/);
    await store.close();
  });
for (const adapter of ["memory", "postgres", "mongo"])
  test(`${adapter}: a Readmodel group name cannot contain the Partition separator`, async () => {
    const store =
      adapter === "memory"
        ? createMemoryReadmodelStore()
        : adapter === "postgres"
          ? createPostgresReadmodelStore({ pool, schema: "readmodel_contract" })
          : createMongoReadmodelStore({
              client,
              database: "readmodel_contract",
            });
    await expect(
      store.register("g#0", {
        m: { schema: { type: "object", properties: {} } },
      }),
    ).rejects.toThrow(/must not contain "#"/);
  });
/**
 * A Partition batch paused inside its transaction (in its validator) while a whole-group
 * transition runs. Postgres and memory make the transition wait, so the batch commits first and
 * the transition sees it; Mongo lets the transition commit, so the paused batch must conflict,
 * and its retry must refuse or skip the stale work. Either way no obsolete write lands after
 * the transition that invalidated it.
 */
for (const adapter of ["memory", "postgres", "mongo"])
  for (const transition of ["fence", "reset", "swap"] as const)
    test(`${adapter}: a Partition batch in flight during a whole-group ${transition} commits before it or not at all`, async () => {
      const group = `transition_${transition}_${adapter}_${Date.now()}`;
      const make = () =>
        adapter === "memory"
          ? createMemoryReadmodelStore()
          : adapter === "postgres"
            ? createPostgresReadmodelStore({
                pool,
                schema: "readmodel_contract",
              })
            : createMongoReadmodelStore({
                client,
                database: "readmodel_contract",
              });
      const a = make(),
        b = adapter === "memory" ? a : make();
      const definitions = {
        m: {
          schema: { type: "object", properties: { n: { type: "integer" } } },
        },
      };
      let pausing = false,
        entered!: () => void,
        resume!: () => void;
      const entering = new Promise<void>((resolve) => (entered = resolve)),
        resumed = new Promise<void>((resolve) => (resume = resolve));
      await a.register(group, definitions, {
        validator: async (_schema, document: any) => {
          if (pausing && document.n === 99) {
            entered();
            await resumed;
          }
        },
      });
      if (b !== a) await b.register(group, definitions);
      const partition = transition === "fence" ? 0 : 1;
      await a.fence(group, 1, 0);
      await a.fence(group, 1, 1);
      await a.batch(
        group,
        1,
        p(1),
        [{ model: "m", id: "x", document: { n: 1 } }],
        { partition },
      );
      const live = await a.activeGeneration(group);
      let shadow = "";
      if (transition === "swap") {
        shadow = await b.createGeneration(group);
        await b.batch(
          group,
          1,
          p(1),
          [{ model: "m", id: "x", document: { n: 1 } }],
          { generation: shadow },
        );
      }
      pausing = true;
      const batch = a.batch(
        group,
        1,
        p(2),
        [{ model: "m", id: "x", document: { n: 99 } }],
        { partition, generation: live, requireActive: true },
      );
      await entering;
      const run =
        transition === "fence"
          ? b.fence(group, 2)
          : transition === "reset"
            ? b.resetPartitions(group, [1, 1], p(5))
            : b.swapGeneration(group, shadow);
      // Postgres and memory: the transition waits for the paused batch; Mongo: it commits now.
      await new Promise((resolve) => setTimeout(resolve, 150));
      resume();
      const [batched, transitioned] = await Promise.allSettled([batch, run]);
      const n = (await a.findOne(group, "m", { _id: "x" }))?.n;
      const waits = adapter !== "mongo";
      if (transition === "fence") {
        // The old epoch commits before the fence, or is refused after it.
        expect(transitioned.status).toBe("fulfilled");
        expect(batched.status).toBe(waits ? "fulfilled" : "rejected");
        expect(n).toBe(waits ? 99 : 1);
      } else if (transition === "reset") {
        expect(transitioned.status).toBe("fulfilled");
        expect(batched.status).toBe("fulfilled");
        expect(await a.checkpoint(group, { partition })).toEqual(p(5));
        // After the reset to P5, the batch at P2 has nothing left to do.
        expect(n).toBe(waits ? 99 : 1);
      } else {
        if (waits) {
          // The batch moved the live Partition to P2, beyond the shadow at P1.
          expect(batched.status).toBe("fulfilled");
          expect(transitioned.status).toBe("rejected");
          expect(await a.activeGeneration(group)).toBe(live);
          expect(n).toBe(99);
        } else {
          expect(transitioned.status).toBe("fulfilled");
          expect(batched.status).toBe("rejected");
          expect((batched as PromiseRejectedResult).reason).toBeInstanceOf(
            StaleGenerationError,
          );
          expect(n).toBe(1);
          expect(
            (await a.findOne(group, "m", { _id: "x" }, { generation: live }))
              ?.n,
          ).toBe(1);
        }
      }
      await a.close();
      if (b !== a) await b.close();
    }, 30000);
for (const adapter of ["memory", "postgres", "mongo"])
  test(`${adapter}: a swap needs the shadow to cover every Partition of the live generation`, async () => {
    const group = `swapfloor_${adapter}_${Date.now()}`;
    const store =
      adapter === "memory"
        ? createMemoryReadmodelStore()
        : adapter === "postgres"
          ? createPostgresReadmodelStore({ pool, schema: "readmodel_contract" })
          : createMongoReadmodelStore({
              client,
              database: "readmodel_contract",
            });
    await store.register(group, {
      m: { schema: { type: "object", properties: { n: { type: "integer" } } } },
    });
    await store.fence(group, 1, 0);
    await store.fence(group, 1, 1);
    // Partition 1 at P10 while the generation's own Checkpoint is unset; the shadow at P5.
    await store.batch(
      group,
      1,
      p(10),
      [{ model: "m", id: "x", document: { n: 10 } }],
      { partition: 1 },
    );
    const shadow = await store.createGeneration(group);
    await store.batch(
      group,
      1,
      p(5),
      [{ model: "m", id: "x", document: { n: 5 } }],
      { generation: shadow },
    );
    await expect(store.swapGeneration(group, shadow)).rejects.toThrow(
      GenerationBehindError,
    );
    await store.batch(group, 1, p(11), [], { generation: shadow });
    await store.swapGeneration(group, shadow);
    // Every Partition continues from the new generation's Checkpoint.
    expect(await store.checkpoint(group, { partitions: 2 })).toEqual(p(11));
    await store.close();
  });
test("a migrated base() chain runs on the Mongo repository's native collection", async () => {
  const { createApplication } = await import("../../src/runtime/index.js");
  const { compile } = await import("../../src/authoring/index.js");
  const { createInMemoryEventStore } =
    await import("../../src/event-store/memory.js");
  const { createInMemoryTransport } =
    await import("../../src/transport/memory.js");
  const { migrateSource } = await import("../../src/migration/index.js");
  const group = `migrated_${Date.now()}`;
  const application = createApplication({
    name: "migrated",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    pollInterval: 5,
    aggregates: [
      compile("crm", "person", {
        commands: {
          add: (command, state) => state.apply.added(command.payload),
        },
        events: { added: () => {} },
      }),
    ],
    readmodels: {
      [group]: {
        store: createMongoReadmodelStore({
          client,
          database: "readmodel_contract",
        }),
        models: {
          people: {
            schema: {
              type: "object",
              properties: {
                name: { type: "string" },
                active: { type: "boolean" },
              },
            },
            reactions: {
              "crm.person.added": (event: any, view: any) => {
                view.set("name", event.payload.name);
                view.set("active", event.payload.active);
              },
            },
          },
        },
      },
    },
  });
  await application.start();
  let outcome: any;
  for (const [id, active] of [
    ["b", true],
    ["a", true],
    ["c", false],
  ] as const)
    outcome = await application.sendCommand({
      stream: { context: "crm", aggregate: "person", id },
      name: "add",
      payload: { name: id.toUpperCase(), active },
    });
  await application.readmodels.waitFor(outcome.events[0].position, [group]);
  const migrated = await migrateSource(
    `module.exports = async (app) => await app.getReadmodels().${group}.people().base().find({ active: true }).sort({ name: 1 }).toArray();`,
  );
  expect(migrated.reviews).toHaveLength(1);
  const module = { exports: undefined as any };
  new Function("module", migrated.source)(module);
  const app = { getReadmodels: () => application.readmodels };
  expect(await module.exports(app)).toEqual([
    { _id: "a", name: "A", active: true },
    { _id: "b", name: "B", active: true },
  ]);
  await application.close();
});

const tagOf = (generation: string) =>
  generation.replaceAll("-", "").slice(0, 8);
const hashOf = (value: string, length = 8) =>
  createHash("sha256").update(value).digest("hex").slice(0, length);
const tablesIn = async (namespace: string) =>
  (
    await pool.query(
      `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
       WHERE n.nspname=$1 AND c.relkind='r' AND c.relname LIKE 'rm\\_%' ORDER BY 1`,
      [namespace],
    )
  ).rows.map((row) => row.relname as string);
const collectionsIn = async (database: string) =>
  (await client.db(database).listCollections().toArray())
    .map((c) => c.name)
    .sort();
const namedModels = {
  invoices: {
    schema: {
      type: "object",
      properties: {
        debtor: { type: "string" },
        lines: {
          type: "array",
          items: { type: "object", properties: { sku: { type: "string" } } },
        },
      },
    },
    indexes: [{ fields: ["debtor"] }, { fields: ["lines.sku"] }],
  },
  "line.Items": { schema: { type: "object", properties: {} } },
  line_Items: { schema: { type: "object", properties: {} } },
};
for (const adapter of ["postgres", "mongo"])
  test(`${adapter}: a generation's storage is named after its group, model and generation`, async () => {
    const namespace = `names_${Date.now()}`;
    const store =
      adapter === "postgres"
        ? createPostgresReadmodelStore({ pool, schema: namespace })
        : createMongoReadmodelStore({ client, database: namespace });
    const long = `g${"x".repeat(79)}`;
    try {
      await store.register("Billing.EU", namedModels);
      await store.register(long, { invoices: namedModels.invoices });
      const live = tagOf(await store.activeGeneration("Billing.EU"));
      const shadowId = await store.createGeneration("Billing.EU"),
        shadow = tagOf(shadowId);
      const longTag = tagOf(await store.activeGeneration(long));
      if (adapter === "postgres") {
        const tables = await tablesIn(namespace);
        for (const tag of [live, shadow])
          expect(tables).toEqual(
            expect.arrayContaining([
              `rm_billing_eu_invoices_${tag}`,
              `rm_billing_eu_invoices_${tag}__lines`,
              `rm_billing_eu_line_items_${tag}`,
              // "line_Items" reads the same as "line.Items" once sanitized, so it keeps a hash.
              expect.stringMatching(
                new RegExp(`^rm_billing_eu_line_items_[0-9a-f]{8}_${tag}$`),
              ),
            ]),
          );
        const longTables = tables.filter((t) => t.startsWith(`rm_gx`));
        expect(longTables).toHaveLength(2);
        for (const table of longTables) {
          expect(table.length).toBeLessThanOrEqual(63);
          expect(table).toMatch(/^rm_gx+_[0-9a-f]{8}/);
        }
        expect(longTables.some((t) => t.endsWith(`_${longTag}`))).toBe(true);
      } else {
        const database = `${namespace}-Billing_EU-readmodels`;
        const collections = await collectionsIn(database);
        expect(collections).toHaveLength(6);
        expect(collections).toEqual(
          expect.arrayContaining(
            [live, shadow].flatMap((tag) => [
              `invoices_${tag}`,
              `line_Items_${tag}`,
              // "line_Items" reads the same as "line.Items" once sanitized, so it keeps a hash.
              expect.stringMatching(
                new RegExp(`^line_Items_[0-9a-f]{8}_${tag}$`),
              ),
            ]),
          ),
        );
        const { databases } = await client.db().admin().listDatabases();
        const longDatabase = databases
          .map((d) => d.name)
          .find((name) => name.startsWith(`${namespace}-gx`))!;
        expect(longDatabase.length).toBeLessThanOrEqual(63);
        expect(longDatabase).toMatch(/_[0-9a-f]{8}$/);
        expect(await collectionsIn(longDatabase)).toEqual([
          `invoices_${longTag}`,
        ]);
        // The group and generation records stay in the configured database.
        expect(await collectionsIn(namespace)).toEqual([
          "event_fabric_generations",
          "event_fabric_groups",
        ]);
      }
      // The shadow serves after the swap; dropping the old generation removes only its storage.
      const old = await store.activeGeneration("Billing.EU");
      await store.fence("Billing.EU", 1);
      await store.batch(
        "Billing.EU",
        1,
        p(1),
        [
          {
            model: "invoices",
            id: "a",
            document: { debtor: "Ada", lines: [{ sku: "s1" }] },
          },
        ],
        { generation: shadowId },
      );
      await store.swapGeneration("Billing.EU", shadowId);
      await store.dropGeneration("Billing.EU", old);
      expect(
        await store.findOne("Billing.EU", "invoices", { _id: "a" }),
      ).toEqual({
        _id: "a",
        debtor: "Ada",
        lines: [{ sku: "s1" }],
      });
      const left =
        adapter === "postgres"
          ? await tablesIn(namespace)
          : await collectionsIn(`${namespace}-Billing_EU-readmodels`);
      expect(left.some((name) => name.includes(live))).toBe(false);
      expect(left.some((name) => name.includes(shadow))).toBe(true);
    } finally {
      if (adapter === "postgres")
        await pool.query(`DROP SCHEMA "${namespace}" CASCADE`);
      else {
        await client.db(namespace).dropDatabase();
        await dropGroupDatabases(client);
      }
    }
  });
test("mongo: a copied store refuses its original's databases until its records are repointed", async () => {
  const original = `copied_from_${Date.now()}`,
    copy = `${original}_copy`;
  const models = {
    m: { schema: { type: "object", properties: { name: { type: "string" } } } },
  };
  try {
    const first = createMongoReadmodelStore({ client, database: original });
    await first.register("g", models);
    await first.fence("g", 1);
    await first.batch("g", 1, p(1), [
      { model: "m", id: "a", document: { name: "Ada" } },
    ]);
    // Copy the group and generation records, as a database copy would, without repointing them.
    for (const name of ["event_fabric_groups", "event_fabric_generations"]) {
      const rows = await client.db(original).collection<any>(name).find().toArray();
      await client.db(copy).collection<any>(name).insertMany(rows);
    }
    const copied = createMongoReadmodelStore({ client, database: copy });
    await expect(copied.find("g", "m")).rejects.toThrow(
      `database ${original}-g-readmodels, outside this store's database ${copy}`,
    );
    await client
      .db(copy)
      .collection<any>("event_fabric_generations")
      .updateMany({}, { $set: { "value.database": `${copy}-g-readmodels` } });
    expect(await createMongoReadmodelStore({ client, database: copy }).find("g", "m")).toEqual([]);
    expect(await first.find("g", "m")).toEqual([{ _id: "a", name: "Ada" }]);
  } finally {
    await client.db(original).dropDatabase();
    await client.db(copy).dropDatabase();
    await dropGroupDatabases(client);
  }
});
for (const adapter of ["postgres", "mongo"])
  test(`${adapter}: generations created before readable names keep their hashed storage`, async () => {
    const namespace = `legacy_names_${Date.now()}`;
    const open = () =>
      adapter === "postgres"
        ? createPostgresReadmodelStore({ pool, schema: namespace })
        : createMongoReadmodelStore({ client, database: namespace });
    const models = {
      m: {
        schema: { type: "object", properties: { name: { type: "string" } } },
        indexes: [{ fields: ["name"] }],
      },
    };
    try {
      const first = open();
      await first.register("legacy", models);
      await first.fence("legacy", 1);
      await first.batch("legacy", 1, p(1), [
        { model: "m", id: "a", document: { name: "Ada" } },
      ]);
      const id = await first.activeGeneration("legacy"),
        hashed = hashOf(`legacy.${id}.m`, 32);
      // Rewrite the generation as an earlier version stored it: hashed names, no recorded names.
      if (adapter === "postgres") {
        await pool.query(
          `ALTER TABLE "${namespace}"."rm_legacy_m_${tagOf(id)}" RENAME TO "rm_${hashed}"`,
        );
        await pool.query(
          `ALTER TABLE "${namespace}"."rm_${hashed}" ALTER COLUMN "_id" TYPE text COLLATE "default"`,
        );
        await pool.query(
          `UPDATE "${namespace}".ef_rm_generations SET data=data-'names' WHERE group_name='legacy'`,
        );
      } else {
        await client.db("admin").command({
          renameCollection: `${namespace}-legacy-readmodels.m_${tagOf(id)}`,
          to: `${namespace}.event_fabric_rm_${hashed}`,
        });
        await client
          .db(namespace)
          .collection<any>("event_fabric_generations")
          .updateMany(
            { _id: `legacy:${id}` },
            { $unset: { "value.names": "", "value.database": "" } },
          );
      }
      const store = open();
      await store.register("legacy", models);
      expect(await store.find("legacy", "m")).toEqual([
        { _id: "a", name: "Ada" },
      ]);
      await store.batch("legacy", 1, p(2), [
        { model: "m", id: "b", document: { name: "Bo" } },
      ]);
      expect(await store.count("legacy", "m", { name: { gte: "A" } })).toBe(2);
      if (adapter === "postgres") {
        // Startup gave the old table an index that serves `_id` in binary order.
        const index = await pool.query(
          `SELECT i.indisvalid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
           JOIN pg_index i ON i.indexrelid=c.oid WHERE n.nspname=$1 AND c.relname=$2`,
          [namespace, `rm_${hashed}__id_c`],
        );
        expect(index.rows).toEqual([{ indisvalid: true }]);
      }
      // A rebuild moves the group to readable names and drops the hashed storage.
      const shadow = await store.createGeneration("legacy");
      await store.batch(
        "legacy",
        1,
        p(2),
        [
          { model: "m", id: "a", document: { name: "Ada" } },
          { model: "m", id: "b", document: { name: "Bo" } },
        ],
        { generation: shadow },
      );
      await store.swapGeneration("legacy", shadow);
      await store.dropGeneration("legacy", id);
      expect(await store.count("legacy", "m")).toBe(2);
      if (adapter === "postgres")
        expect(await tablesIn(namespace)).toEqual([
          `rm_legacy_m_${tagOf(shadow)}`,
        ]);
      else {
        expect(
          (await collectionsIn(namespace)).filter((c) => c.includes("_rm_")),
        ).toEqual([]);
        expect(await collectionsIn(`${namespace}-legacy-readmodels`)).toEqual([
          `m_${tagOf(shadow)}`,
        ]);
      }
    } finally {
      if (adapter === "postgres")
        await pool.query(`DROP SCHEMA "${namespace}" CASCADE`);
      else {
        await client.db(namespace).dropDatabase();
        await dropGroupDatabases(client);
      }
    }
  });
test("mongo: a schema migration's database is the group's database", async () => {
  const namespace = `migration_db_${Date.now()}`;
  const store = createMongoReadmodelStore({ client, database: namespace });
  const before = {
    m: { schema: { type: "object", properties: { a: { type: "string" } } } },
  };
  const after = {
    m: {
      schema: {
        type: "object",
        properties: { a: { type: "string" }, b: { type: "string" } },
      },
    },
  };
  try {
    const { hash: from } = await store.register("g", before);
    const id = await store.activeGeneration("g");
    let seen: string | undefined;
    // The hash depends only on the definitions, so any store computes it.
    const { hash: to } = await createMemoryReadmodelStore().register(
      "g",
      after,
    );
    await createMongoReadmodelStore({ client, database: namespace }).register(
      "g",
      after,
      {
        migrations: [
          {
            from,
            to,
            up: async (native: any) => {
              seen = native.database.databaseName;
              await native.database
                .collection(`m_${tagOf(id)}`)
                .insertOne({ _id: "x", a: "1", b: "2" });
            },
          },
        ],
      },
    );
    expect(seen).toBe(`${namespace}-g-readmodels`);
    expect(await store.findOne("g", "m", { _id: "x" })).toEqual({
      _id: "x",
      a: "1",
      b: "2",
    });
  } finally {
    await client.db(namespace).dropDatabase();
    await dropGroupDatabases(client);
  }
});

/** A store whose document reads are counted: rows of `SELECT *` on Postgres, documents of `find` and `getMore` on Mongo. */
const countingStore = (adapter: string, namespace: string) => {
  const counter = {
    fetched: 0,
    selects: [] as { text: string; values: any[] }[],
  };
  if (adapter === "postgres") {
    const counted = new Pool({ connectionString: postgresUrl });
    const connect = counted.connect.bind(counted),
      wrapped = new WeakSet<object>();
    counted.connect = (async () => {
      const connection: any = await connect();
      if (!wrapped.has(connection)) {
        wrapped.add(connection);
        const query = connection.query.bind(connection);
        connection.query = async (text: any, values?: any[]) => {
          const result = await query(text, values);
          if (typeof text === "string" && text.startsWith("SELECT * FROM")) {
            counter.fetched += result.rowCount ?? 0;
            counter.selects.push({ text, values: values ?? [] });
          }
          return result;
        };
      }
      return connection;
    }) as any;
    return {
      counter,
      store: createPostgresReadmodelStore({ pool: counted, schema: namespace }),
      close: async () => {
        await counted.end();
        await pool.query(`DROP SCHEMA "${namespace}" CASCADE`);
      },
    };
  }
  const monitored = new MongoClient(mongoUrl, { monitorCommands: true });
  monitored.on("commandSucceeded", (event: any) => {
    // Documents only: the group and generation records live in the records database.
    if (
      ["find", "getMore"].includes(event.commandName) &&
      event.reply.cursor?.ns?.includes("-readmodels.")
    )
      counter.fetched += (
        event.reply.cursor.firstBatch ??
        event.reply.cursor.nextBatch ??
        []
      ).length;
  });
  return {
    counter,
    store: createMongoReadmodelStore({
      client: monitored,
      database: namespace,
    }),
    close: async () => {
      await monitored.db(namespace).dropDatabase();
      await dropGroupDatabases(monitored);
      await monitored.close();
    },
  };
};
for (const adapter of ["postgres", "mongo"])
  test(`${adapter}: find with a size and count read pages, not every match`, async () => {
    const { counter, store, close } = countingStore(
      adapter,
      `bounded_${Date.now()}`,
    );
    try {
      await store.register("g", {
        m: {
          schema: {
            type: "object",
            properties: {
              n: { type: "integer" },
              status: { type: "string" },
              note: { "x-store": "json" },
            },
          },
          indexes: [{ fields: ["status"] }],
        },
      });
      await store.fence("g", 1);
      for (let start = 0; start < 100_000; start += 10_000)
        await store.batch(
          "g",
          1,
          p(start + 1),
          Array.from({ length: 10_000 }, (_, i) => ({
            model: "m",
            id: `d${String(start + i).padStart(6, "0")}`,
            document: {
              n: start + i,
              status: (start + i) % 2 ? "open" : "closed",
              note: { even: (start + i) % 2 === 0 },
            },
          })),
        );
      const read = async <T>(work: () => Promise<T>) => {
        counter.fetched = 0;
        return { result: await work(), fetched: counter.fetched };
      };
      const newest = await read(() =>
        store.find("g", "m", { status: "open" }, { sort: { n: -1 }, size: 20 }),
      );
      expect(newest.result.map((d) => d.n)).toEqual(
        Array.from({ length: 20 }, (_, i) => 99_999 - 2 * i),
      );
      expect(newest.fetched).toBeLessThanOrEqual(100);
      const later = await read(() =>
        store.find("g", "m", {}, { sort: { n: 1 }, from: 500, size: 10 }),
      );
      expect(later.result.map((d) => d.n)).toEqual(
        Array.from({ length: 10 }, (_, i) => 500 + i),
      );
      expect(later.fetched).toBeLessThanOrEqual(510);
      const first = await read(() =>
        store.findOne("g", "m", { status: "closed" }, { sort: { n: 1 } }),
      );
      expect(first.result?.n).toBe(0);
      expect(first.fetched).toBeLessThanOrEqual(100);
      // Exact filters count in the database, without reading documents.
      for (const [filter, expected] of [
        [{}, 100_000],
        [{ status: "open" }, 50_000],
        [{ n: { gte: 99_000 }, status: { in: ["open"] } }, 500],
        [{ oneOf: [{ n: { lt: 10 } }, { status: { ne: "open" } }] }, 50_005],
      ] as const) {
        const counted = await read(() => store.count("g", "m", filter));
        expect(counted).toEqual({ result: expected, fetched: 0 });
      }
      // A json field is verified by the shared evaluator, page by page.
      expect(await store.count("g", "m", { note: { even: true } })).toBe(
        50_000,
      );
      if (adapter === "postgres") {
        // Keyset pages on _id and on a declared string index are index range scans.
        counter.selects = [];
        let streamed = 0;
        for await (const _ of store.stream(
          "g",
          "m",
          {},
          { sort: { _id: 1 }, size: 150 },
        ))
          streamed++;
        await store.find("g", "m", {}, { sort: { status: 1 }, size: 5 });
        expect(streamed).toBe(150);
        expect(counter.selects).toHaveLength(3);
        const connection = await pool.connect();
        try {
          for (const { text, values } of counter.selects) {
            await connection.query("BEGIN");
            await connection.query("SET LOCAL enable_seqscan = off");
            const [{ "QUERY PLAN": plan }] = (
              await connection.query(`EXPLAIN (FORMAT JSON) ${text}`, values)
            ).rows;
            await connection.query("ROLLBACK");
            const nodes = JSON.stringify(plan).match(/"Node Type":"[^"]+"/g);
            expect(nodes).toEqual(
              expect.arrayContaining([expect.stringMatching(/Index/)]),
            );
            expect(nodes!.some((node) => node.includes("Sort"))).toBe(false);
          }
        } finally {
          connection.release();
        }
      }
    } finally {
      await close();
    }
  }, 120_000);
test("mongo: a stream outlives the server's transaction lifetime", async () => {
  const namespace = `long_stream_${Date.now()}`;
  const store = createMongoReadmodelStore({ client, database: namespace }),
    admin = client.db("admin");
  const { transactionLifetimeLimitSeconds: lifetime } = await admin.command({
    getParameter: 1,
    transactionLifetimeLimitSeconds: 1,
  });
  try {
    await store.register("g", {
      m: { schema: { type: "object", properties: { n: { type: "integer" } } } },
    });
    await store.fence("g", 1);
    await store.batch(
      "g",
      1,
      p(1),
      Array.from({ length: 300 }, (_, n) => ({
        model: "m",
        id: `d${String(n).padStart(3, "0")}`,
        document: { n },
      })),
    );
    await admin.command({
      setParameter: 1,
      transactionLifetimeLimitSeconds: 1,
    });
    const seen: number[] = [];
    for await (const doc of store.stream("g", "m", {}, { sort: { n: 1 } })) {
      seen.push(doc.n as number);
      // One page is 100 documents; the whole stream takes over 3 s.
      if (seen.length % 100 === 0)
        await new Promise((resolve) => setTimeout(resolve, 1100));
    }
    expect(seen).toEqual(Array.from({ length: 300 }, (_, n) => n));
  } finally {
    await admin.command({
      setParameter: 1,
      transactionLifetimeLimitSeconds: lifetime,
    });
    await client.db(namespace).dropDatabase();
    await dropGroupDatabases(client);
  }
});
test("mongo: a stream past the snapshot history window fails with a clear error", async () => {
  const namespace = `expired_stream_${Date.now()}`;
  const store = createMongoReadmodelStore({ client, database: namespace }),
    admin = client.db("admin");
  const { minSnapshotHistoryWindowInSeconds: window } = await admin.command({
    getParameter: 1,
    minSnapshotHistoryWindowInSeconds: 1,
  });
  try {
    await store.register("g", {
      m: { schema: { type: "object", properties: { n: { type: "integer" } } } },
    });
    await store.fence("g", 1);
    await store.batch(
      "g",
      1,
      p(1),
      Array.from({ length: 200 }, (_, n) => ({
        model: "m",
        id: `d${String(n).padStart(3, "0")}`,
        document: { n },
      })),
    );
    await admin.command({
      setParameter: 1,
      minSnapshotHistoryWindowInSeconds: 1,
    });
    const reading = (async () => {
      let seen = 0;
      for await (const _ of store.stream("g", "m", {}, { sort: { n: 1 } }))
        if (++seen === 100)
          // Later writes move the oldest kept snapshot past this stream's.
          for (let i = 0; i < 30; i++) {
            await store.batch("g", 1, p(2 + i), [
              { model: "m", id: "w", document: { n: i } },
            ]);
            await new Promise((resolve) => setTimeout(resolve, 100));
          }
    })();
    await expect(reading).rejects.toThrow(
      /outlived the server's snapshot history/,
    );
  } finally {
    await admin.command({
      setParameter: 1,
      minSnapshotHistoryWindowInSeconds: window,
    });
    await client.db(namespace).dropDatabase();
    await dropGroupDatabases(client);
  }
});
for (const adapter of ["memory", "postgres", "mongo"])
  test(`${adapter}: an import loads documents only into a pending generation without a Checkpoint, under the group's epoch`, async () => {
    const group = `import_${adapter}_${Date.now()}`;
    const store =
      adapter === "memory"
        ? createMemoryReadmodelStore()
        : adapter === "postgres"
          ? createPostgresReadmodelStore({ pool, schema: "readmodel_contract" })
          : createMongoReadmodelStore({ client, database: "readmodel_contract" });
    await store.register(group, { m: { schema } });
    await store.fence(group, 1);
    const active = await store.activeGeneration(group);
    const generation = await store.createGeneration(group, "r1");
    const load = (
      epoch: number,
      target: string,
      reconstruction?: string,
    ) =>
      store.load!(
        group,
        epoch,
        target,
        [
          {
            model: "m",
            id: "a",
            document: { name: "a", when: "2026-10-17T00:00:00.000Z" },
          },
          { model: "m", id: "b", document: { name: "b", tags: ["x"] } },
        ],
        { reconstruction },
      );
    await expect(load(1, active)).rejects.toThrow("pending generation");
    await expect(load(1, generation, "r2")).rejects.toThrow();
    await expect(load(0, generation, "r1")).rejects.toThrow(/epoch/i);
    await load(1, generation, "r1");
    await expect(
      store.load!(group, 1, generation, [
        { model: "m", id: "c", document: { name: 3 } as any },
      ], { reconstruction: "r1" }),
    ).rejects.toThrow();
    // Loading writes no Checkpoint; an empty batch sets it, after which loading is refused.
    expect(await store.checkpoint(group, { generation })).toBeNull();
    await store.batch(group, 1, p(5), [], { generation, reconstruction: "r1" });
    expect(await store.checkpoint(group, { generation })).toEqual(p(5));
    await expect(load(1, generation, "r1")).rejects.toThrow("Checkpoint");
    await store.swapGeneration(group, generation, undefined, "r1");
    const documents = await store.find(group, "m", {}, { sort: { _id: 1 } });
    expect(documents.map((d) => d._id)).toEqual(["a", "b"]);
    expect(documents[0]!.when).toEqual(new Date("2026-10-17T00:00:00.000Z"));
    expect(documents[1]!.owner).toBe(adapter === "postgres" ? null : undefined);
    expect(documents[1]!.tags).toEqual(["x"]);
    await store.close();
  }, 30000);
