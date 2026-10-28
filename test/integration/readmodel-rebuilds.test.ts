import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { MongoClient } from "mongodb";
import pino from "pino";
import { afterAll, expect, test } from "vitest";
import { compile } from "../../src/authoring/index.js";
import { createApplication } from "../../src/runtime/index.js";
import { createPostgresEventStore } from "../../src/event-store/postgres.js";
import { createPostgresReadmodelStore } from "../../src/adapters/postgres-readmodel.js";
import { createMongoReadmodelStore } from "../../src/adapters/mongo-readmodel.js";
import { dropGroupDatabases } from "./mongo.js";
import { createInMemoryTransport } from "../../src/transport/memory.js";

const pool = new Pool({
  connectionString:
    process.env.EVENTFABRIC_POSTGRES_URL ??
    "postgres://eventfabric:eventfabric@127.0.0.1:55439/eventfabric",
});
const client = new MongoClient(
  process.env.EVENTFABRIC_MONGO_URL ??
    "mongodb://127.0.0.1:57019/?replicaSet=rs0&directConnection=true",
);
afterAll(async () => {
  await pool.end();
  await client.close();
});

test.each(["postgres", "mongo"])(
  "%s: filtered rebuild, query Identities and live catch-up use one chronological fold",
  async (adapter) => {
    const schema = `rebuild_${randomUUID().replaceAll("-", "")}`;
    const store = createPostgresEventStore({ pool, schema });
    const models =
      adapter === "postgres"
        ? createPostgresReadmodelStore({ pool, schema })
        : createMongoReadmodelStore({ client, database: schema });
    let findOnes = 0;
    const findOne = models.findOne.bind(models);
    models.findOne = (...args) => {
      findOnes++;
      return findOne(...args);
    };
    const reaction = async (event: any, vm: any, app: any) => {
      if (event.payload.slow)
        await new Promise((resolve) => setTimeout(resolve, 60));
      if (event.payload.destroy) return vm.destroy();
      const extra = event.payload.readB
        ? (await app.getReadmodels().totals.total.findOne({ _id: "b" })).total
        : 0;
      vm.set("total", vm.get("total") + event.payload.amount + extra);
    };
    const app = createApplication({
      name: "durable-rebuild",
      store,
      transport: createInMemoryTransport(),
      pollInterval: 5,
      logger: pino({ level: "silent" }),
      aggregates: [
        compile("sales", "order", { events: { changed: () => {} } }),
        compile("payments", "receipt", { events: { changed: () => {} } }),
        compile("audit", "record", {
          events: {
            observed: {
              handler: () => {},
              schemaVersion: 2,
              upcasters: {
                1: () => {
                  throw new Error("irrelevant payload must stay unread");
                },
              },
            },
          },
        }),
      ],
      readmodels: {
        totals: {
          store: models,
          models: {
            total: {
              schema: {
                type: "object",
                properties: { total: { type: "number" } },
              },
              initialState: { total: 0 },
              identity: {
                "sales.order.changed": (event) => event.payload.identity,
                "payments.receipt.changed": (event) => event.payload.identity,
              },
              reactions: {
                "sales.order.changed": reaction,
                "payments.receipt.changed": reaction,
              },
            },
          },
        },
      },
    });
    const append = async (
      context: string,
      aggregate: string,
      name: string,
      payload = {},
    ) => {
      const id = randomUUID();
      return (
        await store.appendToStream({ context, aggregate, id }, 0, [
          {
            id,
            name,
            payload,
            metadata: {},
            schemaVersion: 1,
            timestamp: "2026-10-17T00:00:00.000Z",
            commandId: id,
            correlationId: id,
          },
        ])
      )[0]!;
    };
    try {
      await store.migrate();
      await append("audit", "record", "observed");
      await append("sales", "order", "changed", {
        identity: ["a", "b"],
        amount: 1,
        slow: true,
      });
      for (const payload of [
        { identity: { total: 1 }, amount: 2 },
        { identity: "a", destroy: true },
        { identity: { total: { gte: 3 } }, amount: 4 },
        { identity: "a", amount: 5, readB: true },
        { identity: { total: { gte: 5 } }, amount: 1 },
      ])
        await append("payments", "receipt", "changed", payload);
      const tail = await append("audit", "record", "observed");
      const prepared = await app.rebuild("totals", { swap: "manual" });
      expect(await models.find("totals", "total")).toEqual([]);
      expect(
        await models.find(
          "totals",
          "total",
          {},
          { generation: prepared.generation, sort: { _id: 1 } },
        ),
      ).toEqual([
        { _id: "a", total: 13 },
        { _id: "b", total: 8 },
      ]);
      expect(
        await models.checkpoint("totals", { generation: prepared.generation }),
      ).toEqual(tail.position);
      await append("sales", "order", "changed", {
        identity: { total: { gte: 8 } },
        amount: 1,
      });
      await app.swapRebuild("totals");
      await app.start({ only: ["readmodels:totals"] });
      const noise = await append("audit", "record", "observed");
      expect(
        (await app.readmodels.waitFor(noise.position, ["totals"])).behind,
      ).toBe(false);
      const live = await append("payments", "receipt", "changed", {
        identity: "a",
        amount: 3,
      });
      expect(
        (await app.readmodels.waitFor(live.position, ["totals"])).behind,
      ).toBe(false);
      expect(
        await models.find("totals", "total", {}, { sort: { _id: 1 } }),
      ).toEqual([
        { _id: "a", total: 17 },
        { _id: "b", total: 9 },
      ]);
      expect(findOnes).toBe(0);
    } finally {
      await app.close();
      await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
      if (adapter === "mongo") {
        await client.db(schema).dropDatabase();
        await dropGroupDatabases(client);
      }
    }
  },
);
