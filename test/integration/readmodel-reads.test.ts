import { afterAll, expect, it } from "vitest";
import { Pool, type PoolClient } from "pg";
import { createPostgresReadmodelStore } from "../../src/adapters/postgres-readmodel.js";

// A Readmodel read is one transaction that looks up the group, not its generation.
const pool = new Pool({
  connectionString:
    process.env.EVENTFABRIC_POSTGRES_URL ??
    "postgres://eventfabric:eventfabric@127.0.0.1:55439/eventfabric",
});
let statements: string[] = [];
pool.on("connect", (client: PoolClient) => {
  const query = client.query.bind(client) as (...args: any[]) => any;
  (client as any).query = (...args: any[]) => {
    statements.push(typeof args[0] === "string" ? args[0] : args[0]?.text);
    return query(...args);
  };
});
const schema = `readmodel_reads_${Date.now()}`;
afterAll(async () => {
  await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  await pool.end();
});
const p = (n: number) => ({ transactionId: String(n), sequence: "1" });
const definitions = {
  order: {
    schema: {
      type: "object",
      properties: { title: { type: "string" }, total: { type: "number" } },
    },
  },
};

it("a find reads the group record and its rows, the active generation comes from memory, and a swap elsewhere is seen", async () => {
  const replica = createPostgresReadmodelStore({ pool, schema }),
    other = createPostgresReadmodelStore({ pool, schema });
  await replica.register("orders", definitions);
  await replica.fence("orders", 1);
  await replica.batch("orders", 1, p(1), [
    { model: "order", id: "one", document: { title: "first", total: 1 } },
    { model: "order", id: "two", document: { title: "second", total: 2 } },
  ]);
  expect(await replica.findOne("orders", "order", { _id: "one" })).toEqual({
    _id: "one",
    title: "first",
    total: 1,
  });
  statements = [];
  expect(await replica.find("orders", "order", { total: { gte: 2 } })).toEqual([
    { _id: "two", title: "second", total: 2 },
  ]);
  // Layout lock, BEGIN, the group, the rows, COMMIT, unlock: no generation lookup.
  expect(statements).toHaveLength(6);
  expect(statements.filter((s) => /ef_rm_generations/.test(s))).toEqual([]);
  // Another replica rebuilds and swaps: this one reads the new generation on its next read.
  await other.register("orders", definitions);
  await other.fence("orders", 2);
  const generation = await other.createGeneration("orders");
  await other.batch(
    "orders",
    2,
    p(2),
    [{ model: "order", id: "three", document: { title: "third", total: 3 } }],
    { generation },
  );
  await other.swapGeneration("orders", generation);
  expect(
    (await replica.find("orders", "order")).map((doc) => doc._id),
  ).toEqual(["three"]);
  expect(await replica.count("orders", "order", { title: "third" })).toBe(1);
  await replica.close();
  await other.close();
});
