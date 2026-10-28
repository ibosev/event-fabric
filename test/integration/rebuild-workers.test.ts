import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { promisify } from "node:util";
import { Pool } from "pg";
import { afterAll, beforeAll, expect, test } from "vitest";
import { createPostgresReadmodelStore } from "../../src/adapters/postgres-readmodel.js";
import { createMemoryReadmodelStore } from "../../src/adapters/memory-readmodel.js";
import { createPostgresEventStore } from "../../src/event-store/postgres.js";
import { createApplication } from "../../src/runtime/index.js";
import { StaleEpochError } from "../../src/errors.js";

// Fold workers load the built package, so these tests run the CLI from `dist` (npm run build).
const url =
  process.env.EVENTFABRIC_POSTGRES_URL ??
  "postgres://eventfabric:eventfabric@127.0.0.1:55439/eventfabric";
const pool = new Pool({ connectionString: url });
const events = createPostgresEventStore({
  pool,
  schema: "rebuild_workers_events",
});
const models = createPostgresReadmodelStore({
  pool,
  schema: "rebuild_workers_readmodels",
});
let directory: string, configPath: string;
beforeAll(async () => {
  await pool.query(
    "DROP SCHEMA IF EXISTS rebuild_workers_events CASCADE; DROP SCHEMA IF EXISTS rebuild_workers_readmodels CASCADE",
  );
  await events.migrate();
  directory = await mkdtemp(join(tmpdir(), "eventfabric-workers-"));
  configPath = join(directory, "config.mjs");
  const dist = join(process.cwd(), "dist");
  await writeFile(
    configPath,
    `import { compile, createPostgresEventStore } from ${JSON.stringify(`${dist}/index.js`)};
import { createPostgresReadmodelStore } from ${JSON.stringify(`${dist}/postgres.js`)};
import { createInMemoryTransport } from ${JSON.stringify(`${dist}/testing.js`)};
import { threadId } from "node:worker_threads";
const url = ${JSON.stringify(url)};
// Records the thread that folded it, so a test sees which groups rebuilt in worker threads.
const threads = (reads) => ({
  reads,
  store: createPostgresReadmodelStore({ connectionString: url, schema: "rebuild_workers_readmodels" }),
  models: {
    order: {
      schema: { type: "object", properties: { thread: { type: "integer" }, count: { type: "integer" } } },
      initialState: { thread: 0, count: 0 },
      reactions: {
        "sales.order.opened": (event, vm) => {
          vm.set("thread", threadId);
          vm.set("count", vm.get("count") + 1);
        },
      },
    },
  },
});
export const config = () => ({
  name: "workers",
  store: createPostgresEventStore({ connectionString: url, schema: "rebuild_workers_events" }),
  transport: createInMemoryTransport(),
  aggregates: [compile("sales", "order", { commands: {}, events: { opened: () => {} } })],
  readmodels: {
    first: threads(),
    second: threads(["first"]),
    totals: {
      partitions: 3,
      store: createPostgresReadmodelStore({ connectionString: url, schema: "rebuild_workers_readmodels" }),
      models: {
        order: {
          schema: { type: "object", properties: { total: { type: "number" }, count: { type: "integer" } } },
          initialState: { total: 0, count: 0 },
          reactions: {
            "sales.order.opened": (event, vm) => {
              vm.set("total", vm.get("total") + event.payload.amount);
              vm.set("count", vm.get("count") + 1);
            },
          },
        },
      },
    },
  },
});
`,
  );
});
afterAll(async () => {
  await rm(directory, { recursive: true, force: true });
  await models.close();
  await pool.end();
});
const cli = async (...args: string[]) =>
  JSON.parse(
    (
      await promisify(execFile)(
        process.execPath,
        ["dist/cli/main.js", ...args, "--config", configPath],
        { env: { ...process.env, LOG_LEVEL: "warn" } },
      )
    ).stdout
      .trim()
      // The CLI prints its result last, after any log records.
      .split("\n")
      .at(-1)!,
  );
const slices = async (generation: string) =>
  (
    await pool.query(
      "SELECT name, data FROM rebuild_workers_readmodels.ef_rm_groups WHERE name LIKE $1 ORDER BY name",
      [`totals@${generation}#%`],
    )
  ).rows.map((row) => [row.name, row.data.count, row.data.checkpoint]);
// 60 orders of 100 Events: two rebuild pages, split between three workers by document.
const appendOrders = async (from: number) => {
  for (let order = 0; order < 60; order++)
    await events.appendToStream(
      { context: "sales", aggregate: "order", id: `order-${order}` },
      from,
      Array.from({ length: 100 }, (_, i) => ({
        id: `order-${order}-${from + i}`,
        name: "opened",
        payload: { amount: order + 1 },
        metadata: {},
        schemaVersion: 1,
        timestamp: "2026-10-19T00:00:00.000Z",
        commandId: `order-${order}-${from + i}`,
        correlationId: `order-${order}`,
      })),
    );
};
const expectTotals = async (count: number) => {
  const documents = await models.find("totals", "order", {}, { size: 100 });
  expect(documents).toHaveLength(60);
  for (const document of documents) {
    const amount = Number(String(document._id).split("-")[1]) + 1;
    expect(document).toEqual({
      _id: document._id,
      total: amount * count,
      count,
    });
  }
};

test("only stores with Partition locks have rebuild slices, which keep their own Checkpoints and fences", async () => {
  expect(createMemoryReadmodelStore().slices).toBeUndefined();
  const store = createPostgresReadmodelStore({
    pool,
    schema: "rebuild_workers_slices",
  });
  await pool.query("DROP SCHEMA IF EXISTS rebuild_workers_slices CASCADE");
  const definitions = {
    m: { schema: { type: "object", properties: { v: { type: "integer" } } } },
  };
  try {
    await store.register("g", definitions as any);
    const generation = await store.createGeneration("g");
    expect(await store.slices!("g", generation)).toBeNull();
    expect(await store.slices!("g", generation, 2)).toEqual([null, null]);
    await expect(store.slices!("g", generation, 3)).rejects.toThrow(
      /runs in 2 slices/,
    );
    const position = { transactionId: "5", sequence: "1" };
    await store.batch(
      "g",
      0,
      position,
      [{ model: "m", id: "a", document: { v: 1 } }],
      {
        generation,
        slice: 1,
      },
    );
    expect(await store.slices!("g", generation)).toEqual([null, position]);
    expect(await store.checkpoint("g", { generation })).toBeNull();
    expect(await store.findOne("g", "m", { _id: "a" }, { generation })).toEqual(
      { _id: "a", v: 1 },
    );
    await expect(
      store.batch("g", 0, position, [], { generation, slice: 2 }),
    ).rejects.toThrow(/Unknown rebuild slice 2/);
    await store.fence("g", 1);
    await expect(
      store.batch("g", 0, { transactionId: "6", sequence: "1" }, [], {
        generation,
        slice: 0,
      }),
    ).rejects.toBeInstanceOf(StaleEpochError);
    await store.batch("g", 1, position, [], { generation });
    const other = (await store.pendingGeneration("g"))!;
    await store.dropGeneration("g", other);
    const checkpointed = await store.createGeneration("g");
    await store.batch("g", 1, position, [], { generation: checkpointed });
    await expect(store.slices!("g", checkpointed, 2)).rejects.toThrow(
      /without a Checkpoint/,
    );
  } finally {
    await store.close();
  }
});

test("a CLI rebuild of a partitioned group folds in one worker per Partition and hands over to the single loop", async () => {
  await appendOrders(0);
  const result = await cli("rebuild", "totals");
  expect(result.swapped).toBe(true);
  await expectTotals(100);
  const head = await events.head();
  expect(await slices(result.generation)).toEqual(
    [0, 1, 2].map((slice) => [`totals@${result.generation}#${slice}`, 3, head]),
  );
  expect(await models.checkpoint("totals")).toEqual(head);
});

test("a pending sliced rebuild resumes only with workers and the same Partition count", async () => {
  await appendOrders(100);
  // Each Application closes the stores of its own configuration.
  const { config } = await import(pathToFileURL(configPath).href);
  // Stopped right after its slices start: the generation has slices and no Checkpoint.
  const stopped = createApplication({
    ...config(),
    configModule: pathToFileURL(configPath).href,
  });
  try {
    await expect(
      stopped.rebuild("totals", {
        swap: "manual",
        signal: AbortSignal.abort(),
      }),
    ).rejects.toThrow(/abort/i);
  } finally {
    await stopped.close();
  }
  const pending = (await models.pendingGeneration("totals"))!;
  expect(
    (await slices(pending)).map(([, count, checkpoint]) => [count, checkpoint]),
  ).toEqual([
    [3, undefined],
    [3, undefined],
    [3, undefined],
  ]);
  const withoutWorkers = createApplication(config());
  try {
    await expect(withoutWorkers.swapRebuild("totals")).rejects.toThrow(
      /folds in 3 worker threads/,
    );
  } finally {
    await withoutWorkers.close();
  }
  expect((await cli("rebuild", "--swap", "totals")).generation).toBe(pending);
  expect(await models.activeGeneration("totals")).toBe(pending);
  await expectTotals(200);
  const head = await events.head();
  expect((await slices(pending)).map(([, , checkpoint]) => checkpoint)).toEqual(
    [head, head, head],
  );
});

test("a CLI rebuild of several groups rebuilds each in its own worker thread, after the groups it reads", async () => {
  for (let i = 0; i < 5; i++)
    await events.appendToStream(
      { context: "sales", aggregate: "order", id: `threads-${i}` },
      0,
      [0, 1].map((k) => ({
        id: `threads-${i}-${k}`,
        name: "opened",
        payload: { amount: 1 },
        metadata: {},
        schemaVersion: 1,
        timestamp: "2026-10-21T00:00:00.000Z",
        commandId: `threads-${i}-${k}`,
        correlationId: `threads-${i}`,
      })),
    );
  expect(await cli("rebuild", "second", "first")).toMatchObject({
    first: { swapped: true },
    second: { swapped: true },
  });
  const threadsOf = async (group: string) => {
    const documents = await models.find(group, "order", {}, { size: 1000 });
    expect(
      documents.find((d) => d._id === "threads-0")?.count,
    ).toBe(2);
    return new Set(documents.map((d) => d.thread));
  };
  const first = await threadsOf("first"),
    second = await threadsOf("second");
  // One worker thread each, never the main thread (0).
  expect(first.size).toBe(1);
  expect(second.size).toBe(1);
  expect(first.has(0) || second.has(0)).toBe(false);
  expect([...first][0]).not.toBe([...second][0]);
});
