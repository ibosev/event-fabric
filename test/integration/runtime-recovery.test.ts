import pino from "pino";
import { Pool } from "pg";
import { afterAll, expect, test } from "vitest";
import { createApplication } from "../../src/runtime/index.js";
import { compile } from "../../src/authoring/index.js";
import { createPostgresEventStore } from "../../src/event-store/postgres.js";
import { createPostgresReadmodelStore } from "../../src/adapters/postgres-readmodel.js";
import { createInMemoryTransport } from "../../src/transport/memory.js";
import type { Outcome } from "../../src/types.js";
const pool = new Pool({
    connectionString:
      process.env.POSTGRES_URL ??
      "postgres://eventfabric:eventfabric@127.0.0.1:55439/eventfabric",
  }),
  schemas: string[] = [];
afterAll(async () => {
  for (const schema of schemas)
    await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  await pool.end();
});
const logger = pino({ level: "silent" });
const input = (value: number) => ({
  stream: { context: "sales", aggregate: "order", id: "one" },
  name: "record",
  payload: { value },
});
const accepted = (outcome: Outcome) => {
  expect(outcome.status).toBe("accepted");
  if (outcome.status !== "accepted") throw new Error("Command not accepted");
  return outcome.events;
};
async function fixture() {
  const schema = `runtime_recovery_${schemas.length}_${Date.now()}`;
  schemas.push(schema);
  const store = createPostgresEventStore({ pool, schema });
  await store.migrate();
  const models = createPostgresReadmodelStore({ pool, schema });
  const aggregate = compile("sales", "order", {
    initialState: {},
    commands: { record: [(e: any, s: any) => s.apply.recorded(e.payload)] },
    events: { recorded: (e: any, s: any) => s.set("value", e.payload.value) },
  });
  return {
    schema,
    store,
    models,
    base: {
      name: schema,
      store,
      aggregates: [aggregate],
      pollInterval: 10,
      logger,
    },
  };
}

test("Postgres runtime resumes a durable poison halt after restart, audits an exact skip, and catches the Readmodel up", async () => {
  const fixtureState = await fixture();
  const { base, store, models } = fixtureState;
  const make = () =>
    createApplication({
      ...base,
      transport: createInMemoryTransport(),
      readmodels: {
        detail: {
          store: models,
          models: {
            order: {
              schema: {
                type: "object",
                properties: { value: { type: "integer" } },
              },
              reactions: {
                "sales.order.recorded": (e: any, vm: any) => {
                  vm.set("value", e.payload.value);
                  if (e.payload.value === 2)
                    throw new Error("poison durable Event");
                },
              },
            },
          },
        },
      },
    });
  const first = make();
  await first.start();
  let poison: any, last: any;
  try {
    accepted(await first.sendCommand(input(1)));
    [poison] = accepted(await first.sendCommand(input(2)));
    [last] = accepted(await first.sendCommand(input(3)));
    await expect
      .poll(
        async () =>
          (await first.consumerStatus()).find(
            (s) => s.group === "readmodels:detail",
          )?.state,
        { timeout: 7000 },
      )
      .toBe("halted");
    expect((await models.findOne("detail", "order"))?.value).toBe(1);
  } finally {
    await first.stop();
  }
  const second = make();
  await second.start();
  try {
    await expect
      .poll(
        async () =>
          (await second.consumerStatus()).find(
            (s) => s.group === "readmodels:detail",
          )?.attempts,
        { timeout: 1000 },
      )
      .toBeGreaterThan(5);
    expect(
      (await second.consumerStatus()).find(
        (s) => s.group === "readmodels:detail",
      )?.state,
    ).toBe("halted");
    expect(
      (await second.consumerStatus()).find(
        (s) => s.group === "readmodels:detail",
      )?.eventId,
    ).toBe(poison.id);
    await expect(
      second.skipConsumer("readmodels:detail", last.id, "operator"),
    ).rejects.toThrow(/exact|halted/i);
    await second.skipConsumer("readmodels:detail", poison.id, "operator");
    second.wake();
    await expect
      .poll(() => models.checkpoint("detail"), { timeout: 5000 })
      .toEqual(last.position);
    expect((await second.readmodels.detail.order("one")).document.value).toBe(
      3,
    );
    expect(await store.deadLetters()).toEqual([
      expect.objectContaining({
        kind: "skipped-event",
        eventId: poison.id,
        who: "operator",
        position: poison.position,
      }),
    ]);
  } finally {
    await second.stop();
  }
}, 15000);

test("Postgres Saga transaction rolls back queued effects, destroy retains committed Outbox, and Worker retry survives runtime restart", async () => {
  const { base, store } = await fixture();
  let fail = true;
  const delivered: { key: string; attempt: number }[] = [];
  const config = () => ({
    ...base,
    transport: createInMemoryTransport(),
    sagas: {
      notice: {
        reactions: {
          "sales.order.recorded": (_e: any, s: any, a: any) => {
            s.set("seen", true);
            a.queue("mail", { to: "someone" });
            s.destroy();
            if (fail) throw new Error("roll back the Saga");
          },
        },
      },
    },
    workers: {
      mail: {
        handles: ["mail"],
        retry: { baseDelay: 1000, maxDelay: 1000 },
        execute: async (_message: any, ctx: any) => {
          delivered.push({ key: ctx.idempotencyKey, attempt: ctx.attempt });
          if (ctx.attempt === 1) throw new Error("provider unavailable");
        },
      },
    },
  });
  const producer = createApplication(config());
  await producer.start({ only: [`commands:${base.name}`, "saga:notice"] });
  let position: any;
  try {
    const [event] = accepted(await producer.sendCommand(input(1)));
    position = event!.position;
    await expect
      .poll(
        async () =>
          (await store.consumers()).find((s) => s.group === "saga:notice")
            ?.attempts,
      )
      .toBeGreaterThanOrEqual(1);
    expect(await store.loadSaga("saga:notice", "one")).toBeNull();
    expect(await store.checkpoint("saga:notice")).toBeNull();
    expect(await store.pendingOutbox()).toEqual([]);
    fail = false;
    await store.retryConsumer("saga:notice");
    producer.wake();
    await expect.poll(() => store.checkpoint("saga:notice")).toEqual(position);
    expect(await store.loadSaga("saga:notice", "one")).toBeNull();
    expect(await store.pendingOutbox()).toHaveLength(1);
  } finally {
    await producer.stop();
  }
  const worker = createApplication(config());
  await worker.start({ only: [`outbox:${base.name}`, "worker:mail"] });
  try {
    await expect.poll(() => delivered.length).toBe(1);
  } finally {
    await worker.stop();
  }
  const restart = createApplication(config());
  await restart.start({ only: [`outbox:${base.name}`, "worker:mail"] });
  try {
    await expect.poll(() => delivered.length, { timeout: 5000 }).toBe(2);
    expect(delivered.map((d) => d.attempt)).toEqual([1, 2]);
    expect(delivered[0]!.key).toBe(delivered[1]!.key);
    expect(await store.deadLetters()).toEqual([]);
    expect(await store.checkpoint("saga:notice")).toEqual(position);
  } finally {
    await restart.stop();
  }
}, 15000);

test("Postgres consumer lag measures the hour since its actual Checkpoint Event", async () => {
  const { store, base } = await fixture();
  const event = (id: string, timestamp: string) => ({
    id,
    name: "recorded",
    payload: { value: 1 },
    metadata: {},
    schemaVersion: 1,
    timestamp,
    commandId: id,
    correlationId: id,
  });
  const [processed] = await store.appendToStream(input(1).stream, 0, [
    event("lag-one", "2026-01-01T00:00:00.000Z"),
  ]);
  await store.appendToStream(input(1).stream, 1, [
    event("lag-two", "2026-01-01T01:00:00.000Z"),
  ]);
  const lease = (await store.acquireLease("saga:lag", "owner"))!;
  await store.commitCheckpoint(lease, processed!.position);
  await store.setConsumerStatus(
    {
      group: lease.group,
      partition: 0,
      state: "running",
      checkpoint: processed!.position,
    },
    lease,
  );
  const app = createApplication({
    ...base,
    transport: createInMemoryTransport(),
  });
  expect(
    (await app.consumerStatus()).find((status) => status.group === lease.group)
      ?.lagMs,
  ).toBe(3600000);
  await app.close();
});
