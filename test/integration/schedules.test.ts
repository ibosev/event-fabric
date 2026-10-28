import { afterAll, beforeAll, expect, test } from "vitest";
import { Pool } from "pg";
import { createPostgresEventStore } from "../../src/event-store/postgres.js";
import { compile } from "../../src/authoring/index.js";
import { createKernel } from "../../src/kernel/index.js";
import { createCommand } from "../../src/id.js";
import { scheduleContract } from "../contracts/schedules.js";
import { createApplication } from "../../src/runtime/index.js";
import { createInMemoryTransport } from "../../src/transport/memory.js";
import type { Command, Logger } from "../../src/types.js";

const pool = new Pool({
  connectionString:
    process.env.POSTGRES_URL ??
    "postgres://eventfabric:eventfabric@127.0.0.1:55439/eventfabric",
});
const schema = `schedules_${Date.now()}`;
const schemas = [schema];
const store = createPostgresEventStore({ pool, schema });
beforeAll(() => store.migrate());
scheduleContract("postgres", (options) =>
  options ? createPostgresEventStore({ pool, schema, ...options }) : store,
);
afterAll(async () => {
  for (const schema of schemas)
    await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  await pool.end();
});

test("Postgres Schedules respect the visibility barrier and recover after downtime or missed notifications", async () => {
  const schema = `schedule_recovery_${Date.now()}`;
  schemas.push(schema);
  const store = createPostgresEventStore({ pool, schema });
  await store.migrate();
  const stream = { context: "recovery", aggregate: "invoice", id: "one" };
  const starts: number[] = [];
  const warnings: any[] = [];
  const logger: Logger = {
    debug() {},
    info() {},
    error() {},
    warn(fields) {
      warnings.push(fields);
    },
    child() {
      return this;
    },
  };
  const invoice = compile("recovery", "invoice", {
    commands: {
      schedule: [
        (command, _invoice, app) =>
          app.schedule("overdue", command.payload.dueAt, {
            stream,
            name: "check",
          }),
      ],
      check: [
        (_command, invoice) => {
          starts.push(Date.now());
          invoice.apply.checked({});
        },
      ],
    },
    events: { checked() {} },
  });
  const kernel = createKernel({ store, definitions: [invoice] });
  const dueAt = new Date(Date.now() - 500).toISOString();
  const lower = await pool.connect();
  try {
    await lower.query("BEGIN; SELECT pg_current_xact_id()");
    expect(
      await kernel.handle(
        createCommand({ stream, name: "schedule", payload: { dueAt } }),
      ),
    ).toMatchObject({ status: "accepted" });
    expect(await store.pendingOutbox()).toEqual([]);
    expect(await store.nextOutboxDelay()).toBeNull();
  } finally {
    await lower.query("ROLLBACK");
    lower.release();
  }
  const [row] = await store.pendingOutbox();
  expect((row!.message as Command).schedule?.dueAt).toBe(dueAt);
  // This replica receives no NOTIFY hints; polling must still recover durable work.
  const silent = { ...store, listen: async () => async () => {} };
  const app = createApplication({
    name: schema,
    store: silent,
    aggregates: [invoice],
    transport: createInMemoryTransport(),
    logger,
    pollInterval: 20,
  });
  await app.start();
  try {
    await expect.poll(() => starts.length).toBe(1);
    expect(warnings).toContainEqual(
      expect.objectContaining({ dueAt, lateness: expect.any(Number) }),
    );
    expect(await app.getSchedule(stream, "overdue")).toBeNull();
    expect(await kernel.handle(row!.message as Command)).toMatchObject({
      status: "accepted",
      duplicate: true,
    });
    const future = Date.now() + 150;
    await kernel.handle(
      createCommand({
        stream,
        name: "schedule",
        payload: { dueAt: new Date(future).toISOString() },
      }),
    );
    await expect.poll(() => starts.length).toBe(2);
    expect(starts[1]).toBeGreaterThanOrEqual(future);
  } finally {
    await app.close();
  }
});

test("Postgres commits an Invoice Event and named Schedule together", async () => {
  const stream = { context: "billing", aggregate: "invoice", id: "one" };
  const invoice = compile("billing", "invoice", {
    commands: {
      issue: [
        (_command, invoice, app) => {
          invoice.apply.issued({});
          app.schedule("overdue", "2099-01-01T00:00:00Z", {
            stream,
            name: "check",
          });
        },
      ],
      check: [(_command, invoice) => invoice.apply.checked({})],
    },
    events: { issued() {}, checked() {} },
  });
  const kernel = createKernel({ store, definitions: [invoice] });
  expect(
    await kernel.handle(createCommand({ stream, name: "issue" })),
  ).toMatchObject({ status: "accepted", events: [{ name: "issued" }] });
  const schedule = await store.getSchedule(stream, "overdue");
  expect(schedule).toMatchObject({
    stream,
    name: "overdue",
    dueAt: "2099-01-01T00:00:00.000Z",
  });
  expect(
    (await store.pendingOutbox()).some(
      (row) => row.message.id === schedule!.commandId,
    ),
  ).toBe(false);
});
