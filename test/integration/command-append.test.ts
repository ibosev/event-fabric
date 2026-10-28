import { afterAll, beforeAll, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { Pool, type PoolClient } from "pg";
import { createPostgresEventStore } from "../../src/event-store/postgres.js";
import { compile } from "../../src/authoring/index.js";
import { createKernel } from "../../src/kernel/index.js";
import { createCommand } from "../../src/id.js";
import { comparePositions } from "../../src/position.js";
import { ConcurrencyError, DuplicateCommandError } from "../../src/errors.js";
import type { PendingEvent, StreamId } from "../../src/types.js";

// An append is one statement inside its transaction, fenced by the unique Revision index.
const pool = new Pool({
  connectionString:
    process.env.POSTGRES_URL ??
    "postgres://eventfabric:eventfabric@127.0.0.1:55439/eventfabric",
  max: 20,
});
// Every statement a pooled client sends, except the post-commit wake-up.
let statements: string[] = [];
pool.on("connect", (client: PoolClient) => {
  const query = client.query.bind(client) as (...args: any[]) => any;
  (client as any).query = (...args: any[]) => {
    const text = typeof args[0] === "string" ? args[0] : args[0]?.text;
    if (!/pg_notify/.test(text ?? "")) statements.push(text);
    return query(...args);
  };
});
const schema = `command_append_${Date.now()}`;
const store = createPostgresEventStore({ pool, schema });
beforeAll(() => store.migrate());
afterAll(async () => {
  await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  await pool.end();
});
const event = (name = "opened"): PendingEvent => ({
  id: randomUUID(),
  name,
  payload: { amount: 1 },
  metadata: {},
  schemaVersion: 1,
  timestamp: "2026-10-24T00:00:00.000Z",
  commandId: "cmd",
  correlationId: "cmd",
});
const fresh = (): StreamId => ({
  context: "shop",
  aggregate: "order",
  id: randomUUID(),
});
/** Every Event of `stream` in Position order: its Revisions must ascend from 1 without a gap. */
const assertOrdered = async (stream: StreamId, count: number) => {
  const history = await store.readStream(stream);
  expect(history.map((e) => e.revision)).toEqual(
    Array.from({ length: count }, (_, i) => i + 1),
  );
  const byPosition = [...history].sort((a, b) =>
    comparePositions(a.position, b.position),
  );
  expect(byPosition.map((e) => e.revision)).toEqual(
    history.map((e) => e.revision),
  );
  const global = (await store.readAll()).filter(
    (e) => e.stream.id === stream.id,
  );
  expect(global.map((e) => e.revision)).toEqual(history.map((e) => e.revision));
};

it("an append with a Command id is three statements, and a Revision conflict or Duplicate is refused in them", async () => {
  const stream = fresh();
  const command = createCommand({ stream, name: "open", payload: {} });
  statements = [];
  const saved = await store.appendToStream(
    stream,
    0,
    [event(), event("paid")],
    command,
    { fingerprint: "f" },
  );
  expect(saved.map((e) => e.revision)).toEqual([1, 2]);
  expect(statements).toHaveLength(3);
  expect(statements[0]).toMatch(/^BEGIN/);
  expect(statements[2]).toBe("COMMIT");
  await expect(
    store.appendToStream(stream, 1, [event()], createCommand({ stream, name: "x", payload: {} }), {
      fingerprint: "g",
    }),
  ).rejects.toBeInstanceOf(ConcurrencyError);
  // A gap is refused too: the Stream holds 2 Events, not 5.
  await expect(store.appendToStream(stream, 5, [event()])).rejects.toBeInstanceOf(
    ConcurrencyError,
  );
  await expect(
    store.appendToStream(stream, 2, [event()], command, { fingerprint: "f" }),
  ).rejects.toBeInstanceOf(DuplicateCommandError);
  expect(await store.acceptedCommand(command.id)).toMatchObject({
    fingerprint: "f",
    events: saved.map((e) => expect.objectContaining({ id: e.id })),
  });
  await assertOrdered(stream, 2);
});

it("Outbox entries add two statements, whatever their number", async () => {
  const stream = fresh();
  const outbox = Array.from({ length: 5 }, (_, ordinal) => ({
    id: randomUUID(),
    kind: "message" as const,
    message: {
      id: randomUUID(),
      name: "mail",
      schemaVersion: 1,
      timestamp: "2026-10-24T00:00:00.000Z",
      correlationId: "c",
      payload: { ordinal },
      metadata: {},
    },
  }));
  statements = [];
  await store.appendToStream(stream, 0, [event()], undefined, {
    outbox,
  });
  expect(statements).toHaveLength(5);
  const pending = (await store.pendingOutbox(1000)).filter((row) =>
    outbox.some((entry) => entry.id === row.id),
  );
  expect(pending.map((row) => (row.message as any).payload.ordinal)).toEqual([
    0, 1, 2, 3, 4,
  ]);
  for (const row of pending) await store.completeOutbox(row.id);
});

it("concurrent appends to one Stream: one wins each Revision, the others see a Revision conflict, Positions follow Revisions", async () => {
  const stream = fresh();
  const writers = 12,
    rounds = 8;
  const unexpected: unknown[] = [];
  await Promise.all(
    Array.from({ length: writers }, async () => {
      for (let done = 0; done < rounds; ) {
        const revision = (await store.readStream(stream)).length;
        try {
          await store.appendToStream(stream, revision, [event()]);
          done++;
        } catch (error) {
          if (!(error instanceof ConcurrencyError)) unexpected.push(error);
        }
      }
    }),
  );
  expect(unexpected).toEqual([]);
  await assertOrdered(stream, writers * rounds);
});

it("Commands racing on one Stream, some changing a Schedule, all retry or reject as a conflict and keep Positions in Revision order", async () => {
  const stream = fresh();
  const definition = compile(stream.context, stream.aggregate, {
    commands: {
      add: [(_command, order) => order.apply.added({})],
      // A Schedule change takes the locked path; the rest take the one-statement path.
      remind: [
        (_command, order, app) => {
          order.apply.added({});
          app.schedule("remind", "2099-01-01T00:00:00Z", {
            stream,
            name: "add",
          });
        },
      ],
    },
    events: { added() {} },
  });
  const kernel = createKernel({ store, definitions: [definition] });
  const outcomes = await Promise.all(
    Array.from({ length: 40 }, (_, i) =>
      kernel.handle(
        createCommand({ stream, name: i % 4 ? "add" : "remind", payload: {} }),
      ),
    ),
  );
  // Only a concurrency Rejection after the kernel's retries may refuse a Command; never a raw unique violation.
  for (const outcome of outcomes)
    if (outcome.status !== "accepted")
      expect(outcome).toMatchObject({
        status: "rejected",
        error: { kind: "concurrency" },
      });
  const accepted = outcomes.filter((o) => o.status === "accepted").length;
  expect(accepted).toBeGreaterThan(0);
  await assertOrdered(stream, accepted);
});
