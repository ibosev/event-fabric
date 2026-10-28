import { afterAll, beforeAll, expect, test, vi } from "vitest";
import { Pool } from "pg";
import pino from "pino";
import { createPostgresEventStore } from "../../src/event-store/postgres.js";
import { sagaMessageContract } from "../contracts/saga-messages.js";
import { workerReplyContract } from "../contracts/worker-replies.js";
import { compile } from "../../src/authoring/index.js";
import { createApplication } from "../../src/runtime/index.js";
import { createInMemoryTransport } from "../../src/transport/memory.js";
import type { SagaDefinition } from "../../src/runtime/types.js";

const pool = new Pool({
  connectionString:
    process.env.POSTGRES_URL ??
    "postgres://eventfabric:eventfabric@127.0.0.1:55439/eventfabric",
});
const schema = `saga_messages_${Date.now()}`;
const store = createPostgresEventStore({ pool, schema });
beforeAll(() => store.migrate());
sagaMessageContract("postgres", () => store);
workerReplyContract("postgres", async () => {
  const replies = createPostgresEventStore({ pool, schema: `${schema}_replies` });
  await replies.migrate();
  return replies;
});
afterAll(async () => {
  await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  await pool.query(`DROP SCHEMA IF EXISTS "${schema}_replies" CASCADE`);
  await pool.end();
});

const file = compile("crm", "file", {
  commands: { open: [(command, file) => file.apply.opened(command.payload)] },
  events: { opened() {} },
});
const percentile = (values: number[], p: number) =>
  [...values].sort((a, b) => a - b)[Math.min(values.length - 1, Math.floor(values.length * p))]!;

test("Postgres: Deadlines and Saga Messages from another process start within 100 ms, without polling", async () => {
  const runtime = `${schema}_runtime`;
  const owner = createPostgresEventStore({ pool, schema: runtime });
  await owner.migrate();
  const started: { at: number; message: any }[] = [];
  const saga: SagaDefinition = {
    reactions: {
      "crm.file.opened": (event, s: any) => s.deadline("remind", event.payload.remindAt),
    },
    deadlines: { remind: (message) => void started.push({ at: Date.now(), message }) },
    messages: { done: { start: true, handle: (message) => void started.push({ at: Date.now(), message }) } },
  };
  const options = {
    aggregates: [file],
    transport: createInMemoryTransport(),
    sagas: { connector: saga },
    // Ten times the target: only the due-time timer and the store's notification can meet it.
    pollInterval: 1000,
    logger: pino({ level: "silent" }),
  };
  const app = createApplication({ name: "saga-messages-pg", store: owner, ...options });
  // Never started: it only sends, as a Gateway or Worker replica would.
  const sender = createApplication({
    name: "saga-messages-pg",
    store: createPostgresEventStore({ pool, schema: runtime }),
    ...options,
  });
  await app.start();
  try {
    const deadlines: number[] = [];
    for (let i = 0; i < 10; i++) {
      const due = Date.now() + 200;
      await app.sendCommand({
        stream: { context: "crm", aggregate: "file", id: `d${i}` },
        name: "open",
        payload: { remindAt: new Date(due).toISOString() },
      });
      await vi.waitFor(() => expect(started).toHaveLength(i + 1), { timeout: 3000 });
      deadlines.push(started.at(-1)!.at - due);
    }
    const sends: number[] = [];
    for (let i = 0; i < 10; i++) {
      const sentAt = Date.now();
      await sender.sendSagaMessage("connector", "done", {}, { to: `m${i}` });
      await vi.waitFor(() => expect(started).toHaveLength(11 + i), { timeout: 3000, interval: 5 });
      sends.push(started.at(-1)!.at - sentAt);
    }
    console.log(
      `Saga Message start latency (ms): Deadline due→start p50 ${percentile(deadlines, 0.5)} max ${Math.max(...deadlines)}; ` +
        `other process send→start p50 ${percentile(sends, 0.5)} max ${Math.max(...sends)}`,
    );
    expect(Math.min(...deadlines)).toBeGreaterThanOrEqual(-5);
    expect(percentile(deadlines, 0.5)).toBeLessThanOrEqual(100);
    expect(percentile(sends, 0.5)).toBeLessThanOrEqual(100);
  } finally {
    await app.close();
    await sender.close();
    await pool.query(`DROP SCHEMA IF EXISTS "${runtime}" CASCADE`);
  }
}, 30000);
