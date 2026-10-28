import { afterAll, beforeAll, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { Pool, type PoolClient } from "pg";
import { createPostgresEventStore } from "../../src/event-store/postgres.js";
import type { OutboxEntry } from "../../src/types.js";

// One transaction admits a whole dispatch, one statement completes a batch.
const pool = new Pool({
  connectionString:
    process.env.POSTGRES_URL ??
    "postgres://eventfabric:eventfabric@127.0.0.1:55439/eventfabric",
});
let statements: string[] = [];
pool.on("connect", (client: PoolClient) => {
  const query = client.query.bind(client) as (...args: any[]) => any;
  (client as any).query = (...args: any[]) => {
    const text = typeof args[0] === "string" ? args[0] : args[0]?.text;
    if (!/pg_notify/.test(text ?? "")) statements.push(text);
    return query(...args);
  };
});
const schema = `outbox_admission_${Date.now()}`;
const store = createPostgresEventStore({ pool, schema });
beforeAll(() => store.migrate());
afterAll(async () => {
  await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  await pool.end();
});
const command = (stream: string): OutboxEntry => {
  const id = randomUUID();
  return {
    id,
    kind: "command",
    message: {
      id,
      stream: { context: "sales", aggregate: "order", id: stream },
      name: "create",
      payload: {},
      metadata: {},
      schemaVersion: 1,
      correlationId: id,
      timestamp: "2026-10-24T00:00:00.000Z",
    },
  };
};

it("admits a dispatch in four statements and refuses a row behind an earlier one of its Stream", async () => {
  const lease = (await store.acquireLease("outbox", "me"))!;
  const heads = Array.from({ length: 5 }, (_, n) => command(`s${n}`));
  const behind = command("s0");
  await store.recordOutbox([...heads, behind]);
  const pending = await store.pendingOutbox(100);
  expect(pending.map((row) => row.id)).toEqual(heads.map((row) => row.id));
  statements = [];
  const admitted = await store.admitOutbox([...pending, behind], lease);
  // BEGIN, the Lease fence, the Admission, COMMIT.
  expect(statements).toHaveLength(4);
  expect(admitted.sort()).toEqual(heads.map((row) => row.id).sort());
  statements = [];
  await store.completeOutbox(admitted);
  expect(statements).toHaveLength(1);
  expect((await store.pendingOutbox(100)).map((row) => row.id)).toEqual([behind.id]);
  expect(await store.claimOutbox(behind.id, lease)).toBe(true);
  await store.completeOutbox(behind.id);
  expect((await store.outboxStats()).pending).toBe(0);
});
