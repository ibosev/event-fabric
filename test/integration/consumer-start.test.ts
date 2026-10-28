import { afterAll, expect, test } from "vitest";
import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import pino from "pino";
import { createPostgresEventStore } from "../../src/event-store/postgres.js";
import { compile } from "../../src/authoring/index.js";
import { createApplication } from "../../src/runtime/index.js";
import { createInMemoryTransport } from "../../src/transport/memory.js";
import { consumerStartContract } from "../contracts/consumer-start.js";

const pool = new Pool({
  connectionString:
    process.env.POSTGRES_URL ??
    "postgres://eventfabric:eventfabric@127.0.0.1:55439/eventfabric",
});
const schemas: string[] = [];
const open = async () => {
  const schema = `consumer_start_${Date.now()}_${schemas.length}`;
  schemas.push(schema);
  const store = createPostgresEventStore({ pool, schema });
  await store.migrate();
  return { schema, store };
};
// A schema per test, so `beginning` sees only the Events that test recorded.
consumerStartContract("postgres", async () => (await open()).store);
afterAll(async () => {
  for (const schema of schemas) await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  await pool.end();
});

test("postgres: a Saga group with no started marker, halted on its first Event, handles that Event after the upgrade", async () => {
  const { schema, store } = await open();
  // The store as it stood before migration 10: no started marker for any group.
  await pool.query(`DELETE FROM "${schema}".migrations WHERE version=10`);
  const id = randomUUID();
  await store.appendToStream({ context: "c", aggregate: "a", id }, 0, [
    { id, name: "added", payload: {}, metadata: {}, schemaVersion: 1, timestamp: new Date().toISOString(), commandId: id, correlationId: id },
  ]);
  // The old replica took the Lease and halted on that Event: rows, but no Checkpoint and no layout row.
  const lease = (await store.acquireLease("saga:s", "old", 30000))!;
  await store.setConsumerStatus(
    { group: "saga:s", partition: 0, checkpoint: null, state: "halted", attempts: 5, eventId: id, error: "poison" },
    lease,
  );
  await store.releaseLease(lease);
  await store.migrate();
  const app = createApplication({
    name: "upgrade",
    store,
    transport: createInMemoryTransport(),
    aggregates: [compile("c", "a", { commands: {}, events: { added() {} } })],
    sagas: { s: { reactions: { "c.a.added": (_event: any, saga: any) => saga.set("seen", true) } } },
    pollInterval: 5,
    logger: pino({ level: "silent" }),
  });
  await app.start();
  try {
    await expect.poll(() => store.loadSaga("saga:s", id), { timeout: 5000 }).toMatchObject({ seen: true });
  } finally {
    await app.stop();
  }
});
