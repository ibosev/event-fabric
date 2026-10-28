import { afterAll, afterEach, expect, test } from "vitest";
import { Pool } from "pg";
import { createPostgresEventStore } from "../../src/event-store/postgres.js";
import { eventPageContract } from "../contracts/event-pages.js";
import { createApplication } from "../../src/runtime/index.js";
import { createInMemoryTransport } from "../../src/transport/memory.js";
const pool = new Pool({
  connectionString:
    process.env.EVENTFABRIC_POSTGRES_URL ??
    "postgres://eventfabric:eventfabric@127.0.0.1:55439/eventfabric",
});
const schemas: string[] = [];
afterEach(async () => {
  for (const schema of schemas.splice(0))
    await pool.query(`DROP SCHEMA "${schema}" CASCADE`);
});
afterAll(() => pool.end());
const open = async () => {
  const schema = `pages_${crypto.randomUUID().replaceAll("-", "")}`;
  schemas.push(schema);
  const store = createPostgresEventStore({ pool, schema });
  await store.migrate();
  return store;
};
eventPageContract("postgres", open);

test("filtered progress waits for a lower transaction's late matching commit", async () => {
  const store = await open();
  const schema = schemas.at(-1)!;
  const blocker = await pool.connect();
  try {
    await blocker.query("BEGIN");
    await blocker.query(`INSERT INTO "${schema}".events(id,context,aggregate,stream_id,revision,transaction_id,name,schema_version,command_id,correlation_id,timestamp,metadata)
      VALUES('late','pages','record','late',1,pg_current_xact_id(),'changed',1,'late','late',now(),'{}')`);
    await blocker.query(
      `INSERT INTO "${schema}".event_payloads(event_id,payload) VALUES('late','{}')`,
    );
    const [newer] = await store.appendToStream(
      { context: "pages", aggregate: "record", id: "newer" },
      0,
      [
        {
          id: "newer",
          name: "ignored",
          payload: {},
          metadata: {},
          schemaVersion: 1,
          timestamp: "2026-10-17T00:00:00.000Z",
          commandId: "newer",
          correlationId: "newer",
        },
      ],
    );
    expect(
      await store.readPage!({ eventNames: ["pages.record.changed"] }),
    ).toEqual({ events: [], through: null, complete: true });
    // Complete as far as the log is visible: the lower transaction's Event is not, yet.
    expect(await store.readPage!({ eventNames: [] })).toEqual({
      events: [],
      through: null,
      complete: true,
    });
    await blocker.query("COMMIT");
    const page = await store.readPage!({
      eventNames: ["pages.record.changed"],
    });
    expect(page.events.map((event) => event.id)).toEqual(["late"]);
    expect(page.through).toEqual(newer!.position);
  } finally {
    await blocker.query("ROLLBACK");
    blocker.release();
  }
});

test("startup refuses an Event store whose type index is missing or invalid", async () => {
  const store = await open();
  const schema = schemas.at(-1)!;
  await expect(store.assertMigrated!()).resolves.toBeUndefined();
  await pool.query(`DROP INDEX "${schema}".events_type_position`);
  const app = createApplication({
    name: "unmigrated",
    store,
    transport: createInMemoryTransport(),
  });
  try {
    await expect(app.start()).rejects.toThrow(/run `event-fabric migrate`/);
  } finally {
    await app.close();
  }
  await store.migrate();
  await expect(store.assertMigrated!()).resolves.toBeUndefined();
  // An interrupted concurrent build leaves the index invalid.
  await pool.query(
    `UPDATE pg_index SET indisvalid=false WHERE indexrelid='"${schema}".events_type_position'::regclass`,
  );
  await expect(store.assertMigrated!()).rejects.toThrow(/pending/);
  await store.migrate();
  await expect(store.assertMigrated!()).resolves.toBeUndefined();
});
