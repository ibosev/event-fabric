import { afterAll } from "vitest";
import { Pool } from "pg";
import { createPostgresEventStore } from "../../src/event-store/postgres.js";
import { workflowContract } from "../contracts/workflows.js";

const pool = new Pool({
  connectionString:
    process.env.POSTGRES_URL ??
    "postgres://eventfabric:eventfabric@127.0.0.1:55439/eventfabric",
});
const schemas: string[] = [];
// A schema per test, so instances and Leases never meet across tests.
workflowContract("postgres", async () => {
  const schema = `workflows_${Date.now()}_${schemas.length}`;
  schemas.push(schema);
  const store = createPostgresEventStore({ pool, schema });
  await store.migrate();
  return store;
});
afterAll(async () => {
  for (const schema of schemas) await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  await pool.end();
});
