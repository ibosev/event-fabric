import { expect, test } from "vitest";
import { Pool } from "pg";
import { connect } from "amqplib";
import { setTimeout as delay } from "node:timers/promises";
import { compile } from "../../src/authoring/index.js";
import { createApplication } from "../../src/runtime/index.js";
import { createPostgresEventStore } from "../../src/event-store/postgres.js";
import { createRabbitMQTransport } from "../../src/rabbitmq.js";
import type { Logger } from "../../src/types.js";

test("a Schedule survives a lost RabbitMQ reply, broker disconnection and Application downtime", async () => {
  const name = `schedule_delivery_${Date.now()}`;
  const url = process.env.RABBITMQ_URL ?? "amqp://127.0.0.1:55679";
  const pool = new Pool({
    connectionString:
      process.env.POSTGRES_URL ??
      "postgres://eventfabric:eventfabric@127.0.0.1:55439/eventfabric",
  });
  const store = createPostgresEventStore({ pool, schema: name });
  const checks = new Map<string, number>();
  const warnings: Record<string, unknown>[] = [];
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
  const stream = (id: string) => ({
    context: "recovery",
    aggregate: "invoice",
    id,
  });
  const invoice = compile("recovery", "invoice", {
    commands: {
      schedule: [
        (command, invoice, app) =>
          app.schedule("overdue", command.payload.dueAt, {
            stream: stream(invoice.id),
            name: "check",
          }),
      ],
      check: [
        (_command, invoice) => {
          checks.set(invoice.id, (checks.get(invoice.id) ?? 0) + 1);
          invoice.apply.checked({});
        },
      ],
    },
    events: { checked() {} },
  });
  const apps: ReturnType<typeof createApplication>[] = [];
  const firstTransport = await createRabbitMQTransport({
    application: name,
    url,
  });
  let deliveries = 0;
  const first = createApplication({
    name,
    store,
    aggregates: [invoice],
    logger,
    transport: {
      ...firstTransport,
      async send(command, options) {
        const outcome = await firstTransport.send(command, options);
        if (
          command.name === "check" &&
          command.stream.id === "reply" &&
          ++deliveries === 1
        ) {
          // Drop the real broker's accepted reply at the Transport boundary.
          return { status: "unknown", commandId: command.id };
        }
        return outcome;
      },
    },
  });
  apps.push(first);
  try {
    await store.migrate();
    await first.start();
    expect(
      await first.sendCommand({
        stream: stream("reply"),
        name: "schedule",
        payload: { dueAt: new Date(Date.now() - 500).toISOString() },
      }),
    ).toMatchObject({ status: "accepted" });
    await expect.poll(() => deliveries, { timeout: 10000 }).toBe(2);
    await expect.poll(async () => (await store.outboxStats()).pending).toBe(0);
    expect(checks.get("reply")).toBe(1);
    expect(await store.readStream(stream("reply"))).toHaveLength(1);

    const due = Date.now() + 1000;
    const dueAt = new Date(due).toISOString();
    expect(
      await first.sendCommand({
        stream: stream("offline"),
        name: "schedule",
        payload: { dueAt },
      }),
    ).toMatchObject({ status: "accepted" });
    await firstTransport.close();
    await delay(Math.max(0, due - Date.now()) + 200);
    expect(checks.has("offline")).toBe(false);
    expect(await store.getSchedule(stream("offline"), "overdue")).toMatchObject(
      { dueAt },
    );
    await first.close();
    apps.splice(apps.indexOf(first), 1);
    const restarted = createApplication({
      name,
      store,
      aggregates: [invoice],
      logger,
      transport: await createRabbitMQTransport({ application: name, url }),
    });
    apps.push(restarted);
    await restarted.start();
    await expect.poll(() => checks.get("offline"), { timeout: 10000 }).toBe(1);
    await expect
      .poll(() => store.getSchedule(stream("offline"), "overdue"))
      .toBeNull();
    expect(await store.readStream(stream("offline"))).toHaveLength(1);
    expect(warnings).toContainEqual(
      expect.objectContaining({ dueAt, lateness: expect.any(Number) }),
    );
    expect(await store.deadLetters()).toEqual([]);
  } finally {
    await Promise.all(apps.map((app) => app.close()));
    const connection = await connect(url);
    try {
      const channel = await connection.createChannel();
      for (let partition = 0; partition < 128; partition++)
        await channel.deleteQueue(`${name}.commands.recovery.${partition}`);
      await channel.deleteExchange(`${name}.broadcast`);
    } finally {
      await connection.close();
    }
    await pool.query(`DROP SCHEMA IF EXISTS "${name}" CASCADE`);
    await pool.end();
  }
}, 60000);
