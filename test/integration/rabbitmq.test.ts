import { afterAll, expect, test } from "vitest";
import { createRabbitMQTransport } from "../../src/rabbitmq.js";
import type { Transport } from "../../src/types.js";
const transports: Transport[] = [];
afterAll(async () => {
  await Promise.all(transports.map((t) => t.close()));
});
test("RabbitMQ declares 128 single-active-consumer quorum Command queues per Context", async () => {
  const { connect } = await import("amqplib");
  const url = "amqp://127.0.0.1:55679";
  const application = `test-${crypto.randomUUID()}`;
  const first = await createRabbitMQTransport({ application, url });
  transports.push(first);
  const all = Array.from({ length: 128 }, (_, p) => p);
  await first.handle("sales", all, async () => ({
    status: "accepted",
    events: [],
  }));
  const connection = await connect(url);
  try {
    // A redeclare with different arguments fails, so this proves each queue has the flag.
    const declared = async (name: string, args: object) => {
      const ch = await connection.createChannel();
      ch.on("error", () => {});
      try {
        await ch.assertQueue(name, { durable: true, arguments: args });
        await ch.close();
        return true;
      } catch {
        return false;
      }
    };
    for (const p of all) {
      expect(
        await declared(`${application}.commands.sales.${p}`, {
          "x-queue-type": "quorum",
          "x-single-active-consumer": true,
        }),
      ).toBe(true);
      expect(await declared(`${application}.commands.sales.${p}`, {})).toBe(
        false,
      );
    }
    const ch = await connection.createChannel();
    ch.on("error", () => {});
    await expect(
      ch.checkQueue(`${application}.commands.sales.128`),
    ).rejects.toThrow();
  } finally {
    await connection.close();
  }
});

test("RabbitMQ connects on connect() or first use, not when created", async () => {
  // Nothing listens here, so creating must not try to connect.
  const unreachable = await createRabbitMQTransport({
    application: "test-lazy",
    url: "amqp://127.0.0.1:1",
  });
  expect(unreachable.connected()).toBe(false);
  await unreachable.close();
  await expect(unreachable.connect!()).rejects.toThrow("RabbitMQ disconnected");

  const transport = await createRabbitMQTransport({
    application: `test-${crypto.randomUUID()}`,
    url: "amqp://127.0.0.1:55679",
  });
  transports.push(transport);
  expect(transport.connected()).toBe(false);
  const received: unknown[] = [];
  await transport.subscribe("lazy", (message) => void received.push(message));
  expect(transport.connected()).toBe(true);
  await transport.publish("lazy", { ok: true });
  await expect.poll(() => received).toEqual([{ ok: true }]);
});

test("types exits and closes a config-created RabbitMQ connection on success and failure", async () => {
  const { mkdtemp, writeFile, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const { spawnSync } = await import("node:child_process");
  const folder = await mkdtemp(join(tmpdir(), "event-fabric-types-close-"));
  try {
    const configPath = join(folder, "config.mjs");
    await writeFile(
      configPath,
      `import {createRabbitMQTransport} from '${new URL("../../dist/rabbitmq.js", import.meta.url).href}';export const config={name:'types-close',typesPath:'generated.d.ts',transport:await createRabbitMQTransport({application:'types-close',url:'amqp://127.0.0.1:55679'})};`,
    );
    for (const extra of [[], ["--check"]]) {
      if (extra.length)
        await writeFile(join(folder, "generated.d.ts"), "stale");
      const result = spawnSync(
        process.execPath,
        [
          new URL("../../dist/cli/main.js", import.meta.url).pathname,
          "types",
          "--config",
          configPath,
          ...extra,
        ],
        { cwd: folder, encoding: "utf8", timeout: 2000 },
      );
      expect(result.error).toBeUndefined();
      expect(result.signal).toBeNull();
      expect(result.status).toBe(extra.length ? 1 : 0);
    }
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
test("enqueue after the connection is gone fails as a disconnect, not a closed channel", async () => {
  const transport = await createRabbitMQTransport({
    application: `test-${crypto.randomUUID()}`,
    url: "amqp://127.0.0.1:55679",
  });
  await transport.close();
  expect(transport.connected()).toBe(false);
  await expect(
    transport.enqueue("receipts", {
      id: "m",
      name: "send-receipt",
      payload: {},
      metadata: {},
      correlationId: "m",
      timestamp: new Date().toISOString(),
    } as any),
  ).rejects.toThrow("RabbitMQ disconnected");
});

test("a broadcast subscriber that stops reading keeps only the newest Notifications", async () => {
  const { Worker } = await import("node:worker_threads");
  const application = `test-${crypto.randomUUID()}`;
  const transport = await createRabbitMQTransport({
    application,
    url: "amqp://127.0.0.1:55679",
    broadcastMaxLength: 10,
  });
  transports.push(transport);
  const received: number[] = [];
  await transport.subscribe("flood", (message: any) => {
    received.push(message.i);
  });
  // Another thread publishes 2000 Notifications while this one is blocked, as a saturated Gateway's loop is.
  const done = new Int32Array(new SharedArrayBuffer(4));
  const worker = new Worker(
    `const { workerData } = require("node:worker_threads");
     (async () => {
       const connection = await require("amqplib").connect("amqp://127.0.0.1:55679");
       const channel = await connection.createConfirmChannel();
       for (let i = 0; i < 2000; i++)
         channel.publish(workerData.exchange, "flood", Buffer.from(JSON.stringify({ i, pad: "x".repeat(2000) })));
       await channel.waitForConfirms();
       await connection.close();
       Atomics.store(workerData.done, 0, 1);
       Atomics.notify(workerData.done, 0);
     })();`,
    { eval: true, workerData: { exchange: `${application}.broadcast`, done } },
  );
  Atomics.wait(done, 0, 0, 20000);
  await worker.terminate();
  await expect.poll(() => received.at(-1)).toBe(1999);
  await new Promise((resolve) => setTimeout(resolve, 200));
  // The prefetch window of 10 plus the capped queue; unacknowledged, about 3500 of 5000 such messages arrived.
  expect(received.length).toBeLessThanOrEqual(20);
});
