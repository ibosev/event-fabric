import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createServer } from "node:http";
import { monitorEventLoopDelay } from "node:perf_hooks";
import { Worker } from "node:worker_threads";
import { afterEach, expect, test } from "vitest";
import { compile } from "../../src/authoring/index.js";
import { createApplication } from "../../src/runtime/index.js";
import { createInMemoryEventStore } from "../../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../../src/adapters/memory-readmodel.js";
import { createRabbitMQTransport } from "../../src/rabbitmq.js";
import { legacyGateway, legacyRest } from "../../src/compat.js";

/*
 * Live fan-out under load: old-protocol sockets that are idle, auto-refresh a list or hold a
 * pending Command wait, while another thread publishes a burst of Readmodel changes as an import's groups
 * would. The Gateway's event loop must stay responsive and an interactive Command route must keep answering.
 * LOAD_SOCKETS (per kind, default 100) and LOAD_BATCHES (default 500, 20 changes each) scale it, and
 * LOAD_COALESCE sets the sockets' `coalesce` (default 500 ms; 0 turns it off).
 */
const url = "amqp://127.0.0.1:55679";
const perKind = Number(process.env.LOAD_SOCKETS ?? 100);
const batches = Number(process.env.LOAD_BATCHES ?? 500);
const cleanups: (() => unknown)[] = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});
const freePort = () =>
  new Promise<number>((resolve) => {
    const server = createServer().listen(0, () => {
      const { port } = server.address() as { port: number };
      server.close(() => resolve(port));
    });
  });
const quantile = (values: number[], q: number) =>
  [...values].sort((a, b) => a - b)[
    Math.min(values.length - 1, Math.floor(values.length * q))
  ]!;

test("old-protocol sockets during a burst of Readmodel changes keep the Gateway responsive", async () => {
  const { default: WebSocket } = await import("ws");
  (globalThis as any).legacyToolkit = legacyRest;
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "live-load-"));
  cleanups.push(() => fs.rmSync(directory, { recursive: true, force: true }));
  fs.writeFileSync(
    path.join(directory, "package.json"),
    '{ "type": "commonjs" }',
  );
  fs.writeFileSync(
    path.join(directory, "setup.js"),
    `module.exports = {
  access: { readmodels: { 'detail.*': metadata => ({ tenant: metadata.tenant }) }, domain: { '*': true } },
  ws: { connection: [async req => { req.meta = { tenant: 'a' }; }] },
};`,
  );
  fs.mkdirSync(path.join(directory, "routes"));
  fs.writeFileSync(
    path.join(directory, "routes", "tasks.route.js"),
    `const { command } = globalThis.legacyToolkit;
module.exports = { routes: { '/:id/lock': command(() => ({ command: 'sales.task.lock', event: 'domain.sales.task.locked' })) } };`,
  );
  const task = compile("sales", "task", {
    commands: {
      lock: (_command: any, state: any) => state.apply.locked({}),
      touch: () => {},
    },
    events: { locked: () => {} },
  });
  const application = `load-${crypto.randomUUID()}`;
  const port = await freePort();
  const app = createApplication({
    name: application,
    store: createInMemoryEventStore(),
    transport: await createRabbitMQTransport({ url, application }),
    aggregates: [task],
    readmodels: {
      detail: {
        store: createMemoryReadmodelStore(),
        models: {
          order: {
            schema: {
              type: "object",
              properties: { tenant: { type: "string" } },
            },
            reactions: {},
          },
        },
      },
    },
    gateways: {
      legacy: legacyGateway({
        directory,
        port,
        metadata: (req: any) => req.meta ?? { tenant: "a" },
        socket: {
          coalesce: Number(process.env.LOAD_COALESCE ?? 500),
          commandWaitTimeout: 60000,
        },
        live: { notifications: () => true },
      }),
    },
  });
  await app.start();
  cleanups.push(() => app.close());

  type Socket = {
    inbox: any[];
    closed: boolean;
    send: (message: object) => void;
  };
  const sockets: Socket[] = [];
  const open = async () => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}/socket`);
    cleanups.push(() => ws.terminate());
    const socket: Socket = {
      inbox: [],
      closed: false,
      send: (message) => ws.send(JSON.stringify(message)),
    };
    ws.on("message", (raw) => socket.inbox.push(JSON.parse(String(raw))));
    ws.on("close", () => (socket.closed = true));
    await new Promise((resolve) => ws.on("open", resolve));
    sockets.push(socket);
    return socket;
  };
  const subscribed = (socket: { inbox: any[] }, correlationId: string) =>
    expect
      .poll(() =>
        socket.inbox.some(
          (m) =>
            m.type === "subscribedEvents" && m.correlationId === correlationId,
        ),
      )
      .toBe(true);
  const refreshing: Socket[] = [];
  for (let i = 0; i < perKind; i++) {
    const idle = await open();
    idle.send({
      type: "subscribeEvents",
      correlationId: "lock",
      payload: { filter: { type: "system", name: "lock" } },
    });
    const list = await open();
    list.send({
      type: "subscribeEvents",
      correlationId: "list",
      payload: {
        filter: {
          type: "readmodel",
          readmodel: { type: "detail", collection: "order" },
        },
      },
    });
    refreshing.push(list);
    const waiting = await open();
    waiting.send({
      type: "subscribeEvents",
      correlationId: "wait",
      payload: { filter: { metadata: {} } },
    });
    waiting.send({
      type: "sendCommand",
      correlationId: "send",
      payload: {
        context: "sales",
        aggregate: { name: "task", id: `t${i}` },
        name: "touch",
        payload: {},
      },
    });
    await Promise.all([
      subscribed(idle, "lock"),
      subscribed(list, "list"),
      subscribed(waiting, "wait"),
    ]);
  }

  const lock = async (id: string) => {
    const start = performance.now();
    const response = await fetch(`http://127.0.0.1:${port}/tasks/${id}/lock`, {
      method: "POST",
    });
    await response.text();
    expect(response.status).toBe(200);
    return performance.now() - start;
  };
  await lock("warm");

  const delay = monitorEventLoopDelay({ resolution: 5 });
  delay.enable();
  const latencies: number[] = [];
  let publishing = true;
  const probes = (async () => {
    for (let i = 0; publishing; i++) {
      latencies.push(await lock(`p${i}`));
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  })();
  const started = performance.now();
  let drained: number;
  try {
    // The import's groups publish from other processes; a worker thread stands in for them.
    await new Promise<void>((resolve, reject) => {
      const worker = new Worker(
        `const { workerData } = require("node:worker_threads");
       (async () => {
         const connection = await require("amqplib").connect(workerData.url);
         const channel = await connection.createConfirmChannel();
         const pad = "x".repeat(1000);
         for (let b = 0; b < workerData.batches; b++) {
           const last = b === workerData.batches - 1;
           const changes = Array.from({ length: 20 }, (_, i) => {
             const id = last && i === 19 ? "last" : "d" + ((b * 20 + i) % 5000);
             return { model: "order", id, before: { _id: id, tenant: "a", pad }, after: { _id: id, tenant: "a", pad, b }, correlationIds: ["import-" + b] };
           });
           channel.publish(workerData.exchange, "readmodels", Buffer.from(JSON.stringify({
             kind: "readmodel", group: "detail", position: { transactionId: String(1000 + b), sequence: "0" }, changes,
           })));
           if (b % 50 === 49) await channel.waitForConfirms();
         }
         await channel.waitForConfirms();
         await connection.close();
       })().then(() => process.exit(0), (error) => { console.error(error); process.exit(1); });`,
        {
          eval: true,
          workerData: { url, exchange: `${application}.broadcast`, batches },
        },
      );
      worker.on("exit", (code) =>
        code ? reject(new Error(`publisher exited ${code}`)) : resolve(),
      );
    });
    // Every auto-refreshing socket ends on the last change (coalescing keeps the last one), unless it was
    // disconnected for falling `maxBuffer` behind.
    await expect
      .poll(
        () =>
          refreshing.every(
            (socket) =>
              socket.closed ||
              socket.inbox.some((m) => m.payload?.readmodel?.id === "last"),
          ),
        { timeout: 150000, interval: 50 },
      )
      .toBe(true);
    drained = performance.now() - started;
  } finally {
    publishing = false;
    await probes;
    delay.disable();
  }

  const frames = refreshing.reduce(
    (sum, socket) =>
      sum + socket.inbox.filter((m) => m.type === "event").length,
    0,
  );
  const result = {
    sockets: sockets.length,
    changes: batches * 20,
    drainedMs: Math.round(drained),
    loopDelayP99Ms: Math.round(delay.percentile(99) / 1e6),
    loopDelayMaxMs: Math.round(delay.max / 1e6),
    lockP50Ms: Math.round(quantile(latencies, 0.5)),
    lockMaxMs: Math.round(Math.max(...latencies)),
    lockRequests: latencies.length,
    refreshFramesPerSocket: Math.round(frames / refreshing.length),
    disconnected: sockets.filter((socket) => socket.closed).length,
  };
  console.log("live fan-out load", result);
  expect(result.loopDelayP99Ms).toBeLessThan(250);
  expect(result.lockMaxMs).toBeLessThan(1000);
  expect(result.disconnected).toBe(0);
  // No socket's wait saw an import change: their correlation filters them out.
  for (const socket of sockets)
    expect(
      socket.inbox.filter(
        (m) => m.correlationId === "wait" && m.payload?.type === "readmodel",
      ),
    ).toEqual([]);
}, 240000);
