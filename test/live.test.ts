import { createServer } from "node:http";
import { afterEach, expect, test } from "vitest";
import { compile } from "../src/authoring/index.js";
import { createApplication } from "../src/runtime/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import { sharedSubscriptions } from "../src/transport/shared.js";
import { createScopedHandle, live } from "../src/gateway/index.js";
import type { GatewayDefinition } from "../src/runtime/types.js";

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
async function fixture(gateway: Partial<GatewayDefinition> = {}) {
  const port = await freePort(),
    transport = createInMemoryTransport();
  const order = compile("sales", "order", {
    commands: {
      create: (command: any, state: any) =>
        state.apply.created({ title: command.payload.title }),
    },
    events: { created: () => {} },
  });
  const application = createApplication({
    name: "live",
    store: createInMemoryEventStore(),
    transport,
    aggregates: [order],
    pollInterval: 5,
    readmodels: {
      detail: {
        store: createMemoryReadmodelStore(),
        models: {
          order: {
            schema: {
              type: "object",
              properties: {
                tenant: { type: "string" },
                title: { type: "string" },
              },
            },
            reactions: {
              "sales.order.created": (event: any, view: any) =>
                view.set({
                  tenant: event.metadata.tenant,
                  title: event.payload.title,
                }),
            },
          },
        },
      },
    },
    gateways: {
      http: {
        port,
        context: (req) => ({ tenant: req.headers["x-tenant"] as string }),
        commandMetadata: (ctx) => ({ tenant: ctx.tenant }),
        access: {
          readmodels: { "detail.*": (ctx) => ({ tenant: ctx.tenant }) },
          domain: { "*": true },
        },
        routes: [
          { method: "get", path: "/live", handler: live({ heartbeat: 20 }) },
        ],
        ...gateway,
      },
    },
  });
  await application.start();
  cleanups.push(() => application.close());
  const create = (tenant: string, id: string) =>
    application.sendCommand({
      stream: { context: "sales", aggregate: "order", id },
      name: "create",
      payload: { title: id },
      metadata: { tenant },
    });
  return { application, transport, url: `http://127.0.0.1:${port}`, create };
}
// Reads SSE frames until `done` holds for the frames so far.
async function frames(response: Response, done: (frames: any[]) => boolean) {
  const reader = response.body!.getReader(),
    seen: any[] = [];
  let text = "";
  while (!done(seen)) {
    const { value, done: ended } = await reader.read();
    if (ended) break;
    text += new TextDecoder().decode(value);
    const parts = text.split("\n\n");
    text = parts.pop()!;
    for (const part of parts) {
      const event = /^event: (.*)$/m.exec(part)?.[1];
      const data = /^data: (.*)$/m.exec(part)?.[1];
      if (event) seen.push({ event, data: JSON.parse(data!) });
    }
  }
  await reader.cancel();
  return seen;
}
const tenantRules: GatewayDefinition["live"] = {
  events: (ctx, event) => event.metadata.tenant === ctx.tenant,
  notifications: (ctx, notification: any) =>
    notification.payload?.tenant === ctx.tenant,
};

test("one Transport subscription per topic serves every local subscriber until the last one stops", async () => {
  const handlers = new Set<(message: any) => unknown>();
  let subscribed = 0,
    stopped = 0;
  const shared = sharedSubscriptions({
    async subscribe(_topic, handler) {
      subscribed++;
      handlers.add(handler);
      return async () => {
        stopped++;
        handlers.delete(handler);
      };
    },
  });
  const received: string[] = [];
  const first = await shared.subscribe(
    "t",
    (m) => void received.push(`a:${m}`),
  );
  const failing = await shared.subscribe("t", () => {
    throw new Error("subscriber failure");
  });
  const second = await shared.subscribe(
    "t",
    (m) => void received.push(`b:${m}`),
  );
  expect(subscribed).toBe(1);
  for (const handler of handlers) await handler("x");
  // A failing subscriber does not keep the message from the others.
  expect(received.sort()).toEqual(["a:x", "b:x"]);
  await first();
  await failing();
  expect(stopped).toBe(0);
  await second();
  await second();
  expect(stopped).toBe(1);
  await shared.subscribe("t", () => {});
  expect(subscribed).toBe(2);
});

test("the live() SSE feed sends a tenant only its own Events, Notifications and Readmodel changes", async () => {
  const { application, url, create } = await fixture({ live: tenantRules });
  const response = await fetch(
    `${url}/live?events=sales.order.*&notifications=lock&readmodels=detail.order`,
    { headers: { "x-tenant": "a" } },
  );
  expect(response.headers.get("content-type")).toBe("text/event-stream");
  const received = frames(response, (seen) => seen.length >= 3);
  await create("b", "theirs");
  await application.notify("lock", { tenant: "b" });
  await application.notify("other", { tenant: "a" });
  await create("a", "mine");
  await application.notify("lock", { tenant: "a" });
  const seen = await received;
  expect(seen.map(({ event }) => event).sort()).toEqual([
    "event",
    "notification",
    "readmodel",
  ]);
  expect(seen.find(({ event }) => event === "event").data).toMatchObject({
    name: "created",
    stream: { id: "mine" },
  });
  expect(seen.find(({ event }) => event === "notification").data).toEqual({
    name: "lock",
    payload: { tenant: "a" },
  });
  expect(seen.find(({ event }) => event === "readmodel").data).toMatchObject({
    group: "detail",
    model: "order",
    ids: ["mine"],
  });
});

test("live() refuses a kind without a Gateway rule and a malformed Readmodel name", async () => {
  const { url } = await fixture();
  const headers = { "x-tenant": "a" };
  expect((await fetch(`${url}/live?events=*`, { headers })).status).toBe(403);
  expect((await fetch(`${url}/live?notifications=*`, { headers })).status).toBe(
    403,
  );
  expect(
    (await fetch(`${url}/live?readmodels=detail`, { headers })).status,
  ).toBe(400);
});

test("subscribeChanges scopes every model's changes and names the Commands' correlations", async () => {
  const { application, transport, create } = await fixture({
    live: tenantRules,
  });
  const handle = createScopedHandle(
    { tenant: "a" },
    application.config.gateways!.http!,
    application,
  );
  const changes: any[] = [];
  const stop = await handle.subscribeChanges(
    (change) => void changes.push(change),
  );
  cleanups.push(stop);
  await create("b", "theirs");
  const outcome = await create("a", "mine");
  await expect.poll(() => changes.length).toBe(1);
  expect(changes[0]).toMatchObject({
    group: "detail",
    model: "order",
    id: "mine",
    kind: "create",
    document: { tenant: "a", title: "mine" },
    correlationIds: [(outcome as any).events[0].correlationId],
  });
  // A document moved to another tenant is deleted for the one it left, with the state they could read.
  await transport.publish("readmodels", {
    kind: "readmodel",
    group: "detail",
    position: { transactionId: "9", sequence: "1" },
    changes: [
      {
        model: "order",
        id: "mine",
        before: { tenant: "a", title: "mine" },
        after: { tenant: "b", title: "moved" },
        correlationIds: [],
      },
    ],
  });
  await expect.poll(() => changes.length).toBe(2);
  expect(changes[1]).toMatchObject({
    kind: "delete",
    document: { tenant: "a", title: "mine" },
  });
});

test("subscribeChanges drops unselected changes before any Access rule runs, and subscribers share each change", async () => {
  let rules = 0;
  const { application, transport } = await fixture({
    access: {
      readmodels: {
        "detail.*": (ctx: any) => (rules++, { tenant: ctx.tenant }),
      },
      domain: { "*": true },
    },
  });
  const handle = createScopedHandle(
    { tenant: "a" },
    application.config.gateways!.http!,
    application,
  );
  const seen: Record<string, any[]> = { all: [], other: [], mine: [] };
  for (const [name, select] of [
    ["all", {}],
    ["other", { model: "other" }],
    ["mine", { correlated: (ids: string[]) => ids.includes("mine") }],
  ] as const)
    cleanups.push(
      await handle.subscribeChanges(
        (change) => void seen[name]!.push(change),
        select,
      ),
    );
  const change = (id: string, correlationIds: string[]) =>
    transport.publish("readmodels", {
      kind: "readmodel",
      group: "detail",
      position: { transactionId: "9", sequence: id },
      changes: [
        {
          model: "order",
          id,
          after: { tenant: "a", title: id },
          correlationIds,
        },
      ],
    });
  await change("1", ["theirs"]);
  await change("2", ["mine"]);
  await expect.poll(() => seen.all!.length).toBe(2);
  await expect.poll(() => seen.mine!.length).toBe(1);
  expect(seen.other).toEqual([]);
  expect(seen.mine![0]).toBe(seen.all![1]);
  // The rule ran for the two subscriptions that took an `order` change, not for the one selecting another model.
  expect(rules).toBe(2);
});
