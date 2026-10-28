import { afterEach, expect, test, vi } from "vitest";
import { createScopedHandle } from "../src/gateway/access.js";
import { createApplication } from "../src/runtime/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import type { BatchNotification } from "../src/runtime/types.js";
const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});
const position = { transactionId: "1", sequence: "1" };
async function fixture() {
  const transport = createInMemoryTransport(),
    readmodelStore = createMemoryReadmodelStore(),
    store = createInMemoryEventStore();
  const application = createApplication({
    name: "gateway-tests",
    store,
    transport,
    readmodels: {
      detail: {
        store: readmodelStore,
        models: {
          order: {
            schema: {
              type: "object",
              properties: {
                tenant: { type: "string" },
                title: { type: "string" },
              },
            },
            reactions: {},
          },
        },
      },
    },
  });
  await application.initialize();
  cleanups.push(() => application.close());
  await readmodelStore.fence("detail", 1);
  await readmodelStore.batch("detail", 1, position, [
    { model: "order", id: "one", document: { tenant: "a", title: "mine" } },
    { model: "order", id: "other", document: { tenant: "b", title: "secret" } },
  ]);
  const scoped = createScopedHandle(
    { tenant: "a" },
    {
      port: 0,
      access: { readmodels: { "*": (ctx) => ({ tenant: ctx.tenant }) } },
    },
    application,
  );
  return { application, transport, readmodelStore, store, scoped };
}
test("a Gateway read answers its status from one status read, and a query without minPosition reads none", async () => {
  const { application, scoped, readmodelStore, store } = await fixture();
  const checkpoint = vi.spyOn(readmodelStore, "checkpoint"),
    consumers = vi.spyOn(store, "consumers");
  const reads = () => [checkpoint.mock.calls.length, consumers.mock.calls.length];
  // Past the 10 ms in which concurrent readers share one status read.
  const fresh = () => new Promise((resolve) => setTimeout(resolve, 15));
  await fresh();
  const one = await scoped.readmodels.detail.order("one");
  expect(one).toMatchObject({ document: { title: "mine" }, behind: false, position });
  expect(reads()).toEqual([1, 1]);
  checkpoint.mockClear();
  consumers.mockClear();
  await fresh();
  await scoped.readmodels.detail.order.find();
  await fresh();
  await scoped.readmodels.detail.order.findOne({ title: "mine" });
  await fresh();
  await scoped.readmodels.detail.order.count();
  await fresh();
  for await (const _ of scoped.readmodels.detail.order.stream());
  await fresh();
  await application.readmodels.detail.order.find();
  // Nothing reports a status here, so nothing reads one.
  expect(reads()).toEqual([0, 0]);
  await fresh();
  // A minPosition still waits on the group's Checkpoint.
  await application.readmodels.detail.order.find({}, { minPosition: position });
  expect(checkpoint.mock.calls.length).toBeGreaterThan(0);
});
test("Gateway Access map scopes every read and strips internal documents from transfer/delete Notifications", async () => {
  const { scoped, transport } = await fixture(),
    received: any[] = [];
  expect(
    (await scoped.readmodels.detail.order.find()).map((doc: any) => doc._id),
  ).toEqual(["one"]);
  expect((await scoped.readmodels.detail.order("other")).document).toBeNull();
  expect(await scoped.readmodels.detail.order.count()).toBe(1);
  const docs = [];
  for await (const doc of scoped.readmodels.detail.order.stream())
    docs.push(doc._id);
  expect(docs).toEqual(["one"]);
  const subscription = await scoped.readmodels.subscribe(
    "detail",
    "order",
    (notification: any) => received.push(notification),
  );
  cleanups.push(subscription.stop);
  expect(subscription.initial.map((doc: any) => doc._id)).toEqual(["one"]);
  const notification: BatchNotification = {
    kind: "readmodel",
    group: "detail",
    position: { transactionId: "2", sequence: "1" },
    changes: [
      {
        model: "order",
        id: "one",
        before: { tenant: "a", title: "mine" },
        after: { tenant: "b", title: "secret-after-transfer" },
      },
      {
        model: "order",
        id: "other",
        before: { tenant: "b", title: "secret" },
        after: null,
      },
    ],
  };
  await transport.publish("readmodels", notification);
  await expect.poll(() => received.length).toBe(1);
  expect(received[0]).toEqual({
    kind: "readmodel",
    group: "detail",
    model: "order",
    ids: ["one"],
    position: notification.position,
    correlationIds: [],
  });
});

test("a bounded subscription scopes its initial page and still notifies about documents outside it", async () => {
  const { scoped, transport, readmodelStore } = await fixture();
  await readmodelStore.batch("detail", 1, { transactionId: "2", sequence: "1" }, [
    { model: "order", id: "aaa-hidden", document: { tenant: "b", title: "secret" } },
    { model: "order", id: "two", document: { tenant: "a", title: "outside the initial page" } },
  ]);
  const received: any[] = [];
  const subscription = await scoped.readmodels.subscribe(
    "detail", "order", (notification: any) => received.push(notification),
    { initialSize: 1 },
  );
  cleanups.push(subscription.stop);
  expect(subscription.initial.map((doc: any) => doc._id)).toEqual(["one"]);
  await transport.publish("readmodels", {
    kind: "readmodel", group: "detail", position: { transactionId: "3", sequence: "1" },
    changes: [
      { model: "order", id: "two", before: null, after: { tenant: "a", title: "updated" } },
      { model: "order", id: "aaa-hidden", before: null, after: { tenant: "b", title: "secret" } },
    ],
  } satisfies BatchNotification);
  await expect.poll(() => received.length).toBe(1);
  expect(received[0].ids).toEqual(["two"]);
  const empty = await scoped.readmodels.subscribe("detail", "order", () => {}, { initialSize: 0 });
  cleanups.push(empty.stop);
  expect(empty.initial).toEqual([]);
});

test.each([-1, 0.5, Infinity])("a subscription refuses an invalid initial size: %s", async (initialSize) => {
  const { scoped } = await fixture();
  await expect(scoped.readmodels.subscribe("detail", "order", () => {}, { initialSize }))
    .rejects.toThrow("initialSize must be a non-negative safe integer");
});

import {
  startGateway,
  command,
  handle,
  readmodel,
  sse,
} from "../src/gateway/index.js";
import { compile, only } from "../src/authoring/index.js";
import { Rejection } from "../src/errors.js";
import { encodePosition } from "../src/position.js";
import type { GatewayDefinition } from "../src/runtime/types.js";
import type { Logger } from "../src/types.js";
async function httpFixture(
  definition: GatewayDefinition,
  options: { consumers?: boolean } = {},
) {
  const logs: any[] = [];
  let port = 0;
  const logger: Logger = {
    debug() {},
    info(value: any) {
      logs.push(value);
      if (value.address?.port) port = value.address.port;
    },
    warn(value: any) {
      logs.push(value);
    },
    error(value: any) {
      logs.push(value);
    },
    child() {
      return this;
    },
  };
  const aggregate = compile("sales", "order", {
    initialState: {},
    commands: {
      create: [
        only.ifNotExists(),
        only.ifValidatedBy({
          type: "object",
          properties: { title: { type: "string" } },
          required: ["title"],
        }),
        (cmd: any, state: any) => state.apply.created(cmd.payload),
      ],
      reject: [
        () => {
          throw new Rejection(
            "precondition",
            "No credit",
            undefined,
            "no-credit",
          );
        },
      ],
      fail: [(_cmd: any, state: any) => state.apply.created({ title: "fail" })],
      slow: [
        async () => {
          await new Promise((resolve) => setTimeout(resolve, 80));
        },
      ],
    },
    events: {
      created: (event: any, state: any) =>
        state.set("title", event.payload.title),
    },
    eventEnricher: (_event: any, _state: any, cmd: any) => {
      if (cmd.name === "fail") throw new Error("private database credential");
    },
  });
  const readmodelStore = createMemoryReadmodelStore(),
    transport = createInMemoryTransport();
  const application = createApplication({
    name: "http-tests",
    logger,
    store: createInMemoryEventStore(),
    transport,
    aggregates: [aggregate],
    pollInterval: 5,
    readmodels: {
      detail: {
        store: readmodelStore,
        models: {
          order: {
            schema: {
              type: "object",
              properties: {
                title: { type: "string" },
                tenant: { type: "string" },
              },
            },
            reactions: {
              "sales.order.created": (event: any, vm: any) =>
                vm.set({
                  title: event.payload.title,
                  tenant: event.metadata.tenant ?? "a",
                }),
            },
          },
        },
      },
    },
  });
  await application.start({
    only: options.consumers ? ["commands:*", "readmodels:*"] : ["commands:*"],
  });
  cleanups.push(() => application.close());
  const stop = await startGateway("http", definition, {
    application,
    logger,
    invoke: async () => {},
    timeout: 500,
  });
  cleanups.push(stop);
  return {
    application,
    transport,
    readmodelStore,
    logs,
    url: `http://127.0.0.1:${port}`,
    stop,
  };
}
const commandInput = (req: any) => ({
  stream: { context: "sales", aggregate: "order", id: req.params.id ?? "one" },
  name: req.params.action ?? "create",
  payload: req.body,
});
test("HTTP Commands map outcomes, retain Command ids and log sanitized failures with Correlation ids", async () => {
  const { url, logs } = await httpFixture({
    port: 0,
    access: { domain: { "*": true } },
    routes: [
      {
        method: "post",
        path: "/:id/:action",
        handler: command(commandInput, {
          creates: true,
          codes: { "no-credit": 409 },
          deadline: 5,
        }),
      },
    ],
  } as any);
  const send = (id: string, action: string, body: any = { title: "first" }) =>
    fetch(`${url}/${id}/${action}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  const accepted = await send("one", "create");
  expect(accepted.status).toBe(201);
  expect(accepted.headers.get("EventFabric-Command-Id")).toBeTruthy();
  expect(accepted.headers.get("EventFabric-Position")).toBeTruthy();
  const validation = await send("two", "create", {});
  expect(validation.status).toBe(400);
  expect(validation.headers.get("EventFabric-Command-Id")).toBeTruthy();
  const rejected = await send("one", "reject");
  expect(rejected.status).toBe(409);
  const failed = await send("one", "fail");
  expect(failed.status).toBe(500);
  expect(await failed.text()).not.toContain("credential");
  const failedId = failed.headers.get("EventFabric-Command-Id");
  expect(
    logs.some(
      (log) => log.commandId === failedId && log.correlationId === failedId,
    ),
  ).toBe(true);
  const unknown = await send("one", "slow");
  expect(unknown.status).toBe(504);
  expect(unknown.headers.get("EventFabric-Command-Id")).toBeTruthy();
});

test("Command respondWith combines client minPosition and the committed Position using the Gateway wait timeout", async () => {
  const { url, readmodelStore } = await httpFixture({
    port: 0,
    waitTimeout: 5,
    access: { domain: { "*": true }, readmodels: { "*": true } },
    routes: [
      {
        method: "post",
        path: "/:id",
        handler: command(commandInput, { respondWith: "detail.order" }),
      },
    ],
  });
  await readmodelStore.fence("detail", 1);
  await readmodelStore.batch("detail", 1, position, [
    { model: "order", id: "one", document: { title: "old", tenant: "a" } },
  ]);
  const started = Date.now();
  const response = await fetch(`${url}/one`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "EventFabric-Min-Position": encodePosition({
        transactionId: "999",
        sequence: "1",
      }),
    },
    body: JSON.stringify({ title: "new" }),
  });
  expect(response.status).toBe(200);
  expect(await response.json()).toMatchObject({ title: "old" });
  expect(response.headers.get("EventFabric-Behind")).toBe(
    encodePosition(position),
  );
  expect(Date.now() - started).toBeLessThan(500);
});

test("Gateway parses JSON, runs Application middleware in order and builds Request context once with opt-in timing", async () => {
  const order: string[] = [];
  let contexts = 0;
  const { url } = await httpFixture({
    port: 0,
    serverTiming: true,
    middleware: [
      (req: any, _res: any, next: any) => {
        order.push(`first:${req.body.title}`);
        next();
      },
      (_req: any, _res: any, next: any) => {
        order.push("second");
        next();
      },
    ],
    context: async () => {
      contexts++;
      order.push("context");
      return { actor: "one" };
    },
    routes: [
      {
        method: "post",
        path: "/context",
        handler: (req: any, res: any) => {
          expect(handle(req)).toBe(handle(req));
          res.json(handle(req).ctx);
        },
      },
    ],
  });
  const response = await fetch(`${url}/context`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ title: "parsed" }),
  });
  expect(await response.json()).toEqual({ actor: "one" });
  expect(contexts).toBe(1);
  expect(order).toEqual(["first:parsed", "second", "context"]);
  expect(response.headers.get("Server-Timing")).toMatch(/total;dur=\d/);
});

test("HTTP denies missing and specific Access rules before sending and scopes id/query reads", async () => {
  const { url, readmodelStore, application } = await httpFixture({
    port: 0,
    access: {
      domain: { "*": true, "sales.order.create": false },
      readmodels: { "detail.*": (ctx) => ({ tenant: ctx.tenant }) },
    },
    context: () => ({ tenant: "a" }),
    routes: [
      { method: "post", path: "/:id", handler: command(commandInput) },
      {
        method: "get",
        path: "/orders/:id",
        handler: readmodel("detail", "order", {
          id: (req) => String(req.params.id),
        }),
      },
      { method: "get", path: "/orders", handler: readmodel("detail", "order") },
      {
        method: "get",
        path: "/hidden",
        handler: readmodel("missing", "model"),
      },
    ],
  });
  await readmodelStore.fence("detail", 1);
  await readmodelStore.batch("detail", 1, position, [
    { model: "order", id: "mine", document: { title: "mine", tenant: "a" } },
    { model: "order", id: "other", document: { title: "secret", tenant: "b" } },
  ]);
  const denied = await fetch(`${url}/one`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ title: "no" }),
  });
  expect(denied.status).toBe(403);
  expect(denied.headers.get("EventFabric-Command-Id")).toBeTruthy();
  expect(await application.config.store.readAll()).toEqual([]);
  expect((await fetch(`${url}/orders/other`)).status).toBe(404);
  expect(
    (await (await fetch(`${url}/orders`)).json()).map((doc: any) => doc._id),
  ).toEqual(["mine"]);
  expect((await fetch(`${url}/hidden`)).status).toBe(403);
});

test("legacyList answers the old list query string as { total, data } within the caller's Access scope", async () => {
  const { legacyList } = await import("../src/compat.js");
  const { url, readmodelStore } = await httpFixture({
    port: 0,
    access: { readmodels: { "detail.*": (ctx) => ({ tenant: ctx.tenant }) } },
    context: () => ({ tenant: "a" }),
    routes: [
      { method: "get", path: "/list", handler: legacyList("detail", "order") },
      // The route's query overrides the request's, key by key.
      {
        method: "get",
        path: "/pairs",
        handler: legacyList("detail", "order", {
          query: { size: "2", sort: "title" },
        }),
      },
      {
        method: "get",
        path: "/even",
        handler: legacyList("detail", "order", {
          transforms: [
            async (document: any) =>
              Number(document.title.slice(1)) % 2 ? null : document,
            (document: any, request) => ({
              title: document.title,
              tenant: request.ctx.tenant,
            }),
          ],
        }),
      },
    ],
  });
  const title = (i: number) => `t${String(i).padStart(3, "0")}`;
  await readmodelStore.fence("detail", 1);
  await readmodelStore.batch("detail", 1, position, [
    ...Array.from({ length: 120 }, (_, i) => ({
      model: "order",
      id: `a${i}`,
      document: { title: title(i), tenant: "a" },
    })),
    ...Array.from({ length: 5 }, (_, i) => ({
      model: "order",
      id: `b${i}`,
      document: { title: title(i), tenant: "b" },
    })),
  ]);
  const list = async (path: string) => {
    const response = await fetch(`${url}${path}`);
    return { status: response.status, body: await response.json() };
  };
  const titles = (body: any) =>
    body.data.map((document: any) => document.title);
  // The old default page of 100; total counts every readable match, before paging.
  const all = await list("/list");
  expect([all.status, all.body.total, all.body.data.length]).toEqual([
    200, 120, 100,
  ]);
  expect((await list("/list?size=0")).body.data).toHaveLength(120);
  expect(titles((await list("/list?sort=-title&from=1&size=2")).body)).toEqual([
    title(118),
    title(117),
  ]);
  expect((await list("/list?title=t001,%20t002&tenant=b")).body).toEqual({
    total: 0,
    data: [],
  });
  expect((await list("/list?title=t001,%20t002")).body.total).toBe(2);
  expect(
    (await list("/list?title[ne][]=t001&title[ne][]=t002")).body.total,
  ).toBe(118);
  expect((await list("/list?title.gte=t110&title.lt=t115")).body.total).toBe(5);
  const pairs = await list("/pairs?size=7&title.gte=t050");
  expect([pairs.body.total, titles(pairs.body)]).toEqual([
    70,
    [title(50), title(51)],
  ]);
  const even = await list("/even?sort=title&size=4");
  expect(even.body).toEqual({
    total: 120,
    data: [
      { title: title(0), tenant: "a" },
      { title: title(2), tenant: "a" },
    ],
  });
  expect(await list("/list?size=x")).toEqual({
    status: 400,
    body: expect.objectContaining({ message: "Bad Request" }),
  });
});

test("HTTP reports persisted halted Readmodel groups as behind even without minPosition", async () => {
  const { url, application, readmodelStore } = await httpFixture({
    port: 0,
    access: { readmodels: { "*": true } },
    routes: [
      {
        method: "get",
        path: "/one",
        handler: readmodel("detail", "order", { id: () => "one" }),
      },
      { method: "get", path: "/", handler: readmodel("detail", "order") },
    ],
  });
  await readmodelStore.fence("detail", 1);
  await readmodelStore.batch("detail", 1, position, [
    { model: "order", id: "one", document: { tenant: "a", title: "old" } },
  ]);
  await application.config.store.setConsumerStatus({
    group: "readmodels:detail",
    partition: 0,
    state: "halted",
    checkpoint: position,
    eventId: "bad",
    position: { transactionId: "2", sequence: "1" },
  });
  for (const path of ["/one", "/"]) {
    const response = await fetch(`${url}${path}`);
    expect(response.status).toBe(200);
    expect(response.headers.get("EventFabric-Behind")).toBe(
      encodePosition(position),
    );
    expect(response.headers.get("EventFabric-Position")).toBe(
      encodePosition(position),
    );
  }
});

test("HTTP accepted-but-missing respondWith returns 202 and bad Position tokens fail before the Command", async () => {
  const { url, application } = await httpFixture({
    port: 0,
    waitTimeout: 5,
    access: { domain: { "*": true }, readmodels: { "*": true } },
    routes: [
      {
        method: "post",
        path: "/:id",
        handler: command(commandInput, { respondWith: "detail.order" }),
      },
    ],
  });
  const send = (id: string, token?: string) =>
    fetch(`${url}/${id}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(token ? { "EventFabric-Min-Position": token } : {}),
      },
      body: JSON.stringify({ title: "first" }),
    });
  const invalid = await send("bad", "broken");
  expect(invalid.status).toBe(400);
  expect(invalid.headers.get("EventFabric-Command-Id")).toBeTruthy();
  expect(await application.config.store.readAll()).toEqual([]);
  const accepted = await send("one");
  expect(accepted.status).toBe(202);
  expect(accepted.headers.get("EventFabric-Position")).toBeTruthy();
  expect(accepted.headers.get("EventFabric-Behind")).toBe("none");
  expect((await accepted.json()).status).toBe("accepted");
});

test("a failed respondWith read after an accepted Command answers 202 with the Outcome, not 500", async () => {
  const { url, application, logs } = await httpFixture({
    port: 0,
    access: {
      domain: { "*": true },
      readmodels: {
        "*": () => {
          throw new Error("sorry, too many clients already");
        },
      },
    },
    routes: [
      {
        method: "post",
        path: "/:id",
        handler: command(commandInput, { respondWith: "detail.order" }),
      },
    ],
  });
  const response = await fetch(`${url}/one`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ title: "first" }),
  });
  expect(response.status).toBe(202);
  expect(response.headers.get("EventFabric-Position")).toBeTruthy();
  expect(await response.json()).toMatchObject({ status: "accepted" });
  expect(await application.config.store.readAll()).toHaveLength(1);
  expect(logs).toContainEqual(
    expect.objectContaining({ error: "sorry, too many clients already" }),
  );
});

test("respondWith without a model fails when the route is defined, before any Command", () => {
  expect(() => command(commandInput, { respondWith: "detail" as any })).toThrow(
    "respondWith requires group.readmodel",
  );
});

async function until(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  pattern: string,
) {
  let text = "";
  for (;;) {
    const result = await reader.read();
    if (result.done) return text;
    text += new TextDecoder().decode(result.value);
    if (text.includes(pattern)) return text;
  }
}
test("SSE streams retry, heartbeat and scoped Notifications, then Gateway shutdown drains the connection", async () => {
  const { url, transport, readmodelStore, stop } = await httpFixture({
    port: 0,
    access: { readmodels: { "*": (ctx) => ({ tenant: ctx.tenant }) } },
    context: () => ({ tenant: "a" }),
    routes: [
      {
        method: "get",
        path: "/live",
        handler: async (req: any, res: any) => {
          const stream = sse(req, res, { heartbeat: 10, retry: 1234 });
          const subscription = await handle(req).readmodels.subscribe(
            "detail",
            "order",
            (notification: any) => stream.send("changed", notification),
            { onError: () => stream.close() },
          );
          res.on("close", () => void subscription.stop());
          stream.send("initial", {
            documents: subscription.initial,
            position: subscription.position,
          });
        },
      },
    ],
  });
  await readmodelStore.fence("detail", 1);
  await readmodelStore.batch("detail", 1, position, [
    { model: "order", id: "one", document: { tenant: "a", title: "mine" } },
  ]);
  const response = await fetch(`${url}/live`),
    reader = response.body!.getReader();
  expect(response.headers.get("Content-Type")).toBe("text/event-stream");
  // The heartbeat timer starts before the subscription resolves, so wait for each frame separately.
  let initial = await until(reader, "event: initial");
  if (!initial.includes(": heartbeat"))
    initial += await until(reader, ": heartbeat");
  expect(initial).toContain("retry: 1234");
  expect(initial).toContain("event: initial");
  expect(initial).toContain(": heartbeat");
  await transport.publish("readmodels", {
    kind: "readmodel",
    group: "detail",
    position: { transactionId: "2", sequence: "1" },
    changes: [
      {
        model: "order",
        id: "one",
        before: { tenant: "a" },
        after: { tenant: "b", secret: "PRIVATE" },
      },
    ],
  });
  const notification = await until(reader, "event: changed");
  expect(notification).toContain('"ids":["one"]');
  expect(notification).not.toContain("PRIVATE");
  expect(notification).not.toContain("changes");
  await stop();
  // A heartbeat may still arrive before the close; the stream must end without another event.
  expect(await until(reader, "event:")).not.toContain("event:");
});

test("SSE closes before a single oversized frame exceeds its bounded buffer", async () => {
  const { url } = await httpFixture({
    port: 0,
    routes: [
      {
        method: "get",
        path: "/overflow",
        handler: (req: any, res: any) => {
          const stream = sse(req, res, { heartbeat: 10, maxBuffer: 4096 });
          stream.send("too-large", { value: "x".repeat(8192) });
        },
      },
    ],
  });
  const response = await fetch(`${url}/overflow`);
  const text = await response.text();
  expect(text).toContain("retry:");
  expect(text).not.toContain("too-large");
}, 1000);

test("Gateway metadata is explicit and per-call values shallowly override its mapping", async () => {
  const { url } = await httpFixture({
    port: 0,
    context: () => ({ tenant: "a", privateContext: "never-copy" }),
    commandMetadata: (ctx) => ({
      tenant: ctx.tenant,
      owner: { role: "mapped", id: "one" },
    }),
    access: { domain: { "*": true } },
    routes: [
      {
        method: "post",
        path: "/:id",
        handler: command((req) => ({
          ...commandInput(req),
          metadata: { owner: { id: "override" } },
        })),
      },
    ],
  });
  const response = await fetch(`${url}/one`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ title: "first" }),
  });
  expect(response.status).toBe(200);
  expect((await response.json()).events[0].metadata).toEqual({
    tenant: "a",
    owner: { id: "override" },
  });
});

test("Command validation fails with the Command id; a missing awaited Event answers 202 since the Command is recorded", async () => {
  const { url, logs } = await httpFixture({
    port: 0,
    access: { domain: { "*": true } },
    routes: [
      {
        method: "post",
        path: "/bad",
        handler: command(() => ({
          stream: { context: "invalid.address", aggregate: "order", id: "one" },
          name: "create",
        })),
      },
      {
        method: "post",
        path: "/await",
        handler: command(commandInput, { await: "sales.order.never" }),
      },
      { method: "post", path: "/:id", handler: command(commandInput) },
    ],
  });
  const send = (path: string) =>
    fetch(`${url}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ title: "first" }),
    });
  const bad = await send("/bad");
  expect(bad.status).toBe(400);
  expect(bad.headers.get("EventFabric-Command-Id")).toBeTruthy();
  const missing = await send("/await");
  expect(missing.status).toBe(202);
  expect(await missing.json()).toMatchObject({ status: "accepted" });
  const id = missing.headers.get("EventFabric-Command-Id");
  expect(logs.some((log) => log.correlationId === id)).toBe(true);
  await send("/existing");
  const duplicate = await send("/existing");
  expect(duplicate.status).toBe(404);
});

test("SSE closes with an error when a slow subscriber overflows its Notification buffer", async () => {
  const { url, transport } = await httpFixture({
    port: 0,
    access: { readmodels: { "*": true } },
    routes: [
      {
        method: "get",
        path: "/live",
        handler: async (req: any, res: any) => {
          const stream = sse(req, res);
          const subscription = await handle(req).readmodels.subscribe(
            "detail",
            "order",
            async (notification: any) => {
              stream.send("changed", notification);
              await new Promise((resolve) => setTimeout(resolve, 50));
            },
            {
              bufferSize: 1,
              onError: () => {
                stream.send("error", { code: "buffer-overflow" });
                stream.close();
              },
            },
          );
          res.on("close", () => void subscription.stop());
          stream.send("ready", {});
        },
      },
    ],
  });
  const response = await fetch(`${url}/live`),
    reader = response.body!.getReader();
  await until(reader, "event: ready");
  for (const tx of ["2", "3", "4"])
    await transport.publish("readmodels", {
      kind: "readmodel",
      group: "detail",
      position: { transactionId: tx, sequence: "1" },
      changes: [
        { model: "order", id: "one", before: null, after: { tenant: "a" } },
      ],
    });
  const text = await until(reader, "buffer-overflow");
  expect(text).toContain("event: error");
  expect((await reader.read()).done).toBe(true);
});

test("Idempotency-Key with a scope makes an HTTP retry a Duplicate Command; other scopes and payloads do not replay", async () => {
  const { url } = await httpFixture({
    port: 0,
    context: (req) => ({ tenant: req.headers["x-tenant"] }),
    idempotencyScope: (ctx) => ctx.tenant,
    access: { domain: { "*": true } },
    routes: [
      {
        method: "post",
        path: "/orders",
        handler: command(
          (req) => ({
            stream: { context: "sales", aggregate: "order" },
            name: "create",
            payload: req.body,
          }),
          { creates: true },
        ),
      },
    ],
  });
  const post = (tenant: string, title: string) =>
    fetch(`${url}/orders`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": "order-1",
        "X-Tenant": tenant,
      },
      body: JSON.stringify({ title }),
    });
  const first = await post("a", "first");
  const firstBody = await first.json();
  expect(first.status).toBe(201);
  expect(first.headers.get("EventFabric-Duplicate")).toBeNull();
  const retry = await post("a", "first");
  expect(retry.status).toBe(201);
  expect(retry.headers.get("EventFabric-Duplicate")).toBe("true");
  expect(retry.headers.get("EventFabric-Command-Id")).toBe(
    first.headers.get("EventFabric-Command-Id"),
  );
  // A retried create replays the Aggregate it created.
  expect((await retry.json()).events[0].stream.id).toBe(
    firstBody.events[0].stream.id,
  );
  const otherTenant = await post("b", "first");
  expect(otherTenant.status).toBe(201);
  expect(otherTenant.headers.get("EventFabric-Duplicate")).toBeNull();
  expect(otherTenant.headers.get("EventFabric-Command-Id")).not.toBe(
    first.headers.get("EventFabric-Command-Id"),
  );
  const reused = await post("a", "different");
  expect(reused.status).toBe(409);
  expect((await reused.json()).error.kind).toBe("conflict");
});

test("without an idempotencyScope the Idempotency-Key header is ignored", async () => {
  const { url } = await httpFixture({
    port: 0,
    access: { domain: { "*": true } },
    routes: [
      {
        method: "post",
        path: "/orders",
        handler: command(
          (req) => ({
            stream: { context: "sales", aggregate: "order" },
            name: "create",
            payload: req.body,
          }),
          { creates: true },
        ),
      },
    ],
  });
  const post = () =>
    fetch(`${url}/orders`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Idempotency-Key": "order-1",
      },
      body: JSON.stringify({ title: "first" }),
    });
  const first = await post(),
    second = await post();
  expect([first.status, second.status]).toEqual([201, 201]);
  expect(second.headers.get("EventFabric-Duplicate")).toBeNull();
  expect(second.headers.get("EventFabric-Command-Id")).not.toBe(
    first.headers.get("EventFabric-Command-Id"),
  );
});
test("an old REST command descriptor rewritten by migrateSource runs over HTTP and answers with the respondWith document", async () => {
  const { migrateSource } = await import("../src/migration/index.js");
  const { source, reviews } = await migrateSource(
    "const route = command(domain => ({ command: domain.sales.order.create, event: 'readmodel.detail.order.created' }), { id: req => req.params.id, payload: req => req.body });",
  );
  expect(source).toBe(
    "const route = command((req) => ({ stream: { context: 'sales', aggregate: 'order', id: req.params.id }, name: 'create', payload: req.body }), { respondWith: 'detail.order' });",
  );
  expect(reviews).toHaveLength(1);
  const handler = new Function("command", `${source}\nreturn route;`)(command);
  const { url } = await httpFixture(
    {
      port: 0,
      access: { domain: { "*": true }, readmodels: { "*": true } },
      routes: [{ method: "post", path: "/:id", handler }],
    },
    { consumers: true },
  );
  const send = (body: unknown) =>
    fetch(`${url}/migrated`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  const created = await send({ title: "first" });
  expect(created.status).toBe(200);
  expect(await created.json()).toMatchObject({ title: "first", tenant: "a" });
  expect((await send({ title: "again" })).status).toBe(404);
  expect((await send({})).status).toBe(400);
});
