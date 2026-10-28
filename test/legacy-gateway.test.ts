import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createServer } from "node:http";
import { afterEach, expect, test } from "vitest";
import { compile } from "../src/authoring/index.js";
import { createApplication } from "../src/runtime/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import {
  legacyGateway,
  legacyPath,
  legacyRest,
  legacyServices,
} from "../src/compat.js";
import { legacyError } from "../src/legacy-gateway.js";
import { Rejection } from "../src/errors.js";

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
// An old Gateway folder: CommonJS files that take the toolkit from a global, as their replaced import would.
const folder = (files: Record<string, string>) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "legacy-gateway-"));
  cleanups.push(() => fs.rmSync(directory, { recursive: true, force: true }));
  fs.writeFileSync(
    path.join(directory, "package.json"),
    '{ "type": "commonjs" }',
  );
  for (const [name, source] of Object.entries(files)) {
    fs.mkdirSync(path.dirname(path.join(directory, name)), { recursive: true });
    fs.writeFileSync(path.join(directory, name), source);
  }
  return directory;
};
// JSON of a response, or its URL, status and text when it is not JSON.
const json = async (response: Response) => {
  const text = await response.text();
  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`${response.url} ${response.status}: ${text}`);
  }
};
class ConflictError extends Error {
  override name = "ConflictError";
  statusCode = 409;
  errorCode = "conflict";
  data = { field: "title" };
}

test("old route trees answer with the old toolkit, request Application, errors and HTTP defaults", async () => {
  (globalThis as any).legacyToolkit = legacyRest;
  const directory = folder({
    "setup.js": `module.exports = { access: { readmodels: { 'detail.*': metadata => ({ tenant: metadata.tenant }) } } };`,
    "routes/orders.route.js": `
const { services, command, readmodels, async } = globalThis.legacyToolkit;
module.exports = {
  schema: { tags: ['orders'] },
  before: [services(s => s.auth)],
  routes: {
    '/': {
      post: [command(domain => ({ command: domain.sales.order.create, event: 'readmodel.detail.order.create' }), { id: req => req.body.id })],
      get: [readmodels.list(r => r.detail.order, { transforms: [doc => doc.title === 'skip' ? null : { ...doc, seen: true }] })],
    },
    '/:order_id': [readmodels.one(r => r.detail.order)],
    '/:id/rename': command(domain => ({ command: 'sales.order.rename', event: 'domain.sales.order.renamed' })),
    '/:id/done': command(domain => ({ command: 'system.manager.done', event: 'system.manager.done' }), { transforms: [o => o.payload] }),
    'app/:id': { post: async(async (req, res) => {
      const event = await req.getApp().getDomain().sales.order(req.params.id).rename({ title: 'own' }, { source: 'own', extra: 1 }).await('sales.order.renamed');
      const found = await req.getApp().getReadmodels().detail.order.find();
      const built = await req.getApp().getReadmodels().detail.order().filter({ title: ['first', 'second', 'own'] }).sort([{ title: 'asc' }]);
      res.json({ metadata: event.metadata, found: found.length, built: built.map(doc => doc._id), errors: Object.keys(req.getApp().errors) });
    }) },
    'app/:id/correlated': { post: async(async (req, res) => {
      const waited = await req.getApp().awaitCorrelated({ stream: { context: 'sales', aggregate: 'order', id: req.params.id }, name: 'rename', payload: { title: 'own' }, metadata: { source: 'own' } }, 'sales.order.renamed');
      res.json({ status: waited.status, metadata: waited.event.metadata });
    }) },
    // A wait route code never consumes, on a rejected Command.
    'app/:id/unconsumed': { post: async(async (req, res) => {
      req.getApp().getDomain().sales.order(req.params.id).rename({ title: 'taken' }).await('sales.order.renamed');
      res.json({ sent: true });
    }) },
    'native/all': async(async (req, res) => res.json(await req.getApp().getReadmodels().detail.order().base().find({}).limit(1).toArray())),
  },
};`,
    "routes/raw/index.js": `
module.exports = {
  // Old middleware added to the query object, as Express 4 allowed.
  before: [(req, _res, next) => { Object.assign(req.query, { added: 'yes' }); next(); }],
  routes: { '/:x?': { post: (req, res) => res.json({ raw: req.rawBody && req.rawBody.toString(), x: req.params.x || null, q: req.query, app: req.app_name }) } },
};`,
  });
  const port = await freePort();
  const described: string[] = [];
  const order = compile("sales", "order", {
    commands: {
      create: (command: any, state: any) =>
        state.apply.created({ title: command.payload.title }),
      rename: (command: any, state: any) => {
        if (command.payload.title === "taken")
          throw new ConflictError("Title taken");
        state.apply.renamed({ title: command.payload.title });
      },
    },
    events: { created: () => {}, renamed: () => {} },
  });
  const application = createApplication({
    name: "legacy-gateway",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [order],
    pollInterval: 5,
    services: {
      auth: (req: any, _res: any, next: any) => {
        req.meta = { tenant: req.get("x-tenant"), source: "request" };
        next();
      },
    },
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
              "sales.order.created": (event: any, view: any) => {
                view.set("tenant", event.metadata.tenant);
                view.set("title", event.payload.title);
              },
              "sales.order.renamed": (event: any, view: any) =>
                view.set("title", event.payload.title),
            },
          },
        },
      },
    },
    gateways: {
      legacy: legacyGateway({
        directory,
        port,
        metadata: (req: any) => req.meta,
        idempotencyScope: (metadata: any) => metadata?.tenant,
        requestFields: { app_name: "legacy" },
        errors: { ConflictError },
        commandSchemas: { "sales.order.create": { type: "object" } },
        // An old Command that is no longer one: the route answers what this returns.
        replacedCommands: {
          "system.manager.done": (req: any, { id, payload }) => {
            if (id === "late") throw new ConflictError("Too late");
            return {
              aggregate: { id },
              payload: { id, payload, tenant: req.meta.tenant },
            };
          },
        },
        readmodelSchemas: { "detail.order": { type: "object" } },
        // As the old swagger plugin read it: the route's schema, or a toolkit operation's in a handler list.
        onRoute: ({ uri, method, operation }) => {
          const inner = [operation.handler]
            .flat()
            .find((handler: any) => typeof handler === "object")?.schema;
          described.push(
            `${method} ${uri} ${operation.schema.tags ?? ""}${inner?.body ? " body" : ""}${inner?.responses?.[200]?.schema?.properties?.total ? " list" : ""}`,
          );
        },
        afterRoutes: async (app) => {
          await new Promise((resolve) => setTimeout(resolve, 5));
          app.get("/described", (_req, res) => void res.json(described));
        },
      }),
    },
  } as any);
  await application.start();
  cleanups.push(() => application.close());
  const url = `http://127.0.0.1:${port}`;
  const post = (route: string, body: unknown, tenant = "a") =>
    fetch(`${url}${route}`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-tenant": tenant },
      body: JSON.stringify(body),
    });

  // A Command waits for the Readmodel document its Events wrote, read through the Access map.
  const created = await post("/orders", { id: "one", title: "first" });
  expect(created.status).toBe(200);
  expect(created.headers.get("access-control-allow-origin")).toBe("*");
  expect(await json(created)).toMatchObject({
    _id: "one",
    tenant: "a",
    title: "first",
  });
  await post("/orders", { id: "two", title: "skip" });
  await post("/orders", { id: "other", title: "secret" }, "b");

  // Lists answer { total, data }: total counts before transforms; the Access map hides the other tenant.
  const listed = await fetch(`${url}/orders?sort=title`, {
    headers: { "x-tenant": "a" },
  });
  expect(await json(listed)).toEqual({
    total: 2,
    data: [expect.objectContaining({ _id: "one", seen: true })],
  });
  // One document: absent or another tenant's answers 200 with an empty body, as the old stream did.
  const mine = await fetch(`${url}/orders/one`, {
    headers: { "x-tenant": "a" },
  });
  expect(await json(mine)).toMatchObject({ title: "first" });
  const theirs = await fetch(`${url}/orders/other`, {
    headers: { "x-tenant": "a" },
  });
  expect([theirs.status, await theirs.text()]).toEqual([200, ""]);

  // A domain Event wait answers { aggregate, payload }; a thrown error answers in the old body as its class.
  const renamed = await post("/orders/one/rename", { title: "second" });
  expect(await json(renamed)).toEqual({
    aggregate: { id: "one", name: "order", revision: 2 },
    payload: { title: "second" },
  });
  // Command routes answer the new Gateway's Command headers, and an Idempotency-Key repeats as a Duplicate.
  expect(renamed.headers.get("eventfabric-command-id")).toMatch(/^[0-9a-f-]{36}$/);
  expect(renamed.headers.get("eventfabric-position")).toBeTruthy();
  const keyed = (title: string) =>
    fetch(`${url}/orders/one/rename`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-tenant": "a", "idempotency-key": "rename-once" },
      body: JSON.stringify({ title }),
    });
  const once = await keyed("second"),
    again = await keyed("second");
  expect(await json(again)).toEqual(await json(once));
  expect(once.headers.get("eventfabric-duplicate")).toBeNull();
  expect(again.headers.get("eventfabric-duplicate")).toBe("true");
  expect(again.headers.get("eventfabric-command-id")).toBe(once.headers.get("eventfabric-command-id"));
  const taken = await post("/orders/one/rename", { title: "taken" });
  expect([taken.status, await json(taken)]).toEqual([
    409,
    {
      error: {
        code: "conflict",
        message: "Title taken",
        data: { field: "title" },
      },
    },
  ]);

  // A replaced Command sends nothing; its route answers the function's value or its error.
  expect(await json(await post("/orders/one/done", { at: 1 }))).toEqual({
    id: "one",
    payload: { at: 1 },
    tenant: "a",
  });
  const late = await post("/orders/late/done", {});
  expect([late.status, (await json(late)).error.message]).toEqual([
    409,
    "Too late",
  ]);

  // req.getApp(): Commands carry the request metadata over their own; reads are Access-scoped.
  const own = await post("/orders/app/one", {});
  expect(await json(own)).toEqual({
    metadata: expect.objectContaining({
      tenant: "a",
      source: "request",
      extra: 1,
    }),
    found: 2,
    built: ["one"],
    errors: ["ConflictError"],
  });
  // The awaitCorrelated migrateSource writes for a chain-wide .await: the request metadata wins, as with getDomain().
  const correlated = await post("/orders/app/one/correlated", {});
  expect(await json(correlated)).toEqual({
    status: "accepted",
    metadata: expect.objectContaining({ tenant: "a", source: "request" }),
  });
  // Its rejection raises nothing (the old wait ran only once consumed), so the process keeps running.
  expect(await json(await post("/orders/app/one/unconsumed", {}))).toEqual({
    sent: true,
  });
  await new Promise((resolve) => setTimeout(resolve, 50));
  // The old builder's base(): this store cannot scope its native handle to the tenant rule.
  const base = await fetch(`${url}/orders/native/all`, {
    headers: { "x-tenant": "a" },
  });
  expect(base.status).toBe(401);

  // HTTP defaults: the raw JSON body, vnd.api+json, bracket queries, optional params, CORS, 404, bad JSON.
  const raw = await fetch(`${url}/raw/value?filter[a]=1`, {
    method: "POST",
    headers: { "content-type": "application/vnd.api+json" },
    body: '{"a":1}',
  });
  expect(await json(raw)).toEqual({
    raw: '{"a":1}',
    x: "value",
    q: { filter: { a: "1" }, added: "yes" },
    app: "legacy",
  });
  expect(await json(await post("/raw", {}))).toMatchObject({ x: null });
  const preflight = await fetch(`${url}/orders`, {
    method: "OPTIONS",
    headers: { "access-control-request-headers": "x-tenant" },
  });
  expect([
    preflight.status,
    preflight.headers.get("access-control-allow-methods"),
    preflight.headers.get("access-control-allow-headers"),
  ]).toEqual([200, "GET,POST,PATCH,PUT,DELETE", "x-tenant"]);
  // Each route method as the old onRoute hook saw it: the old path and the file's schema; then afterRoutes.
  expect(await json(await fetch(`${url}/described`))).toEqual([
    "post /orders orders body",
    "get /orders orders list",
    "get /orders/:order_id orders",
    "post /orders/:id/rename orders",
    "post /orders/:id/done orders",
    "post /orders/app/:id orders",
    "post /orders/app/:id/correlated orders",
    "post /orders/app/:id/unconsumed orders",
    "get /orders/native/all orders",
    "post /raw/:x? ",
  ]);
  const missing = await fetch(`${url}/nothing`);
  expect([missing.status, await missing.text()]).toEqual([404, "Not Found."]);
  const invalid = await fetch(`${url}/orders`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: "{",
  });
  expect(invalid.status).toBe(400);
  expect(await json(invalid)).toMatchObject({
    error: { message: expect.any(String) },
  });
});

test("onRoute rewriting schemas does not change a second build of the routes", async () => {
  const directory = folder({
    "routes/index.js": "module.exports = { schema: { tags: ['own'], pattern: /^a/ }, routes: { '/': { get: (req, res) => res.json({ ok: true }) } } };",
  });
  const port = await freePort();
  const seen: unknown[] = [];
  let builds = 0;
  const application = createApplication({
    name: "legacy-routes",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    gateways: {
      legacy: legacyGateway({
        directory,
        port,
        metadata: () => ({}),
        // As the old swagger plugin did: the route's schema rewritten in place.
        onRoute: ({ operation }) => {
          seen.push([operation.schema.tags, operation.schema.pattern instanceof RegExp]);
          operation.schema.tags = ["rewritten"];
        },
        afterRoutes: () => {
          if (!builds++) throw new Error("first build fails");
        },
      }),
    },
  } as any);
  await application.start();
  cleanups.push(() => application.close());
  const statuses = [];
  for (let i = 0; i < 2; i++) statuses.push((await fetch(`http://127.0.0.1:${port}/`)).status);
  expect([statuses, seen]).toEqual([[500, 200], [[["own"], true], [["own"], true]]]);
});
test("old route paths translate to Express 5", () => {
  expect(legacyPath("payments")).toBe("/payments");
  expect(legacyPath("/unlock/:hours?")).toBe("/unlock{/:hours}");
  expect(legacyPath("/phonecall/:entity/:file?")).toBe(
    "/phonecall/:entity{/:file}",
  );
  expect(legacyPath("updateFlowProfileMany/")).toBe("/updateFlowProfileMany");
  expect(() => legacyPath("/files/*")).toThrow("Unsupported");
});

test("old services folders build in folder order under their namespace, with the old accessor tree", async () => {
  const closed: string[] = [];
  (globalThis as any).legacyClosed = closed;
  const directory = folder({
    "a.js": `module.exports = { init: (store, app) => ({ app: app.name }) };`,
    "team/auth.service.js": `module.exports = { init: store => ({ base: store.get('a').app, own: store.services.a === store.get('a') }), close: () => globalThis.legacyClosed.push('auth') };`,
    "team/early.js": `module.exports = { init: store => store.get('team:late') };`,
    "team/late.js": `module.exports = { value: 1 };`,
    "notes.txt": "",
  });
  expect(() => legacyServices(directory, { name: "app" })).toThrow(
    "No service team:late initialized.",
  );
  fs.rmSync(path.join(directory, "team/early.js"));
  const { services, close } = legacyServices(
    directory,
    { name: "app" },
    { validator: { check: true } },
  );
  expect(services.team.auth()).toEqual({ base: "app", own: true });
  expect(services.team.late()).toEqual({ value: 1 });
  expect(services.validator()).toEqual({ check: true });
  await close();
  expect(closed).toEqual(["auth"]);
});

test("the old socket protocol reads, streams live notifications and answers Command waits", async () => {
  const { default: WebSocket } = await import("ws");
  const directory = folder({
    "setup.js": `
module.exports = {
  access: { readmodels: { 'detail.*': metadata => ({ tenant: metadata.tenant }) } },
  ws: {
    connection: [async req => {
      if (req.params.token !== 'good') throw new Error('Unauthorized');
      req.meta = { tenant: 'a', auth: { channel: req.app_name, socket: req.socket } };
    }],
    incoming: [async msg => { if (msg.payload && msg.payload.metadata && msg.payload.metadata.asTenant) msg.meta.tenant = msg.payload.metadata.asTenant; }],
  },
};`,
  });
  const port = await freePort();
  const order = compile("sales", "order", {
    commands: {
      create: (command: any, state: any) =>
        state.apply.created({ title: command.payload.title }),
      rename: (command: any) => {
        if (command.payload.title === "taken")
          throw new ConflictError("Title taken");
      },
    },
    events: { created: () => {} },
  });
  const application = createApplication({
    name: "legacy-socket",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
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
              "sales.order.created": (event: any, view: any) => {
                view.set("tenant", event.metadata.tenant);
                view.set("title", event.payload.title);
              },
            },
          },
        },
      },
    },
    gateways: {
      legacy: legacyGateway({
        directory,
        port,
        metadata: (req: any) => req.meta,
        requestFields: { app_name: "legacy" },
        errors: { ConflictError },
        live: {
          notifications: (metadata: any, notification: any) =>
            notification.payload.tenant === metadata.tenant,
        },
      }),
    },
  });
  await application.start();
  cleanups.push(() => application.close());

  const refused = new WebSocket(`ws://127.0.0.1:${port}/socket?token=bad`);
  const status = await new Promise((resolve) => {
    refused.on("unexpected-response", (_req, res) => resolve(res.statusCode));
    refused.on("open", () => resolve("open"));
  });
  expect(status).toBe(401);
  // The old client connects to `//?token=…`: its query is read, so a bad token is refused, not hung up on.
  const doubled = new WebSocket(`ws://127.0.0.1:${port}//?token=bad`);
  expect(
    await new Promise((resolve) => {
      doubled.on("unexpected-response", (_req, res) => resolve(res.statusCode));
      doubled.on("error", (error) => resolve(error.message));
    }),
  ).toBe(401);

  const socket = new WebSocket(
    `ws://127.0.0.1:${port}/socket?token=good&uniqueId=u1`,
  );
  cleanups.push(() => socket.close());
  const inbox: any[] = [];
  let arrived = () => {};
  socket.on("message", (raw) => {
    inbox.push(JSON.parse(String(raw)));
    arrived();
  });
  await new Promise((resolve) => socket.on("open", resolve));
  const send = (message: object) => socket.send(JSON.stringify(message));
  // The frames for one correlation id, once `count` of them arrived.
  const answers = async (correlationId: string, count: number) => {
    for (;;) {
      const found = inbox.filter((m) => m.correlationId === correlationId);
      if (found.length >= count) return found;
      await new Promise<void>((resolve) => (arrived = resolve));
    }
  };

  // Lock-style Notifications: the Gateway rule keeps other tenants' out.
  send({
    type: "subscribeEvents",
    correlationId: "locks",
    payload: { filter: { type: "system", name: "lock" } },
  });
  await answers("locks", 1);
  await application.notify("lock", { tenant: "b" });
  await application.notify("lock", { tenant: "a", operation: "locked" });
  const [, lock] = await answers("locks", 2);
  expect(lock).toMatchObject({
    type: "event",
    payload: {
      type: "system",
      name: "lock",
      fullname: "system.lock",
      payload: { tenant: "a", operation: "locked" },
    },
  });

  // A Command wait without a correlation id waits for the next Command this socket sends.
  send({
    type: "subscribeEvents",
    correlationId: "wait",
    payload: { filter: { metadata: {} } },
  });
  await answers("wait", 1);
  send({
    type: "sendCommand",
    correlationId: "send",
    payload: {
      context: "sales",
      aggregate: { name: "order", id: "s1" },
      name: "create",
      payload: { title: "sock" },
      metadata: { asTenant: "a" },
    },
  });
  expect((await answers("send", 1))[0]).toMatchObject({
    type: "sentCommand",
    statusCode: 200,
  });
  const waited = (await answers("wait", 4)).map((m) => m.payload);
  expect(waited.map((m) => m.fullname)).toEqual([
    undefined,
    "domain.sales.order.commandReceived",
    "domain.sales.order.created",
    "readmodel.detail.order.create",
  ]);
  expect(waited[2]).toMatchObject({
    type: "domain",
    aggregate: { name: "order", id: "s1", revision: 1 },
    metadata: {
      tenant: "a",
      auth: { channel: "legacy", socket: { uniqueId: "u1" } },
    },
  });
  expect(waited[3]).toMatchObject({
    readmodel: { type: "detail", collection: "order", id: "s1" },
    payload: { tenant: "a", title: "sock" },
    metadata: { correlationId: waited[2].metadata.correlationId },
  });

  send({
    type: "subscribeEvents",
    correlationId: "wait2",
    payload: { filter: { metadata: {} } },
  });
  await answers("wait2", 1);
  send({
    type: "sendCommand",
    correlationId: "send2",
    payload: {
      context: "sales",
      aggregate: { name: "order", id: "s1" },
      name: "rename",
      payload: { title: "taken" },
    },
  });
  const rejected = (await answers("wait2", 3))[2].payload;
  expect(rejected).toMatchObject({
    fullname: "domain.sales.order.commandRejected",
    payload: { name: "ConflictError", message: "Title taken" },
  });

  // Reads: a list answers its total, then its items; one document answers it alone.
  send({
    type: "subscribeRead",
    correlationId: "list",
    payload: { readmodel: { type: "detail", collection: "order" }, query: {} },
  });
  expect((await answers("list", 4)).map((m) => [m.type, m.payload])).toEqual([
    ["subscribedRead", undefined],
    ["stats", { total: 1 }],
    ["item", expect.objectContaining({ _id: "s1", title: "sock" })],
    ["finish", undefined],
  ]);
  send({
    type: "subscribeRead",
    correlationId: "one",
    payload: {
      readmodel: { type: "detail", collection: "order" },
      single: true,
      query: { id: "s1" },
    },
  });
  expect((await answers("one", 3)).map((m) => m.type)).toEqual([
    "subscribedRead",
    "item",
    "finish",
  ]);
  // The incoming middleware's metadata scopes reads: another tenant sees nothing.
  send({
    type: "subscribeRead",
    correlationId: "as-b",
    payload: {
      readmodel: { type: "detail", collection: "order" },
      query: {},
      metadata: { asTenant: "b" },
    },
  });
  expect((await answers("as-b", 3))[1].payload).toEqual({ total: 0 });

  // A frame cannot supply the connection's fields: its own metadata field is ignored.
  send({
    type: "subscribeRead",
    correlationId: "spoof",
    meta: { tenant: "b" },
    payload: { readmodel: { type: "detail", collection: "order" }, query: {} },
  });
  expect((await answers("spoof", 3))[1].payload).toEqual({ total: 1 });

  send({ type: "subscribeRead", payload: {} });
  await new Promise((resolve) => setTimeout(resolve, 50));
  expect(inbox.at(-1)).toMatchObject({
    type: "error",
    statusCode: 400,
    payload: { message: "corellationId missing." },
  });
});

test("the old socket coalesces auto-refresh changes, shares notifications between sockets and ends a Command wait that sees no change", async () => {
  const { default: WebSocket } = await import("ws");
  const directory = folder({
    "setup.js": `
module.exports = {
  access: { readmodels: { 'detail.*': metadata => ({ tenant: metadata.tenant }) } },
  ws: { connection: [async req => { req.meta = { tenant: 'a' }; }] },
};`,
  });
  const port = await freePort();
  const order = compile("sales", "order", {
    commands: {
      create: (command: any, state: any) =>
        state.apply.created({ title: command.payload.title }),
      touch: () => {},
    },
    events: { created: () => {} },
  });
  const application = createApplication({
    name: "legacy-socket-load",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
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
              "sales.order.created": (event: any, view: any) => {
                view.set("tenant", event.metadata.tenant);
                view.set("title", event.payload.title);
              },
            },
          },
        },
      },
    },
    gateways: {
      legacy: legacyGateway({
        directory,
        port,
        metadata: (req: any) => req.meta,
        socket: { coalesce: 500, commandWaitTimeout: 200 },
      }),
    },
  });
  await application.start();
  cleanups.push(() => application.close());
  const open = async () => {
    const socket = new WebSocket(`ws://127.0.0.1:${port}/socket`);
    cleanups.push(() => socket.close());
    const inbox: { raw: string; message: any }[] = [];
    socket.on("message", (raw) =>
      inbox.push({ raw: String(raw), message: JSON.parse(String(raw)) }),
    );
    await new Promise((resolve) => socket.on("open", resolve));
    const frames = (correlationId: string) =>
      inbox.filter((frame) => frame.message.correlationId === correlationId);
    return {
      send: (message: object) => socket.send(JSON.stringify(message)),
      frames,
      payloads: (correlationId: string) =>
        frames(correlationId)
          .slice(1)
          .map((frame) => frame.message.payload),
    };
  };
  const [first, second] = await Promise.all([open(), open()]);
  const refresh = {
    type: "subscribeEvents",
    payload: { filter: { type: "readmodel", readmodel: { type: "detail" } } },
  };
  first.send({ ...refresh, correlationId: "refresh" });
  second.send({ ...refresh, correlationId: "other" });
  await expect.poll(() => first.frames("refresh").length).toBe(1);
  await expect.poll(() => second.frames("other").length).toBe(1);

  // Two Command waits on one socket share its feed, and each sees only its own Command's change.
  const command = (id: string, name = "create") => ({
    type: "sendCommand",
    correlationId: `send-${id}`,
    payload: {
      context: "sales",
      aggregate: { name: "order", id },
      name,
      payload: { title: id },
    },
  });
  for (const id of ["o1", "o2"]) {
    first.send({
      type: "subscribeEvents",
      correlationId: `wait-${id}`,
      payload: { filter: { metadata: {} } },
    });
    first.send(command(id));
  }
  await expect.poll(() => first.frames("wait-o2").length).toBe(4);
  for (const id of ["o1", "o2"]) {
    const waited = first.payloads(`wait-${id}`);
    expect(waited.map((m) => m.fullname)).toEqual([
      "domain.sales.order.commandReceived",
      "domain.sales.order.created",
      "readmodel.detail.order.create",
    ]);
    expect(waited[2].readmodel.id).toBe(id);
  }
  first.send(command("o3"));

  // A wait whose Command changes no document receives `commandRejected` after `commandWaitTimeout`.
  first.send({
    type: "subscribeEvents",
    correlationId: "wait-touch",
    payload: { filter: { metadata: {} } },
  });
  first.send(command("o1", "touch"));
  await expect.poll(() => first.frames("wait-touch").length).toBe(3);
  expect(first.payloads("wait-touch")[1]).toMatchObject({
    fullname: "domain.sales.order.commandRejected",
    payload: { name: "CommandTimeoutError", statusCode: 504 },
  });

  // Auto-refresh: the first change at once, then only the last one of the window.
  await expect.poll(() => first.frames("refresh").length).toBe(3);
  const refreshed = first.payloads("refresh");
  expect(refreshed.map((m) => m.readmodel.id)).toEqual(["o1", "o3"]);
  await new Promise((resolve) => setTimeout(resolve, 600));
  expect(first.frames("refresh")).toHaveLength(3);
  // Both sockets received the same notification, and frames are what `JSON.stringify` makes of them.
  const [shared] = second.frames("other").slice(1);
  expect(second.payloads("other")[0].id).toBe(refreshed[0].id);
  expect(shared!.raw).toBe(JSON.stringify(shared!.message));
});

test("a rejected Command's error answers with all its fields only for the Application's own error classes", () => {
  class ConflictError extends Error {}
  const rejection = (thrown: Record<string, unknown>) =>
    Object.assign(new Rejection("check", "failed"), { thrown });
  const own = legacyError(
    rejection({ name: "ConflictError", statusCode: 409, data: { field: "a" } }),
    { ConflictError },
  );
  expect(own).toBeInstanceOf(ConflictError);
  expect(own).toMatchObject({ statusCode: 409, data: { field: "a" } });
  // Another library's error can carry connection details; only its name, message and codes answer.
  const foreign = legacyError(
    rejection({
      name: "HttpClientError",
      code: "ECONNRESET",
      config: { headers: { authorization: "x" } },
    }),
    { ConflictError },
  );
  expect({ ...foreign, message: foreign.message }).toEqual({
    name: "HttpClientError",
    code: "ECONNRESET",
    message: "failed",
  });
  // A schema rejection's issues reach the old ValidationError with the old validator's property paths.
  const issues = [
    {
      path: "reason",
      keyword: "required",
      params: { missingProperty: "reason" },
    },
    { path: "items.0.name", keyword: "type", params: { type: "string" } },
  ];
  const formAjvError = (errors: any[]) =>
    Object.assign(new Error("validation failed"), { errors });
  expect(
    legacyError(new Rejection("validation", "invalid", issues), {
      ValidationError: { formAjvError },
    }),
  ).toMatchObject({
    errors: [
      { dataPath: ".reason", keyword: "required" },
      { dataPath: ".items[0].name", keyword: "type" },
    ],
  });
});
test("legacyGateway gives its Gateway the Command lane", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "legacy-lane-"));
  try {
    const definition = legacyGateway({
      directory,
      port: 0,
      metadata: () => ({}),
      lane: "bulk",
    });
    expect(definition.lane).toBe("bulk");
  } finally {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
