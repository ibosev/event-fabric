import { createServer } from "node:net";
import { afterEach, expect, test } from "vitest";
import {
  context,
  propagation,
  trace,
  type Context,
  type Span,
  type TextMapPropagator,
} from "@opentelemetry/api";
import { createApplication } from "../src/runtime/index.js";
import { compile, only } from "../src/authoring/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import { partitionFor } from "../src/transport/partition.js";
import { createScopedHandle } from "../src/gateway/access.js";
import { ForbiddenError } from "../src/errors.js";
import type { Logger, RecordedEvent } from "../src/types.js";

const cleanups: (() => Promise<unknown>)[] = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});
const order = compile("sales", "order", {
  initialState: {},
  commands: {
    create: [
      only.ifNotExists(),
      (command: any, state: any) => state.apply.created(command.payload ?? {}),
    ],
    touch: [(_: any, state: any) => state.apply.touched({})],
    fail: [
      () => {
        throw new Error("handler exploded");
      },
    ],
  },
  events: {
    created: (event: any, state: any) =>
      state.set("title", event.payload.title),
    touched: () => {},
  },
});
const event = (
  id: string,
  timestamp: string,
  correlationId = id,
): Parameters<
  ReturnType<typeof createInMemoryEventStore>["appendToStream"]
>[2][number] => ({
  id,
  name: "touched",
  payload: {},
  metadata: {},
  schemaVersion: 1,
  timestamp,
  commandId: id,
  correlationId,
});
const capture = () => {
  const entries: object[] = [];
  const logger = (bindings: object = {}): Logger => ({
    debug: (value, message) => entries.push({ ...bindings, ...value, message }),
    info: (value, message) => entries.push({ ...bindings, ...value, message }),
    warn: (value, message) => entries.push({ ...bindings, ...value, message }),
    error: (value, message) => entries.push({ ...bindings, ...value, message }),
    child: (child) => logger({ ...bindings, ...child }),
  });
  return { entries, logger: logger() };
};
const freePort = () =>
  new Promise<number>((resolve) => {
    const server = createServer().listen(0, () => {
      const { port } = server.address() as { port: number };
      server.close(() => resolve(port));
    });
  });

test("plugins start in order, receive the route table and shut down in reverse even when a hook throws or a request hangs", async () => {
  const calls: string[] = [];
  let routes: unknown;
  const port = await freePort();
  const store = createInMemoryEventStore();
  const application = createApplication({
    name: "plugins",
    store,
    transport: createInMemoryTransport(),
    aggregates: [order],
    pollInterval: 5,
    shutdownTimeout: 400,
    gateways: {
      http: {
        port,
        routes: [
          {
            method: "get",
            path: "/hang",
            handler: () => new Promise(() => {}),
          },
        ],
      },
    },
    plugins: [
      (ctx) => {
        expect(Object.keys(ctx).sort()).toEqual([
          "awaitCorrelated",
          "config",
          "logger",
          "model",
          "notify",
          "on",
          "queryWorkflow",
          "readmodels",
          "sendCommand",
          "sendSagaMessage",
          "sendSignal",
          "services",
          "startWorkflow",
          "subscribeEvents",
          "subscribeNotifications",
        ]);
        expect(Object.isFrozen(ctx.model)).toBe(true);
        ctx.on("start", () => calls.push("start:first"));
        ctx.on("gateway", (_: unknown, gateway: any) => {
          routes = gateway.routes;
        });
        ctx.on("shutdown", () => calls.push("shutdown:first"));
      },
      (ctx) => {
        ctx.on("start", () => calls.push("start:second"));
        ctx.on("shutdown", () => {
          calls.push("shutdown:second");
          throw new Error("hook failed");
        });
      },
    ],
  });
  await application.start();
  expect(calls).toEqual(["start:first", "start:second"]);
  expect(routes).toEqual([{ method: "get", path: "/hang" }]);
  const hanging = fetch(`http://127.0.0.1:${port}/hang`).catch(() => null);
  await new Promise((resolve) => setTimeout(resolve, 50));
  const started = Date.now();
  await expect(application.stop()).rejects.toThrow();
  expect(Date.now() - started).toBeLessThan(1000);
  expect(calls).toEqual([
    "start:first",
    "start:second",
    "shutdown:second",
    "shutdown:first",
  ]);
  expect(
    (await store.consumers()).filter(
      (status) =>
        status.lease && Date.parse(status.lease.expiresAt) > Date.now(),
    ),
  ).toEqual([]);
  await hanging;
});

test("a hanging shutdown hook reports a timeout, runs the remaining hooks and releases Leases", async () => {
  const calls: string[] = [];
  const store = createInMemoryEventStore();
  const application = createApplication({
    name: "hanging-hook",
    store,
    transport: createInMemoryTransport(),
    shutdownTimeout: 150,
    pollInterval: 5,
    plugins: [
      (ctx) => {
        ctx.on("shutdown", () => calls.push("first"));
        ctx.on("shutdown", () => {
          calls.push("second");
          return new Promise(() => {});
        });
      },
    ],
  });
  await application.start();
  await expect
    .poll(async () => (await store.consumers()).length)
    .toBeGreaterThan(0);
  await expect(application.stop()).rejects.toThrow(/shutdown.*timed out/i);
  expect(calls).toEqual(["second", "first"]);
  expect(
    (await store.consumers()).some(
      (s) => s.lease && Date.parse(s.lease.expiresAt) > Date.now(),
    ),
  ).toBe(false);
});

test("close releases every owned resource even when a shutdown hook fails", async () => {
  const closed: string[] = [];
  const application = createApplication({
    name: "close-failure",
    store: {
      ...createInMemoryEventStore(),
      close: async () => {
        closed.push("events");
      },
    },
    transport: {
      ...createInMemoryTransport(),
      close: async () => {
        closed.push("transport");
      },
    },
    readmodelStore: {
      ...createMemoryReadmodelStore(),
      close: async () => {
        closed.push("readmodels");
      },
    },
    readmodels: { detail: { models: {} } },
    plugins: [
      (ctx) =>
        ctx.on("shutdown", () => {
          throw new Error("hook failed");
        }),
    ],
  });
  await application.start({ only: ["commands:close-failure"] });
  await expect(application.close()).rejects.toThrow("hook failed");
  expect(closed.sort()).toEqual(["events", "readmodels", "transport"]);
});

test("awaitCorrelated unsubscribes when the initial Event-log read fails", async () => {
  const transport = createInMemoryTransport();
  const application = createApplication({
    name: "failed-wait",
    store: {
      ...createInMemoryEventStore(),
      head: async () => {
        throw new Error("database unavailable");
      },
    },
    transport,
  });
  let inspected = 0;
  const stream = { context: "sales", aggregate: "order", id: "one" };
  await expect(
    application.awaitCorrelated(
      { id: "waiting", stream, name: "touch" },
      "sales.order.touched",
      {
        predicate: () => {
          inspected++;
          return true;
        },
      },
    ),
  ).rejects.toThrow("database unavailable");
  await transport.publish("events", {
    ...event("later", "2026-01-01T00:00:00Z", "waiting"),
    stream,
    revision: 1,
    position: { transactionId: "1", sequence: "1" },
  });
  expect(inspected).toBe(0);
  await transport.close();
});

test("running Consumer groups renew their Leases in batches, without per-tick writes", async () => {
  const persistence = createInMemoryEventStore();
  const batches: string[][] = [];
  const application = createApplication({
    name: "batched-leases",
    store: {
      ...persistence,
      renewLeases: async (...args) => {
        batches.push(args[0].map((lease) => lease.group));
        return persistence.renewLeases(...args);
      },
    },
    transport: createInMemoryTransport(),
    pollInterval: 5,
    leaseDuration: 150,
  });
  await application.start();
  try {
    await expect.poll(() => batches.length).toBeGreaterThan(0);
    expect(
      batches.some(
        (groups) => groups.includes("outbox") && groups.includes("relay"),
      ),
    ).toBe(true);
    expect((await application.health()).readiness).toBe(true);
  } finally {
    await application.stop();
  }
});

test("logs carry chain ids but never Command metadata", async () => {
  const { entries, logger } = capture();
  const failing = compile("sales", "order", {
    initialState: {},
    commands: { touch: [(_: any, state: any) => state.apply.touched({})] },
    events: { touched: () => {} },
    // An enricher failure happens after the decision, so it is a failed outcome.
    eventEnricher: () => {
      throw new Error("enricher exploded");
    },
  });
  const application = createApplication({
    name: "logs",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [failing],
    logger,
    pollInterval: 5,
  });
  await application.start();
  cleanups.push(() => application.stop());
  const outcome = await application.sendCommand({
    stream: { context: "sales", aggregate: "order", id: "one" },
    name: "touch",
    metadata: { secret: "S3CR3T" },
  });
  expect(outcome.status).toBe("failed");
  expect(entries.some((entry: any) => entry.correlationId)).toBe(true);
  expect(JSON.stringify(entries)).not.toContain("S3CR3T");
});

test("spans cover Command handling and Saga Reactions link to the Command's trace", async () => {
  const spans: { name: string; links: string[] }[] = [];
  const traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
  const propagator: TextMapPropagator = {
    inject: () => {},
    extract: (ctx: Context, carrier: any) => {
      const [, trace_, span] = String(carrier?.traceparent ?? "").split("-");
      return trace_
        ? trace.setSpanContext(ctx, {
            traceId: trace_,
            spanId: span!,
            traceFlags: 1,
          })
        : ctx;
    },
    fields: () => ["traceparent"],
  };
  const span = {
    end: () => {},
    setStatus: () => span,
    spanContext: () => ({ traceId, spanId: "00f067aa0ba902b7", traceFlags: 1 }),
  } as unknown as Span;
  trace.setGlobalTracerProvider({
    getTracer: () =>
      ({
        startActiveSpan: (name: string, options: any, _: any, fn: any) => {
          spans.push({
            name,
            links: (options.links ?? []).map(
              (link: any) => link.context.traceId,
            ),
          });
          return fn(span);
        },
      }) as any,
  });
  propagation.setGlobalPropagator(propagator);
  cleanups.push(async () => {
    trace.disable();
    propagation.disable();
    context.disable();
  });
  const application = createApplication({
    name: "traces",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [order],
    pollInterval: 5,
    sagas: {
      follow: {
        reactions: {
          "sales.order.created": (_: any, saga: any) => saga.set("seen", true),
        },
      },
    },
  });
  await application.start();
  cleanups.push(() => application.stop());
  await application.sendCommand({
    stream: { context: "sales", aggregate: "order", id: "one" },
    name: "create",
    traceparent: `00-${traceId}-00f067aa0ba902b7-01`,
  });
  await expect
    .poll(() => spans.find((entry) => entry.name === "eventfabric.reaction"))
    .toEqual({ name: "eventfabric.reaction", links: [traceId] });
  expect(spans.map((entry) => entry.name)).toEqual(
    expect.arrayContaining(["eventfabric.send", "eventfabric.command.handle"]),
  );
});

test("the relay publishes each visible Event once and subscribers drop republished Positions", async () => {
  const store = createInMemoryEventStore(),
    transport = createInMemoryTransport();
  const application = createApplication({
    name: "relay",
    store,
    transport,
    aggregates: [order],
    pollInterval: 5,
  });
  const seen: string[] = [];
  const stop = await application.subscribeEvents((event) => {
    seen.push(event.id);
  });
  cleanups.push(stop);
  await application.start();
  cleanups.push(() => application.stop());
  const stream = { context: "sales", aggregate: "order", id: "one" };
  const now = new Date().toISOString();
  const [first] = await store.appendToStream(stream, 0, [event("e1", now)]);
  await store.appendToStream(stream, 1, [event("e2", now)]);
  await expect.poll(() => seen).toEqual(["e1", "e2"]);
  await transport.publish("events", first as RecordedEvent);
  await new Promise((resolve) => setTimeout(resolve, 30));
  expect(seen).toEqual(["e1", "e2"]);
  expect(await store.checkpoint("relay")).toEqual((await store.head())!);
});

test("a relay further behind than relayMaxLag jumps to the head without publishing the backlog", async () => {
  const store = createInMemoryEventStore(),
    transport = createInMemoryTransport();
  const stream = { context: "sales", aggregate: "order", id: "one" };
  await store.appendToStream(stream, 0, [
    event("old", "2020-01-01T00:00:00.000Z"),
    event("new", "2020-01-01T00:00:05.000Z"),
  ]);
  const application = createApplication({
    name: "stale",
    store,
    transport,
    aggregates: [order],
    pollInterval: 5,
    relayMaxLag: 1000,
    only: ["relay:stale"],
  });
  const seen: string[] = [];
  cleanups.push(await application.subscribeEvents((e) => void seen.push(e.id)));
  await application.start();
  cleanups.push(() => application.stop());
  await expect
    .poll(() => store.checkpoint("relay"))
    .toEqual(await store.head());
  await store.appendToStream(stream, 2, [
    event("live", "2020-01-01T00:00:05.500Z"),
  ]);
  await expect.poll(() => seen).toEqual(["live"]);
});

test("a slow Outbox transaction does not hold back another transaction's delivery", async () => {
  // Distinct Partitions, so the Command lane itself never serializes the two.
  const ids = ["slow", ...Array.from({ length: 50 }, (_, i) => `fast${i}`)];
  const slow = ids[0]!;
  const fast = ids.find(
    (id) =>
      id !== slow &&
      partitionFor({ context: "sales", aggregate: "order", id }) !==
        partitionFor({ context: "sales", aggregate: "order", id: slow }),
  )!;
  let release!: () => void;
  const gate = new Promise<void>((resolve) => (release = resolve));
  const handled: string[] = [];
  const target = compile("sales", "order", {
    initialState: {},
    commands: {
      create: [(_: any, state: any) => state.apply.created({})],
      follow: [
        async (_: any, state: any) => {
          if (state.id === slow) await gate;
          handled.push(state.id);
          state.apply.followed({});
        },
      ],
    },
    events: { created: () => {}, followed: () => {} },
  });
  const application = createApplication({
    name: "outbox",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [target],
    pollInterval: 5,
    sagas: {
      chain: {
        reactions: {
          "sales.order.created": (event: any, _: any, app: any) =>
            app.getDomain().sales.order(event.aggregate.id).follow({}),
        },
      },
    },
  });
  await application.start();
  cleanups.push(async () => {
    release();
    await application.stop();
  });
  for (const id of [slow, fast])
    await application.sendCommand({
      stream: { context: "sales", aggregate: "order", id },
      name: "create",
    });
  await expect.poll(() => handled).toEqual([fast]);
  release();
  await expect.poll(() => handled).toEqual([fast, slow]);
});

test("one committed Readmodel batch publishes exactly one Notification for all its models", async () => {
  const transport = createInMemoryTransport();
  const published: any[] = [];
  cleanups.push(
    await transport.subscribe("readmodels", (message) => {
      published.push(message);
    }),
  );
  const reaction = {
    "sales.order.created": (_: any, vm: any) => vm.set("seen", true),
  };
  const schema = {
    type: "object",
    properties: { seen: { type: "boolean" } },
  } as const;
  const application = createApplication({
    name: "batch",
    store: createInMemoryEventStore(),
    transport,
    aggregates: [order],
    pollInterval: 5,
    readmodelStore: createMemoryReadmodelStore(),
    readmodels: {
      detail: {
        models: {
          first: { schema, reactions: reaction },
          second: { schema, reactions: reaction },
        },
      },
    },
  });
  await application.start();
  cleanups.push(() => application.stop());
  await application.sendCommand({
    stream: { context: "sales", aggregate: "order", id: "one" },
    name: "create",
  });
  await expect.poll(() => published.length).toBe(1);
  await new Promise((resolve) => setTimeout(resolve, 30));
  expect(published).toHaveLength(1);
  expect(
    published[0].changes.map((change: any) => change.model).sort(),
  ).toEqual(["first", "second"]);
});

test("Readmodel batches hold at most 100 Events and close after 50 ms", async () => {
  const store = createInMemoryEventStore();
  const stream = { context: "sales", aggregate: "order", id: "one" };
  const now = new Date().toISOString();
  await store.appendToStream(
    stream,
    0,
    Array.from({ length: 150 }, (_, i) => event(`e${i}`, now)),
  );
  const readmodelStore = createMemoryReadmodelStore();
  const sizes: number[] = [];
  let previous = -1;
  const batch = readmodelStore.batch.bind(readmodelStore);
  readmodelStore.batch = async (group, epoch, position, changes, options) => {
    const index = Number(
      (
        await store.readAll({ toPosition: position, reverse: true, limit: 1 })
      )[0]!.id.slice(1),
    );
    sizes.push(index - previous);
    previous = index;
    return batch(group, epoch, position, changes, options);
  };
  let slow = false;
  const application = createApplication({
    name: "sizes",
    store,
    transport: createInMemoryTransport(),
    aggregates: [order],
    pollInterval: 5,
    readmodelStore,
    readmodels: {
      detail: {
        models: {
          order: {
            schema: { type: "object", properties: {} },
            reactions: {
              "sales.order.touched": async () => {
                if (slow)
                  await new Promise((resolve) => setTimeout(resolve, 30));
              },
            },
          },
        },
      },
    },
  });
  await application.start();
  cleanups.push(() => application.stop());
  await expect.poll(() => previous).toBe(149);
  expect(sizes).toEqual([100, 50]);
  slow = true;
  await store.appendToStream(
    stream,
    150,
    Array.from({ length: 5 }, (_, i) => event(`e${150 + i}`, now)),
  );
  await expect.poll(() => previous, { timeout: 2000 }).toBe(154);
  expect(Math.max(...sizes.slice(2))).toBeLessThanOrEqual(2);
});

test("Workers honour concurrency and Commands they send inherit Message metadata and chain ids", async () => {
  let active = 0,
    peak = 0;
  const store = createInMemoryEventStore();
  const application = createApplication({
    name: "workers",
    store,
    transport: createInMemoryTransport(),
    aggregates: [order],
    pollInterval: 5,
    sagas: {
      fan: {
        reactions: {
          "sales.order.created": (_: any, __: any, app: any) => {
            for (const suffix of ["a", "b", "c", "d"])
              app.queue("job", { suffix }, { suffix });
          },
        },
      },
    },
    workers: {
      job: {
        handles: ["job"],
        concurrency: 2,
        execute: async (message, ctx) => {
          active++;
          peak = Math.max(peak, active);
          await new Promise((resolve) => setTimeout(resolve, 20));
          active--;
          if (message.payload.suffix === "a")
            await ctx.send({
              stream: { context: "sales", aggregate: "order", id: "child" },
              name: "touch",
              metadata: { extra: true },
            });
        },
      },
    },
  });
  await application.start();
  cleanups.push(() => application.stop());
  const outcome = await application.sendCommand({
    stream: { context: "sales", aggregate: "order", id: "parent" },
    name: "create",
    metadata: { tenant: "a" },
  });
  if (outcome.status !== "accepted") throw new Error("Expected acceptance");
  const parent = outcome.events[0]!;
  await expect
    .poll(
      async () =>
        (
          await store.readStream({
            context: "sales",
            aggregate: "order",
            id: "child",
          })
        ).length,
    )
    .toBe(1);
  const [child] = await store.readStream({
    context: "sales",
    aggregate: "order",
    id: "child",
  });
  expect(child!.correlationId).toBe(parent.correlationId);
  expect(child!.metadata).toMatchObject({ tenant: "a", extra: true });
  await expect.poll(() => active).toBe(0);
  expect(peak).toBe(2);
});

test("a failing Consumer group stays running for four failures and reports halted on the fifth", async () => {
  const store = createInMemoryEventStore();
  const application = createApplication({
    name: "halting",
    store,
    transport: createInMemoryTransport(),
    aggregates: [order],
    pollInterval: 5,
    sagas: {
      broken: {
        reactions: {
          "sales.order.created": () => {
            throw new Error("poison");
          },
        },
      },
    },
  });
  const states = new Map<number, string>();
  await application.start();
  cleanups.push(() => application.stop());
  await application.sendCommand({
    stream: { context: "sales", aggregate: "order", id: "one" },
    name: "create",
  });
  await expect
    .poll(
      async () => {
        const status = (await store.consumers()).find(
          (entry) => entry.group === "saga:broken",
        );
        if (status?.attempts) states.set(status.attempts, status.state);
        return status?.attempts ?? 0;
      },
      { timeout: 5000, interval: 10 },
    )
    .toBeGreaterThanOrEqual(5);
  for (const attempts of [1, 2, 3, 4])
    if (states.has(attempts)) expect(states.get(attempts)).toBe("running");
  expect(states.get(5)).toBe("halted");
});

test("startup rejects unknown Events and unknown declared Readmodel groups; undeclared reads fail the Reaction", async () => {
  const base = {
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [order],
  };
  await expect(
    createApplication({
      ...base,
      name: "unknown-event",
      sagas: { s: { reactions: { "sales.order.missing": () => {} } } },
    }).initialize(),
  ).rejects.toThrow("unknown Event sales.order.missing");
  await expect(
    createApplication({
      ...base,
      name: "unknown-group",
      sagas: {
        s: {
          reads: ["nowhere"],
          reactions: { "sales.order.created": () => {} },
        },
      },
    }).initialize(),
  ).rejects.toThrow("Unknown declared Readmodel group nowhere");
  const store = createInMemoryEventStore();
  const application = createApplication({
    name: "undeclared",
    store,
    transport: createInMemoryTransport(),
    aggregates: [order],
    pollInterval: 5,
    readmodelStore: createMemoryReadmodelStore(),
    readmodels: {
      other: {
        models: {
          doc: { schema: { type: "object", properties: {} }, reactions: {} },
        },
      },
    },
    sagas: {
      reader: {
        reactions: {
          "sales.order.created": (_: any, __: any, app: any) =>
            app.getReadmodels().other.doc("x"),
        },
      },
    },
  });
  await application.start();
  cleanups.push(() => application.stop());
  await application.sendCommand({
    stream: { context: "sales", aggregate: "order", id: "one" },
    name: "create",
  });
  await expect
    .poll(
      async () =>
        (await store.consumers()).find((entry) => entry.group === "saga:reader")
          ?.error,
    )
    .toContain("Undeclared cross-group read other");
  await expect(application.rebuild("reader")).rejects.toThrow(
    "Sagas cannot be rebuilt",
  );
});

test("Gateway handles expose awaitCorrelated behind the domain Access map and Command metadata", async () => {
  const store = createInMemoryEventStore();
  const application = createApplication({
    name: "gateway-await",
    store,
    transport: createInMemoryTransport(),
    aggregates: [order],
    pollInterval: 5,
  });
  await application.start();
  cleanups.push(() => application.stop());
  const gateway = {
    port: 0,
    access: {
      domain: { "sales.order.create": (ctx: any) => ctx.user === "admin" },
    },
    commandMetadata: (ctx: any) => ({ user: ctx.user }),
  };
  const input = {
    stream: { context: "sales", aggregate: "order", id: "one" },
    name: "create",
  };
  await expect(
    createScopedHandle({ user: "guest" }, gateway, application).awaitCorrelated(
      input,
      "sales.order.created",
    ),
  ).rejects.toBeInstanceOf(ForbiddenError);
  const result = await createScopedHandle(
    { user: "admin" },
    gateway,
    application,
  ).awaitCorrelated(input, "sales.order.created");
  expect(result.status).toBe("accepted");
  expect((result as any).event.metadata).toMatchObject({ user: "admin" });
});

test("an Access-map filter with an undefined context value denies instead of matching unowned documents", async () => {
  const readmodelStore = createMemoryReadmodelStore();
  const application = createApplication({
    name: "tenants",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [order],
    readmodelStore,
    readmodels: {
      detail: {
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
  });
  await application.initialize();
  await readmodelStore.fence("detail", 1);
  await readmodelStore.batch(
    "detail",
    1,
    { transactionId: "1", sequence: "1" },
    [{ model: "order", id: "unowned", document: {} }],
  );
  const scoped = createScopedHandle(
    {} as { tenant?: string },
    {
      port: 0,
      access: {
        readmodels: { "detail.order": (ctx) => ({ tenant: ctx.tenant }) },
      },
    },
    application,
  );
  await expect(scoped.readmodels.detail.order.find()).rejects.toBeInstanceOf(
    ForbiddenError,
  );
});

test("a Readmodel field named id survives later Reactions", async () => {
  const application = createApplication({
    name: "ids",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [order],
    pollInterval: 5,
    readmodelStore: createMemoryReadmodelStore(),
    readmodels: {
      detail: {
        models: {
          order: {
            schema: {
              type: "object",
              properties: {
                id: { type: "string" },
                touched: { type: "boolean" },
              },
            },
            reactions: {
              "sales.order.created": (_: any, vm: any) => vm.set("id", "EXT-1"),
              "sales.order.touched": (_: any, vm: any) =>
                vm.set("touched", true),
            },
          },
        },
      },
    },
  });
  await application.start();
  cleanups.push(() => application.stop());
  const stream = { context: "sales", aggregate: "order", id: "one" };
  await application.sendCommand({ stream, name: "create" });
  const outcome = await application.sendCommand({ stream, name: "touch" });
  if (outcome.status !== "accepted") throw new Error("Expected acceptance");
  const { document } = await application.readmodels.detail.order("one", {
    minPosition: outcome.events[0]!.position,
  });
  expect(document).toMatchObject({ _id: "one", id: "EXT-1", touched: true });
});

test("an Application registers its Aggregates' Upcasters with the Event store", async () => {
  const store = createInMemoryEventStore();
  const stream = { context: "bank", aggregate: "account", id: "one" };
  await store.appendToStream(stream, 0, [
    {
      ...event("old", new Date().toISOString()),
      name: "deposited",
      payload: { cents: 500 },
    },
  ]);
  const account = compile("bank", "account", {
    initialState: { balance: 0 },
    commands: {
      check: [
        (_: any, state: any) => {
          if (state.get("balance") !== 5) throw new Error("not upcast");
          state.apply.checked({});
        },
      ],
    },
    events: {
      deposited: {
        schemaVersion: 2,
        upcasters: { 1: (payload: any) => ({ amount: payload.cents / 100 }) },
        handler: (event: any, state: any) =>
          state.set("balance", state.get("balance") + event.payload.amount),
      },
      checked: () => {},
    },
  });
  const application = createApplication({
    name: "upcasting",
    store,
    transport: createInMemoryTransport(),
    aggregates: [account],
    pollInterval: 5,
  });
  await application.start();
  cleanups.push(() => application.stop());
  const outcome = await application.sendCommand({ stream, name: "check" });
  expect(outcome.status).toBe("accepted");
});

test("commandTo builds a Command from a qualified name and rejects malformed names", async () => {
  const { commandTo } = await import("../src/id.js");
  expect(commandTo("sales.order.create", "one", { title: "x" })).toEqual({
    stream: { context: "sales", aggregate: "order", id: "one" },
    name: "create",
    payload: { title: "x" },
  });
  expect(() => commandTo("sales.create" as any, "one", {})).toThrow(
    "context.aggregate.command",
  );
});

test("a Worker's Message schema is checked before the Message is recorded", async () => {
  const store = createInMemoryEventStore();
  const executed: unknown[] = [];
  const base = {
    store,
    transport: createInMemoryTransport(),
    aggregates: [order],
    pollInterval: 5,
  };
  await expect(
    createApplication({
      ...base,
      name: "unhandled-schema",
      workers: {
        mail: {
          handles: ["mail"],
          schemas: { other: { type: "object" } } as any,
          execute: () => {},
        },
      },
    }).initialize(),
  ).rejects.toThrow("schema for unhandled other");
  const application = createApplication({
    ...base,
    name: "message-schemas",
    sagas: {
      mailer: {
        reactions: {
          "sales.order.created": (event: any, _: any, app: any) =>
            app.queue("mail", { to: event.payload.title ?? 42 }),
        },
      },
    },
    workers: {
      mail: {
        handles: ["mail"],
        schemas: {
          mail: {
            type: "object",
            properties: { to: { type: "string" } },
            required: ["to"],
          },
        },
        execute: (message) => void executed.push(message.payload),
      },
    },
  });
  await application.start();
  cleanups.push(() => application.stop());
  const send = (id: string, payload?: object) =>
    application.sendCommand({
      stream: { context: "sales", aggregate: "order", id },
      name: "create",
      payload,
    });
  await send("good", { title: "ana@example.test" });
  await expect.poll(() => executed).toEqual([{ to: "ana@example.test" }]);
  await send("bad");
  await expect
    .poll(
      async () =>
        (await store.consumers()).find((entry) => entry.group === "saga:mailer")
          ?.error,
    )
    .toContain("Queued Message mail payload is invalid");
  expect(executed).toHaveLength(1);
});
