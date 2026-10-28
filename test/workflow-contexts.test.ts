import { expect, test, vi } from "vitest";
import pino from "pino";
import { Type } from "typebox";
import { aggregate, defineDomain, message, worker } from "../src/authoring/aggregate.js";
import { createApplication } from "../src/runtime/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import { createScopedHandle } from "../src/gateway/access.js";
import { ForbiddenError } from "../src/errors.js";
import { signal, workflow } from "../src/workflows/index.js";
import type { PluginContext, Reactions, ReactionApp, ReactionView } from "../src/runtime/types.js";
import type { EventsOf } from "../src/authoring/aggregate.js";

// Sagas, Workers, Plugins and Gateways start, signal and query Workflows.
const order = aggregate("shop", "order", {
  state: Type.Object({}),
  initialState: {},
  events: { placed: { schema: Type.Object({}), handler() {} } },
  commands: { place: { schema: Type.Object({}), steps: [(_, order) => order.apply.placed({})] } },
});
const domain = defineDomain(order);
const ping = message("ping", Type.Object({ order: Type.String() }));
const fulfilment = workflow("fulfilment", {
  version: 1,
  signals: { packed: signal<{ by: string }>(), shipped: signal() },
  async run(ctx, input: { order: string }) {
    const { by } = await ctx.waitFor(ctx.signals.packed);
    await ctx.queue(ping, { order: input.order });
    await ctx.waitFor(ctx.signals.shipped);
    return `${input.order} packed by ${by}`;
  },
});
// A Saga starts the instance with its first Signal, both committed with its state.
const reactions: Reactions<EventsOf<typeof domain>, ReactionView, ReactionApp> = {
  [domain.shop.order.placed]: (event, _saga, app) =>
    app.startWorkflow(fulfilment, event.aggregate.id, { order: event.aggregate.id }, {
      signal: fulfilment.signals.packed({ by: "saga" }),
    }),
};
// The Worker the Workflow queues signals it back.
const pinger = worker(ping, {
  execute: ({ payload }, ctx) => ctx.sendSignal(payload.order, fulfilment.signals.shipped()),
});

const application = async () => {
  let plugin: PluginContext | undefined;
  const app = createApplication({
    name: "contexts",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [order],
    sagas: { orders: { reactions } },
    workflows: { fulfilment },
    workers: { ping: pinger },
    plugins: [(ctx) => void (plugin = ctx)],
    pollInterval: 20,
    logger: pino({ level: "silent" }),
  });
  await app.start();
  return { app, plugin: plugin! };
};
const status = (app: { queryWorkflow: PluginContext["queryWorkflow"] }, id: string, expected: string) =>
  vi.waitFor(async () => expect(await app.queryWorkflow(fulfilment, id)).toMatchObject({ status: expected }), {
    timeout: 5000,
    interval: 20,
  });

test("a Saga starts a Workflow with a Signal, a Worker signals it, a Plugin queries it", async () => {
  const { app, plugin } = await application();
  try {
    await app.sendCommand(domain.shop.order.place("o1", {}));
    await status(plugin, "o1", "completed");
    expect(await plugin.queryWorkflow(fulfilment, "o1")).toEqual({
      status: "completed",
      result: "o1 packed by saga",
    });
    await plugin.startWorkflow(fulfilment, "o2", { order: "o2" });
    await plugin.sendSignal("o2", fulfilment.signals.packed({ by: "plugin" }));
    await status(plugin, "o2", "completed");
    expect((await plugin.queryWorkflow(fulfilment, "o2"))!.result).toBe("o2 packed by plugin");
    expect(await plugin.queryWorkflow(fulfilment, "missing")).toBeNull();
  } finally {
    await app.close();
  }
});

test("a Gateway reaches a Workflow only through its Access-map entries", async () => {
  const { app } = await application();
  try {
    const denied = createScopedHandle({}, { port: 0, access: {} }, app);
    await expect(denied.startWorkflow(fulfilment, "g1", { order: "g1" })).rejects.toBeInstanceOf(ForbiddenError);
    await expect(denied.queryWorkflow(fulfilment, "g1")).rejects.toBeInstanceOf(ForbiddenError);
    const gateway = createScopedHandle(
      { user: "ada" },
      {
        port: 0,
        access: {
          workflows: {
            "fulfilment.start": true,
            "fulfilment.query": true,
            "fulfilment.signals.packed": (ctx, request) => request.id === "g1" && ctx.user === "ada",
          },
        },
      },
      app,
    );
    // A start with a Signal needs the Signal's entry too.
    await expect(
      gateway.startWorkflow(fulfilment, "g2", { order: "g2" }, { signal: fulfilment.signals.packed({ by: "x" }) }),
    ).rejects.toBeInstanceOf(ForbiddenError);
    await gateway.startWorkflow(fulfilment, "g1", { order: "g1" }, { signal: fulfilment.signals.packed({ by: "ada" }) });
    await expect(gateway.sendSignal("g1", fulfilment.signals.shipped())).rejects.toBeInstanceOf(
      ForbiddenError,
    );
    await status(gateway, "g1", "completed");
    expect((await gateway.queryWorkflow(fulfilment, "g1"))!.result).toBe("g1 packed by ada");
  } finally {
    await app.close();
  }
});

test("a Workflow registered under another name, or a Signal it does not declare, is refused", async () => {
  expect(() =>
    createApplication({
      name: "misnamed",
      store: createInMemoryEventStore(),
      transport: createInMemoryTransport(),
      workflows: { shipping: fulfilment },
    }),
  ).toThrow("Workflow fulfilment is registered as shipping");
  const { app } = await application();
  try {
    await expect(app.sendSignal("o1", { workflow: "fulfilment", name: "lost", payload: null })).rejects.toThrow(
      "Workflow fulfilment has no Signal lost",
    );
    // A start's Signal belongs to the Workflow it starts.
    await expect(
      app.startWorkflow("fulfilment", "o9", { order: "o9" }, { signal: { workflow: "shipping", name: "shipped", payload: null } }),
    ).rejects.toThrow("Workflow fulfilment cannot take a Signal of Workflow shipping");
  } finally {
    await app.close();
  }
});

test("a Saga that starts a Workflow the Application does not register fails its Reaction", async () => {
  const lines: string[] = [];
  const app = createApplication({
    name: "ghosts",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [order],
    sagas: {
      orders: {
        reactions: {
          [domain.shop.order.placed]: (event, _saga, app) =>
            app.startWorkflow("ghost" as never, event.aggregate.id, {} as never),
        } as Reactions<EventsOf<typeof domain>, ReactionView, ReactionApp>,
      },
    },
    pollInterval: 20,
    logger: pino({ level: "error" }, { write: (line: string) => void lines.push(line) }),
  });
  await app.start();
  try {
    await app.sendCommand(domain.shop.order.place("o1", {}));
    await vi.waitFor(() => expect(lines.join("")).toContain("Unknown Workflow ghost"), { timeout: 5000 });
  } finally {
    await app.close();
  }
});
