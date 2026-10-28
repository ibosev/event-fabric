import { expect, test, vi } from "vitest";
import { compile } from "../src/authoring/index.js";
import { createApplication } from "../src/runtime/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import { command, readmodel } from "../src/gateway/index.js";
import { ReadmodelRebuildingError } from "../src/errors.js";
import type { Logger, PendingEvent, StreamId } from "../src/types.js";

const pending = (id: string, name: string, payload = {}): PendingEvent => ({
  id,
  name,
  payload,
  metadata: {},
  schemaVersion: 1,
  timestamp: "2026-10-17T00:00:00.000Z",
  commandId: id,
  correlationId: id,
});
const order: StreamId = { context: "sales", aggregate: "order", id: "one" };
const pulse: StreamId = { context: "audit", aggregate: "pulse", id: "one" };
const aggregates = [
  compile("sales", "order", {
    commands: {
      open: (command: any, order: any) => order.apply.opened(command.payload),
    },
    events: { opened: () => {} },
  }),
  compile("audit", "pulse", { commands: {}, events: { observed: () => {} } }),
];
const capture = () => {
  const logs: any[] = [];
  let port = 0;
  const logger: Logger = {
    debug() {},
    info(value: any) {
      if (value?.address?.port) port = value.address.port;
    },
    warn(value: any) {
      logs.push(value);
    },
    error(value: any, message?: string) {
      logs.push({ ...value, message });
    },
    child() {
      return this;
    },
  };
  return { logs, logger, port: () => port };
};
/** `extra` adds a schema field, which makes the `detail` group require a rebuild. */
const definition = (
  stores: {
    store: ReturnType<typeof createInMemoryEventStore>;
    models: ReturnType<typeof createMemoryReadmodelStore>;
  },
  options: {
    extra?: boolean;
    react?: () => Promise<void> | void;
    logger?: Logger;
    sagaRuns?: string[];
  } = {},
) =>
  createApplication({
    name: "startup-rebuilds",
    store: stores.store,
    transport: createInMemoryTransport(),
    aggregates,
    pollInterval: 5,
    ...(options.logger ? { logger: options.logger } : {}),
    readmodels: {
      detail: {
        store: stores.models,
        models: {
          order: {
            schema: {
              type: "object",
              properties: {
                total: { type: "number" },
                ...(options.extra ? { note: { type: "string" } } : {}),
              },
            },
            initialState: { total: 0 },
            reactions: {
              "sales.order.opened": async (event: any, vm: any) => {
                await options.react?.();
                vm.set("total", vm.get("total") + event.payload.amount);
              },
            },
          },
        },
      },
      other: {
        store: stores.models,
        models: {
          pulse: {
            schema: {
              type: "object",
              properties: { seen: { type: "number" } },
            },
            initialState: { seen: 0 },
            reactions: {
              "audit.pulse.observed": (_event: any, vm: any) =>
                vm.set("seen", vm.get("seen") + 1),
            },
          },
        },
      },
      dependent: {
        store: stores.models,
        reads: ["detail"],
        models: {
          pulse: {
            schema: {
              type: "object",
              properties: { seen: { type: "number" } },
            },
            initialState: { seen: 0 },
            reactions: {
              "audit.pulse.observed": (_event: any, vm: any) =>
                vm.set("seen", vm.get("seen") + 1),
            },
          },
        },
      },
    },
    sagas: {
      watcher: {
        reads: ["detail"],
        reactions: {
          "audit.pulse.observed": (event: any) => {
            options.sagaRuns?.push(event.id);
          },
        },
      },
    },
    gateways: {
      http: {
        port: 0,
        access: { domain: { "*": true }, readmodels: { "*": true } },
        routes: [
          {
            method: "post",
            path: "/orders/:id",
            handler: command(
              (req) => ({
                stream: { ...order, id: String(req.params.id) },
                name: "open",
                payload: req.body,
              }),
              { respondWith: "detail.order" },
            ),
          },
          {
            method: "get",
            path: "/orders",
            handler: readmodel("detail", "order"),
          },
          {
            method: "get",
            path: "/pulses",
            handler: readmodel("other", "pulse"),
          },
        ],
      },
    },
  });
const seed = async (stores: Parameters<typeof definition>[0]) => {
  for (let i = 1; i <= 5; i++)
    await stores.store.appendToStream(order, i - 1, [
      pending(`opened-${i}`, "opened", { amount: i }),
    ]);
  await stores.store.appendToStream(pulse, 0, [pending("pulse-1", "observed")]);
  const before = definition(stores);
  await before.start();
  await expect
    .poll(async () => stores.models.checkpoint("dependent"))
    .toEqual(await stores.store.head());
  await before.close();
};
const gate = () => {
  let open!: () => void;
  const opened = new Promise<void>((resolve) => (open = resolve));
  return { opened, open };
};

test("a replica serves while its required startup rebuild runs behind it", async () => {
  const stores = {
    store: createInMemoryEventStore(),
    models: createMemoryReadmodelStore(),
  };
  await seed(stores);
  const dependentBefore = await stores.models.checkpoint("dependent");
  const held = gate();
  let rebuilding = false;
  const { logger, port } = capture();
  const sagaRuns: string[] = [];
  const app = definition(stores, {
    extra: true,
    logger,
    sagaRuns,
    react: async () => {
      rebuilding = true;
      await held.opened;
    },
  });
  try {
    await app.start();
    await expect.poll(() => rebuilding).toBe(true);
    const health = await app.health();
    expect(health.liveness).toBe(true);
    expect(health.readiness).toBe(true);
    expect(health.rebuilding).toEqual(["detail"]);
    await expect(app.readmodels.detail.order.find()).rejects.toBeInstanceOf(
      ReadmodelRebuildingError,
    );
    await expect(
      app.readmodels.waitFor((await stores.store.head())!, ["detail"]),
    ).rejects.toBeInstanceOf(ReadmodelRebuildingError);
    const unavailable = await fetch(`http://127.0.0.1:${port()}/orders`);
    expect(unavailable.status).toBe(503);
    expect(unavailable.headers.get("Retry-After")).toBe("5");
    expect((await unavailable.json()).code).toBe("readmodel-rebuilding");

    // Unaffected groups keep consuming; groups reading `detail` wait for its swap.
    const [live] = await stores.store.appendToStream(pulse, 1, [
      pending("pulse-2", "observed"),
    ]);
    await expect
      .poll(() => stores.models.checkpoint("other"))
      .toEqual(live!.position);
    const pulses = await fetch(`http://127.0.0.1:${port()}/pulses`);
    expect(pulses.status).toBe(200);
    expect((await pulses.json())[0].seen).toBe(2);
    expect(await stores.models.checkpoint("dependent")).toEqual(
      dependentBefore,
    );
    expect(sagaRuns).toEqual([]);
    // A Command answered from the rebuilding group is accepted; its document follows the swap.
    const accepted = await fetch(`http://127.0.0.1:${port()}/orders/two`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ amount: 2 }),
    });
    expect(accepted.status).toBe(202);
    expect((await accepted.json()).status).toBe("accepted");
    // An Event recorded during the rebuild is applied exactly once after the swap.
    await stores.store.appendToStream(order, 5, [
      pending("opened-6", "opened", { amount: 6 }),
    ]);
    held.open();
    await expect
      .poll(() => app.readmodels.detail.order.find().catch(() => null))
      .toEqual([
        { _id: "one", total: 21 },
        { _id: "two", total: 2 },
      ]);
    expect((await app.health()).rebuilding).toEqual([]);
    await expect
      .poll(() => stores.models.checkpoint("dependent"))
      .toEqual(await stores.store.head());
    await expect.poll(() => sagaRuns).toEqual(["pulse-2"]);
    const orders = await fetch(`http://127.0.0.1:${port()}/orders`);
    expect(orders.status).toBe(200);
  } finally {
    held.open();
    await app.close();
  }
});

test("a startup rebuild stopped part-way resumes its pending generation on the next start", async () => {
  const stores = {
    store: createInMemoryEventStore(),
    models: createMemoryReadmodelStore(),
  };
  await seed(stores);
  const created = vi.spyOn(stores.models, "createGeneration");
  const held = gate();
  let reactions = 0;
  const first = definition(stores, {
    extra: true,
    react: async () => {
      if (++reactions === 3) await held.opened;
    },
  });
  await first.start();
  await expect.poll(() => reactions).toBe(3);
  const closing = first.close();
  held.open();
  await closing;
  const generation = await stores.models.pendingGeneration("detail");
  expect(generation).toBeTruthy();
  const second = definition(stores, { extra: true });
  try {
    await second.start();
    await expect
      .poll(() => second.readmodels.detail.order.find().catch(() => null))
      .toEqual([{ _id: "one", total: 15 }]);
    expect(await stores.models.activeGeneration("detail")).toBe(generation);
    expect(created).toHaveBeenCalledTimes(1);
  } finally {
    await second.close();
  }
});

test("a failing startup rebuild is logged and retried while the replica serves", async () => {
  const stores = {
    store: createInMemoryEventStore(),
    models: createMemoryReadmodelStore(),
  };
  await seed(stores);
  const { logs, logger } = capture();
  let failures = 1;
  const app = definition(stores, {
    extra: true,
    logger,
    react: () => {
      if (failures-- > 0) throw new Error("transient");
    },
  });
  try {
    await app.start();
    expect((await app.health()).readiness).toBe(true);
    await expect
      .poll(() => logs.map((log) => log.message))
      .toContain("Startup rebuild failed; retrying from its Checkpoint");
    await expect
      .poll(() => app.readmodels.detail.order.find().catch(() => null), {
        timeout: 10000,
      })
      .toEqual([{ _id: "one", total: 15 }]);
  } finally {
    await app.close();
  }
}, 15000);
