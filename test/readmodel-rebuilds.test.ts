import { expect, test } from "vitest";
import { compile } from "../src/authoring/index.js";
import { createApplication, rebuildMemo } from "../src/runtime/index.js";
import {
  jsonLength,
  otherGroupReads,
  rebuildCache,
} from "../src/runtime/rebuild-cache.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import type { PendingEvent, Position } from "../src/types.js";

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

test("a partial rebuild fold commits only completed Events and decodes and identifies the retained page once", async () => {
  let decoded = 0,
    identified = 0;
  const store = createInMemoryEventStore();
  const models = createMemoryReadmodelStore();
  const commits: Position[] = [];
  const batch = models.batch.bind(models);
  models.batch = async (...args) => {
    commits.push(args[2]);
    return batch(...args);
  };
  const app = createApplication({
    name: "partial",
    store,
    transport: createInMemoryTransport(),
    aggregates: [
      compile("sales", "order", {
        events: {
          opened: {
            handler: () => {},
            schemaVersion: 2,
            upcasters: {
              1: (payload) => {
                decoded++;
                return payload;
              },
            },
          },
        },
      }),
      aggregates[2]!,
    ],
    readmodels: {
      detail: {
        store: models,
        models: {
          order: {
            schema,
            initialState: { total: 0 },
            identity: {
              "sales.order.opened": (event) => {
                identified++;
                return event.stream.id;
              },
            },
            reactions: {
              "sales.order.opened": async (event, vm) => {
                if (event.payload.amount === 1)
                  // Longer than a rebuild's 2 s fold budget, so the fold stops after this Event.
                  await new Promise((resolve) => setTimeout(resolve, 2050));
                vm.set("total", vm.get("total") + event.payload.amount);
              },
            },
          },
        },
      },
    },
  });
  try {
    const events = await store.appendToStream(
      order,
      0,
      [1, 2, 3, 4, 5, 6].map((amount) =>
        pending(`sum-${amount}`, "opened", { amount }),
      ),
    );
    const [tail] = await store.appendToStream(pulse, 0, [
      pending("tail", "observed"),
    ]);
    await app.rebuild("detail");
    expect(commits[0]).toEqual(events[0]!.position);
    expect(commits.at(-1)).toEqual(tail!.position);
    expect(await models.findOne("detail", "order", { _id: order.id })).toEqual({
      _id: order.id,
      total: 21,
    });
    expect(decoded).toBe(6);
    expect(identified).toBe(6);
  } finally {
    await app.close();
  }
});

test.each(["Reaction", "Event set", "version"])(
  "a pending generation cannot resume after its %s changes",
  async (change) => {
    const store = createInMemoryEventStore(),
      models = createMemoryReadmodelStore();
    const reaction = (event: any, vm: any) =>
      vm.set("total", vm.get("total") + event.payload.amount);
    const changedReaction = (event: any, vm: any) =>
      vm.set("total", vm.get("total") + event.payload.amount * 2);
    const appFor = (updated: boolean) =>
      createApplication({
        name: "definition",
        store,
        transport: createInMemoryTransport(),
        aggregates,
        readmodels: {
          detail: {
            store: models,
            rebuildVersion: updated && change === "version" ? "two" : "one",
            models: {
              order: {
                schema,
                initialState: { total: 0 },
                reactions: {
                  "sales.order.opened":
                    updated && change === "Reaction"
                      ? changedReaction
                      : reaction,
                  ...(updated && change === "Event set"
                    ? { "payments.receipt.recorded": reaction }
                    : {}),
                },
              },
            },
          },
        },
      });
    const first = appFor(false);
    await store.appendToStream(order, 0, [
      pending("before-definition", "opened", { amount: 10 }),
    ]);
    const prepared = await first.rebuild("detail", { swap: "manual" });
    const active = await models.activeGeneration("detail");
    const checkpoint = await models.checkpoint("detail", {
      generation: prepared.generation,
    });
    await first.close();
    const second = appFor(true);
    try {
      if (change === "Reaction")
        await store.appendToStream(order, 1, [
          pending("after-definition", "opened", { amount: 3 }),
        ]);
      await expect(second.swapRebuild("detail")).rejects.toThrow(
        /rebuild definition changed/i,
      );
      expect(await models.activeGeneration("detail")).toBe(active);
      expect(
        await models.checkpoint("detail", { generation: prepared.generation }),
      ).toEqual(checkpoint);
    } finally {
      await second.close();
    }
  },
);

test("a pending generation resumes after an unrelated Application schema changes", async () => {
  const store = createInMemoryEventStore(),
    models = createMemoryReadmodelStore();
  const appFor = (unrelated: string) =>
    createApplication({
      name: "unrelated-schema",
      store,
      transport: createInMemoryTransport(),
      aggregates,
      schemas: { Unrelated: { type: unrelated } },
      readmodels: {
        detail: {
          store: models,
          models: {
            order: {
              schema,
              initialState: { total: 0 },
              reactions: {
                "sales.order.opened": (event, vm) =>
                  vm.set("total", event.payload.amount),
              },
            },
          },
        },
      },
    });
  await store.appendToStream(order, 0, [
    pending("unrelated-schema", "opened", { amount: 4 }),
  ]);
  const first = appFor("string");
  const prepared = await first.rebuild("detail", { swap: "manual" });
  await first.close();
  const second = appFor("number");
  try {
    await second.swapRebuild("detail");
    expect(await models.activeGeneration("detail")).toBe(prepared.generation);
    expect(await models.findOne("detail", "order", { _id: order.id })).toEqual({
      _id: order.id,
      total: 4,
    });
  } finally {
    await second.close();
  }
});

test("the legacy reader does not checkpoint a matching append after an empty filtered read", async () => {
  const persistence = createInMemoryEventStore(),
    models = createMemoryReadmodelStore();
  let injected = false;
  const app = createApplication({
    name: "legacy-race",
    transport: createInMemoryTransport(),
    aggregates,
    store: {
      ...persistence,
      readPage: undefined,
      readAll: async (options) => {
        const events = await persistence.readAll(options);
        if (!injected && options?.eventNames) {
          injected = true;
          await persistence.appendToStream(order, 0, [
            pending("concurrent-match", "opened", { amount: 7 }),
          ]);
        }
        return events;
      },
    },
    readmodels: {
      detail: {
        store: models,
        models: {
          order: {
            schema,
            reactions: {
              "sales.order.opened": (event, vm) =>
                vm.set("total", event.payload.amount),
            },
          },
        },
      },
    },
  });
  try {
    await persistence.appendToStream(pulse, 0, [
      pending("before-race", "observed"),
    ]);
    await app.rebuild("detail");
    expect(await models.findOne("detail", "order", { _id: order.id })).toEqual({
      _id: order.id,
      total: 7,
    });
    expect(await models.checkpoint("detail")).toEqual(await persistence.head());
  } finally {
    await app.close();
  }
});

test("bulk Identity preparation preserves chronological queries, staged reads and deletion/recreation", async () => {
  const store = createInMemoryEventStore(),
    models = createMemoryReadmodelStore();
  let findOnes = 0,
    identities = 0;
  const findOne = models.findOne.bind(models);
  models.findOne = (...args) => {
    findOnes++;
    return findOne(...args);
  };
  const app = createApplication({
    name: "identity",
    store,
    transport: createInMemoryTransport(),
    aggregates,
    readmodels: {
      detail: {
        store: models,
        models: {
          order: {
            schema,
            initialState: { total: 0 },
            identity: {
              "sales.order.opened": (event) => {
                identities++;
                return event.payload.identity;
              },
            },
            reactions: {
              "sales.order.opened": async (event, vm, app) => {
                if (event.payload.destroy) return vm.destroy();
                const extra = event.payload.readB
                  ? (
                      await app
                        .getReadmodels()
                        .detail.order.findOne({ _id: "b" })
                    ).total
                  : 0;
                vm.set("total", vm.get("total") + event.payload.amount + extra);
              },
            },
          },
        },
      },
    },
  });
  try {
    const payloads = [
      { identity: ["a", "b"], amount: 1 },
      { identity: { total: 1 }, amount: 2 },
      { identity: "a", destroy: true },
      { identity: { total: { gte: 3 } }, amount: 4 },
      { identity: "a", amount: 5, readB: true },
      { identity: { total: { gte: 5 } }, amount: 1 },
    ];
    await store.appendToStream(
      order,
      0,
      payloads.map((payload, index) =>
        pending(`identity-${index}`, "opened", payload),
      ),
    );
    await app.rebuild("detail");
    expect(
      await models.find("detail", "order", {}, { sort: { _id: 1 } }),
    ).toEqual([
      { _id: "a", total: 13 },
      { _id: "b", total: 8 },
    ]);
    expect(identities).toBe(6);
    expect(findOnes).toBe(0);
  } finally {
    await app.close();
  }
});

test("a Reaction that changes a staged document it read does not change later reads of it", async () => {
  const store = createInMemoryEventStore(),
    models = createMemoryReadmodelStore();
  const app = createApplication({
    name: "staged",
    store,
    transport: createInMemoryTransport(),
    aggregates,
    readmodels: {
      detail: {
        store: models,
        models: {
          order: {
            schema,
            initialState: { total: 0 },
            identity: {
              "sales.order.opened": (event) => event.payload.identity,
            },
            reactions: {
              "sales.order.opened": async (event, vm, app) => {
                if (!event.payload.copyB)
                  return vm.set("total", event.payload.amount);
                const order = app.getReadmodels().detail.order;
                (await order.findOne({ _id: "b" })).total = 999;
                vm.set("total", (await order.find({ total: 2 }))[0].total);
              },
            },
          },
        },
      },
    },
  });
  try {
    await store.appendToStream(order, 0, [
      pending("staged-0", "opened", { identity: "b", amount: 2 }),
      pending("staged-1", "opened", { identity: "a", copyB: true }),
    ]);
    await app.rebuild("detail");
    expect(
      await models.find("detail", "order", {}, { sort: { _id: 1 } }),
    ).toEqual([
      { _id: "a", total: 2 },
      { _id: "b", total: 2 },
    ]);
  } finally {
    await app.close();
  }
});

test("preparing a later failing Identity preserves an earlier completed fold", async () => {
  const store = createInMemoryEventStore(),
    models = createMemoryReadmodelStore();
  const app = createApplication({
    name: "identity-failure",
    store,
    transport: createInMemoryTransport(),
    aggregates,
    readmodels: {
      detail: {
        store: models,
        models: {
          order: {
            schema,
            identity: {
              "sales.order.opened": (event) => {
                if (event.payload.fail) throw new Error("Identity failed");
                return "one";
              },
            },
            reactions: {
              "sales.order.opened": async (_event, vm) => {
                // Longer than a rebuild's 2 s fold budget, so the fold stops after this Event.
                await new Promise((resolve) => setTimeout(resolve, 2050));
                vm.set("total", 1);
              },
            },
          },
        },
      },
    },
  });
  try {
    const [first] = await store.appendToStream(order, 0, [
      pending("good-identity", "opened"),
      pending("bad-identity", "opened", { fail: true }),
    ]);
    await expect(app.rebuild("detail")).rejects.toThrow("Identity failed");
    const generation = (await models.pendingGeneration("detail"))!;
    expect(await models.checkpoint("detail", { generation })).toEqual(
      first!.position,
    );
    expect(
      await models.findOne("detail", "order", { _id: "one" }, { generation }),
    ).toEqual({ _id: "one", total: 1 });
    expect(await models.find("detail", "order")).toEqual([]);
  } finally {
    await app.close();
  }
});
const schema = { type: "object", properties: { total: { type: "number" } } };
const order = { context: "sales", aggregate: "order", id: "order-one" };
const receipt = {
  context: "payments",
  aggregate: "receipt",
  id: "receipt-one",
};
const pulse = { context: "audit", aggregate: "pulse", id: "pulse-one" };
const aggregates = [
  compile("sales", "order", { commands: {}, events: { opened: () => {} } }),
  compile("payments", "receipt", {
    commands: {},
    events: { recorded: () => {} },
  }),
  compile("audit", "pulse", {
    commands: {},
    events: {
      observed: {
        handler: () => {},
        schemaVersion: 2,
        upcasters: {
          1: () => {
            throw new Error("unrelated old payload");
          },
        },
      },
    },
  }),
];

test.each([false, true])(
  "rebuild and live Readmodels progress across irrelevant payloads, with legacy adapter %s",
  async (legacy) => {
    const persistence = createInMemoryEventStore();
    const store = legacy
      ? { ...persistence, readPage: undefined }
      : persistence;
    const models = createMemoryReadmodelStore();
    const application = createApplication({
      name: "selective",
      store,
      transport: createInMemoryTransport(),
      aggregates,
      pollInterval: 5,
      readmodels: {
        detail: {
          store: models,
          models: {
            order: {
              schema,
              initialState: { total: 0 },
              identity: {
                "payments.receipt.recorded": (event) => event.payload.orderId,
              },
              reactions: {
                "sales.order.opened": (event, vm) =>
                  vm.set("total", event.payload.amount),
                "payments.receipt.recorded": (event, vm) =>
                  vm.set("total", vm.get("total") + event.payload.amount),
              },
            },
          },
        },
        empty: { store: models, models: { none: { schema, reactions: {} } } },
      },
    });
    try {
      await store.appendToStream(pulse, 0, [
        pending("noise-first", "observed"),
      ]);
      await store.appendToStream(order, 0, [
        pending("order", "opened", { amount: 10 }),
      ]);
      await store.appendToStream(receipt, 0, [
        pending("receipt", "recorded", { amount: 7, orderId: "order-one" }),
      ]);
      await store.appendToStream(pulse, 1, [pending("noise-last", "observed")]);
      await application.rebuild("detail");
      await application.rebuild("empty");
      expect(
        await models.findOne("detail", "order", { _id: "order-one" }),
      ).toEqual({ _id: "order-one", total: 17 });
      expect(await models.checkpoint("detail")).toEqual(await store.head());
      expect(await models.checkpoint("empty")).toEqual(await store.head());
      await application.start({
        only: ["readmodels:detail", "readmodels:empty"],
      });
      const [noise] = await store.appendToStream(pulse, 2, [
        pending("noise-live", "observed"),
      ]);
      await expect
        .poll(() => models.checkpoint("detail"))
        .toEqual(noise!.position);
      await expect
        .poll(() => models.checkpoint("empty"))
        .toEqual(noise!.position);
      const [payment] = await store.appendToStream(receipt, 1, [
        pending("receipt-live", "recorded", {
          amount: 3,
          orderId: "order-one",
        }),
      ]);
      await expect
        .poll(() => models.checkpoint("detail"))
        .toEqual(payment!.position);
      expect(
        await models.findOne("detail", "order", { _id: "order-one" }),
      ).toEqual({ _id: "order-one", total: 20 });
    } finally {
      await application.close();
    }
  },
);

test("a rebuild folds the next page while the previous batch commits, one commit at a time and in order", async () => {
  const store = createInMemoryEventStore(),
    models = createMemoryReadmodelStore();
  const commits: Position[] = [],
    written: number[] = [],
    log: string[] = [];
  let inFlight = 0,
    maxInFlight = 0;
  const batch = models.batch.bind(models);
  models.batch = async (...args) => {
    maxInFlight = Math.max(maxInFlight, ++inFlight);
    log.push("commit");
    await new Promise((resolve) => setTimeout(resolve, 100));
    try {
      commits.push(args[2]);
      // What the store writes after the delay: a later fold must not have changed it meanwhile.
      written.push(...args[3].map((change) => change.document?.total as number));
      return await batch(...args);
    } finally {
      inFlight--;
    }
  };
  const app = createApplication({
    name: "pipelined",
    store,
    transport: createInMemoryTransport(),
    aggregates,
    readmodels: {
      detail: {
        store: models,
        models: {
          order: {
            schema,
            initialState: { total: 0 },
            reactions: {
              "sales.order.opened": (event, vm) => {
                if (inFlight) log.push("fold during commit");
                vm.set("total", vm.get("total") + event.payload.amount);
              },
            },
          },
        },
      },
    },
  });
  try {
    // One more Event than a rebuild page, all on one document: page two folds against page one's uncommitted change.
    const events = await store.appendToStream(
      order,
      0,
      Array.from({ length: 5001 }, (_, i) =>
        pending(`pipelined-${i}`, "opened", { amount: 1 }),
      ),
    );
    await app.rebuild("detail");
    expect(await models.findOne("detail", "order", { _id: order.id })).toEqual({
      _id: order.id,
      total: 5001,
    });
    expect(maxInFlight).toBe(1);
    expect(log).toContain("fold during commit");
    expect(written.slice(0, 2)).toEqual([5000, 5001]);
    expect(commits.slice(0, 2)).toEqual([
      events[4999]!.position,
      events[5000]!.position,
    ]);
  } finally {
    await app.close();
  }
});

test("a failed rebuild commit stops the rebuild before a later batch commits, and it resumes from the stored Checkpoint", async () => {
  const store = createInMemoryEventStore(),
    models = createMemoryReadmodelStore();
  const commits: Position[] = [];
  let fail = true;
  const batch = models.batch.bind(models);
  models.batch = async (...args) => {
    commits.push(args[2]);
    await new Promise((resolve) => setTimeout(resolve, 50));
    if (fail) throw new Error("commit failed");
    return batch(...args);
  };
  const app = createApplication({
    name: "pipelined-failure",
    store,
    transport: createInMemoryTransport(),
    aggregates,
    readmodels: {
      detail: {
        store: models,
        models: {
          order: {
            schema,
            initialState: { total: 0 },
            reactions: {
              "sales.order.opened": (event, vm) =>
                vm.set("total", vm.get("total") + event.payload.amount),
            },
          },
        },
      },
    },
  });
  try {
    await store.appendToStream(
      order,
      0,
      Array.from({ length: 5001 }, (_, i) =>
        pending(`failing-${i}`, "opened", { amount: 1 }),
      ),
    );
    const { generation } = await app
      .rebuild("detail", { swap: "manual" })
      .then(
        () => ({ generation: undefined }),
        async (error) => {
          expect(error.message).toBe("commit failed");
          return { generation: await models.pendingGeneration("detail") };
        },
      );
    expect(commits).toHaveLength(1);
    expect(await models.checkpoint("detail", { generation })).toBeNull();
    fail = false;
    // Finishing the pending generation continues from its Checkpoint.
    await app.swapRebuild("detail");
    expect(await models.activeGeneration("detail")).toBe(generation);
    expect(await models.findOne("detail", "order", { _id: order.id })).toEqual({
      _id: order.id,
      total: 5001,
    });
  } finally {
    await app.close();
  }
});

test("a rebuild reads its documents once while they fit its cache, and evicting them keeps the same result", async () => {
  const store = createInMemoryEventStore();
  // Over two rebuild pages on 300 documents, each Reaction also reading another document.
  await store.appendToStream(
    order,
    0,
    Array.from({ length: 12000 }, (_, i) =>
      pending(`cached-${i}`, "opened", {
        identity: `d${i % 300}`,
        other: `d${(i * 7) % 300}`,
        amount: i % 7,
      }),
    ),
  );
  const rebuild = async (rebuildCacheBytes?: number) => {
    const models = createMemoryReadmodelStore();
    let byId = 0;
    const find = models.find.bind(models),
      batch = models.batch.bind(models);
    models.find = async (...args) => {
      if (args[3]?.generation && "_id" in (args[2] ?? {})) byId++;
      return find(...args);
    };
    // Slow commits, so each page folds against the previous one's uncommitted changes.
    models.batch = async (...args) => {
      await new Promise((resolve) => setTimeout(resolve, 20));
      return batch(...args);
    };
    const app = createApplication({
      name: "cached",
      store,
      transport: createInMemoryTransport(),
      aggregates,
      rebuildCacheBytes,
      readmodels: {
        detail: {
          store: models,
          models: {
            order: {
              schema,
              initialState: { total: 0 },
              identity: {
                "sales.order.opened": (event) => event.payload.identity,
              },
              reactions: {
                "sales.order.opened": async (event, vm, app) => {
                  const other = await app
                    .getReadmodels()
                    .detail.order.findOne({ _id: event.payload.other });
                  vm.set(
                    "total",
                    (vm.get("total") * 3 +
                      (other?.total ?? 1) +
                      event.payload.amount) %
                      10007,
                  );
                },
              },
            },
          },
        },
      },
    });
    try {
      await app.rebuild("detail");
      return {
        byId,
        documents: await models.find(
          "detail",
          "order",
          {},
          { sort: { _id: 1 } },
        ),
      };
    } finally {
      await app.close();
    }
  };
  const uncached = await rebuild(0),
    cached = await rebuild(),
    evicting = await rebuild(2000);
  expect(uncached.documents).toHaveLength(300);
  expect(uncached.byId).toBeGreaterThan(0);
  expect(cached.documents).toEqual(uncached.documents);
  // The generation began empty and kept every document, so no read by `_id` reached the store.
  expect(cached.byId).toBe(0);
  expect(evicting.documents).toEqual(uncached.documents);
  expect(evicting.byId).toBeGreaterThan(0);
}, 60000);

test("a document evicted while its batch commits is not cached in its earlier state", async () => {
  const store = createInMemoryEventStore(),
    models = createMemoryReadmodelStore();
  const batch = models.batch.bind(models);
  models.batch = async (...args) => {
    await new Promise((resolve) => setTimeout(resolve, 200));
    return batch(...args);
  };
  // Page one writes x first, so the batch's later documents evict it; pages two and three read it, page two
  // while page one still commits.
  await store.appendToStream(order, 0, [
    pending("evicted-x", "opened", { identity: "x", amount: 5 }),
    ...Array.from({ length: 4999 }, (_, i) =>
      pending(`evicted-a${i}`, "opened", { identity: `a${i}`, amount: 1 }),
    ),
    pending("evicted-r1", "opened", { identity: "r1", read: "x" }),
    // Skipped, so batch two writes only r1 and keeps what it read of x.
    ...Array.from({ length: 4999 }, (_, i) =>
      pending(`evicted-b${i}`, "opened", { identity: null }),
    ),
    pending("evicted-r2", "opened", { identity: "r2", read: "x" }),
  ]);
  const app = createApplication({
    name: "evicted",
    store,
    transport: createInMemoryTransport(),
    aggregates,
    rebuildCacheBytes: 2000,
    readmodels: {
      detail: {
        store: models,
        models: {
          order: {
            schema,
            initialState: { total: 0 },
            identity: {
              "sales.order.opened": (event) => event.payload.identity,
            },
            reactions: {
              "sales.order.opened": async (event, vm, app) =>
                vm.set(
                  "total",
                  event.payload.read
                    ? ((
                        await app
                          .getReadmodels()
                          .detail.order.findOne({ _id: event.payload.read })
                      )?.total ?? -1)
                    : event.payload.amount,
                ),
            },
          },
        },
      },
    },
  });
  try {
    await app.rebuild("detail");
    expect(
      await models.find(
        "detail",
        "order",
        { _id: { in: ["r1", "r2"] } },
        { sort: { _id: 1 } },
      ),
    ).toEqual([
      { _id: "r1", total: 5 },
      { _id: "r2", total: 5 },
    ]);
  } finally {
    await app.close();
  }
}, 60000);

test("a rebuild reads each document of a group it reads once, and keeps rebuildMemo values until it ends", async () => {
  const store = createInMemoryEventStore();
  await store.appendToStream(
    receipt,
    0,
    Array.from({ length: 10 }, (_, k) =>
      pending(`other-r${k}`, "recorded", { id: `r${k}`, amount: k }),
    ),
  );
  await store.appendToStream(
    order,
    0,
    Array.from({ length: 6000 }, (_, i) =>
      pending(`other-o${i}`, "opened", { ref: `r${i % 10}` }),
    ),
  );
  const rebuild = async (rebuildCacheBytes?: number) => {
    const models = createMemoryReadmodelStore();
    let otherReads = 0,
      loads = 0;
    const find = models.find.bind(models);
    models.find = async (...args) => {
      if (args[0] === "basic" && !args[3]?.generation) otherReads++;
      return find(...args);
    };
    const app = createApplication({
      name: "other",
      store,
      transport: createInMemoryTransport(),
      aggregates,
      rebuildCacheBytes,
      readmodels: {
        basic: {
          store: models,
          models: {
            entity: {
              schema,
              identity: {
                "payments.receipt.recorded": (event) => event.payload.id,
              },
              reactions: {
                "payments.receipt.recorded": (event, vm) =>
                  vm.set("total", event.payload.amount),
              },
            },
          },
        },
        detail: {
          store: models,
          reads: ["basic"],
          models: {
            order: {
              schema,
              initialState: { total: 0 },
              reactions: {
                "sales.order.opened": async (event, vm, app) => {
                  const { document } = await app
                    .getReadmodels()
                    .basic.entity(event.payload.ref);
                  const doubled = await rebuildMemo(
                    ["double", event.payload.ref],
                    () => {
                      loads++;
                      return document.total * 2;
                    },
                  );
                  vm.set("total", vm.get("total") + doubled);
                },
              },
            },
          },
        },
      },
    });
    try {
      await app.rebuild("basic");
      otherReads = 0;
      await app.rebuild("detail");
      return {
        otherReads,
        loads,
        document: await models.findOne("detail", "order", { _id: order.id }),
      };
    } finally {
      await app.close();
    }
  };
  const uncached = await rebuild(0),
    cached = await rebuild();
  // 600 rounds over r0..r9 (2 × 45 each) = 54000.
  expect(uncached.document).toEqual({ _id: order.id, total: 54000 });
  expect(cached.document).toEqual(uncached.document);
  expect(uncached.otherReads).toBe(6000);
  expect(uncached.loads).toBe(6000);
  expect(cached.otherReads).toBe(10);
  expect(cached.loads).toBe(10);
  // Outside a rebuild, rebuildMemo always loads.
  let calls = 0;
  await rebuildMemo("key", () => ++calls);
  await rebuildMemo("key", () => ++calls);
  expect(calls).toBe(2);
}, 60000);

test("a kept read of another group serves only batches its Checkpoint covers, and only plain JSON keys", async () => {
  const cache = rebuildCache(2 ** 20),
    at = (sequence: number) => ({
      transactionId: "1",
      sequence: String(sequence),
    });
  let position = at(10),
    reads = 0;
  const readmodels = {
    status: async () => ({ position, behind: false }),
    repository: () =>
      Object.assign(
        async () => ({ document: { reads: ++reads }, position, behind: false }),
        {
          find: async () => [{ reads: ++reads }],
        },
      ),
  };
  const batch = (through: number) =>
    otherGroupReads(cache, "s", readmodels, { basic: ["entity"] }, at(through))
      .basic!.entity!;
  expect((await batch(5)("a")).document).toEqual({ reads: 1 });
  expect((await batch(10)("a")).document).toEqual({ reads: 1 });
  // A batch past the kept Checkpoint reads again; the group has moved on.
  position = at(20);
  expect((await batch(11)("a")).document).toEqual({ reads: 2 });
  expect((await batch(20)("a")).document).toEqual({ reads: 2 });
  // Distinct regular expressions would share a JSON key, so they are never kept.
  expect(await batch(20).find({ name: /a/ })).toEqual([{ reads: 3 }]);
  expect(await batch(20).find({ name: /b/ })).toEqual([{ reads: 4 }]);
  expect(await batch(20).find({ name: "a" })).toEqual([{ reads: 5 }]);
  expect(await batch(20).find({ name: "a" })).toEqual([{ reads: 5 }]);
  // A dropped rebuild keeps nothing.
  cache.drop("s");
  expect((await batch(20)("a")).document).toEqual({ reads: 6 });
});

test("an import takes over another store's documents at a Position and folds only the Events after it", async () => {
  const events = (from: number, count: number) =>
    Array.from({ length: count }, (_, i) =>
      pending(`imported-${from + i}`, "opened", {
        identity: `d${(from + i) % 50}`,
        amount: (from + i) % 7,
      }),
    );
  // The old platform's log stopped at `cutover`; the new one holds the same Events and more.
  const old = createInMemoryEventStore(),
    store = createInMemoryEventStore();
  await old.appendToStream(order, 0, events(0, 600));
  await store.appendToStream(order, 0, events(0, 600));
  const cutover = (await store.head())!;
  expect(await old.head()).toEqual(cutover);
  await store.appendToStream(order, 600, events(600, 400));
  let folded = 0;
  const application = (
    events: ReturnType<typeof createInMemoryEventStore>,
    models = createMemoryReadmodelStore(),
  ) => ({
    models,
    app: createApplication({
      name: "imports",
      store: events,
      transport: createInMemoryTransport(),
      aggregates,
      readmodels: {
        detail: {
          store: models,
          models: {
            order: {
              schema,
              initialState: { total: 0 },
              identity: {
                "sales.order.opened": (event) => event.payload.identity,
              },
              reactions: {
                "sales.order.opened": (event, vm) => {
                  folded++;
                  // Order-dependent, so continuing from the wrong Position changes the result.
                  vm.set(
                    "total",
                    (vm.get("total") * 3 + event.payload.amount) % 10007,
                  );
                },
              },
            },
          },
        },
      },
    }),
  });
  const documents = (models: ReturnType<typeof createMemoryReadmodelStore>) =>
    models.find("detail", "order", {}, { sort: { _id: 1 } });
  const run = async <T>(
    { app }: ReturnType<typeof application>,
    work: (app: ReturnType<typeof createApplication>) => Promise<T>,
  ) => {
    try {
      return await work(app);
    } finally {
      await app.close();
    }
  };

  const before = application(old);
  await run(before, (app) => app.rebuild("detail"));
  const full = application(store);
  await run(full, (app) => app.rebuild("detail"));
  expect(await documents(full.models)).toHaveLength(50);

  folded = 0;
  const after = application(store);
  const result = await run(after, (app) =>
    app.importReadmodels(
      "detail",
      {
        order: (async function* () {
          yield* await documents(before.models);
        })(),
      },
      { position: cutover, chunk: 7 },
    ),
  );
  expect(result).toMatchObject({
    position: cutover,
    counts: { order: 50 },
    swapped: true,
  });
  expect(folded).toBe(400);
  expect(await documents(after.models)).toEqual(await documents(full.models));
  expect(await after.models.checkpoint("detail")).toEqual(
    await full.models.checkpoint("detail"),
  );

  // At the head by default, nothing is left to fold; refused imports leave no pending generation.
  folded = 0;
  const atHead = application(store);
  await run(atHead, async (app) => {
    await expect(
      app.importReadmodels("detail", { invoice: [] }),
    ).rejects.toThrow("Unknown Readmodel detail.invoice");
    await expect(
      app.importReadmodels("detail", { order: [{ _id: 1, total: 0 } as any] }),
    ).rejects.toThrow("string _id");
    await expect(
      app.importReadmodels("detail", {
        order: [{ _id: "d0", total: "not a number" } as any],
      }),
    ).rejects.toThrow();
    // A bad document after several chunks in flight: they settle, then the generation is dropped.
    await expect(
      app.importReadmodels(
        "detail",
        {
          order: [
            ...(await documents(full.models)),
            { _id: "late", total: "not a number" } as any,
          ],
        },
        { chunk: 5, concurrency: 3 },
      ),
    ).rejects.toThrow();
    await expect(
      app.importReadmodels("detail", { order: [] }, { concurrency: 0 }),
    ).rejects.toThrow("concurrency");
    expect(await atHead.models.pendingGeneration("detail")).toBeNull();
    await app.importReadmodels("detail", {
      order: await documents(full.models),
    });
  });
  expect(folded).toBe(0);
  expect(await documents(atHead.models)).toEqual(await documents(full.models));
}, 60000);

test("rebuildGroups starts a group after the groups it reads, and a failed group stops only its readers", async () => {
  const store = createInMemoryEventStore();
  await store.appendToStream(
    order,
    0,
    Array.from({ length: 20 }, (_, i) =>
      pending(`groups-${i}`, "opened", { identity: `d${i % 4}` }),
    ),
  );
  const models = createMemoryReadmodelStore();
  const log: string[] = [],
    folded: Record<string, number> = {};
  let baseWhenReaderStarted: number | undefined;
  const group = (name: string, reads?: string[], fail = false) => ({
    store: models,
    reads,
    models: {
      order: {
        schema,
        initialState: { total: 0 },
        identity: {
          "sales.order.opened": (event: any) => event.payload.identity,
        },
        reactions: {
          "sales.order.opened": async (_event: any, vm: any) => {
            if (!log.includes(name)) log.push(name);
            if (name === "reader") baseWhenReaderStarted ??= folded.base;
            folded[name] = (folded[name] ?? 0) + 1;
            if (fail) throw new Error(`${name} fails`);
            await new Promise((resolve) => setTimeout(resolve, 1));
            vm.set("total", vm.get("total") + 1);
          },
        },
      },
    },
  });
  const app = createApplication({
    name: "groups",
    store,
    transport: createInMemoryTransport(),
    aggregates,
    readmodels: {
      reader: group("reader", ["base"]),
      base: group("base"),
      broken: group("broken", undefined, true),
      blocked: group("blocked", ["broken"]),
      alone: group("alone"),
    },
  });
  try {
    const failure = await app
      .rebuildGroups(undefined, { concurrency: 4 })
      .catch((error) => error);
    expect(failure).toBeInstanceOf(AggregateError);
    expect(failure.message).toBe("Rebuild failed for broken, blocked");
    expect(failure.errors[1].message).toMatch("Not rebuilt: broken");
    // `reader` started folding only after `base` folded every Event; `blocked` never started.
    expect(baseWhenReaderStarted).toBe(20);
    expect(log).not.toContain("blocked");
    for (const name of ["base", "reader", "alone"])
      expect(
        (await models.find(name, "order", {})).map((d) => d.total).sort(),
      ).toEqual([5, 5, 5, 5]);
    expect(await app.rebuildGroups(["alone"])).toMatchObject({
      alone: { swapped: true },
    });
    await expect(app.rebuildGroups(["missing"])).rejects.toThrow(
      "Unknown Readmodel group missing",
    );
  } finally {
    await app.close();
  }
});

test("a complete rebuild answers equality and in filters from memory, with the store's results and order", async () => {
  const store = createInMemoryEventStore();
  await store.appendToStream(
    order,
    0,
    // Three rebuild pages; after the first, only 50 of the 200 documents change, so the others are
    // read only through the index, never through the batches' overlay.
    Array.from({ length: 12000 }, (_, i) =>
      pending(`filters-${i}`, "opened", {
        identity: `d${(i * 7) % (i < 5000 ? 200 : 50)}`,
        tag: `t${i % 13}`,
        owner: i % 5 ? `o${i % 4}` : null,
        when: i % 3 ? "2026-10-17T00:00:00.000Z" : null,
      }),
    ),
  );
  const filterSchema = {
    type: "object",
    properties: {
      total: { type: "number" },
      owner: { type: ["string", "null"] },
      meta: { type: "object", properties: { tag: { type: "string" } } },
      tags: { type: "array", items: { type: "string" } },
      when: { type: ["string", "null"], format: "date-time" },
    },
  };
  const rebuild = async (rebuildCacheBytes?: number) => {
    const models = createMemoryReadmodelStore();
    let indexable = 0,
      other = 0;
    const find = models.find.bind(models),
      batch = models.batch.bind(models);
    models.find = async (...args) => {
      const filter = args[2] ?? {};
      if (args[3]?.generation && !("_id" in filter))
        if ("total" in filter) other++;
        else indexable++;
      return find(...args);
    };
    models.batch = async (...args) => {
      await new Promise((resolve) => setTimeout(resolve, 10));
      return batch(...args);
    };
    const app = createApplication({
      name: "filters",
      store,
      transport: createInMemoryTransport(),
      aggregates,
      rebuildCacheBytes,
      readmodels: {
        detail: {
          store: models,
          models: {
            order: {
              schema: filterSchema,
              initialState: { total: 0, tags: [] },
              identity: {
                "sales.order.opened": (event) => event.payload.identity,
              },
              reactions: {
                "sales.order.opened": async (event, vm, app) => {
                  const { tag, owner, when } = event.payload;
                  const order = app.getReadmodels().detail.order;
                  // An unsorted read's order depends on whether the previous batch has committed, so
                  // the order-dependent reads here sort.
                  const first = await order.findOne(
                    { "meta.tag": tag },
                    { sort: { owner: -1 } },
                  );
                  const tagged = await order.find(
                    { tags: tag },
                    { sort: { _id: 1 } },
                  );
                  const unowned = await order.find({ owner: null });
                  const mixed = await order.find({
                    "meta.tag": { in: [tag, "t1"] },
                    owner: { ne: null },
                  });
                  const dated = await order.find({
                    when: "2026-10-17T00:00:00.000Z",
                  });
                  const large = await order.find({ total: { gt: 5000 } });
                  const ids = (list: any[]) =>
                    list.reduce((n, d) => n * 31 + Number(d._id.slice(1)), 7);
                  const set = (list: any[]) =>
                    list.reduce((n, d) => n + Number(d._id.slice(1)) ** 2, 0);
                  vm.set(
                    "total",
                    (vm.get("total") * 3 +
                      Number(first?._id.slice(1) ?? 0) +
                      ids(tagged) +
                      unowned.length +
                      set(mixed) +
                      dated.length * 5 +
                      large.length) %
                      10007,
                  );
                  vm.set("owner", owner);
                  vm.set("meta", { tag });
                  vm.set("when", when);
                  vm.set("tags", [...new Set([...vm.get("tags"), tag])].slice(-3));
                },
              },
            },
          },
        },
      },
    });
    try {
      await app.rebuild("detail");
      return {
        indexable,
        other,
        documents: await models.find(
          "detail",
          "order",
          {},
          { sort: { _id: 1 } },
        ),
      };
    } finally {
      await app.close();
    }
  };
  const uncached = await rebuild(0),
    cached = await rebuild(),
    evicting = await rebuild(60000);
  expect(uncached.documents).toHaveLength(200);
  expect(uncached.indexable).toBeGreaterThan(0);
  expect(cached.documents).toEqual(uncached.documents);
  // Every equality filter came from memory; the range filter still read the store.
  expect(cached.indexable).toBe(0);
  expect(cached.other).toBeGreaterThan(0);
  // Once the cache evicts a document, filters read the store again, with the same result.
  expect(evicting.documents).toEqual(uncached.documents);
  expect(evicting.indexable).toBeGreaterThan(0);
}, 120000);

test("the cache estimates a document's size close to its JSON length, and a cycle never fits", () => {
  const document = {
    name: "Some entity",
    amount: 1234.56,
    active: true,
    removed: null,
    at: new Date(0),
    contacts: Array.from({ length: 5 }, (_, i) => ({ id: `c${i}`, n: i })),
    tags: ["a", "b"],
  };
  const length = JSON.stringify(document).length;
  expect(jsonLength(document)).toBeGreaterThan(length * 0.8);
  expect(jsonLength(document)).toBeLessThan(length * 1.25);
  // A value with `toJSON` is measured by what it writes, not by its own graph.
  class Driven {
    graph = { self: this, big: "x".repeat(10000) };
    toJSON() {
      return { id: 1 };
    }
  }
  expect(jsonLength({ row: new Driven() })).toBeLessThan(100);
  const cycle: any = { a: 1 };
  cycle.self = cycle;
  expect(jsonLength(cycle)).toBe(Infinity);
  const cache = rebuildCache(10000);
  cache.set("scope", "cycle", cycle);
  expect(cache.get("cycle")).toBeUndefined();
});
