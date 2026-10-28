import pino from "pino";
import { expect, test } from "vitest";
import { createApplication } from "../src/runtime/index.js";
import { compile } from "../src/authoring/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import type { Persistence, RecordedEvent } from "../src/types.js";

const logger = pino({ level: "silent" });
const aggregate = compile("c", "a", {
  commands: {},
  events: { added: () => {}, other: () => {}, failed: () => {} },
});

/** Wraps `target` so every call of the named methods is recorded with its arguments. */
const counted = <T extends object>(target: T, methods: string[]) => {
  const calls: Record<string, unknown[][]> = Object.fromEntries(
    methods.map((method) => [method, []]),
  );
  const proxy = new Proxy(target, {
    get(object, key, receiver) {
      const value = Reflect.get(object, key, receiver);
      if (typeof key !== "string" || !methods.includes(key)) return value;
      return (...args: unknown[]) => {
        calls[key]!.push(args);
        return (value as (...args: unknown[]) => unknown).apply(object, args);
      };
    },
  });
  const reset = () => methods.forEach((method) => (calls[method] = []));
  return { proxy, calls: () => calls, reset };
};

const appender = (store: Persistence) => {
  let revision = 0;
  return async (name: string, payload: object = {}, count = 1) => {
    let last: RecordedEvent | undefined;
    for (let i = 0; i < count; i++)
      [last] = await store.appendToStream(
        { context: "c", aggregate: "a", id: "one" },
        revision++,
        [
          {
            id: crypto.randomUUID(),
            name,
            payload,
            metadata: {},
            schemaVersion: 1,
            timestamp: new Date().toISOString(),
            commandId: crypto.randomUUID(),
            correlationId: crypto.randomUUID(),
          },
        ],
      );
    return last!;
  };
};
const idle = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

test("a live Readmodel Partition reads its Checkpoint and dead letters once per Lease and leaves Lease renewal to the timer", async () => {
  const events = createInMemoryEventStore(),
    models = createMemoryReadmodelStore();
  const store = counted(events, ["deadLetters", "renewLeases"]),
    readmodelStore = counted(models, ["checkpoint"]);
  const app = createApplication({
    name: "ticks",
    store: store.proxy,
    transport: createInMemoryTransport(),
    pollInterval: 5,
    // The renewal timer runs every 10 s: none falls inside this test.
    leaseDuration: 30000,
    logger,
    aggregates: [aggregate],
    readmodels: {
      g: {
        store: readmodelStore.proxy,
        models: {
          total: {
            schema: { type: "object", properties: { count: { type: "integer" } } },
            identity: { "c.a.added": () => "all" },
            reactions: {
              "c.a.added": (_e: any, vm: any) =>
                vm.set("count", (vm.get("count") ?? 0) + 1),
            },
          },
        },
      },
    },
  });
  await app.start();
  try {
    const append = appender(events);
    const last = await append("added", {}, 250);
    await expect
      .poll(() => models.checkpoint("g"), { timeout: 5000 })
      .toEqual(last.position);
    // Three batches of at most 100 Events, each committed without a Lease renewal.
    expect(store.calls().renewLeases).toHaveLength(0);
    // Read once, for the first page under this Lease; no skip can appear while the Partition is healthy.
    expect(store.calls().deadLetters.length).toBeLessThanOrEqual(1);
    // The first tick reads the stored Checkpoint; every later tick and status update answers from memory.
    const startup = readmodelStore.calls().checkpoint.length;
    expect(startup).toBeLessThanOrEqual(3);
    readmodelStore.reset();
    store.reset();
    await idle(300);
    expect(readmodelStore.calls().checkpoint).toHaveLength(0);
    expect(store.calls().deadLetters).toHaveLength(0);
    const more = await append("added", {}, 30);
    await expect
      .poll(() => models.checkpoint("g"), { timeout: 5000 })
      .toEqual(more.position);
    expect(readmodelStore.calls().checkpoint).toHaveLength(0);
    expect(store.calls().deadLetters).toHaveLength(0);
    expect((await models.findOne("g", "total", { _id: "all" }))?.count).toBe(280);
  } finally {
    await app.stop();
  }
});

test("a Saga reads only the Events it reacts to, moves its Checkpoint across the others and keeps it in memory", async () => {
  const events = createInMemoryEventStore();
  const store = counted(events, ["readAll", "readPage", "checkpoint"]);
  const app = createApplication({
    name: "ticks",
    store: store.proxy,
    transport: createInMemoryTransport(),
    pollInterval: 5,
    logger,
    // Only the Saga: the Event Notification relay reads every Event by design.
    only: ["saga:s"],
    aggregates: [aggregate],
    sagas: {
      s: {
        identity: { "c.a.added": () => "only" },
        reactions: {
          "c.a.added": (e: any, saga: any) =>
            saga.set("values", [...(saga.get("values") ?? []), e.payload.value]),
        },
      },
    },
  });
  await app.start();
  try {
    const append = appender(events);
    await append("other", {}, 150);
    await append("added", { value: 1 });
    const last = await append("other", {}, 120);
    await expect
      .poll(() => events.checkpoint("saga:s"), { timeout: 5000 })
      .toEqual(last.position);
    expect(await events.loadSaga("saga:s", "only")).toMatchObject({ values: [1] });
    const reads = store.calls();
    // No unfiltered log read from a Saga tick: every one selects the Saga's Events.
    expect(reads.readAll).toEqual([]);
    expect(reads.readPage.length).toBeGreaterThan(0);
    expect(
      reads.readPage.every(([options]: any) =>
        JSON.stringify(options.eventNames) === JSON.stringify(["c.a.added"]),
      ),
    ).toBe(true);
    // Its Checkpoint comes from memory once the tick has read or committed it.
    store.reset();
    await idle(300);
    expect(store.calls().checkpoint).toHaveLength(0);
    expect(store.calls().readPage.length).toBeGreaterThan(10);
  } finally {
    await app.stop();
  }
});

test("a Deadline falls due while unrelated Events keep arriving", async () => {
  const events = createInMemoryEventStore();
  const handled: unknown[] = [];
  const app = createApplication({
    name: "ticks",
    store: events,
    transport: createInMemoryTransport(),
    pollInterval: 5,
    logger,
    aggregates: [aggregate],
    sagas: {
      s: {
        identity: { "c.a.added": () => "only" },
        reactions: {
          "c.a.added": (_e: any, saga: any) =>
            saga.deadline("remind", new Date(Date.now() + 100).toISOString(), {}),
        },
        deadlines: { remind: (message: any) => void handled.push(message) },
      },
    },
  });
  await app.start();
  const append = appender(events);
  let writing = true;
  // Every tick sees new Events; none of them is one the Saga reacts to.
  const writer = (async () => {
    while (writing) {
      await append("other", {}, 5);
      // The memory store resolves without a timer turn; yield so the Application's timers run.
      await idle(1);
    }
  })();
  try {
    await append("added");
    await expect.poll(() => handled.length, { timeout: 5000 }).toBe(1);
  } finally {
    writing = false;
    await writer;
    await app.stop();
  }
});

test("a skip recorded without its final batch still skips the Event", async () => {
  const events = createInMemoryEventStore(),
    models = createMemoryReadmodelStore();
  const app = createApplication({
    name: "ticks",
    store: events,
    transport: createInMemoryTransport(),
    pollInterval: 5,
    logger,
    aggregates: [aggregate],
    readmodels: {
      detail: {
        store: models,
        models: {
          item: {
            schema: { type: "object", properties: { value: { type: "integer" } } },
            identity: { "c.a.added": () => "one" },
            reactions: {
              "c.a.added": (e: any, vm: any) => {
                if (e.payload.value === 2) throw new Error("poison");
                vm.set("value", e.payload.value);
              },
            },
          },
        },
      },
    },
  });
  await app.start();
  try {
    const append = appender(events);
    await append("added", { value: 1 });
    const poison = await append("added", { value: 2 });
    const last = await append("added", { value: 3 });
    await expect
      .poll(
        async () =>
          (await app.consumerStatus()).find((s) => s.group === "readmodels:detail")
            ?.state,
        { timeout: 7000 },
      )
      .toBe("halted");
    const status = (await app.consumerStatus()).find(
      (s) => s.group === "readmodels:detail",
    )!;
    // What `skipConsumer` does before its process stops: fence the store, record the skip; no final batch.
    await models.prepareSkip("detail", status.lease!.epoch, poison.position);
    await events.skipConsumer("readmodels:detail", poison.id, "operator", {
      expectedEpoch: status.lease!.epoch,
    });
    app.wake();
    await expect
      .poll(() => models.checkpoint("detail"), { timeout: 5000 })
      .toEqual(last.position);
    expect((await models.findOne("detail", "item", { _id: "one" }))?.value).toBe(3);
  } finally {
    await app.stop();
  }
}, 15000);

test("a Reaction's declaration is read once, not per Event", async () => {
  const events = createInMemoryEventStore(),
    models = createMemoryReadmodelStore();
  let reads = 0;
  // An old-toolkit marker whose every read is counted.
  const marker = {
    get settings() {
      reads++;
      return { autoCreate: true };
    },
  };
  const app = createApplication({
    name: "ticks",
    store: events,
    transport: createInMemoryTransport(),
    pollInterval: 5,
    logger,
    aggregates: [aggregate],
    readmodels: {
      g: {
        store: models,
        models: {
          total: {
            schema: { type: "object", properties: { count: { type: "integer" } } },
            identity: { "c.a.added": () => "all" },
            reactions: {
              "c.a.added": [
                marker,
                (_e: any, vm: any) => vm.set("count", (vm.get("count") ?? 0) + 1),
              ],
            },
          },
        },
      },
    },
  });
  await app.start();
  try {
    const append = appender(events);
    const first = await append("added", {}, 10);
    await expect.poll(() => models.checkpoint("g"), { timeout: 5000 }).toEqual(first.position);
    const afterTen = reads;
    const last = await append("added", {}, 150);
    await expect.poll(() => models.checkpoint("g"), { timeout: 5000 }).toEqual(last.position);
    expect((await models.findOne("g", "total", { _id: "all" }))?.count).toBe(160);
    expect(reads).toBe(afterTen);
  } finally {
    await app.stop();
  }
});
