import pino from "pino";
import { expect, test } from "vitest";
import { createApplication } from "../src/runtime/index.js";
import { compile } from "../src/authoring/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import {
  createInMemoryBroker,
  createInMemoryTransport,
} from "../src/transport/memory.js";
import { keyPartition } from "../src/transport/partition.js";
import type { Persistence } from "../src/types.js";
import type { ReadmodelStore } from "../src/readmodel-store/types.js";

/**
 * Layout switches whose coordinator stalls mid-switch. Every
 * document counts the Events it saw, so a skipped or repeated Event shows as a wrong count.
 */
const logger = pino({ level: "silent" });
const world = () => {
  const store = createInMemoryEventStore(),
    models = createMemoryReadmodelStore(),
    broker = createInMemoryBroker();
  const make = (
    partitions: number,
    leaseDuration: number,
    persistence: Persistence = store,
    readmodelStore: ReadmodelStore = models,
  ) =>
    createApplication({
      name: "layout",
      store: persistence,
      transport: createInMemoryTransport({ broker }),
      logger,
      pollInterval: 5,
      leaseDuration,
      aggregates: [
        compile("c", "a", { commands: {}, events: { added: () => {} } }),
      ],
      readmodels: {
        g: {
          partitions,
          store: readmodelStore,
          models: {
            m: {
              schema: {
                type: "object",
                properties: { seen: { type: "integer" } },
              },
              reactions: {
                "c.a.added": (_e: any, vm: any) =>
                  vm.set("seen", (vm.get("seen") ?? 0) + 1),
              },
            },
          },
        },
      },
    });
  const revisions = new Map<string, number>();
  const append = async (id: string) => {
    const revision = revisions.get(id) ?? 0;
    revisions.set(id, revision + 1);
    return (
      await store.appendToStream(
        { context: "c", aggregate: "a", id },
        revision,
        [
          {
            id: crypto.randomUUID(),
            name: "added",
            payload: {},
            metadata: {},
            schemaVersion: 1,
            timestamp: new Date().toISOString(),
            commandId: crypto.randomUUID(),
            correlationId: "c",
          },
        ],
      )
    )[0]!;
  };
  const seen = async (id: string) =>
    (await models.findOne("g", "m", { _id: id }))?.seen;
  const recorded = async () =>
    (await store.consumers()).find((s) => s.group === "layout:readmodels:g")
      ?.partitions ?? 1;
  /** A document whose owner differs between the two layouts. */
  const moving = (from: number, to: number) =>
    Array.from({ length: 200 }, (_, i) => `d${i}`).find(
      (id) => keyPartition(`m/${id}`, from) !== keyPartition(`m/${id}`, to),
    )!;
  return { store, models, make, append, seen, recorded, moving };
};
const gate = () => {
  let entered!: () => void, resume!: () => void;
  const entering = new Promise<void>((resolve) => (entered = resolve)),
    resumed = new Promise<void>((resolve) => (resume = resolve));
  return { entering, resumed, entered, resume };
};
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

test("a coordinator whose Leases expired before it published cannot overwrite a newer layout", async () => {
  const { store, make, append, seen, recorded, moving } = world();
  const pause = gate();
  let stalled = false;
  // A stalls just before publishing 2 Partitions, then loses every Lease.
  const stale: Persistence = {
    ...store,
    acquireLease: (...args) =>
      stalled ? Promise.resolve(null) : store.acquireLease(...args),
    renewLeases: (leases, ttl) =>
      stalled
        ? Promise.resolve(leases.map(() => null))
        : store.renewLeases(leases, ttl),
    publishLayout: async (...args) => {
      if (args[2] === 2) {
        stalled = true;
        pause.entered();
        await pause.resumed;
      }
      return store.publishLayout(...args);
    },
  };
  const a = make(2, 60, stale),
    b = make(3, 10000);
  try {
    await a.start({ only: ["readmodels:g"] });
    await pause.entering;
    await sleep(140);
    await b.start({ only: ["readmodels:g"] });
    await expect.poll(recorded, { timeout: 5000 }).toBe(3);
    pause.resume();
    await sleep(100);
    expect(await recorded()).toBe(3);
    const id = moving(2, 3);
    await append(id);
    await append(id);
    await expect.poll(() => seen(id), { timeout: 5000 }).toBe(2);
    await sleep(100);
    expect(await seen(id)).toBe(2);
  } finally {
    pause.resume();
    await a.stop();
    await b.stop();
  }
});

test("a layout reset fails when another instance took one of the coordinator's Partitions, so no Event runs twice", async () => {
  const { store, models, make, append, seen, recorded, moving } = world();
  const pause = gate();
  let intercept = false;
  const delayed: ReadmodelStore = {
    ...models,
    resetPartitions: async (...args) => {
      if (intercept) {
        intercept = false;
        pause.entered();
        await pause.resumed;
      }
      return models.resetPartitions(...args);
    },
  };
  const initial = make(2, 10000);
  await initial.start({ only: ["readmodels:g"] });
  const bootstrap = await append("bootstrap");
  await expect
    .poll(async () =>
      (await models.checkpoint("g", { partitions: 2 }))?.transactionId,
    )
    .toBe(bootstrap.position.transactionId);
  await initial.stop();
  // A switches 2 → 3 and stalls before its reset, while its Leases of Partitions 1 and 2 expire.
  intercept = true;
  // Balancing could prefer stalled A for Partition 1 by its random instance id, and B would then
  // wait a whole Lease duration for it. B does not see A's instance row, so it takes Partition 1.
  const blind: Persistence = {
    ...store,
    consumers: async () =>
      (await store.consumers()).filter(
        (status) => !status.groups?.includes("readmodels:g#2"),
      ),
  };
  const a = make(3, 60, store, delayed),
    b = make(2, 10000, blind);
  try {
    await a.start({ only: ["readmodels:g"] });
    await pause.entering;
    await sleep(140);
    // B still runs the recorded 2-Partition layout and takes Partition 1.
    await b.start({ only: ["readmodels:g"] });
    const id = moving(2, 3);
    await append(id);
    await expect.poll(() => seen(id), { timeout: 5000 }).toBe(1);
    pause.resume();
    await sleep(200);
    expect(await recorded()).toBe(2);
    expect(await seen(id)).toBe(1);
    // Once B stops, A switches; the next Event is applied exactly once more.
    await b.stop();
    await expect.poll(recorded, { timeout: 10000 }).toBe(3);
    await append(id);
    await expect.poll(() => seen(id), { timeout: 5000 }).toBe(2);
    await sleep(100);
    expect(await seen(id)).toBe(2);
  } finally {
    pause.resume();
    await a.stop();
    await b.stop();
  }
});

test("a switch that fails after its reset leaves the old layout running, and the next attempt neither skips nor repeats an Event", async () => {
  const { store, make, append, seen, recorded } = world();
  let failures = 1;
  // The first publication fails as if the coordinator crashed between reset and publication.
  const crashing: Persistence = {
    ...store,
    publishLayout: async (...args) => {
      if (failures-- > 0) throw new Error("coordinator crashed");
      return store.publishLayout(...args);
    },
  };
  const ids = Array.from({ length: 12 }, (_, i) => `d${i}`);
  const initial = make(2, 10000);
  await initial.start({ only: ["readmodels:g"] });
  for (const id of ids) await append(id);
  await expect.poll(() => seen(ids.at(-1)!), { timeout: 5000 }).toBe(1);
  await initial.stop();
  const a = make(3, 10000, crashing);
  try {
    await a.start({ only: ["readmodels:g"] });
    for (const id of ids) await append(id);
    await expect.poll(recorded, { timeout: 15000 }).toBe(3);
    for (const id of ids) await append(id);
    for (const id of ids)
      await expect.poll(() => seen(id), { timeout: 5000 }).toBe(3);
    await sleep(100);
    for (const id of ids) expect(await seen(id)).toBe(3);
  } finally {
    await a.stop();
  }
});
