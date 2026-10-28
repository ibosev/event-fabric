import pino from "pino";
import { expect, test } from "vitest";
import { createApplication } from "../src/runtime/index.js";
import { createReadmodels } from "../src/runtime/readmodels.js";
import { compile } from "../src/authoring/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import {
  createInMemoryBroker,
  createInMemoryTransport,
} from "../src/transport/memory.js";
import { keyPartition } from "../src/transport/partition.js";
import type { Persistence } from "../src/types.js";

const logger = pino({ level: "silent" });
const STREAMS = 24,
  ACCOUNTS = 5;
const account = (stream: number) => `acct${stream % ACCOUNTS}`;

/**
 * Every document and Saga instance records the values it saw, in order, so a missed,
 * repeated or reordered Event shows in the result. `account` documents and Saga
 * instances combine Events from several Streams; `closed` reaches items by a query Identity.
 */
const world = () => {
  const store = createInMemoryEventStore(),
    models = createMemoryReadmodelStore(),
    broker = createInMemoryBroker();
  const appended: { stream: number; value: number }[] = [];
  let version = new Map<number, number>();
  const append = async (stream: number, name: string, payload: object) => {
    const revision = version.get(stream) ?? 0;
    version.set(stream, revision + 1);
    const [event] = await store.appendToStream(
      { context: "c", aggregate: "a", id: `s${stream}` },
      revision,
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
    return event!;
  };
  /** Appends `rounds` values to every Stream, interleaving Streams. */
  const round = async (from: number, to: number) => {
    let last;
    for (let value = from; value <= to; value++)
      for (let stream = 0; stream < STREAMS; stream++) {
        appended.push({ stream, value });
        last = await append(stream, "added", {
          account: account(stream),
          value,
        });
      }
    return last!;
  };
  const make = (partitions: number) =>
    createApplication({
      name: "partitions",
      store,
      transport: createInMemoryTransport({ broker }),
      pollInterval: 5,
      leaseDuration: 600,
      logger,
      aggregates: [
        compile("c", "a", {
          commands: {},
          events: { added: () => {}, closed: () => {} },
        }),
      ],
      readmodels: {
        g: {
          store: models,
          partitions,
          models: {
            item: {
              schema: {
                type: "object",
                properties: {
                  account: { type: "string" },
                  seen: { type: "array", items: { type: "integer" } },
                  closed: { type: "boolean" },
                },
              },
              identity: {
                "c.a.closed": (e: any) => ({ account: e.payload.account }),
              },
              reactions: {
                "c.a.added": (e: any, vm: any) => {
                  vm.set("account", e.payload.account);
                  vm.set("seen", [...(vm.get("seen") ?? []), e.payload.value]);
                },
                "c.a.closed": (_e: any, vm: any) => vm.set("closed", true),
              },
            },
            account: {
              schema: {
                type: "object",
                properties: {
                  seen: { type: "array", items: { type: "string" } },
                },
              },
              identity: { "c.a.added": (e: any) => e.payload.account },
              reactions: {
                "c.a.added": (e: any, vm: any) =>
                  vm.set("seen", [
                    ...(vm.get("seen") ?? []),
                    `${e.stream.id}:${e.payload.value}`,
                  ]),
              },
            },
          },
        },
      },
      sagas: {
        s: {
          partitions,
          // It sees the Events recorded before the replicas start.
          from: "beginning",
          identity: { "c.a.added": (e: any) => e.payload.account },
          reactions: {
            "c.a.added": (e: any, saga: any) =>
              saga.set("seen", [
                ...(saga.get("seen") ?? []),
                `${e.stream.id}:${e.payload.value}`,
              ]),
          },
        },
      },
    });
  const expected = () => {
    const accounts = new Map<string, string[]>();
    for (const { stream, value } of appended) {
      const id = account(stream);
      accounts.set(id, [...(accounts.get(id) ?? []), `s${stream}:${value}`]);
    }
    return accounts;
  };
  /** Waits until every document and Saga instance holds exactly the expected sequence. */
  const settled = async (app: ReturnType<typeof make>, rounds: number) => {
    await expect
      .poll(
        async () => {
          for (let stream = 0; stream < STREAMS; stream++) {
            const item = (await app.readmodels.g.item(`s${stream}`)).document;
            if (
              JSON.stringify(item?.seen) !==
              JSON.stringify(Array.from({ length: rounds }, (_, i) => i + 1))
            )
              return `s${stream}: ${JSON.stringify(item?.seen)}`;
          }
          for (const [id, seen] of expected()) {
            const doc = (await app.readmodels.g.account(id)).document;
            if (JSON.stringify(doc?.seen) !== JSON.stringify(seen))
              return `account ${id}: ${doc?.seen?.length} of ${seen.length}`;
            const saga = await (store as Persistence).loadSaga("saga:s", id);
            if (JSON.stringify(saga?.seen) !== JSON.stringify(seen))
              return `saga ${id}: ${(saga?.seen as any)?.length} of ${seen.length}`;
          }
          return "ok";
        },
        { timeout: 15000, interval: 50 },
      )
      .toBe("ok");
  };
  return { store, make, round, append, settled };
};

test("Partitioned Readmodel groups and Sagas on three replicas see every Event once, in Position order, for every document and Saga instance", async () => {
  const { store, make, round, append, settled } = world();
  await round(1, 2);
  const replicas = [make(4), make(4), make(4)];
  try {
    await Promise.all(replicas.map((app) => app.start()));
    const last = await round(3, 6);
    // Read-your-writes waits for every Partition.
    const read = await replicas[1]!.readmodels.g.account(account(0), {
      minPosition: last.position,
    });
    expect(read.behind).toBe(false);
    expect(read.document?.seen).toHaveLength(
      Array.from({ length: STREAMS }, (_, i) => i).filter(
        (i) => account(i) === account(0),
      ).length * 6,
    );
    await settled(replicas[0]!, 6);
    for (let i = 0; i < ACCOUNTS; i++)
      await append(i, "closed", { account: `acct${i}` });
    await expect
      .poll(
        async () => replicas[2]!.readmodels.g.item.count({ closed: true }),
        {
          timeout: 5000,
        },
      )
      .toBe(STREAMS);
    // Partitions spread over the replicas.
    // shortcut: 15 s, because under a loaded test run Lease renewals of three in-process replicas lag; tighten if balancing gets an event-driven trigger.
    await expect
      .poll(
        async () =>
          new Set(
            (await store.consumers())
              .filter((s) => s.group === "readmodels:g" && s.lease)
              .map((s) => s.lease!.owner),
          ).size,
        { timeout: 15000 },
      )
      .toBeGreaterThan(1);
  } finally {
    for (const app of replicas) await app.stop();
  }
}, 40000);

test("switching 1 → 4 → 2 Partitions while Events keep coming neither skips nor repeats an Event", async () => {
  const { make, round, settled } = world();
  const one = make(1);
  await one.start();
  await round(1, 2);
  await settled(one, 2);
  await round(3, 3); // left for the new layout's drain
  await one.stop();
  const four = [make(4), make(4)];
  await Promise.all(four.map((app) => app.start()));
  await round(4, 5);
  await settled(four[0]!, 5);
  await round(6, 6);
  for (const app of four) await app.stop();
  const two = [make(2), make(2)];
  try {
    await Promise.all(two.map((app) => app.start()));
    await round(7, 8);
    await settled(two[1]!, 8);
  } finally {
    for (const app of two) await app.stop();
  }
});

test("a partitioned Readmodel group that reads its own documents halts with the reason", async () => {
  const store = createInMemoryEventStore();
  const app = createApplication({
    name: "selfread",
    store,
    transport: createInMemoryTransport(),
    pollInterval: 5,
    logger,
    aggregates: [
      compile("c", "a", { commands: {}, events: { added: () => {} } }),
    ],
    readmodels: {
      g: {
        store: createMemoryReadmodelStore(),
        partitions: 2,
        models: {
          m: {
            schema: { type: "object", properties: {} },
            reactions: {
              "c.a.added": async (_e: any, _vm: any, app: any) => {
                await app.getReadmodels().g.m.findOne({});
              },
            },
          },
        },
      },
    },
  });
  await store.appendToStream({ context: "c", aggregate: "a", id: "x" }, 0, [
    {
      id: "e1",
      name: "added",
      payload: {},
      metadata: {},
      schemaVersion: 1,
      timestamp: new Date().toISOString(),
      commandId: "c1",
      correlationId: "c1",
    },
  ]);
  await app.start();
  try {
    await expect
      .poll(
        async () =>
          (await app.consumerStatus()).find(
            (s) => s.group === "readmodels:g" && s.state === "halted",
          )?.error ?? JSON.stringify(await app.consumerStatus()),
        { timeout: 7000 },
      )
      .toMatch(/2 Partitions, so its Reactions cannot read its own documents/);
  } finally {
    await app.stop();
  }
});

test("a Readmodel group or Saga name cannot contain the Partition separator", () => {
  const app = (readmodels: object, sagas: object) =>
    createApplication({
      name: "names",
      store: createInMemoryEventStore(),
      transport: createInMemoryTransport(),
      logger,
      aggregates: [],
      readmodels: readmodels as any,
      sagas: sagas as any,
    });
  expect(() =>
    app({ "g#1": { store: createMemoryReadmodelStore(), models: {} } }, {}),
  ).toThrow('Readmodel group name g#1 must not contain "#"');
  expect(() => app({}, { "s#1": { identity: {}, reactions: {} } })).toThrow(
    'Saga name s#1 must not contain "#"',
  );
});

test("Saga effects get one Idempotency key per instance, the same under any Partition layout", async () => {
  const ids = ["instance-a", "instance-b"];
  expect(keyPartition(ids[0]!, 2)).not.toBe(keyPartition(ids[1]!, 2));
  const keys = async (partitions: number) => {
    const store = createInMemoryEventStore();
    const delivered: string[] = [];
    const app = createApplication({
      name: "keys",
      store,
      transport: createInMemoryTransport(),
      pollInterval: 5,
      logger,
      aggregates: [
        compile("c", "a", { commands: {}, events: { happened: () => {} } }),
      ],
      sagas: {
        s: {
          partitions,
          // It sees the Events recorded before the replicas start.
          from: "beginning",
          identity: { "c.a.happened": () => ids },
          reactions: {
            "c.a.happened": (_e: any, saga: any, app: any) => {
              app.queue("notify", { instance: saga.id });
              app.queue("notify", { instance: saga.id }, { suffix: "x" });
            },
          },
        },
      },
      workers: {
        notifier: {
          handles: ["notify"],
          execute: async (_message: any, ctx: any) => {
            delivered.push(ctx.idempotencyKey);
          },
        },
      },
    });
    await store.appendToStream({ context: "c", aggregate: "a", id: "s" }, 0, [
      {
        id: "event-1",
        name: "happened",
        payload: {},
        metadata: {},
        schemaVersion: 1,
        timestamp: new Date().toISOString(),
        commandId: "command-1",
        correlationId: "chain-1",
      },
    ]);
    await app.start();
    try {
      await expect.poll(() => delivered.length).toBe(4);
      return delivered.sort();
    } finally {
      await app.stop();
    }
  };
  const expected = ids
    .flatMap((id) => [
      JSON.stringify(["saga:s", id, "event-1", "0"]),
      JSON.stringify(["saga:s", id, "event-1", "x"]),
    ])
    .sort();
  expect(await keys(1)).toEqual(expected);
  expect(await keys(2)).toEqual(expected);
});

test("a subscription keeps one cursor per document: other Partitions' changes at equal or lower Positions arrive, stale and repeated ones don't", async () => {
  const models = createMemoryReadmodelStore(),
    transport = createInMemoryTransport();
  const definitions = {
    m: { schema: { type: "object", properties: {} }, reactions: {} },
  };
  await models.register("g", definitions as any);
  // Everything at or before P10 is in the initial read.
  await models.batch("g", 0, { transactionId: "10", sequence: "1" }, []);
  const readmodels = createReadmodels(
    { g: { store: models, models: definitions, partitions: 2 } } as any,
    transport,
  );
  const seen: string[] = [];
  const subscription = await readmodels.subscribe("g", "m", (n) => {
    seen.push(...n.ids);
  });
  try {
    for (const [id, tx] of [
      ["a", "20"],
      ["b", "20"], // another Partition, equal Position
      ["c", "19"], // another Partition, lower Position
      ["a", "20"], // repeated
      ["a", "18"], // older than what a's subscriber already has
      ["d", "9"], // in the initial read
      ["a", "21"],
      ["d", "11"],
    ] as const)
      await transport.publish("readmodels", {
        kind: "readmodel",
        group: "g",
        position: { transactionId: tx, sequence: "1" },
        changes: [{ model: "m", id, before: null, after: { _id: id } }],
      });
    await expect.poll(() => seen.length).toBe(5);
    expect(seen).toEqual(["a", "b", "c", "a", "d"]);
  } finally {
    await subscription.stop();
  }
});

test("a subscription to a partitioned group hears every document's latest change", async () => {
  const store = createInMemoryEventStore(),
    models = createMemoryReadmodelStore();
  const app = createApplication({
    name: "subscribed",
    store,
    transport: createInMemoryTransport(),
    pollInterval: 5,
    logger,
    aggregates: [
      compile("c", "a", { commands: {}, events: { added: () => {} } }),
    ],
    readmodels: {
      g: {
        store: models,
        partitions: 4,
        models: {
          m: {
            schema: { type: "object", properties: { n: { type: "integer" } } },
            reactions: {
              "c.a.added": (e: any, vm: any) => vm.set("n", e.payload.n),
            },
          },
        },
      },
    },
  });
  await app.start();
  const latest = new Map<string, number>();
  const subscription = await app.readmodels.subscribe("g", "m", (n: any) => {
    for (const change of n.changes) latest.set(change.id, change.after?.n);
  });
  try {
    const ids = Array.from({ length: 24 }, (_, i) => `d${i}`);
    for (let n = 0; n < 3; n++)
      for (const [i, id] of ids.entries())
        await store.appendToStream({ context: "c", aggregate: "a", id }, n, [
          {
            id: crypto.randomUUID(),
            name: "added",
            payload: { n: n * 100 + i },
            metadata: {},
            schemaVersion: 1,
            timestamp: new Date().toISOString(),
            commandId: crypto.randomUUID(),
            correlationId: "c",
          },
        ]);
    await expect
      .poll(() => ids.filter((id, i) => latest.get(id) !== 200 + i), {
        timeout: 10000,
      })
      .toEqual([]);
  } finally {
    await subscription.stop();
    await app.stop();
  }
});

test("concurrent read-your-writes waits on one group share their store reads and never return early", async () => {
  const models = createMemoryReadmodelStore();
  const definitions = {
    m: { schema: { type: "object", properties: {} }, reactions: {} },
  };
  await models.register("g", definitions as any);
  await models.batch("g", 0, { transactionId: "1", sequence: "1" }, []);
  let reads = 0;
  const counted = {
    ...models,
    checkpoint: (...args: Parameters<typeof models.checkpoint>) => {
      reads++;
      return models.checkpoint(...args);
    },
  };
  const readmodels = createReadmodels(
    { g: { store: counted, models: definitions } } as any,
    createInMemoryTransport(),
  );
  const target = { transactionId: "5", sequence: "1" };
  const waits = Array.from({ length: 50 }, () =>
    readmodels.waitFor(target, ["g"], { timeout: 2000 }),
  );
  await new Promise((resolve) => setTimeout(resolve, 200));
  await models.batch("g", 0, target, []);
  const results = await Promise.all(waits);
  for (const result of results) {
    expect(result.behind).toBe(false);
    expect(result.positions.g).toEqual(target);
  }
  // 50 waiters polling every 20 ms for 200 ms would read about 500 times on their own.
  expect(reads).toBeLessThan(60);
});
