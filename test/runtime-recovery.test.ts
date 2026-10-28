import pino from "pino";
import { expect, test } from "vitest";
import { createApplication, permanent } from "../src/runtime/index.js";
import { compile } from "../src/authoring/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import type { ApplicationConfig } from "../src/runtime/types.js";
import type { Outcome } from "../src/types.js";
const logger = pino({ level: "silent" });
const aggregate = () =>
  compile("sales", "order", {
    initialState: {},
    commands: {
      record: [
        (command: any, state: any) => state.apply.recorded(command.payload),
      ],
    },
    events: {
      recorded: (event: any, state: any) =>
        state.set("value", event.payload.value),
    },
  });
const input = (id: string, value: number) => ({
  stream: { context: "sales", aggregate: "order", id },
  name: "record",
  payload: { value },
});
const accepted = (outcome: Outcome) => {
  expect(outcome.status).toBe("accepted");
  if (outcome.status !== "accepted")
    throw new Error("Command was not accepted");
  return outcome.events;
};
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
};
const base = (): ApplicationConfig => ({
  name: "recovery",
  store: createInMemoryEventStore(),
  transport: createInMemoryTransport(),
  aggregates: [aggregate()],
  pollInterval: 5,
  logger,
});

test("a poison Readmodel Event halts exactly, an audited skip advances it, and later Events recover", async () => {
  const config = base(),
    models = createMemoryReadmodelStore();
  const app = createApplication({
    ...config,
    readmodels: {
      detail: {
        store: models,
        models: {
          order: {
            schema: {
              type: "object",
              properties: { value: { type: "integer" } },
            },
            reactions: {
              "sales.order.recorded": (event: any, vm: any) => {
                vm.set("value", event.payload.value);
                if (event.payload.value === 2) throw new Error("poison");
              },
            },
          },
        },
      },
    },
  });
  await app.start();
  try {
    const [first] = accepted(await app.sendCommand(input("one", 1)));
    const [poison] = accepted(await app.sendCommand(input("one", 2)));
    const [last] = accepted(await app.sendCommand(input("one", 3)));
    await expect
      .poll(
        async () =>
          (await app.consumerStatus()).find(
            (s) => s.group === "readmodels:detail",
          )?.state,
        { timeout: 7000 },
      )
      .toBe("halted");
    const status = (await app.consumerStatus()).find(
      (s) => s.group === "readmodels:detail",
    )!;
    expect(status.eventId).toBe(poison!.id);
    expect(status.position).toEqual(poison!.position);
    expect(status.attempts).toBeGreaterThanOrEqual(5);
    expect(await models.checkpoint("detail")).toEqual(first!.position);
    expect((await app.readmodels.detail.order("one")).document.value).toBe(1);
    await expect(
      app.skipConsumer("readmodels:detail", first!.id, "operator"),
    ).rejects.toThrow(/exact|halted/i);
    await app.skipConsumer("readmodels:detail", poison!.id, "operator");
    app.wake();
    await expect
      .poll(() => models.checkpoint("detail"), { timeout: 5000 })
      .toEqual(last!.position);
    expect((await app.readmodels.detail.order("one")).document.value).toBe(3);
    expect(await config.store.deadLetters()).toEqual([
      expect.objectContaining({
        kind: "skipped-event",
        group: "readmodels:detail",
        eventId: poison!.id,
        who: "operator",
        position: poison!.position,
      }),
    ]);
  } finally {
    await app.stop();
  }
}, 15000);

test("Saga rollback drops effects while destroy commits its Outbox, and Worker retry survives restart with a stable key", async () => {
  const persistence = createInMemoryEventStore();
  let failSaga = true;
  const deliveries: {
    key: string;
    attempt: number;
    metadata: any;
    causation: string | undefined;
  }[] = [];
  const config = () => ({
    ...base(),
    store: persistence,
    sagas: {
      notice: {
        reactions: {
          "sales.order.recorded": (_event: any, state: any, app: any) => {
            state.set("queued", true);
            app.queue("mail", { subject: "hello" });
            state.destroy();
            if (failSaga) throw new Error("rollback");
          },
        },
      },
    },
    workers: {
      mail: {
        handles: ["mail"],
        retry: { baseDelay: 300, maxDelay: 300 },
        execute: async (message: any, ctx: any) => {
          deliveries.push({
            key: ctx.idempotencyKey,
            attempt: ctx.attempt,
            metadata: message.metadata,
            causation: message.causationId,
          });
          if (ctx.attempt === 1) throw new Error("temporary");
        },
      },
    },
  });
  const producer = createApplication(config());
  await producer.start({ only: ["commands:recovery", "saga:notice"] });
  const [event] = accepted(
    await producer.sendCommand({
      ...input("one", 1),
      metadata: { tenant: "a" },
    }),
  );
  try {
    await expect
      .poll(
        async () =>
          (await persistence.consumers()).find((s) => s.group === "saga:notice")
            ?.attempts,
        { timeout: 3000 },
      )
      .toBeGreaterThanOrEqual(1);
    expect(await persistence.loadSaga("saga:notice", "one")).toBeNull();
    expect(await persistence.checkpoint("saga:notice")).toBeNull();
    expect(await persistence.pendingOutbox()).toEqual([]);
    failSaga = false;
    await persistence.retryConsumer("saga:notice");
    producer.wake();
    await expect
      .poll(() => persistence.checkpoint("saga:notice"))
      .toEqual(event!.position);
    expect(await persistence.loadSaga("saga:notice", "one")).toBeNull();
    expect(await persistence.pendingOutbox()).toHaveLength(1);
  } finally {
    await producer.stop();
  }
  const first = createApplication(config());
  await first.start({ only: ["outbox:recovery", "worker:mail"] });
  try {
    await expect.poll(() => deliveries.length).toBe(1);
    await expect
      .poll(async () => (await persistence.pendingOutbox()).length)
      .toBe(0);
  } finally {
    await first.stop();
  }
  const second = createApplication(config());
  await second.start({ only: ["outbox:recovery", "worker:mail"] });
  try {
    await expect.poll(() => deliveries.length, { timeout: 5000 }).toBe(2);
    expect(deliveries.map((d) => d.attempt)).toEqual([1, 2]);
    expect(deliveries[1]!.key).toBe(deliveries[0]!.key);
    expect(deliveries[0]).toMatchObject({
      metadata: { tenant: "a" },
      causation: event!.id,
    });
    expect(await persistence.deadLetters()).toEqual([]);
  } finally {
    await second.stop();
  }
}, 10000);

test("a permanent Worker failure records one durable dead letter without retry", async () => {
  const config = base();
  let calls = 0;
  const app = createApplication({
    ...config,
    sagas: {
      notice: {
        reactions: {
          "sales.order.recorded": (_e: any, _s: any, a: any) =>
            a.queue("mail", {}),
        },
      },
    },
    workers: {
      mail: {
        handles: ["mail"],
        execute: () => {
          calls++;
          throw permanent(new Error("invalid destination"));
        },
      },
    },
  });
  await app.start();
  try {
    accepted(await app.sendCommand(input("one", 1)));
    await expect
      .poll(async () => (await config.store.deadLetters()).length)
      .toBe(1);
    expect(calls).toBe(1);
    const [letter] = await config.store.deadLetters();
    expect(letter).toMatchObject({
      kind: "worker",
      attempts: 1,
      error: "invalid destination",
    });
    expect(letter!.message!.message.idempotencyKey).toBeTruthy();
  } finally {
    await app.stop();
  }
});

test("a dependent Readmodel reports waiting and cannot react before its declared group reaches the Event, but passes Events it has no Reaction to", async () => {
  const config = base(),
    models = createMemoryReadmodelStore(),
    gate = deferred();
  let downstream = 0;
  const app = createApplication({
    ...config,
    readmodels: {
      source: {
        store: models,
        models: {
          order: {
            schema: {
              type: "object",
              properties: { value: { type: "integer" } },
            },
            reactions: {
              "sales.order.recorded": async (e: any, vm: any) => {
                await gate.promise;
                vm.set("value", e.payload.value);
              },
            },
          },
        },
      },
      unrelated: {
        store: models,
        reads: ["source"],
        models: {
          order: { schema: { type: "object", properties: {} }, reactions: {} },
        },
      },
      derived: {
        store: models,
        reads: ["source"],
        models: {
          order: {
            schema: {
              type: "object",
              properties: { value: { type: "integer" } },
            },
            reactions: {
              "sales.order.recorded": async (e: any, vm: any, a: any) => {
                downstream++;
                const source = await a
                  .getReadmodels()
                  .source.order.findOne({ _id: e.aggregate.id });
                vm.set("value", source.value);
              },
            },
          },
        },
      },
    },
  });
  await app.start();
  try {
    const [event] = accepted(await app.sendCommand(input("one", 7)));
    await expect
      .poll(
        async () =>
          (await app.consumerStatus()).find(
            (s) => s.group === "readmodels:derived",
          )?.state,
      )
      .toBe("waiting");
    expect(downstream).toBe(0);
    expect(await models.checkpoint("derived")).toBeNull();
    await expect
      .poll(() => models.checkpoint("unrelated"))
      .toEqual(event!.position);
    gate.resolve();
    await expect
      .poll(() => models.checkpoint("derived"))
      .toEqual(event!.position);
    expect((await app.readmodels.derived.order("one")).document.value).toBe(7);
  } finally {
    gate.resolve();
    await app.stop();
  }
});

test("a timed-out Command cannot append Events after its asynchronous Step resumes", async () => {
  const config = base(),
    entered = deferred(),
    release = deferred();
  const delayed = compile("sales", "order", {
    initialState: {},
    commands: {
      record: [
        async (c: any, s: any) => {
          entered.resolve();
          await release.promise;
          s.apply.recorded(c.payload);
        },
      ],
    },
    events: { recorded: (e: any, s: any) => s.set("value", e.payload.value) },
  });
  const app = createApplication({
    ...config,
    aggregates: [delayed],
    commandTimeout: 20,
  });
  await app.start();
  try {
    const pending = app.sendCommand(input("one", 1));
    await entered.promise;
    expect(await pending).toMatchObject({
      status: "failed",
      error: { code: "timeout" },
    });
    release.resolve();
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(await config.store.readStream(input("one", 1).stream)).toEqual([]);
  } finally {
    release.resolve();
    await app.stop();
  }
});

test("an online manual rebuild retains live reads, catches concurrent Events, and emits no shadow Notifications", async () => {
  const config = base(),
    models = createMemoryReadmodelStore(),
    entered = deferred(),
    release = deferred();
  let factor = 1,
    blockNext = false;
  const app = createApplication({
    ...config,
    readmodels: {
      detail: {
        store: models,
        models: {
          order: {
            schema: {
              type: "object",
              properties: { value: { type: "integer" } },
            },
            reactions: {
              "sales.order.recorded": async (e: any, vm: any) => {
                if (blockNext) {
                  blockNext = false;
                  entered.resolve();
                  await release.promise;
                }
                vm.set("value", e.payload.value * factor);
              },
            },
          },
        },
      },
    },
  });
  await app.start();
  let subscription: any;
  try {
    const [first] = accepted(await app.sendCommand(input("one", 1)));
    await app.readmodels.waitFor(first!.position, ["detail"]);
    const old = await models.activeGeneration("detail");
    const notifications: string[][] = [];
    subscription = await app.readmodels.subscribe(
      "detail",
      "order",
      (notice) => {
        notifications.push(notice.ids);
      },
    );
    factor = 10;
    blockNext = true;
    const rebuilding = app.rebuild("detail", { swap: "manual" });
    await Promise.race([
      entered.promise,
      rebuilding.then(() => {
        throw new Error("Rebuild completed before replay gate");
      }),
    ]);
    expect(await models.activeGeneration("detail")).toBe(old);
    expect((await app.readmodels.detail.order("one")).document.value).toBe(1);
    const [concurrent] = accepted(await app.sendCommand(input("two", 2)));
    await app.readmodels.waitFor(concurrent!.position, ["detail"]);
    expect((await app.readmodels.detail.order("two")).document.value).toBe(20);
    release.resolve();
    const result = await rebuilding;
    expect(result.swapped).toBe(false);
    expect(await models.pendingGeneration("detail")).toBe(result.generation);
    expect(await models.activeGeneration("detail")).toBe(old);
    expect(
      (
        await models.findOne(
          "detail",
          "order",
          { _id: "one" },
          { generation: result.generation },
        )
      )?.value,
    ).toBe(10);
    expect(
      await models.checkpoint("detail", { generation: result.generation }),
    ).toEqual(concurrent!.position);
    expect(notifications).toEqual([["two"]]);
    const [beforeSwap] = accepted(await app.sendCommand(input("three", 3)));
    await app.readmodels.waitFor(beforeSwap!.position, ["detail"]);
    await app.swapRebuild("detail");
    expect((await app.readmodels.detail.order("one")).document.value).toBe(10);
    expect((await app.readmodels.detail.order("three")).document.value).toBe(
      30,
    );
    expect(await models.checkpoint("detail")).toEqual(beforeSwap!.position);
    expect(await models.pendingGeneration("detail")).toBeNull();
    await expect(
      models.find("detail", "order", {}, { generation: old }),
    ).rejects.toThrow(/generation/i);
    // Live processing moves to the swapped-in generation.
    const [afterSwap] = accepted(await app.sendCommand(input("four", 4)));
    await app.readmodels.waitFor(afterSwap!.position, ["detail"]);
    expect((await app.readmodels.detail.order("four")).document.value).toBe(40);
  } finally {
    release.resolve();
    await subscription?.stop();
    await app.stop();
  }
}, 10000);

test("an instance that regains a Readmodel Lease resumes from the stored Checkpoint, not the one it last committed", async () => {
  const config = base(),
    models = createMemoryReadmodelStore();
  const app = () =>
    createApplication({
      ...config,
      readmodels: {
        detail: {
          store: models,
          models: {
            order: {
              schema: {
                type: "object",
                properties: { count: { type: "integer" } },
              },
              reactions: {
                "sales.order.recorded": (_e: any, vm: any) =>
                  vm.set("count", (vm.get("count") ?? 0) + 1),
              },
            },
          },
        },
      },
    });
  const first = app(),
    second = app();
  const record = async (instance: ReturnType<typeof app>) => {
    const [event] = accepted(await instance.sendCommand(input("one", 1)));
    await instance.readmodels.waitFor(event!.position, ["detail"]);
  };
  try {
    await first.start();
    await record(first);
    await first.stop();
    await second.start();
    await record(second);
    await record(second);
    await second.stop();
    await first.start();
    await record(first);
    expect((await first.readmodels.detail.order("one")).document.count).toBe(4);
  } finally {
    await first.stop();
    await second.stop();
  }
});

test("a Readmodel batch reads the documents its Events name by Stream id in one store call", async () => {
  const config = base(),
    models = createMemoryReadmodelStore();
  let finds = 0,
    findOnes = 0;
  const counted = {
    ...models,
    find: (...args: Parameters<typeof models.find>) => {
      finds++;
      return models.find(...args);
    },
    findOne: (...args: Parameters<typeof models.findOne>) => {
      findOnes++;
      return models.findOne(...args);
    },
  };
  const app = createApplication({
    ...config,
    readmodels: {
      detail: {
        store: counted,
        models: {
          order: {
            schema: {
              type: "object",
              properties: { count: { type: "integer" } },
            },
            reactions: {
              "sales.order.recorded": (_e: any, vm: any) =>
                vm.set("count", (vm.get("count") ?? 0) + 1),
            },
          },
        },
      },
    },
  });
  try {
    await app.start();
    const outcomes = await Promise.all(
      Array.from({ length: 20 }, (_, i) =>
        app.sendCommand(input(`order-${i % 10}`, i)),
      ),
    );
    for (const outcome of outcomes)
      await app.readmodels.waitFor(accepted(outcome)[0]!.position, ["detail"]);
    expect(findOnes).toBe(0);
    expect(finds).toBeLessThanOrEqual(20);
    expect((await app.readmodels.detail.order("order-3")).document.count).toBe(
      2,
    );
  } finally {
    await app.stop();
  }
});

test("a Saga takes Events in batches, each seeing the states earlier ones wrote, and still halts on the exact poison Event", async () => {
  const config = base();
  const before = createApplication(config);
  await before.start();
  for (const value of [1, 2, 3])
    accepted(await before.sendCommand(input("one", value)));
  await before.stop();
  const app = createApplication({
    ...config,
    sagas: {
      sum: {
        // The first three Events are recorded before the Saga starts.
        from: "beginning",
        reactions: {
          "sales.order.recorded": (event: any, saga: any) => {
            if (event.payload.value === 5) throw new Error("poison");
            saga.set("total", (saga.get("total") ?? 0) + event.payload.value);
          },
        },
      },
    },
  });
  await app.start();
  try {
    await expect
      .poll(() => config.store.loadSaga("saga:sum", "one"))
      .toMatchObject({ total: 6 });
    const [poison] = accepted(await app.sendCommand(input("one", 5)));
    await expect
      .poll(
        async () =>
          (await app.consumerStatus()).find((s) => s.group === "saga:sum")
            ?.state,
        { timeout: 7000 },
      )
      .toBe("halted");
    expect(
      (await app.consumerStatus()).find((s) => s.group === "saga:sum")?.eventId,
    ).toBe(poison!.id);
    expect(await config.store.loadSaga("saga:sum", "one")).toMatchObject({
      total: 6,
    });
  } finally {
    await app.stop();
  }
});

test("status marks a Saga group no running replica declares, and retire is refused while one does", async () => {
  // Replicas list their groups on the renewal interval, a third of the Lease duration.
  const config = { ...base(), leaseDuration: 300 };
  const notice = {
    reactions: { "sales.order.recorded": () => {} },
  };
  const before = createApplication({
    ...config,
    sagas: { notice, kept: notice },
  });
  const after = createApplication({ ...config, sagas: { kept: notice } });
  const state = async (group: string) =>
    (await after.consumerStatus()).find((s) => s.group === group)?.state;
  await before.start();
  await after.start();
  try {
    accepted(await before.sendCommand(input("one", 1)));
    const instances = async () =>
      (await config.store.consumers()).filter(
        (s) => s.group.startsWith("instance:") && s.groups,
      ).length;
    await expect.poll(instances).toBe(2);
    expect(await state("saga:notice")).toBe("running");
    await expect(after.retireConsumer("saga:notice")).rejects.toThrow(
      "still declared",
    );
    await before.stop();
    await expect.poll(() => state("saga:notice")).toBe("undeclared");
    expect(await state("saga:kept")).toBe("running");
    await expect(after.retireConsumer("saga:kept")).rejects.toThrow(
      "still declared",
    );
    await after.retireConsumer("saga:notice");
    expect(await state("saga:notice")).toBeUndefined();
  } finally {
    await before.stop();
    await after.stop();
  }
});

test("a Readmodel group waiting on a group it reads resumes when that group commits, not at the next poll", async () => {
  const models = createMemoryReadmodelStore();
  const value = {
    schema: { type: "object", properties: { value: { type: "integer" } } },
  };
  const app = createApplication({
    ...base(),
    pollInterval: 60000,
    readmodels: {
      source: {
        store: models,
        models: {
          order: {
            ...value,
            reactions: {
              "sales.order.recorded": async (e: any, vm: any) => {
                // Slower than the derived group's first look, so it has to wait.
                await new Promise((resolve) => setTimeout(resolve, 50));
                vm.set("value", e.payload.value);
              },
            },
          },
        },
      },
      derived: {
        store: models,
        reads: ["source"],
        models: {
          order: {
            ...value,
            reactions: {
              "sales.order.recorded": (e: any, vm: any) =>
                vm.set("value", e.payload.value),
            },
          },
        },
      },
    },
  });
  await app.start();
  try {
    const [event] = accepted(await app.sendCommand(input("one", 7)));
    await expect
      .poll(() => models.checkpoint("derived"), { timeout: 2000 })
      .toEqual(event!.position);
  } finally {
    await app.stop();
  }
});
