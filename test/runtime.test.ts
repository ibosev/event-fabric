import { expect, test, vi } from "vitest";
import { createApplication, permanent } from "../src/runtime/index.js";
import { compile, only } from "../src/authoring/index.js";
import { createCommand } from "../src/id.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import {
  createInMemoryTransport,
  createInMemoryBroker,
} from "../src/transport/memory.js";
test("a Command commits, a Readmodel catches up, and a Saga queues a Worker effect", async () => {
  const deliveries: string[] = [],
    titles: string[] = [];
  const aggregate = compile("sales", "order", {
    initialState: {},
    commands: {
      create: [
        only.ifNotExists(),
        (_: any, state: any) => state.apply.created({ title: "first" }),
      ],
    },
    events: {
      created: (event: any, state: any) =>
        state.set("title", event.payload.title),
    },
  });
  const application = createApplication({
    name: "example",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [aggregate],
    pollInterval: 5,
    readmodels: {
      detail: {
        store: createMemoryReadmodelStore(),
        models: {
          order: {
            schema: {
              type: "object",
              properties: { title: { type: "string" } },
            },
            reactions: {
              "sales.order.created": (event: any, vm: any) =>
                vm.set("title", event.payload.title),
            },
          },
        },
      },
    },
    sagas: {
      notice: {
        reactions: {
          "sales.order.created": (event: any, saga: any, app: any) => {
            saga.set("seen", true);
            app.queue("mail", { title: event.payload.title });
          },
        },
      },
    },
    workers: {
      mail: {
        handles: ["mail"],
        execute: async (_message, ctx) => {
          deliveries.push(ctx.idempotencyKey);
          // Workers read Readmodels as Tasks do; they may lag the Message's cause.
          for (let read = 0; read < 100 && !titles.length; read++) {
            const { document } = await ctx.readmodels.detail.order("one");
            if (document) titles.push(document.title);
            else await new Promise((resolve) => setTimeout(resolve, 10));
          }
        },
      },
    },
  });
  await application.start();
  try {
    const outcome = await application.sendCommand({
      stream: { context: "sales", aggregate: "order", id: "one" },
      name: "create",
    });
    expect(outcome.status).toBe("accepted");
    if (outcome.status !== "accepted") throw new Error("Expected acceptance");
    const result = await application.readmodels.detail.order("one", {
      minPosition: outcome.events[0]!.position,
    });
    expect(result.document).toMatchObject({ _id: "one", title: "first" });
    await expect.poll(() => deliveries.length).toBe(1);
    expect(deliveries[0]).toContain("notice");
    await expect.poll(() => titles).toEqual(["first"]);
  } finally {
    await application.stop();
  }
});
test("startup rejects a declared cross-group dependency cycle", async () => {
  const store = createMemoryReadmodelStore();
  const application = createApplication({
    name: "bad",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    readmodels: {
      a: { store, models: {}, reads: ["b"] },
      b: { store, models: {}, reads: ["a"] },
    },
  });
  await expect(application.start()).rejects.toThrow(/cycle/i);
});

test("run starts the Application, closes it on its signal and resolves the exit code", async () => {
  const application = createApplication({
    name: "entry",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
  });
  const controller = new AbortController();
  const running = application.run({ signal: controller.signal });
  await vi.waitFor(() => expect(process.listenerCount("SIGTERM")).toBeGreaterThan(0));
  const listeners = process.listenerCount("SIGTERM");
  controller.abort();
  expect(await running).toBe(0);
  expect(process.listenerCount("SIGTERM")).toBe(listeners - 1);
  await expect(application.start()).rejects.toThrow("closed");
});

test("start opens a lazy Transport's connection; initialize does not", async () => {
  const connect = vi.fn(async () => {});
  const application = createApplication({
    name: "lazy",
    store: createInMemoryEventStore(),
    transport: { ...createInMemoryTransport(), connect },
  });
  await application.initialize();
  expect(connect).not.toHaveBeenCalled();
  await application.start();
  expect(connect).toHaveBeenCalledTimes(1);
  await application.close();
});

test("Readmodel Reactions reject every Command and queued Message path", async () => {
  const errors: string[] = [];
  const application = createApplication({
    name: "read-only",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    pollInterval: 5,
    aggregates: [
      compile("c", "a", {
        commands: { add: (_: any, state: any) => state.apply.added({}) },
        events: { added: () => {} },
      }),
    ],
    readmodels: {
      g: {
        store: createMemoryReadmodelStore(),
        models: {
          m: {
            schema: { type: "object", properties: {} },
            reactions: {
              "c.a.added": (_: any, _vm: any, app: any) => {
                for (const effect of [
                  () =>
                    app.tell({
                      stream: { context: "c", aggregate: "a", id: "x" },
                      name: "add",
                    }),
                  () => app.tell().c.a("x").add({}),
                  () => app.getDomain().c.a("x").add({}),
                  () => app.queue("mail", {}),
                ]) {
                  try {
                    effect();
                    errors.push("allowed");
                  } catch (error) {
                    errors.push((error as Error).message);
                  }
                }
              },
            },
          },
        },
      },
    },
  });
  await application.start();
  try {
    await application.sendCommand({
      stream: { context: "c", aggregate: "a", id: "x" },
      name: "add",
    });
    await expect.poll(() => errors.length).toBe(4);
    expect(
      errors.every((error) => /Readmodel Reactions cannot/.test(error)),
    ).toBe(true);
  } finally {
    await application.close();
  }
});

test("Application startup resolves shared and group named Readmodel schemas", async () => {
  const store = createMemoryReadmodelStore();
  const application = createApplication({
    name: "named-schemas",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    schemas: { "/shared/Name": { type: "string" } },
    readmodels: {
      g: {
        store,
        schemas: { "/group/Age": { type: "integer" } },
        models: {
          person: {
            schema: {
              type: "object",
              properties: {
                name: { $ref: "/shared/Name" },
                age: { $ref: "/group/Age" },
              },
            },
            reactions: {},
          },
        },
      },
    },
  });
  try {
    await application.initialize();
    await store.fence("g", 1);
    await store.batch("g", 1, { transactionId: "1", sequence: "1" }, [
      { model: "person", id: "a", document: { name: "Ada", age: 36 } },
    ]);
    expect(await application.readmodels.g.person.findOne({})).toMatchObject({
      name: "Ada",
      age: 36,
    });
  } finally {
    await application.close();
  }
});

test("awaitCorrelated uses one overall deadline and scans the durable log when broadcasts are lost", async () => {
  vi.useFakeTimers();
  const store = createInMemoryEventStore(),
    transport = createInMemoryTransport();
  const app = createApplication({ name: "correlated", store, transport });
  await transport.handle(
    "c",
    Array.from({ length: 128 }, (_, i) => i),
    async (command) => {
      await new Promise((resolve) => setTimeout(resolve, 40));
      await store.appendToStream(command.stream, 0, [
        {
          id: "done",
          name: "done",
          payload: {},
          metadata: {},
          schemaVersion: 1,
          timestamp: new Date().toISOString(),
          commandId: command.id,
          correlationId: command.correlationId,
        },
      ]);
      return { status: "accepted", events: [] };
    },
  );
  let result: any;
  try {
    const waiting = app
      .awaitCorrelated(
        { stream: { context: "c", aggregate: "a", id: "x" }, name: "start" },
        "c.a.done",
        { deadline: 60 },
      )
      .then((value) => (result = value));
    await vi.advanceTimersByTimeAsync(60);
    expect(result).toMatchObject({ status: "accepted", event: { id: "done" } });
    await waiting;
  } finally {
    await app.close();
    vi.useRealTimers();
  }
});

test("Outbox replacement between selection and dispatch never sends the removed pending Message", async () => {
  const store = createInMemoryEventStore(),
    delivered: string[] = [];
  const entry = (id: string) => ({
    id,
    kind: "message" as const,
    replaceKey: "reminder",
    message: {
      id,
      name: "mail",
      payload: {},
      metadata: {},
      schemaVersion: 1,
      timestamp: "2026-01-01T00:00:00.000Z",
      correlationId: "chain",
      idempotencyKey: id,
    },
  });
  await store.recordOutbox([entry("old")]);
  let replaced = false;
  const app = createApplication({
    name: "replace",
    transport: createInMemoryTransport(),
    pollInterval: 5,
    store: {
      ...store,
      async pendingOutbox(limit) {
        const rows = await store.pendingOutbox(limit);
        if (!replaced && rows.some((row) => row.id === "old")) {
          replaced = true;
          await store.recordOutbox([entry("new")]);
        }
        return rows;
      },
    },
    workers: {
      mail: {
        handles: ["mail"],
        execute(message) {
          delivered.push(message.id);
        },
      },
    },
  });
  await app.start({ only: ["outbox:*", "worker:*"] });
  try {
    await expect.poll(() => delivered).toEqual(["new"]);
  } finally {
    await app.close();
  }
});

test("startup refuses unknown component selectors", async () => {
  for (const selector of [
    "readmodels:typo",
    "commands:typo",
    "commands:selectors/typo",
    "saga:typo",
    "gateway:typo",
    "worker:typo",
    "outbox:typo",
    "relay:typo",
  ]) {
    const app = createApplication({
      name: "selectors",
      store: createInMemoryEventStore(),
      transport: createInMemoryTransport(),
    });
    try {
      await expect(app.start({ only: [selector] })).rejects.toThrow(
        /Unknown component selector/,
      );
    } finally {
      await app.close();
    }
  }
});

test("a synchronous Command Step exceeding its handler limit records no Events", async () => {
  const store = createInMemoryEventStore();
  const app = createApplication({
    name: "cpu-timeout",
    store,
    transport: createInMemoryTransport(),
    commandTimeout: 5,
    aggregates: [
      compile("c", "a", {
        commands: {
          add: (_: any, state: any) => {
            const end = performance.now() + 15;
            while (performance.now() < end) {}
            state.apply.added({});
          },
        },
        events: { added: () => {} },
      }),
    ],
  });
  await app.start({ only: ["commands:*"] });
  try {
    expect(
      (
        await app.sendCommand({
          stream: { context: "c", aggregate: "a", id: "x" },
          name: "add",
        })
      ).status,
    ).toBe("failed");
    expect(await store.readAll()).toEqual([]);
  } finally {
    await app.close();
  }
});

test("a synchronous Worker exceeding its attempt limit is durably dead-lettered", async () => {
  const store = createInMemoryEventStore(),
    transport = createInMemoryTransport();
  const app = createApplication({
    name: "cpu-worker",
    store,
    transport,
    pollInterval: 5,
    workers: {
      work: {
        handles: ["work"],
        timeout: 5,
        retry: { attempts: 1 },
        execute() {
          const end = performance.now() + 15;
          while (performance.now() < end) {}
        },
      },
    },
  });
  await app.start({ only: ["worker:*"] });
  try {
    await transport.enqueue("cpu-worker.workers.work", {
      id: "cpu",
      name: "work",
      payload: {},
      metadata: {},
      schemaVersion: 1,
      timestamp: new Date().toISOString(),
      correlationId: "cpu",
      idempotencyKey: "cpu",
    });
    await expect
      .poll(async () => await store.deadLetters())
      .toEqual([
        expect.objectContaining({
          kind: "worker",
          error: "Worker attempt timed out",
          attempts: 1,
        }),
      ]);
  } finally {
    await app.close();
  }
});

test("an exact Readmodel skip fences a batch already in progress before recording its audit", async () => {
  const store = createInMemoryEventStore(),
    models = createMemoryReadmodelStore();
  let release!: () => void;
  const gate = new Promise<void>((resolve) => (release = resolve));
  const groupStore = {
    ...models,
    async batch(...args: Parameters<typeof models.batch>) {
      if (args[3].some((change) => change.id === "late")) await gate;
      return models.batch(...args);
    },
  };
  const app = createApplication({
    name: "skip-race",
    store,
    transport: createInMemoryTransport(),
    readmodels: {
      g: {
        store: groupStore,
        models: {
          m: {
            schema: { type: "object", properties: { n: { type: "integer" } } },
            reactions: {},
          },
        },
      },
    },
  });
  await app.initialize();
  const [event] = await store.appendToStream(
    { context: "c", aggregate: "a", id: "x" },
    0,
    [
      {
        id: "skip-event",
        name: "added",
        payload: {},
        metadata: {},
        schemaVersion: 1,
        timestamp: new Date().toISOString(),
        commandId: "command",
        correlationId: "command",
      },
    ],
  );
  const lease = (await store.acquireLease("readmodels:g", "old-owner"))!;
  await models.fence("g", lease.epoch);
  await store.setConsumerStatus(
    {
      group: lease.group,
      partition: 0,
      checkpoint: null,
      state: "halted",
      eventId: event!.id,
      position: event!.position,
      attempts: 5,
    },
    lease,
  );
  const oldBatch = groupStore.batch("g", lease.epoch, event!.position, [
    { model: "m", id: "late", document: { n: 99 } },
  ]);
  try {
    await app.skipConsumer("readmodels:g", event!.id, "operator");
    release();
    await expect(oldBatch).rejects.toThrow(/epoch/i);
    expect(await models.find("g", "m")).toEqual([]);
    expect(await models.checkpoint("g")).toEqual(event!.position);
    expect(await store.deadLetters()).toEqual([
      expect.objectContaining({
        kind: "skipped-event",
        eventId: event!.id,
        who: "operator",
      }),
    ]);
  } finally {
    release();
    await oldBatch.catch(() => {});
    await app.close();
  }
});

test("Readmodel writes use the configured async validator without filling defaults or mutating data", async () => {
  const { createValidator } = await import("../src/validation.js");
  const models = createMemoryReadmodelStore();
  const validator = createValidator({
    formats: {
      "allowed-code": {
        async: true,
        validate: async (value: string) => value === "valid",
      },
    },
  });
  const app = createApplication({
    name: "custom-validation",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    validate: validator,
    readmodels: {
      g: {
        store: models,
        models: {
          m: {
            schema: {
              $async: true,
              type: "object",
              required: ["code"],
              properties: {
                code: {
                  type: "string",
                  format: "allowed-code",
                  default: "valid",
                  transform: ["trim"],
                },
              },
            },
            reactions: {},
          },
        },
      },
    },
  });
  try {
    await app.initialize();
    await models.fence("g", 1);
    const write = (document: any) =>
      models.batch("g", 1, { transactionId: "1", sequence: "1" }, [
        { model: "m", id: "x", document },
      ]);
    await expect(write({ code: "invalid" })).rejects.toThrow(/format/i);
    await expect(write({})).rejects.toThrow(/required|mutate/i);
    await expect(write({ code: " valid " })).rejects.toThrow(/mutate/i);
    await write({ code: "valid" });
    expect(await models.findOne("g", "m")).toEqual({ _id: "x", code: "valid" });
  } finally {
    await app.close();
  }
});

test("a healthy standby reports readiness when another replica owns the Consumer group Lease", async () => {
  const store = createInMemoryEventStore(),
    models = createMemoryReadmodelStore(),
    broker = createInMemoryBroker();
  const make = () =>
    createApplication({
      name: "replicas",
      store,
      transport: createInMemoryTransport({ broker }),
      pollInterval: 5,
      readmodels: { g: { store: models, models: {} } },
    });
  const first = make(),
    second = make();
  await first.start({ only: ["readmodels:*"] });
  await second.start({ only: ["readmodels:*"] });
  try {
    await expect
      .poll(async () => [
        (await first.health()).readiness,
        (await second.health()).readiness,
      ])
      .toEqual([true, true]);
  } finally {
    await second.close();
    await first.close();
  }
});

test("Readmodel groups inherit the Application's default store and can override it", async () => {
  const shared = createMemoryReadmodelStore(),
    override = createMemoryReadmodelStore();
  const definition = {
    schema: { type: "object", properties: { n: { type: "integer" } } },
    reactions: {},
  };
  const app = createApplication({
    name: "default-store",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    readmodelStore: shared,
    readmodels: {
      a: { models: { m: definition } },
      b: { store: override, models: { m: definition } },
    },
  });
  try {
    await app.initialize();
    await shared.fence("a", 1);
    await override.fence("b", 1);
    await shared.batch("a", 1, { transactionId: "1", sequence: "1" }, [
      { model: "m", id: "one", document: { n: 1 } },
    ]);
    await override.batch("b", 1, { transactionId: "1", sequence: "1" }, [
      { model: "m", id: "one", document: { n: 2 } },
    ]);
    expect(await app.readmodels.a.m.findOne({})).toMatchObject({ n: 1 });
    expect(await app.readmodels.b.m.findOne({})).toMatchObject({ n: 2 });
  } finally {
    await app.close();
  }
});

test("same-group reads see prior batch changes in the current generation during rebuild", async () => {
  const store = createMemoryReadmodelStore();
  const app = createApplication({
    name: "batch-reads",
    clock: () => new Date("2026-01-01T00:00:00.000Z"),
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    pollInterval: 2,
    aggregates: [
      compile("c", "a", {
        commands: { add: (_: any, state: any) => state.apply.added({}) },
        events: { added: () => {} },
      }),
    ],
    readmodels: {
      g: {
        store,
        models: {
          m: {
            schema: {
              type: "object",
              properties: {
                previous: { type: "integer" },
                priorIds: { type: "array", items: { type: "string" } },
                streamed: { type: "integer" },
                first: { type: ["string", "null"] },
                lookup: { type: ["string", "null"] },
                at: { type: "string", format: "date-time" },
                priorDate: { type: "boolean" },
                guards: { type: "integer" },
              },
            },
            reactions: {
              "c.a.added": async (_event: any, vm: any, app: any) => {
                const api = app.getReadmodels();
                const repo = api.repository("g", "m");
                vm.set("previous", await repo.count());
                vm.set(
                  "priorIds",
                  (await api.g.m.find({}, { sort: { _id: 1 } })).map(
                    (doc: any) => doc._id,
                  ),
                );
                let streamed = 0;
                for await (const _ of repo.stream()) streamed++;
                vm.set("streamed", streamed);
                vm.set(
                  "first",
                  (await repo.findOne({}, { sort: { _id: 1 } }))?._id ?? null,
                );
                vm.set("lookup", (await repo("one")).document?._id ?? null);
                vm.set("at", _event.timestamp);
                vm.set(
                  "priorDate",
                  (await repo.findOne({}))?.at instanceof Date,
                );
                let guards = 0;
                for (const read of [
                  () => repo.native(),
                  () => api.waitFor(_event.position, ["g"]),
                  () => repo.count({}, { minPosition: _event.position }),
                ]) {
                  try {
                    await read();
                  } catch {
                    guards++;
                  }
                }
                vm.set("guards", guards);
              },
            },
          },
        },
      },
    },
  });
  await app.start();
  try {
    for (const id of ["one", "two"]) {
      const outcome = await app.sendCommand({
        stream: { context: "c", aggregate: "a", id },
        name: "add",
      });
      if (outcome.status !== "accepted") throw Error("Expected acceptance");
      await app.readmodels.waitFor(outcome.events[0]!.position, ["g"]);
    }
    const expected = [
      {
        _id: "one",
        previous: 0,
        priorIds: [],
        streamed: 0,
        first: null,
        lookup: null,
        at: new Date("2026-01-01T00:00:00.000Z"),
        priorDate: false,
        guards: 3,
      },
      {
        _id: "two",
        previous: 1,
        priorIds: ["one"],
        streamed: 1,
        first: "one",
        lookup: "one",
        at: new Date("2026-01-01T00:00:00.000Z"),
        priorDate: true,
        guards: 3,
      },
    ];
    expect(await store.find("g", "m", {}, { sort: { _id: 1 } })).toEqual(
      expected,
    );
    await app.rebuild("g");
    expect(await store.find("g", "m", {}, { sort: { _id: 1 } })).toEqual(
      expected,
    );
  } finally {
    await app.close();
  }
});
test("a Command handler's queued Message routes to its Worker, and one without a Worker fails before commit", async () => {
  const store = createInMemoryEventStore();
  const aggregate = compile("sales", "order", {
    initialState: {},
    commands: {
      create: (_: any, state: any, app: any) => {
        state.apply.created({});
        app.queue(state.id === "routed" ? "mail" : "unrouted", {});
      },
    },
    events: { created: () => {} },
  });
  const application = createApplication({
    name: "routing",
    store,
    transport: createInMemoryTransport(),
    aggregates: [aggregate],
    workers: { mailer: { handles: ["mail"], execute: () => {} } },
  });
  await application.initialize();
  const handle = (id: string) =>
    application.kernel.handle(
      createCommand({
        stream: { context: "sales", aggregate: "order", id },
        name: "create",
      }),
    );
  expect((await handle("routed")).status).toBe("accepted");
  expect(await store.pendingOutbox()).toMatchObject([
    { queue: "routing.workers.mailer", message: { name: "mail" } },
  ]);
  const failed = await handle("lost");
  expect(failed).toMatchObject({
    status: "failed",
    error: { message: "Queued Message unrouted has no Worker" },
  });
  expect(
    await store.readStream({
      context: "sales",
      aggregate: "order",
      id: "lost",
    }),
  ).toEqual([]);
});

test("startup refuses a log holding an undeclared Event name, and a retired Event that stays declared starts", async () => {
  const store = createInMemoryEventStore();
  await store.appendToStream({ context: "c", aggregate: "a", id: "x" }, 0, [
    {
      id: "old",
      name: "renamed",
      payload: {},
      metadata: {},
      schemaVersion: 1,
      timestamp: "2026-01-01T00:00:00Z",
      commandId: "cmd",
      correlationId: "cmd",
    },
  ]);
  const app = (events: Record<string, () => void>) =>
    createApplication({
      name: "permanent-names",
      store,
      transport: createInMemoryTransport(),
      aggregates: [compile("c", "a", { commands: {}, events })],
    });
  // The Event was renamed in code: the recorded name is no longer declared.
  await expect(app({ added: () => {} }).start()).rejects.toThrow(
    "Events no Aggregate declares: c.a.renamed",
  );
  // Retiring it means keeping it declared.
  const retired = app({ added: () => {}, renamed: () => {} });
  await retired.start();
  await retired.close();
});

test("commands:<app>/<context> runs only that Context's Command handlers", async () => {
  const broker = createInMemoryBroker();
  const aggregates = ["a", "b"].map((context) =>
    compile(context, "counter", {
      commands: {
        add: (_: any, state: any) => {
          state.apply.added({});
        },
      },
      events: { added: () => {} },
    }),
  );
  // Separate stores, so where Events land shows which instance handled a Command.
  const instances = ["a", "b"].map((context) => {
    const store = createInMemoryEventStore();
    const app = createApplication({
      name: "split",
      store,
      transport: createInMemoryTransport({ broker }),
      aggregates,
    });
    return { context, store, app };
  });
  try {
    for (const { context, app } of instances)
      await app.start({ only: [`commands:split/${context}`] });
    for (const context of ["a", "b"])
      expect(
        (
          await instances[0]!.app.sendCommand({
            stream: { context, aggregate: "counter", id: "1" },
            name: "add",
          })
        ).status,
      ).toBe("accepted");
    for (const { context, store } of instances)
      for (const other of ["a", "b"])
        expect(
          await store.readStream({
            context: other,
            aggregate: "counter",
            id: "1",
          }),
        ).toHaveLength(other === context ? 1 : 0);
  } finally {
    for (const { app } of instances) await app.close();
  }
});

test("replicas starting after a schema change rebuild the Readmodel group once", async () => {
  const store = createInMemoryEventStore(),
    models = createMemoryReadmodelStore(),
    broker = createInMemoryBroker();
  for (let i = 1; i <= 50; i++)
    await store.appendToStream(
      { context: "c", aggregate: "a", id: `${i}` },
      0,
      [
        {
          id: `e${i}`,
          name: "added",
          payload: {},
          metadata: {},
          schemaVersion: 1,
          timestamp: "2026-10-16T00:00:00.000Z",
          commandId: `cmd${i}`,
          correlationId: `cmd${i}`,
        },
      ],
    );
  const make = (properties: Record<string, unknown>) =>
    createApplication({
      name: "replicas",
      store,
      transport: createInMemoryTransport({ broker }),
      pollInterval: 5,
      aggregates: [
        compile("c", "a", { commands: {}, events: { added: () => {} } }),
      ],
      readmodels: {
        g: {
          store: models,
          models: {
            m: {
              schema: { type: "object", properties },
              reactions: {
                "c.a.added": (event: any, vm: any) => vm.set({ n: 1 }),
              },
            },
          },
        },
      },
    });
  const before = make({ n: { type: "integer" } });
  await before.initialize();
  await before.close();
  const created = vi.spyOn(models, "createGeneration");
  const replicas = [0, 1, 2].map(() =>
    make({ n: { type: "integer" }, extra: { type: "string" } }),
  );
  try {
    await Promise.all(
      replicas.map((app) => app.start({ only: ["readmodels:*"] })),
    );
    await expect
      .poll(() => replicas[0]!.readmodels.g.m.count({}).catch(() => -1))
      .toBe(50);
    for (const app of replicas)
      await expect
        .poll(async () => (await app.health()).rebuilding)
        .toEqual([]);
    expect(created).toHaveBeenCalledTimes(1);
  } finally {
    for (const app of replicas) await app.close();
  }
});

test("declarations the runtime would ignore are refused before any Event is processed", async () => {
  const order = compile("sales", "order", {
    commands: { create: (c: any, a: any) => a.apply.created(c.payload) },
    events: { created: () => {} },
  });
  const setTitle = (event: any, view: any) => view.set("title", event.payload.title);
  const model = (extra: object, reactions: unknown[] = [setTitle]) => ({
    schema: { type: "object", properties: { title: { type: "string" } } },
    reactions: { "sales.order.created": reactions },
    ...extra,
  });
  const refused = async (definitions: { readmodel?: object; saga?: object }) => {
    const store = createInMemoryEventStore();
    const application = createApplication({
      name: "legacy",
      store,
      transport: createInMemoryTransport(),
      aggregates: [order],
      ...(definitions.readmodel && {
        readmodels: {
          g: { store: createMemoryReadmodelStore(), models: { m: definitions.readmodel as any } },
        },
      }),
      ...(definitions.saga && { sagas: { s: definitions.saga as any } }),
    });
    const error = await application.start().catch((error: Error) => error.message);
    await application.close();
    return error;
  };
  expect(await refused({ readmodel: model({ identity: { "sales.order.created": "payload.target" } }) })).toBe(
    "Readmodel g.m: identity for sales.order.created must be a function, e.g. (event) => event.payload.id; anything else would fall back to the Stream id",
  );
  expect(await refused({ readmodel: model({}, [{ useAsId: "payload.target" }, setTitle]) })).toMatch(
    "Readmodel g.m: Reaction for sales.order.created has an unsupported item useAsId",
  );
  expect(await refused({ readmodel: model({ query: { "sales.order.created": () => ({}) } }) })).toBe(
    "Readmodel g.m: unsupported key query; return the filter from an identity function instead",
  );
  expect(await refused({ readmodel: model({}, [{ settings: { autoCreate: "no" } }, setTitle]) })).toMatch(
    "unsupported item settings.autoCreate",
  );
  expect(
    await refused({
      saga: { identity: { "sales.order.created": "payload.target" }, reactions: { "sales.order.created": () => {} } },
    }),
  ).toMatch("Saga s: identity for sales.order.created must be a function");
  expect(
    await refused({ saga: { reactions: { "sales.order.created": [{ shouldHandle: true }, () => {}] } } }),
  ).toMatch("Saga s: Reaction for sales.order.created has an unsupported item shouldHandle");
  // A grouped key such as `event:group` is not an Event name.
  expect(
    await refused({ saga: { reactions: { "sales.order.created:audit": () => {} } } }),
  ).toBe("s names unknown Event sales.order.created:audit");
});

test("only.if and the old autoCreate/shouldHandle settings skip the whole Reaction: no document, instance or effect", async () => {
  const deliveries: string[] = [];
  const order = compile("sales", "order", {
    commands: { create: (c: any, a: any) => a.apply.created(c.payload) },
    events: { created: () => {} },
  });
  const handled = (event: any) => event.payload.title !== "skip";
  const setTitle = (event: any, view: any) => view.set("title", event.payload.title);
  const model = (reaction: unknown[]) => ({
    schema: { type: "object", properties: { title: { type: "string" } } },
    reactions: { "sales.order.created": reaction },
  });
  const application = createApplication({
    name: "markers",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [order],
    pollInterval: 5,
    readmodels: {
      g: {
        store: createMemoryReadmodelStore(),
        models: {
          current: model([only.if(handled), setTitle]),
          old: model([{ settings: { shouldHandleEvent: handled } }, setTitle]),
          existing: model([{ settings: { autoCreate: false } }, setTitle]),
          created: model([{ settings: { autoCreate: true } }, setTitle]),
        },
      },
    },
    sagas: {
      notice: {
        reactions: {
          "sales.order.created": [
            { shouldHandle: (event: any, saga: any) => handled(event) && !saga.get("seen") },
            (event: any, saga: any, app: any) => {
              saga.set("seen", true);
              app.queue("mail", { title: event.payload.title });
            },
          ],
        },
      },
    },
    workers: {
      mail: {
        handles: ["mail"],
        execute: async (message: any) => {
          deliveries.push(message.payload.title);
        },
      },
    },
  });
  await application.start();
  try {
    const send = (id: string, title: string) =>
      application.sendCommand({ stream: { context: "sales", aggregate: "order", id }, name: "create", payload: { title } });
    await send("b", "skip");
    const last = await send("a", "keep");
    if (last.status !== "accepted") throw new Error(JSON.stringify(last));
    const read = async (name: "current" | "old" | "existing" | "created", id: string) =>
      (await (application.readmodels.g as any)[name](id, { minPosition: last.events[0]!.position })).document;
    for (const name of ["current", "old"] as const) {
      expect(await read(name, "a")).toMatchObject({ title: "keep" });
      expect(await read(name, "b")).toBeNull();
    }
    expect(await read("existing", "a")).toBeNull();
    expect(await read("created", "b")).toMatchObject({ title: "skip" });
    await expect.poll(() => deliveries).toEqual(["keep"]);
  } finally {
    await application.stop();
  }
});

test("Reaction sets react to the same Event with their own Identities and markers, after the model's own Reactions", async () => {
  const order = compile("sales", "order", {
    commands: { create: (c: any, a: any) => a.apply.created(c.payload) },
    events: { created: () => {} },
  });
  const created = "sales.order.created";
  const model = (extra: object) => ({
    schema: {
      type: "object",
      properties: { title: { type: "string" }, orders: { type: "integer" } },
    },
    reactions: {
      [created]: (event: any, view: any) =>
        view.set("title", event.payload.title),
    },
    ...extra,
  });
  const application = createApplication({
    name: "reaction-sets",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [order],
    pollInterval: 5,
    readmodels: {
      g: {
        store: createMemoryReadmodelStore(),
        models: {
          m: model({
            reactionSets: {
              // Its own document: one per customer.
              customer: {
                identity: { [created]: (event: any) => event.payload.customer },
                reactions: {
                  [created]: (_event: any, view: any) =>
                    view.set("orders", (view.get("orders") ?? 0) + 1),
                },
              },
              // The Stream id, like the model; it sees the document the model's Reaction just created.
              mark: {
                reactions: {
                  [created]: [
                    only.ifExists(),
                    (_event: any, view: any) =>
                      view.set("title", `${view.get("title")}!`),
                  ],
                },
              },
            },
          }),
        },
      },
    },
  });
  await application.start();
  try {
    const send = (id: string, title: string) =>
      application.sendCommand({
        stream: { context: "sales", aggregate: "order", id },
        name: "create",
        payload: { title, customer: "c1" },
      });
    await send("a", "first");
    const last = await send("b", "second");
    if (last.status !== "accepted") throw new Error(JSON.stringify(last));
    const read = async (id: string) =>
      (
        await (application.readmodels.g as any).m(id, {
          minPosition: last.events[0]!.position,
        })
      ).document;
    expect(await read("a")).toEqual({ _id: "a", title: "first!" });
    expect(await read("b")).toEqual({ _id: "b", title: "second!" });
    expect(await read("c1")).toEqual({ _id: "c1", orders: 2 });
  } finally {
    await application.stop();
  }
  const refused = async (model: object) => {
    const application = createApplication({
      name: "reaction-sets-refused",
      store: createInMemoryEventStore(),
      transport: createInMemoryTransport(),
      aggregates: [order],
      readmodels: {
        g: { store: createMemoryReadmodelStore(), models: { m: model as any } },
      },
    });
    const error = await application
      .start()
      .catch((error: Error) => error.message);
    await application.close();
    return error;
  };
  expect(
    await refused(model({ reactionSets: { audit: { reactions: {}, schema: {} } } })),
  ).toBe(
    "Readmodel g.m: Reaction set audit has unsupported key schema; a Reaction set has only identity and reactions",
  );
  expect(
    await refused(
      model({
        reactionSets: { audit: { reactions: { "sales.order.removed": () => {} } } },
      }),
    ),
  ).toBe("g.m:audit names unknown Event sales.order.removed");
  expect(
    await refused(
      model({
        reactionSets: {
          audit: {
            identity: { [created]: "payload.id" },
            reactions: { [created]: () => {} },
          },
        },
      }),
    ),
  ).toMatch(
    "Readmodel g.m:audit: identity for sales.order.created must be a function",
  );
});

test("Reactions folding one document twice in a batch leave its published before and the initial state untouched", async () => {
  const aggregate = compile("sales", "counter", {
    initialState: {},
    commands: {
      bumpTwice: (_: any, state: any) => {
        state.apply.bumped({});
        state.apply.bumped({});
      },
    },
    events: { bumped: () => {} },
  });
  const transport = createInMemoryTransport();
  const initialState = { stats: { count: 0 } };
  const application = createApplication({
    name: "example",
    store: createInMemoryEventStore(),
    transport,
    aggregates: [aggregate],
    pollInterval: 5,
    readmodels: {
      counts: {
        store: createMemoryReadmodelStore(),
        models: {
          counter: {
            initialState,
            schema: {
              type: "object",
              properties: {
                stats: {
                  type: "object",
                  properties: { count: { type: "integer" } },
                },
              },
            },
            reactions: {
              "sales.counter.bumped": (_: any, vm: any) =>
                vm.set("stats.count", vm.get("stats.count") + 1),
            },
          },
        },
      },
    },
  });
  const changes: any[] = [];
  const stop = await transport.subscribe("readmodels", async (n: any) => {
    changes.push(...n.changes);
  });
  await application.start();
  try {
    for (const id of ["one", "one", "two"]) {
      const outcome = await application.sendCommand({
        stream: { context: "sales", aggregate: "counter", id },
        name: "bumpTwice",
      });
      if (outcome.status !== "accepted") throw new Error("Expected acceptance");
      await application.readmodels.counts.counter(id, {
        minPosition: outcome.events.at(-1)!.position,
      });
    }
    expect(
      changes.map((c) => [c.id, c.before?.stats.count, c.after.stats.count]),
    ).toEqual([
      ["one", undefined, 2],
      ["one", 2, 4],
      ["two", undefined, 2],
    ]);
    expect(initialState).toEqual({ stats: { count: 0 } });
  } finally {
    await stop();
    await application.stop();
  }
});

test("stop waits for Outbox deliveries started by the relay's last pass before it returns", async () => {
  const store = createInMemoryEventStore();
  const polled = Promise.withResolvers<void>(),
    gate = Promise.withResolvers<void>();
  let gated = false;
  const pendingOutbox = store.pendingOutbox.bind(store);
  store.pendingOutbox = async (...args) => {
    const rows = await pendingOutbox(...args);
    if (gated && rows.length) {
      polled.resolve();
      await gate.promise;
    }
    return rows;
  };
  let delivered = false;
  const application = createApplication({
    name: "draining",
    store,
    transport: {
      ...createInMemoryTransport(),
      send: async () => {
        await new Promise((resolve) => setTimeout(resolve, 50));
        delivered = true;
        return { status: "accepted", events: [] };
      },
    },
    pollInterval: 5,
  });
  await application.start();
  gated = true;
  await store.recordOutbox([
    {
      id: "late",
      kind: "command",
      message: createCommand({
        stream: { context: "sales", aggregate: "order", id: "1" },
        name: "create",
      }),
    },
  ]);
  application.wake();
  await polled.promise;
  const stopping = application.stop();
  // Stop functions finish first; the relay's pass is still polling when stop looks for deliveries.
  setTimeout(gate.resolve, 20);
  await stopping;
  expect(delivered).toBe(true);
  expect((await store.outboxStats()).pending).toBe(0);
  await application.close();
});

test("a Date in Saga state is stored as its ISO text, as later Events and reloads see it", async () => {
  const seen: unknown[] = [];
  const store = createInMemoryEventStore();
  const application = createApplication({
    name: "dated",
    store,
    transport: createInMemoryTransport(),
    pollInterval: 5,
    aggregates: [
      compile("sales", "order", {
        commands: { touch: (_: any, state: any) => state.apply.touched({}) },
        events: { touched: () => {} },
      }),
    ],
    sagas: {
      reminder: {
        identity: { "sales.order.touched": (event: any) => event.aggregate.id },
        reactions: {
          "sales.order.touched": (_: any, saga: any) => {
            seen.push(saga.get("due"));
            saga.set("due", new Date(0));
            // Within the Reaction, the value is the Date it set.
            seen.push(saga.get("due") instanceof Date);
          },
        },
      },
    },
  });
  await application.start();
  try {
    for (let i = 0; i < 2; i++)
      await application.sendCommand({
        stream: { context: "sales", aggregate: "order", id: "one" },
        name: "touch",
      });
    await expect.poll(() => seen.length).toBe(4);
    expect(seen).toEqual([undefined, true, "1970-01-01T00:00:00.000Z", true]);
    expect(await store.loadSaga("saga:reminder", "one")).toMatchObject({
      due: "1970-01-01T00:00:00.000Z",
    });
  } finally {
    await application.stop();
  }
});

test("a Saga's creation Command without a Stream id gets one from its handler", async () => {
  const store = createInMemoryEventStore();
  const application = createApplication({
    name: "creating",
    store,
    transport: createInMemoryTransport(),
    pollInterval: 5,
    aggregates: [
      compile("sales", "order", {
        commands: { place: (_: any, state: any) => state.apply.placed({}) },
        events: { placed: () => {} },
      }),
      compile("sales", "invoice", {
        commands: { open: (_: any, state: any) => state.apply.opened({}) },
        events: { opened: () => {} },
      }),
    ],
    sagas: {
      invoicing: {
        reactions: {
          "sales.order.placed": (_: any, _saga: any, app: any) =>
            app.getDomain().sales.invoice().open({}),
        },
      },
    },
  });
  await application.start();
  try {
    await application.sendCommand({
      stream: { context: "sales", aggregate: "order", id: "one" },
      name: "place",
    });
    await expect
      .poll(async () =>
        (await store.readAll({ eventNames: ["sales.invoice.opened"] })).map(
          (event) => event.stream.id,
        ),
      )
      .toEqual([expect.stringMatching(/^[0-9a-f-]{36}$/)]);
  } finally {
    await application.stop();
  }
});

test("close lets in-flight work finish its Commands before the Command handlers stop", async () => {
  const store = createInMemoryEventStore(),
    transport = createInMemoryTransport();
  const release = Promise.withResolvers<void>(),
    started = Promise.withResolvers<void>();
  const outcomes: string[] = [];
  const app = createApplication({
    name: "drain",
    store,
    transport,
    pollInterval: 5,
    shutdownTimeout: 2000,
    aggregates: [
      compile("sales", "order", {
        initialState: {},
        commands: { create: (_: any, state: any) => state.apply.created({}) },
        events: { created: () => {} },
      }),
    ],
    workers: {
      work: {
        handles: ["work"],
        async execute(_message, ctx) {
          started.resolve();
          await release.promise;
          const outcome = await ctx.send({
            stream: { context: "sales", aggregate: "order", id: "one" },
            name: "create",
            payload: {},
          });
          outcomes.push(outcome.status);
        },
      },
    },
  });
  await app.start();
  await transport.enqueue("drain.workers.work", {
    id: "w",
    name: "work",
    payload: {},
    metadata: {},
    schemaVersion: 1,
    timestamp: new Date().toISOString(),
    correlationId: "w",
    idempotencyKey: "w",
  });
  await started.promise;
  const closed = app.close();
  // The Worker sends once close() has begun stopping consumers.
  setTimeout(release.resolve, 20);
  await closed;
  expect(outcomes).toEqual(["accepted"]);
});
