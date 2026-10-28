import { expect, test } from "vitest";
import { createApplication } from "../src/runtime/index.js";
import { compile, only } from "../src/authoring/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import { createCommand } from "../src/id.js";

const until = async (check: () => boolean) => {
  for (let i = 0; i < 200 && !check(); i++)
    await new Promise((resolve) => setTimeout(resolve, 5));
  expect(check()).toBe(true);
};
const account = compile("bank", "account", {
  commands: {
    open: [
      only.ifNotExists(),
      async (command: any, state: any, app: any) => {
        await app.getServices().registry.verify(command.payload.owner);
        state.apply.opened(command.payload);
      },
    ],
  },
  events: { opened: () => {} },
});
const base = () => ({
  name: "services",
  store: createInMemoryEventStore(),
  transport: createInMemoryTransport(),
  pollInterval: 5,
});

// A Readmodel whose Reaction records the owner it sees and which services it can reach.
const ownerReadmodel = (
  configure: (services: Record<string, unknown>) => Record<string, unknown>,
) => {
  const directory = { current: "mutable owner" };
  const services = {
    registry: { verify: async () => {} },
    directory: { owner: () => directory.current },
  };
  const application = createApplication({
    ...base(),
    aggregates: [account],
    services,
    ...configure(services),
    readmodels: {
      bank: {
        store: createMemoryReadmodelStore(),
        models: {
          accounts: {
            schema: {
              type: "object",
              properties: {
                owner: { type: "string" },
                services: { type: "string" },
              },
            },
            reactions: {
              "bank.account.opened": (event: any, view: any, app: any) => {
                const reached = app.getServices();
                view.set(
                  "owner",
                  reached.directory?.owner() ??
                    reached.owner?.(event.payload.owner) ??
                    event.payload.owner,
                );
                view.set("services", Object.keys(reached).sort().join(","));
              },
            },
          },
        },
      },
    },
  } as any);
  /** The document after live processing, then after a rebuild once the directory changed. */
  const liveThenRebuilt = async () => {
    try {
      await application.start();
      const opened = await application.sendCommand({
        stream: { context: "bank", aggregate: "account", id: "a-1" },
        name: "open",
        payload: { owner: "ada" },
      });
      await application.readmodels.waitFor(opened.events[0]!.position, ["bank"]);
      const live = (await application.readmodels.bank.accounts("a-1")).document;
      directory.current = "changed after the Event";
      await application.stop();
      await application.rebuild("bank");
      return [live, (await application.readmodels.bank.accounts("a-1")).document];
    } finally {
      await application.close();
    }
  };
  return liveThenRebuilt;
};

test.each(["borrowed", "managed"] as const)(
  "without readmodelServices, Readmodels keep the %s Application services, live and on rebuild",
  async (supply) => {
    const [live, rebuilt] = await ownerReadmodel((services) =>
      supply === "managed"
        ? { services: undefined, setupServices: async () => services }
        : {},
    )();
    expect(live).toMatchObject({ owner: "mutable owner", services: "directory,registry" });
    // Legacy access keeps its replay risk: the rebuild reads the changed directory.
    expect(rebuilt).toMatchObject({ owner: "changed after the Event" });
  },
);

test.each([
  { readmodelServices: {}, owner: "ada", services: "" },
  {
    readmodelServices: { owner: (name: string) => name.toUpperCase() },
    owner: "ADA",
    services: "owner",
  },
])(
  "readmodelServices $services replaces Application services in Readmodels, live and on rebuild",
  async ({ readmodelServices, ...expected }) => {
    const [live, rebuilt] = await ownerReadmodel(() => ({ readmodelServices }))();
    expect(live).toMatchObject(expected);
    expect(rebuilt).toMatchObject(expected);
  },
);

test("Readmodel helpers are read-only, so a Reaction cannot keep state in them", async () => {
  const helpers = { seen: 0 };
  const application = createApplication({
    ...base(),
    aggregates: [account],
    services: { registry: { verify: async () => {} } },
    readmodelServices: helpers,
    readmodels: {
      bank: {
        store: createMemoryReadmodelStore(),
        models: {
          accounts: {
            schema: { type: "object", properties: { owner: { type: "string" } } },
            reactions: {
              "bank.account.opened": (event: any, view: any, app: any) => {
                expect(() => app.getServices().seen++).toThrow(
                  "Readmodel helpers are read-only",
                );
                expect(() => delete app.getServices().seen).toThrow(TypeError);
                view.set("owner", event.payload.owner);
              },
            },
          },
        },
      },
    },
  } as any);
  try {
    await application.start();
    const opened = await application.sendCommand({
      stream: { context: "bank", aggregate: "account", id: "a-1" },
      name: "open",
      payload: { owner: "ada" },
    });
    await application.readmodels.waitFor(opened.events[0]!.position, ["bank"]);
    expect((await application.readmodels.bank.accounts("a-1")).document).toMatchObject({
      owner: "ada",
    });
    expect(helpers).toEqual({ seen: 0 });
  } finally {
    await application.close();
  }
});

test("setupServices runs once before plugins, and every context reads its services", async () => {
  const log: string[] = [];
  let setups = 0;
  const application = createApplication({
    ...base(),
    aggregates: [account],
    readmodels: {
      bank: {
        store: createMemoryReadmodelStore(),
        models: {
          accounts: {
            schema: {
              type: "object",
              properties: { owner: { type: "string" } },
            },
            reactions: {
              "bank.account.opened": (event: any, view: any, app: any) => {
                app
                  .getServices()
                  .audit.push(`readmodel ${event.payload.owner}`);
                view.set("owner", event.payload.owner);
              },
            },
          },
        },
      },
    },
    sagas: {
      welcome: {
        reactions: {
          "bank.account.opened": (event: any, _state: any, app: any) => {
            app.getServices().audit.push(`saga ${event.payload.owner}`);
            app.queue("welcome", { owner: event.payload.owner });
          },
        },
      },
    },
    workers: {
      welcome: {
        handles: ["welcome"],
        execute: (message: any, ctx: any) => {
          ctx.services.audit.push(`worker ${message.payload.owner}`);
        },
      },
    },
    plugins: [(ctx) => void log.push(`plugin ${ctx.services.audit.length}`)],
    async setupServices({ resources }) {
      setups++;
      await new Promise((resolve) => setTimeout(resolve, 5));
      log.push("setup");
      resources.defer(() => void log.push("cleanup"));
      return {
        audit: [] as string[],
        registry: {
          verify: async (owner: string) => void log.push(`verify ${owner}`),
        },
      };
    },
  });
  expect(() => application.getServices()).toThrow(/not ready/);
  await Promise.all([application.initialize(), application.initialize()]);
  expect(setups).toBe(1);
  await application.start();
  expect(log).toEqual(["setup", "plugin 0"]);
  const outcome = await application.sendCommand({
    stream: { context: "bank", aggregate: "account", id: "a-1" },
    name: "open",
    payload: { owner: "ada" },
  });
  expect(outcome.status).toBe("accepted");
  const audit = application.getServices().audit as string[];
  await until(() => audit.length === 3);
  expect(audit.sort()).toEqual(["readmodel ada", "saga ada", "worker ada"]);
  expect(log).toContain("verify ada");
  await application.close();
  expect(log.at(-1)).toBe("cleanup");
  expect(setups).toBe(1);
});

test("legacyApp adds old accessors to Command steps and Reactions; built-in members win", async () => {
  const reached: string[] = [];
  const crud = { reached: (where: string) => void reached.push(where) };
  const application = createApplication({
    ...base(),
    services: { crud },
    legacyApp: (app) => ({
      getCrud: () => app.getServices().crud,
      getServices: () => ({ crud: { reached: () => reached.push("overridden") } }),
    }),
    aggregates: [
      compile("bank", "account", {
        commands: {
          open: (command: any, state: any, app: any) => {
            app.getCrud().reached("command");
            app.getServices().crud.reached("command services");
            state.apply.opened(command.payload);
          },
        },
        events: { opened: () => {} },
      }),
    ],
    readmodels: {
      bank: {
        store: createMemoryReadmodelStore(),
        models: {
          accounts: {
            schema: { type: "object", properties: {} },
            reactions: {
              "bank.account.opened": (_event: any, _view: any, app: any) =>
                app.getCrud().reached("readmodel"),
            },
          },
        },
      },
    },
    sagas: {
      welcome: {
        reactions: {
          "bank.account.opened": (_event: any, _state: any, app: any) =>
            app.getCrud().reached("saga"),
        },
      },
    },
  } as any);
  try {
    await application.start();
    await application.sendCommand({
      stream: { context: "bank", aggregate: "account", id: "a-1" },
      name: "open",
      payload: {},
    });
    await until(() => reached.length === 4);
    expect(reached.sort()).toEqual(["command", "command services", "readmodel", "saga"]);
  } finally {
    await application.close();
  }
});

test("a failed setup cleans up what it acquired, keeps both failures, and is not retried", async () => {
  const cleaned: string[] = [];
  let setups = 0;
  const application = createApplication({
    ...base(),
    async setupServices({ resources }) {
      setups++;
      resources.adopt("pool", (name) => void cleaned.push(name));
      resources.defer(() => {
        cleaned.push("client");
        throw new Error("client close failed");
      });
      throw new Error("cache unreachable");
    },
  });
  const failure = await application.initialize().catch((error) => error);
  expect(failure).toBeInstanceOf(AggregateError);
  expect(failure.errors.map((error: Error) => error.message)).toEqual([
    "cache unreachable",
    "client close failed",
  ]);
  // Last acquired, first cleaned; a failing cleanup does not skip the rest.
  expect(cleaned).toEqual(["client", "pool"]);
  await expect(application.start()).rejects.toBe(failure);
  expect(setups).toBe(1);
  await application.close();
  expect(cleaned).toEqual(["client", "pool"]);
});

test("close is terminal and idempotent: shutdown hooks still see services, cleanup runs after them", async () => {
  const log: string[] = [];
  const store = createInMemoryEventStore();
  let storeCloses = 0;
  const close = store.close.bind(store);
  store.close = async () => {
    storeCloses++;
    log.push("store");
    await close();
  };
  const application = createApplication({
    ...base(),
    store,
    plugins: [
      (ctx) =>
        ctx.on("shutdown", () => void log.push(`hook ${ctx.services.name}`)),
    ],
    async setupServices({ resources }) {
      resources.defer(async () => void log.push("first acquired"));
      resources.defer(async () => {
        log.push("second acquired");
        throw new Error("second close failed");
      });
      return { name: "mail" };
    },
  });
  await application.start();
  const first = application.close();
  expect(application.close()).toBe(first);
  await expect(first).rejects.toThrow("second close failed");
  expect(log).toEqual([
    "hook mail",
    "second acquired",
    "first acquired",
    "store",
  ]);
  expect(storeCloses).toBe(1);
  expect(() => application.getServices()).toThrow(/closed/);
  await expect(application.start()).rejects.toThrow(/closed/);
  await expect(application.initialize()).rejects.toThrow(/closed/);
});

test("close during setup waits for it and cleans up what it acquired", async () => {
  const log: string[] = [];
  let release!: () => void;
  const gate = new Promise<void>((resolve) => (release = resolve));
  const application = createApplication({
    ...base(),
    async setupServices({ resources }) {
      await gate;
      resources.defer(() => void log.push("cleanup"));
      return {};
    },
  });
  const initializing = application.initialize();
  await new Promise((resolve) => setTimeout(resolve, 5));
  const closing = application.close();
  release();
  await initializing;
  await closing;
  expect(log).toEqual(["cleanup"]);
  await expect(application.start()).rejects.toThrow(/closed/);
});

test("close while checks run before setup: setup never starts", async () => {
  let setups = 0;
  const application = createApplication({
    ...base(),
    setupServices: () => ({ count: ++setups }),
  });
  const initializing = application.initialize();
  await application.close();
  await expect(initializing).rejects.toThrow(/closed/);
  expect(setups).toBe(0);
});

test("services stay borrowed and available; both forms together are refused", async () => {
  const services = { name: "direct" };
  const application = createApplication({ ...base(), services });
  expect(application.getServices()).toBe(services);
  await application.close();
  expect(application.getServices()).toBe(services);
  expect(() =>
    createApplication({
      ...base(),
      services,
      setupServices: () => services,
    } as any),
  ).toThrow("Configure services or setupServices, not both");
  const invalid = createApplication({
    ...base(),
    setupServices: () => undefined as any,
  });
  await expect(invalid.initialize()).rejects.toThrow(
    "setupServices must return the services object",
  );
  await invalid.close();
});

test.each([[null], [[]], ["helpers"], [() => ({})]])(
  "invalid Readmodel service maps are rejected at configuration (%s)",
  (readmodelServices) => {
    expect(() => createApplication({ ...base(), readmodelServices } as any)).toThrow(
      "readmodelServices must be an object of Readmodel helpers",
    );
  },
);

test("close drains a running rebuild and a direct kernel.handle() before cleaning up services", async () => {
  const log: string[] = [];
  let rebuilding = false;
  const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
  const application = createApplication({
    ...base(),
    shutdownTimeout: 1000,
    aggregates: [account],
    readmodels: {
      bank: {
        store: createMemoryReadmodelStore(),
        models: {
          accounts: {
            schema: {
              type: "object",
              properties: { owner: { type: "string" } },
            },
            reactions: {
              "bank.account.opened": async (event: any, view: any, app: any) => {
                if (rebuilding) {
                  await sleep(30);
                  app.getServices().registry.seen(event.payload.owner);
                }
                view.set("owner", event.payload.owner);
              },
            },
          },
        },
      },
    },
    async setupServices({ resources }) {
      resources.defer(() => void log.push("cleanup"));
      return {
        registry: {
          verify: async (owner: string) => {
            await sleep(30);
            log.push(`verify ${owner}`);
          },
          seen: (owner: string) => void log.push(`rebuilt ${owner}`),
        },
      };
    },
  });
  await application.start();
  const opened = await application.sendCommand({
    stream: { context: "bank", aggregate: "account", id: "a-1" },
    name: "open",
    payload: { owner: "ada" },
  });
  await application.readmodels.waitFor(opened.events[0]!.position, ["bank"]);
  rebuilding = true;
  log.length = 0;
  const rebuild = application.rebuild("bank");
  const handled = application.kernel.handle(
    createCommand({
      stream: { context: "bank", aggregate: "account", id: "a-2" },
      name: "open",
      payload: { owner: "grace" },
    }),
  );
  await sleep(5);
  await application.close();
  expect(log.at(-1)).toBe("cleanup");
  expect(log.sort()).toEqual(["cleanup", "rebuilt ada", "verify grace"]);
  expect((await handled).status).toBe("accepted");
  // A rebuild stops between batches; its generation resumes from the checkpoint.
  await expect(rebuild).rejects.toThrow("rebuild stopped");
});
