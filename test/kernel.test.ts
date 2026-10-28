import { expect, test, vi } from "vitest";
import { compile, only } from "../src/authoring/index.js";
import {
  AggregateState,
  createKernel,
  loadAggregate,
} from "../src/kernel/index.js";
import { createCommand } from "../src/id.js";
import { legacyEnricher } from "../src/compat.js";
import type { EventStore, RecordedEvent, OutboxEntry } from "../src/types.js";

const memory = () => {
  const events: RecordedEvent[] = [],
    outbox: OutboxEntry[] = [];
  const store = {
    async readStream() {
      return structuredClone(events);
    },
    async acceptedCommand() {
      return null;
    },
    async appendToStream(stream, revision, pending, _command, options) {
      const recorded = pending.map((event, i) => ({
        ...event,
        stream,
        revision: revision + i + 1,
        position: {
          transactionId: "1",
          sequence: String(events.length + i + 1),
        },
      }));
      events.push(...structuredClone(recorded));
      outbox.push(...structuredClone(options?.outbox ?? []));
      return recorded;
    },
  } as EventStore;
  return { store, outbox };
};
test("folds own Event data copied into State and can update it without changing history", async () => {
  const definition = compile("sales", "account", {
    commands: {
      change: [(_command, aggregate) => {
        aggregate.apply.created({ profile: { active: false }, tags: ["one"] });
        aggregate.apply.changed({ active: true, tag: "two" });
      }],
    },
    events: {
      created: ({ payload }, state) => {
        state.set(payload);
        state.set("originalProfile", payload.profile);
        state.set("rows", [{ profile: payload.profile }]);
      },
      changed: ({ payload }, state) => {
        state.set("profile.active", payload.active);
        state.get("tags").push(payload.tag);
        state.get("rows")[0].profile.active = payload.active;
      },
    },
  });
  const { store } = memory();
  const outcome = await createKernel({ definitions: [definition], store }).handle(
    createCommand({ stream: { context: "sales", aggregate: "account", id: "a" }, name: "change" }),
  );
  expect(outcome.status).toBe("accepted");
  if (outcome.status !== "accepted") throw Error("Command failed");
  expect(loadAggregate(definition, "a", outcome.events).get()).toEqual({
    id: "a", profile: { active: true }, originalProfile: { active: true },
    tags: ["one", "two"], rows: [{ profile: { active: true } }],
  });
  expect(outcome.events[0]!.payload).toEqual({ profile: { active: false }, tags: ["one"] });
  expect((await store.readStream(outcome.events[0]!.stream))[0]!.payload).toEqual(outcome.events[0]!.payload);
});
test("ordered Steps fold Events at the attempt timestamp and preserve only-throw rejection semantics", async () => {
  const definition = compile("sales", "account", {
    initialState: { total: 0 },
    commands: {
      add: [
        only.ifNotExists(),
        (command, aggregate) => aggregate.apply.added(command.payload),
        only.ifState((_c, a) =>
          a.get("total") === 4
            ? false
            : (() => {
                throw Error("wrong order");
              })(),
        ),
        only.after(() => false),
      ],
    },
    events: {
      added: (event, state) =>
        state.set({ total: event.payload.amount, at: event.timestamp }),
    },
  });
  const { store } = memory();
  const kernel = createKernel({
    definitions: [definition],
    store,
    clock: () => new Date("2026-01-02T03:04:05Z"),
  });
  const outcome = await kernel.handle(
    createCommand({
      stream: { context: "sales", aggregate: "account", id: "a" },
      name: "add",
      payload: { amount: 4 },
    }),
  );
  expect(outcome.status).toBe("accepted");
  if (outcome.status !== "accepted") throw Error(JSON.stringify(outcome));
  expect(loadAggregate(definition, "a", outcome.events).get()).toEqual({
    id: "a",
    total: 4,
    at: "2026-01-02T03:04:05.000Z",
  });
  expect(outcome.events[0]).toMatchObject({
    schemaVersion: 1,
    causationId: outcome.events[0]!.commandId,
  });
});
test("rejected Rules roll back Events and told Commands; accepted no-Event Commands still commit their Outbox", async () => {
  const { store, outbox } = memory();
  const definition = compile("sales", "account", {
    commands: {
      reject: [
        (c, a, app) => {
          a.apply.added({ amount: 9 });
          app.tell({ stream: c.stream, name: "follow" });
        },
        only.after(() => {
          throw Error("refused");
        }),
      ],
      follow: (c, _a, app) =>
        app.tell({
          stream: c.stream,
          name: "later",
          metadata: { nested: { next: true } },
        }),
    },
    events: { added: (_e, state) => state.set("total", 9) },
  });
  const kernel = createKernel({ definitions: [definition], store });
  const command = createCommand({
    stream: { context: "sales", aggregate: "account", id: "a" },
    name: "reject",
    metadata: { nested: { prior: true } },
  });
  expect(await kernel.handle(command)).toMatchObject({
    status: "rejected",
    error: { kind: "rule" },
  });
  expect(await store.readStream(command.stream)).toEqual([]);
  expect(outbox).toEqual([]);
  expect(await kernel.handle({ ...command, name: "follow" })).toMatchObject({
    status: "accepted",
    events: [],
  });
  expect(outbox).toMatchObject([
    {
      kind: "command",
      message: {
        causationId: command.id,
        correlationId: command.correlationId,
        metadata: { nested: { next: true } },
      },
    },
  ]);
});
test("metadata merges shallowly after Rules, enforces limits, and never exposes mutable Events", async () => {
  const { store } = memory();
  const definition = compile("sales", "account", {
    commands: {
      add: [
        (c, a) => a.apply.added({}, { nested: { apply: true } }),
        only.after((_current, _previous, events) => {
          expect(events[0]!.metadata).toEqual({ nested: { apply: true } });
        }),
      ],
    },
    events: {
      added: (event, state) => {
        expect(() => {
          event.payload.changed = true;
        }).toThrow();
        state.set("ok", true);
      },
    },
    eventEnricher: (event) => {
      expect(Object.isFrozen(event)).toBe(true);
      return { nested: { enriched: true } };
    },
  });
  const kernel = createKernel({ definitions: [definition], store });
  const command = createCommand({
    stream: { context: "sales", aggregate: "account", id: "a" },
    name: "add",
    metadata: { nested: { command: true } },
  });
  const outcome = await kernel.handle(command);
  expect(outcome).toMatchObject({ status: "accepted" });
  if (outcome.status !== "accepted") throw Error(JSON.stringify(outcome));
  // toEqual: a deep merge would keep nested.command / nested.apply
  expect(outcome.events[0]!.metadata).toEqual({ nested: { enriched: true } });
  expect(
    await kernel.handle({ ...command, metadata: { huge: "x".repeat(8192) } }),
  ).toMatchObject({ status: "rejected", error: { kind: "validation" } });
  definition.enrich = () => ({ huge: "x".repeat(8192) });
  expect(await kernel.handle(command)).toMatchObject({ status: "failed" });
});
test("an old enricher that sets event.metadata fields and returns nothing keeps them; its payload and envelope stay frozen", async () => {
  const { store } = memory();
  const definition = compile("sales", "account", {
    commands: { add: (c, a) => a.apply.added({ n: 1 }, { source: "apply" }) },
    events: { added: () => {} },
    eventEnricher: (event: any) => {
      event.metadata.tenant = { id: "t1" };
      expect(() => {
        event.payload.n = 2;
      }).toThrow();
    },
  });
  const kernel = createKernel({ definitions: [definition], store });
  const outcome = await kernel.handle(
    createCommand({
      stream: { context: "sales", aggregate: "account", id: "a" },
      name: "add",
    }),
  );
  if (outcome.status !== "accepted") throw Error(JSON.stringify(outcome));
  expect(outcome.events[0]).toMatchObject({
    payload: { n: 1 },
    metadata: { source: "apply", tenant: { id: "t1" } },
  });
});
test("a legacyEnricher runs inside each apply on the State before that Event, and what it changes is recorded and folded", async () => {
  const { store } = memory();
  const seen: unknown[] = [];
  const definition = compile("sales", "account", {
    initialState: { total: 0 },
    commands: {
      add: [
        (c, a) => {
          a.apply.added({ n: 1 });
          // A later Step sees the enriched Event's fold.
          a.apply.added({ n: a.get("total") });
        },
        only.after((_current, _previous, events) => {
          seen.push(events.map((event) => event.metadata.before));
        }),
      ],
    },
    events: {
      added: (event, state) =>
        state.set("total", state.get("total") + event.payload.n),
    },
    eventEnricher: legacyEnricher((event, state) => {
      event.payload.n *= 10;
      event.metadata.before = state.get("total");
      // A returned Event replaces the mutated one, as the old `enricher(...) || event` did.
      return event.payload.n >= 100
        ? { ...event, payload: { n: 0 } }
        : undefined;
    }),
  });
  const kernel = createKernel({ definitions: [definition], store });
  const outcome = await kernel.handle(
    createCommand({
      stream: { context: "sales", aggregate: "account", id: "a" },
      name: "add",
    }),
  );
  if (outcome.status !== "accepted") throw Error(JSON.stringify(outcome));
  expect(
    outcome.events.map(({ payload, metadata }) => ({ payload, metadata })),
  ).toEqual([
    { payload: { n: 10 }, metadata: { before: 0 } },
    { payload: { n: 0 }, metadata: { before: 10 } },
  ]);
  expect(seen).toEqual([[0, 10]]);
  definition.enrich = legacyEnricher((event) => {
    event.name = "removed";
  });
  expect(
    await kernel.handle(
      createCommand({
        stream: { context: "sales", aggregate: "account", id: "b" },
        name: "add",
      }),
    ),
  ).toMatchObject({
    status: "rejected",
    error: {
      kind: "precondition",
      message:
        "eventEnricher changed the Event's name; only payload and metadata are recorded",
    },
  });
});
test("Command validation supports trim/defaults, named async schemas and legacy keywords before Steps", async () => {
  const { createValidator } = await import("../src/validation.js");
  const validate = createValidator({
    schemas: {
      "/account/Input": {
        $async: true,
        type: "object",
        anyRequired: ["name", "alias"],
        properties: {
          name: { type: "string", transform: ["trim"], format: "known" },
          amount: { type: "number", multipleOf: 0.01, default: 1.15 },
        },
      },
    },
    formats: {
      known: {
        async: true,
        type: "string",
        validate: async (value: string) => value === "Alice",
      },
    },
  });
  const { store } = memory();
  const definition = compile("sales", "account", {
    commands: {
      add: [
        only.ifValidatedBy("/account/Input"),
        (c, a) => a.apply.added(c.payload),
      ],
    },
    events: { added: (e, state) => state.set("value", e.payload) },
  });
  const kernel = createKernel({ definitions: [definition], store, validate });
  const command = createCommand({
    stream: { context: "sales", aggregate: "account", id: "a" },
    name: "add",
    payload: { name: " Alice " },
  });
  expect(await kernel.handle(command)).toMatchObject({
    status: "accepted",
    events: [{ payload: { name: "Alice", amount: 1.15 } }],
  });
  expect(command.payload).toEqual({ name: " Alice " });
  expect(await kernel.handle({ ...command, payload: {} })).toMatchObject({
    status: "rejected",
    error: {
      kind: "validation",
      details: expect.arrayContaining([
        expect.objectContaining({ keyword: "anyRequired" }),
      ]),
    },
  });
  // A schema rejection is EventFabric's, not an Application error the old error classes would rebuild.
  expect(((await kernel.handle({ ...command, payload: {} })) as any).error.thrown).toBeUndefined();
});
test("legacy tell chains inherit metadata and schedule durable Commands, while getDomain reads fresh state", async () => {
  const { store, outbox } = memory();
  const definition = compile("sales", "account", {
    commands: {
      follow: async (_c, _a, app) => {
        await app
          .tell()
          .sales.account("b")
          .later(
            { amount: 2 },
            { actor: "next" },
            { notBefore: "2026-10-16T00:00:00Z", replaceKey: "later-b" },
          );
        const current = await app.getDomain().sales.account("a").load();
        expect(current.get("total")).toBe(0);
      },
      later: () => {},
    },
    events: {},
    initialState: { total: 0 },
  });
  const kernel = createKernel({ definitions: [definition], store });
  const command = createCommand({
    stream: { context: "sales", aggregate: "account", id: "a" },
    name: "follow",
    metadata: { owner: "team", actor: "prior" },
  });
  expect(await kernel.handle(command)).toMatchObject({ status: "accepted" });
  expect(outbox).toMatchObject([
    {
      notBefore: "2026-10-16T00:00:00Z",
      replaceKey: "later-b",
      message: {
        name: "later",
        stream: { id: "b" },
        metadata: { actor: "next", owner: "team" },
      },
    },
  ]);
});
test("conflicts reload state and redecide with a fresh timestamp, dropping the previous attempt Outbox", async () => {
  const { ConcurrencyError } = await import("../src/errors.js");
  const base = memory();
  let competing = true,
    tick = 0;
  const store = {
    ...base.store,
    async appendToStream(stream, revision, pending, command, options) {
      if (competing) {
        competing = false;
        await base.store.appendToStream(
          stream,
          revision,
          [{ ...pending[0]!, payload: { amount: 10 } }],
          command,
        );
        throw new ConcurrencyError("competing Command");
      }
      return base.store.appendToStream(
        stream,
        revision,
        pending,
        command,
        options,
      );
    },
  } as EventStore;
  const definition = compile("sales", "account", {
    initialState: { total: 0 },
    commands: {
      add: (c, a, app) => {
        a.apply.added({ amount: a.get("total") + 1 });
        app.queue("charge", { total: a.get("total") });
      },
    },
    events: { added: (e, state) => state.set("total", e.payload.amount) },
  });
  const kernel = createKernel({
    definitions: [definition],
    store,
    clock: () => new Date(Date.UTC(2026, 0, 1, 0, 0, tick++)),
  });
  const command = createCommand({
    stream: { context: "sales", aggregate: "account", id: "a" },
    name: "add",
  });
  expect(await kernel.handle(command)).toMatchObject({
    status: "accepted",
    events: [
      {
        payload: { amount: 11 },
        timestamp: "2026-01-01T00:00:01.000Z",
        revision: 2,
        id: expect.stringMatching(
          /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
        ),
      },
    ],
  });
  expect(base.outbox).toMatchObject([
    {
      message: {
        payload: { total: 11 },
        idempotencyKey: JSON.stringify(["sales.account", command.id, "0"]),
      },
    },
  ]);
  expect(base.outbox).toHaveLength(1);
});

test("a Command canceled while a Step is running never commits late Events or effects", async () => {
  const { store, outbox } = memory();
  const controller = new AbortController();
  const definition = compile("sales", "account", {
    commands: {
      add: async (_c, a, app) => {
        a.apply.added();
        app.queue("charge", {});
        await Promise.resolve();
        controller.abort();
      },
    },
    events: { added: () => {} },
  });
  const kernel = createKernel({ definitions: [definition], store });
  const command = createCommand({
    stream: { context: "sales", aggregate: "account", id: "a" },
    name: "add",
  });
  expect(
    await kernel.handle(command, { signal: controller.signal }),
  ).toMatchObject({ status: "failed" });
  expect(await store.readStream(command.stream)).toEqual([]);
  expect(outbox).toEqual([]);
});
test("validation rejects synchronous invalid payloads with field paths and metadata must be JSON", async () => {
  const { store } = memory();
  const definition = compile("sales", "account", {
    commands: {
      add: [
        only.ifValidatedBy({ type: "object", required: ["name"] }),
        () => {},
      ],
    },
    events: {},
  });
  const kernel = createKernel({ definitions: [definition], store });
  const command = createCommand({
    stream: { context: "sales", aggregate: "account", id: "a" },
    name: "add",
  });
  expect(await kernel.handle(command)).toMatchObject({
    status: "rejected",
    error: {
      kind: "validation",
      details: [{ path: "name", keyword: "required" }],
    },
  });
  expect(
    await kernel.handle({
      ...command,
      payload: { name: "Alice" },
      metadata: { invalid: () => 1 },
    }),
  ).toMatchObject({ status: "rejected", error: { kind: "validation" } });
});
test("pre-load checks precede Stream reads and id generation; duplicate definitions and bad authoring fail startup", async () => {
  const { store } = memory();
  const definition = compile("sales", "account", {
    idGenerator: () => {
      throw Error("must not generate before checks");
    },
    commands: {
      add: [
        only.ifValidatedBy(() => {
          throw Error("not allowed");
        }),
        () => {},
      ],
    },
    events: {},
  });
  const kernel = createKernel({ definitions: [definition], store });
  expect(
    await kernel.handle(
      createCommand({
        stream: { context: "sales", aggregate: "account", id: "" },
        name: "add",
      }),
    ),
  ).toMatchObject({
    status: "rejected",
    error: { kind: "precondition", message: "not allowed" },
  });
  // An old error constructor that is not an Error keeps its message.
  function OldError(this: any, message: string) {
    this.message = message;
  }
  const legacy = compile("sales", "ledger", {
    commands: {
      add: [
        () => {
          throw new (OldError as any)("wrong status");
        },
      ],
    },
    events: {},
  });
  expect(
    await createKernel({ definitions: [legacy], store }).handle(
      createCommand({
        stream: { context: "sales", aggregate: "ledger", id: "l" },
        name: "add",
      }),
    ),
  ).toMatchObject({
    status: "rejected",
    error: { kind: "precondition", message: "wrong status" },
  });
  expect(() =>
    createKernel({ definitions: [definition, definition], store }),
  ).toThrow("Duplicate Aggregate");
  expect(() =>
    compile("sales", "account", {
      commands: { add: [() => {}, only.ifValidatedBy(() => {})] },
    }),
  ).toThrow("pre-load");
});
test("Command schemas and custom formats load from an Application folder without changing store values", async () => {
  const { mkdtemp, mkdir, writeFile, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const { createValidator } = await import("../src/validation.js");
  const folder = await mkdtemp(join(tmpdir(), "event-fabric-schema-"));
  try {
    const schemasDir = join(folder, "schemas"),
      formatsDir = join(folder, "formats");
    await mkdir(join(schemasDir, "account"), { recursive: true });
    await mkdir(formatsDir);
    await writeFile(
      join(formatsDir, "allowed.mjs"),
      'export default value => value === "Alice";',
    );
    await writeFile(
      join(schemasDir, "account", "Input.schema.json"),
      JSON.stringify({
        type: "object",
        required: ["name"],
        properties: {
          name: { type: "string", format: "allowed", transform: ["trim"] },
          category: { default: "normal" },
        },
      }),
    );
    const validate = createValidator({ schemasDir, formatsDir });
    const { store } = memory();
    const definition = compile("sales", "account", {
      commands: {
        add: [
          only.ifValidatedBy("account/Input"),
          (c, a) => a.apply.added(c.payload),
        ],
      },
      events: { added: () => {} },
    });
    const kernel = createKernel({ definitions: [definition], store, validate });
    expect(
      await kernel.handle(
        createCommand({
          stream: { context: "sales", aggregate: "account", id: "a" },
          name: "add",
          payload: { name: " Alice " },
        }),
      ),
    ).toMatchObject({
      status: "accepted",
      events: [{ payload: { name: "Alice", category: "normal" } }],
    });
    const value = { name: " Alice " };
    await createValidator({ mutate: false })(
      {
        type: "object",
        properties: {
          name: { transform: ["trim"] },
          defaulted: { default: true },
        },
      },
      value,
    );
    expect(value).toEqual({ name: " Alice " });
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});

test("unsupported Aggregate options and non-function Rules are refused at compile time", () => {
  // `disablePersistence` used to be dropped silently, so its Events were persisted anyway.
  expect(() =>
    compile("sales", "account", {
      options: { disablePersistence: true, skipHistory: true },
      commands: { add: () => {} },
      events: {},
    } as any),
  ).toThrow("sales.account: unsupported Aggregate option options (Events are always persisted");
  expect(() =>
    compile("sales", "account", { commands: {}, events: {}, version: 2 } as any),
  ).toThrow("unsupported Aggregate option version");
  expect(() =>
    compile("sales", "account", {
      rules: [{ name: "minimum", rule: () => {} }] as any,
      commands: { add: () => {} },
      events: {},
    }),
  ).toThrow("sales.account: rule 0 is not a function");
});
test("an unknown format fails the schema, a named format definition registers its alias, and YAML schemas are refused", async () => {
  const { mkdtemp, mkdir, writeFile, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const { createValidator, loadNamedSchemas } = await import("../src/validation.js");
  const folder = await mkdtemp(join(tmpdir(), "event-fabric-format-"));
  try {
    await expect(
      createValidator()({ type: "string", format: "iban-alias" }, "anything"),
    ).rejects.toThrow('unknown format "iban-alias"');
    const formatsDir = join(folder, "formats");
    await mkdir(formatsDir);
    await writeFile(
      join(formatsDir, "iban.mjs"),
      'export default { name: "iban-alias", options: { validate: (value) => /^[A-Z]{2}\\d+$/.test(value) } };',
    );
    const validate = createValidator({ formatsDir });
    const schema = { type: "string", format: "iban-alias" };
    await expect(validate(schema, "DE123")).resolves.toBeUndefined();
    await expect(validate(schema, "not an iban")).rejects.toThrow();
    const schemasDir = join(folder, "schemas");
    await mkdir(schemasDir);
    await writeFile(join(schemasDir, "Input.schema.yaml"), "type: object\n");
    await expect(loadNamedSchemas(schemasDir)).rejects.toThrow(
      "Schema Input.schema.yaml: JSON5 and YAML are not loaded",
    );
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
test("a state attribute named id survives; get('id') falls back to the Stream id only when absent", () => {
  const own = new AggregateState("stream-id", { id: "own", n: 1 });
  expect(own.attributes).toEqual({ id: "own", n: 1 });
  expect(own.get("id")).toBe("own");
  expect(own.clone().attributes).toEqual({ id: "own", n: 1 });
  const plain = new AggregateState("stream-id", { n: 1 });
  expect(plain.attributes).toEqual({ n: 1 });
  expect(plain.get("id")).toBe("stream-id");
  expect(plain.get()).toEqual({ id: "stream-id", n: 1 });
  expect(plain.attributes).toEqual({ n: 1 });
  plain.set("id", "later");
  expect(plain.get("id")).toBe("later");
});
test("undefined follows JSON semantics in payloads, apply() and enricher output and metadata; other non-JSON values reject", async () => {
  const { store, outbox } = memory();
  const definition = compile("sales", "account", {
    commands: {
      add: (c, a, app) => {
        a.apply.added(
          { kept: c.payload.kept, gone: undefined, list: [1, undefined] },
          { extra: undefined },
        );
        app.queue("charge", { gone: undefined });
      },
      bad: (c, a) => a.apply.added({ value: c.payload.value }),
    },
    events: { added: () => {} },
    eventEnricher: () => ({ enriched: undefined, ok: true }),
  });
  const kernel = createKernel({ definitions: [definition], store });
  const command = createCommand({
    stream: { context: "sales", aggregate: "account", id: "a" },
    name: "add",
    payload: { kept: 1, gone: undefined },
    metadata: { actor: "x", gone: undefined, list: [undefined] },
  });
  const outcome = await kernel.handle(command);
  expect(outcome).toMatchObject({ status: "accepted" });
  if (outcome.status !== "accepted") throw Error(JSON.stringify(outcome));
  expect(outcome.events[0]!.payload).toStrictEqual({
    kept: 1,
    list: [1, null],
  });
  expect(outcome.events[0]!.metadata).toStrictEqual({
    actor: "x",
    list: [null],
    ok: true,
  });
  expect(outbox[0]!.message.payload).toStrictEqual({});
  for (const payload of [
    { f: () => 1 },
    { n: 1n },
    { n: Number.NaN },
    { s: Symbol("s") },
  ])
    expect(await kernel.handle({ ...command, payload })).toMatchObject({
      status: "rejected",
      error: { kind: "validation" },
    });
  const cyclic: any = {};
  cyclic.self = cyclic;
  expect(
    await kernel.handle({ ...command, metadata: { cyclic } }),
  ).toMatchObject({ status: "rejected", error: { kind: "validation" } });
  expect(
    await kernel.handle({ ...command, name: "bad", payload: {} }),
  ).toMatchObject({ status: "accepted" });
  const step = createKernel({
    definitions: [
      compile("sales", "account", {
        commands: { add: (_c, a) => a.apply.added({ value: Infinity }) },
        events: { added: () => {} },
      }),
    ],
    store,
  });
  expect(await step.handle(command)).toMatchObject({ status: "rejected" });
});
test("each Revision-conflict attempt sees a fresh copy of the validated payload and metadata", async () => {
  const { ConcurrencyError } = await import("../src/errors.js");
  const base = memory();
  let conflicts = 1;
  const store = {
    ...base.store,
    async appendToStream(...args: Parameters<EventStore["appendToStream"]>) {
      if (conflicts-- > 0) throw new ConcurrencyError("competing Command");
      return base.store.appendToStream(...args);
    },
  } as EventStore;
  const definition = compile("sales", "account", {
    commands: {
      add: (c, a) => {
        c.payload.items.push("step");
        c.metadata.seen = [...(c.metadata.seen ?? []), "step"];
        a.apply.added({ items: c.payload.items, seen: c.metadata.seen });
      },
    },
    events: { added: () => {} },
  });
  const kernel = createKernel({ definitions: [definition], store });
  const outcome = await kernel.handle(
    createCommand({
      stream: { context: "sales", aggregate: "account", id: "a" },
      name: "add",
      payload: { items: [] },
    }),
  );
  expect(outcome).toMatchObject({
    status: "accepted",
    events: [{ payload: { items: ["step"], seen: ["step"] } }],
  });
});
test("a Command conflicting on every attempt retries exactly three times, then rejects with concurrency", async () => {
  const { ConcurrencyError } = await import("../src/errors.js");
  let attempts = 0;
  const store = {
    ...memory().store,
    async appendToStream() {
      attempts++;
      throw new ConcurrencyError("competing Command");
    },
  } as EventStore;
  const definition = compile("sales", "account", {
    commands: { add: (_c, a) => a.apply.added() },
    events: { added: () => {} },
  });
  const kernel = createKernel({ definitions: [definition], store });
  expect(
    await kernel.handle(
      createCommand({
        stream: { context: "sales", aggregate: "account", id: "a" },
        name: "add",
      }),
    ),
  ).toMatchObject({ status: "rejected", error: { kind: "concurrency" } });
  expect(attempts).toBe(4);
});
test("the store JSON check accepts nested undefined like JSON, but not at the top level", async () => {
  const { assertJson } = await import("../src/event-store/upcasters.js");
  expect(() => assertJson({ a: undefined, b: [undefined] })).not.toThrow();
  expect(() => assertJson(undefined)).toThrow("Value must be JSON");
  expect(() => assertJson({ f: () => 1 })).toThrow("Value must be JSON");
});

test("the validator compiles an equal schema once, even when each call passes a fresh copy", async () => {
  const { createValidator } = await import("../src/validation.js");
  const validate = createValidator({ mutate: false });
  // A stored Readmodel generation is parsed again for every batch; Ajv refuses a second
  // compile of a schema with the same `$id`, so this fails if equal copies compile again.
  const stored = JSON.stringify({
    $id: "https://example.test/doc",
    type: "object",
    properties: { n: { type: "integer" } },
  });
  for (let i = 0; i < 3; i++) await validate(JSON.parse(stored), { n: i });
  await expect(validate(JSON.parse(stored), { n: "x" })).rejects.toThrow(
    "must be integer",
  );
});

test("a rejected Outcome carries the thrown error's name and own fields, without its stack", async () => {
  const { store } = memory();
  class ConflictError extends Error {
    statusCode = 409;
    errorCode = "conflict";
    data = { field: "email" };
    callback = () => {};
  }
  const definition = compile("sales", "account", {
    commands: {
      add: [
        () => {
          throw Object.assign(new ConflictError("Email already exists"), {
            name: "ConflictError",
          });
        },
      ],
    },
    events: {},
  });
  const outcome = await createKernel({ definitions: [definition], store }).handle(
    createCommand({
      stream: { context: "sales", aggregate: "account", id: "a" },
      name: "add",
    }),
  );
  expect(outcome).toEqual({
    status: "rejected",
    error: {
      code: "precondition",
      kind: "precondition",
      message: "Email already exists",
      details: undefined,
      thrown: {
        name: "ConflictError",
        statusCode: 409,
        errorCode: "conflict",
        data: { field: "email" },
      },
    },
  });
});
test("a Command Step logs through app.logger, the kernel's logger", async () => {
  const { store } = memory();
  const lines: unknown[] = [];
  const logger = { warn: (...args: unknown[]) => lines.push(args) } as any;
  const definition = compile("sales", "account", {
    commands: { add: (_c, a, app) => (app.logger.warn("noted"), a.apply.added({})) },
    events: { added: () => {} },
  });
  const command = createCommand({ stream: { context: "sales", aggregate: "account", id: "a" }, name: "add", payload: {} });
  expect(await createKernel({ definitions: [definition], store, logger }).handle(command)).toMatchObject({ status: "accepted" });
  expect(lines).toEqual([["noted"]]);
  const silent = await createKernel({ definitions: [definition], store }).handle(
    createCommand({ stream: { context: "sales", aggregate: "account", id: "b" }, name: "add", payload: {} }),
  );
  expect(silent).toMatchObject({ status: "accepted" });
});

test("a Command's load copies no historical Event: the store's fresh history is frozen in place", async () => {
  const definition = compile("sales", "counter", {
    commands: { add: [(_command, counter) => counter.apply.added({ by: 1, note: { text: "n" } })] },
    events: {
      added: ({ payload }, state) => {
        state.set("total", (state.get("total") ?? 0) + payload.by);
        state.set("last", payload.note);
      },
    },
  });
  const { store } = memory();
  const kernel = createKernel({ store, definitions: [definition] });
  const stream = { context: "sales", aggregate: "counter", id: "c" };
  const clones = vi.spyOn(globalThis, "structuredClone");
  const copiesFor = async () => {
    clones.mockClear();
    const outcome = await kernel.handle(createCommand({ stream, name: "add", payload: {} }));
    expect(outcome.status).toBe("accepted");
    // The test store's own copy of its history is not the kernel's.
    return clones.mock.calls.filter(([value]) => !Array.isArray(value)).length;
  };
  const first = await copiesFor();
  for (let i = 0; i < 40; i++) await copiesFor();
  expect(await copiesFor()).toBe(first);
  clones.mockRestore();
  const state = loadAggregate(definition, "c", await store.readStream(stream));
  expect(state.get()).toMatchObject({ total: 42, last: { text: "n" } });
  // A caller's own history stays its own: loadAggregate copies what it did not read itself.
  const history = await store.readStream(stream);
  loadAggregate(definition, "c", history);
  expect(Object.isFrozen(history[0]!.payload)).toBe(false);
});

test("a legacyEnricher's apply copies the Event once for the enricher", async () => {
  const { store } = memory();
  const definition = compile("sales", "ledger", {
    commands: {
      add: [(_c, a) => { for (let i = 0; i < 3; i++) a.apply.added({ n: i, rows: [{ deep: true }] }); }],
    },
    events: { added: () => {} },
    eventEnricher: legacyEnricher((event) => {
      event.metadata.seen = true;
    }),
  });
  const kernel = createKernel({ definitions: [definition], store });
  const clones = vi.spyOn(globalThis, "structuredClone");
  try {
    const outcome = await kernel.handle(
      createCommand({ stream: { context: "sales", aggregate: "ledger", id: "l" }, name: "add" }),
    );
    expect(outcome).toMatchObject({ status: "accepted", events: [{ metadata: { seen: true } }, {}, {}] });
    const eventCopies = clones.mock.calls.filter(([value]) => value && typeof value === "object" && "payload" in value);
    // Per apply: the enricher's mutable Event and the fold's frozen one.
    expect(eventCopies).toHaveLength(6);
  } finally {
    clones.mockRestore();
  }
});
