import { expect, test } from "vitest";
import { migrateSource, draftReadmodelSchema } from "../src/migration/index.js";
import {
  normalizeDocument,
  prepareDefinitions,
} from "../src/readmodel-store/schema.js";

test("schema drafts preserve nested identifiers and arrays through reviewed JSON fields", async () => {
  const document = {
    _id: "one",
    contact: { _id: "nested", active: true },
    rows: [["one", "two"], null, ["three"]],
  };
  const draft = draftReadmodelSchema({}, [document]);
  expect(draft.reviews).toEqual([
    "contact: nested reserved properties; review JSON storage",
    "rows[]: nested arrays; review JSON storage",
  ]);
  expect(draft.schema.properties).not.toHaveProperty("_id");
  const { definitions } = prepareDefinitions({ sample: { schema: draft.schema } });
  expect(await normalizeDocument(definitions.sample!.schema, document, "one")).toEqual(document);
});
test("keepGetCrud leaves getCrud() for the Application's legacyApp, in Tasks too", async () => {
  const source = "module.exports = async (task, app) => app.getCrud().system.setting();\nconst read = req => req.getApp().getCrud();";
  expect((await migrateSource(source, { keepGetCrud: true })).source).toContain("req.getApp().getCrud()");
  const task = await migrateSource(source, { keepGetCrud: true, task: true });
  expect(task.source).not.toContain("services.crud");
  expect((await migrateSource(source)).source).toContain("req.getApp().getServices().crud");
});
test("mechanical transforms preserve strings/comments and draft schemas flag unresolved shapes", async () => {
  const source = `// app.getCrud() stays in comments\nconst note = "app.getCrud()";\nconst aggregate = { [domain.sales.account.added]: () => app.getCrud().system.setting() };\nconst revision = aggregate._aggregateModel.getRevision();\naggregate._aggregateModel.__note__ = true;\nconst name = require('event-fabric-next');\nconst cli = require.resolve('event-fabric-next');`;
  const result = await migrateSource(source, {
    symbols: { "domain.sales.account.added": "sales.account.added" },
    packages: "cutover",
  });
  expect(result.source).toContain('["sales.account.added"]');
  expect(result.source).toContain("app.getServices().crud.system.setting()");
  expect(result.source).toContain("aggregate.revision");
  expect(result.source).toContain("aggregate.locals.__note__ = true");
  expect(result.source).toContain('"app.getCrud()"');
  expect(result.source).toContain("// app.getCrud() stays in comments");
  expect(result.source).toContain("require('event-fabric')");
  expect(result.source).toContain("require.resolve('event-fabric')");
  const draft = draftReadmodelSchema(
    { count: 0, tags: [], uncertain: null, at: new Date("2026-01-01") },
    [
      {
        count: 2,
        tags: ["one"],
        uncertain: { nested: true },
        at: new Date("2026-01-02"),
      },
    ],
  );
  expect(draft.schema.properties).toMatchObject({
    count: { type: "integer" },
    tags: { type: "array", items: { type: "string" } },
    at: { type: "string", format: "date-time" },
    uncertain: { type: ["object", "null"] },
  });
  expect(draft.reviews).toEqual([]);
  expect(draftReadmodelSchema({ unknown: [] }).reviews).toContain(
    "unknown: no array element evidence; review JSON storage",
  );
  expect(draftReadmodelSchema({ id: "a", name: "x" }).reviews).toEqual([
    "id: the old ViewModel stored each document's id here, and Reactions no longer do; set it where the document is created (view.set('id', view.id)) or read _id instead",
  ]);
});
test("historical envelope migration preserves identity/time, maps chain metadata and refuses pending Saga effects", async () => {
  const { migrateHistoricalEvent, migrateSagaState } =
    await import("../src/migration/audit.js");
  const result = migrateHistoricalEvent(
    {
      eventId: "old-event",
      stream: { context: "sales", aggregate: "account", id: "a" },
      name: "domain.sales.account.added",
      payload: { amount: 2 },
      revision: 0,
      commandId: "old-command",
      causationId: "chain",
      timestamp: "2020-01-01T00:00:00.000Z",
      metadata: {
        timestamp: "old",
        auth: {
          user: "u",
          socket: "s",
          platform: "p",
          forAccount: "a",
          channel: "web",
        },
        owner: { account: "a" },
      },
    },
    { revisionBase: 0 },
  );
  expect(result.event).toMatchObject({
    id: "old-event",
    revision: 1,
    name: "added",
    timestamp: "2020-01-01T00:00:00.000Z",
    correlationId: "chain",
    causationId: undefined,
    schemaVersion: 1,
    metadata: { auth: { user: "u", channel: "web" }, owner: { account: "a" } },
  });
  expect(result.event.metadata).not.toHaveProperty("timestamp");
  // Mongo-held Events: Date values need an explicit choice, ObjectIds a hand conversion.
  const bson = {
    eventId: "e2",
    stream: { context: "sales", aggregate: "account", id: "a" },
    name: "added",
    payload: { due: new Date(0) },
    revision: 1,
    commandId: "c2",
    timestamp: new Date("2020-01-01T00:00:00.000Z"),
    metadata: { timestamp: new Date(1) },
  };
  expect(() => migrateHistoricalEvent(bson, { revisionBase: 1 })).toThrow(
    "Event payload.due is a Date",
  );
  expect(
    migrateHistoricalEvent(bson, { revisionBase: 1, dates: "iso" }).event,
  ).toMatchObject({
    payload: { due: "1970-01-01T00:00:00.000Z" },
    timestamp: "2020-01-01T00:00:00.000Z",
    metadata: {},
  });
  class ObjectId {}
  expect(() =>
    migrateHistoricalEvent(
      { ...bson, payload: { ref: new ObjectId() } },
      { revisionBase: 1 },
    ),
  ).toThrow("Event payload.ref is a ObjectId, not JSON");
  expect(result.event.metadata.auth).not.toHaveProperty("socket");
  expect(
    migrateSagaState({
      active: true,
      _commands: [],
      _hash: "x",
      _commitStamp: "y",
    }),
  ).toEqual({ active: true });
  expect(() =>
    migrateSagaState({ _commands: [{ command: "pending" }] }),
  ).toThrow("reconciliation");
  expect(
    migrateSagaState({ step: 2, _timeoutAt: null, _timeoutCommands: [] }),
  ).toEqual({ step: 2 });
  expect(() =>
    migrateSagaState({
      _timeoutAt: new Date("2026-01-01"),
      _timeoutCommands: [{ name: "remind" }],
    }),
  ).toThrow("would never fire");
  const dated = {
    at: new Date("2026-01-02T03:04:05.000Z"),
    items: [{ due: new Date(0) }],
  };
  expect(() => migrateSagaState(dated)).toThrow("Saga state at is a Date");
  expect(migrateSagaState(dated, { dates: "iso" })).toEqual({
    at: "2026-01-02T03:04:05.000Z",
    items: [{ due: "1970-01-01T00:00:00.000Z" }],
  });
  expect(migrateSagaState(dated, { dates: "epoch" })).toEqual({
    at: 1767323045000,
    items: [{ due: 0 }],
  });
  expect(() =>
    migrateSagaState({ owner: { ref: new ObjectId() } }, { dates: "iso" }),
  ).toThrow("Saga state owner.ref is a ObjectId, not JSON");
});
test("replay audit gates unexplained state/Command differences and runs rebuilds without Saga effects", async () => {
  const { auditReplay } = await import("../src/migration/audit.js");
  const { compile } = await import("../src/authoring/index.js");
  const { createCommand } = await import("../src/id.js");
  const { createInMemoryEventStore } =
    await import("../src/event-store/memory.js");
  const { createInMemoryTransport } =
    await import("../src/transport/memory.js");
  const { createMemoryReadmodelStore } =
    await import("../src/adapters/memory-readmodel.js");
  const definition = compile("sales", "account", {
    commands: { add: (c, a) => a.apply.added(c.payload) },
    events: { added: (e, state) => state.set("total", e.payload.amount) },
  });
  const command = createCommand({
    id: "command",
    stream: { context: "sales", aggregate: "account", id: "a" },
    name: "add",
    payload: { amount: 2 },
    timestamp: "2026-01-01T00:00:00.000Z",
  });
  const event = {
    id: "event",
    name: "added",
    payload: { amount: 2 },
    metadata: {},
    schemaVersion: 1,
    timestamp: command.timestamp,
    commandId: command.id,
    causationId: command.id,
    correlationId: command.correlationId,
    stream: command.stream,
    revision: 1,
    position: { transactionId: "1", sequence: "1" },
  };
  const result = await auditReplay({
    definitions: [definition],
    streams: [
      {
        stream: command.stream,
        events: [event],
        expectedState: { id: "a", total: 2 },
      },
    ],
    commands: [{ command, history: [], expectedEvents: [event] }],
    readmodels: {
      config: {
        name: "audit",
        aggregates: [definition],
        store: createInMemoryEventStore(),
        transport: createInMemoryTransport(),
        readmodels: {
          dependent: {
            store: createMemoryReadmodelStore(),
            reads: ["api"],
            models: {
              account: {
                schema: {
                  type: "object",
                  properties: { total: { type: "number" } },
                },
                reactions: {
                  "sales.account.added": async (event, vm, app) => {
                    const document = await app
                      .getReadmodels()
                      .api.account.findOne({ _id: event.aggregate.id });
                    vm.set("total", document.total);
                  },
                },
              },
            },
          },
          api: {
            store: createMemoryReadmodelStore(),
            models: {
              account: {
                schema: {
                  type: "object",
                  properties: { total: { type: "number" } },
                },
                reactions: {
                  "sales.account.added": (e, vm) =>
                    vm.set("total", e.payload.amount),
                },
              },
            },
          },
        },
        sagas: {
          forbidden: {
            reactions: {
              "sales.account.added": () => {
                throw Error("Sagas must never replay");
              },
            },
          },
        },
      },
      history: [event],
      expected: [
        { group: "api", model: "account", documents: [{ _id: "a", total: 2 }] },
        {
          group: "dependent",
          model: "account",
          documents: [{ _id: "a", total: 2 }],
        },
      ],
    },
  });
  expect(result).toMatchObject({
    passed: true,
    checked: { streams: 1, commands: 1, readmodels: 2 },
    differences: [],
  });
  expect(
    await auditReplay({
      definitions: [definition],
      streams: [
        {
          stream: command.stream,
          events: [event],
          expectedState: { id: "a", total: 99 },
        },
      ],
    }),
  ).toMatchObject({ passed: false, differences: [{ kind: "state" }] });
});
test("Task signature transforms are explicit and unfamiliar delayed calls remain flagged for review", async () => {
  const task = await migrateSource(
    "module.exports = async (task, app) => { const { config } = app.wire.app; return app.sendCommand(config.command); };",
    { task: true },
  );
  expect(task.source).toContain("exports.run = async (ctx, args)");
  expect(task.source).toContain(
    "const { config } = ctx; return ctx.sendCommand",
  );
  expect(task.reviews).toEqual([
    "app.wire.app: now ctx; ctx.config is the EventFabric config, so the Application's own settings (such as a database URI) read undefined from it; give them to an Application-owned service in setupServices, or read them from the environment, by hand",
  ]);
  const delayed = await migrateSource(
    'app.scheduleTask("legacy", payload, delay);',
  );
  expect(delayed.reviews.join("\n")).toContain("scheduleTask");
  expect(delayed.source).toContain("scheduleTask");
});
test("Access maps lose the filter wrapper and keep today's permissive Command behaviour explicitly", async () => {
  const result = await migrateSource(`const access = {
	readmodels: {
		'api_views.*': m => ({ 'owner.account': m.owner.account }),
		'search.file': m => ({ filter: { account: m.owner.account } }),
		'detail.*': m => { return { filter: { account: m.owner.account } }; },
		'other.*': m => ({ filter: m.scope }),
		'*': true, // TODO - remove this
	},
	domain: {
		'shop.basket': false,
		'billing.invoice': m => m.auth.role.includes('admin'),
	},
};
module.exports = { access };`);
  expect(result.source).toContain(
    "'search.file': m => ({ account: m.owner.account }),",
  );
  expect(result.source).toContain("return { account: m.owner.account };");
  expect(result.source).toContain("'other.*': m => ({ filter: m.scope }),");
  expect(result.source).toContain(`	domain: {
		// Not enforced before migration; review before enabling:
		// 'shop.basket': false,
		// 'billing.invoice': m => m.auth.role.includes('admin'),
		'*': true,
	},`);
  const { access } = new Function(
    "module",
    `${result.source}; return module.exports;`,
  )({ exports: {} });
  expect(access.domain).toEqual({ "*": true });
  expect(result.reviews).toEqual([
    "access domain: Command rules were never enforced before migration; they are kept as comments under '*': true. Tighten after cutover: 'shop.basket', 'billing.invoice'",
    "access readmodels '*': true opens every Readmodel; tighten after cutover",
    "access readmodels filter: { filter: m.scope } is not an object literal, so it cannot be told apart from a document field named filter; unwrap by hand",
  ]);
  const implicit = await migrateSource(
    "exports.access = { readmodels: { 'a.b': true } };\nconst other = { access: { readmodels: {}, domain: { '*': true } } };",
  );
  expect(implicit.source).toBe(
    "exports.access = { readmodels: { 'a.b': true }, domain: { '*': true } };\nconst other = { access: { readmodels: {}, domain: { '*': true } } };",
  );
  expect(implicit.reviews).toEqual([]);
});
test("inline Access maps remain valid JavaScript after migrating domain rules", async () => {
  const result = await migrateSource(
    "const access = { domain: { 'sales.order': false }, readmodels: {} }; module.exports = access;",
  );
  const access = new Function(
    "module",
    `${result.source}; return module.exports;`,
  )({ exports: {} });
  expect(access.domain).toEqual({ "*": true });
  expect(result.source).toContain("// 'sales.order': false");
});

test("old REST descriptors are flagged with their translation and chain-wide awaits become awaitCorrelated", async () => {
  const result =
    await migrateSource(`router.post('/', command(domain => ({ command: domain.sales.customer.create, event: 'readmodel.api_views.customer.create' })));
const done = app.getDomain().shop.order(id).cancel({ reason }, meta).await('domain.shop.order.statusChanged');
const file = await app.getDomain().billing.invoice().submit(payload, meta).await('domain.shop.order.*');
const later = app.getDomain().shop.order(id).pause().await(waitFor);
const WAIT = 'readmodel.api_views.customer.update';`);
  expect(result.source).toContain(
    "command(domain => ({ command: domain.sales.customer.create, event: 'readmodel.api_views.customer.create' }))",
  );
  expect(result.source).toContain(
    ".cancel({ reason }, meta).await('shop.order.statusChanged')",
  );
  expect(result.source).toContain(
    "await app.awaitCorrelated({ stream: { context: 'billing', aggregate: 'invoice' }, name: 'submit', payload: payload, metadata: meta }, 'shop.order.*');",
  );
  expect(result.source).toContain(".pause().await(waitFor)");
  // Kept as the old Event, the wait throws a Rejection's Outcome (see the next tests), so nothing is left to review.
  expect(result.source).toContain("const file = waited.event;");
  expect(result.reviews).toEqual([
    "await(waitFor): the Event pattern is not a string literal, so it cannot be told whether the wait is chain-wide; use .await for the sent Command's own Events or awaitCorrelated for downstream Events",
    "domain.sales.customer.create: an old REST command descriptor, which the new command() would send as the Command itself; write command(req => ({ stream: { context: 'sales', aggregate: 'customer', id: <from req> }, name: 'create', payload: <from req> }), { respondWith: 'api_views.customer' }) by hand, moving the old id and payload options into the callback and dropping transforms and schema. respondWith reads the document whose id is the Stream id; for another Identity write a handler that waits for the Position",
    "readmodel.api_views.customer.update: a Readmodel wait name outside an old REST command descriptor; where it is a wait, use respondWith or readmodels.waitFor by hand",
  ]);
  const task = await migrateSource(
    "module.exports = async (task, app) => app.getDomain().a.b('x').c(1).await('domain.d.e.f');",
    { task: true },
  );
  expect(task.source).toContain(
    "ctx.awaitCorrelated({ stream: { context: 'a', aggregate: 'b', id: 'x' }, name: 'c', payload: 1 }, 'd.e.f')",
  );
});
test("fixture-file audit maps old envelopes and reads source fixtures without modifying them", async () => {
  const { auditFixtureFiles } = await import("../src/migration/audit.js");
  const { compile } = await import("../src/authoring/index.js");
  const { mkdtemp, writeFile, readFile, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const folder = await mkdtemp(join(tmpdir(), "event-fabric-audit-"));
  try {
    const stream = { context: "sales", aggregate: "account", id: "a" };
    const metadata = {
      timestamp: "2020-01-01T00:00:00Z",
      auth: { user: "u", socket: "drop" },
    };
    const raw =
      JSON.stringify({
        eventId: "event",
        name: "domain.sales.account.added",
        stream,
        payload: { amount: 2 },
        metadata,
        revision: 0,
        commandId: "command",
        timestamp: "2020-01-01T00:00:00Z",
        causationId: "chain",
      }) + "\n";
    await writeFile(join(folder, "events.jsonl"), raw);
    await writeFile(
      join(folder, "states.jsonl"),
      JSON.stringify({ stream, state: { id: "a", total: 2 } }) + "\n",
    );
    await writeFile(
      join(folder, "commands.jsonl"),
      JSON.stringify({
        commandId: "command",
        name: "add",
        stream,
        payload: { amount: 2 },
        metadata,
        eventIds: ["event"],
      }) + "\n",
    );
    const definition = compile("sales", "account", {
      commands: { add: (c, a) => a.apply.added(c.payload) },
      events: { added: (e, state) => state.set("total", e.payload.amount) },
    });
    expect(
      await auditFixtureFiles(folder, {
        definitions: [definition],
        revisionBase: 0,
      }),
    ).toMatchObject({
      passed: true,
      checked: { streams: 1, commands: 1 },
      skipped: [],
      differences: [],
    });
    expect(await readFile(join(folder, "events.jsonl"), "utf8")).toBe(raw);
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
test("old Consumer markers the runtime cannot honor are flagged and left unchanged; unrelated query data is not", async () => {
  const source = `const model = { identity: { 'sales.order.created': 'payload.target' }, query: { 'sales.order.created': () => ({}) }, reactions: { 'sales.order.created': [{ useAsId: 'payload.target' }, { settings: { autoCreate: false } }, { settings: { shouldHandleEvent: () => false } }, handler] } };\nconst payload = { query: { status: 'open' } };`;
  const result = await migrateSource(source);
  expect(result.source).toBe(source);
  expect(result.reviews).toEqual([
    "identity 'sales.order.created': a property-path string; write (event) => event.payload.target",
    "query: a separate Readmodel query map; return each filter from that Event's identity function instead",
    "useAsId: an inline Identity marker; move it into identity as (event) => event.payload.target",
  ]);
});
test("old QueryBuilder chains are translated whole or flagged, never half-rewritten", async () => {
  const native = await migrateSource(
    "const docs = await app.getReadmodels().g.m().base().find({ active: true }).toArray();",
  );
  expect(native.source).toBe(
    "const docs = await (await app.getReadmodels().g.m.native()).find({ active: true }).toArray();",
  );
  expect(native.reviews).toEqual([
    "app.getReadmodels().g.m.base(): now (await app.getReadmodels().g.m.native()), the Mongo collection; it works only with the Mongo Readmodel store. Port the query to app.getReadmodels().g.m.find(filter, options) to use another store",
  ]);
  const flagged = [
    "const collection = app.getReadmodels().g.m().base();",
    "const rows = await app.getReadmodels().g.m().raw();",
    "const row = await app.getReadmodels().g.m(id).exec();",
    "const readmodels = app.getReadmodels();",
  ];
  for (const source of flagged) {
    const result = await migrateSource(source);
    expect(result.source).toBe(source);
    expect(result.reviews).toHaveLength(1);
  }
  expect((await migrateSource(flagged[1]!)).reviews[0]).toContain(
    "{ document, position, behind }",
  );
  expect(
    await migrateSource(
      "const rows = await app.getReadmodels().g.m.find({ a: 1 });",
    ),
  ).toEqual({
    source: "const rows = await app.getReadmodels().g.m.find({ a: 1 });",
    reviews: [],
  });
});
test("old list routes become legacyList with their query and transforms; other options are listed", async () => {
  const { source, reviews } = await migrateSource(
    "router.get('/', readmodels.list(m => m.api.customers, { query: { size: '2' }, transforms: [(doc, app) => doc] }));\nrouter.get('/all', readmodels.list(models => models.g.m));",
  );
  expect(source).toBe(
    "const { legacyList } = require(\"event-fabric-next/compat\");\nrouter.get('/', legacyList('api', 'customers', { query: { size: '2' }, transforms: [(doc, app) => doc] }));\nrouter.get('/all', legacyList('g', 'm'));",
  );
  expect(reviews.map((review) => review.split(": rewritten")[0])).toEqual([
    "readmodels.list(m => m.api.customers, { query: { size: '2' }, transforms: [(doc, app) => doc] })",
    "readmodels.list(models => models.g.m)",
  ]);
  expect(reviews[0]).toContain("transforms receive (document, handle(req))");
  expect((await migrateSource(source)).source).toBe(source);
  const schema = await migrateSource(
    "readmodels.list(m => m.g.m, { schema: s })",
  );
  expect(schema.source).toBe("readmodels.list(m => m.g.m, { schema: s })");
  expect(schema.reviews[0]).toContain("serialized each document as {}");
});
test("legacyGateways keeps old toolkit routes and req.getApp() for legacyGateway, with their import replaced", async () => {
  const old = `const { command, readmodels } = require('old-toolkit').tools.rest;
module.exports = { routes: {
  '/': { post: command(domain => ({ command: domain.sales.customer.create, event: 'readmodel.api_views.customer.create' }), { id: req => req.body.id, payload: req => req.body }), get: readmodels.list(m => m.api_views.customer, { schema: s }) },
  '/:customer_id': readmodels.one(m => m.api_views.customer),
  '/me': (req, res) => res.json(req.getApp().getServices().me()),
} };`;
  const { source, reviews } = await migrateSource(old, {
    legacyGateways: true,
    replacements: {
      "require('old-toolkit').tools.rest":
        "require('event-fabric-next/compat').legacyRest",
    },
  });
  expect(source).toBe(
    old.replace(
      "require('old-toolkit').tools.rest",
      "require('event-fabric-next/compat').legacyRest",
    ),
  );
  expect(reviews).toEqual([]);
});
test("old Gateway helpers and access defaults are flagged; current helpers and data fields are left alone", async () => {
  const old =
    await migrateSource(`router.get('/:customer_id', readmodels.one(models => models.api_views.customer));
router.get('/', readmodels.list(m => m.api.customers, { size: 10 }));
router.use((req, res, next) => { req.getApp(); next(); });
const access = { '*': true, readmodels: {}, domain: { '*': true } };`);
  expect(old.reviews.map((review) => review.split(":")[0])).toEqual([
    "access '*'",
    "readmodels.list(m => m.api.customers, { size",
    "readmodels.one(models => models.api_views.customer)",
    "req.getApp()",
  ]);
  expect(old.reviews[2]).toContain(
    "write readmodel('api_views', 'customer', { id: (req) => req.params.<id> })",
  );
  const current =
    "router.post('/', command(req => ({ stream: { context: 'a', aggregate: 'b', id: req.params.id }, name: 'c' }), { respondWith: 'g.m' }));";
  expect(await migrateSource(current)).toEqual({
    source: current,
    reviews: [],
  });
  const data = "const payload = { event: 'readmodel.api.invoice.create' };";
  expect((await migrateSource(data)).source).toBe(data);
});
test("imports resolve to the intended package during coexistence and after cutover", async () => {
  const { mkdtemp, mkdir, writeFile, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const { pathToFileURL } = await import("node:url");
  const source =
    "import { which } from 'event-fabric-next';\nimport { which as testing } from 'event-fabric-next/testing';\nexport default [which, testing];";
  const folder = await mkdtemp(join(tmpdir(), "event-fabric-packages-"));
  const install = async (root: string, name: string, which: string) => {
    const directory = join(root, "node_modules", name);
    await mkdir(directory, { recursive: true });
    await writeFile(
      join(directory, "package.json"),
      JSON.stringify({
        name,
        type: "module",
        exports: { ".": "./index.js", "./testing": "./index.js" },
      }),
    );
    await writeFile(
      join(directory, "index.js"),
      `export const which = "${which}";`,
    );
  };
  try {
    const run = async (
      phase: "coexistence" | "cutover",
      installed: [string, string][],
    ) => {
      const root = join(folder, phase);
      for (const [name, which] of installed) await install(root, name, which);
      const migrated = await migrateSource(source, { packages: phase });
      await writeFile(join(root, "migrated.mjs"), migrated.source);
      return {
        reviews: migrated.reviews,
        resolved: (await import(pathToFileURL(join(root, "migrated.mjs")).href))
          .default,
      };
    };
    // Coexistence: the vendored old package still owns the name event-fabric.
    expect(
      await run("coexistence", [
        ["event-fabric", "old"],
        ["event-fabric-next", "new"],
      ]),
    ).toEqual({ reviews: [], resolved: ["new", "new"] });
    expect(await run("cutover", [["event-fabric", "new"]])).toEqual({
      reviews: [],
      resolved: ["new", "new"],
    });
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
  const old = await migrateSource(
    "const a = require('event-fabric'); export * from 'event-fabric-next/lib/x';",
  );
  expect(old.source).toBe(
    "const a = require('event-fabric'); export * from 'event-fabric-next/lib/x';",
  );
  expect(old.reviews).toEqual([
    "event-fabric-next/lib/x: the new package exports only event-fabric-next, event-fabric-next/postgres, event-fabric-next/mongo, event-fabric-next/rabbitmq, event-fabric-next/testing, event-fabric-next/compat, event-fabric-next/decorators, event-fabric-next/eslint-plugin, event-fabric-next/migration, event-fabric-next/workflows; import from one of them by hand",
    "event-fabric: during coexistence this is the vendored old package; import event-fabric-next for the new one",
  ]);
});
test("every retained research example is migrated correctly or listed for review", async () => {
  const flagged = [
    "const revision = event.aggregate.revision;",
    "const payload = { event: 'readmodel.api.invoice.create' };",
    "const route = command(domain => ({command: domain.sales.order.create, event:'readmodel.g.m.create'}), {id:req=>req.params.id,payload:req=>req.body,transforms:[x=>x]});",
    "const model={identity:{'sales.order.created':'payload.target'},reactions:{'sales.order.created':[{useAsId:'payload.target'},{settings:{autoCreate:false}},handler]}};",
  ];
  for (const source of flagged) {
    const result = await migrateSource(source);
    expect(result.source, source).toBe(source);
    expect(result.reviews.length, source).toBeGreaterThan(0);
  }
  const task = await migrateSource(
    "module.exports = async (task, app) => app.getServices().mail();",
    { task: true },
  );
  const exports: any = {};
  new Function("exports", task.source)(exports);
  let mailed = 0;
  await exports.run({ services: { mail: () => mailed++ } }, []);
  expect([task.reviews, mailed]).toEqual([[], 1]);
});
test("Task rewrites follow the parameter binding and keep grouping parentheses", async () => {
  const task = await migrateSource(
    "module.exports = async (task, app) => { function other(app) { return app.getServices(); } return [other(app), app.getServices()]; };",
    { task: true },
  );
  expect(task.source).toContain(
    "function other(app) { return app.getServices(); } return [other(ctx), ctx.services];",
  );
  const grouped = await migrateSource(
    "module.exports = async (task, app) => app.getDomain().a.b((s(), 'x')).c((t(), {value:1}), (u(), {})).await('domain.a.b.d');",
    { task: true },
  );
  expect(grouped.source).toContain(
    "ctx.awaitCorrelated({ stream: { context: 'a', aggregate: 'b', id: (s(), 'x') }, name: 'c', payload: (t(), {value:1}), metadata: (u(), {}) }, 'a.b.d')",
  );
  const owned = await migrateSource(
    "const r = (a || b).getDomain().a.b('x').c({}).await('domain.x.y.z');",
  );
  expect(owned.source).toContain("(a || b).awaitCorrelated(");
});
test("old QueryBuilder chains become one find or stream through legacyQuery, keeping the old result shapes", async () => {
  const { source, reviews } = await migrateSource(`const { x } = require("y");
const a = await app.getReadmodels().g.m().find({ filter: { status: ['open', 'paid'] }, sort: [{ created: 'desc' }], size: 10 });
const b = await app.getReadmodels().g.m().filter(f).sort({ total: 'asc' }).from(20).size(0).exec();
const c = await app.getReadmodels().g.m();
const d = await app.getReadmodels().g.m(id);
app.getReadmodels().g.m().find(query).q('acme').stream().pipe(out);
const e = await app.getReadmodels().g.m().find({ filter: { a: app.getCrud() } });`);
  expect(source).toBe(`const { legacyOne, legacyQuery, legacyStream } = require("event-fabric-next/compat");
const { x } = require("y");
const a = await app.getReadmodels().g.m.find(...legacyQuery({ filter: { status: ['open', 'paid'] }, sort: [{ created: 'desc' }], size: 10 }));
const b = await app.getReadmodels().g.m.find(...legacyQuery({ filter: f, sort: { total: 'asc' }, from: 20, size: 0 }));
const c = await app.getReadmodels().g.m.find(...legacyQuery());
const d = await legacyOne(app.getReadmodels().g.m(id));
legacyStream(app.getReadmodels().g.m.stream(...legacyQuery({ ...query, q: 'acme' }))).pipe(out);
const e = await app.getReadmodels().g.m.find(...legacyQuery({ filter: { a: app.getServices().crud } }));`);
  expect(reviews).toEqual([
    "app.getReadmodels().g.m: text search (q) now needs a declared text index and matches case-insensitive substrings and phrases, not Mongo $text; compare the results",
  ]);
  // A `const` holding getReadmodels() reads the same; a reassignable one is left alone.
  const aliased = await migrateSource(`const { x } = require("y");
const rms = req.getApp().getReadmodels(), alias = rms;
const [file] = await alias.g.m(id);
const files = await rms.g.m().filter(f);
let other = req.getApp().getReadmodels();
const [kept] = await other.g.m(id);`);
  expect(aliased.source).toBe(`const { legacyOne, legacyQuery } = require("event-fabric-next/compat");
const { x } = require("y");
const rms = req.getApp().getReadmodels(), alias = rms;
const [file] = await legacyOne(alias.g.m(id));
const files = await rms.g.m.find(...legacyQuery({ filter: f }));
let other = req.getApp().getReadmodels();
const [kept] = await other.g.m(id);`);
  // A bare builder passed on is still a builder where it lands: unchanged and listed.
  const passed = await migrateSource("run(app.getReadmodels().g.m(), agg);");
  expect(passed.source).toBe("run(app.getReadmodels().g.m(), agg);");
  expect(passed.reviews).toEqual([
    expect.stringContaining("app.getReadmodels().g.m(): an old QueryBuilder call."),
  ]);
  const esm = await migrateSource(
    "import x from 'y';\nexport const run = (ctx) => ctx.app.getReadmodels().g.m(id);",
    { packages: "cutover" },
  );
  expect(esm.source).toBe(
    'import { legacyOne } from "event-fabric/compat";\nimport x from \'y\';\nexport const run = (ctx) => legacyOne(ctx.app.getReadmodels().g.m(id));',
  );
});
test("old Aggregate code keeps its metadata merge, callable apply, reads and Revision", async () => {
  const { source, reviews } = await migrateSource(`module.exports = {
  commands: {
    open: (cmd, aggregate) => aggregate.apply.opened(cmd.payload, { tenant: cmd.payload.tenant }),
    close: (cmd, aggregate) => aggregate.apply('closed', {}, cmd.metadata),
    note: (cmd, aggregate) => aggregate.apply.noted({}),
    pick: (cmd, aggregate) => aggregate.apply(cmd.payload.name, {}),
    keep: ({ payload }, aggregate) => aggregate.apply.kept(payload, meta),
    own: ({ payload, metadata }, aggregate) => aggregate.apply.owned(payload, { at: 1 }),
    rest: ({ payload, ...rest }, aggregate) => aggregate.apply.rested(payload, meta),
    spread: (cmd, aggregate) => handler.apply(this, [cmd]),
  },
  rules: [(state) => state.getRevision() > 0],
};
async function step(cmd, aggregate, app) { const other = await app.getDomain().sales.account('x'); }`);
  expect(source).toBe(`const { legacyMetadata } = require("event-fabric-next/compat");
module.exports = {
  commands: {
    open: (cmd, aggregate) => aggregate.apply.opened(cmd.payload, legacyMetadata({ tenant: cmd.payload.tenant }, cmd.metadata)),
    close: (cmd, aggregate) => aggregate.apply.closed({}, legacyMetadata(cmd.metadata, cmd.metadata)),
    note: (cmd, aggregate) => aggregate.apply.noted({}),
    pick: (cmd, aggregate) => aggregate.apply(cmd.payload.name, {}),
    keep: ({ payload, metadata: commandMetadata }, aggregate) => aggregate.apply.kept(payload, legacyMetadata(meta, commandMetadata)),
    own: ({ payload, metadata }, aggregate) => aggregate.apply.owned(payload, legacyMetadata({ at: 1 }, metadata)),
    rest: ({ payload, ...rest }, aggregate) => aggregate.apply.rested(payload, meta),
    spread: (cmd, aggregate) => handler.apply(this, [cmd]),
  },
  rules: [(state) => state.revision > 0],
};
async function step(cmd, aggregate, app) { const other = await app.getDomain().sales.account('x').load(); }`);
  expect(reviews.map((review) => review.split(": ")[0])).toEqual([
    "aggregate.apply(cmd.payload.name, {})",
    "aggregate.apply.rested(payload, meta)",
    "app.getDomain().sales.account('x').load()",
    "state.getRevision()",
  ]);
  // Running the codemod again changes nothing.
  expect((await migrateSource(source)).source).toBe(source);
});
test("old enrichers keep their old timing through legacyEnricher, once", async () => {
  const { source, reviews } = await migrateSource(
    "const a = { eventEnricher: (event, aggregate) => { event.payload.x = aggregate.get('x'); event.metadata.at = event.metadata.timestamp; return { ...event, metadata: { flag: true } }; } };",
  );
  expect(source).toBe(
    "const { legacyEnricher } = require(\"event-fabric-next/compat\");\nconst a = { eventEnricher: legacyEnricher((event, aggregate) => { event.payload.x = aggregate.get('x'); event.metadata.at = event.timestamp; return { ...event, metadata: { flag: true } }; }) };",
  );
  expect(reviews).toEqual([
    "metadata.timestamp: the Event's commit time is now the top-level timestamp, an ISO string; check code that expected a Date or a number",
  ]);
  expect((await migrateSource(source)).source).toBe(source);
  expect(
    (await migrateSource("const a = { eventEnricher: enrich };")).source,
  ).toContain("eventEnricher: legacyEnricher(enrich)");
});
test("grouped keys with their own Identity, query entry or markers become Readmodel groups", async () => {
  const { source, reviews } = await migrateSource(`module.exports = {
  identity: { 'sales.order.created': (e) => e.payload.id, 'sales.order.created:customer': (e) => e.payload.customer },
  query: { 'sales.order.created:batch': (e) => ({ batch: e.payload.batches }) },
  reactions: {
    'sales.order.created:totals': addTotal,
    'sales.order.created:customer': (event, vm) => vm.set('orders', vm.get('orders') + 1),
    'sales.order.created:batch': (event, vm) => vm.set('context', event.context),
    'sales.order.created:mark': [{ settings: { autoCreate: false } }, mark],
  },
};`);
  expect(source).toContain(
    "reactions: {\n    'sales.order.created': [addTotal],\n  }, reactionSets: { customer: { identity: { 'sales.order.created': (e) => e.payload.customer }, reactions: { 'sales.order.created': (event, vm) => vm.set('orders', vm.get('orders') + 1) } }, batch: { identity: { 'sales.order.created': legacyQueryIdentity((e) => ({ batch: e.payload.batches })) }, reactions: { 'sales.order.created': (event, vm) => vm.set('context', event.context.name) } }, mark: { identity: { 'sales.order.created': (e) => e.payload.id }, reactions: { 'sales.order.created': [{ settings: { autoCreate: false } }, mark] } } },",
  );
  expect(source).toContain(
    "identity: { 'sales.order.created': (e) => e.payload.id },\n  reactions",
  );
  expect(source).not.toMatch(/query:|:customer'|:batch'/);
  expect(reviews).toContain(
    "'sales.order.created:customer', 'sales.order.created:batch', 'sales.order.created:mark': moved into reactionSets with their own Identities and markers; they fold the documents they select after the model's own Reactions, in declaration order",
  );
  expect((await migrateSource(source)).source).toBe(source);
  // Grouped entries leave the identity and query maps; the query map's other entries move into identity.
  const mixed = await migrateSource(`module.exports = {
  identity: { 'a.b.c:g': (e) => e.payload.g },
  query: { 'a.b.d': (e) => ({ x: [1, 2] }), 'a.b.c:h': (e) => ({ y: 1 }) },
  reactions: { 'a.b.c:g': f, 'a.b.c:h': h, 'a.b.d': d },
};`);
  expect(mixed.source)
    .toBe(`const { legacyQueryIdentity } = require("event-fabric-next/compat");
module.exports = {
  identity: { 'a.b.d': legacyQueryIdentity((e) => ({ x: [1, 2] }))  },
  reactions: { 'a.b.d': d }, reactionSets: { g: { identity: { 'a.b.c': (e) => e.payload.g }, reactions: { 'a.b.c': f } }, h: { identity: { 'a.b.c': legacyQueryIdentity((e) => ({ y: 1 })) }, reactions: { 'a.b.c': h } } },
};`);
});
test("old Reaction markers and grouped keys keep their old meaning; new markers are left alone", async () => {
  const { source, reviews } = await migrateSource(`const only = require('old-tools').only;
module.exports = {
  identity: { 'sales.order.created': (e) => e.payload.id },
  reactions: {
    'sales.order.created:totals': [only.ifNotExists(), addTotal],
    'sales.order.created:list': (event, vm) => vm.set('listed', true),
    'sales.order.paid': [only.ifExists(), markPaid],
  },
};`);
  expect(source).toBe(`const only = require('old-tools').only;
module.exports = {
  identity: { 'sales.order.created': (e) => e.payload.id },
  reactions: {
    'sales.order.created': [addTotal, (event, vm) => vm.set('listed', true)],
    'sales.order.paid': [only.ifExists(), markPaid],
  },
};`);
  expect(reviews).toEqual([
    "'sales.order.created:totals', 'sales.order.created:list': merged into one Reaction for sales.order.created that runs their functions in this order on the document its Identity selects, as the old groups did",
    "only.ifNotExists(): the old marker is removed, since it allowed creation and still handled existing documents and Saga instances, as an unmarked Reaction does now. The new only.ifNotExists() skips existing ones",
  ]);
  const current =
    "import { only } from 'event-fabric-next';\nexport const r = { 'sales.order.created': [only.ifNotExists(), add] };";
  expect(await migrateSource(current)).toEqual({ source: current, reviews: [] });
  const marked = await migrateSource(
    "const r = { 'a.b.c:x': [{ settings: { autoCreate: false } }, f], 'a.b.c:y': g };",
  );
  expect(marked.reviews[0]).toContain("merge each Event's groups into one Reaction by hand");
});
test("Saga Identities for renamed Event constants come in one pass, one per line in a multi-line map", async () => {
  const symbols = { "domain.a.b.queued": "a.b.queued", "domain.a.b.set": "a.b.set", "domain.a.b.unset": "a.b.unset" };
  const source = `const identity = {
	[domain.a.b.queued]: aggId, // the old one
};

const reactions = {
	[domain.a.b.queued]: [f],
	[domain.a.b.set]: [g],
	[domain.a.b.unset]: [h],
};
module.exports = { identity, reactions };`;
  const once = await migrateSource(source, { symbols, saga: true });
  expect(once.source).toBe(`const identity = {
	["a.b.queued"]: aggId, // the old one
	["a.b.set"]: (event) => event.id,
	["a.b.unset"]: (event) => event.id,
};

const reactions = {
	["a.b.queued"]: [f],
	["a.b.set"]: [g],
	["a.b.unset"]: [h],
};
module.exports = { identity, reactions };`);
  expect((await migrateSource(once.source, { symbols, saga: true })).source).toBe(once.source);
  const commented = await migrateSource(
    "const identity = {\n\t// [domain.a.b.set]: e => e.metadata.saga, // id is generated\n};\nconst reactions = {\n\t[domain.a.b.set]: [g],\n};\nmodule.exports = { identity, reactions };",
    { symbols, saga: true },
  );
  expect(commented.source).toBe(
    "const identity = {\n\t// [domain.a.b.set]: e => e.metadata.saga, // id is generated\n\t[\"a.b.set\"]: (event) => event.id,\n};\nconst reactions = {\n\t[\"a.b.set\"]: [g],\n};\nmodule.exports = { identity, reactions };",
  );
  const empty = await migrateSource(
    "const identity = {};\nconst reactions = {\n\t[domain.a.b.set]: [g],\n};\nmodule.exports = { identity, reactions };",
    { symbols, saga: true },
  );
  expect(empty.source).toBe(
    "const identity = {\n\t[\"a.b.set\"]: (event) => event.id,\n};\nconst reactions = {\n\t[\"a.b.set\"]: [g],\n};\nmodule.exports = { identity, reactions };",
  );
  const absent = await migrateSource(
    "const reactions = {\n\t[domain.a.b.set]: [g],\n};\n\nmodule.exports = {\n\treactions,\n};",
    { symbols, saga: true },
  );
  expect(absent.source).toBe(
    "const reactions = {\n\t[\"a.b.set\"]: [g],\n};\n\nmodule.exports = {\n\tidentity: {\n\t\t[\"a.b.set\"]: (event) => event.id,\n\t},\n\treactions,\n};",
  );
});
test("old Sagas keep a new instance per Event without an Identity and drop the initialState the old builder ignored", async () => {
  const { source, reviews } = await migrateSource(
    `module.exports = {
  // Ignored by the old builder.
  initialState: { step: 0 },
  identity: { 'sales.order.paid': (e) => e.payload.saga },
  reactions: {
    'sales.order.created': (event, saga, app) => app.tell(x),
    'sales.order.paid': (event, saga) => saga.set('paid', true),
  },
};`,
    { saga: true },
  );
  expect(source).toBe(`module.exports = {
  identity: { 'sales.order.paid': (e) => e.payload.saga, 'sales.order.created': (event) => event.id },
  reactions: {
    'sales.order.created': (event, saga, app) => app.tell(x),
    'sales.order.paid': (event, saga) => saga.set('paid', true),
  },
};`);
  expect(reviews.map((review) => review.split(":")[0])).toEqual([
    "identity",
    "initialState",
  ]);
  const inline = await migrateSource(
    "const saga = { reactions: { 'a.b.c': f } };",
    { saga: true },
  );
  expect(inline.source).toBe(
    "const saga = { identity: { 'a.b.c': (event) => event.id }, reactions: { 'a.b.c': f } };",
  );
  const declared = await migrateSource(
    `const identity = { 'a.b.paid': (e) => e.payload.saga };
const reactions = { ['a.b.created']: f, 'a.b.paid': g };
module.exports = { identity, reactions };`,
    { saga: true },
  );
  expect(declared.source)
    .toBe(`const identity = { 'a.b.paid': (e) => e.payload.saga, 'a.b.created': (event) => event.id };
const reactions = { ['a.b.created']: f, 'a.b.paid': g };
module.exports = { identity, reactions };`);
  const alone = await migrateSource(
    "const initialState = {};\nconst reactions = { 'a.b.c': f };\nmodule.exports = { initialState, reactions };",
    { saga: true },
  );
  expect(alone.source).toBe(
    "const initialState = {};\nconst reactions = { 'a.b.c': f };\nmodule.exports = { identity: { 'a.b.c': (event) => event.id }, reactions };",
  );
  expect(alone.reviews.map((review) => review.split(":")[0])).toEqual([
    "identity",
    "initialState",
  ]);
  const reassigned = await migrateSource(
    "let reactions = { 'a.b.c': f }; reactions = g; module.exports = { reactions };",
    { saga: true },
  );
  expect(reassigned.source).toBe(
    "let reactions = { 'a.b.c': f }; reactions = g; module.exports = { reactions };",
  );
});
test("old auxiliary Application accessors are listed", async () => {
  const { reviews } = await migrateSource(
    "app.getLogger().info('x'); app.getWorkers().mail.send(m);",
  );
  expect(reviews.map((review) => review.split(":")[0])).toEqual([
    "app.getLogger()",
    "app.getWorkers()",
  ]);
});
test("an old query map becomes Identities through legacyQueryIdentity; Aggregate existence checks are kept", async () => {
  const { source, reviews } = await migrateSource(`module.exports = {
  identity: { 'sales.order.created': (e) => e.payload.id },
  query: { 'sales.order.paid': (e) => ({ customer: e.payload.customer, status: ['open', 'due'] }) },
  reactions: { 'sales.order.paid': (e, vm) => vm.set('paid', true) },
};
const aggregate = { commands: { create: [only.ifNotExists(), (cmd, a) => a.apply.created({})] } };`);
  expect(source).toBe(`const { legacyQueryIdentity } = require("event-fabric-next/compat");
module.exports = {
  identity: { 'sales.order.created': (e) => e.payload.id, 'sales.order.paid': legacyQueryIdentity((e) => ({ customer: e.payload.customer, status: ['open', 'due'] })) },
  reactions: { 'sales.order.paid': (e, vm) => vm.set('paid', true) },
};
const aggregate = { commands: { create: [only.ifNotExists(), (cmd, a) => a.apply.created({})] } };`);
  expect(reviews).toEqual([
    "query: each entry moved into identity through legacyQueryIdentity, which translates the old filter; the Reaction still runs on every matching document",
  ]);
  const alone = await migrateSource(
    "const m = { query: { 'a.b.c': q }, reactions: { 'a.b.c': f } };",
  );
  expect(alone.source).toContain("identity: { 'a.b.c': legacyQueryIdentity(q) }, reactions");
});
test("a callable apply with a computed string name becomes a computed apply", async () => {
  const { source, reviews } = await migrateSource(`const add = (aggregate, type, items) => {
  const eventName = \`\${type}Added\`;
  aggregate.apply(eventName, items);
  let unset = 'primaryContactUnset';
  if (type === 'address') unset = type ? 'primaryAddressUnset' : \`\${type}Unset\`;
  aggregate.apply(unset, {});
  aggregate.apply(\`\${type}Updated\`, items);
  aggregate.apply('plain' + type, items);
};
const call = (handler, self, args, name) => { handler.apply(self, args); handler.apply(name, args); };`);
  expect(source).toBe(`const add = (aggregate, type, items) => {
  const eventName = \`\${type}Added\`;
  aggregate.apply[eventName](items);
  let unset = 'primaryContactUnset';
  if (type === 'address') unset = type ? 'primaryAddressUnset' : \`\${type}Unset\`;
  aggregate.apply[unset]({});
  aggregate.apply[\`\${type}Updated\`](items);
  aggregate.apply['plain' + type](items);
};
const call = (handler, self, args, name) => { handler.apply(self, args); handler.apply(name, args); };`);
  expect(reviews).toEqual([]);
  expect((await migrateSource(source)).source).toBe(source);
});
test("a State's attributes become get(), which keeps the Stream id the old attributes held", async () => {
  const { source } = await migrateSource(`const commands = {
  move: [
    async (cmd, aggregate, app) => { const { owner } = (await app.getDomain().sales.account(cmd.payload.id)).attributes; },
    only.after((cur, pre, events, cmd, app) => app.record(cur.attributes, pre.attributes.total)),
  ],
};
const loaded = async (app) => (await app.getDomain().sales.account('a').load()).attributes;
const rules = [(state) => state.attributes.open];
const other = (cur, response) => cur.attributes.url + response.attributes.url;`);
  expect(source).toBe(`const commands = {
  move: [
    async (cmd, aggregate, app) => { const { owner } = (await app.getDomain().sales.account(cmd.payload.id).load()).get(); },
    only.after((cur, pre, events, cmd, app) => app.record(cur.get(), pre.get().total)),
  ],
};
const loaded = async (app) => (await app.getDomain().sales.account('a').load()).get();
const rules = [(state) => state.get().open];
const other = (cur, response) => cur.attributes.url + response.attributes.url;`);
  expect((await migrateSource(source)).source).toBe(source);
});
test("old Revision reads, envelope fields and helper applies are rewritten or listed", async () => {
  const { source, reviews } = await migrateSource(`const events = {
  opened: (event, state) => state.set('label', \`\${event.context}-\${event.metadata.timestamp}\`),
};
const reactions = { 'a.b.c': (event, vm) => vm.set('at', event.metadata.timestamp) };
const rules = [(state) => state._revision + state.get('_revision') + state.attributes._revision];
const read = (cmd, aggregate) => aggregate._aggregateModel.attributes;
const helper = (aggregate, payload) => aggregate.apply('opened', payload);
const named = (aggregate, payload, meta) => aggregate.apply.opened(payload, meta);
const log = (cmd) => \`\${cmd.context}\` + cmd.metadata.timestamp;`);
  expect(source).toBe(`const events = {
  opened: (event, state) => state.set('label', \`\${event.context.name}-\${event.timestamp}\`),
};
const reactions = { 'a.b.c': (event, vm) => vm.set('at', event.timestamp) };
const rules = [(state) => state.revision + state.revision + state.revision];
const read = (cmd, aggregate) => aggregate.get();
const helper = (aggregate, payload) => aggregate.apply.opened(payload);
const named = (aggregate, payload, meta) => aggregate.apply.opened(payload, meta);
const log = (cmd) => \`\${cmd.context}\` + cmd.metadata.timestamp;`);
  expect(reviews.map((review) => review.split(":")[0])).toEqual([
    "aggregate._aggregateModel.attributes",
    "aggregate.apply.opened(payload, meta)",
    "cmd.context",
    "cmd.metadata.timestamp",
    "context",
    "metadata.timestamp",
    "state._revision",
    "state.attributes._revision",
    "state.get('_revision')",
  ]);
});

test("enricher shorthand and methods migrate to valid properties", async () => {
  for (const property of [
    "eventEnricher",
    "eventEnricher(event, state) { event.metadata.label = state.get('label'); }",
  ]) {
    const input = `const eventEnricher = (event) => {}; const aggregate = { ${property} };`;
    const first = await migrateSource(input);
    expect(first.source).toContain("eventEnricher: legacyEnricher(");
    expect((await migrateSource(first.source)).source).toBe(first.source);
  }
});

test("single-document compatibility supports computed names and is idempotent", async () => {
  for (const repository of ["app.getReadmodels().sales.account", "app.getReadmodels()[path[0]][path[1]]"]) {
    const first = await migrateSource(`const [document] = await ${repository}(id);`);
    expect(first.source).toContain(`legacyOne(${repository}(id))`);
    expect((await migrateSource(first.source)).source).toBe(first.source);
  }
});

test("CommonJS top-level returns and destructured Task args stay valid", async () => {
  expect((await migrateSource("if (!module.parent) return; module.exports = {};" )).source)
    .toBe("if (!module.parent) return; module.exports = {};");
  const first = await migrateSource(`module.exports = async (task, app) => {
    const { args } = task;
    return [args, app.getServices()];
  };`, { task: true });
  expect((await migrateSource(first.source, { task: true })).source).toBe(first.source);
  const module = { exports: {} as { run?: (ctx: any, args: string[]) => unknown } };
  new Function("exports", first.source)(module.exports);
  expect(await module.exports.run!({ services: "service" }, ["argument"]))
    .toEqual([["argument"], "service"]);
});

test("temporary Aggregate fields stay connected to Rule reads", async () => {
  const first = await migrateSource(`const commands = { update: [
    (cmd, aggregate) => { aggregate._aggregateModel.__work = cmd.payload; },
    only.after((current, previous) => { const work = current.__work; delete current.__work; return work; }),
  ] };`);
  expect(first.source).toContain("aggregate.locals.__work = cmd.payload");
  expect(first.source).toContain("current.locals.__work");
  expect(first.source).toContain("delete current.locals.__work");
  expect((await migrateSource(first.source)).source).toBe(first.source);
});

test("destructured Event envelope helpers require explicit review", async () => {
  const result = await migrateSource(`const helper = ({ metadata, context }, vm) => {
    vm.set('at', new Date(metadata.timestamp));
    vm.set('context', context);
  };`);
  expect(result.reviews.some(review => review.includes("destructured") && review.includes("timestamp"))).toBe(true);
  expect(result.reviews.some(review => review.includes("destructured") && review.includes("context"))).toBe(true);
});

const taskRun = (source: string) => {
  const exports: { run?: (ctx: any, args: string[]) => unknown } = {};
  new Function("exports", source)(exports);
  return exports.run!;
};
const taskHelper = "Task helper: replace the old task helper with args, logger, stdin or Application-owned clients";

test("other Task export shapes become exports.run = (ctx, args) or are listed", async () => {
  const converted: [string, string, string[]][] = [
    ["module.exports = async ({ args }, app) => { return [args, app.getServices()]; };",
      "exports.run = async (ctx, args) => { return [args, ctx.services]; };", []],
    ["module.exports = async (task) => task.args;", "exports.run = async (ctx, args) => task.args;", [taskHelper]],
    ["module.exports = async task => task.args;", "exports.run = async (ctx, args) => task.args;", [taskHelper]],
    ["module.exports = async () => 1;", "exports.run = async (ctx, args) => 1;", []],
    ["module.exports = function () { return 1; };", "exports.run = function (ctx, args) { return 1; };", []],
  ];
  for (const [input, output, reviews] of converted) {
    const first = await migrateSource(input, { task: true });
    expect([first.source, first.reviews], input).toEqual([output, reviews]);
    expect((await migrateSource(first.source, { task: true })).source).toBe(first.source);
  }
  expect(await taskRun((await migrateSource(converted[0]![0], { task: true })).source)({ services: "service" }, ["a"]))
    .toEqual([["a"], "service"]);
  const exportReview = "module.exports: this Task export is not a function of (task, app), ({ args }, app), (task) or (); write exports.run = async (ctx, args) => … by hand, with ctx for the Application and args for task.args";
  for (const input of [
    "module.exports = async ({ args: list }, app) => list;",
    "const task = async (task, app) => 1; module.exports = task;",
    "module.exports = {};",
  ]) {
    const result = await migrateSource(input, { task: true });
    expect([result.source, result.reviews], input).toEqual([input, [exportReview]]);
  }
  expect((await migrateSource("module.exports = { run };", { task: true })).reviews).toEqual([]);
});

test("a shorthand Task Application property keeps its key", async () => {
  const first = await migrateSource("module.exports = async (task, app) => { const options = { id: 1, app }; return options; };", { task: true });
  expect(first.source).toBe("exports.run = async (ctx, args) => { const options = { id: 1, app: ctx }; return options; };");
  expect((await migrateSource(first.source, { task: true })).source).toBe(first.source);
  expect(await taskRun(first.source)("context", [])).toEqual({ id: 1, app: "context" });
});

test("an old base() on the Task Application is flagged with its receiver rewritten", async () => {
  const first = await migrateSource(
    "module.exports = async (task, app) => { const collection = app.getReadmodels().g.m().base(); return collection.find(); };",
    { task: true },
  );
  expect(first.source).toBe("exports.run = async (ctx, args) => { const collection = ctx.readmodels.g.m().base(); return collection.find(); };");
  expect(first.reviews).toEqual([
    "app.getReadmodels().g.m().base(): the old base() returned the Mongo collection synchronously; inside an awaited chain on getReadmodels().<group>.<model>() write (await <repository>.native()), or port the query to <repository>.find(filter, options), by hand",
  ]);
  expect((await migrateSource(first.source, { task: true })).source).toBe(first.source);
});

test("the Task context passed on to a helper is listed", async () => {
  const first = await migrateSource(
    "module.exports = async (task, app) => { const local = (a) => a.getServices(); return [local(app), lib.load(app, 1), new Report({ app }), app.readAll(), app.getServices()]; };",
    { task: true },
  );
  expect(first.source).toBe("exports.run = async (ctx, args) => { const local = (a) => a.getServices(); return [local(ctx), lib.load(ctx, 1), new Report({ app: ctx }), ctx.readAll(), ctx.services]; };");
  const passed = (callee: string) => `${callee}(…): passes the Task context on, whose fields (config, services, readmodels, logger, sendCommand, awaitCorrelated) replace the Application's accessors, so a helper calling getServices(), getReadmodels() or getDomain() on it fails; update the helper, or pass it what it reads, by hand`;
  expect(first.reviews).toEqual([passed("Report"), passed("lib.load"), passed("local")]);
  expect((await migrateSource(first.source, { task: true })).source).toBe(first.source);
});

test("Task config reads are listed", async () => {
  const first = await migrateSource(
    "module.exports = async (task, app) => { const { config } = app.wire.app; return [config.mongoose.uri, app.wire.app.config.rootPath]; };",
    { task: true },
  );
  expect(first.source).toBe("exports.run = async (ctx, args) => { const { config } = ctx; return [config.mongoose.uri, ctx.config.rootPath]; };");
  const config = (text: string, now: string) => `${text}: now ${now}; ctx.config is the EventFabric config, so the Application's own settings (such as a database URI) read undefined from it; give them to an Application-owned service in setupServices, or read them from the environment, by hand`;
  expect(first.reviews).toEqual([config("app.wire.app.config", "ctx.config"), config("app.wire.app", "ctx")]);
  expect((await migrateSource(first.source, { task: true })).source).toBe(first.source);
});
test("Aggregate Event handlers read the Event's timestamp; clock reads are the Application's to fix", async () => {
  const source = `const commands = {
	go: [async ({ payload, metadata }, aggregate, app) => {
		use(metadata.timestamp);
	}],
};
const events = {
	added: ({ payload, metadata }, aggregate) => {
		aggregate.set('x', payload.x);

		const { timestamp } = metadata;

		aggregate.set('at', timestamp);
	},
	moved: ({ payload, metadata }, aggregate) => {
		aggregate.set('at', new Date(metadata.timestamp));
		aggregate.set('by', metadata.user);
	},
	seen: ({ payload, metadata }, aggregate) => {
		aggregate.set('seen', new Date(metadata.timestamp));
	},
};
const helper = ({ metadata }) => new Date(metadata.timestamp);
module.exports = { commands, events };
`;
  const result = await migrateSource(source);
  expect(result.source).toBe(`const commands = {
	go: [async ({ payload, metadata }, aggregate, app) => {
		use(metadata.timestamp ?? app.now);
	}],
};
const events = {
	added: ({ payload, timestamp }, aggregate) => {
		aggregate.set('x', payload.x);

		aggregate.set('at', timestamp);
	},
	moved: ({ payload, metadata, timestamp }, aggregate) => {
		aggregate.set('at', new Date(timestamp));
		aggregate.set('by', metadata.user);
	},
	seen: ({ payload, timestamp }, aggregate) => {
		aggregate.set('seen', new Date(timestamp));
	},
};
const helper = ({ metadata }) => new Date(metadata.timestamp);
module.exports = { commands, events };
`);
  // Only the helper outside the maps is left to review.
  expect(result.reviews).toEqual(["destructured metadata.timestamp: on an Event, read its top-level timestamp (an ISO string); update this helper and its callers by hand"]);
  expect((await migrateSource(result.source)).source).toBe(result.source);
  // A Reaction reading the old envelope's commit time reads the Event's; a clock read is left alone.
  const reaction = "const reactions = {\n\t'a.b.c': [(event, vm) => { vm.set('at', new Date(event.metadata.timestamp)); vm.set('now', new Date()); }],\n};\nmodule.exports = { reactions };\n";
  expect((await migrateSource(reaction)).source).toBe(
    "const reactions = {\n\t'a.b.c': [(event, vm) => { vm.set('at', new Date(event.timestamp)); vm.set('now', new Date()); }],\n};\nmodule.exports = { reactions };\n",
  );
});
test("Aggregate getLogger and the old Task fork are rewritten", async () => {
  expect((await migrateSource("const f = (app) => app.getLogger().error('x');\n", { aggregate: true })).source).toBe(
    "const f = (app) => app.logger.error('x');\n",
  );
  const fork = await migrateSource(`const corePath = process.cwd();
const dis = \`\${corePath}/distributed.js\`;
const env = { OLD_ENV: process.env.OLD_ENV };
fork(dis, ['task', 'a.task.js', 'x=1'], { cwd: corePath, env });
`);
  expect(fork.source).toBe(`const corePath = process.cwd();
// The Task runs on EventFabric's Task runner, with this process's environment.
const cli = require('node:path').join(require('node:path').dirname(require.resolve('event-fabric-next')), 'cli/main.js');
const env = process.env;
fork(cli, ['task', 'a.task.js', '--', 'x=1'], { cwd: corePath, env });
`);
  expect((await migrateSource(fork.source)).source).toBe(fork.source);
});
test("A wait kept as the old Event throws a Rejection's Outcome, and its aggregate.id is the Stream id", async () => {
  const source = `const run = async (req) => {
	const app = req.getApp();
	const claim = await app.getDomain().a.b().c({}).await('domain.x.y.z');
	return claim.aggregate.id;
};
const other = async (req) => {
	const app = req.getApp();
	const c = await app.getDomain().a.b().c({}).await('a.b.done');
	return c.aggregate.id;
};
`;
  const result = await migrateSource(source, { legacyGateways: true });
  expect(result.source).toBe(`const run = async (req) => {
	const app = req.getApp();
	const waited = await app.awaitCorrelated({ stream: { context: 'a', aggregate: 'b' }, name: 'c', payload: {} }, 'x.y.z');
	// The old await threw on a rejection.
	if (waited.status !== 'accepted')
		throw waited.outcome;
	const claim = waited.event;
	return claim.stream.id;
};
const other = async (req) => {
	const app = req.getApp();
	const c = await app.getDomain().a.b().c({}).await('a.b.done');
	return c.stream.id;
};
`);
  expect(result.reviews).toEqual([]);
  expect((await migrateSource(result.source, { legacyGateways: true })).source).toBe(result.source);
});
