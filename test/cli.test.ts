import { expect, test, vi } from "vitest";
import { mkdtemp, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runCLI } from "../src/cli/index.js";
test.each(["event-fabric", "event-fabric-next"])("types writes/checks declarations for %s without running plugins or consumers", async (module) => {
  const folder = await mkdtemp(join(tmpdir(), "event-fabric-cli-"));
  const output: string[] = [];
  try {
    await writeFile(
      join(folder, "event-fabric.config.mjs"),
      `export const config = { name: 'types', typesPath: 'generated.d.ts', ${module === "event-fabric" ? "" : `typesModule: '${module}',`} plugins: [() => { throw Error('plugin must not run'); }], setupServices: () => { throw Error('setupServices must not run'); }, aggregates: [{ context:'sales', name:'account', commands:{add:{schema:{type:'number'},steps:[],rules:[],preLoad:[]}}, events:{added:()=>{}}, initialState:{} }] };`,
    );
    const io = {
      cwd: folder,
      out: (value: string) => {
        output.push(value);
      },
      err: (value: string) => {
        output.push(value);
      },
    };
    expect(
      await runCLI(["types", "--config", "event-fabric.config.mjs"], io),
    ).toBe(0);
    const generated = await readFile(join(folder, "generated.d.ts"), "utf8");
    expect(generated).toContain(`declare module "${module}"`);
    expect(generated).toMatch(
      /"sales": \{\s+"account": \{\s+events: \{\s+"added": any;/,
    );
    expect(
      await runCLI(
        ["types", "--check", "--config", "event-fabric.config.mjs"],
        io,
      ),
    ).toBe(0);
    await writeFile(join(folder, "generated.d.ts"), "stale");
    expect(
      await runCLI(
        ["types", "--check", "--config", "event-fabric.config.mjs"],
        io,
      ),
    ).toBe(1);
    expect(output.join("\n")).toContain("stale");
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
test("Tasks run with connected public services and argument passthrough while starting no consumers/hooks", async () => {
  const folder = await mkdtemp(join(tmpdir(), "event-fabric-cli-task-"));
  const errors: string[] = [];
  try {
    const storeURL = new URL("../src/event-store/memory.ts", import.meta.url)
      .href;
    const transportURL = new URL("../src/transport/memory.ts", import.meta.url)
      .href;
    await writeFile(
      join(folder, "config.mjs"),
      `import { createInMemoryEventStore } from '${storeURL}'; import { createInMemoryTransport } from '${transportURL}'; export const config = { name:'tasks', store:createInMemoryEventStore(), transport:createInMemoryTransport(), services:{ resultPath:${JSON.stringify(join(folder, "result.json"))} }, plugins:[()=>{throw Error('must not run plugins')}], workers:{ task:{handles:['task'],execute:()=>{throw Error('must not consume')}} } };`,
    );
    await writeFile(
      join(folder, "task.mjs"),
      `import { writeFile } from 'node:fs/promises'; export async function run(ctx,args) { await writeFile(ctx.services.resultPath,JSON.stringify({args,events:await ctx.readAll(),name:ctx.config.name,awaitCorrelated:typeof ctx.awaitCorrelated})); }`,
    );
    expect(
      await runCLI(
        [
          "task",
          "./task.mjs",
          "--config",
          "./config.mjs",
          "--",
          "--flag",
          "value",
        ],
        { cwd: folder, err: (value) => errors.push(value) },
      ),
    ).toBe(0);
    expect(errors).toEqual([]);
    expect(
      JSON.parse(await readFile(join(folder, "result.json"), "utf8")),
    ).toEqual({
      args: ["--flag", "value"],
      events: [],
      name: "tasks",
      awaitCorrelated: "function",
    });
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
test("a Task gets the services setupServices built, and their cleanup runs after it, even when it fails", async () => {
  const folder = await mkdtemp(join(tmpdir(), "event-fabric-cli-setup-"));
  const errors: string[] = [];
  try {
    const storeURL = new URL("../src/event-store/memory.ts", import.meta.url)
      .href;
    const transportURL = new URL("../src/transport/memory.ts", import.meta.url)
      .href;
    const log = join(folder, "log.txt");
    await writeFile(
      join(folder, "config.mjs"),
      `import { appendFileSync } from 'node:fs'; import { createInMemoryEventStore } from '${storeURL}'; import { createInMemoryTransport } from '${transportURL}'; const log = (line) => appendFileSync(${JSON.stringify(log)}, line + '\\n'); export const config = { name:'tasks', store:createInMemoryEventStore(), transport:createInMemoryTransport(), plugins:[()=>{throw Error('must not run plugins')}], async setupServices({ resources }) { log('setup'); resources.defer(() => log('cleanup')); return { log }; } };`,
    );
    await writeFile(
      join(folder, "task.mjs"),
      `export async function run(ctx, [outcome]) { ctx.services.log('task'); if (outcome === 'fail') throw Error('task failed'); }`,
    );
    const task = (outcome: string) =>
      runCLI(
        ["task", "./task.mjs", "--config", "./config.mjs", "--", outcome],
        {
          cwd: folder,
          err: (value) => errors.push(value),
        },
      );
    expect(await task("ok")).toBe(0);
    expect(await task("fail")).toBe(1);
    expect(errors).toEqual(["task failed"]);
    expect((await readFile(log, "utf8")).trim().split("\n")).toEqual([
      "setup",
      "task",
      "cleanup",
      "setup",
      "task",
      "cleanup",
    ]);
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
test("migrate dry-run prints quoted SQL without loading plugins or opening the database", async () => {
  const folder = await mkdtemp(join(tmpdir(), "event-fabric-cli-migrate-"));
  const output: string[] = [];
  try {
    const storeURL = new URL("../src/event-store/postgres.ts", import.meta.url)
      .href;
    const transportURL = new URL("../src/transport/memory.ts", import.meta.url)
      .href;
    await writeFile(
      join(folder, "config.mjs"),
      `import { createPostgresEventStore } from '${storeURL}'; import { createInMemoryTransport } from '${transportURL}'; export const config={ name:'dry',store:createPostgresEventStore({connectionString:'postgres://localhost:1/no_database',schema:'quoted-schema'}),transport:createInMemoryTransport(),plugins:[()=>{throw Error('must not run')}] };`,
    );
    expect(
      await runCLI(["migrate", "--dry-run", "--config", "./config.mjs"], {
        cwd: folder,
        out: (value) => output.push(value),
      }),
    ).toBe(0);
    expect(output.join("\n")).toContain(
      'CREATE SCHEMA IF NOT EXISTS "quoted-schema"',
    );
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
test("operators inspect/replay dead letters and can skip only the confirmed exact halted Event", async () => {
  const folder = await mkdtemp(join(tmpdir(), "event-fabric-cli-ops-"));
  const output: string[] = [],
    errors: string[] = [];
  try {
    const storeURL = new URL("../src/event-store/memory.ts", import.meta.url)
        .href,
      transportURL = new URL("../src/transport/memory.ts", import.meta.url)
        .href,
      authoringURL = new URL("../src/authoring/index.ts", import.meta.url).href;
    const configPath = join(folder, "config.mjs");
    // The log below holds sales.account.added, so the config must declare it.
    await writeFile(
      configPath,
      `import { createInMemoryEventStore } from '${storeURL}'; import { createInMemoryTransport } from '${transportURL}'; import { compile } from '${authoringURL}'; export const config = { name:'ops',store:createInMemoryEventStore(),transport:createInMemoryTransport(),aggregates:[compile('sales','account',{commands:{},events:{added:()=>{}}})] };`,
    );
    const { config } = await import(new URL(`file://${configPath}`).href);
    const message = {
      id: "message",
      name: "mail",
      payload: {},
      metadata: {},
      schemaVersion: 1,
      timestamp: "2026-01-01T00:00:00Z",
      correlationId: "chain",
      attempt: 5,
    };
    await config.store.recordDeadLetter({
      id: "letter",
      kind: "worker",
      message: { id: "message", kind: "message", message, queue: "mail" },
      error: "provider",
      attempts: 5,
      createdAt: "2026-01-01T00:00:00Z",
    });
    const io = {
      cwd: folder,
      out: (value: string) => {
        output.push(value);
      },
      err: (value: string) => {
        errors.push(value);
      },
    };
    expect(
      await runCLI(["dead-letters", "list", "--config", "./config.mjs"], io),
    ).toBe(0);
    expect(output.join("\n")).toContain("provider");
    expect(
      await runCLI(
        ["dead-letters", "replay", "letter", "--config", "./config.mjs"],
        io,
      ),
    ).toBe(0);
    expect(await config.store.pendingOutbox()).toMatchObject([
      { message: { id: "message", attempt: 1 } },
    ]);
    const [event] = await config.store.appendToStream(
      { context: "sales", aggregate: "account", id: "a" },
      0,
      [
        {
          id: "halted",
          name: "added",
          payload: {},
          metadata: {},
          schemaVersion: 1,
          timestamp: message.timestamp,
          commandId: "command",
          correlationId: "chain",
        },
      ],
    );
    await config.store.setConsumerStatus({
      group: "saga:test",
      partition: 0,
      state: "halted",
      checkpoint: null,
      eventId: "halted",
      position: event.position,
      attempts: 5,
      error: "bug",
    });
    expect(
      await runCLI(
        ["consumers", "status", "--json", "--config", "./config.mjs"],
        io,
      ),
    ).toBe(0);
    expect(output.join("\n")).toContain("saga:test");
    expect(
      await runCLI(
        [
          "consumers",
          "skip",
          "saga:test",
          "--event",
          "other",
          "--yes",
          "--config",
          "./config.mjs",
        ],
        io,
      ),
    ).toBe(1);
    expect(
      await runCLI(
        [
          "consumers",
          "skip",
          "saga:test",
          "--event",
          "halted",
          "--config",
          "./config.mjs",
        ],
        { ...io, confirm: async () => false },
      ),
    ).toBe(1);
    expect(await config.store.checkpoint("saga:test")).toBeNull();
    expect(
      await runCLI(
        [
          "consumers",
          "skip",
          "saga:test",
          "--event",
          "halted",
          "--yes",
          "--who",
          "operator",
          "--config",
          "./config.mjs",
        ],
        io,
      ),
    ).toBe(0);
    expect(await config.store.checkpoint("saga:test")).toEqual(event.position);
    expect(await config.store.deadLetters()).toMatchObject([
      { kind: "skipped-event", who: "operator", eventId: "halted" },
    ]);
    const retire = ["consumers", "retire", "saga:test", "--config", "./config.mjs"];
    expect(await runCLI(retire, { ...io, confirm: async () => false })).toBe(1);
    expect(await config.store.checkpoint("saga:test")).toEqual(event.position);
    expect(await runCLI([...retire, "--yes"], io)).toBe(0);
    expect(await config.store.checkpoint("saga:test")).toBeNull();
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
test("run exits with 1 once the broker connection is lost, after closing the Application", async () => {
  const folder = await mkdtemp(join(tmpdir(), "event-fabric-cli-lost-"));
  const errors: string[] = [],
    output: string[] = [];
  try {
    const url = (path: string) => new URL(path, import.meta.url).href;
    const configPath = join(folder, "config.mjs");
    await writeFile(
      configPath,
      `import {createInMemoryEventStore} from '${url("../src/event-store/memory.ts")}';import {createInMemoryTransport} from '${url("../src/transport/memory.ts")}';const transport=createInMemoryTransport();let connected=true;export const lose=()=>{connected=false};export const records=[];const logger={info(){},debug(){},warn(){},error(fields,msg){records.push(msg)},child(){return logger}};export const config={name:'lost',store:createInMemoryEventStore(),transport:{...transport,connected:()=>connected},pollInterval:5,logger};`,
    );
    const module = await import(new URL(`file://${configPath}`).href);
    const close = vi.spyOn(module.config.store, "close");
    const running = runCLI(["run", "--config", "./config.mjs"], {
      cwd: folder,
      out: (value) => void output.push(value),
      err: (value) => void errors.push(value),
    });
    await expect.poll(() => output).toContain("Started lost");
    module.lose();
    expect(await running).toBe(1);
    expect(errors).toEqual([
      "Broker connection lost; exiting so the supervisor restarts this instance",
    ]);
    expect(module.records).toContain("Broker connection lost; exiting");
    expect(close).toHaveBeenCalled();
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
test("run honors component selectors and shuts down on the supplied signal", async () => {
  const folder = await mkdtemp(join(tmpdir(), "event-fabric-cli-run-"));
  const errors: string[] = [],
    output: string[] = [];
  const controller = new AbortController();
  try {
    const url = (path: string) => new URL(path, import.meta.url).href;
    const configPath = join(folder, "config.mjs");
    await writeFile(
      configPath,
      `import {createInMemoryEventStore} from '${url("../src/event-store/memory.ts")}';import {createInMemoryTransport} from '${url("../src/transport/memory.ts")}';import {compile} from '${url("../src/authoring/index.ts")}';export const config={name:'selected',store:createInMemoryEventStore(),transport:createInMemoryTransport(),aggregates:[compile('sales','account',{commands:{add:(c,a)=>a.apply.added(c.payload)},events:{added:(e,s)=>s.set('total',e.payload.amount)}})],pollInterval:5};`,
    );
    const { config } = await import(new URL(`file://${configPath}`).href);
    const running = runCLI(
      ["run", "--only", "commands:selected", "--config", "./config.mjs"],
      {
        cwd: folder,
        signal: controller.signal,
        out: (value) => {
          output.push(value);
        },
        err: (value) => {
          errors.push(value);
        },
      },
    );
    await expect.poll(() => output).toContain("Started selected");
    const { createCommand } = await import("../src/id.js");
    expect(
      await config.transport.send(
        createCommand({
          stream: { context: "sales", aggregate: "account", id: "a" },
          name: "add",
          payload: { amount: 3 },
        }),
      ),
    ).toMatchObject({ status: "accepted" });
    controller.abort();
    expect(await running).toBe(0);
    expect(errors).toEqual([]);
  } finally {
    controller.abort();
    await rm(folder, { recursive: true, force: true });
  }
});
test.each(["api", "auto", "manual"])(
  "manual rebuild of group %s stays shadowed until a later swap catches up and replaces the active generation",
  async (group) => {
    const folder = await mkdtemp(join(tmpdir(), "event-fabric-cli-rebuild-"));
    const output: string[] = [],
      errors: string[] = [];
    try {
      const url = (path: string) => new URL(path, import.meta.url).href;
      const configPath = join(folder, "config.mjs");
      await writeFile(
        configPath,
        `import {createInMemoryEventStore} from '${url("../src/event-store/memory.ts")}';import {createInMemoryTransport} from '${url("../src/transport/memory.ts")}';import {createMemoryReadmodelStore} from '${url("../src/adapters/memory-readmodel.ts")}';import {compile} from '${url("../src/authoring/index.ts")}';export const config={name:'rebuild',store:createInMemoryEventStore(),transport:createInMemoryTransport(),aggregates:[compile('sales','account',{events:{added:()=>{}}})],readmodels:{${group}:{store:createMemoryReadmodelStore(),models:{account:{schema:{type:'object',properties:{total:{type:'number'}}},reactions:{'sales.account.added':(e,vm)=>vm.set('total',e.payload.amount)}}}}}};`,
      );
      const { config } = await import(new URL(`file://${configPath}`).href);
      const event = (id: string, amount: number) => ({
        id,
        name: "added",
        payload: { amount },
        metadata: {},
        timestamp: "2026-01-01T00:00:00Z",
        schemaVersion: 1,
        commandId: id,
        correlationId: id,
      });
      const stream = { context: "sales", aggregate: "account", id: "a" };
      await config.store.appendToStream(stream, 0, [event("one", 2)]);
      const io = {
        cwd: folder,
        out: (value: string) => {
          output.push(value);
        },
        err: (value: string) => {
          errors.push(value);
        },
      };
      expect(
        await runCLI(
          ["rebuild", group, "--swap", "manual", "--config", "./config.mjs"],
          io,
        ),
      ).toBe(0);
      expect(JSON.parse(output.at(-1)!)).toMatchObject({
        group,
        swapped: false,
      });
      expect(
        await config.readmodels[group].store.find(group, "account"),
      ).toEqual([]);
      await config.store.appendToStream(stream, 1, [event("two", 3)]);
      expect(
        await runCLI(
          ["rebuild", "--swap", group, "--config", "./config.mjs"],
          io,
        ),
      ).toBe(0);
      expect(
        await config.readmodels[group].store.find(group, "account"),
      ).toEqual([{ _id: "a", total: 3 }]);
      expect(errors).toEqual([]);
    } finally {
      await rm(folder, { recursive: true, force: true });
    }
  },
);
test("a migrated old Task runs through runCLI with services, CRUD, Readmodels, Commands and config", async () => {
  const { migrateSource } = await import("../src/migration/index.js");
  const folder = await mkdtemp(join(tmpdir(), "event-fabric-cli-migrated-"));
  try {
    const url = (path: string) => new URL(path, import.meta.url).href;
    const result = join(folder, "result.json");
    await writeFile(
      join(folder, "config.mjs"),
      `import { createInMemoryEventStore } from '${url("../src/event-store/memory.ts")}'; import { createInMemoryTransport } from '${url("../src/transport/memory.ts")}'; import { createMemoryReadmodelStore } from '${url("../src/adapters/memory-readmodel.ts")}'; import { compile } from '${url("../src/authoring/index.ts")}';
export const config = { name: 'migrated', store: createInMemoryEventStore(), transport: createInMemoryTransport(),
  aggregates: [compile('sales', 'account', { commands: { close: (command, state) => { if (!command.payload.by) throw Error('no closer'); state.apply.closed(command.payload); } }, events: { closed: () => {} } })],
  readmodels: { g: { store: createMemoryReadmodelStore(), models: { m: { schema: { type: 'object', properties: {} }, reactions: {} } } } },
  services: { crud: { system: { setting: async () => 'acct-1' } }, mailer: () => ({ send: (to) => to }), resultPath: ${JSON.stringify(result)} } };`,
    );
    const migrated = await migrateSource(
      `const { writeFileSync } = require('node:fs');
module.exports = async (task, app) => {
  const { config } = app.wire.app;
  const id = await app.getCrud().system.setting();
  const sent = app.getServices().mailer().send(config.name);
  const row = await app.getReadmodels().g.m.findOne({ _id: id });
  const outcome = await app.getDomain().sales.account(id).close({ by: sent }, { user: 'task' });
  const events = await app.readAll();
  writeFileSync(app.getServices().resultPath, JSON.stringify({ sent, row, status: outcome.status, events: events.map((event) => [event.stream.id, event.name, event.payload]) }));
};`,
      { task: true },
    );
    expect(migrated.reviews).toEqual([
      "app.wire.app: now ctx; ctx.config is the EventFabric config, so the Application's own settings (such as a database URI) read undefined from it; give them to an Application-owned service in setupServices, or read them from the environment, by hand",
      "sendCommand: resolves to the outcome { status, … } and does not throw on a Rejection; check status where the old call threw",
    ]);
    await writeFile(join(folder, "task.cjs"), migrated.source);
    // The Task handles no Commands itself; a running replica of the Application does.
    const { createApplication } = await import("../src/runtime/index.js");
    const { config } = await import(
      (await import("node:url")).pathToFileURL(join(folder, "config.mjs")).href
    );
    const running = createApplication(config);
    await running.start();
    const errors: string[] = [];
    expect(
      await runCLI(["task", "./task.cjs", "--config", "./config.mjs"], {
        cwd: folder,
        err: (value) => errors.push(value),
      }),
    ).toBe(0);
    expect(errors).toEqual([]);
    expect(JSON.parse(await readFile(result, "utf8"))).toEqual({
      sent: "migrated",
      row: null,
      status: "accepted",
      events: [["acct-1", "closed", { by: "migrated" }]],
    });
    await running.close();
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
test("an old Task runs unmigrated through runCLI with the old Application accessors", async () => {
  const folder = await mkdtemp(join(tmpdir(), "event-fabric-cli-legacy-task-"));
  try {
    const url = (path: string) => new URL(path, import.meta.url).href;
    const result = join(folder, "result.json");
    await writeFile(
      join(folder, "config.mjs"),
      `import { createInMemoryEventStore } from '${url("../src/event-store/memory.ts")}'; import { createInMemoryTransport } from '${url("../src/transport/memory.ts")}'; import { createMemoryReadmodelStore } from '${url("../src/adapters/memory-readmodel.ts")}'; import { compile } from '${url("../src/authoring/index.ts")}';
export const config = { name: 'legacy', store: createInMemoryEventStore(), transport: createInMemoryTransport(),
  aggregates: [compile('sales', 'account', { commands: { close: (command, state) => { if (!command.payload.by) throw Error('no closer'); state.apply.closed(command.payload); }, note: (command, state) => { state.apply.noted(command.payload); } }, events: { closed: () => {}, noted: () => {} } })],
  readmodels: { g: { store: createMemoryReadmodelStore(), models: { m: { schema: { type: 'object', properties: {} }, reactions: {} } } } },
  services: { crud: { system: { setting: async () => 'acct-1' } }, resultPath: ${JSON.stringify(result)} } };
export const legacyTaskApp = (ctx) => ({ wire: { app: { config: { name: 'old-config' } } }, getCrud: () => ctx.services.crud });`,
    );
    await writeFile(
      join(folder, "task.cjs"),
      `const { writeFileSync } = require('node:fs');
module.exports = async ({ args }, app) => {
  const { config } = app.wire.app;
  const id = await app.getCrud().system.setting();
  const rows = await app.getReadmodels().g.m().filter({ _id: id });
  const event = await app.getDomain().sales.account(id).close({ by: config.name }, { user: 'task' }).await('domain.sales.account.closed');
  await app.getDomain().sales.account(id).note({ args });
  const rejected = await app.getDomain().sales.account(id).close({}).await('domain.sales.account.closed').catch((error) => error.message);
  app.getLogger().info('done');
  writeFileSync(app.getServices().resultPath, JSON.stringify({ rows, fullname: event.fullname, aggregate: event.aggregate.id, payload: event.payload, rejected }));
};`,
    );
    const { createApplication } = await import("../src/runtime/index.js");
    const { config } = await import(
      (await import("node:url")).pathToFileURL(join(folder, "config.mjs")).href
    );
    const running = createApplication(config);
    await running.start();
    const errors: string[] = [];
    expect(
      await runCLI(
        ["task", "./task.cjs", "--config", "./config.mjs", "--", "a=1"],
        { cwd: folder, err: (value) => errors.push(value) },
      ),
    ).toBe(0);
    expect(errors).toEqual([]);
    expect(JSON.parse(await readFile(result, "utf8"))).toEqual({
      rows: [],
      fullname: "domain.sales.account.closed",
      aggregate: "acct-1",
      payload: { by: "old-config" },
      rejected: "no closer",
    });
    expect(
      (await running.config.store.readAll()).map((event) => [event.name, event.payload]),
    ).toEqual([
      ["closed", { by: "old-config" }],
      ["noted", { args: ["a=1"] }],
    ]);
    await running.close();
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
test("migrate-app migrates an old-framework Application found by its folder layout, leaving Tasks unchanged", async () => {
  const { mkdir } = await import("node:fs/promises");
  const folder = await mkdtemp(join(tmpdir(), "event-fabric-migrate-app-"));
  const files: Record<string, string> = {
    "app/domain/sales/order.sales.aggregate.js": "const events = { placed: () => {} };\nmodule.exports = { commands: {}, events };\n",
    "app/domain/sales/invoice.sales.aggregate.js": "const { tools } = require('old-framework');\nconst { only } = tools.domain.command;\nconst events = { sent: () => {} };\nmodule.exports = { commands: { send: [only.ifExists()] }, events };\n",
    "bin/old.js": "old\n",
    "README.md": "one\ntwo\n",
    "app/sagas/billing/invoice.billing.saga.js": "const { only } = require('old-framework').tools.saga;\nconst reactions = {\n\t[domain.sales.order.placed]: [only.ifExists(), f],\n};\nmodule.exports = { reactions };\n",
    "app/gateways/rest/shop/routes/orders.shop.route.js": "const { async } = require('old-framework').tools.rest;\nmodule.exports = {};\n",
    "app/tasks/sales/report.sales.task.js": "module.exports = async (task, app) => app.getCrud();\n",
    "package.json": '{\n\t"name": "shop",\n\t"_moduleAliases": {\n\t\t"lib": "./lib"\n\t}\n}\n',
  };
  const output: string[] = [];
  const io = { cwd: folder, out: (value: string) => { output.push(value); }, err: (value: string) => { output.push(value); } };
  try {
    for (const [file, source] of Object.entries(files)) {
      await mkdir(join(folder, file, ".."), { recursive: true });
      await writeFile(join(folder, file), source);
    }
    expect(await runCLI(["migrate-app", ".", "--check"], io)).toBe(1);
    expect(await readFile(join(folder, "app/sagas/billing/invoice.billing.saga.js"), "utf8")).toBe(files["app/sagas/billing/invoice.billing.saga.js"]);
    expect(await runCLI(["migrate-app", ".", "--report", "report.json", "--dependency", "file:../event-fabric"], io)).toBe(0);
    expect(await readFile(join(folder, "app/sagas/billing/invoice.billing.saga.js"), "utf8")).toBe(
      "const { only } = require('event-fabric-next');\nconst reactions = {\n\t[\"sales.order.placed\"]: [only.ifExists(), f],\n};\nmodule.exports = { identity: { [\"sales.order.placed\"]: (event) => event.id }, reactions };\n",
    );
    expect(await readFile(join(folder, "app/gateways/rest/shop/routes/orders.shop.route.js"), "utf8")).toBe(
      "const { async } = require('event-fabric-next/compat').legacyRest;\nmodule.exports = {};\n",
    );
    expect(await readFile(join(folder, "app/tasks/sales/report.sales.task.js"), "utf8")).toBe(files["app/tasks/sales/report.sales.task.js"]);
    // The old toolkit's `only` taken in two statements becomes EventFabric's.
    expect(await readFile(join(folder, "app/domain/sales/invoice.sales.aggregate.js"), "utf8")).toBe(
      "\nconst { only } = require('event-fabric-next');\nconst events = { sent: () => {} };\nmodule.exports = { commands: { send: [only.ifExists()] }, events };\n",
    );
    const report = JSON.parse(await readFile(join(folder, "report.json"), "utf8"));
    expect([report.framework, report.events, report.files.filter((file: any) => file.kind !== "composition").map((file: any) => file.kind)]).toEqual(["old-framework", 4, ["aggregate", "aggregate", "gateway", "saga"]]);
    // The composition root: thin modules over the compat loaders, with the conventions filled in.
    expect(report.files.filter((file: any) => file.kind === "composition").map((file: any) => file.file)).toContain("migration/declarations.cjs");
    expect(await readFile(join(folder, "migration/gateways.cjs"), "utf8")).toContain("const gateways = [['rest', 'shop']];\nconst blocked");
    expect(await readFile(join(folder, "migration/services.cjs"), "utf8")).toContain("require('module-alias').addAliases({ lib: path.join(local.root, 'lib') });\n");
    // It runs the Gateways and the old services, behind the gate the Application opens.
    const config = await readFile(join(folder, "event-fabric.config.js"), "utf8");
    expect([config.includes("const sourceMigrationReady = false;"), config.includes("gateways: definitions(),"), config.includes("return gatewayServices({")]).toEqual([true, true, true]);
    expect(JSON.parse(await readFile(join(folder, "package.json"), "utf8")).optionalDependencies).toEqual({ "event-fabric-next": "file:../event-fabric" });
    expect(await runCLI(["migrate-app", ".", "--check", "--dependency", "file:../event-fabric"], io)).toBe(0);
    // The Application's own decisions come as patches after the rules; applied ones are recognized on the next run.
    await mkdir(join(folder, "patches"));
    // A patch can also create a file the Application adds.
    await writeFile(join(folder, "patches", "2-notes.patch"), "--- /dev/null\n+++ b/migration/notes.txt\n@@ -0,0 +1,2 @@\n+one\n+two\n");
    // A zero-context hunk (`diff -U0`) inserts after the line it names.
    await writeFile(join(folder, "patches", "4-readme.patch"), "--- a/README.md\n+++ b/README.md\n@@ -1,0 +2 @@\n+middle\n");
    // And delete one the Application retires.
    await writeFile(join(folder, "patches", "3-retire.patch"), "--- a/bin/old.js\n+++ /dev/null\n@@ -1 +0,0 @@\n-old\n");
    await writeFile(join(folder, "patches", "1-placed.patch"), "--- a/app/domain/sales/order.sales.aggregate.js\n+++ b/app/domain/sales/order.sales.aggregate.js\n@@ -1,2 +1,2 @@\n-const events = { placed: () => {} };\n+const events = { placed: () => {} }; // kept empty\n module.exports = { commands: {}, events };\n");
    expect(await runCLI(["migrate-app", ".", "--patches", "patches", "--dependency", "file:../event-fabric"], io)).toBe(0);
    expect(await readFile(join(folder, "app/domain/sales/order.sales.aggregate.js"), "utf8")).toBe("const events = { placed: () => {} }; // kept empty\nmodule.exports = { commands: {}, events };\n");
    expect(await readFile(join(folder, "migration/notes.txt"), "utf8")).toBe("one\ntwo\n");
    await expect(readFile(join(folder, "bin/old.js"), "utf8")).rejects.toThrow();
    expect(await readFile(join(folder, "README.md"), "utf8")).toBe("one\nmiddle\ntwo\n");
    expect(await runCLI(["migrate-app", ".", "--patches", "patches", "--check", "--dependency", "file:../event-fabric"], io)).toBe(0);
    // Cutover, once no source imports the old framework: EventFabric's package takes its own name in the sources,
    // the composition root and package.json.
    for (const [args, code] of [[["--check"], 1], [[], 0], [["--check"], 0]] as const)
      expect(await runCLI(["migrate-app", ".", "--packages", "cutover", "--patches", "patches", "--dependency", "file:../event-fabric", ...args], io)).toBe(code);
    expect(await readFile(join(folder, "app/gateways/rest/shop/routes/orders.shop.route.js"), "utf8")).toBe(
      "const { async } = require('event-fabric/compat').legacyRest;\nmodule.exports = {};\n",
    );
    const cut = await readFile(join(folder, "event-fabric.config.js"), "utf8");
    expect([cut.includes("require('event-fabric')"), cut.includes("event-fabric-next"), (await readFile(join(folder, "migration/legacy-app.d.ts"), "utf8")).includes("event-fabric-next")]).toEqual([true, false, false]);
    const cutPackage = JSON.parse(await readFile(join(folder, "package.json"), "utf8"));
    expect([cutPackage.dependencies, cutPackage.optionalDependencies]).toEqual([{ "event-fabric": "file:../event-fabric" }, undefined]);
    await writeFile(join(folder, "app/domain/sales/order.sales.aggregate.js"), "const events = { placed: () => {}, paid: () => {} };\nmodule.exports = { commands: {}, events };\n");
    expect(await runCLI(["migrate-app", ".", "--patches", "patches", "--dependency", "file:../event-fabric"], io)).toBe(1);
    expect(output.at(-1)).toBe("1-placed.patch: app/domain/sales/order.sales.aggregate.js no longer matches the hunk at line 1");
  } finally {
    await rm(folder, { recursive: true, force: true });
  }
});
