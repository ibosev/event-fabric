/**
 * ─── Migrating an old-framework slice end to end ───────────────────────────────
 * `fixtures/legacy-slice/` holds a synthetic dunning slice written for the old framework
 * stack. This test runs `migrateSource` over each file, checks the rewrites and the review
 * list, then runs the migrated copies on EventFabric and checks that they behave as they did
 * before: the old metadata merge, enricher timing, Reaction markers, grouped keys, query map,
 * per-Event Saga instances, QueryBuilder results, list routes, REST command descriptors and
 * Tasks. A last test imports old stored Events and Saga state into a fresh store. See
 * docs/migration.md. It runs on `dist/` (`npm run build` first), because the
 * migrated CommonJS copies `require('event-fabric')`, which resolves to this package's build.
 */
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { afterAll, beforeAll, test } from "vitest";

const fixtures = new URL("./fixtures/legacy-slice/", import.meta.url);
// Inside the package, so the copies resolve event-fabric and event-fabric/compat to dist.
const output = await mkdtemp(join(process.cwd(), "test/.legacy-slice-"));
const require = createRequire(pathToFileURL(`${output}/`));
// Loaded by Node, as the copies load it: through vitest's loader it would be a second copy,
// and the kernel would not recognize the compat helpers' markers.
const { compile, createApplication, startGateway }: typeof import("../dist/index.js") = require("event-fabric");
const { createInMemoryEventStore, createInMemoryTransport, createMemoryReadmodelStore, accepted }: typeof import("../dist/testing.js") =
  require("event-fabric/testing");
const { migrateSource }: typeof import("../dist/migration/index.js") = require("../../dist/migration/index.js");
const { migrateHistoricalEvent, migrateSagaState }: typeof import("../dist/migration/audit.js") = require("../../dist/migration/audit.js");
const migrate = async (file: string, options: object = {}) => {
  const result = await migrateSource(await readFile(new URL(`${file}.js`, fixtures), "utf8"), {
    packages: "cutover",
    // The old declaration toolkit's `only` becomes the new package's.
    replacements: { "require('legacy-toolkit')": "require('event-fabric')" },
    ...options,
  });
  await writeFile(join(output, `${file}.cjs`), result.source);
  return { ...result, module: require(`./${file}.cjs`) };
};

const aggregate = await migrate("dunning.aggregate");
const cases = await migrate("cases.readmodel");
const letters = await migrate("letters.readmodel");
const escalation = await migrate("escalation.saga", { saga: true });
const queries = await migrate("cases.queries");
const routes = await migrate("cases.routes");
const task = await migrate("remind.task", { task: true });

test("migrateSource rewrites the old slice and lists what it changed in meaning", () => {
  assert.match(aggregate.source, /aggregate\.apply\.opened\([\s\S]*legacyMetadata\(/);
  assert.match(aggregate.source, /aggregate\.apply\.reminded\(\{ level/);
  assert.match(aggregate.source, /aggregate\.revision > 10/);
  assert.match(aggregate.source, /state\.revision > 20/);
  assert.match(aggregate.source, /openedAt: event\.timestamp/);
  assert.match(aggregate.source, /eventEnricher: legacyEnricher\(\(event, aggregate\) =>/);
  // An Aggregate's only.ifNotExists() means the same in both stacks and is kept.
  assert.match(aggregate.source, /open: \[\s*only\.ifNotExists\(\)/);
  assert.match(cases.source, /'billing\.dunning\.reminded': \[/);
  assert.match(cases.source, /identity: \{ 'billing\.dunning\.settled': legacyQueryIdentity\(/);
  assert.doesNotMatch(cases.source, /ifNotExists|query:|:customer'/);
  assert.match(cases.source, /reactionSets: \{ customer: \{ identity: \{ 'billing\.dunning\.opened': \(event\) =>/);
  // Saga Identities are written in the map's own style, one entry per line.
  assert.match(escalation.source, /identity: \{\s*'billing\.dunning\.reminded': \(event\) => event\.id,?\s*\}/);
  assert.doesNotMatch(escalation.source, /initialState:/);
  assert.match(queries.source, /cases\.find\(\.\.\.legacyQuery\(\{ filter: .*, size: 0 \}\)\)/);
  assert.match(queries.source, /await legacyOne\(app\.getReadmodels\(\)\.dunning\.cases\(id\)\)/);
  assert.match(routes.source, /handler: legacyList\('dunning', 'cases', \{/);
  // 51: an awaited getDomain() read now loads the State; 51: event.context is { name }.
  assert.match(aggregate.source, /await app\.getDomain\(\)\.billing\.dunning\(cmd\.payload\.lead\)\.load\(\)/);
  assert.match(escalation.source, /context: event\.context\.name/);
  // 52: the old marker is kept and honored at runtime.
  assert.match(cases.source, /\{ settings: \{ shouldHandleEvent: /);
  // 53: .stream() keeps its Node stream.
  assert.match(queries.source, /legacyStream\(app\.getReadmodels\(\)\.dunning\.cases\.stream\(\.\.\.legacyQuery\(/);
  // 54: an old REST command descriptor becomes command(req => CommandInput, { respondWith }).
  assert.match(routes.source, /command\(\(req\) => \(\{ stream: \{ context: 'billing', aggregate: 'dunning', id: req\.params\.id \}, name: 'remind', payload: req\.body \}\), \{ respondWith: 'dunning\.cases' \}\)/);
  // 55: an old Task becomes run(ctx, args); 56: a data field that only looks like a wait name stays.
  assert.match(task.source, /exports\.run = async \(ctx, args\) =>/);
  assert.match(task.source, /ctx\.readmodels\.dunning\.cases\.find\(\.\.\.legacyQuery\(/);
  assert.match(task.source, /return \{ event: 'readmodel\.dunning\.cases\.reminded', count: open\.length \}/);
  const reviews = [aggregate, cases, letters, escalation, queries, routes, task].flatMap((file) => file.reviews);
  for (const expected of [/^aggregate\.get\('_revision'\)/, /^state\._revision/, /^metadata\.timestamp/,
    /^'billing\.dunning\.reminded:count', 'billing\.dunning\.reminded:status': merged/, /^only\.ifNotExists\(\)/,
    /^query: each entry moved/, /^'billing\.dunning\.opened:customer': moved into reactionSets/, /^identity: billing\.dunning\.reminded had no Identity/, /^readmodels\.list\(.*rewritten to legacyList/s, /^initialState: removed/,
    /^app\.getDomain\(\)\.billing\.dunning\(cmd\.payload\.lead\)\.load\(\): getDomain\(\) now builds a Command handle/,
    /^context: the Event's Context is now \{ name \}/, /^domain\.billing\.dunning\.remind: rewritten to command/,
    /^readmodel\.dunning\.cases\.reminded: a Readmodel wait name outside an old REST command descriptor/])
    assert.ok(reviews.some((review: string) => expected.test(review)), `${expected}`);
});

test("56: during coexistence the new package keeps the name event-fabric-next, beside the vendored old one", async () => {
  const source = await readFile(new URL("cases.routes.js", fixtures), "utf8");
  const coexisting = await migrateSource(source, {
    packages: "coexistence",
    replacements: { "require('legacy-toolkit')": "require('event-fabric-next')" },
  });
  assert.match(coexisting.source, /require\("event-fabric-next\/compat"\)/);
  assert.match(coexisting.source, /require\('event-fabric-next'\)/);
  // An import of the old package's name is listed, never redirected: during coexistence it is the old stack.
  const old = await migrateSource("const { only } = require('event-fabric');", { packages: "coexistence" });
  assert.equal(old.source, "const { only } = require('event-fabric');");
  assert.ok(old.reviews.some((review: string) => /^event-fabric: during coexistence this is the vendored old package/.test(review)));
});

test("51: compile still refuses the old Aggregate persistence options", () => {
  assert.throws(
    () => compile("billing", "dunning", { ...aggregate.module, options: { disablePersistence: true } }),
    /unsupported Aggregate option options \(Events are always persisted/,
  );
});

type Letter = { saga: string; letters: number; context: string };
const deliveries: Letter[] = [];
const legacyReadmodels = {
  dunning: {
    models: {
      // Old Readmodels had no schema; declaring one is a manual migration step.
      cases: {
        ...cases.module,
        schema: {
          type: "object",
          properties: {
            customer: { type: "string" },
            amount: { type: "number" },
            status: { type: "string" },
            reminders: { type: "integer" },
          },
        },
      },
      letters: { ...letters.module, schema: { type: "object", properties: { level: { type: "integer" } } } },
    },
  },
};
const letterWorker = (into: Letter[]) => ({
  handles: ["letter"],
  execute: async (message: { payload: Letter }) => {
    into.push(message.payload);
  },
});
const application = createApplication({
  name: "legacy-slice-test",
  aggregates: [compile("billing", "dunning", aggregate.module)],
  store: createInMemoryEventStore(),
  transport: createInMemoryTransport(),
  readmodelStore: createMemoryReadmodelStore(),
  readmodels: legacyReadmodels,
  sagas: { escalation: escalation.module },
  workers: { letter: letterWorker(deliveries) },
  pollInterval: 10,
});
beforeAll(() => application.start());
afterAll(async () => {
  await application.close();
  await rm(output, { recursive: true, force: true });
});

const send = (id: string, name: string, payload: object = {}) =>
  accepted(
    application.sendCommand({
      stream: { context: "billing", aggregate: "dunning", id },
      name,
      payload,
      metadata: { tenant: { id: "t1", region: "us" } },
    }),
  );
// Untyped: the slice has no generated Register.
const readmodels: any = application.readmodels;

test("the migrated slice keeps the old behavior on EventFabric", { timeout: 20_000 }, async () => {
  const opened = await send("case-1", "open", { customer: "c1", amount: 120 });
  // The old deep merge: Command metadata wins and nested fields survive; the enricher's fields are kept.
  assert.deepEqual(opened.events[0]!.metadata, {
    tenant: { id: "t1", region: "us" },
    source: "dunning",
    channel: "letter",
    previousStatus: "none",
  });
  await send("case-2", "open", { customer: "c1", amount: 80 });
  await send("case-3", "open", { customer: "c2", amount: 50 });
  const first = await send("case-1", "remind");
  const second = await send("case-1", "remind");
  // The old enricher timing: each Event's enricher saw the State before that Event, not the final State.
  assert.deepEqual(
    [first, second].map((outcome) => outcome.events[0]!.metadata.previousStatus),
    ["open", "reminded"],
  );
  const third = await send("case-3", "remind");
  await readmodels.dunning.cases("case-3", { minPosition: third.events[0]!.position });
  // The old QueryBuilder results: size(0) is unlimited, sorted by amount; an id read is an array.
  const app = { getReadmodels: () => readmodels };
  assert.deepEqual(
    (await queries.module.openCases(app)).map((document: any) => document._id),
    ["case-1", "case-2", "case-3"],
  );
  assert.deepEqual(await queries.module.caseById(app, "missing"), []);
  assert.equal((await queries.module.caseById(app, "case-1"))[0].reminders, 2);

  const settled = await send("case-2", "settle");
  const minPosition = settled.events.at(-1)!.position;

  // The query map settled every open case of customer c1, through the old array membership.
  const status = async (id: string) =>
    (await readmodels.dunning.cases(id, { minPosition })).document;
  assert.deepEqual(
    [await status("case-1"), await status("case-2"), await status("case-3")].map((document) => [
      document.status,
      document.reminders,
    ]),
    [["settled", 2], ["settled", 0], ["reminded", 1]],
  );
  // The grouped key's own Identity: one running total per customer, beside the cases.
  assert.equal((await status("customer-c1")).amount, 200);
  // only.if: the first reminders of case-1 and case-3 created no letter; the second did.
  const letter = async (id: string) => (await readmodels.dunning.letters(id, { minPosition })).document;
  assert.equal(await letter("case-3"), null);
  assert.deepEqual(await letter("case-1"), { _id: "case-1", level: 2 });

  // Three reminders, three Saga instances named by their Events, each starting empty.
  const reminders = [second.events[0]!.id];
  for (;;) {
    if (deliveries.length === 3) break;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.ok(deliveries.every((delivery) => delivery.letters === 1 && delivery.context === "billing"));
  assert.equal(new Set(deliveries.map((delivery) => delivery.saga)).size, 3);
  assert.ok(reminders.every((id) => deliveries.some((delivery) => delivery.saga === id)));

  // The old list route over HTTP: the client's query string, the route's sort, { total, data }.
  let port = 0;
  const logger: any = {
    debug() {}, warn() {}, error() {},
    info(value: any) { port ||= value?.address?.port ?? 0; },
    child() { return logger; },
  };
  const access = { domain: { "billing.dunning.remind": true }, readmodels: { "dunning.*": true } };
  const stop = await startGateway("legacy", { port: 0, access, ...routes.module }, {
    application, invoke: async () => {}, logger, timeout: 1000,
  });
  try {
    const list = async (query: string) => (await fetch(`http://127.0.0.1:${port}/cases?${query}`)).json();
    assert.deepEqual(await list("customer=c1&size=1&sort=amount"), {
      total: 2,
      data: [{ id: "case-1", customer: "c1", amount: 120, status: "settled" }],
    });
    assert.deepEqual((await list("status=open,reminded")).data.map((row: any) => row.id), ["case-3"]);
    // The migrated REST command descriptor answers with the case once the reminder reached it.
    const reminded = await fetch(`http://127.0.0.1:${port}/cases/case-3/remind`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: "{}",
    });
    assert.equal(reminded.status, 200);
    assert.deepEqual(await reminded.json().then(({ status, reminders }) => ({ status, reminders })), { status: "reminded", reminders: 2 });
  } finally {
    await stop();
  }
});

test("old reads, markers and Tasks keep working after migration", async () => {
  // 51: the awaited getDomain() read loads case-1's State; case-3 escalates past its level.
  const escalated = await send("case-3", "escalate", { lead: "case-1" });
  assert.equal(escalated.events[0]!.payload.level, 3);
  // 52: the old shouldHandleEvent marker skips the whole Reaction: no case for an internal transfer.
  const internal = await send("case-internal", "open", { customer: "internal", amount: 5 });
  assert.equal((await readmodels.dunning.cases("case-internal", { minPosition: internal.events[0]!.position })).document, null);
  // 53: `.stream()` is a Node object stream again.
  const streamed: string[] = [];
  for await (const document of queries.module.customerCases({ getReadmodels: () => readmodels }, "c1"))
    streamed.push(document._id);
  assert.deepEqual(streamed.sort(), ["case-1", "case-2"]);
  // 55: the migrated Task runs with a Task context: it reminds every open case.
  const opened = await send("case-4", "open", { customer: "c4", amount: 10 });
  await readmodels.dunning.cases("case-4", { minPosition: opened.events[0]!.position });
  const summary = await task.module.run({ readmodels, sendCommand: application.sendCommand }, []);
  assert.deepEqual(summary, { event: "readmodel.dunning.cases.reminded", count: 1 });
  const events = await application.config.store.readStream({ context: "billing", aggregate: "dunning", id: "case-4" });
  assert.deepEqual(events.map((event) => event.name), ["opened", "reminded"]);
});

test("57: old stored Events and Saga state import into a fresh store, and the new Application continues", async () => {
  // An old stored Event: storage Revisions started at 0, and Dates were stored as Dates.
  const stored = {
    eventId: "old-event-1",
    stream: { context: "billing", aggregate: "dunning", id: "case-9" },
    name: "domain.billing.dunning.opened",
    payload: { customer: "c9", amount: 40 },
    revision: 0,
    commandId: "old-command-1",
    timestamp: new Date("2025-03-01T09:00:00Z"),
    metadata: { timestamp: new Date("2025-03-01T09:00:00Z"), reviewedAt: new Date("2025-03-01T10:00:00Z") },
  };
  // A Date is refused, with its path, until the import says how to store Dates.
  assert.throws(() => migrateHistoricalEvent(stored, { revisionBase: 0 }), /reviewedAt/);
  const { event } = migrateHistoricalEvent(stored, { revisionBase: 0, dates: "iso" });
  assert.deepEqual([event.id, event.name, event.revision, event.timestamp, event.metadata.reviewedAt],
    ["old-event-1", "opened", 1, "2025-03-01T09:00:00.000Z", "2025-03-01T10:00:00.000Z"]);

  // A pending old Saga timeout would never fire here, so it is refused; settled state imports.
  assert.throws(() => migrateSagaState({ letters: 3, _timeoutAt: new Date() }), /pending Saga timeout/);
  const state = migrateSagaState({ letters: 3, _commitStamp: 1, _hash: "x" });
  assert.deepEqual(state, { letters: 3 });

  const history = createInMemoryEventStore();
  const [imported] = await history.import([event]);
  // Ids and timestamps are kept; the Saga group starts after the imported history.
  assert.deepEqual([imported!.id, imported!.timestamp], ["old-event-1", "2025-03-01T09:00:00.000Z"]);
  await history.importSagas("saga:escalation", [{ id: "old-instance", state }], imported!.position);

  const letters: Letter[] = [];
  const cutover = createApplication({
    name: "legacy-slice-cutover",
    aggregates: [compile("billing", "dunning", aggregate.module)],
    store: history,
    transport: createInMemoryTransport(),
    readmodelStore: createMemoryReadmodelStore(),
    readmodels: legacyReadmodels,
    sagas: { escalation: escalation.module },
    workers: { letter: letterWorker(letters) },
    pollInterval: 10,
  });
  await cutover.start();
  try {
    const outcome = await cutover.sendCommand({
      stream: { context: "billing", aggregate: "dunning", id: "case-9" }, name: "remind", payload: {},
    });
    assert.equal(outcome.status, "accepted");
    // The imported Event is Revision 1, so the reminder is Revision 2, folded from imported State.
    assert.equal(outcome.status === "accepted" && outcome.events[0]!.revision, 2);
    assert.equal(outcome.status === "accepted" && outcome.events[0]!.metadata.previousStatus, "open");
    const { document } = await (cutover.readmodels as any).dunning.cases("case-9", {
      minPosition: outcome.status === "accepted" ? outcome.events[0]!.position : undefined,
    });
    assert.deepEqual([document.customer, document.amount, document.reminders], ["c9", 40, 1]);
    assert.deepEqual(await history.loadSaga("saga:escalation", "old-instance"), { letters: 3 });
  } finally {
    await cutover.close();
  }
});
