import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, expect, test } from "vitest";
import { LEGACY_EFFECTS_STATE, LEGACY_SCHEDULER, legacyApplication } from "../src/compat.js";

// An old-layout Application: a plain Aggregate, an `options` Aggregate, a disabled context, a Readmodel and two Sagas.
const files: Record<string, string> = {
  "app/domain/sales/order.js": "module.exports = { commands: { place: [() => {}] }, events: { placed: () => {} } };\n",
  "app/domain/system/reminder.js":
    "module.exports = { options: {}, commands: { remind: [{ schema: {} }, (command, state) => state.apply.reminded({ at: 1 })] }, events: { reminded: () => {} } };\n",
  "app/domain/testing/probe.js": "module.exports = { options: {}, commands: { poke: [() => {}] }, events: { poked: () => {} } };\n",
  "app/readmodels/shop/orders.js":
    "module.exports = { repositorySettings: { indexes: [] }, query: { 'domain.sales.order.placed': { filter: { id: 'aggregate.id' } } }, reactions: { 'domain.sales.order.placed': (event, view) => view.set('seen', { context: event.context, at: event.metadata.timestamp }) } };\n",
  "app/readmodels/shop/marks.js":
    "module.exports = { reactions: { 'domain.sales.order.placed': [(event) => { event.payload.marks = (event.payload.marks || 0) + 1; }, (event, view) => view.set('marks', event.payload.marks)] } };\n",
  "app/sagas/mail/notify.js":
    "module.exports = { reactions: { 'domain.sales.order.placed': (event, view, app) => app.getServices().mailer().send(event.aggregate.id), 'domain.sales.order.commandRejected': () => {} } };\n",
  "app/sagas/mail/remind.js": "module.exports = { reactions: { 'domain.system.reminder.reminded': (event, view) => view.set('reminded', true) } };\n",
};
let root: string;
beforeAll(async () => {
  root = await mkdtemp(join(tmpdir(), "event-fabric-legacy-application-"));
  for (const [file, source] of Object.entries(files)) {
    await mkdir(join(root, file, ".."), { recursive: true });
    await writeFile(join(root, file), source);
  }
});
afterAll(() => rm(root, { recursive: true, force: true }));

const application = () =>
  legacyApplication({
    root,
    readmodelGroups: ["shop"],
    sagaGroups: ["mail"],
    sagaBindings: { mail: "shop" },
    readmodelStorage: () => ({ schema: { type: "object" }, indexes: [{ fields: ["id"] }] }),
    sagaReads: { mail: ["shop"] },
    historicalEvents: ["sales.order.archived"],
    disabledContexts: ["testing"],
    effects: { reactions: { "mail.notify": ["sales.order.placed"] }, providers: { mailer: [] } },
  });

test("legacyApplication loads the old folders as declarations, with options Aggregates and effects in Workers", () => {
  const declared = application();
  expect(declared.aggregates.map((entry) => `${entry.context}.${entry.name}`)).toEqual(["sales.order", "system.reminder", "testing.probe"]);
  expect(declared.deferred).toEqual([
    { context: "system", name: "reminder", commands: ["remind"], reason: "runs in the orchestration Worker" },
    { context: "testing", name: "probe", commands: ["poke"], reason: "disabled testing" },
  ]);
  expect(Object.keys(declared.aggregates[2]!.commands ?? {})).toEqual([]);
  expect(() => (declared.aggregates[0]!.events as any).archived()).toThrow(/recover it before Aggregate replay/);
  const orders = declared.readmodels.shop!.models.orders as any;
  expect(orders.schema).toEqual({ type: "object", "x-undeclared": "drop" });
  expect(Object.keys(orders.identity)).toEqual(["sales.order.placed"]);
  expect(Object.keys(declared.sagas)).toEqual(["mail.notify", "mail.remind"]);
  const notify = declared.sagas["mail.notify"] as any, remind = declared.sagas["mail.remind"] as any;
  expect(declared.rejections).toEqual(["mail.notify: sales.order.commandRejected"]);
  expect(notify.reads).toEqual(["shop"]);
  expect(Object.keys(notify.messages)).toEqual([LEGACY_EFFECTS_STATE]);
  // A Reaction to an options Aggregate's Event becomes a Saga Message the orchestration Worker sends.
  expect(Object.keys(remind.reactions)).toEqual([]);
  expect(Object.keys(remind.messages)).toEqual(["system.reminder.reminded"]);
  expect(Object.keys(declared.orchestrationSagas)).toEqual([LEGACY_SCHEDULER]);
  expect(Object.keys(declared.workers)).toEqual(["orchestration", "effects:mail.notify"]);
});

test("Readmodel Reactions get the old envelope; a moved Saga Reaction queues its Worker, which runs it with providers", async () => {
  const declared = application();
  const event = {
    id: "e1", name: "placed", context: { name: "sales" }, aggregate: { id: "o1", name: "order" }, stream: { context: "sales", aggregate: "order", id: "o1" },
    revision: 3, payload: {}, metadata: {}, timestamp: "2026-01-02T03:04:05.000Z", commandId: "c1",
  };
  const stored: Record<string, unknown> = {};
  const view = { id: "o1", get: (at?: string) => (at === undefined ? stored : stored[at]), set: (at: string, value: unknown) => void (stored[at] = value) };
  const orders = declared.readmodels.shop!.models.orders as any;
  for (const reaction of orders.reactions["sales.order.placed"]) await reaction(event, view, {});
  expect(stored).toEqual({ id: "o1", seen: { context: "sales", at: new Date(event.timestamp) } });

  const queued: unknown[] = [];
  const [reaction] = (declared.sagas["mail.notify"] as any).reactions["sales.order.placed"];
  await reaction(event, { id: "o1", get: () => ({}) }, { queue: (name: string, payload: unknown) => queued.push([name, payload]) });
  expect(queued).toMatchObject([["effects.mail.notify", { saga: "mail.notify", event: "sales.order.placed", instance: "o1" }]]);
  const sent: unknown[] = [];
  await (declared.workers["effects:mail.notify"] as any).execute(
    { payload: (queued[0] as any)[1], idempotencyKey: "k" },
    { services: { mailer: () => ({ send: (id: string) => sent.push(id) }) }, sendSagaMessage: () => {}, logger: console },
  );
  expect(sent).toEqual(["o1"]);
});

test("two Partitions of one group interleaving Events keep one shared old Event each", async () => {
  const marks = application().readmodels.shop!.models.marks as any;
  const event = (id: string) => ({
    id, name: "placed", context: { name: "sales" }, aggregate: { id, name: "order" }, stream: { context: "sales", aggregate: "order", id },
    revision: 1, payload: {}, metadata: {}, timestamp: "2026-01-02T03:04:05.000Z", commandId: id,
  });
  const view = () => {
    const stored: Record<string, unknown> = {};
    return { stored, id: "x", get: (at?: string) => (at === undefined ? stored : stored[at]), set: (at: string, value: unknown) => void (stored[at] = value) };
  };
  const [keep, mark, copy] = marks.reactions["sales.order.placed"];
  const first = event("e1"), second = event("e2"), a = view(), b = view();
  await keep(first, a); await mark(first, a);
  // Another Partition of the group reacts to its own Event between the first one's Reactions.
  await keep(second, b); await mark(second, b);
  await copy(first, a); await copy(second, b);
  expect([a.stored.marks, b.stored.marks]).toEqual([1, 1]);
  // The recorded Events are untouched.
  expect([first.payload, second.payload]).toEqual([{}, {}]);
});
