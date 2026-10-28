import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { afterAll, afterEach, beforeAll, expect, test } from "vitest";
import { createApplication } from "../src/runtime/index.js";
import { loadAggregate } from "../src/kernel/index.js";
import { resolveComponents } from "../src/authoring/decorators.js";
import { generateTypes } from "../src/cli/types.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import { createInMemoryTransport } from "../src/transport/memory.js";

// Decorated source needs a compiler (Node's type stripping rejects decorators), so the
// fixtures go through tsc the way an Application's build does, against the built package.
const directory = await mkdtemp(join(tmpdir(), "eventfabric-decorators-"));
const copy = async (name: string) => {
  const source = await readFile(
    join(process.cwd(), "test/fixtures", name),
    "utf8",
  );
  const target = join(directory, name);
  await writeFile(
    target,
    source.replaceAll("../../dist/", `${join(process.cwd(), "dist")}/`),
  );
  return target;
};
const tsc = (files: string[], ...flags: string[]) =>
  execFileSync(
    join(process.cwd(), "node_modules/.bin/tsc"),
    [
      "--ignoreConfig",
      "--strict",
      "--skipLibCheck",
      "--target",
      "ES2023",
      "--module",
      "NodeNext",
      "--types",
      "node",
      ...flags,
      ...files,
    ],
    { encoding: "utf8" },
  );
let fixture: any;
beforeAll(async () => {
  tsc([await copy("decorated.ts")], "--outDir", directory);
  fixture = await import(pathToFileURL(join(directory, "decorated.js")).href);
}, 60_000);
afterAll(() => rm(directory, { recursive: true, force: true }));
const cleanups: (() => Promise<unknown>)[] = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

const run = async (config: Record<string, unknown>) => {
  const store = createInMemoryEventStore();
  const application = createApplication({
    name: "decorated",
    store,
    transport: createInMemoryTransport(),
    readmodelStore: createMemoryReadmodelStore(),
    pollInterval: 5,
    ...config,
  });
  await application.start();
  cleanups.push(() => application.stop());
  return { application, store };
};
const commands = () => [
  fixture.MakeDeposit.to("a-1", { amount: 5 }), // does not exist
  fixture.OpenAccount.to("a-1", { owner: "ann" }),
  fixture.OpenAccount.to("a-1", { owner: "ann" }), // exists
  fixture.MakeDeposit.to("a-1", { amount: -1 }), // schema
  fixture.MakeDeposit.to("a-1", { amount: 60 }),
  fixture.MakeDeposit.to("a-1", { amount: 60 }), // Rule
  fixture.MakeDeposit.to("a-1", { amount: 20 }),
];
const outcomes = async (application: any) => {
  const result = [];
  for (const command of commands()) {
    const outcome = await application.sendCommand(command);
    result.push(
      outcome.status === "accepted"
        ? outcome.events.map((e: any) => [e.name, e.payload, e.schemaVersion])
        : [outcome.status, outcome.error?.kind],
    );
  }
  return result;
};

test("a decorated Aggregate decides, records and replays exactly like its aggregate() twin", async () => {
  const decorated = await run({ aggregates: [fixture.Account] });
  const plain = await run({ aggregates: [fixture.accountObject] });
  const expected = await outcomes(plain.application);
  expect(await outcomes(decorated.application)).toEqual(expected);
  expect(expected).toEqual([
    ["rejected", "existence"],
    [["opened", { owner: "ann" }, 1]],
    ["rejected", "existence"],
    ["rejected", "validation"],
    [["deposited", { amount: 60, note: "by ann" }, 2]],
    ["rejected", "rule"],
    [["deposited", { amount: 20, note: "by ann" }, 2]],
  ]);
  const [definition] = resolveComponents({
    aggregates: [fixture.Account],
  } as any).aggregates!;
  const history = await decorated.store.readStream({
    context: "bank",
    aggregate: "account",
    id: "a-1",
  });
  expect(loadAggregate(definition!, "a-1", history).get()).toEqual(
    loadAggregate(fixture.accountObject, "a-1", history).get(),
  );
  expect(loadAggregate(definition!, "a-1", history).get()).toEqual({
    id: "a-1",
    owner: "ann",
    balance: 80,
  });
});

test("an Event type's Schema version and Upcasters apply to stored Events", async () => {
  const { application, store } = await run({ aggregates: [fixture.Account] });
  await application.sendCommand(
    fixture.OpenAccount.to("a-1", { owner: "ann" }),
  );
  const stream = { context: "bank", aggregate: "account", id: "a-1" };
  const [head] = await store.readStream(stream);
  await store.appendToStream(stream, head!.revision, [
    {
      ...head!,
      id: crypto.randomUUID(),
      name: "deposited",
      payload: { amount: 5 },
      schemaVersion: 1,
    },
  ]);
  const events = await store.readStream(stream);
  expect(events.at(-1)).toMatchObject({
    payload: { amount: 5, note: "" },
    schemaVersion: 2,
  });
});

test("decorated and existing components run together: Readmodels, Sagas, the Outbox and both bridges", async () => {
  const { application } = await run({
    aggregates: [fixture.Account, fixture.audit],
    readmodels: {
      bank: {
        models: {
          balances: fixture.Balances,
          notes: fixture.AuditNotes,
          counts: {
            schema: {
              type: "object",
              properties: { events: { type: "number" } },
            },
            reactions: fixture.counts,
          },
        },
      },
    },
    sagas: { "big-deposits": fixture.BigDeposits, reopen: fixture.Reopen },
  });
  await application.sendCommand(
    fixture.OpenAccount.to("a-1", { owner: "ann" }),
  );
  await application.sendCommand(fixture.MakeDeposit.to("a-1", { amount: 60 }));
  const readmodels = application.readmodels as any;
  // Saga (decorated) → audit.note (aggregate()) → Noted (eventOf) → Readmodel and Saga (decorated) → OpenAccount.
  await expect
    .poll(
      async () =>
        (await readmodels.bank.balances("from-audit-a-1")).document?.owner,
    )
    .toBe("audit");
  expect((await readmodels.bank.balances("a-1")).document).toMatchObject({
    owner: "ann",
    balance: 60,
  });
  expect((await readmodels.bank.notes("audit-a-1")).document).toMatchObject({
    count: 1,
    total: 60,
  });
  expect((await readmodels.bank.counts("a-1")).document).toMatchObject({
    events: 2,
  });
  expect(await readmodels.bank.balances.find({ owner: "ann" })).toHaveLength(1);
});

test("a decorated Saga handles its Saga Messages and Deadlines", async () => {
  const { application, store } = await run({
    aggregates: [fixture.Account],
    sagas: { reminders: fixture.Reminders },
  });
  await application.sendCommand(fixture.OpenAccount.to("a-1", { owner: "ann" }));
  await expect.poll(() => store.loadSaga("saga:reminders", "a-1")).toEqual({ _id: "a-1", reminded: 0 });
  await expect(
    application.sendSagaMessage("reminders", "nudge", { by: 1 }, { to: "a-1" }),
  ).rejects.toThrow();
  await application.sendSagaMessage("reminders", "nudge", { by: "route" }, { to: "a-1" });
  await expect
    .poll(() => store.loadSaga("saga:reminders", "a-1"))
    .toEqual({ _id: "a-1", by: "route", reminded: 1 });
  await expect
    .poll(async () => (await store.readStream({ context: "bank", aggregate: "account", id: "a-1" })).map((e) => e.name))
    .toEqual(["opened", "deposited"]);
});

test("the config resolves decorated classes for Register generation, and rejects misplaced ones", () => {
  const config = resolveComponents({
    aggregates: [fixture.Account, fixture.audit],
    readmodels: { bank: { models: { balances: fixture.Balances } } },
    sagas: { s: fixture.BigDeposits },
  } as any);
  expect(resolveComponents({ sagas: { r: fixture.Reminders } } as any).sagas!.r!.from).toBe("beginning");
  const types = generateTypes(config);
  expect(types).toContain('"opened": { "owner": string }');
  expect(types).toContain('"deposit": { "amount": number }');
  expect(types).toContain(
    '"balances": { _id: string } & { "owner": string; "balance": number }',
  );
  expect(config.sagas!.s!.reactions).toHaveProperty(["bank.account.deposited"]);
  expect(() =>
    resolveComponents({ aggregates: [fixture.notDecorated] } as any),
  ).toThrow("aggregates: Plain is not decorated with @Aggregate");
  expect(() =>
    resolveComponents({ sagas: { s: fixture.Balances } } as any),
  ).toThrow("sagas.s: Balances is a @Readmodel, not a @Saga");
  // An undecorated subclass must not run its parent's handlers in place of its own.
  class Child extends fixture.Account {}
  expect(() => resolveComponents({ aggregates: [Child] } as any)).toThrow(
    "aggregates: Child extends a decorated class; inheritance is not supported",
  );
});

test("unsupported class shapes fail where they are declared", () => {
  const messages = Object.fromEntries(
    Object.entries(fixture.invalid as Record<string, () => unknown>).map(
      ([name, declare]) => {
        try {
          declare();
          return [name, "no error"];
        } catch (error) {
          return [name, (error as Error).message];
        }
      },
    ),
  );
  expect(messages).toEqual({
    "no handlers": "@Aggregate Empty: has no handlers",
    "two Aggregates":
      "@Aggregate Mixed: handles messages of bank.account, bank.audit; an Aggregate handles its own only",
    "duplicate handler":
      "@Aggregate Twice: bank.account.opened is handled by both one and two",
    "undecorated method":
      "@Readmodel Helper: format is not a decorated handler",
    "wrong role":
      "@Readmodel Wrong: @SagaEventHandler is not allowed on opened",
    inheritance:
      "@Readmodel Child: extends another class; inheritance is not supported",
    "static member":
      "@Readmodel Static: static members are not supported (count)",
    "static handler":
      "@EventHandler(bank.account.opened) on opened: only public instance methods can be handlers",
    "Command as Event": "@EventHandler on opened: expected an event() type",
    "unqualified name": "Command open: name it <context>.<aggregate>.<name>",
  });
});

test("handlers hold no state: `this` is undefined, and another Aggregate's Event cannot be applied", async () => {
  const { aggregates, readmodels } = resolveComponents({
    aggregates: [fixture.ForeignApply],
    readmodels: { g: { models: { m: fixture.Stateful } } },
  } as any);
  const reaction = readmodels!.g!.models.m!.reactions[
    "bank.account.opened"
  ] as Function;
  expect(() => reaction({}, { set() {} })).toThrow(
    "Cannot read properties of undefined (reading 'owner')",
  );
  const { application } = await run({ aggregates });
  const outcome: any = await application.sendCommand(
    fixture.OpenAccount.to("x", { owner: "x" }),
  );
  expect(outcome.status).toBe("rejected");
  expect(outcome.error.message).toContain(
    "bank.account has no @EventSourcingHandler for bank.audit.noted",
  );
});

test("compile errors catch wrong names, payloads and State; a decorated Application stays cheap to check", async () => {
  // Each @ts-expect-error must fire, or tsc fails with TS2578.
  tsc([await copy("decorated-errors.ts")], "--noEmit");
  const output = tsc(
    [join(directory, "decorated.ts")],
    "--noEmit",
    "--extendedDiagnostics",
  );
  const instantiations = Number(/Instantiations:\s+(\d+)/.exec(output)?.[1]);
  expect(instantiations).toBeGreaterThan(0);
  expect(instantiations).toBeLessThan(100_000);
}, 60_000);
