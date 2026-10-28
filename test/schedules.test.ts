import { expect, test, vi } from "vitest";
import { compile } from "../src/authoring/index.js";
import { createKernel } from "../src/kernel/index.js";
import { createCommand } from "../src/id.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import type { Command } from "../src/types.js";
import { createApplication } from "../src/runtime/index.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import pino from "pino";
import { scheduleContract } from "./contracts/schedules.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";

scheduleContract("memory", (options) => createInMemoryEventStore(options));

test("an Invoice records its named Schedule atomically with its Events", async () => {
  const store = createInMemoryEventStore();
  const stream = { context: "billing", aggregate: "invoice", id: "one" };
  const dueAt = "2099-01-01T10:00:00.000Z";
  const invoice = compile("billing", "invoice", {
    commands: {
      issue: [
        (_command, invoice, app) => {
          invoice.apply.issued({});
          app.schedule("overdue", dueAt, { stream, name: "markOverdue" });
        },
      ],
      markOverdue: [(_command, invoice) => invoice.apply.becameOverdue({})],
    },
    events: { issued() {}, becameOverdue() {} },
  });
  const kernel = createKernel({ store, definitions: [invoice] });
  expect(
    await kernel.handle(createCommand({ stream, name: "issue" })),
  ).toMatchObject({ status: "accepted", events: [{ name: "issued" }] });
  expect(await store.getSchedule(stream, "overdue")).toMatchObject({
    stream,
    name: "overdue",
    dueAt,
  });
  expect(await store.pendingOutbox()).toEqual([]);
});

test("a future Schedule wakes the idle dispatcher at its due time instead of the next poll", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
  const store = createInMemoryEventStore();
  const stream = { context: "billing", aggregate: "invoice", id: "timer" };
  let startedAt = 0;
  const invoice = compile("billing", "invoice", {
    commands: {
      issue: [
        (command, invoice, app) => {
          invoice.apply.issued({});
          app.schedule("overdue", command.payload.dueAt, {
            stream,
            name: "check",
          });
        },
      ],
      check: [
        () => {
          startedAt = Date.now();
        },
      ],
    },
    events: { issued() {} },
  });
  const app = createApplication({
    name: "scheduled-wake",
    store,
    aggregates: [invoice],
    transport: createInMemoryTransport(),
    pollInterval: 1000,
    logger: pino({ level: "silent" }),
  });
  await app.start();
  try {
    const due = Date.now() + 150;
    expect(
      await app.sendCommand({
        stream,
        name: "issue",
        payload: { dueAt: new Date(due).toISOString() },
      }),
    ).toMatchObject({ status: "accepted" });
    await vi.advanceTimersByTimeAsync(149);
    expect(startedAt).toBe(0);
    await vi.advanceTimersByTimeAsync(2);
    expect(startedAt).toBeGreaterThanOrEqual(due);
    expect(startedAt - due).toBeLessThanOrEqual(100);
    expect(await app.getSchedule(stream, "overdue")).toBeNull();
  } finally {
    const closing = app.close();
    await vi.advanceTimersByTimeAsync(100);
    await closing;
    vi.useRealTimers();
  }
});

test("replacing a Schedule makes an already travelling old attempt a durable no-op", async () => {
  const store = createInMemoryEventStore();
  const stream = { context: "billing", aggregate: "invoice", id: "replace" };
  const invoice = compile("billing", "invoice", {
    commands: {
      change: [
        (command, invoice, app) => {
          invoice.apply.changed({});
          app.schedule("overdue", command.payload.dueAt, {
            stream,
            name: "check",
          });
        },
      ],
      check: [(_command, invoice) => invoice.apply.checked({})],
    },
    events: { changed() {}, checked() {} },
  });
  const kernel = createKernel({ store, definitions: [invoice] });
  await kernel.handle(
    createCommand({
      stream,
      name: "change",
      payload: { dueAt: "2020-01-01T00:00:00Z" },
    }),
  );
  const old = (await store.pendingOutbox())[0]!;
  const lease = (await store.acquireLease("outbox", "one"))!;
  expect(await store.claimOutbox(old.id, lease)).toBe(true);
  expect(
    await kernel.handle(
      createCommand({
        stream,
        name: "change",
        payload: { dueAt: "2021-01-01T00:00:00Z" },
      }),
    ),
  ).toMatchObject({ status: "accepted" });
  const replacement = await store.getSchedule(stream, "overdue");
  expect(replacement?.commandId).not.toBe(old.message.id);
  expect(await kernel.handle(old.message as Command)).toMatchObject({
    status: "accepted",
    events: [],
  });
  expect(await kernel.handle(old.message as Command)).toMatchObject({
    status: "accepted",
    events: [],
    duplicate: true,
  });
  expect(await store.getSchedule(stream, "overdue")).toEqual(replacement);
  expect((await store.pendingOutbox()).map((row) => row.message.id)).toEqual([
    replacement!.commandId,
  ]);
});

const scheduledCheck = (stream: Command["stream"], check: () => void) =>
  compile("billing", "invoice", {
    commands: {
      issue: [
        (command, invoice, app) => {
          invoice.apply.issued({});
          app.schedule("overdue", command.payload.dueAt, { stream, name: "check" });
        },
      ],
      check: [
        (_command, invoice) => {
          check();
          invoice.apply.checked({});
        },
      ],
    },
    events: { issued() {}, checked() {} },
  });

test("a late Schedule attempt reports its miss once, not once per delivery", async () => {
  const store = createInMemoryEventStore();
  const stream = { context: "billing", aggregate: "invoice", id: "late" };
  const warn = vi.fn();
  const logger = { ...pino({ level: "silent" }), warn } as any;
  const kernel = createKernel({
    store,
    logger,
    definitions: [scheduledCheck(stream, () => {})],
  });
  await kernel.handle(
    createCommand({ stream, name: "issue", payload: { dueAt: "2020-01-01T00:00:00Z" } }),
  );
  const attempt = (await store.pendingOutbox())[0]!.message as Command;
  const append = store.appendToStream.bind(store);
  store.appendToStream = vi
    .fn(append)
    .mockRejectedValueOnce(new Error("database unavailable"));
  expect(await kernel.handle(attempt)).toMatchObject({ status: "failed" });
  expect(await kernel.handle(attempt)).toMatchObject({ status: "accepted" });
  expect(await kernel.handle(attempt)).toMatchObject({ duplicate: true });
  expect(
    warn.mock.calls.filter(([, message]) => /missed/.test(message)),
  ).toHaveLength(1);
});

test("an attempt that arrives slightly early waits for its due time instead of returning unknown", async () => {
  const store = createInMemoryEventStore();
  const stream = { context: "billing", aggregate: "invoice", id: "early" };
  const due = Date.now() - 1000;
  const skew = 1050;
  let checkedAt = 0;
  const definitions = [scheduledCheck(stream, () => (checkedAt = Date.now() - skew))];
  await createKernel({ store, definitions }).handle(
    createCommand({ stream, name: "issue", payload: { dueAt: new Date(due).toISOString() } }),
  );
  const attempt = (await store.pendingOutbox())[0]!.message as Command;
  // The relay sees the row as due; this kernel's clock is still 50 ms behind.
  const behind = createKernel({
    store,
    definitions,
    clock: () => new Date(Date.now() - skew),
  });
  expect(await behind.handle(attempt)).toMatchObject({ status: "accepted" });
  expect(checkedAt).toBeGreaterThanOrEqual(due);
});

test("public senders cannot forge a Schedule attempt", () => {
  const stream = { context: "billing", aggregate: "invoice", id: "forged" };
  expect(() =>
    createCommand({
      stream,
      name: "check",
      schedule: { name: "overdue", dueAt: "2020-01-01T00:00:00.000Z" },
    } as any),
  ).toThrow("Schedule attempts are created by EventFabric");
});

test("a Schedule outlives its requesting Saga, and Readmodel rebuilds create no Schedule effects", async () => {
  const store = createInMemoryEventStore();
  const stream = { context: "billing", aggregate: "invoice", id: "saga-owned" };
  const dueAt = "2099-01-01T00:00:00.000Z";
  const invoice = compile("billing", "invoice", {
    commands: {
      open: [(_command, invoice) => invoice.apply.opened({})],
      issue: [
        (command, invoice, app) => {
          invoice.apply.issued({});
          app.schedule("overdue", command.payload.dueAt, { stream, name: "check" });
        },
      ],
      check: [(_command, invoice) => invoice.apply.checked({})],
    },
    events: { opened() {}, issued() {}, checked() {} },
  });
  const app = createApplication({
    name: "saga-schedules",
    store,
    aggregates: [invoice],
    transport: createInMemoryTransport(),
    logger: pino({ level: "silent" }),
    sagas: {
      requester: {
        reactions: {
          "billing.invoice.opened": (event: any, saga: any, app: any) => {
            app.tell({ stream: event.stream, name: "issue", payload: { dueAt } });
            saga.destroy();
          },
        },
      },
    },
    readmodels: {
      ledger: {
        store: createMemoryReadmodelStore(),
        models: {
          invoices: {
            schema: { type: "object", properties: { events: { type: "number" } } },
            reactions: {
              "billing.invoice.issued": (_event: any, invoice: any) =>
                invoice.set("events", (invoice.get("events") ?? 0) + 1),
            },
          },
        },
      },
    },
  } as any);
  await app.start();
  try {
    await app.sendCommand({ stream, name: "open" });
    await expect.poll(() => app.getSchedule(stream, "overdue")).toMatchObject({ dueAt });
    const schedule = await app.getSchedule(stream, "overdue");
    // The 2099 attempt is the only Outbox row; pendingOutbox lists only due rows.
    expect((await store.outboxStats()).pending).toBe(1);
    await (app as any).rebuild("ledger", { swap: "auto" });
    expect(await app.getSchedule(stream, "overdue")).toEqual(schedule);
    expect((await store.outboxStats()).pending).toBe(1);
    expect((await store.readStream(stream)).map((event) => event.name)).toEqual([
      "opened",
      "issued",
    ]);
  } finally {
    await app.close();
  }
});
