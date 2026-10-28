import { expect, test, vi } from "vitest";
import pino from "pino";
import { sagaMessageContract } from "./contracts/saga-messages.js";
import { workerReplyContract } from "./contracts/worker-replies.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { compile } from "../src/authoring/index.js";
import { createApplication } from "../src/runtime/index.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import { createScopedHandle } from "../src/gateway/access.js";
import { ForbiddenError } from "../src/errors.js";
import type { Persistence } from "../src/types.js";
import type { SagaDefinition } from "../src/runtime/types.js";

sagaMessageContract("memory", () => createInMemoryEventStore());
workerReplyContract("memory", () => createInMemoryEventStore());

const file = compile("crm", "file", {
  commands: {
    open: [(command, file) => file.apply.opened(command.payload)],
    postpone: [(command, file) => file.apply.postponed(command.payload)],
    determine: [(_command, file) => file.apply.determined({})],
    close: [(_command, file) => file.apply.closed({})],
    remind: [(command, file) => file.apply.reminded(command.payload)],
  },
  events: { opened() {}, postponed() {}, determined() {}, closed() {}, reminded() {} },
});
const later = (ms: number) => new Date(Date.now() + ms).toISOString();
const stream = (id: string) => ({ context: "crm", aggregate: "file", id });

const fixture = async (
  saga: SagaDefinition,
  options: { store?: Persistence; others?: Record<string, SagaDefinition>; workers?: any } = {},
) => {
  const store = options.store ?? createInMemoryEventStore();
  const app = createApplication({
    name: "saga-messages",
    store,
    aggregates: [file],
    transport: createInMemoryTransport(),
    sagas: { connector: saga, ...options.others },
    workers: options.workers,
    pollInterval: 50,
    logger: pino({ level: "silent" }),
  });
  await app.start();
  const send = (name: string, id: string, payload: any = {}) =>
    app.sendCommand({ stream: stream(id), name, payload });
  const reminders = async (id: string) =>
    (await store.readStream(stream(id))).filter((event) => event.name === "reminded");
  return { app, store, send, reminders, [Symbol.asyncDispose]: () => app.close() };
};

/** A reminder: set on opening, reset by `postpone`, cancelled by `determine`, gone on `close`. */
const connector = (handled: { at: number; state: any; message: any }[] = []): SagaDefinition => ({
  reactions: {
    "crm.file.opened": (event, saga: any) => {
      saga.set("opened", true);
      saga.deadline("remind", event.payload.remindAt, { round: 1 });
    },
    "crm.file.postponed": (event, saga: any) =>
      saga.deadline("remind", event.payload.remindAt, { round: 2 }),
    "crm.file.determined": (_event, saga: any) => saga.cancelDeadline("remind"),
    "crm.file.closed": (_event, saga: any) => saga.destroy(),
  },
  deadlines: {
    remind: (message, saga, app) => {
      handled.push({ at: Date.now(), state: saga.get(), message });
      app.tell({ stream: stream(saga.id), name: "remind", payload: message.payload });
    },
  },
});

test("a Deadline wakes its instance at its due time, against its state, and tells a Command", async () => {
  const handled: { at: number; state: any; message: any }[] = [];
  await using f = await fixture(connector(handled));
  const remindAt = later(150);
  await f.send("open", "f1", { remindAt });
  await vi.waitFor(async () => expect(await f.reminders("f1")).toHaveLength(1), { timeout: 2000 });
  expect(handled).toHaveLength(1);
  expect(handled[0]!.at).toBeGreaterThanOrEqual(Date.parse(remindAt));
  expect(handled[0]!.state).toMatchObject({ opened: true });
  expect(handled[0]!.message).toMatchObject({
    saga: "connector",
    instance: "f1",
    name: "remind",
    deadline: "remind",
    payload: { round: 1 },
    dueAt: remindAt,
  });
  expect(Object.isFrozen(handled[0]!.message)).toBe(true);
  const [opened] = await f.store.readStream(stream("f1"));
  expect(handled[0]!.message).toMatchObject({
    causationId: opened!.id,
    correlationId: opened!.correlationId,
  });
  const [reminded] = await f.reminders("f1");
  // The Command it told carries the Correlation on: Saga Message, Command, Event.
  expect(reminded).toMatchObject({ payload: { round: 1 }, correlationId: opened!.correlationId });
});

test("setting a Deadline again replaces it; cancelling or destroying the instance cancels it", async () => {
  const handled: { at: number; state: any; message: any }[] = [];
  await using f = await fixture(connector(handled));
  await f.send("open", "replaced", { remindAt: later(100) });
  const postponed = later(300);
  await f.send("postpone", "replaced", { remindAt: postponed });
  await f.send("open", "cancelled", { remindAt: later(100) });
  await f.send("determine", "cancelled");
  await f.send("open", "destroyed", { remindAt: later(100) });
  await f.send("close", "destroyed");
  await vi.waitFor(async () => expect(await f.reminders("replaced")).toHaveLength(1), { timeout: 2000 });
  await new Promise((resolve) => setTimeout(resolve, 150));
  expect(handled.map((h) => [h.message.instance, h.message.payload.round])).toEqual([["replaced", 2]]);
  expect(handled[0]!.at).toBeGreaterThanOrEqual(Date.parse(postponed));
  expect(await f.reminders("cancelled")).toEqual([]);
  expect(await f.reminders("destroyed")).toEqual([]);
  expect(await f.store.dueSagaMessages!("saga:connector", undefined, 10)).toEqual({
    messages: [],
    nextDelay: null,
  });
});

test("a Saga Message reaches an existing instance, is dropped for a missing one, and a start handler creates one", async () => {
  await using f = await fixture({
    reactions: { "crm.file.opened": (_event, saga: any) => saga.set("opened", true) },
    messages: {
      done: (message, saga: any) => saga.set("done", message.payload.by),
      report: {
        start: true,
        handle: (message, saga: any) => saga.set("report", message.payload.month),
      },
      byFile: {
        identity: "fileId",
        handle: (message, saga: any) => saga.set("byFile", message.payload.fileId),
      },
    },
  });
  await f.send("open", "f1", { remindAt: later(60000) });
  await f.app.sendSagaMessage("connector", "done", { by: "route" }, { to: "f1" });
  await f.app.sendSagaMessage("connector", "done", { by: "route" }, { to: "missing" });
  await f.app.sendSagaMessage("connector", "report", { month: "2026-10" }, { id: "report-1" });
  await f.app.sendSagaMessage("connector", "byFile", { fileId: "f1" });
  await vi.waitFor(
    async () => {
      expect(await f.store.loadSaga("saga:connector", "f1")).toMatchObject({ done: "route", byFile: "f1" });
      expect(await f.store.loadSaga("saga:connector", "report-1")).toMatchObject({ report: "2026-10" });
    },
    { timeout: 2000 },
  );
  expect(await f.store.loadSaga("saga:connector", "missing")).toBeNull();
  await expect(f.app.sendSagaMessage("connector", "done", {})).rejects.toThrow(
    "Saga Message connector.done needs an instance id: pass `to`",
  );
  await expect(f.app.sendSagaMessage("connector", "unknown", {}, { to: "f1" })).rejects.toThrow(
    "Saga connector handles no Saga Message unknown",
  );
  await expect(f.app.sendSagaMessage("nope", "done", {}, { to: "f1" })).rejects.toThrow("Unknown Saga nope");
});

test("a Saga Message is handled only after the Partition has passed every Event recorded before it fell due", async () => {
  const store = createInMemoryEventStore();
  // Recorded while no Saga runs: the Event first, then the Saga Message, already due.
  const seed = createApplication({
    name: "seed",
    store,
    aggregates: [file],
    transport: createInMemoryTransport(),
    logger: pino({ level: "silent" }),
  });
  await seed.start();
  await seed.sendCommand({ stream: stream("f1"), name: "open", payload: { remindAt: later(60000) } });
  await seed.close();
  await store.recordSagaMessages!([
    {
      id: "m1",
      group: "saga:connector",
      instance: "f1",
      name: "done",
      payload: {},
      metadata: {},
      correlationId: "m1",
      timestamp: new Date().toISOString(),
      dueAt: new Date(Date.now() - 1000).toISOString(),
    },
  ]);
  const seen: any[] = [];
  await using f = await fixture(
    {
      // The Event is recorded before the Saga starts.
      from: "beginning",
      reactions: { "crm.file.opened": (_event, saga: any) => saga.set("opened", true) },
      messages: { done: (_message, saga: any) => void seen.push(saga.get()) },
    },
    { store },
  );
  await vi.waitFor(() => expect(seen).toHaveLength(1), { timeout: 2000 });
  expect(seen[0]).toMatchObject({ opened: true });
});

test("a failing handler retries with backoff, dead-letters on the fifth attempt and never halts the Saga's Events", async () => {
  let failing = true,
    attempts = 0;
  const saga = connector();
  saga.deadlines!.remind = (_message, s: any) => {
    attempts++;
    if (failing) throw new Error("provider down");
    s.set("reminded", true);
  };
  await using f = await fixture(saga);
  await f.send("open", "f1", { remindAt: later(0) });
  await vi.waitFor(
    async () => expect(await f.store.deadLetters()).toMatchObject([{ kind: "saga-message", attempts: 5 }]),
    { timeout: 5000 },
  );
  expect(attempts).toBe(5);
  // Events still reach the Saga.
  await f.send("postpone", "f2", { remindAt: later(60000) });
  await f.send("open", "f3", { remindAt: later(60000) });
  await vi.waitFor(async () => expect(await f.store.loadSaga("saga:connector", "f3")).toMatchObject({ opened: true }));
  const status = (await f.app.consumerStatus()).find((s) => s.group === "saga:connector");
  expect(status?.state).toBe("running");
  failing = false;
  const [letter] = await f.store.deadLetters();
  await f.store.replayDeadLetter(letter!.id);
  f.app.wake();
  await vi.waitFor(
    async () => expect(await f.store.loadSaga("saga:connector", "f1")).toMatchObject({ reminded: true }),
    { timeout: 2000 },
  );
}, 10000);

test("Sagas and Workers send Saga Messages that inherit their cause", async () => {
  const received: any[] = [];
  await using f = await fixture(
    {
      reactions: {
        "crm.file.opened": (event, _saga: any, app) => {
          app.sendSagaMessage("tracker", "track", { step: "saga" }, { to: event.aggregate.id });
          app.queue("lookup", {});
        },
      },
    },
    {
      others: {
        tracker: {
          reactions: {},
          messages: {
            track: { start: true, handle: (message) => void received.push(message) },
          },
        },
      },
      workers: {
        lookup: {
          handles: ["lookup"],
          execute: (_message: any, ctx: any) =>
            ctx.sendSagaMessage("tracker", "track", { step: "worker" }, { to: "f1" }),
        },
      },
    },
  );
  const outcome = await f.send("open", "f1", { remindAt: later(60000) });
  if (outcome.status !== "accepted") throw new Error("expected acceptance");
  const [opened] = outcome.events;
  await vi.waitFor(() => expect(received).toHaveLength(2), { timeout: 2000 });
  const fromSaga = received.find((m) => m.payload.step === "saga");
  const fromWorker = received.find((m) => m.payload.step === "worker");
  expect(fromSaga).toMatchObject({ instance: "f1", causationId: opened!.id, correlationId: opened!.correlationId });
  expect(fromWorker).toMatchObject({ instance: "f1", correlationId: opened!.correlationId });
  expect(fromWorker.causationId).not.toBe(opened!.id);
});

test("a Gateway sends a Saga Message only where its Access map allows it", async () => {
  await using f = await fixture({
    reactions: {},
    messages: { done: { start: true, handle: (_message, saga: any) => saga.set("done", true) } },
  });
  const handle = (sagaMessages?: any) =>
    createScopedHandle({ account: "a" }, { port: 0, access: { sagaMessages } }, f.app);
  await expect(handle().sendSagaMessage("connector", "done", {}, { to: "x" })).rejects.toBeInstanceOf(
    ForbiddenError,
  );
  await expect(
    handle({ "connector.done": (_ctx: any, message: any) => message.to === "mine" }).sendSagaMessage(
      "connector",
      "done",
      {},
      { to: "theirs" },
    ),
  ).rejects.toBeInstanceOf(ForbiddenError);
  // The rule judges the instance it is shown: a Gateway send names it and cannot pick the id.
  await expect(handle({ "connector.*": true }).sendSagaMessage("connector", "done", {})).rejects.toThrow(
    "A Gateway Saga Message names its instance with `to`",
  );
  await expect(
    handle({ "connector.*": true }).sendSagaMessage("connector", "done", {}, { to: "mine", id: "chosen" }),
  ).rejects.toBeInstanceOf(ForbiddenError);
  await handle({ "connector.*": true }).sendSagaMessage("connector", "done", {}, { to: "mine" });
  await vi.waitFor(async () =>
    expect(await f.store.loadSaga("saga:connector", "mine")).toMatchObject({ done: true }),
  );
});

test("each Saga Partition handles the Saga Messages of the instances it owns, each once", async () => {
  const handled: string[] = [];
  await using f = await fixture({
    partitions: 3,
    reactions: {},
    messages: { ping: { start: true, handle: (message) => void handled.push(message.instance) } },
  });
  const instances = Array.from({ length: 20 }, (_, i) => `i${i}`);
  for (const to of instances) await f.app.sendSagaMessage("connector", "ping", {}, { to });
  await vi.waitFor(() => expect(handled).toHaveLength(20), { timeout: 3000 });
  await new Promise((resolve) => setTimeout(resolve, 100));
  expect(handled.sort()).toEqual(instances.sort());
});

test("Saga Message declarations and calls are checked", async () => {
  const refused = async (saga: any, store: Persistence = createInMemoryEventStore()) => {
    const app = createApplication({
      name: "refused",
      store,
      aggregates: [file],
      transport: createInMemoryTransport(),
      sagas: { s: saga },
      logger: pino({ level: "silent" }),
    });
    const error = await app.start().catch((error: Error) => error.message);
    await app.close();
    return error;
  };
  expect(await refused({ reactions: {}, messages: { done: { start: true } } })).toBe(
    "Saga s: Saga Message done needs a handler function",
  );
  expect(await refused({ reactions: {}, messages: { done: { handle() {}, retries: 3 } } })).toBe(
    "Saga s: Saga Message done has unsupported key retries",
  );
  expect(await refused({ reactions: {}, deadlines: { remind: true } })).toBe(
    "Saga s: Deadline remind needs a handler function",
  );
  const { dueSagaMessages: _, ...withoutMessages } = createInMemoryEventStore();
  expect(await refused({ reactions: {}, deadlines: { remind() {} } }, withoutMessages as Persistence)).toBe(
    "Saga s handles Saga Messages, which this Event store does not keep",
  );
});

test("a Deadline without a handler or a due time without a timezone fails the Reaction", async () => {
  const errors: string[] = [];
  const store = createInMemoryEventStore();
  const app = createApplication({
    name: "checked",
    store,
    aggregates: [file],
    transport: createInMemoryTransport(),
    pollInterval: 50,
    sagas: {
      s: {
        reactions: {
          "crm.file.opened": (event, saga: any) => {
            try {
              saga.deadline(event.payload.name, event.payload.remindAt);
            } catch (error) {
              errors.push((error as Error).message);
            }
          },
        },
        deadlines: { remind() {} },
      },
    },
    logger: pino({ level: "silent" }),
  });
  await app.start();
  try {
    await app.sendCommand({ stream: stream("a"), name: "open", payload: { name: "other", remindAt: later(10) } });
    await app.sendCommand({ stream: stream("b"), name: "open", payload: { name: "remind", remindAt: "2030-01-01T10:00" } });
    await vi.waitFor(() => expect(errors).toHaveLength(2));
    expect(errors).toEqual([
      "Saga s has no Deadline handler other",
      "Deadline due time must be an instant with a timezone",
    ]);
  } finally {
    await app.close();
  }
});

test("a Deadline starts its handler within 100 ms of its due time while the Application is idle", async () => {
  const handled: { at: number; state: any; message: any }[] = [];
  const app = createApplication({
    name: "deadline-timing",
    store: createInMemoryEventStore(),
    aggregates: [file],
    transport: createInMemoryTransport(),
    sagas: { connector: connector(handled) },
    // Far longer than the wait below: only the Partition's timer can start the handler in time.
    pollInterval: 60000,
    logger: pino({ level: "silent" }),
  });
  await app.start();
  try {
    const due = Date.now() + 300;
    await app.sendCommand({ stream: stream("t"), name: "open", payload: { remindAt: new Date(due).toISOString() } });
    await vi.waitFor(() => expect(handled).toHaveLength(1), { timeout: 2000, interval: 5 });
    expect(handled[0]!.at).toBeGreaterThanOrEqual(due);
    expect(handled[0]!.at - due).toBeLessThanOrEqual(100);
  } finally {
    await app.close();
  }
});

test("Saga Messages fall due on the clock the Application and the in-memory store share", async () => {
  let now = new Date("2026-10-01T08:00:00.000Z");
  const clock = () => now;
  const store = createInMemoryEventStore({ clock });
  const app = createApplication({
    name: "virtual-clock",
    store,
    aggregates: [file],
    transport: createInMemoryTransport(),
    sagas: {
      connector: { reactions: {}, messages: { ping: { start: true, handle: (_m, s: any) => s.set("pinged", true) } } },
    },
    clock,
    logger: pino({ level: "silent" }),
  });
  await app.sendSagaMessage("connector", "ping", {}, { to: "now" });
  await app.sendSagaMessage("connector", "ping", {}, { to: "later", at: "2026-10-01T09:00:00.000Z" });
  const due = async () =>
    (await store.dueSagaMessages!("saga:connector", undefined, 10)).messages.map((m) => m.instance);
  expect(await due()).toEqual(["now"]);
  expect((await store.dueSagaMessages!("saga:connector", undefined, 10)).nextDelay).toBe(3600000);
  const [sent] = (await store.dueSagaMessages!("saga:connector", undefined, 10)).messages;
  expect(sent).toMatchObject({ timestamp: now.toISOString(), dueAt: now.toISOString() });
  now = new Date("2026-10-01T09:00:00.000Z");
  expect(await due()).toEqual(["now", "later"]);
  await app.close();
});

test("pending Saga Messages follow their instances through a Partition layout switch", async () => {
  const store = createInMemoryEventStore();
  const handled: string[] = [];
  const saga = (partitions: number): SagaDefinition => ({
    partitions,
    reactions: {},
    messages: { ping: { start: true, handle: (message) => void handled.push(message.instance) } },
  });
  const instances = Array.from({ length: 12 }, (_, i) => `i${i}`);
  {
    await using f = await fixture(saga(1), { store });
    for (const to of instances)
      await f.app.sendSagaMessage("connector", "ping", {}, { to, at: later(400) });
  }
  expect(handled).toEqual([]);
  await using f = await fixture(saga(3), { store });
  await vi.waitFor(() => expect(handled).toHaveLength(12), { timeout: 3000 });
  await new Promise((resolve) => setTimeout(resolve, 100));
  expect(handled.sort()).toEqual(instances.sort());
  expect((await f.app.consumerStatus()).find((s) => s.group === "layout:saga:connector")).toMatchObject({
    partitions: 3,
  });
});

test("only a Saga Reaction asks a Worker to reply, with a Saga Message its Saga handles", async () => {
  const commands = compile("crm", "file", {
    commands: {
      open: [
        (command, file, app) => {
          app.queue("lookup", {}, { replyTo: "looked-up" });
          file.apply.opened(command.payload);
        },
      ],
      close: [(_command, file) => file.apply.closed({})],
    },
    events: { opened() {}, closed() {} },
  });
  const app = createApplication({
    name: "replies-checked",
    store: createInMemoryEventStore(),
    aggregates: [commands],
    transport: createInMemoryTransport(),
    pollInterval: 50,
    sagas: {
      s: {
        reactions: { "crm.file.closed": (_event, _saga, app) => app.queue("lookup", {}, { replyTo: "missing" }) },
        messages: { "looked-up": () => {} },
      },
    },
    workers: { lookup: { handles: ["lookup"], execute: () => {} } },
    logger: pino({ level: "silent" }),
  });
  await app.start();
  try {
    expect(await app.sendCommand({ stream: stream("a"), name: "open", payload: {} })).toMatchObject({
      status: "rejected",
      error: { message: "Only a Saga Reaction can ask a Worker to reply" },
    });
    await app.sendCommand({ stream: stream("a"), name: "close", payload: {} });
    await vi.waitFor(async () =>
      expect((await app.consumerStatus()).find((status) => status.error)?.error).toContain(
        "Saga s handles no Saga Message missing for a Worker's reply",
      ),
    );
  } finally {
    await app.close();
  }
});
