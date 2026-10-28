import { expect, test, vi } from "vitest";
import { createTimings } from "../src/observability.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import { createOutboxRelay } from "../src/runtime/delivery.js";
import { createReadmodels } from "../src/runtime/readmodels.js";
import { createApplication } from "../src/runtime/index.js";
import { createConsumerGroups } from "../src/runtime/consumer-groups.js";
import { compile } from "../src/authoring/index.js";
import type { Logger, Transport } from "../src/types.js";

type Entry = Record<string, any>;
// Keeps records with their child bindings, like pino does.
const capturing = (records: Entry[], bindings: Entry = {}, enabled = () => true): Logger => ({
  isLevelEnabled: enabled,
  debug: (fields, msg) => records.push({ ...bindings, ...fields, msg }),
  info: (fields, msg) => records.push({ ...bindings, ...fields, msg }),
  warn: (fields, msg) => records.push({ ...bindings, ...fields, msg }),
  error: (fields, msg) => records.push({ ...bindings, ...fields, msg }),
  child: (more) => capturing(records, { ...bindings, ...more }, enabled),
});

test("timings sum per key and log each key once per flush, then start a new window", async () => {
  const records: Entry[] = [];
  const timings = createTimings(capturing(records), "Timings", "consumerGroup");
  await timings.time("saga:a", "read", async () => {});
  await timings.time("saga:a", "read", async () => {});
  timings.add("saga:a", "events", 7);
  await expect(timings.time("saga:b", "commit", async () => { throw new Error("down"); })).rejects.toThrow("down");
  timings.flush();
  // One Admission per dispatch.
  expect(records).toMatchObject([
    { consumerGroup: "saga:a", readCount: 2, events: 7, msg: "Timings" },
    { consumerGroup: "saga:b", commitCount: 1, msg: "Timings" },
  ]);
  expect(records[0]).toHaveProperty("readMs");
  expect(records[0]).toHaveProperty("windowMs");
  timings.flush();
  expect(records).toHaveLength(2);
});

test("disabled timings return the original work without measuring or keeping counts", async () => {
  const records: Entry[] = [];
  let enabled = false;
  const timings = createTimings(capturing(records, {}, () => enabled), "Timings", "group");
  const clock = vi.spyOn(performance, "now");
  const work = Promise.resolve("done");
  try {
    expect(timings.time("saga:a", "read", () => work)).toBe(work);
    timings.add("saga:a", "events", 7);
    expect(clock).not.toHaveBeenCalled();
    enabled = true;
    timings.flush();
    expect(records).toEqual([]);
    await timings.time("saga:a", "read", () => work);
    timings.flush();
    expect(records).toMatchObject([{ readCount: 1 }]);
  } finally {
    clock.mockRestore();
  }
});

test.each(["accepted", "failed", "thrown"] as const)("the Outbox relay records %s deliveries", async (outcome) => {
  const records: Entry[] = [];
  const timings = createTimings(capturing(records), "Outbox timings", "consumerGroup");
  const store = createInMemoryEventStore();
  const lease = (await store.acquireLease("outbox", "me"))!;
  const send = async () => {
    if (outcome === "thrown") throw new Error("broker disconnected");
    return outcome === "failed"
      ? { status: "failed", error: { message: "handler failed" } } as const
      : { status: "accepted", events: [] } as const;
  };
  const relay = createOutboxRelay({ store, transport: { send, connected: () => true } as unknown as Transport,
    routing: { queue: (name) => name }, logger: capturing([]), held: () => true, running: () => true, wake() {}, timings });
  await store.recordOutbox([1, 2].map((i) => ({
    id: `c${i}`, kind: "command" as const,
    message: { id: `c${i}`, stream: { context: "sales", aggregate: "order", id: `${i}` }, name: "create",
      payload: {}, metadata: {}, schemaVersion: 1, correlationId: `c${i}`, timestamp: "2026-10-15T00:00:00.000Z" },
  })));
  await relay.dispatch(lease);
  await Promise.all(relay.inFlight());
  timings.flush();
  // One Admission per dispatch.
  expect(records).toMatchObject([
    { consumerGroup: "outbox", pollCount: 1, polledRows: 2, claimCount: 1, sendCount: 2,
      [outcome === "thrown" ? "sendErrors" : outcome]: 2,
      ...(outcome === "accepted" ? { completeCount: 2 } : {}),
    },
  ]);
  expect((await store.outboxStats()).pending).toBe(outcome === "accepted" ? 0 : 2);
});

test("Readmodel reads record the status wait apart from the document read", async () => {
  const records: Entry[] = [];
  const timings = createTimings(capturing(records), "Readmodel read timings", "readmodelGroup");
  const store = { checkpoint: async () => null, findOne: async () => ({ _id: "1" }) };
  const readmodels = createReadmodels(
    { billing: { store, models: { invoices: {} } } } as any,
    createInMemoryTransport(),
    () => false,
    () => false,
    timings,
  );
  await readmodels.repository("billing", "invoices")("1");
  timings.flush();
  expect(records).toMatchObject([{ readmodelGroup: "billing", statusCount: 1, findCount: 1 }]);
});

test("Consumer group Lease changes are debug records of the consumers component", async () => {
  const records: Entry[] = [];
  const app = createApplication({
    name: "diagnostics",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [compile("sales", "order", { initialState: {}, commands: {}, events: {} })],
    sagas: { notice: { reactions: {} } },
    pollInterval: 5,
    logger: capturing(records),
  });
  await app.start();
  try {
    expect(records.find((r) => r.msg === "Application starting consumers"))
      .toMatchObject({ component: "consumers" });
    await expect
      .poll(() => records.find((r) => r.msg === "Lease acquired" && r.consumerGroup === "saga:notice"))
      .toMatchObject({ component: "consumers", epoch: 1 });
  } finally {
    await app.stop();
  }
});

test("Consumer group timings include the pause while its reads are behind", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "performance"] });
  const records: Entry[] = [];
  const timings = createTimings(capturing(records), "Timings", "consumerGroup");
  let running = true;
  const consumers = createConsumerGroups({
    store: createInMemoryEventStore(), owner: "me", leaseDuration: 30000,
    logger: capturing([]), timings, running: () => running,
    pause: async () => {
      await new Promise((resolve) => setTimeout(resolve, 250));
      running = false;
    },
  });
  try {
    consumers.run("saga:waiting", async (lease) => {
      await consumers.status(lease, { state: "waiting", waitingOn: ["billing"] });
      return false;
    });
    await vi.advanceTimersByTimeAsync(250);
    await Promise.all(consumers.loops);
    timings.flush();
    expect(records).toMatchObject([
      { consumerGroup: "saga:waiting", waitReadsMs: 250, waitReadsCount: 1 },
    ]);
  } finally {
    running = false;
    vi.useRealTimers();
  }
});

test("renewal logs a lost Lease even while the Consumer group is idle", async () => {
  vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
  const records: Entry[] = [];
  const store = createInMemoryEventStore();
  let running = true;
  const idle = Promise.withResolvers<void>();
  const paused = Promise.withResolvers<void>();
  const consumers = createConsumerGroups({
    store, owner: "me", leaseDuration: 30000, logger: capturing(records),
    running: () => running,
    pause: () => { paused.resolve(); return idle.promise; },
  });
  const stopRenewals = consumers.startRenewals();
  try {
    consumers.run("saga:notice", async () => false);
    await paused.promise;
    const lease = consumers.lease("saga:notice")!;
    await store.releaseLease(lease);
    await store.acquireLease("saga:notice", "other");
    await vi.advanceTimersByTimeAsync(10000);
    expect(records.find((r) => r.msg === "Lease lost"))
      .toMatchObject({ consumerGroup: "saga:notice", epoch: lease.epoch });
  } finally {
    running = false;
    idle.resolve();
    await stopRenewals();
    await Promise.all(consumers.loops);
    vi.useRealTimers();
  }
});

test("pool summaries sample only while debug is enabled and stop with the Application", async () => {
  vi.useFakeTimers({ toFake: ["setInterval", "clearInterval"] });
  const records: Entry[] = [];
  let enabled = false;
  const poolStats = vi.fn(() => ({ total: 5, idle: 2, waiting: 3 }));
  const app = createApplication({
    name: "diagnostics", only: ["commands:*"],
    store: Object.assign(createInMemoryEventStore(), { poolStats }),
    transport: createInMemoryTransport(),
    logger: capturing(records, {}, () => enabled),
  });
  try {
    await app.start();
    await vi.advanceTimersByTimeAsync(10000);
    expect(poolStats).not.toHaveBeenCalled();
    expect(records.filter((r) => r.msg === "Pool occupancy")).toEqual([]);
    enabled = true;
    await vi.advanceTimersByTimeAsync(10000);
    expect(records.find((r) => r.msg === "Pool occupancy"))
      .toMatchObject({ component: "store", pool: "events", waitingAvg: 3, waitingMax: 3, busyMax: 3 });
    await app.stop();
    poolStats.mockClear();
    await vi.advanceTimersByTimeAsync(10000);
    expect(poolStats).not.toHaveBeenCalled();
  } finally {
    await app.close();
    vi.useRealTimers();
  }
});

test("start and stop records say what the replica runs, without addresses", async () => {
  const records: Entry[] = [];
  const app = createApplication({
    name: "diagnostics",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [compile("sales", "order", { initialState: {}, commands: {}, events: {} })],
    sagas: { notice: { reactions: {} } },
    workers: { mail: { handles: ["mail"], execute: async () => {} } },
    pollInterval: 5,
    logger: capturing(records),
  });
  await app.start({ only: ["saga:notice", "worker:*"] });
  expect(records.find((r) => r.msg === "Application started")).toMatchObject({
    component: "application",
    application: "diagnostics",
    version: expect.any(String),
    node: process.version,
    roles: { saga: ["notice"], worker: ["mail"] },
    sagas: ["notice"],
    workers: ["mail"],
    slowHandlerThreshold: 1000,
  });
  await app.stop();
  expect(records.find((r) => r.msg === "Application stopping")).toMatchObject({ component: "application" });
  expect(records.find((r) => r.msg === "Application stopped")).toMatchObject({
    component: "application",
    durationMs: expect.any(Number),
  });
});

test("slow Command handlers and Reactions are warnings that name the Command or Event", async () => {
  const records: Entry[] = [];
  const pause = () => new Promise((resolve) => setTimeout(resolve, 30));
  const app = createApplication({
    name: "diagnostics",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [
      compile("sales", "order", {
        initialState: {},
        commands: { place: async (_: any, state: any) => { await pause(); state.apply.placed({}); } },
        events: { placed: () => {} },
      }),
    ],
    sagas: { notice: { reactions: { "sales.order.placed": async () => { await pause(); } } } },
    pollInterval: 5,
    slowHandlerThreshold: 20,
    logger: capturing(records),
  });
  await app.start();
  try {
    const outcome = await app.sendCommand({ stream: { context: "sales", aggregate: "order", id: "1" }, name: "place" });
    expect(records.find((r) => r.msg === "Slow Command handler")).toMatchObject({
      component: "commands",
      command: "place",
      stream: "sales.order.1",
      correlationId: outcome.status === "accepted" ? outcome.events[0]!.correlationId : undefined,
      durationMs: expect.any(Number),
    });
    await expect.poll(() => records.find((r) => r.msg === "Slow Reaction")).toMatchObject({
      component: "consumers",
      consumerGroup: "saga:notice",
      event: "sales.order.placed",
      stream: "sales.order.1",
    });
  } finally {
    await app.stop();
  }
});

test("dead-letter and halt records name the recovery command", async () => {
  const records: Entry[] = [];
  const store = createInMemoryEventStore();
  const app = createApplication({
    name: "diagnostics",
    store,
    transport: createInMemoryTransport(),
    aggregates: [compile("sales", "order", { initialState: {}, commands: {}, events: {} })],
    workers: { mail: { handles: ["mail"], execute: async () => { throw new Error("provider down"); }, retry: { attempts: 1 } } },
    pollInterval: 5,
    logger: capturing(records),
  });
  await app.start();
  try {
    await app.config.transport.enqueue("diagnostics.workers.mail", { id: "m1", name: "mail", payload: {}, metadata: {}, correlationId: "c1", idempotencyKey: "k1" } as any);
    await expect.poll(() => records.find((r) => r.msg === "Worker Message dead-lettered")).toBeDefined();
    const [letter] = await store.deadLetters();
    expect(records.find((r) => r.msg === "Worker Message dead-lettered")).toMatchObject({
      deadLetterId: letter!.id,
      recovery: `fix the cause, then event-fabric dead-letters replay ${letter!.id} (or discard ${letter!.id})`,
    });
  } finally {
    await app.stop();
  }

  const halted: Entry[] = [];
  let clock = Date.now(), running = true;
  const consumers = createConsumerGroups({
    store: createInMemoryEventStore(), owner: "me", leaseDuration: 30000, logger: capturing(halted),
    running: () => running, now: () => clock,
    pause: async (ms?: number) => { clock += ms ?? 1000; },
  });
  consumers.run("saga:broken", async (lease) => {
    consumers.observe(lease.group, { id: "e1", correlationId: "c1", causationId: "c0" } as any);
    if (halted.some((r) => r.state === "halted")) running = false;
    throw new Error("poison");
  });
  await Promise.all(consumers.loops);
  expect(halted.find((r) => r.state === "halted")).toMatchObject({
    msg: "Consumer group halted; retrying the same Event",
    consumerGroup: "saga:broken",
    eventId: "e1",
    attempts: 5,
    recovery: "fix the cause, then event-fabric consumers retry saga:broken (or consumers skip saga:broken --event e1)",
  });
});
