import { expect, test, vi } from "vitest";
import { count } from "../src/observability.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import {
  createOutboxRelay,
  createWorkerRunner,
  permanent,
  retryDelay,
} from "../src/runtime/delivery.js";
import type { Logger, Message, OutboxEntry, Transport } from "../src/types.js";

vi.mock("../src/observability.js", async (actual) => {
  const module = await actual<typeof import("../src/observability.js")>();
  return { ...module, count: vi.fn(module.count) };
});

const quiet: Logger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
  child: () => quiet,
};
const command = (id: string, attempt?: number): OutboxEntry => ({
  id,
  kind: "command",
  attempt,
  message: {
    id,
    stream: { context: "sales", aggregate: "order", id },
    name: "create",
    payload: {},
    metadata: {},
    schemaVersion: 1,
    correlationId: id,
    timestamp: "2026-10-15T00:00:00.000Z",
  },
});
const failing = {
  send: async () => ({ status: "failed", error: { message: "down" } }),
} as unknown as Transport;

test("a delivery the broker loss interrupts stays pending without an attempt and logs a warning", async () => {
  const store = createInMemoryEventStore();
  const lease = (await store.acquireLease("outbox", "me"))!;
  const levels: string[] = [];
  const logger: Logger = {
    ...quiet,
    warn: () => void levels.push("warn"),
    error: () => void levels.push("error"),
  };
  const relay = createOutboxRelay({
    store,
    transport: {
      enqueue: async () => {
        throw new Error("Channel closed");
      },
      connected: () => false,
    } as unknown as Transport,
    routing: { queue: (name) => name },
    logger,
    held: () => true,
    running: () => true,
    wake: () => {},
  });
  await store.recordOutbox([{ ...command("receipt", 4), kind: "message", queue: "receipts" } as OutboxEntry]);
  for (const _ of [1, 2]) {
    await relay.dispatch(lease);
    await Promise.all(relay.inFlight());
  }
  expect(await store.deadLetters()).toEqual([]);
  expect(await store.pendingOutbox()).toMatchObject([{ id: "receipt", attempt: 4 }]);
  expect(levels).toEqual(["warn", "warn"]);
});

test("retry delays double from the base and stop at the cap", () => {
  expect([1, 2, 3].map((attempt) => retryDelay(attempt))).toEqual([
    1000, 2000, 4000,
  ]);
  expect(retryDelay(30)).toBe(600000);
  expect(retryDelay(3, { baseDelay: 10, maxDelay: 25 })).toBe(25);
});

test("the Outbox relay admits at most 100 concurrent deliveries across repeated wakes", async () => {
  const store = createInMemoryEventStore();
  const lease = (await store.acquireLease("outbox", "bounded", 30000))!;
  const release = Promise.withResolvers<void>();
  const send = vi.fn(async () => {
    await release.promise;
    return { status: "accepted", events: [] } as const;
  });
  const relay = createOutboxRelay({ store, transport: { send } as unknown as Transport,
    routing: { queue: (name) => name }, logger: quiet, held: () => true, running: () => true, wake() {} });
  await store.recordOutbox(Array.from({ length: 150 }, (_, i) => command(`bounded-${i}`)));
  try {
    expect(await relay.dispatch(lease)).toBe(true);
    expect(await relay.dispatch(lease)).toBe(false);
    expect(relay.inFlight()).toHaveLength(100);
    expect(await relay.nextDelay()).toBeNull();
  } finally {
    release.resolve();
    await Promise.all(relay.inFlight());
  }
  await relay.dispatch(lease);
  await Promise.all(relay.inFlight());
  expect(send).toHaveBeenCalledTimes(150);
});

test("the Outbox relay polls again at once only after a full batch", async () => {
  const store = createInMemoryEventStore();
  const lease = (await store.acquireLease("outbox", "partial", 30000))!;
  const send = vi.fn(async () => ({ status: "accepted", events: [] }) as const);
  const relay = createOutboxRelay({ store, transport: { send } as unknown as Transport,
    routing: { queue: (name) => name }, logger: quiet, held: () => true, running: () => true, wake() {} });
  await store.recordOutbox(Array.from({ length: 3 }, (_, i) => command(`partial-${i}`)));
  expect(await relay.dispatch(lease)).toBe(false);
  await Promise.all(relay.inFlight());
  expect(send).toHaveBeenCalledTimes(3);
});

test("Outbox delivery continues while metric scans are sampled every five seconds", async () => {
  const store = createInMemoryEventStore();
  const lease = (await store.acquireLease("outbox", "me"))!;
  const stats = vi.spyOn(store, "outboxStats");
  const clock = vi.spyOn(Date, "now");
  const now = Date.now();
  const send = vi.fn(async () => ({ status: "accepted", events: [] }) as const);
  const relay = createOutboxRelay({
    store,
    transport: { send } as unknown as Transport,
    routing: { queue: (name) => name },
    logger: quiet,
    held: () => true,
    running: () => true,
    wake: () => {},
  });
  try {
    for (const [index, elapsed] of [0, 100, 4999, 5000].entries()) {
      clock.mockReturnValue(now + elapsed);
      await store.recordOutbox([command(`sample-${index}`)]);
      await relay.dispatch(lease);
      await Promise.all(relay.inFlight());
    }
    expect(send).toHaveBeenCalledTimes(4);
    expect(stats).toHaveBeenCalledTimes(2);
  } finally {
    clock.mockRestore();
    stats.mockRestore();
  }
});

test("the Outbox relay schedules a retry, then dead-letters on the fifth failed delivery", async () => {
  const store = createInMemoryEventStore();
  const lease = (await store.acquireLease("outbox", "me"))!;
  const relay = createOutboxRelay({
    store,
    transport: failing,
    routing: { queue: (name) => name },
    logger: quiet,
    held: () => true,
    running: () => true,
    wake: () => {},
  });
  await store.recordOutbox([command("fresh")]);
  await store.recordOutbox([command("last", 4)]);
  const before = Date.now();
  await relay.dispatch(lease);
  await Promise.all(relay.inFlight());
  const letters = await store.deadLetters();
  expect(letters).toMatchObject([
    { id: "last", kind: "outbox", attempts: 5, error: "down" },
  ]);
  // The retried row is hidden until its notBefore, about one second out.
  expect(await store.pendingOutbox()).toEqual([]);
  expect((await store.outboxStats()).pending).toBe(1);
  expect(Date.now() - before).toBeLessThan(1000);
});

test("a failed Worker attempt is retried through the Outbox with jitter, and a permanent failure is dead-lettered", async () => {
  const memory = createInMemoryEventStore();
  const recorded: OutboxEntry[] = [];
  const store = {
    ...memory,
    recordOutbox: async (entries: OutboxEntry[]) => {
      recorded.push(...entries);
      return memory.recordOutbox(entries);
    },
  };
  const attempt = createWorkerRunner({
    store,
    logger: quiet,
    services: () => ({}),
    sendCommand: async () => ({ status: "accepted", events: [] }),
    workflowCalls: () => ({}) as never,
    workerQueue: (worker) => `app.workers.${worker}`,
    wake: () => {},
    random: () => 1,
  });
  const message: Message = {
    id: "m",
    name: "mail",
    payload: {},
    metadata: {},
    schemaVersion: 1,
    timestamp: "2026-10-15T00:00:00.000Z",
    correlationId: "c",
    idempotencyKey: "k",
    attempt: 2,
  };
  const before = Date.now();
  await attempt(
    "mailer",
    {
      handles: ["mail"],
      retry: { baseDelay: 100 },
      execute: () => {
        throw new Error("busy");
      },
    },
    message,
  );
  await attempt(
    "mailer",
    {
      handles: ["mail"],
      execute: () => {
        throw permanent(new Error("bad address"));
      },
    },
    { ...message, id: "n" },
  );
  // With random() = 1 the third attempt waits the full 100 * 2 ** 1 ms.
  expect(recorded).toMatchObject([
    { queue: "app.workers.mailer", message: { id: "m", attempt: 3 } },
  ]);
  const wait = Date.parse(recorded[0]!.notBefore!) - before;
  expect(wait).toBeGreaterThanOrEqual(200);
  expect(wait).toBeLessThan(400);
  expect(await store.deadLetters()).toMatchObject([
    {
      kind: "worker",
      attempts: 2,
      error: "bad address",
      message: { queue: "app.workers.mailer", message: { id: "n" } },
    },
  ]);
});

test("an unknown outcome is re-sent without counting an attempt, and warns once after ten minutes", async () => {
  const memory = createInMemoryEventStore();
  const lease = (await memory.acquireLease("outbox", "me"))!;
  await memory.recordOutbox([command("lost-reply", 4)]);
  const [row] = await memory.pendingOutbox();
  const warnings: string[] = [];
  // The row has waited eleven minutes and is always due again.
  const store = {
    ...memory,
    pendingOutbox: async () => [
      {
        ...row!,
        createdAt: new Date(Date.now() - 11 * 60_000).toISOString(),
      },
    ],
    claimOutbox: async () => true,
  };
  const relay = createOutboxRelay({
    store,
    transport: {
      send: async (sent: any) => ({ status: "unknown", commandId: sent.id }),
    } as unknown as Transport,
    routing: { queue: (name) => name },
    logger: { ...quiet, warn: (_: unknown, message: string) => void warnings.push(message) },
    held: () => true,
    running: () => true,
    wake: () => {},
  });
  for (const _ of [1, 2, 3]) {
    await relay.dispatch(lease);
    await Promise.all(relay.inFlight());
  }
  expect(warnings.filter((w) => /still unknown/.test(w))).toHaveLength(1);
  // Re-sends never reach the fifth attempt, so nothing is dead-lettered.
  expect(await memory.deadLetters()).toEqual([]);
  expect((await memory.outboxStats()).pending).toBe(1);
});

test("a rejected Outbox Command is dead-lettered and counted, without holding back the rest of its transaction", async () => {
  const store = createInMemoryEventStore();
  const lease = (await store.acquireLease("outbox", "me"))!;
  const sent: string[] = [];
  let refuse = true;
  const relay = createOutboxRelay({
    store,
    transport: {
      send: async (message: any) => {
        sent.push(message.id);
        return refuse && message.id === "refused"
          ? {
              status: "rejected",
              error: { code: "too-big", kind: "rule", message: "No", details: { max: 1 } },
            }
          : { status: "accepted", events: [] };
      },
    } as unknown as Transport,
    routing: { queue: (name) => name },
    logger: quiet,
    held: () => true,
    running: () => true,
    wake: () => {},
  });
  // One transaction: the rejected Command comes first.
  await store.recordOutbox([command("refused"), command("next")]);
  // An entry becomes due once the earlier ones of its transaction have left the Outbox.
  for (const _ of [1, 2]) {
    await relay.dispatch(lease);
    await Promise.all(relay.inFlight());
  }

  expect(sent).toEqual(["refused", "next"]);
  expect(await store.deadLetters()).toMatchObject([
    {
      id: "refused",
      kind: "outbox-rejected",
      error: "No",
      rejection: { code: "too-big", kind: "rule", message: "No", details: { max: 1 } },
      message: { message: { id: "refused" } },
    },
  ]);
  expect((await store.outboxStats()).pending).toBe(0);
  expect(count).toHaveBeenCalledWith("eventfabric.outbox.rejected", {
    command: "sales.order.create",
    code: "too-big",
  });

  // Not retried: another pass sends nothing.
  await relay.dispatch(lease);
  await Promise.all(relay.inFlight());
  expect(sent).toEqual(["refused", "next"]);

  // Once the refused data is fixed, replay re-sends the same Command id and clears the letter.
  refuse = false;
  await store.replayDeadLetter("refused");
  await relay.dispatch(lease);
  await Promise.all(relay.inFlight());
  expect(sent).toEqual(["refused", "next", "refused"]);
  expect(await store.deadLetters()).toEqual([]);
});

test("a dispatch admits its rows in one store call and completes them in batches", async () => {
  const memory = createInMemoryEventStore();
  const lease = (await memory.acquireLease("outbox", "me"))!;
  await memory.recordOutbox(Array.from({ length: 10 }, (_, n) => command(`row-${n}`)));
  const calls = { admit: 0, claim: 0, complete: [] as unknown[] };
  const store = {
    ...memory,
    admitOutbox: (...args: Parameters<typeof memory.admitOutbox>) => {
      calls.admit++;
      return memory.admitOutbox(...args);
    },
    claimOutbox: (...args: Parameters<typeof memory.claimOutbox>) => {
      calls.claim++;
      return memory.claimOutbox(...args);
    },
    completeOutbox: (ids: string | string[]) => {
      calls.complete.push(ids);
      return memory.completeOutbox(ids);
    },
  };
  const sent: string[] = [];
  const relay = createOutboxRelay({
    store,
    transport: {
      send: async (message: any) => {
        sent.push(message.id);
        return { status: "accepted", events: [] };
      },
    } as unknown as Transport,
    routing: { queue: (name) => name },
    logger: quiet,
    held: () => true,
    running: () => true,
    wake: () => {},
  });
  await relay.dispatch(lease);
  await Promise.all(relay.inFlight());
  expect(sent.sort()).toEqual(Array.from({ length: 10 }, (_, n) => `row-${n}`).sort());
  expect(calls.admit).toBe(1);
  expect(calls.claim).toBe(0);
  // Completions that arrive while one is written share the next write.
  expect(calls.complete.length).toBeLessThanOrEqual(2);
  expect(calls.complete.flat()).toHaveLength(10);
  expect((await memory.outboxStats()).pending).toBe(0);
});
