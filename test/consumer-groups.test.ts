import { expect, test } from "vitest";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createConsumerGroups } from "../src/runtime/consumer-groups.js";
import type { Logger } from "../src/types.js";

const quiet: Logger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
  child: () => quiet,
};
// A fake clock: every pause advances time instead of waiting, and yields one macrotask.
const harness = () => {
  const store = createInMemoryEventStore();
  let time = Date.now(),
    running = true;
  const consumers = createConsumerGroups({
    store,
    owner: "me",
    leaseDuration: 30000,
    logger: quiet,
    pause: async (ms = 1000) => {
      time += ms;
      await new Promise((resolve) => setImmediate(resolve));
    },
    running: () => running,
    now: () => time,
  });
  return {
    store,
    consumers,
    stop: async () => {
      running = false;
      await Promise.all(consumers.loops);
    },
  };
};

test("a Consumer group halts after five failures of the same Event", async () => {
  const { consumers, stop } = harness();
  let attempts = 0;
  consumers.run("saga:poison", async () => {
    attempts++;
    if (attempts === 5) setImmediate(stop);
    throw new Error("poison");
  });
  await expect.poll(() => attempts).toBe(5);
  await stop();
  expect(consumers.statuses.get("saga:poison")).toMatchObject({
    state: "halted",
    attempts: 5,
    error: "poison",
  });
});

test("retryConsumer ends the backoff without waiting for it", async () => {
  const { store, consumers, stop } = harness();
  let attempts = 0;
  consumers.run("saga:flaky", async () => {
    attempts++;
    if (attempts === 1) {
      await store.retryConsumer("saga:flaky");
      throw new Error("once");
    }
    void stop();
    return false;
  });
  await expect.poll(() => attempts).toBe(2);
  await stop();
  expect(consumers.statuses.get("saga:flaky")).toMatchObject({
    state: "running",
    attempts: 0,
  });
});

test("a group kind fences writes with each new Lease epoch and owns its Checkpoint", async () => {
  const { consumers, stop } = harness();
  const fenced: number[] = [];
  consumers.define("readmodels:list", {
    checkpoint: async () => ({ transactionId: "7", sequence: "1" }),
    fence: async (epoch) => {
      fenced.push(epoch);
    },
  });
  consumers.run("readmodels:list", async () => {
    void stop();
    return false;
  });
  await expect.poll(() => fenced.length).toBe(1);
  await stop();
  expect(consumers.statuses.get("readmodels:list")?.checkpoint).toEqual({
    transactionId: "7",
    sequence: "1",
  });
});

test("hold acquires, renews and releases a Lease for bounded work, and hands a held Lease to busy", async () => {
  const store = createInMemoryEventStore();
  const consumers = createConsumerGroups({
    store,
    owner: "me",
    leaseDuration: 60,
    logger: quiet,
    pause: async () => {},
    running: () => true,
  });
  const result = await consumers.hold(
    "rebuild:g",
    async (lease, alive) => {
      const expiry = lease.expiresAt;
      // Longer than the Lease itself: only renewal keeps it alive.
      await new Promise((resolve) => setTimeout(resolve, 150));
      alive();
      const [status] = (await store.consumers()).filter(
        (status) => status.group === "rebuild:g",
      );
      expect(Date.parse(status!.lease!.expiresAt)).toBeGreaterThan(
        Date.parse(expiry),
      );
      return "done";
    },
    async () => "busy",
  );
  expect(result).toBe("done");
  expect(await store.acquireLease("rebuild:g", "other", 60000)).toBeTruthy();
  expect(
    await consumers.hold(
      "rebuild:g",
      async () => "work",
      async () => "busy",
    ),
  ).toBe("busy");
});

test("Leases spread evenly across live instances by capped rendezvous score, and move to the survivor when one stops", async () => {
  const store = createInMemoryEventStore();
  const groups = Array.from({ length: 8 }, (_, i) => `saga:g${i}`);
  const instance = (owner: string) => {
    let running = true;
    const consumers = createConsumerGroups({
      store,
      owner,
      leaseDuration: 300,
      logger: quiet,
      pause: (ms = 10) =>
        new Promise((resolve) => setTimeout(resolve, Math.min(ms, 10))),
      running: () => running,
    });
    for (const group of groups) consumers.run(group, async () => false);
    const renewals = consumers.startRenewals();
    return async () => {
      running = false;
      await Promise.all(consumers.loops);
      await renewals();
      await consumers.release();
    };
  };
  const owners = async () =>
    Object.fromEntries(
      (await store.consumers())
        .filter(
          (s) =>
            groups.includes(s.group) &&
            s.lease &&
            Date.parse(s.lease.expiresAt) > Date.now(),
        )
        .map((s) => [s.group, s.lease!.owner]),
    );
  const stopA = instance("a"),
    stopB = instance("b");
  // Capped rendezvous: 8 groups over 2 instances is 4 each.
  const spread = async () => {
    const counts: Record<string, number> = {};
    for (const owner of Object.values(await owners()))
      counts[owner] = (counts[owner] ?? 0) + 1;
    return counts;
  };
  await expect.poll(spread, { timeout: 3000 }).toEqual({ a: 4, b: 4 });
  await stopB();
  await expect
    .poll(owners, { timeout: 3000 })
    .toEqual(Object.fromEntries(groups.map((g) => [g, "a"])));
  await stopA();
});

test("an instance that cannot run a group releases its Lease and stops advertising it, so another keeps it", async () => {
  const store = createInMemoryEventStore();
  const instance = (owner: string, requires: () => boolean) => {
    let running = true;
    const consumers = createConsumerGroups({
      store,
      owner,
      leaseDuration: 300,
      logger: quiet,
      pause: (ms = 10) =>
        new Promise((resolve) => setTimeout(resolve, Math.min(ms, 10))),
      running: () => running,
    });
    consumers.run("outbox", async () => false, { requires });
    const renewals = consumers.startRenewals();
    return async () => {
      running = false;
      await Promise.all(consumers.loops);
      await renewals();
      await consumers.release();
    };
  };
  const owner = async () => {
    const row = (await store.consumers()).find((s) => s.group === "outbox");
    return row?.lease && Date.parse(row.lease.expiresAt) > Date.now()
      ? row.lease.owner
      : undefined;
  };
  const advertised = async (name: string) =>
    (await store.consumers()).find((s) => s.group === `instance:${name}`)?.groups;
  let connected = true;
  const stopA = instance("a", () => connected);
  await expect.poll(owner, { timeout: 3000 }).toBe("a");
  connected = false;
  await expect.poll(owner, { timeout: 3000 }).toBeUndefined();
  await expect.poll(() => advertised("a"), { timeout: 3000 }).toEqual([]);
  const stopB = instance("b", () => true);
  await expect.poll(owner, { timeout: 3000 }).toBe("b");
  // Several Lease durations later, balancing has not handed it back to "a".
  await new Promise((resolve) => setTimeout(resolve, 1000));
  expect(await owner()).toBe("b");
  await stopB();
  await stopA();
});

test("status reports lag only for groups that keep a Checkpoint", async () => {
  const { store, consumers } = harness();
  await store.appendToStream({ context: "sales", aggregate: "order", id: "1" }, 0, [
    { id: "e1", name: "sales.order.opened", payload: {}, metadata: {}, schemaVersion: 1,
      timestamp: new Date().toISOString(), commandId: "c", correlationId: "c" },
  ]);
  for (const group of ["outbox", "saga:notice", "rebuild:list"]) await store.acquireLease(group, "me");
  const lag = Object.fromEntries((await consumers.consumers()).map((s) => [s.group, s.lagMs]));
  expect(lag).toEqual({ outbox: undefined, "saga:notice": 0, "rebuild:list": undefined });
});
