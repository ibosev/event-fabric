import { describe, expect, it } from "vitest";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { LeaseLostError } from "../src/errors.js";
import type { PendingEvent, OutboxEntry } from "../src/types.js";
const stream = { context: "shop", aggregate: "order", id: "1" };
const event = (id: string): PendingEvent => ({
  id,
  name: "shop.order.opened",
  payload: { amount: 1 },
  metadata: {},
  schemaVersion: 1,
  timestamp: "2026-10-15T00:00:00.000Z",
  commandId: "cmd",
  correlationId: "cmd",
});
export const entry = (id: string): OutboxEntry => ({
  id,
  kind: "message",
  queue: "mail",
  message: {
    id,
    name: "mail",
    payload: {},
    metadata: {},
    schemaVersion: 1,
    timestamp: "2026-10-15T00:00:00.000Z",
    correlationId: "cmd",
  },
});
describe("in-memory Event store", () => {
  it("keeps Stream Revisions and atomically refuses an empty stale append and its Outbox", async () => {
    const store = createInMemoryEventStore();
    const [saved] = await store.appendToStream(stream, 0, [event("e1")]);
    expect(saved?.revision).toBe(1);
    await expect(
      store.appendToStream(stream, 0, [], undefined, { outbox: [entry("no")] }),
    ).rejects.toThrow("Revision");
    expect(await store.pendingOutbox()).toEqual([]);
    expect(await store.readStream({ ...stream, aggregate: "other" })).toEqual(
      [],
    );
    expect((await store.readAll())[0]?.id).toBe("e1");
  });
});

it("validates append envelopes and imports without changing stored state", async () => {
  const store = createInMemoryEventStore();
  await expect(
    store.appendToStream(stream, 0, [
      { ...event("bad"), timestamp: "yesterday" },
    ]),
  ).rejects.toThrow("timestamp");
  await expect(
    store.appendToStream(stream, 0, [
      { ...event("bad"), metadata: { a: "x".repeat(8192) } },
    ]),
  ).rejects.toThrow("8192 bytes");
  await expect(
    store.appendToStream(stream, 0, [
      { ...event("bad"), payload: { value: Infinity } },
    ]),
  ).rejects.toThrow("JSON");
  expect(await store.readAll()).toEqual([]);
});

it("treats Stream triple as identity regardless of object property order", async () => {
  const store = createInMemoryEventStore();
  await store.appendToStream(stream, 0, [event("one")]);
  expect(
    await store.readStream({ id: "1", aggregate: "order", context: "shop" }),
  ).toHaveLength(1);
});

it("upcasts only read copies and rejects incomplete chains at construction", async () => {
  const options = {
    upcasters: {
      "shop.order.opened": {
        version: 3,
        upcasters: {
          1: (payload: any) => ({ cents: payload.amount * 100 }),
          2: (payload: any) => ({ ...payload, currency: "EUR" }),
        },
      },
    },
  };
  const store = createInMemoryEventStore(options);
  const [saved] = await store.appendToStream(stream, 0, [event("old")]);
  expect(saved?.schemaVersion).toBe(1);
  const [read] = await store.readStream(stream);
  expect(read).toMatchObject({
    schemaVersion: 3,
    payload: { cents: 100, currency: "EUR" },
  });
  read!.payload.cents = 0;
  expect((await store.readStream(stream))[0]?.payload.cents).toBe(100);
  expect(() =>
    createInMemoryEventStore({
      upcasters: {
        x: { version: 3, upcasters: { 1: (payload: any) => payload } },
      },
    }),
  ).toThrow("Missing Upcaster");
});

it("atomically fences Saga state and Outbox, including expired ownership", async () => {
  const store = createInMemoryEventStore(),
    position = { transactionId: "1", sequence: "1" };
  const first = (await store.acquireLease("payments", "first"))!;
  await store.releaseLease(first);
  const second = (await store.acquireLease("payments", "second"))!;
  await expect(
    store.commitSaga(
      first,
      position,
      [{ id: "one", state: { charged: true } }],
      [entry("charge")],
    ),
  ).rejects.toThrow("Lease");
  expect(await store.loadSaga("payments", "one")).toBeNull();
  expect(await store.checkpoint("payments")).toBeNull();
  expect(await store.pendingOutbox()).toEqual([]);
  await store.commitSaga(
    second,
    position,
    [{ id: "one", state: { charged: true } }],
    [entry("charge")],
  );
  expect(await store.loadSaga("payments", "one")).toEqual({ charged: true });
});

it("a delayed Outbox row holds back nothing else in its transaction, and its replacement is pending at once", async () => {
  const store = createInMemoryEventStore();
  await store.recordOutbox([
    {
      ...entry("later"),
      notBefore: "2099-01-01T00:00:00.000Z",
      replaceKey: "reminder",
    },
    entry("after"),
  ]);
  expect((await store.pendingOutbox()).map((row) => row.id)).toEqual(["after"]);
  await store.recordOutbox([
    { ...entry("replacement"), replaceKey: "reminder" },
  ]);
  expect((await store.pendingOutbox()).map((row) => row.id)).toEqual([
    "after",
    "replacement",
  ]);
  await store.deadLetterOutbox("replacement", "failed");
  await store.replayDeadLetter("replacement");
  const replayed = (await store.pendingOutbox()).find(
    (row) => row.message.id === "replacement",
  );
  expect(replayed?.id).not.toBe("replacement");
  expect(replayed?.attempt).toBe(0);
  expect(replayed?.message.attempt).toBe(1);
});

it("operator skip records the exact halted Event and fences its old owner", async () => {
  const store = createInMemoryEventStore(),
    position = { transactionId: "7", sequence: "2" };
  const lease = (await store.acquireLease("broken", "old"))!;
  await store.setConsumerStatus(
    {
      group: "broken",
      partition: 0,
      checkpoint: null,
      state: "halted",
      eventId: "bad",
      position,
      error: "invalid",
      attempts: 5,
    },
    lease,
  );
  await expect(
    store.skipConsumer("broken", "different", "operator"),
  ).rejects.toThrow("exactly");
  expect(await store.checkpoint("broken")).toBeNull();
  await store.skipConsumer("broken", "bad", "operator");
  expect(await store.checkpoint("broken")).toEqual(position);
  expect(await store.deadLetters()).toMatchObject([
    { kind: "skipped-event", eventId: "bad", who: "operator" },
  ]);
  await expect(
    store.commitSaga(
      lease,
      position,
      [{ id: "one", state: { bad: true } }],
      [],
    ),
  ).rejects.toThrow("Lease");
});

it("keeps memory commits atomic when a Saga state is not JSON and protects dead-letter replacement keys", async () => {
  const store = createInMemoryEventStore(),
    lease = (await store.acquireLease("atomic", "one"))!;
  await expect(
    store.commitSaga(
      lease,
      { transactionId: "1", sequence: "1" },
      [{ id: "bad", state: { fn: () => 1 } }],
      [entry("must-not-exist")],
    ),
  ).rejects.toThrow("JSON");
  expect(await store.pendingOutbox()).toEqual([]);
  expect(await store.checkpoint("atomic")).toBeNull();
  await store.recordOutbox([{ ...entry("blocked"), replaceKey: "protected" }]);
  await store.deadLetterOutbox("blocked", "offline");
  await expect(
    store.recordOutbox([{ ...entry("replace"), replaceKey: "protected" }]),
  ).rejects.toThrow("dead-letter");
  expect(await store.pendingOutbox()).toEqual([]);
});

it("Outbox metrics count the full delayed backlog beyond one dispatcher page", async () => {
  const store = createInMemoryEventStore();
  await store.recordOutbox(
    Array.from({ length: 101 }, (_, index) => ({
      ...entry(`backlog-${index}`),
      notBefore: "2100-01-01T00:00:00.000Z",
    })),
  );
  expect(await store.pendingOutbox()).toEqual([]);
  expect(await store.outboxStats()).toMatchObject({ pending: 101 });
});

it("refuses to audit a Saga skip after its retry has already committed", async () => {
  const store = createInMemoryEventStore();
  const [saved] = await store.appendToStream(stream, 0, [event("skip-race")]);
  const lease = (await store.acquireLease("saga:skip-race", "owner"))!;
  await store.setConsumerStatus(
    {
      group: lease.group,
      partition: 0,
      state: "halted",
      eventId: saved!.id,
      position: saved!.position,
      attempts: 5,
    },
    lease,
  );
  await store.commitSaga(
    lease,
    saved!.position,
    [{ id: "one", state: { done: true } }],
    [entry("committed-effect")],
  );
  await expect(
    store.skipConsumer(lease.group, saved!.id, "operator"),
  ).rejects.toThrow(/already processed/);
  expect(await store.deadLetters()).toEqual([]);
  expect(await store.loadSaga(lease.group, "one")).toEqual({ done: true });
  expect((await store.pendingOutbox()).map((row) => row.id)).toEqual([
    "committed-effect",
  ]);
});

it("registers compiled Upcasters after creation and refuses conflicting definitions", async () => {
  const store = createInMemoryEventStore();
  await store.appendToStream(stream, 0, [{ ...event("v1"), name: "opened" }]);
  const opened = {
    version: 2,
    upcasters: { 1: (payload: any) => ({ cents: payload.amount * 100 }) },
  };
  store.registerUpcasters({ "shop.order.opened": opened });
  store.registerUpcasters({ "shop.order.opened": { ...opened } });
  expect(await store.readStream(stream)).toMatchObject([
    { schemaVersion: 2, payload: { cents: 100 } },
  ]);
  expect((await store.readAll())[0]?.payload).toEqual({ cents: 100 });
  expect(() =>
    store.registerUpcasters({
      "shop.order.opened": { ...opened, upcasters: { 1: (p: any) => p } },
    }),
  ).toThrow("Conflicting Upcasters");
  expect(() =>
    store.registerUpcasters({
      "shop.order.closed": { version: 2, upcasters: {} },
    }),
  ).toThrow("Missing Upcaster");
});

it("renews a batch of Leases in input order and reports lost ones as null", async () => {
  const store = createInMemoryEventStore();
  const a = (await store.acquireLease("a", "me"))!,
    b = (await store.acquireLease("b", "me"))!;
  const renewed = await store.renewLeases(
    [b, { ...a, epoch: a.epoch + 1 }, a],
    60000,
  );
  expect(renewed.map((lease) => lease?.group ?? null)).toEqual([
    "b",
    null,
    "a",
  ]);
  expect(Date.parse(renewed[0]!.expiresAt)).toBeGreaterThan(
    Date.parse(b.expiresAt),
  );
  expect(await store.renewLeases([])).toEqual([]);
});

it("dead-letters an Outbox row with the true number of delivery attempts", async () => {
  const store = createInMemoryEventStore();
  await store.recordOutbox([entry("five")]);
  await store.retryOutbox("five", 4, new Date(0).toISOString());
  await store.deadLetterOutbox("five", "failed");
  expect((await store.deadLetters())[0]?.attempts).toBe(5);
});

it("imports Saga state only at the current log head", async () => {
  const store = createInMemoryEventStore();
  await store.importSagas("empty", [{ id: "one", state: {} }], null);
  const [first] = await store.appendToStream(stream, 0, [event("h1")]);
  const [second] = await store.appendToStream(stream, 1, [event("h2")]);
  await expect(
    store.importSagas("sagas", [{ id: "one", state: {} }], null),
  ).rejects.toThrow("log head");
  await expect(
    store.importSagas("sagas", [{ id: "one", state: {} }], first!.position),
  ).rejects.toThrow("log head");
  expect(await store.loadSaga("sagas", "one")).toBeNull();
  await store.importSagas(
    "sagas",
    [{ id: "one", state: {} }],
    second!.position,
  );
  expect(await store.checkpoint("sagas")).toEqual(second!.position);
});

it("applies the configured metadata limit and metadata schema to appends, imports and Outbox", async () => {
  const store = createInMemoryEventStore({
    metadataMaxBytes: 16,
    metadataSchema: { type: "object" },
    validate: async (_schema, value) => {
      if (typeof value.a === "number") throw new Error("metadata invalid");
    },
  });
  const large = { a: "x".repeat(20) };
  await expect(
    store.appendToStream(stream, 0, [{ ...event("big"), metadata: large }]),
  ).rejects.toThrow("16 bytes");
  await expect(
    store.recordOutbox([
      {
        ...entry("big"),
        message: { ...entry("big").message, metadata: large },
      },
    ]),
  ).rejects.toThrow("16 bytes");
  await expect(
    store.import([
      {
        ...event("bad"),
        metadata: { a: 1 },
        stream,
        revision: 1,
        position: { transactionId: "1", sequence: "1" },
      },
    ]),
  ).rejects.toThrow("metadata invalid");
  expect(await store.readAll()).toEqual([]);
});

it("reads backwards from an exclusive upper Position and filters only fully qualified Event names", async () => {
  const store = createInMemoryEventStore();
  const opened = (id: string) => ({ ...event(id), name: "opened" });
  await store.appendToStream(stream, 0, [opened("r1")]);
  await store.appendToStream(stream, 1, [opened("r2")]);
  const [last] = await store.appendToStream(stream, 2, [opened("r3")]);
  await store.appendToStream({ ...stream, aggregate: "invoice" }, 0, [
    opened("i1"),
  ]);
  expect(
    (await store.readAll({ reverse: true, fromPosition: last!.position })).map(
      (read) => read.id,
    ),
  ).toEqual(["r2", "r1"]);
  expect(
    (await store.readAll({ eventNames: ["shop.invoice.opened"] })).map(
      (read) => read.id,
    ),
  ).toEqual(["i1"]);
  await expect(store.readAll({ eventNames: ["opened"] })).rejects.toThrow(
    "fully qualified",
  );
});

it("refuses a backwards Checkpoint in memory as a lost Lease, like Postgres", async () => {
  const store = createInMemoryEventStore(),
    lease = (await store.acquireLease("backwards", "one"))!;
  await store.commitCheckpoint(lease, { transactionId: "2", sequence: "1" });
  await expect(
    store.commitCheckpoint(lease, { transactionId: "1", sequence: "1" }),
  ).rejects.toBeInstanceOf(LeaseLostError);
});
