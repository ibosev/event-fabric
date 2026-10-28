import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { Pool } from "pg";
import { createPostgresEventStore } from "../../src/event-store/postgres.js";
import { createInMemoryEventStore } from "../../src/event-store/memory.js";
import { createConsumerGroups } from "../../src/runtime/consumer-groups.js";
import { createValidator } from "../../src/validation.js";
import { createCommand } from "../../src/id.js";
import type { OutboxEntry, PendingEvent } from "../../src/types.js";
const pool = new Pool({
  connectionString:
    process.env.POSTGRES_URL ??
    "postgres://eventfabric:eventfabric@127.0.0.1:55439/eventfabric",
});
const schema = `persistence_${Date.now()}`;
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
const store = createPostgresEventStore({ pool, schema });
const adapters = ["memory", "postgres"] as const;
/** The adapter under test; a fresh memory store per test, the shared Postgres schema otherwise. */
const persistence = (adapter: (typeof adapters)[number]) =>
  adapter === "memory" ? createInMemoryEventStore() : store;
for (const adapter of ["memory", "postgres"])
  it(`${adapter}: persisted Events, Sagas and Outbox use the same JSON undefined semantics`, async () => {
    const persistence =
      adapter === "memory" ? createInMemoryEventStore() : store;
    const input = {
      missing: undefined,
      list: [undefined, { missing: undefined }],
    };
    const expected = { list: [null, {}] };
    const id = `json_${adapter}_${crypto.randomUUID()}`;
    const [saved] = await persistence.appendToStream({ ...stream, id }, 0, [
      { ...event(id), payload: input, metadata: input },
    ]);
    expect(saved).toMatchObject({ payload: expected, metadata: expected });
    expect(saved!.payload).not.toHaveProperty("missing");
    expect(
      (await persistence.readStream({ ...stream, id }))[0]!.payload,
    ).toEqual(expected);
    const [imported] = await persistence.import([
      { ...saved!, id: `${id}-import`, revision: 2, payload: input },
    ]);
    expect(imported!.payload).toEqual(expected);
    const lease = (await persistence.acquireLease(`saga:${id}`, "writer"))!;
    await persistence.commitSaga(
      lease,
      imported!.position,
      [{ id, state: input }],
      [
        {
          id,
          kind: "message",
          message: {
            id,
            name: "mail",
            schemaVersion: 1,
            timestamp: saved!.timestamp,
            correlationId: id,
            payload: input,
            metadata: input,
          },
        },
      ],
    );
    expect(await persistence.loadSaga(lease.group, id)).toEqual(expected);
    const row = (await persistence.pendingOutbox()).find(
      (entry) => entry.id === id,
    )!;
    expect(row.message.payload).toEqual(expected);
    expect(row.message.metadata).toEqual(expected);
    await persistence.importSagas(
      `saga:import-${id}`,
      [{ id, state: input }],
      await persistence.head(),
    );
    expect(await persistence.loadSaga(`saga:import-${id}`, id)).toEqual(
      expected,
    );
    await persistence.releaseLease(lease);
    await persistence.completeOutbox(id);
  });
beforeAll(async () => {
  await store.migrate();
});
for (const adapter of ["memory", "postgres"])
  it(`${adapter}: an invalid Outbox metadata schema rolls back Events and Saga state`, async () => {
    const options = {
      metadataSchema: {
        type: "object",
        required: ["tenant"],
        properties: { tenant: { type: "string" } },
      },
      validate: createValidator({ mutate: false }),
    };
    const persistence =
      adapter === "memory"
        ? createInMemoryEventStore(options)
        : createPostgresEventStore({ pool, schema, ...options });
    const id = `schema_${adapter}_${crypto.randomUUID()}`;
    const bad: OutboxEntry = {
      id,
      kind: "message",
      message: {
        id,
        name: "mail",
        schemaVersion: 1,
        timestamp: event(id).timestamp,
        correlationId: id,
        payload: {},
        metadata: {},
      },
    };
    await expect(
      persistence.appendToStream(
        { ...stream, id },
        0,
        [{ ...event(id), metadata: { tenant: "one" } }],
        undefined,
        { outbox: [bad] },
      ),
    ).rejects.toThrow(/tenant/);
    expect(await persistence.readStream({ ...stream, id })).toEqual([]);
    const lease = (await persistence.acquireLease(`saga:${id}`, "owner"))!;
    await expect(
      persistence.commitSaga(
        lease,
        { transactionId: "1", sequence: "1" },
        [{ id, state: { sent: true } }],
        [bad],
      ),
    ).rejects.toThrow(/tenant/);
    expect(await persistence.loadSaga(lease.group, id)).toBeNull();
    expect(await persistence.checkpoint(lease.group)).toBeNull();
    await expect(persistence.recordOutbox([bad])).rejects.toThrow(/tenant/);
    expect(
      (await persistence.pendingOutbox()).some((row) => row.id === id),
    ).toBe(false);
    await persistence.releaseLease(lease);
  });
for (const adapter of ["memory", "postgres"])
  it(`${adapter}: admits Outbox rows by one rule for ids and replacement keys`, async () => {
    const persistence =
      adapter === "memory" ? createInMemoryEventStore() : store;
    const entry = (id: string, replaceKey?: string): OutboxEntry => ({
      id: `${adapter}-admit-${id}`,
      kind: "message",
      ...(replaceKey && { replaceKey: `${adapter}-admit-${replaceKey}` }),
      message: {
        id,
        name: "mail",
        payload: {},
        metadata: {},
        schemaVersion: 1,
        timestamp: "2026-10-15T00:00:00.000Z",
        correlationId: "admit",
      },
    });
    const pending = async () =>
      (await persistence.pendingOutbox(1000))
        .map((row) => row.id)
        .filter((id) => id.startsWith(`${adapter}-admit-`));
    await persistence.recordOutbox([entry("plain")]);
    await expect(persistence.recordOutbox([entry("plain")])).rejects.toThrow(
      "Duplicate Outbox id",
    );
    await expect(
      persistence.recordOutbox([entry("twice"), entry("twice")]),
    ).rejects.toThrow("Duplicate Outbox id");

    await persistence.recordOutbox([entry("old", "key")]);
    await persistence.recordOutbox([entry("new", "key")]);
    expect(await pending()).toEqual([
      `${adapter}-admit-plain`,
      `${adapter}-admit-new`,
    ]);

    const lease = (await persistence.acquireLease(
      `${adapter}-admit`,
      "dispatcher",
    ))!;
    expect(await persistence.claimOutbox(`${adapter}-admit-new`, lease)).toBe(
      true,
    );
    await persistence.recordOutbox([entry("next", "key")]);
    expect(await pending()).toEqual([
      `${adapter}-admit-plain`,
      `${adapter}-admit-new`,
      `${adapter}-admit-next`,
    ]);

    await persistence.deadLetterOutbox(`${adapter}-admit-next`, "offline");
    await expect(
      persistence.recordOutbox([entry("after", "key")]),
    ).rejects.toThrow("Cannot replace a dead-lettered Outbox row");
    expect(await pending()).toEqual([
      `${adapter}-admit-plain`,
      `${adapter}-admit-new`,
    ]);

    await persistence.discardDeadLetter(`${adapter}-admit-next`);
    for (const id of ["plain", "new"])
      await persistence.completeOutbox(`${adapter}-admit-${id}`);
    await persistence.releaseLease(lease);
  });
for (const adapter of ["memory", "postgres"])
  it(`${adapter}: records accepted Command ids atomically with the append and refuses a second append`, async () => {
    const { DuplicateCommandError } = await import("../../src/errors.js");
    const persistence =
      adapter === "memory" ? createInMemoryEventStore() : store;
    const id = `${adapter}-accepted-${Date.now()}`;
    const command = createCommand({
      id,
      stream: { context: "shop", aggregate: "order", id },
      name: "open",
    });
    const own = (eventId: string) => ({ ...event(eventId), commandId: id });
    const target = { context: "shop", aggregate: "order", id };
    expect(await persistence.acceptedCommand(id)).toBeNull();
    const saved = await persistence.appendToStream(
      target,
      0,
      [own(`${id}-e1`)],
      command,
      { fingerprint: "fp" },
    );
    expect(await persistence.acceptedCommand(id)).toMatchObject({
      fingerprint: "fp",
      events: [{ id: saved[0]!.id, position: saved[0]!.position }],
    });
    await expect(
      persistence.appendToStream(target, 1, [own(`${id}-e2`)], command, {
        fingerprint: "fp",
      }),
    ).rejects.toBeInstanceOf(DuplicateCommandError);
    expect(await persistence.readStream(target)).toHaveLength(1);
    // Concurrent appends of one id on two Streams: exactly one commits.
    const racing = `${id}-race`;
    const raceCommand = { ...command, id: racing };
    const results = await Promise.allSettled(
      ["x", "y"].map((suffix) =>
        persistence.appendToStream(
          { ...target, id: `${racing}-${suffix}` },
          0,
          [{ ...own(`${racing}-${suffix}`), commandId: racing }],
          raceCommand,
          { fingerprint: "fp" },
        ),
      ),
    );
    expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
    expect(results.find((r) => r.status === "rejected")?.reason).toBeInstanceOf(
      DuplicateCommandError,
    );
    // Without a fingerprint nothing is recorded, and an accepted Command may record no Events.
    const plain = `${id}-plain`;
    await persistence.appendToStream({ ...target, id: plain }, 0, [], {
      ...command,
      id: plain,
    });
    expect(await persistence.acceptedCommand(plain)).toBeNull();
    const empty = `${id}-empty`;
    await persistence.appendToStream(
      { ...target, id: empty },
      0,
      [],
      { ...command, id: empty },
      { fingerprint: "fp" },
    );
    expect(await persistence.acceptedCommand(empty)).toEqual({
      fingerprint: "fp",
      events: [],
    });
  });
for (const adapter of ["memory", "postgres"])
  it(`${adapter}: lists recorded Event names so startup can refuse undeclared ones`, async () => {
    const { assertDeclaredEvents } = await import("../../src/runtime/index.js");
    const { compile } = await import("../../src/authoring/index.js");
    const persistence =
      adapter === "memory" ? createInMemoryEventStore() : store;
    const id = `${adapter}-names-${Date.now()}`;
    await persistence.appendToStream(
      { context: "names", aggregate: "probe", id },
      0,
      [
        { ...event(`${id}-1`), name: "kept" },
        { ...event(`${id}-2`), name: "renamed" },
      ],
    );
    const names = await persistence.eventNames();
    expect(names).toEqual(
      expect.arrayContaining(["names.probe.kept", "names.probe.renamed"]),
    );
    expect(new Set(names).size).toBe(names.length);
    const declared = async (events: string[]) => [
      compile("names", "probe", {
        commands: {},
        events: Object.fromEntries(events.map((name) => [name, () => {}])),
      }),
      // Other tests share the Postgres log; declare every name they record.
      ...(await persistence.eventNames())
        .filter((name) => !name.startsWith("names.probe."))
        .map((name) => {
          // Some tests record dotted Event names, so only the first two dots separate.
          const [context, aggregate, ...rest] = name.split(".");
          return compile(context!, aggregate!, {
            commands: {},
            events: { [rest.join(".")]: () => {} },
          });
        }),
    ];
    await expect(
      assertDeclaredEvents(persistence, await declared(["kept"])),
    ).rejects.toThrow("names.probe.renamed");
    await assertDeclaredEvents(
      persistence,
      await declared(["kept", "renamed"]),
    );
  });
for (const adapter of ["memory", "postgres"])
  it(`${adapter}: a rejected Outbox Command is dead-lettered with its Rejection and replays with its Command id`, async () => {
    const { createOutboxRelay } = await import("../../src/runtime/delivery.js");
    const persistence =
      adapter === "memory" ? createInMemoryEventStore() : store;
    const id = `${adapter}-rejected-${crypto.randomUUID()}`;
    const quiet: any = {
      debug() {},
      info() {},
      warn() {},
      error() {},
      child: () => quiet,
    };
    const relay = createOutboxRelay({
      store: persistence,
      transport: {
        send: async () => ({
          status: "rejected",
          error: {
            code: "existence",
            kind: "existence",
            message: "Aggregate does not exist",
          },
        }),
      } as any,
      routing: { queue: (name) => name },
      logger: quiet,
      held: () => true,
      running: () => true,
      wake: () => {},
    });
    const command = createCommand({
      id,
      stream: { context: "shop", aggregate: "order", id },
      name: "pay",
      payload: {},
    });
    await persistence.recordOutbox([{ id, kind: "command", message: command }]);
    const lease = (await persistence.acquireLease("outbox", `owner-${id}`))!;
    // Other rows in the shared Postgres Outbox may come first; dispatch until ours is done.
    for (
      let i = 0;
      i < 20 && !(await persistence.deadLetters()).some((l) => l.id === id);
      i++
    ) {
      await relay.dispatch(lease);
      await Promise.all(relay.inFlight());
    }
    await persistence.releaseLease(lease);
    expect(
      (await persistence.deadLetters()).find((letter) => letter.id === id),
    ).toMatchObject({
      kind: "outbox-rejected",
      error: "Aggregate does not exist",
      rejection: { code: "existence", kind: "existence" },
      message: { id, kind: "command", message: { id } },
    });
    expect(
      (await persistence.pendingOutbox(1000)).some(
        (row) => row.message.id === id,
      ),
    ).toBe(false);

    await persistence.replayDeadLetter(id);
    const replayed = (await persistence.pendingOutbox(1000)).filter(
      (row) => row.message.id === id,
    );
    expect(replayed).toHaveLength(1);
    expect(replayed[0]!.id).not.toBe(id);
    expect(
      (await persistence.deadLetters()).some((letter) => letter.id === id),
    ).toBe(false);
    await persistence.completeOutbox(replayed[0]!.id);
  });
afterAll(async () => {
  await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  await pool.end();
});
for (const adapter of adapters)
  it(`${adapter}: commits Events and atomically refuses stale concurrent Revisions even for empty appends`, async () => {
    const store = persistence(adapter);
    const attempts = await Promise.allSettled([
      store.appendToStream(stream, 0, [event("a")]),
      store.appendToStream(stream, 0, [event("b")]),
    ]);
    expect(
      attempts.filter((result) => result.status === "fulfilled"),
    ).toHaveLength(1);
    expect(await store.readStream(stream)).toHaveLength(1);
    await expect(store.appendToStream(stream, 0, [])).rejects.toThrow(
      "Revision",
    );
    expect((await store.readAll())[0]?.position.transactionId).toMatch(/^\d+$/);
  });

// Postgres-only: Position visibility below the oldest running transaction (ADR-0004).
it("never advances a global cursor past a writing transaction that commits late", async () => {
  const blocker = await pool.connect();
  try {
    await blocker.query("BEGIN");
    await blocker.query("SELECT pg_current_xact_id()");
    const before = await store.head();
    const [written] = await store.appendToStream(
      { ...stream, id: "later" },
      0,
      [event("later")],
    );
    expect(
      (await store.readAll({ fromPosition: before })).map((item) => item.id),
    ).not.toContain("later");
    expect(await store.head()).toEqual(before);
    await blocker.query("COMMIT");
    expect(
      (await store.readAll({ fromPosition: before })).map((item) => item.id),
    ).toContain("later");
    expect(await store.head()).toEqual(written?.position);
  } finally {
    await blocker.query("ROLLBACK");
    blocker.release();
  }
});

for (const adapter of adapters)
  it(`${adapter}: commits Saga changes, Outbox and Checkpoint together and fences a replaced owner`, async () => {
    const store = persistence(adapter);
    const first = (await store.acquireLease("saga-test", "first"))!;
    const position = (await store.head())!;
    await store.commitSaga(
      first,
      position,
      [{ id: "one", state: { count: 1 } }],
      [],
    );
    expect(await store.loadSaga("saga-test", "one")).toEqual({ count: 1 });
    expect(await store.checkpoint("saga-test")).toEqual(position);
    await store.releaseLease(first);
    const second = (await store.acquireLease("saga-test", "second"))!;
    expect(second.epoch).toBe(first.epoch + 1);
    await expect(
      store.commitSaga(
        first,
        position,
        [{ id: "one", state: { count: 2 } }],
        [],
      ),
    ).rejects.toThrow("Lease");
    expect(await store.loadSaga("saga-test", "one")).toEqual({ count: 1 });
    await store.releaseLease(second);
  });

for (const adapter of adapters)
  it(`${adapter}: keeps original historical envelopes, rejects gaps and assigns fresh Positions`, async () => {
    const store = persistence(adapter);
    const historical = {
      ...event("historical"),
      stream: { ...stream, id: "imported" },
      revision: 1,
      position: { transactionId: "999999999", sequence: "7" },
    };
    const [saved] = await store.import([historical]);
    expect(saved).toMatchObject({
      id: "historical",
      timestamp: historical.timestamp,
      revision: 1,
    });
    expect(saved?.position).not.toEqual(historical.position);
    await expect(
      store.import([{ ...historical, id: "gap", revision: 3 }]),
    ).rejects.toThrow("Revision");
    expect(await store.readStream(historical.stream)).toHaveLength(1);
  });

for (const adapter of adapters)
  it(`${adapter}: imports a batch across Streams in input order and a later tail continues them`, async () => {
    const store = persistence(adapter);
    const a = { ...stream, id: `batch-a-${crypto.randomUUID()}` },
      b = { ...stream, id: `batch-b-${crypto.randomUUID()}` };
    const historical = (id: string, on: typeof stream, revision: number) => ({
      ...event(`${id}-${crypto.randomUUID()}`),
      stream: on,
      revision,
    });
    const before = await store.head();
    const first = await store.import([
      historical("a1", a, 1),
      historical("b1", b, 1),
      historical("a2", a, 2),
    ]);
    const tail = await store.import([
      historical("b2", b, 2),
      historical("a3", a, 3),
    ]);
    const saved = [...first, ...tail];
    const all = await store.readAll({ fromPosition: before });
    const ids = saved.map((e) => e.id);
    expect(all.map((e) => e.id).filter((id) => ids.includes(id))).toEqual(ids);
    expect(all.find((e) => e.id === saved[1]!.id)?.position).toEqual(saved[1]!.position);
    await expect(store.import([historical("a5", a, 5)])).rejects.toThrow("Revision");
    await expect(
      store.import([historical("b3", b, 3), historical("b3again", b, 3)]),
    ).rejects.toThrow("Revision");
    expect(await store.readStream(b)).toHaveLength(2);
  });

for (const adapter of adapters)
  it(`${adapter}: queued Messages of one transaction leave independently, and a dead letter holds back nothing`, async () => {
    const store = persistence(adapter);
    const make = (id: string) => ({
      id,
      kind: "message" as const,
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
    await store.recordOutbox([make("first"), make("second")]);
    await store.recordOutbox([make("parallel")]);
    expect((await store.pendingOutbox()).map((row) => row.id)).toEqual([
      "first",
      "second",
      "parallel",
    ]);
    await store.deadLetterOutbox("first", "provider failed");
    expect((await store.pendingOutbox()).map((row) => row.id)).toEqual([
      "second",
      "parallel",
    ]);
    expect(await store.deadLetters()).toMatchObject([
      { id: "first", kind: "outbox", error: "provider failed" },
    ]);
    await store.discardDeadLetter("first");
    await store.completeOutbox("second");
    await store.completeOutbox("parallel");
  });

for (const adapter of adapters)
  it(`${adapter}: rolls Saga effects back when an Outbox conflict aborts the transaction`, async () => {
    const store = persistence(adapter);
    const group = "rollback";
    const lease = (await store.acquireLease(group, "owner"))!;
    const make = (id: string) => ({
      id,
      kind: "message" as const,
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
    await store.recordOutbox([make("duplicate")]);
    await expect(
      store.commitSaga(
        lease,
        { transactionId: "99", sequence: "1" },
        [{ id: "one", state: { partial: true } }],
        [make("duplicate")],
      ),
    ).rejects.toThrow();
    expect(await store.loadSaga(group, "one")).toBeNull();
    expect(await store.checkpoint(group)).toBeNull();
    await store.completeOutbox("duplicate");
    await store.releaseLease(lease);
  });

for (const adapter of adapters)
  it(`${adapter}: imports Saga state at a supplied log head without recording effects`, async () => {
    const store = persistence(adapter);
    const head = await store.head();
    await store.importSagas(
      "imported-sagas",
      [{ id: "one", state: { waiting: true } }],
      head,
    );
    expect(await store.loadSaga("imported-sagas", "one")).toEqual({
      waiting: true,
    });
    expect(await store.checkpoint("imported-sagas")).toEqual(head);
    expect(await store.pendingOutbox()).toEqual([]);
    const lease = (await store.acquireLease("imported-sagas", "owner"))!;
    await expect(
      store.importSagas(
        "imported-sagas",
        [{ id: "one", state: { changed: true } }],
        head,
      ),
    ).rejects.toThrow("active Lease");
    expect(await store.loadSaga("imported-sagas", "one")).toEqual({
      waiting: true,
    });
    await store.releaseLease(lease);
  });

for (const adapter of adapters)
  it(`${adapter}: operator skip only advances the exact halted Event and invalidates the old Lease`, async () => {
    const store = persistence(adapter);
    const lease = (await store.acquireLease("halted", "owner"))!;
    await store.appendToStream({ ...stream, id: `halted_${adapter}` }, 0, [
      event(`halted_${adapter}`),
    ]);
    const position = (await store.head())!;
    await store.setConsumerStatus(
      {
        group: "halted",
        partition: 0,
        checkpoint: null,
        state: "halted",
        position,
        eventId: "poison",
        attempts: 5,
        error: "invalid",
      },
      lease,
    );
    await expect(
      store.skipConsumer("halted", "other", "operator"),
    ).rejects.toThrow("exactly");
    expect(await store.checkpoint("halted")).toBeNull();
    await store.skipConsumer("halted", "poison", "operator");
    expect(await store.checkpoint("halted")).toEqual(position);
    expect(await store.deadLetters()).toMatchObject([
      {
        kind: "skipped-event",
        group: "halted",
        eventId: "poison",
        who: "operator",
      },
    ]);
    await expect(store.commitCheckpoint(lease, position)).rejects.toThrow(
      "Lease",
    );
  });

// Postgres-only: schema quoting and SQL migrations.
it("quotes configurable schema and metadata paths and supports explicit idempotent dry-run migrations", async () => {
  const unusual = `odd '"_${Date.now()}`;
  const other = createPostgresEventStore({
    pool,
    schema: unusual,
    metadataIndexes: ["owner.account", "odd'path.field"],
  });
  const sql = await other.migrate({ dryRun: true });
  expect(sql).toContain("CREATE SCHEMA IF NOT EXISTS");
  await expect(other.readAll()).rejects.toThrow();
  try {
    await other.migrate();
    await other.migrate();
    await other.appendToStream(stream, 0, [event("quoted")]);
    expect((await other.readAll())[0]?.id).toBe("quoted");
  } finally {
    await pool.query(`DROP SCHEMA "${unusual.replaceAll('"', '""')}" CASCADE`);
  }
});

// Postgres-only: blocks the write with a row lock held by another connection.
it("holds the Lease fence through Saga effects and rolls back if it expires during a blocked write", async () => {
  const group = "blocked-saga",
    position = (await store.head())!,
    setup = (await store.acquireLease(group, "setup"))!;
  await store.commitSaga(
    setup,
    position,
    [{ id: "one", state: { count: 1 } }],
    [],
  );
  await store.releaseLease(setup);
  // The short Lease starts only now, so a slow setup cannot outlive it.
  const lease = (await store.acquireLease(group, "old", 120))!;
  const blocker = await pool.connect();
  try {
    await blocker.query("BEGIN");
    await blocker.query(
      `SELECT 1 FROM "${schema}".sagas WHERE group_name=$1 AND id='one' FOR UPDATE`,
      [group],
    );
    const work = store.commitSaga(
      lease,
      position,
      [{ id: "one", state: { count: 2 } }],
      [],
    );
    const failed = expect(work).rejects.toThrow("Lease");
    await new Promise((resolve) => setTimeout(resolve, 160));
    const takeover = store.acquireLease(group, "new");
    await blocker.query("COMMIT");
    await failed;
    const replacement = await takeover;
    expect(replacement?.epoch).toBe(lease.epoch + 1);
    expect(await store.loadSaga(group, "one")).toEqual({ count: 1 });
    expect(await store.checkpoint(group)).toEqual(position);
    if (replacement) await store.releaseLease(replacement);
  } finally {
    await blocker.query("ROLLBACK");
    blocker.release();
  }
});

for (const adapter of adapters)
  it(`${adapter}: replays a dead-lettered Message with a fresh Outbox id, without holding back the rest of its transaction`, async () => {
    const store = persistence(adapter);
    const make = (id: string) => ({
      id,
      kind: "message" as const,
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
    await store.recordOutbox([make("replay-head"), make("replay-tail")]);
    await store.retryOutbox("replay-head", 5, "2099-01-01T00:00:00.000Z");
    await store.deadLetterOutbox("replay-head", "offline");
    expect((await store.pendingOutbox()).map((item) => item.id)).toEqual([
      "replay-tail",
    ]);
    await store.replayDeadLetter("replay-head");
    const [row, tail] = await store.pendingOutbox();
    expect(row?.id).not.toBe("replay-head");
    expect(row?.message.id).toBe("replay-head");
    expect(row?.attempt).toBe(0);
    expect(row?.message.attempt).toBe(1);
    expect(tail?.id).toBe("replay-tail");
    await store.completeOutbox(row!.id);
    await store.completeOutbox("replay-tail");
  });

for (const adapter of adapters)
  it(`${adapter}: upcasts reads without rewriting historical payloads and store validation never mutates metadata`, async () => {
    const options: Parameters<typeof createInMemoryEventStore>[0] = {
      metadataSchema: { type: "object" },
      validate: async (_schema, value) => {
        value.changed = true;
      },
      upcasters: {
        "shop.order.old": {
          version: 2,
          upcasters: { 1: (payload) => ({ cents: payload.amount * 100 }) },
        },
      },
    };
    const upgraded =
      adapter === "memory"
        ? createInMemoryEventStore(options)
        : createPostgresEventStore({ pool, schema, ...options });
    const input = {
      ...event("upcast"),
      name: "old",
      metadata: { actor: "one" },
    };
    await upgraded.appendToStream({ ...stream, id: "upcast" }, 0, [input]);
    expect(
      (await upgraded.readStream({ ...stream, id: "upcast" }))[0],
    ).toMatchObject({
      schemaVersion: 2,
      payload: { cents: 100 },
      metadata: { actor: "one" },
    });
    // Only Postgres has a second, non-upcasting view of the same stored rows.
    if (adapter === "postgres")
      expect(
        (await store.readStream({ ...stream, id: "upcast" }))[0],
      ).toMatchObject({
        schemaVersion: 1,
        payload: { amount: 1 },
        metadata: { actor: "one" },
      });
    expect(input.metadata).toEqual({ actor: "one" });
  });

for (const adapter of adapters)
  it(`${adapter}: never admits a replaced pending Outbox row and keeps an admitted delivery recoverable`, async () => {
    const store = persistence(adapter);
    const entry = (id: string) => ({
      id,
      kind: "message" as const,
      replaceKey: "replace-race",
      message: {
        id,
        name: "mail",
        payload: {},
        metadata: {},
        schemaVersion: 1,
        timestamp: "2026-01-01T00:00:00.000Z",
        correlationId: "chain",
      },
    });
    const lease = (await store.acquireLease("outbox-race", "dispatcher"))!;
    await store.recordOutbox([entry("outbox-old")]);
    const stale = (await store.pendingOutbox()).find(
      (row) => row.id === "outbox-old",
    )!;
    await store.recordOutbox([entry("outbox-new")]);
    expect(await store.claimOutbox(stale.id, lease)).toBe(false);
    expect(await store.claimOutbox("outbox-new", lease)).toBe(true);
    await store.recordOutbox([entry("outbox-next")]);
    expect((await store.pendingOutbox()).map((row) => row.id)).toContain(
      "outbox-new",
    );
    expect((await store.pendingOutbox()).map((row) => row.id)).toContain(
      "outbox-next",
    );
    await store.completeOutbox("outbox-new");
    await store.completeOutbox("outbox-next");
  });

for (const adapter of adapters)
  it(`${adapter}: Postgres Outbox metrics include the full delayed backlog beyond the dispatcher page`, async () => {
    const store = persistence(adapter);
    const before = await store.outboxStats();
    const entries = Array.from({ length: 101 }, (_, index) => ({
      id: `metrics-${index}`,
      kind: "message" as const,
      notBefore: "2100-01-01T00:00:00.000Z",
      message: {
        id: `metrics-${index}`,
        name: "mail",
        payload: {},
        metadata: {},
        schemaVersion: 1,
        timestamp: "2026-01-01T00:00:00.000Z",
        correlationId: "metrics",
      },
    }));
    await store.recordOutbox(entries);
    expect((await store.outboxStats()).pending).toBe(before.pending + 101);
    for (const entry of entries) await store.completeOutbox(entry.id);
  });

for (const adapter of adapters)
  it(`${adapter}: refuses to audit a Saga skip after its retry has already committed`, async () => {
    const store = persistence(adapter);
    const [saved] = await store.appendToStream(
      { ...stream, id: "skip-race" },
      0,
      [event("skip-race")],
    );
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
      [
        {
          id: "committed-effect",
          kind: "message",
          message: {
            id: "effect",
            name: "work",
            payload: {},
            metadata: {},
            schemaVersion: 1,
            timestamp: "2026-01-01T00:00:00.000Z",
            correlationId: "chain",
          },
        },
      ],
    );
    await expect(
      store.skipConsumer(lease.group, saved!.id, "operator"),
    ).rejects.toThrow(/already processed/);
    expect(
      (await store.deadLetters()).filter(
        (letter) => letter.group === lease.group,
      ),
    ).toEqual([]);
    expect(await store.loadSaga(lease.group, "one")).toEqual({ done: true });
    expect(
      (await store.pendingOutbox()).some(
        (row) => row.id === "committed-effect",
      ),
    ).toBe(true);
    await store.completeOutbox("committed-effect");
  });

const message = (id: string): OutboxEntry => ({
  id,
  kind: "message",
  message: {
    id,
    name: "work",
    payload: {},
    metadata: {},
    schemaVersion: 1,
    timestamp: "2026-01-01T00:00:00.000Z",
    correlationId: id,
  },
});

for (const adapter of adapters)
  it(`${adapter}: renews a batch of Leases in one write, in input order, with null for lost Leases`, async () => {
    const store = persistence(adapter);
    const a = (await store.acquireLease("batch-a", "me"))!,
      b = (await store.acquireLease("batch-b", "me", 30000, 3))!;
    const renewed = await store.renewLeases(
      [b, { ...a, epoch: a.epoch + 1 }, a],
      60000,
    );
    expect(
      renewed.map((lease) => (lease ? [lease.group, lease.partition] : null)),
    ).toEqual([["batch-b", 3], null, ["batch-a", 0]]);
    expect(Date.parse(renewed[0]!.expiresAt)).toBeGreaterThan(
      Date.parse(b.expiresAt),
    );
    expect(await store.renewLeases([])).toEqual([]);
    await store.releaseLease(a);
    await store.releaseLease(b);
  });

// Postgres-only: LISTEN/NOTIFY wake-ups; memory has no `listen`.
it("wakes listening instances when Saga effects or Outbox entries are written", async () => {
  let wakes = 0;
  const stop = await store.listen!(() => wakes++);
  const woke = async (write: () => Promise<void>) => {
    const before = wakes;
    await write();
    for (let i = 0; i < 50 && wakes === before; i++)
      await new Promise((resolve) => setTimeout(resolve, 20));
    return wakes > before;
  };
  try {
    expect(await woke(() => store.recordOutbox([message("wake-record")]))).toBe(
      true,
    );
    const lease = (await store.acquireLease("wake-saga", "me"))!,
      head = (await store.head())!;
    expect(
      await woke(() =>
        store.commitSaga(lease, head, [], [message("wake-saga")]),
      ),
    ).toBe(true);
    expect(
      await woke(() =>
        store.retryOutbox("wake-record", 1, new Date(0).toISOString()),
      ),
    ).toBe(true);
    await store.releaseLease(lease);
  } finally {
    await stop();
    await store.completeOutbox("wake-record");
    await store.completeOutbox("wake-saga");
  }
});

for (const adapter of adapters)
  it(`${adapter}: dead-letters an Outbox row with the true number of delivery attempts`, async () => {
    const store = persistence(adapter);
    await store.recordOutbox([message("five-attempts")]);
    await store.retryOutbox("five-attempts", 4, new Date(0).toISOString());
    await store.deadLetterOutbox("five-attempts", "failed");
    expect(
      (await store.deadLetters()).find(
        (letter) => letter.id === "five-attempts",
      )?.attempts,
    ).toBe(5);
    await store.discardDeadLetter("five-attempts");
  });

for (const adapter of adapters)
  it(`${adapter}: imports Saga state only at the current log head`, async () => {
    const store = persistence(adapter);
    const [older] = await store.appendToStream({ ...stream, id: "import" }, 0, [
      event("import-1"),
    ]);
    await store.appendToStream({ ...stream, id: "import" }, 1, [
      event("import-2"),
    ]);
    for (const position of [null, older!.position])
      await expect(
        store.importSagas("head-only", [{ id: "x", state: {} }], position),
      ).rejects.toThrow("log head");
    expect(await store.loadSaga("head-only", "x")).toBeNull();
    await store.importSagas(
      "head-only",
      [{ id: "x", state: {} }],
      await store.head(),
    );
    const empty = createPostgresEventStore({ pool, schema: `${schema}_empty` });
    try {
      await empty.migrate();
      await empty.importSagas("first", [{ id: "x", state: {} }], null);
      expect(await empty.loadSaga("first", "x")).toEqual({});
    } finally {
      await pool.query(`DROP SCHEMA "${schema}_empty" CASCADE`);
    }
  });

for (const adapter of adapters)
  it(`${adapter}: reads backwards from an exclusive upper Position, filters only qualified Event names and registers Upcasters`, async () => {
    const store = persistence(adapter);
    const reversed = { context: "rev", aggregate: "order", id: "1" };
    const opened = (id: string) => ({ ...event(id), name: "opened" });
    await store.appendToStream(reversed, 0, [opened("rev-1")]);
    await store.appendToStream(reversed, 1, [opened("rev-2")]);
    const [last] = await store.appendToStream(reversed, 2, [opened("rev-3")]);
    await store.appendToStream({ ...reversed, aggregate: "invoice" }, 0, [
      opened("rev-invoice"),
    ]);
    expect(
      (
        await store.readAll({
          reverse: true,
          fromPosition: last!.position,
          limit: 2,
        })
      ).map((read) => read.id),
    ).toEqual(["rev-2", "rev-1"]);
    expect(
      (await store.readAll({ eventNames: ["rev.invoice.opened"] })).map(
        (read) => read.id,
      ),
    ).toEqual(["rev-invoice"]);
    await expect(store.readAll({ eventNames: ["opened"] })).rejects.toThrow(
      "fully qualified",
    );
    store.registerUpcasters({
      "rev.invoice.opened": {
        version: 2,
        upcasters: { 1: (payload: any) => ({ cents: payload.amount * 100 }) },
      },
    });
    expect(
      await store.readStream({ ...reversed, aggregate: "invoice" }),
    ).toMatchObject([{ schemaVersion: 2, payload: { cents: 100 } }]);
  });

for (const adapter of adapters)
  it(`${adapter}: applies the configured metadata limit to appends and Outbox`, async () => {
    const store = persistence(adapter);
    const small = createPostgresEventStore({
      pool,
      schema,
      metadataMaxBytes: 16,
    });
    const metadata = { a: "x".repeat(20) };
    await expect(
      small.appendToStream({ ...stream, id: "small" }, 0, [
        { ...event("small"), metadata },
      ]),
    ).rejects.toThrow("16 bytes");
    await expect(
      small.recordOutbox([
        {
          ...message("small"),
          message: { ...message("small").message, metadata },
        },
      ]),
    ).rejects.toThrow("16 bytes");
  });

for (const adapter of adapters)
  it(`${adapter}: merges immediate and due Outbox entries in Position order with limits and exclusions`, async () => {
    const store = persistence(adapter);
    const prefix = `mixed-${adapter}-`;
    const immediate = (n: number) => message(`${prefix}${n}`);
    const due = (n: number) => ({
      ...immediate(n),
      notBefore: "2000-01-01T00:00:00.000Z",
    });
    const entries = [
      immediate(0), due(1),
      { ...immediate(2), notBefore: "2100-01-01T00:00:00.000Z" },
      immediate(3), due(4),
    ];
    await store.recordOutbox(entries);
    await store.recordOutbox([due(5), immediate(6)]);
    try {
      expect(
        (await store.pendingOutbox(2, [`${prefix}0`, `${prefix}4`])).map((row) => row.id),
      ).toEqual([`${prefix}1`, `${prefix}3`]);
      expect(
        (await store.pendingOutbox(2, [0, 1, 3, 4].map((n) => `${prefix}${n}`))).map((row) => row.id),
      ).toEqual([`${prefix}5`, `${prefix}6`]);
    } finally {
      for (let n = 0; n < 7; n++) await store.completeOutbox(`${prefix}${n}`);
    }
  });

it.each([
  { future: 20000, ready: 0, scheduled: false },
  { future: 20000, ready: 5000, scheduled: false },
  { future: 0, ready: 5000, scheduled: false },
  { future: 0, ready: 50000, scheduled: true },
])("Postgres polls $ready ready Outbox rows with $future future rows (scheduled=$scheduled)", async ({ future, ready, scheduled }) => {
  const name = `due_outbox_${Date.now()}`;
  const isolated = createPostgresEventStore({ pool, schema: name });
  try {
    await isolated.migrate();
    await pool.query(`INSERT INTO "${name}".outbox(id,transaction_id,ordinal,entry,not_before)
      SELECT 'future-'||n, pg_current_xact_id(), n, jsonb_build_object('padding',repeat('x',500)), '2100-01-01'::timestamptz
      FROM generate_series(1,$1::integer) n`, [future]);
    await pool.query(`DROP INDEX "${name}".outbox_due`);
    await pool.query(`DROP INDEX "${name}".outbox_immediate`);
    expect(await isolated.migrate({ dryRun: true })).toContain(
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS "outbox_due"',
    );
    expect(await isolated.migrate({ dryRun: true })).toContain(
      'CREATE INDEX CONCURRENTLY IF NOT EXISTS "outbox_immediate"',
    );
    await isolated.migrate();
    const entries = Array.from({ length: ready }, (_, n) => ({
      ...message(`ready-${n}`),
      ...(scheduled ? { notBefore: "2000-01-01T00:00:00.000Z" } : {}),
    }));
    await pool.query(`INSERT INTO "${name}".outbox(id,transaction_id,ordinal,entry,not_before)
      SELECT entry->>'id',pg_current_xact_id(),(ordinality-1)::integer,entry,(entry->>'notBefore')::timestamptz
      FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS input(entry,ordinality)`, [JSON.stringify(entries)]);
    await pool.query(`ANALYZE "${name}".outbox`);
    const query = vi.spyOn(pool, "query");
    let sql: string, values: unknown[];
    try {
      expect((await isolated.pendingOutbox()).map((row) => row.id)).toEqual(
        entries.slice(0, 100).map((row) => row.id),
      );
      [sql, values] = query.mock.calls[0] as unknown as [string, unknown[]];
    } finally {
      query.mockRestore();
    }
    const plan = (
      await pool.query(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${sql}`, values)
    ).rows[0]["QUERY PLAN"][0].Plan;
    const filtered = (node: any): number =>
      (node["Rows Removed by Filter"] ?? 0) * node["Actual Loops"] +
      (node.Plans ?? []).reduce(
        (sum: number, child: any) => sum + filtered(child),
        0,
      );
    expect(filtered(plan)).toBeLessThan(100);
    expect(plan["Shared Hit Blocks"] + plan["Shared Read Blocks"]).toBeLessThan(
      500,
    );
    expect((await isolated.outboxStats()).pending).toBe(future + ready);
  } finally {
    await pool.query(`DROP SCHEMA IF EXISTS "${name}" CASCADE`);
  }
});

// Postgres-only: SQL migrations.
it("applies only pending versioned migrations and builds metadata indexes concurrently", async () => {
  const name = `migrations_${Date.now()}`;
  const fresh = createPostgresEventStore({
    pool,
    schema: name,
    metadataIndexes: ["owner.account"],
  });
  try {
    const planned = await fresh.migrate({ dryRun: true });
    expect(planned).toContain("CREATE SCHEMA IF NOT EXISTS");
    expect(planned).toContain("CREATE INDEX CONCURRENTLY IF NOT EXISTS");
    expect(await fresh.migrate()).toBe(planned);
    expect(await fresh.migrate()).toBe("");
    expect(await fresh.migrate({ dryRun: true })).toBe("");
    const indexed = createPostgresEventStore({
      pool,
      schema: name,
      metadataIndexes: ["owner.account", "tenant"],
    });
    const added = await indexed.migrate({ dryRun: true });
    expect(added.trim().split("\n")).toHaveLength(1);
    expect(added).toMatch(/^CREATE INDEX CONCURRENTLY IF NOT EXISTS/);
    expect(await indexed.migrate()).toBe(added);
    expect(
      (
        await pool.query(
          `SELECT count(*)::int AS n FROM pg_indexes WHERE schemaname=$1 AND indexname LIKE 'metadata_%'`,
          [name],
        )
      ).rows[0].n,
    ).toBe(2);
    expect(
      (
        await pool.query(
          `SELECT version FROM "${name}".migrations ORDER BY version`,
        )
      ).rows,
    ).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11].map((version) => ({ version })));
  } finally {
    await pool.query(`DROP SCHEMA IF EXISTS "${name}" CASCADE`);
  }
});

// Postgres-only: SQL migrations.
it("upgrades a schema migrated by the previous single-step migration", async () => {
  const name = `legacy_${Date.now()}`;
  const legacy = createPostgresEventStore({
    pool,
    schema: name,
    metadataIndexes: ["owner.account"],
  });
  try {
    // The previous release ran the whole script, indexes included, in one transaction.
    await pool.query(legacy.migrationSQL().replaceAll(" CONCURRENTLY", ""));
    await legacy.appendToStream(stream, 0, [event("legacy")]);
    expect(await legacy.migrate({ dryRun: true })).toBe("");
    expect(await legacy.migrate()).toBe("");
    expect((await legacy.readAll())[0]?.id).toBe("legacy");
  } finally {
    await pool.query(`DROP SCHEMA IF EXISTS "${name}" CASCADE`);
  }
});
for (const adapter of ["memory", "postgres"])
  it(`${adapter}: a replayed dead letter is delivered afresh whether or not its Outbox row remains`, async () => {
    const persistence =
      adapter === "memory" ? createInMemoryEventStore() : store;
    const outboxEntry = (id: string): OutboxEntry => ({
      id,
      kind: "message",
      queue: "mail",
      attempt: 3,
      message: {
        id,
        name: "mail",
        payload: {},
        metadata: {},
        schemaVersion: 1,
        timestamp: "2026-10-15T00:00:00.000Z",
        correlationId: "cmd",
        attempt: 4,
      },
    });
    const kept = `kept_${crypto.randomUUID()}`,
      gone = `gone_${crypto.randomUUID()}`;
    await persistence.recordOutbox([outboxEntry(kept)]);
    await persistence.recordOutbox([outboxEntry(gone)]);
    await persistence.deadLetterOutbox(kept, "failed");
    await persistence.deadLetterOutbox(gone, "failed");
    const letter = (await persistence.deadLetters()).find(
      (value) => value.id === kept,
    );
    expect(letter).toMatchObject({ kind: "outbox", attempts: 4 });
    expect(letter?.message).toEqual(outboxEntry(kept));
    await persistence.completeOutbox(gone);
    await persistence.replayDeadLetter(kept);
    await persistence.replayDeadLetter(gone);
    const pending = (await persistence.pendingOutbox(1000)).filter((row) =>
      [kept, gone].includes(row.message.id),
    );
    expect(pending).toHaveLength(2);
    for (const row of pending) {
      expect(row.id).not.toBe(row.message.id);
      expect(row).toMatchObject({ attempt: 0, message: { attempt: 1 } });
      expect(row.blocked ?? false).toBe(false);
    }
  });

for (const adapter of ["memory", "postgres"])
  it(`${adapter}: Commands to one Stream leave the Outbox in recorded order, behind retries but not behind later reminders`, async () => {
    const persistence =
      adapter === "memory" ? createInMemoryEventStore() : store;
    const run = crypto.randomUUID();
    const command = (
      id: string,
      stream: string,
      notBefore?: string,
    ): OutboxEntry => ({
      id: `${run}-${id}`,
      kind: "command",
      ...(notBefore && { notBefore }),
      message: {
        id: `${run}-${id}`,
        stream: { context: "c", aggregate: "a", id: `${run}-${stream}` },
        name: "do",
        payload: {},
        metadata: {},
        schemaVersion: 1,
        timestamp: "2026-10-16T00:00:00.000Z",
        correlationId: run,
      } as any,
    });
    const pending = async () =>
      (await persistence.pendingOutbox(1000))
        .map((row) => row.id.slice(run.length + 1))
        .filter((id) => id.length === 1)
        .sort();
    // Each its own transaction, in this order.
    await persistence.recordOutbox([
      command("r", "x", "2099-01-01T00:00:00.000Z"),
    ]);
    await persistence.recordOutbox([command("a", "x")]);
    await persistence.recordOutbox([command("b", "x")]);
    await persistence.recordOutbox([command("c", "y")]);
    expect(await pending()).toEqual(["a", "c"]);
    const lease = (await persistence.acquireLease(
      `outbox-${run}`,
      "me",
      60000,
    ))!;
    expect(await persistence.claimOutbox(`${run}-b`, lease)).toBe(false);
    expect(await persistence.claimOutbox(`${run}-a`, lease)).toBe(true);
    await persistence.retryOutbox(`${run}-a`, 1, "2099-01-01T00:00:00.000Z");
    // A retrying Command still holds back the next one to its Stream.
    expect(await pending()).toEqual(["c"]);
    await persistence.completeOutbox(`${run}-a`);
    expect(await pending()).toEqual(["b", "c"]);
    await persistence.releaseLease(lease);
  });

for (const adapter of ["memory", "postgres"])
  it(`${adapter}: Commands of one transaction to different Streams leave together; one Stream's still wait in order`, async () => {
    const persistence =
      adapter === "memory" ? createInMemoryEventStore() : store;
    const run = crypto.randomUUID();
    const command = (
      id: string,
      stream: string,
      notBefore?: string,
    ): OutboxEntry => ({
      id: `${run}-${id}`,
      kind: "command",
      ...(notBefore && { notBefore }),
      message: {
        id: `${run}-${id}`,
        stream: { context: "c", aggregate: "a", id: `${run}-${stream}` },
        name: "do",
        payload: {},
        metadata: {},
        schemaVersion: 1,
        timestamp: "2026-10-16T00:00:00.000Z",
        correlationId: run,
      } as any,
    });
    const pending = async () =>
      (await persistence.pendingOutbox(1000))
        .filter((row) => row.id.startsWith(run))
        .map((row) => row.id.slice(run.length + 1))
        .sort();
    // One transaction, as a Saga batch records it: a reminder, then x, y, x.
    await persistence.recordOutbox([
      command("r", "z", "2099-01-01T00:00:00.000Z"),
      command("a", "x"),
      command("b", "y"),
      command("c", "x"),
    ]);
    expect(await pending()).toEqual(["a", "b"]);
    const lease = (await persistence.acquireLease(
      `outbox-${run}`,
      "me",
      60000,
    ))!;
    expect(await persistence.claimOutbox(`${run}-b`, lease)).toBe(true);
    expect(await persistence.claimOutbox(`${run}-c`, lease)).toBe(false);
    // A dead-lettered Command no longer holds back its Stream or its transaction.
    await persistence.deadLetterOutbox(`${run}-a`, "failed");
    expect(await pending()).toEqual(["b", "c"]);
    expect(await persistence.claimOutbox(`${run}-c`, lease)).toBe(true);
    for (const id of ["b", "c"])
      await persistence.completeOutbox(`${run}-${id}`);
    await persistence.discardDeadLetter(`${run}-a`);
    await persistence.releaseLease(lease);
  });

it("postgres: an open lower-xid Outbox writer holds back admission until it commits or rolls back", async () => {
  const run = crypto.randomUUID();
  const command = (id: string): OutboxEntry => ({
    id: `${run}-${id}`,
    kind: "command",
    message: {
      id: `${run}-${id}`,
      stream: { context: "c", aggregate: "a", id: `${run}-x` },
      name: "do",
      payload: {},
      metadata: {},
      schemaVersion: 1,
      timestamp: "2026-10-16T00:00:00.000Z",
      correlationId: run,
    } as any,
  });
  const pending = async () =>
    (await store.pendingOutbox(1000))
      .filter((row) => row.id.startsWith(`${run}-`))
      .map((row) => row.id.slice(run.length + 1));
  const lease = (await store.acquireLease(`outbox-${run}`, "me", 60000))!;
  /** Records `id` in a transaction left open, with a lower xid than anything recorded after. */
  const open = async (id: string) => {
    const client = await pool.connect();
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO "${schema}".outbox(id,transaction_id,ordinal,entry) VALUES($1,pg_current_xact_id(),0,$2)`,
      [`${run}-${id}`, command(id)],
    );
    return client;
  };
  const older = await open("a");
  try {
    await store.recordOutbox([command("b")]);
    // b is committed, but a, with a lower xid, may still commit before it.
    expect(await pending()).toEqual([]);
    expect(await store.claimOutbox(`${run}-b`, lease)).toBe(false);
    await older.query("COMMIT");
  } finally {
    older.release();
  }
  expect(await pending()).toEqual(["a"]);
  expect(await store.claimOutbox(`${run}-b`, lease)).toBe(false);
  expect(await store.claimOutbox(`${run}-a`, lease)).toBe(true);
  await store.completeOutbox(`${run}-a`);
  expect(await pending()).toEqual(["b"]);
  expect(await store.claimOutbox(`${run}-b`, lease)).toBe(true);
  await store.completeOutbox(`${run}-b`);
  // A rolled-back predecessor releases its successor.
  const rolledBack = await open("c");
  try {
    await store.recordOutbox([command("d")]);
    expect(await store.claimOutbox(`${run}-d`, lease)).toBe(false);
    await rolledBack.query("ROLLBACK");
  } finally {
    rolledBack.release();
  }
  expect(await pending()).toEqual(["d"]);
  expect(await store.claimOutbox(`${run}-d`, lease)).toBe(true);
  await store.releaseLease(lease);
});

it("the Outbox is vacuumed after a fixed number of dead rows, whatever its size", async () => {
  const name = `outbox_vacuum_${Date.now()}`;
  const isolated = createPostgresEventStore({ pool, schema: name });
  try {
    await isolated.migrate();
    const { rows } = await pool.query(
      `SELECT reloptions FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname='outbox'`,
      [name],
    );
    expect(rows[0].reloptions).toEqual(
      expect.arrayContaining([
        "autovacuum_vacuum_scale_factor=0",
        "autovacuum_vacuum_threshold=1000",
      ]),
    );
  } finally {
    await pool.query(`DROP SCHEMA IF EXISTS "${name}" CASCADE`);
  }
});

for (const adapter of adapters)
  it(`${adapter}: retiring an undeclared Saga group deletes its rows and state, never a declared or leased one`, async () => {
    const store = persistence(adapter);
    const id = crypto.randomUUID(),
      gone = `saga:gone-${id}`,
      kept = `saga:kept-${id}`;
    // A previous deploy ran `gone` with two Partitions; its Checkpoint, layout and state remain.
    const old = (await store.acquireLease(gone, "old"))!;
    const [saved] = await store.appendToStream({ ...stream, id }, 0, [
      event(id),
    ]);
    await store.commitSaga(old, saved!.position, [{ id, state: { open: true } }], []);
    await store.releaseLease(old);
    await store.releaseLease((await store.acquireLease(gone, "old", 30000, 1))!);
    await store.setConsumerStatus({
      group: `layout:${gone}`,
      partition: 0,
      checkpoint: null,
      state: "running",
      partitions: 2,
    });
    await store.releaseLease((await store.acquireLease(kept, "new"))!);
    await store.releaseLease((await store.acquireLease(`rebuild:${id}`, "new"))!);
    const register = async (owner: string, groups: string[]) => {
      const lease = (await store.acquireLease(`instance:${owner}`, owner))!;
      await store.setConsumerStatus(
        { group: lease.group, partition: 0, checkpoint: null, state: "running", groups },
        lease,
      );
      return lease;
    };
    const quiet = { debug() {}, info() {}, warn() {}, error() {}, child: () => quiet };
    const cli = createConsumerGroups({
      store,
      owner: "cli",
      leaseDuration: 30000,
      logger: quiet,
      pause: async () => {},
      running: () => false,
    });
    const states = async () =>
      Object.fromEntries(
        (await cli.consumers())
          .filter((s) => s.group.includes(id) && !s.group.startsWith("instance:"))
          .map((s) => [`${s.group}#${s.partition}`, s.state]),
      );
    const stale = await register(`old-${id}`, [kept, gone, `${gone}#1`]);
    const live = await register(`new-${id}`, [kept]);
    expect(await states()).toMatchObject({
      [`${gone}#0`]: "running",
      [`${gone}#1`]: "running",
    });
    await expect(cli.retire(gone)).rejects.toThrow(`still declared by old-${id}`);
    await expect(cli.retire(`readmodels:${id}`)).rejects.toThrow("Only a Saga or Workflow group");
    await expect(cli.retire(`saga:missing-${id}`)).rejects.toThrow("Unknown");
    await store.releaseLease(stale);
    expect(await states()).toEqual({
      [`${gone}#0`]: "undeclared",
      [`${gone}#1`]: "undeclared",
      [`layout:${gone}#0`]: "running",
      [`rebuild:${id}#0`]: "running",
      [`${kept}#0`]: "running",
    });
    // A Lease still held on the group refuses in the store itself.
    const stray = (await store.acquireLease(gone, "stray"))!;
    await expect(cli.retire(gone)).rejects.toThrow("live Lease");
    expect(await store.loadSaga(gone, id)).toEqual({ open: true });
    await store.releaseLease(stray);
    await cli.retire(gone);
    expect(await states()).toEqual({
      [`rebuild:${id}#0`]: "running",
      [`${kept}#0`]: "running",
    });
    expect(await store.loadSaga(gone, id)).toBeNull();
    await store.releaseLease(live);
  });
