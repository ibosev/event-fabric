import { expect, test } from "vitest";
import { randomUUID } from "node:crypto";
import { compile } from "../../src/authoring/index.js";
import { createKernel } from "../../src/kernel/index.js";
import { createCommand } from "../../src/id.js";
import type { Command, Persistence, StreamId } from "../../src/types.js";
import type { EventStoreOptions } from "../../src/event-store/upcasters.js";

export function scheduleContract(
  adapter: string,
  persistence: (options?: EventStoreOptions) => Persistence,
) {
  const fixture = (
    scheduleName = "overdue",
    owner?: StreamId,
    store = persistence(),
  ) => {
    const stream = owner ?? {
      context: "schedules",
      aggregate: "invoice",
      id: randomUUID(),
    };
    let now = new Date("2030-01-01T00:00:00Z");
    let duringCheck: (() => Promise<void>) | undefined;
    const definition = compile(stream.context, stream.aggregate, {
      commands: {
        schedule: [
          (command, _invoice, app) =>
            app.schedule(scheduleName, command.payload.dueAt, {
              stream,
              name: "check",
              payload: { amount: 10 },
            }),
        ],
        cancel: [
          (_command, invoice, app) => {
            invoice.apply.settled({});
            app.cancelSchedule(scheduleName);
          },
        ],
        check: [
          async (_command, invoice) => {
            await duringCheck?.();
            invoice.apply.checked({});
          },
        ],
        invalid: [
          (_command, invoice, app) => {
            invoice.apply.settled({});
            app.cancelSchedule("overdue");
            throw new Error("Refused");
          },
        ],
        otherTarget: [
          (_command, invoice, app) => {
            invoice.apply.settled({});
            app.schedule(scheduleName, "2099-01-01T00:00:00Z", {
              stream: { ...stream, id: "another" },
              name: "check",
            });
          },
        ],
      },
      events: { checked() {}, settled() {} },
    });
    const kernel = createKernel({
      store,
      definitions: [definition],
      clock: () => now,
    });
    const send = (name: string, payload = {}) =>
      kernel.handle(createCommand({ stream, name, payload }));
    const record = async (dueAt = "2020-01-01T00:00:00.000Z") => {
      expect(await send("schedule", { dueAt })).toMatchObject({
        status: "accepted",
      });
      return (await store.getSchedule(stream, scheduleName))!;
    };
    const entry = async (commandId: string) =>
      (await store.pendingOutbox(10000)).find(
        (row) => row.message.id === commandId,
      )!;
    return {
      store,
      stream,
      definition,
      kernel,
      send,
      record,
      entry,
      setClock: (date: string) => {
        now = new Date(date);
      },
      during: (fn: () => Promise<void>) => {
        duringCheck = fn;
      },
    };
  };

  test(`${adapter}: cancellation reports its committed result, and a duplicate cannot cancel a replacement`, async () => {
    const f = fixture();
    await f.record();
    const command = createCommand({ stream: f.stream, name: "cancel" });
    expect(await f.kernel.handle(command)).toMatchObject({
      status: "accepted",
      cancellations: { overdue: "cancelled" },
    });
    expect(await f.store.getSchedule(f.stream, "overdue")).toBeNull();
    const replacement = await f.record();
    expect(await f.kernel.handle(command)).toMatchObject({
      status: "accepted",
      duplicate: true,
      cancellations: { overdue: "cancelled" },
    });
    expect(await f.store.getSchedule(f.stream, "overdue")).toEqual(replacement);
    const row = await f.entry(replacement.commandId);
    const lease = (await f.store.acquireLease(
      `claim-${f.stream.id}`,
      "test",
      30000,
    ))!;
    expect(await f.store.claimOutbox(row.id, lease)).toBe(true);
    expect(await f.send("cancel")).toMatchObject({
      status: "accepted",
      cancellations: { overdue: "dispatched" },
    });
    expect(await f.kernel.handle(row.message as Command)).toMatchObject({
      status: "accepted",
      events: [],
    });
    expect(await f.send("cancel")).toMatchObject({
      status: "accepted",
      cancellations: { overdue: "inactive" },
    });
  });

  test(`${adapter}: a Schedule replacement fences a running attempt even without a new Stream Revision`, async () => {
    const f = fixture();
    const original = await f.record();
    const row = await f.entry(original.commandId);
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    f.during(async () => {
      entered.resolve();
      await release.promise;
    });
    const running = f.kernel.handle(row.message as Command);
    await entered.promise;
    const replacement = await f.record("2021-01-01T00:00:00.000Z");
    release.resolve();
    expect(await running).toMatchObject({ status: "accepted", events: [] });
    expect(await f.store.readStream(f.stream)).toEqual([]);
    expect(await f.store.getSchedule(f.stream, "overdue")).toEqual(replacement);
  });

  test(`${adapter}: a refused cancellation leaves both the Invoice and Schedule intact`, async () => {
    const f = fixture();
    const original = await f.record();
    expect(await f.send("invalid")).toMatchObject({ status: "rejected" });
    expect(await f.store.readStream(f.stream)).toEqual([]);
    expect(await f.store.getSchedule(f.stream, "overdue")).toEqual(original);
    expect(await f.entry(original.commandId)).toBeDefined();
  });

  test(`${adapter}: replaying a superseded dead letter cannot remove or revive a Schedule`, async () => {
    const f = fixture();
    const original = await f.record();
    const row = await f.entry(original.commandId);
    await f.store.deadLetterOutbox(row.id, "technical failure");
    const replacement = await f.record("2099-01-01T00:00:00.000Z");
    await f.store.replayDeadLetter(row.id);
    const replay = await f.entry(original.commandId);
    expect(await f.store.getSchedule(f.stream, "overdue")).toEqual(replacement);
    expect(await f.kernel.handle(replay.message as Command)).toMatchObject({
      status: "accepted",
      events: [],
    });
    await f.store.completeOutbox(replay.id);
    expect(await f.store.getSchedule(f.stream, "overdue")).toEqual(replacement);
  });

  test(`${adapter}: cancellation preserves reserved-looking Schedule names`, async () => {
    const f = fixture("__proto__");
    await f.record();
    const command = createCommand({ stream: f.stream, name: "cancel" });
    const outcome = await f.kernel.handle(command);
    expect(outcome.status).toBe("accepted");
    if (outcome.status !== "accepted") throw Error("Expected acceptance");
    expect(Object.entries(outcome.cancellations!)).toEqual([
      ["__proto__", "cancelled"],
    ]);
    expect(await f.kernel.handle(command)).toMatchObject({
      ...outcome,
      duplicate: true,
    });
  });

  test(`${adapter}: obsolete attempts survive changed business schemas, including a validation race`, async () => {
    const f = fixture();
    const original = await f.record();
    const row = await f.entry(original.commandId);
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const kernel = createKernel({
      store: f.store,
      definitions: [f.definition],
      metadataSchema: {},
      validate: async () => {
        entered.resolve();
        await release.promise;
        throw Error("New required metadata");
      },
    });
    const running = kernel.handle(row.message as Command);
    await entered.promise;
    await f.record();
    release.resolve();
    expect(await running).toMatchObject({ status: "accepted", events: [] });
    expect(await kernel.handle(row.message as Command)).toMatchObject({
      status: "accepted",
      duplicate: true,
      events: [],
    });
  });

  test(`${adapter}: discarding a dead letter clears only its own current Schedule`, async () => {
    const f = fixture();
    const original = await f.record();
    const row = await f.entry(original.commandId);
    await f.store.deadLetterOutbox(row.id, "failed");
    await f.store.discardDeadLetter(row.id);
    expect(await f.store.getSchedule(f.stream, "overdue")).toBeNull();
    const another = await f.record();
    const old = await f.entry(another.commandId);
    await f.store.deadLetterOutbox(old.id, "failed");
    const replacement = await f.record();
    await f.store.discardDeadLetter(old.id);
    expect(await f.store.getSchedule(f.stream, "overdue")).toEqual(replacement);
  });

  test(`${adapter}: an obsolete dead letter remains replayable after the store metadata schema changes`, async () => {
    let changed = false;
    const store = persistence({
      metadataSchema: {},
      validate: async () => {
        if (changed) throw Error("New required metadata");
      },
    });
    const f = fixture("overdue", undefined, store);
    const original = await f.record();
    const row = await f.entry(original.commandId);
    await store.deadLetterOutbox(row.id, "failed");
    await f.send("cancel");
    changed = true;
    await store.replayDeadLetter(row.id);
    const replay = await f.entry(original.commandId);
    expect(await f.kernel.handle(replay.message as Command)).toMatchObject({
      status: "accepted",
      events: [],
    });
    expect(await store.getSchedule(f.stream, "overdue")).toBeNull();
  });

  test(`${adapter}: mismatched Schedule and Outbox envelopes are rejected before commit`, async () => {
    const f = fixture();
    const original = await f.record();
    const row = await f.entry(original.commandId);
    const entry = {
      ...row,
      id: randomUUID(),
      message: { ...row.message, id: randomUUID() },
    };
    await expect(
      f.store.appendToStream(f.stream, 0, [], undefined, {
        outbox: [entry],
        schedules: [
          {
            name: "overdue",
            entry: {
              ...entry,
              message: { ...entry.message, payload: { altered: true } },
            },
          },
        ],
      }),
    ).rejects.toThrow("Invalid Schedule change");
    expect(await f.store.getSchedule(f.stream, "overdue")).toEqual(original);
    expect(await f.entry(original.commandId)).toBeDefined();
  });

  test(`${adapter}: direct early delivery waits, and accepted delivery consumes the Schedule only once`, async () => {
    const f = fixture();
    const original = await f.record("2025-01-01T00:00:00.000Z");
    const row = await f.entry(original.commandId);
    f.setClock("2024-12-31T23:59:59.999Z");
    expect(await f.kernel.handle(row.message as Command)).toMatchObject({
      status: "unknown",
    });
    expect(await f.store.readStream(f.stream)).toEqual([]);
    f.setClock("2025-01-01T00:00:00.000Z");
    expect(await f.kernel.handle(row.message as Command)).toMatchObject({
      status: "accepted",
      events: [{ name: "checked" }],
    });
    expect(await f.store.getSchedule(f.stream, "overdue")).toBeNull();
    expect(await f.kernel.handle(row.message as Command)).toMatchObject({
      status: "accepted",
      duplicate: true,
      events: [{ name: "checked" }],
    });
  });

  test(`${adapter}: changing a scheduled payload cannot impersonate the recorded attempt`, async () => {
    const f = fixture();
    const original = await f.record();
    const row = await f.entry(original.commandId);
    expect(
      await f.kernel.handle({
        ...(row.message as Command),
        payload: { amount: 100 },
      }),
    ).toMatchObject({ status: "rejected", error: { code: "conflict" } });
    expect(await f.store.readStream(f.stream)).toEqual([]);
    expect(await f.store.getSchedule(f.stream, "overdue")).toEqual(original);
  });

  test(`${adapter}: a failed Event append rolls back the Schedule cancellation`, async () => {
    const f = fixture();
    const original = await f.record();
    const event = {
      id: randomUUID(),
      name: "settled",
      payload: {},
      metadata: {},
      schemaVersion: 1,
      timestamp: new Date().toISOString(),
      commandId: randomUUID(),
      correlationId: randomUUID(),
    };
    await expect(
      f.store.appendToStream(f.stream, 0, [event, event], undefined, {
        schedules: [{ name: "overdue" }],
      }),
    ).rejects.toThrow();
    expect(await f.store.readStream(f.stream)).toEqual([]);
    expect(await f.store.getSchedule(f.stream, "overdue")).toEqual(original);
  });

  test(`${adapter}: claim and cancellation serialize at the dispatch boundary`, async () => {
    const f = fixture();
    const lease = (await f.store.acquireLease(
      `race-${f.stream.id}`,
      "dispatcher",
      30000,
    ))!;
    for (let i = 0; i < 6; i++) {
      const original = await f.record();
      const row = await f.entry(original.commandId);
      // The relay passes the row it read; both forms must take the Stream lock.
      const claim = () => f.store.claimOutbox(row.id, lease, i < 3 ? row : undefined);
      const cancel = () => f.send("cancel");
      const [claimed, outcome] =
        i % 2 === 0
          ? await Promise.all([claim(), cancel()])
          : await Promise.all([cancel(), claim()]).then(
              ([outcome, claimed]) => [claimed, outcome] as const,
            );
      expect(outcome).toMatchObject({
        status: "accepted",
        cancellations: { overdue: claimed ? "dispatched" : "cancelled" },
      });
      expect(await f.kernel.handle(row.message as Command)).toMatchObject({
        status: "accepted",
        events: [],
      });
    }
  });

  test(`${adapter}: a replacement dispatcher fences the old Lease and retains the original due time across retry`, async () => {
    const f = fixture();
    const original = await f.record();
    const row = await f.entry(original.commandId);
    const group = `handover-${f.stream.id}`;
    const old = (await f.store.acquireLease(group, "old", 30000))!;
    await f.store.releaseLease(old);
    const current = (await f.store.acquireLease(group, "new", 30000))!;
    await expect(f.store.claimOutbox(row.id, old)).rejects.toThrow();
    expect(await f.store.claimOutbox(row.id, current)).toBe(true);
    await f.store.retryOutbox(row.id, 1, "2099-01-01T00:00:00.000Z");
    expect(await f.entry(original.commandId)).toBeUndefined();
    expect(await f.store.getSchedule(f.stream, "overdue")).toMatchObject({
      dueAt: original.dueAt,
    });
    await f.store.deadLetterOutbox(row.id, "failed");
    await f.store.replayDeadLetter(row.id);
    const replay = await f.entry(original.commandId);
    expect(replay.notBefore).toBe(original.dueAt);
    expect(await f.kernel.handle(replay.message as Command)).toMatchObject({
      status: "accepted",
      events: [{ name: "checked" }],
    });
    await f.store.releaseLease(current);
  });

  test(`${adapter}: full Stream identity isolates Schedule names and forbids another target`, async () => {
    const id = randomUUID();
    const store = persistence();
    const owners = [
      { context: "sales", aggregate: "invoice", id },
      { context: "purchases", aggregate: "invoice", id },
      { context: "sales", aggregate: "order", id },
    ].map((stream) => fixture("overdue", stream, store));
    const originals = await Promise.all(owners.map((f) => f.record()));
    await owners[0]!.send("cancel");
    for (const index of [1, 2])
      expect(
        await owners[index]!.store.getSchedule(
          owners[index]!.stream,
          "overdue",
        ),
      ).toEqual(originals[index]);
    expect(await owners[1]!.send("otherTarget")).toMatchObject({
      status: "rejected",
    });
    expect(await owners[1]!.store.readStream(owners[1]!.stream)).toEqual([]);
    expect(
      await owners[1]!.store.getSchedule(owners[1]!.stream, "overdue"),
    ).toEqual(originals[1]);
  });
}
