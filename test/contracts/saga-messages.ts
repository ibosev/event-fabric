import { expect, test } from "vitest";
import { randomUUID } from "node:crypto";
import { StaleSagaMessageError } from "../../src/errors.js";
import { fnv1a } from "../../src/transport/partition.js";
import type { Persistence, SagaMessage } from "../../src/types.js";

/** The Saga Message storage every Event store adapter that keeps them must provide (ADR-0020). */
export function sagaMessageContract(adapter: string, persistence: () => Persistence) {
  const fixture = async () => {
    const store = persistence(),
      group = `saga:${randomUUID()}`;
    const lease = (await store.acquireLease(group, "owner", 30000))!;
    const message = (instance: string, extra: Partial<SagaMessage> = {}): SagaMessage => ({
      id: randomUUID(),
      group,
      instance,
      name: "remind",
      payload: { n: 1 },
      metadata: {},
      correlationId: "c",
      timestamp: new Date().toISOString(),
      dueAt: new Date(Date.now() - 1000).toISOString(),
      ...extra,
    });
    const due = async (slice?: { partition: number; count: number }) =>
      (await store.dueSagaMessages!(group, slice, 100)).messages;
    return { store, group, lease, message, due };
  };

  test(`${adapter}: due Saga Messages come oldest first, with the delay to the next one`, async () => {
    const { store, group, message, due } = await fixture();
    const later = message("a", { dueAt: new Date(Date.now() + 60000).toISOString() }),
      second = message("b", { dueAt: new Date(Date.now() - 500).toISOString() }),
      first = message("a", { dueAt: new Date(Date.now() - 2000).toISOString() });
    await store.recordSagaMessages!([later, second, first]);
    expect((await due()).map((m) => m.id)).toEqual([first.id, second.id]);
    const { nextDelay } = await store.dueSagaMessages!(group, undefined, 100);
    expect(nextDelay).toBeGreaterThan(55000);
    expect(nextDelay).toBeLessThanOrEqual(60000);
    expect(await store.dueSagaMessages!(`saga:${randomUUID()}`, undefined, 100)).toEqual({
      messages: [],
      nextDelay: null,
    });
  });

  test(`${adapter}: Saga Messages due at the same instant come in the order they were sent`, async () => {
    const { store, message, due } = await fixture();
    const dueAt = new Date(Date.now() - 1000).toISOString();
    const sent = ["z", "a", "m"].map((prefix) => message("a", { id: `${prefix}-${randomUUID()}`, dueAt }));
    for (const one of sent) await store.recordSagaMessages!([one]);
    expect((await due()).map((m) => m.id)).toEqual(sent.map((m) => m.id));
  });

  test(`${adapter}: a Partition sees only the instances it owns`, async () => {
    const { store, message, due } = await fixture();
    const instances = ["a", "b", "c", "d", "e", "f"];
    await store.recordSagaMessages!(instances.map((instance) => message(instance)));
    for (const partition of [0, 1, 2])
      expect((await due({ partition, count: 3 })).map((m) => m.instance).sort()).toEqual(
        instances.filter((instance) => fnv1a(instance) % 3 === partition).sort(),
      );
  });

  test(`${adapter}: recording keeps an id or a Deadline already pending`, async () => {
    const { store, message, due } = await fixture();
    const first = message("a", { deadline: "remind" });
    await store.recordSagaMessages!([first]);
    await store.recordSagaMessages!([
      { ...first, payload: { n: 2 } },
      message("a", { deadline: "remind", payload: { n: 3 } }),
    ]);
    expect(await due()).toMatchObject([{ id: first.id, payload: { n: 1 } }]);
  });

  test(`${adapter}: a commit consumes its Saga Message, replaces and cancels Deadlines, keeps the Checkpoint`, async () => {
    const { store, group, lease, message, due } = await fixture();
    const handled = message("a"),
      deadline = message("a", { deadline: "remind" }),
      other = message("a", { deadline: "other" }),
      destroyed = message("b", { deadline: "remind" });
    await store.recordSagaMessages!([handled, deadline, other, destroyed]);
    const replacement = message("a", { deadline: "remind", payload: { n: 2 } });
    await store.commitSaga(lease, null, [{ id: "a", state: { done: true } }], [], {
      consumed: handled.id,
      changes: [
        { send: replacement },
        { cancel: { group, instance: "a", deadline: "other" } },
        { cancel: { group, instance: "b" } },
      ],
    });
    expect(await due()).toMatchObject([{ id: replacement.id, payload: { n: 2 } }]);
    expect(await store.loadSaga(group, "a")).toEqual({ done: true });
    // A Saga Message sent again under a pending id keeps the pending one.
    const sent = message("c");
    await store.commitSaga(lease, null, [], [], { changes: [{ send: sent }] });
    await store.commitSaga(lease, null, [], [], { changes: [{ send: { ...sent, payload: { n: 9 } } }] });
    expect((await due()).filter((m) => m.id === sent.id)).toMatchObject([{ payload: { n: 1 } }]);
    await store.commitSaga(lease, null, [], [], { consumed: sent.id });
    expect(await store.checkpoint(group)).toBeNull();
    // Handled, replaced or cancelled: committing it again changes nothing.
    for (const stale of [handled, deadline, other])
      await expect(
        store.commitSaga(lease, null, [{ id: "a", state: { done: false } }], [], {
          consumed: stale.id,
        }),
      ).rejects.toBeInstanceOf(StaleSagaMessageError);
    expect(await store.loadSaga(group, "a")).toEqual({ done: true });
  });

  test(`${adapter}: a stale Lease cannot consume a Saga Message`, async () => {
    const { store, lease, message, due } = await fixture();
    const pending = message("a");
    await store.recordSagaMessages!([pending]);
    await store.releaseLease(lease);
    await expect(
      store.commitSaga(lease, null, [], [], { consumed: pending.id }),
    ).rejects.toThrow();
    expect(await due()).toHaveLength(1);
  });

  test(`${adapter}: a failed Saga Message retries later, then dead-letters; replay and discard`, async () => {
    const { store, lease, message, due } = await fixture();
    const failing = message("a", { deadline: "remind" });
    await store.recordSagaMessages!([failing]);
    await store.retrySagaMessage!(lease, failing.id, 1, new Date(Date.now() + 60000).toISOString());
    expect(await due()).toEqual([]);
    await store.retrySagaMessage!(lease, failing.id, 4, new Date(Date.now() - 1).toISOString());
    expect(await due()).toMatchObject([{ id: failing.id, attempt: 4 }]);
    await store.deadLetterSagaMessage!(lease, failing.id, "boom");
    expect(await due()).toEqual([]);
    expect(await store.deadLetters()).toContainEqual(
      expect.objectContaining({ id: failing.id, kind: "saga-message", attempts: 5, error: "boom" }),
    );
    await store.replayDeadLetter(failing.id);
    expect(await due()).toMatchObject([{ id: failing.id, attempt: 0 }]);
    await store.deadLetterSagaMessage!(lease, failing.id, "again");
    await store.discardDeadLetter(failing.id);
    expect(await store.deadLetters()).not.toContainEqual(expect.objectContaining({ id: failing.id }));
    // Discarded: the Deadline name is free again.
    const next = message("a", { deadline: "remind" });
    await store.recordSagaMessages!([next]);
    expect(await due()).toMatchObject([{ id: next.id }]);
  });

  test(`${adapter}: replaying a Deadline replaced since it was dead-lettered is a no-op`, async () => {
    const { store, lease, message, due } = await fixture();
    const failing = message("a", { deadline: "remind" });
    await store.recordSagaMessages!([failing]);
    await store.deadLetterSagaMessage!(lease, failing.id, "boom");
    const replacement = message("a", {
      deadline: "remind",
      dueAt: new Date(Date.now() + 60000).toISOString(),
    });
    await store.commitSaga(lease, null, [], [], { changes: [{ send: replacement }] });
    await store.replayDeadLetter(failing.id);
    expect(await due()).toEqual([]);
    expect(await store.deadLetters()).not.toContainEqual(expect.objectContaining({ id: failing.id }));
  });

  test(`${adapter}: retiring a Saga drops its Saga Messages`, async () => {
    const { store, lease, message, due, group } = await fixture();
    await store.recordSagaMessages!([message("a")]);
    await store.releaseLease(lease);
    await store.retireConsumer(group);
    expect(await due()).toEqual([]);
  });
}
