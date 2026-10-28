import { expect, test } from "vitest";
import { event, processFixture, reply, sagaMessage, signal, start, timePasses, workflowStep } from "../src/testing.js";
import type { SagaDefinition, WorkflowDefinition } from "../src/runtime/types.js";
import { encodePosition } from "../src/position.js";

const remind = (id: string, round: number) => ({
  stream: { context: "crm", aggregate: "file", id },
  name: "remind",
  payload: { round },
});

/** A reminder: set on opening, reset by `postpone`, cancelled by `determine`. */
const connector: SagaDefinition = {
  reactions: {
    "crm.file.opened": (event, saga: any) => {
      saga.set({ opened: event.payload.by });
      saga.deadline("remind", event.payload.remindAt, { round: 1 });
    },
    "crm.file.postponed": (event, saga: any) =>
      saga.deadline("remind", event.payload.remindAt, { round: 2 }),
    "crm.file.determined": (_event, saga: any) => saga.cancelDeadline("remind"),
    "crm.file.closed": (_event, saga: any) => saga.destroy(),
  },
  deadlines: {
    remind: (message, saga, app) => app.tell(remind(saga.id, message.payload.round)),
  },
  messages: {
    escalate: (message, saga: any, app) => {
      saga.set("escalated", message.payload.to);
      app.queue("notify", { file: saga.id, to: message.payload.to }, { suffix: "escalation" });
    },
  },
};
const at = (time: string) => `2026-10-01T${time}:00.000Z`;

test("a Deadline falls due when time passes, tells its Command, and only the step under test counts", async () => {
  await using saga = processFixture(connector, { now: at("08:00") });
  await saga
    .given(event("crm.file.opened", "f1", { by: "ann", remindAt: at("09:00") }))
    .expectState({ opened: "ann" })
    .expectDeadline("remind", at("09:00"))
    .when(timePasses(59 * 60_000))
    .expectCommands([])
    .expectDeadline("remind", at("09:00"))
    .when(timePasses(60_000))
    .expectCommands([remind("f1", 1)])
    .expectNoDeadline("remind");
  expect(saga.now().toISOString()).toBe(at("09:00"));
});

test("given Events reach the process under test whatever its from says", async () => {
  // A Position far past the fixture's log would skip every Event it records.
  const later = encodePosition({ transactionId: "1000000", sequence: "1" });
  await using saga = processFixture({ ...connector, from: { position: later } }, { now: at("08:00") });
  await saga
    .given(event("crm.file.opened", "f1", { by: "ann", remindAt: at("09:00") }))
    .expectState({ opened: "ann" });
});

test("Reactions replace and cancel Deadlines; destroying the instance clears its state", async () => {
  await using saga = processFixture(connector, { now: at("08:00") });
  await saga
    .given(event("crm.file.opened", "f1", { by: "ann", remindAt: at("09:00") }))
    .when(event("crm.file.postponed", "f1", { remindAt: at("12:00") }))
    .expectDeadline("remind", at("12:00"))
    .when(event("crm.file.determined", "f1", {}))
    .expectNoDeadline("remind")
    .when(timePasses(24 * 3600_000))
    .expectCommands([])
    .when(event("crm.file.closed", "f1", {}))
    .expectState(null);
});

test("a Saga Message reaches its instance and the Messages it queues are expected", async () => {
  await using saga = processFixture(connector, {
    workers: { notifier: { handles: ["notify"], execute() {} } },
  });
  await saga
    .given(event("crm.file.opened", "f1", { by: "ann", remindAt: at("09:00") }))
    .when(sagaMessage("escalate", { to: "lead" }, { to: "f1" }))
    .expectQueued([{ name: "notify", payload: { file: "f1", to: "lead" } }])
    .expectCommands([])
    .expectState({ opened: "ann", escalated: "lead" });
});

test("a queued request is answered by stepping its Worker's reply; the Worker itself never runs", async () => {
  const executed: unknown[] = [];
  await using saga = processFixture(
    {
      ...connector,
      messages: {
        escalate: (_message, _saga, app) => app.queue("notify", { file: "f1" }, { replyTo: "notified" }),
        notified: (message, saga: any) => saga.set("notified", message.payload),
      },
    },
    { workers: { notifier: { handles: ["notify"], execute: (message) => void executed.push(message) } } },
  );
  await saga
    .given(event("crm.file.opened", "f1", { by: "ann", remindAt: at("09:00") }))
    .when(sagaMessage("escalate", {}, { to: "f1" }))
    .expectQueued([{ name: "notify", payload: { file: "f1" }, replyTo: { saga: "process", instance: "f1", name: "notified" } }])
    .when(reply("notify", { outcome: "ok", value: { channel: "mail" } }))
    .expectState({ opened: "ann", notified: { outcome: "ok", value: { channel: "mail" } } })
    .given(sagaMessage("escalate", {}, { to: "f1" }))
    .when(reply("notify", { outcome: "failed", error: "bounced", attempts: 5 }))
    .expectState({ opened: "ann", notified: { outcome: "failed", error: "bounced", attempts: 5 } });
  await expect(saga.when(reply("notify", { outcome: "ok", value: null }))).rejects.toThrow(
    "No queued notify awaits a reply",
  );
  expect(executed).toEqual([]);
});

test("time passes through every due time on the way, so retries run and dead-letter in order", async () => {
  let attempts = 0;
  await using saga = processFixture({
    ...connector,
    deadlines: {
      remind: () => {
        attempts++;
        throw new Error("provider down");
      },
    },
  });
  await saga
    .given(event("crm.file.opened", "f1", { by: "ann", remindAt: saga.now().toISOString() }))
    .when(timePasses(3600_000))
    .expectNoDeadline("remind");
  expect(attempts).toBe(5);
  expect(await saga.store.deadLetters()).toMatchObject([{ kind: "saga-message", attempts: 5 }]);
});

test("a failed expectation names what differs and fails the rest of the chain", async () => {
  const opened = event("crm.file.opened", "f1", { by: "ann", remindAt: at("09:00") });
  {
    await using saga = processFixture(connector, { now: at("08:00") });
    const failing = saga.given(opened).expectDeadline("remind", at("10:00"));
    await expect(failing).rejects.toThrow(/2026-10-01T09:00:00.000Z/);
    await expect(saga.expectState({ opened: "ann" })).rejects.toThrow(/2026-10-01T09:00:00.000Z/);
  }
  await using saga = processFixture(connector);
  await expect(saga.given(opened).expectState({ opened: "bob" })).rejects.toThrow(/bob/);
  // A handler failure retries quietly, so the expectation it breaks lists it.
  await using unrouted = processFixture(connector);
  await expect(
    unrouted.given(opened).when(sagaMessage("escalate", { to: "lead" }, { to: "f1" })).expectState({ opened: "ann", escalated: "lead" }),
  ).rejects.toThrow("Saga Message failed; retrying: Queued Message notify has no Worker");
  await using empty = processFixture(connector);
  await expect(empty.expectState({})).rejects.toThrow(
    "No Saga instance yet: pass the instance id",
  );
});

const day = 24 * 3600_000;
/** A dunning-like Workflow: waits a day for the close, asks for a reminder, tells it, and a promise extends the wait. */
const chaser: WorkflowDefinition = {
  version: 1,
  start: "crm.file.opened",
  id: (opened) => opened.aggregate.id,
  correlate: { "crm.file.closed": (closed) => closed.aggregate.id },
  signals: { promised: {} },
  async run(ctx, opened) {
    const id = opened.aggregate?.id ?? opened.id;
    await ctx.set({ stage: "waiting" });
    let closed = await ctx.waitFor({ kind: "event", name: "crm.file.closed" }, { timeout: day });
    if (closed.timedOut) {
      const answer = await ctx.ask("notify", { file: id });
      await ctx.tell("crm.file.remind", { stream: { context: "crm", aggregate: "file", id }, name: "remind", payload: { round: 1, channel: answer.value } });
      await ctx.set({ stage: "reminded" });
      const promise = await ctx.waitFor({ kind: "signal", name: "promised" }, { timeout: day });
      if (promise.timedOut) ctx.fail("never promised");
      closed = await ctx.waitFor({ kind: "event", name: "crm.file.closed" }, { timeout: promise.value.days * day });
      if (closed.timedOut) return "overdue";
    }
    return closed.value.payload.reason;
  },
};
const chaserOptions = {
  name: "chaser",
  now: at("08:00"),
  workers: { notifier: { handles: ["notify"], execute: () => {} } },
};

test("a Workflow starts on an Event, times out, asks a Worker, tells a Command and fails when nobody promises", async () => {
  await using flow = processFixture(chaser, chaserOptions);
  await flow
    .given(event("crm.file.opened", "f1", {}))
    .expectStatus("running")
    .expectState({ stage: "waiting" })
    .expectDeadline(workflowStep.waitFor("crm.file.closed"), "2026-10-02T08:00:00.000Z")
    .when(timePasses(day))
    .expectQueued([{ name: "notify", payload: { file: "f1" }, replyTo: { workflow: "chaser", instance: "f1", step: workflowStep.ask(chaserOptions.workers.notifier) } }])
    .when(reply("notify", { outcome: "ok", value: "mail" }))
    .expectCommands([{ stream: { context: "crm", aggregate: "file", id: "f1" }, name: "remind", payload: { round: 1, channel: "mail" } }])
    .expectState({ stage: "reminded" })
    .when(timePasses(day))
    .expectStatus("failed")
    .expectResult(undefined);
});

test("a Workflow started by id takes a Signal, and its status and result are expected", async () => {
  await using flow = processFixture(chaser, chaserOptions);
  await flow
    .given(start("chaser", "f2", { id: "f2" }), timePasses(day), reply("notify", { outcome: "ok", value: "sms" }))
    .when(signal("f2", { workflow: "chaser", name: "promised", payload: { days: 3 } }))
    .expectStatus("running")
    .expectDeadline(workflowStep.waitFor("crm.file.closed", 1), "2026-10-05T08:00:00.000Z")
    .when(event("crm.file.closed", "f2", { reason: "paid" }))
    .expectStatus("completed")
    .expectResult("paid")
    .when(start("chaser", "f2", { id: "f2" }, { reuse: true }))
    .expectStatus("running", "f2");
  await expect(flow.when(signal("f2", { workflow: "chaser", name: "other", payload: null }))).rejects.toThrow("Workflow chaser has no Signal other");
});

test("a start or Signal names the Workflow under test", async () => {
  await using flow = processFixture(chaser, chaserOptions);
  await expect(flow.when(start("other", "x"))).rejects.toThrow("The fixture runs Workflow chaser, not Workflow other");
});
