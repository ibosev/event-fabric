import { expect, test } from "vitest";
import { setTimeout as delay } from "node:timers/promises";
import { WorkflowCancelled } from "../src/errors.js";
import { newWorkflowInstance, runWorkflow } from "../src/workflows/engine.js";
import type {
  WorkflowBody,
  EngineContext,
  WorkflowEffect,
  WorkflowHandle,
  WorkflowInstance,
  WorkflowOptions,
  WorkflowRun,
  WorkflowWake,
} from "../src/workflows/engine.js";

const t0 = Date.parse("2026-01-01T00:00:00.000Z");
const at = (ms = 0) => new Date(t0 + ms);
const day = 86_400_000;
const start: WorkflowWake = { kind: "start", at: at() };
const signal = (name: string): WorkflowHandle => ({ kind: "signal", name });
const event = (name: string): WorkflowHandle => ({ kind: "event", name });
const sig = (name: string, value: unknown, ms = 0): WorkflowWake => ({
  kind: "signal",
  name,
  value,
  at: at(ms),
});
const deadline = (step: string, ms: number): WorkflowWake => ({
  kind: "deadline",
  step,
  at: at(ms),
});

/** Runs one wake after another, each against the instance the last one committed. */
async function drive(
  body: WorkflowBody | WorkflowBody[],
  wakes: WorkflowWake[],
  options?: WorkflowOptions,
  instance: WorkflowInstance = newWorkflowInstance(),
) {
  const effects: WorkflowEffect[] = [];
  let run!: WorkflowRun;
  for (const [i, wake] of wakes.entries()) {
    run = await runWorkflow(
      Array.isArray(body) ? body[i]! : body,
      instance,
      wake,
      options,
    );
    instance = run.instance;
    effects.push(...run.effects);
  }
  return { ...run, effects };
}
const tells = (effects: WorkflowEffect[]) =>
  effects.flatMap((e) =>
    e.type === "tell" ? [`${e.command}:${JSON.stringify(e.message)}`] : [],
  );
/** Collects ctx calls made after the run was judged suspended. */
const lateCalls = () => {
  const late: string[] = [];
  return { late, options: { onLate: (step: string) => late.push(step) } };
};

// Journal and replay

test("a recorded step resolves with its value and a tell is not re-sent on replay", async () => {
  const body: WorkflowBody = async (ctx) => {
    const a = await ctx.waitFor(signal("a"));
    await ctx.tell("got", a);
    const b = await ctx.waitFor(signal("b"));
    return a + b;
  };
  const run = await drive(body, [start, sig("a", 1), sig("b", 2)]);
  expect(run.instance).toMatchObject({ status: "completed", result: 3 });
  expect(tells(run.effects)).toEqual(["got:1"]);
});

test("an unsatisfied step records a pending wait and the run suspends", async () => {
  const run = await runWorkflow(
    async (ctx) => ctx.waitFor(signal("a")),
    newWorkflowInstance(),
    start,
  );
  expect(run.instance.status).toBe("running");
  expect(run.pending).toEqual(["signal:a#0"]);
  expect(run.instance.journal).toEqual({ "signal:a#0": { s: "pending" } });
});

test("now, random, uuid and set are journaled and replay the same values", async () => {
  const seen: unknown[][] = [];
  const body: WorkflowBody = async (ctx) => {
    const values = [await ctx.now(), await ctx.random(), await ctx.uuid()];
    seen.push(values);
    await ctx.set({ values });
    await ctx.waitFor(signal("go"));
    await ctx.waitFor(signal("never"));
  };
  const run = await drive(body, [start, sig("go", 1, 5000), sig("x", 1, 9000)]);
  expect(seen).toHaveLength(3);
  expect(seen[1]).toEqual(seen[0]);
  expect(seen[2]).toEqual(seen[0]);
  expect(seen[0]![0]).toEqual(at());
  // State is JSON, so the Date is its ISO text.
  expect(run.instance.state).toEqual({ values: JSON.parse(JSON.stringify(seen[0])) });
});

test("a loop reuses one name with occurrence-numbered keys", async () => {
  const run = await runWorkflow(
    async (ctx) => {
      for (let i = 0; i < 3; i++) await ctx.tell("x", i);
      await ctx.waitFor(signal("never"));
    },
    newWorkflowInstance(),
    start,
  );
  expect(Object.keys(run.instance.journal)).toEqual([
    "tell:x#0",
    "tell:x#1",
    "tell:x#2",
    "signal:never#0",
  ]);
});

test("read is a journaled step: the Readmodel is read once and replays use the record", async () => {
  let reads = 0;
  const reader = async () => (await delay(2), { balance: 10 + reads++ });
  const body: WorkflowBody = async (ctx) => {
    const account = await ctx.read("billing", "accounts", "acc-1");
    await ctx.waitFor(signal("go"));
    return account;
  };
  const run = await drive(body, [start, sig("go", 1)], { reader });
  expect(reads).toBe(1);
  expect(run.instance.result).toEqual({ balance: 10 });
});

test("waitFor with a timeout sets a Deadline that fires as timedOut or is cancelled by the Signal", async () => {
  const body: WorkflowBody = async (ctx) =>
    ctx.waitFor(signal("paid"), { timeout: 60_000 });
  const first = await runWorkflow(body, newWorkflowInstance(), start);
  expect(first.effects).toEqual([
    { type: "deadline", step: "signal:paid#0", at: at(60_000) },
  ]);
  const timedOut = await runWorkflow(
    body,
    first.instance,
    deadline("signal:paid#0", 60_000),
  );
  expect(timedOut.instance.result).toEqual({ timedOut: true });
  const paid = await runWorkflow(body, first.instance, sig("paid", 42));
  expect(paid.instance.result).toEqual({ value: 42 });
  expect(paid.effects).toEqual([
    { type: "cancelDeadline", step: "signal:paid#0" },
  ]);
});

test("waitFor and sleep take an absolute due time as well as a duration", async () => {
  const due = "2026-01-15T00:00:00.000Z";
  const body: WorkflowBody = async (ctx) => {
    const paid = await ctx.waitFor(signal("paid"), { at: due });
    await ctx.sleep("grace", new Date(Date.parse(due) + day));
    return paid;
  };
  const first = await runWorkflow(body, newWorkflowInstance(), start);
  expect(first.effects).toEqual([{ type: "deadline", step: "signal:paid#0", at: new Date(due) }]);
  const timedOut = await runWorkflow(body, first.instance, deadline("signal:paid#0", 14 * day));
  expect(timedOut.effects).toContainEqual({ type: "deadline", step: "sleep:grace#0", at: new Date(Date.parse(due) + day) });
  const slept = await runWorkflow(body, timedOut.instance, deadline("sleep:grace#0", 15 * day));
  expect(slept.instance).toMatchObject({ status: "completed", result: { timedOut: true } });
});

test("ask sends one Outbox Message and resolves with the Worker's reply", async () => {
  const body: WorkflowBody = async (ctx) => ctx.ask("mailer", { to: "a@b" });
  const first = await runWorkflow(body, newWorkflowInstance(), start);
  expect(first.effects).toEqual([
    {
      type: "ask",
      step: "ask:mailer#0",
      name: "mailer",
      message: { to: "a@b" },
      run: first.instance.run,
    },
  ]);
  const replayed = await runWorkflow(body, first.instance, sig("other", 1));
  expect(replayed.effects).toEqual([]);
  const done = await runWorkflow(body, replayed.instance, {
    kind: "reply",
    step: "ask:mailer#0",
    value: { outcome: "ok", value: 1 },
    at: at(),
  });
  expect(done.instance.result).toEqual({ outcome: "ok", value: 1 });
});

test("ask with a timeout resolves timedOut when its Deadline fires first", async () => {
  const body: WorkflowBody = async (ctx) =>
    ctx.ask("mailer", { to: "a@b" }, { timeout: 1000 });
  const first = await runWorkflow(body, newWorkflowInstance(), start);
  expect(first.effects).toContainEqual({
    type: "deadline",
    step: "ask:mailer#0",
    at: at(1000),
  });
  const run = await runWorkflow(
    body,
    first.instance,
    deadline("ask:mailer#0", 1000),
  );
  expect(run.instance.result).toEqual({ outcome: "timedOut" });
});

// Suspension: one macrotask boundary, plus two guards

test("Promise.all over several ctx calls suspends on every pending branch", async () => {
  const body: WorkflowBody = async (ctx) =>
    Promise.all([
      ctx.waitFor(signal("a")),
      ctx.waitFor(signal("b")),
      ctx.tell("t", 1),
    ]);
  const first = await runWorkflow(body, newWorkflowInstance(), start);
  expect(first.pending.sort()).toEqual(["signal:a#0", "signal:b#0"]);
  const run = await drive(body, [sig("b", 2), sig("a", 1)], {}, first.instance);
  expect(run.instance.result).toEqual([1, 2, null]); // the result is JSON
  expect(tells([...first.effects, ...run.effects])).toEqual(["t:1"]);
});

test("awaiting a non-ctx promise between steps parks the instance and the late call throws", async () => {
  const { late, options } = lateCalls();
  const run = await runWorkflow(
    async (ctx) => {
      await ctx.tell("x", 1);
      await delay(20);
      await ctx.tell("y", 1);
    },
    newWorkflowInstance(),
    start,
    options,
  );
  expect(run.instance.status).toBe("drifted");
  expect(run.reason).toMatch(/non-ctx promise/);
  expect(run.effects).toEqual([]);
  await delay(40);
  expect(late).toEqual(["tell:y"]);
});

test("a non-ctx await beside a pending branch is caught only as a late call", async () => {
  const { late, options } = lateCalls();
  const run = await runWorkflow(
    async (ctx) =>
      Promise.all([
        ctx.waitFor(signal("a")),
        (async () => (await delay(20), ctx.tell("y", 1)))(),
      ]),
    newWorkflowInstance(),
    start,
    options,
  );
  // Judged suspended on signal:a; the foreign await surfaces after the commit decision.
  expect(run.instance.status).toBe("running");
  await delay(40);
  expect(late).toEqual(["tell:y"]);
});

test("setTimeout(0) in the body is either flagged or harmless, never wrong", async () => {
  const body: WorkflowBody = async (ctx) => {
    await new Promise((resolve) => setTimeout(resolve, 0));
    await ctx.tell("y", 1);
    return "ok";
  };
  for (let i = 0; i < 20; i++) {
    const run = await runWorkflow(body, newWorkflowInstance(), start);
    await delay(3);
    if (run.instance.status === "completed")
      expect(tells(run.effects)).toEqual(["y:1"]);
    else expect(run.instance.status).toBe("drifted");
  }
});

test("a body that returns synchronously completes and a synchronous throw fails it", async () => {
  const ok = await runWorkflow(
    (_ctx, input) => input * 2,
    newWorkflowInstance(21),
    start,
  );
  expect(ok.instance).toMatchObject({ status: "completed", result: 42 });
  const bad = await runWorkflow(
    () => {
      throw new Error("boom");
    },
    newWorkflowInstance(),
    start,
  );
  expect(bad.instance.status).toBe("failed");
  expect(bad.instance.error).toMatch(/boom/);
});

test("a deep chain of async helpers that await only ctx calls completes in one wake", async () => {
  const deep = async (ctx: EngineContext, n: number): Promise<void> =>
    n === 0 ? ctx.tell("leaf", 1) : await deep(ctx, n - 1);
  const run = await runWorkflow(
    async (ctx) => {
      await deep(ctx, 300);
      await ctx.tell("after", 1);
      return "ok";
    },
    newWorkflowInstance(),
    start,
  );
  expect(run.instance.status).toBe("completed");
  expect(tells(run.effects)).toEqual(["leaf:1", "after:1"]);
});

test("awaiting already-settled non-ctx promises is allowed", async () => {
  const cached = Promise.resolve(5);
  const run = await runWorkflow(
    async (ctx) => {
      const c = await cached;
      await Promise.resolve();
      await ctx.tell("x", c);
      return c;
    },
    newWorkflowInstance(),
    start,
  );
  expect(run.instance.result).toBe(5);
});

// Drift: v1 handles some wakes, then changed code handles the next

test("renaming a finished step drifts", async () => {
  const v1: WorkflowBody = async (ctx) => {
    await ctx.sleep("first", 1000);
    await ctx.waitFor(signal("go"));
    await ctx.tell("t", 1);
  };
  const v2: WorkflowBody = async (ctx) => {
    await ctx.sleep("cooldown", 1000);
    await ctx.waitFor(signal("go"));
    await ctx.tell("t", 1);
  };
  const run = await drive(
    [v1, v1, v2],
    [start, deadline("sleep:first#0", 1000), sig("go", 1)],
  );
  expect(run.instance.status).toBe("drifted");
  expect(run.reason).toMatch(/sleep:first#0 not reached/);
  expect(tells(run.effects)).toEqual([]);
});

test("removing a finished step drifts at the next new step, not before", async () => {
  const v1: WorkflowBody = async (ctx) => {
    await ctx.tell("a", 1);
    await ctx.waitFor(signal("go"));
    await ctx.tell("b", 1);
    await ctx.waitFor(signal("end"));
  };
  const v2: WorkflowBody = async (ctx) => {
    await ctx.waitFor(signal("go"));
    await ctx.tell("b", 1);
    await ctx.waitFor(signal("end"));
  };
  const quiet = await drive([v1, v2], [start, sig("other", 1)]);
  expect(quiet.instance.status).toBe("running");
  const run = await drive(v2, [sig("go", 1)], {}, quiet.instance);
  expect(run.instance.status).toBe("drifted");
  expect(run.reason).toMatch(/tell:a#0 not reached/);
});

const branch = (
  ctx: EngineContext,
  name: string,
  tell = `t${name}`,
  message: unknown = 1,
) =>
  (async () => {
    await ctx.waitFor(signal(name));
    await ctx.tell(tell, message);
  })();

test("reordering independent branches does not drift", async () => {
  const v1: WorkflowBody = async (ctx) =>
    Promise.all([branch(ctx, "a"), branch(ctx, "b")]);
  const v2: WorkflowBody = async (ctx) =>
    Promise.all([branch(ctx, "b"), branch(ctx, "a")]);
  const run = await drive([v1, v1, v2], [start, sig("a", 1), sig("b", 1)]);
  expect(run.instance.status).toBe("completed");
  expect(tells(run.effects).sort()).toEqual(["ta:1", "tb:1"]);
});

test("inserting a step before a finished one drifts and sends nothing", async () => {
  const v1: WorkflowBody = async (ctx) => {
    await ctx.tell("a", 1);
    await ctx.waitFor(signal("go"));
  };
  const v2: WorkflowBody = async (ctx) => {
    await ctx.tell("audit", 1);
    await ctx.tell("a", 1);
    await ctx.waitFor(signal("go"));
  };
  const run = await drive([v1, v2], [start, sig("go", 1)]);
  expect(run.instance.status).toBe("drifted");
  expect(run.reason).toMatch(/tell:a#0 not reached/);
  expect(tells(run.effects)).toEqual(["a:1"]); // from v1's wake only
});

test("appending a step after the last finished one does not drift", async () => {
  const v1: WorkflowBody = async (ctx) => {
    await ctx.tell("a", 1);
    await ctx.waitFor(signal("go"));
  };
  const v2: WorkflowBody = async (ctx) => {
    await ctx.tell("a", 1);
    await ctx.waitFor(signal("go"));
    await ctx.tell("b", 1);
  };
  const run = await drive([v1, v2], [start, sig("go", 1)]);
  expect(run.instance.status).toBe("completed");
  expect(tells(run.effects)).toEqual(["a:1", "b:1"]);
});

test("concurrent branches woken out of order do not drift: the check runs after replay", async () => {
  const body: WorkflowBody = async (ctx) =>
    Promise.all([branch(ctx, "a"), branch(ctx, "b")]);
  const run = await drive(body, [start, sig("b", 1), sig("a", 1)]);
  expect(run.instance.status).toBe("completed");
});

test("same-named steps in concurrent branches that swap occurrences drift on their arguments", async () => {
  const body: WorkflowBody = async (ctx) =>
    Promise.all([
      branch(ctx, "a", "notify", "A"),
      branch(ctx, "b", "notify", "B"),
    ]);
  const run = await drive(body, [start, sig("b", 1), sig("a", 1)]);
  // Without the check, A's notify is silently skipped and B's sent twice.
  expect(run.instance.status).toBe("drifted");
  expect(run.reason).toMatch(/tell:notify#0 was recorded with other arguments/);
});

// Lifecycle

test("fail ends the instance failed even if the body catches it; later steps send nothing", async () => {
  const body: WorkflowBody = async (ctx) => {
    await ctx.waitFor(signal("a"));
    try {
      ctx.fail("no money");
    } catch {}
    await ctx.tell("after", 1);
  };
  const run = await drive(body, [start, sig("a", 1)]);
  expect(run.instance).toMatchObject({
    status: "failed",
    error: "no money",
    journal: {},
  });
  expect(tells(run.effects)).toEqual([]);
});

test("ctx.cancel ends the instance cancelled and cancels its Deadlines", async () => {
  const body: WorkflowBody = async (ctx) => {
    const remind = ctx.sleep("remind", 1000);
    if (await ctx.waitFor(signal("stop"))) ctx.cancel();
    await remind;
  };
  const run = await drive(body, [start, sig("stop", true)]);
  expect(run.instance.status).toBe("cancelled");
  expect(run.effects.filter((e) => e.type !== "deadline")).toEqual([
    { type: "cancelDeadline", step: "sleep:remind#0" },
  ]);
});

test("an external cancel is thrown at the next ctx await, can be caught, and replays at the same step", async () => {
  const body: WorkflowBody = async (ctx) => {
    await ctx.tell("charge", 1);
    try {
      await ctx.waitFor(signal("shipped"));
      return "shipped";
    } catch (error) {
      if (!(error instanceof WorkflowCancelled)) throw error;
      await ctx.tell("refund", 1);
      await ctx.waitFor(signal("ack"));
      return "refunded";
    }
  };
  const run = await drive(body, [
    start,
    { kind: "cancel", at: at() },
    sig("ack", 1),
  ]);
  expect(run.instance).toMatchObject({
    status: "completed",
    result: "refunded",
  });
  expect(tells(run.effects)).toEqual(["charge:1", "refund:1"]);
});

test("an uncaught external cancel ends the instance cancelled", async () => {
  const run = await drive(
    async (ctx) => ctx.sleep("long", day),
    [start, { kind: "cancel", at: at() }],
  );
  expect(run.instance.status).toBe("cancelled");
  expect(run.effects.at(-1)).toEqual({
    type: "cancelDeadline",
    step: "sleep:long#0",
  });
});

test("restart begins a fresh journal under the new input and carries buffered Signals over", async () => {
  const body: WorkflowBody = async (ctx, input: { n: number }) => {
    await ctx.sleep("pause", 1000);
    const tick = await ctx.waitFor(signal("tick"));
    await ctx.tell("tick", { n: input.n, tick });
    ctx.restart({ n: input.n + 1 });
  };
  const run = await drive(
    body,
    [start, sig("tick", 1), sig("tick", 2), deadline("sleep:pause#0", 1000)],
    {},
    newWorkflowInstance({ n: 0 }),
  );
  expect(run.restarts).toBe(1);
  expect(run.instance).toMatchObject({
    status: "running",
    input: { n: 1 },
    inbox: { "signal:tick": [2] },
  });
  expect(Object.keys(run.instance.journal)).toEqual(["sleep:pause#0"]);
  expect(tells(run.effects)).toEqual(['tick:{"n":0,"tick":1}']);
});

test("restart runs the body the caller names for a fresh run, and ctx.start passes reuse", async () => {
  const old: WorkflowBody = async (ctx) => {
    await ctx.waitFor(signal("go"));
    ctx.restart("again");
  };
  const latest: WorkflowBody = async (ctx, input) => {
    await ctx.start("child", "c1", input, { reuse: true });
    return `latest ${input}`;
  };
  const first = await runWorkflow(old, newWorkflowInstance(), start);
  const run = await runWorkflow(old, first.instance, sig("go", 1), { restartBody: latest });
  expect(run.restarts).toBe(1);
  expect(run.instance).toMatchObject({ status: "completed", result: "latest again" });
  expect(run.effects).toEqual([
    { type: "start", step: "start:child#0", name: "child", id: "c1", input: "again", reuse: true },
  ]);
});

test("a body that restarts without waiting fails after 100 restarts in one wake", async () => {
  const run = await runWorkflow(
    async (ctx, n: number) => {
      await ctx.tell("loop", n);
      ctx.restart(n + 1);
    },
    newWorkflowInstance(0),
    start,
  );
  expect(run.instance.status).toBe("failed");
  expect(run.instance.error).toMatch(/100 restarts/);
});

test("completion records the result and drops the journal", async () => {
  const run = await drive(
    async (ctx) => ({ total: (await ctx.waitFor(signal("a"))) + 1 }),
    [start, sig("a", 1)],
  );
  expect(run.instance).toMatchObject({
    status: "completed",
    result: { total: 2 },
    journal: {},
  });
});

// Signals and correlated Events

test("Signals buffer per name, waitFor takes the oldest, and unread ones are dropped at completion", async () => {
  const body: WorkflowBody = async (ctx) => {
    await ctx.sleep("s", 10);
    return [await ctx.waitFor(signal("x")), await ctx.waitFor(signal("x"))];
  };
  const run = await drive(body, [
    start,
    sig("x", 1),
    sig("x", 2),
    sig("x", 3),
    sig("y", 9),
    deadline("sleep:s#0", 10),
  ]);
  expect(run.instance.result).toEqual([1, 2]);
  expect(run.droppedSignals).toBe(2);
  expect(run.instance.inbox).toEqual({});
});

test("a correlated Event that arrives while the body waits on something else is buffered", async () => {
  const dunning: WorkflowBody = async (ctx) => {
    const first = await ctx.waitFor(event("invoice.settled"), {
      timeout: 14 * day,
    });
    if (!first.timedOut) return "paid";
    await ctx.ask("mailer", { kind: "reminder" });
    const second = await ctx.waitFor(event("invoice.settled"), {
      timeout: 14 * day,
    });
    return second.timedOut ? "overdue" : "paid after reminder";
  };
  const run = await drive(dunning, [
    start,
    deadline("event:invoice.settled#0", 14 * day),
    {
      kind: "event",
      name: "invoice.settled",
      value: { id: "inv-1" },
      at: at(14 * day + 1),
    },
    {
      kind: "reply",
      step: "ask:mailer#0",
      value: { outcome: "ok" },
      at: at(14 * day + 2),
    },
  ]);
  expect(run.instance.result).toBe("paid after reminder");
});

test("the committed instance survives a JSON round trip, as a store would hold it", async () => {
  const body: WorkflowBody = async (ctx) => {
    const now = await ctx.now();
    await ctx.waitFor(signal("go"), { timeout: 1000 });
    return now.toISOString();
  };
  const first = await runWorkflow(body, newWorkflowInstance(), start);
  const stored = JSON.parse(JSON.stringify(first.instance));
  const run = await runWorkflow(body, stored, sig("go", 1, 500));
  expect(run.instance.result).toBe(at().toISOString());
});

// Workflow edge cases

test("a suspended run is not retained once nothing holds it", async () => {
  const v8 = await import("node:v8");
  const vm = await import("node:vm");
  v8.setFlagsFromString("--expose-gc");
  const gc = vm.runInNewContext("gc") as () => void;
  const body: WorkflowBody = async (ctx, input) => {
    await ctx.waitFor(signal("go"));
    return input;
  };
  let alive = 0;
  const kept = new FinalizationRegistry(() => alive--);
  const first = await runWorkflow(body, newWorkflowInstance(), start);
  // In its own function, so no frame of this test keeps the last input.
  const wake = async (i: number) => {
    const input = { big: new Array(1000).fill(i) };
    alive++;
    kept.register(input, i);
    await runWorkflow(body, { ...first.instance, input }, sig("other", i));
  };
  for (let i = 0; i < 20; i++) await wake(i);
  for (let i = 0; i < 10 && alive; i++) {
    gc();
    await delay(10);
  }
  expect(alive).toBe(0);
});

test("arguments compare independent of property order, as Postgres JSONB reorders them", async () => {
  const body: WorkflowBody = async (ctx, input) => {
    await ctx.queue("ping", input);
    return ctx.waitFor(signal("go"));
  };
  const first = await runWorkflow(body, newWorkflowInstance({ zz: 1, a: 2 }), start);
  const reordered = { ...first.instance, input: { a: 2, zz: 1 } };
  const run = await runWorkflow(body, reordered, sig("go", "ok"));
  expect(run.reason).toBeUndefined();
  expect(run.instance.result).toBe("ok");
});

test("a journal written before 172 still replays without drift", async () => {
  const body: WorkflowBody = async (ctx, input) => {
    await ctx.queue("ping", input);
    return ctx.waitFor(signal("go"));
  };
  const old: WorkflowInstance = {
    ...newWorkflowInstance({ zz: 1, a: 2 }),
    journal: { "queue:ping#0": { s: "done", a: '{"zz":1,"a":2}' }, "signal:go#0": { s: "pending" } },
  };
  const run = await runWorkflow(body, old, sig("go", "ok"));
  expect(run.reason).toBeUndefined();
  expect(run.instance.result).toBe("ok");
});

test("the body's changes to its input or a step's value do not rewrite the journal", async () => {
  const body: WorkflowBody = async (ctx, input: { count: number }) => {
    input.count++;
    const got = await ctx.waitFor(signal("a"));
    got.count++;
    await ctx.waitFor(signal("b"));
    return input.count + got.count;
  };
  const run = await drive(body, [start, sig("a", { count: 10 }), sig("b", null)], {}, newWorkflowInstance({ count: 0 }));
  expect(run.instance.result).toBe(12);
});

test("each restart in one wake queues under its own run", async () => {
  const body: WorkflowBody = async (ctx, n: number) => {
    const got = await ctx.waitFor(signal("go"));
    await ctx.queue("ping", got);
    ctx.restart(n + 1);
  };
  const first = await runWorkflow(body, newWorkflowInstance(0), start);
  const buffered = { ...first.instance, inbox: { "signal:go": [1, 2] } };
  const run = await runWorkflow(body, buffered, sig("go", 3));
  const runs = run.effects.flatMap((e) => (e.type === "queue" ? [e.run] : []));
  expect(runs).toHaveLength(3);
  expect(new Set(runs).size).toBe(3);
  expect(run.instance.run).not.toBe(first.instance.run);
});

test("state and result hold JSON, so a Date becomes its ISO text", async () => {
  const run = await drive(
    async (ctx) => {
      const now = await ctx.now();
      await ctx.set({ at: now });
      return { at: now };
    },
    [start],
  );
  expect(run.instance).toMatchObject({
    status: "completed",
    state: { at: at().toISOString() },
    result: { at: at().toISOString() },
  });
});

test("state or a result that cannot be JSON fails the instance", async () => {
  const run = await drive(async (ctx) => {
    await ctx.set({ f: () => 1 });
  }, [start]);
  expect(run.instance).toMatchObject({ status: "failed", error: expect.stringMatching(/JSON/) });
});

// Cost per wake

test("replay cost grows linearly and stays under a millisecond-scale budget at 1,000 steps", async () => {
  const steps =
    (n: number): WorkflowBody =>
    async (ctx) => {
      for (let i = 0; i < n; i++) {
        if (i % 4 === 0) await ctx.tell("t", { i });
        else if (i % 4 === 1) await ctx.now();
        else if (i % 4 === 2) await ctx.set({ i });
        else await ctx.random();
      }
      await ctx.waitFor(signal("go"));
      await ctx.tell("after", 1);
      await ctx.waitFor(signal("never"));
    };
  const p50 = async (f: () => Promise<unknown>, iterations = 50) => {
    for (let i = 0; i < 5; i++) await f();
    const samples: number[] = [];
    for (let i = 0; i < iterations; i++) {
      const t = performance.now();
      await f();
      samples.push(performance.now() - t);
    }
    return samples.sort((a, b) => a - b)[iterations >> 1]!;
  };
  const rows: Record<number, { replay: number; advance: number }> = {};
  for (const n of [10, 100, 1000]) {
    const body = steps(n);
    const { instance } = await runWorkflow(body, newWorkflowInstance(), start);
    rows[n] = {
      replay: await p50(() => runWorkflow(body, instance, deadline("none", 1))),
      advance: await p50(() => runWorkflow(body, instance, sig("go", 1, 1))),
    };
  }
  // Generous bounds: the prototype measured 0.63 ms p50 at 1,000 steps.
  expect(rows[1000]!.replay).toBeLessThan(25);
  expect(rows[1000]!.advance).toBeLessThan(25);
});
