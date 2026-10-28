import { expect, test, vi } from "vitest";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pino from "pino";
import { runCLI } from "../../src/cli/index.js";
import { WorkflowCancelled } from "../../src/errors.js";
import { compile } from "../../src/authoring/index.js";
import { createApplication } from "../../src/runtime/index.js";
import { createInMemoryTransport } from "../../src/transport/memory.js";
import { workflowsApi } from "../../src/internal.js";
import type { Persistence } from "../../src/types.js";
import type { WorkflowConfig } from "../../src/runtime/types.js";
import { aggregate, DateTime, defineDomain, message, worker } from "../../src/authoring/aggregate.js";
import { createMemoryReadmodelStore } from "../../src/adapters/memory-readmodel.js";
import { signal, workflow } from "../../src/workflows/index.js";
import { Type } from "typebox";

const file = compile("crm", "file", {
  commands: {
    open: [(command, file) => file.apply.opened(command.payload)],
    close: [(command, file) => file.apply.closed(command.payload)],
    note: [(command, file) => file.apply.noted(command.payload)],
  },
  events: { opened() {}, closed() {}, noted() {} },
});
const stream = (id: string) => ({ context: "crm", aggregate: "file", id });

// The authored form: typed Handles in, typed query out.
const Text = Type.Object({ text: Type.String() });
const issue = aggregate("crm", "issue", {
  state: Type.Object({}),
  initialState: {},
  events: { noted: { schema: Text, handler() {} } },
  commands: { note: { schema: Text, steps: [(command, issue) => issue.apply.noted(command.payload)] } },
});
const domain = defineDomain(issue);
const lookup = message("lookup", Type.Object({ issue: Type.String() }));
const lookupWorker = worker(lookup, { execute: ({ payload }) => `found ${payload.issue}` });
const triage = workflow("triage", {
  version: 1,
  domain,
  signals: { assign: signal(Type.Object({ to: Type.String() })), close: signal() },
  state: Type.Object({ stage: Type.String() }),
  async run(ctx, input: { issue: string; priority: number }) {
    const { to } = await ctx.waitFor(ctx.signals.assign);
    await ctx.set({ stage: "assigned" });
    const reply = await ctx.ask(lookupWorker, { issue: input.issue });
    if (reply.outcome !== "ok") return ctx.fail(reply.error);
    await ctx.tell(domain.crm.issue.note(input.issue, { text: `${to}: ${reply.value}` }));
    await ctx.waitFor(ctx.signals.close);
    return { to, priority: input.priority };
  },
});

/** The Workflow consumer kind on an Event store that keeps Workflows and Saga Messages. */
export function workflowContract(adapter: string, persistence: () => Persistence | Promise<Persistence>) {
  const fixture = async (
    workflows: Record<string, WorkflowConfig>,
    options: { store?: Persistence; workers?: any; logger?: any; readmodels?: any } = {},
  ) => {
    const store = options.store ?? (await persistence());
    const app = createApplication({
      name: "workflows",
      store,
      aggregates: [file, issue],
      transport: createInMemoryTransport(),
      workflows,
      workers: options.workers,
      ...(options.readmodels && { readmodels: options.readmodels, readmodelStore: createMemoryReadmodelStore() }),
      pollInterval: 50,
      logger: options.logger ?? pino({ level: "silent" }),
    });
    await app.start();
    const api = app[workflowsApi];
    const settled = (name: string, id: string, status: string) =>
      vi.waitFor(async () => expect(await api.query(name, id)).toMatchObject({ status }), {
        timeout: 5000,
        interval: 20,
      });
    return { app, store, api, settled };
  };

  test(`${adapter}: a Workflow started by id waits for a Signal, completes and is queried from the store`, async () => {
    const { app, api, settled } = await fixture({
      greeting: {
        version: 1,
        signals: { name: {} },
        async run(ctx, input) {
          await ctx.set({ stage: "waiting" });
          const name = await ctx.waitFor({ kind: "signal", name: "name" });
          return `${input.greeting}, ${name}`;
        },
      },
    });
    try {
      await api.start("greeting", "g1", { greeting: "Hello" });
      await settled("greeting", "g1", "running");
      expect(await api.query("greeting", "g1")).toEqual({ status: "running", state: { stage: "waiting" } });
      // Idempotent on a live id.
      await api.start("greeting", "g1", { greeting: "Bye" });
      await api.signal("greeting", "g1", "name", "Ada");
      await settled("greeting", "g1", "completed");
      expect(await api.query("greeting", "g1")).toEqual({
        status: "completed",
        state: { stage: "waiting" },
        result: "Hello, Ada",
      });
      await expect(api.start("greeting", "g1", {})).rejects.toThrow(/finished/);
      await expect(api.signal("greeting", "g1", "other", 1)).rejects.toThrow(/no Signal other/);
    } finally {
      await app.close();
    }
  });

  test(`${adapter}: an authored Workflow starts with a first Signal, tells through a Command Handle, asks a Worker and is queried`, async () => {
    const { app, store, settled } = await fixture({ triage }, { workers: { lookup: lookupWorker } });
    try {
      await app.startWorkflow(triage, "t1", { issue: "t1", priority: 2 }, { signal: triage.signals.assign({ to: "ada" }) });
      await vi.waitFor(
        async () =>
          expect(
            (await store.readStream({ context: "crm", aggregate: "issue", id: "t1" })).map((event) => event.payload),
          ).toEqual([{ text: "ada: found t1" }]),
        { timeout: 5000, interval: 20 },
      );
      expect(await app.queryWorkflow(triage, "t1")).toEqual({ status: "running", state: { stage: "assigned" } });
      // A start with a Signal on a live instance delivers the Signal.
      await app.startWorkflow(triage, "t1", { issue: "t1", priority: 9 }, { signal: triage.signals.close() });
      await settled("triage", "t1", "completed");
      expect(await app.queryWorkflow(triage, "t1")).toEqual({
        status: "completed",
        state: { stage: "assigned" },
        result: { to: "ada", priority: 2 },
      });
      // The Signal's schema is checked when it is sent; a validation Rejection is a Gateway's 400.
      await expect(
        app.sendSignal("t2", { workflow: triage.name, name: "assign", payload: { to: 1 } }),
      ).rejects.toMatchObject({ name: "Rejection", kind: "validation", message: expect.stringMatching(/^Invalid Signal assign/) });
    } finally {
      await app.close();
    }
  });

  test(`${adapter}: an Event starts an instance, a correlated Event wakes it, Deadlines time out and sleep, Commands and a Worker's reply flow`, async () => {
    const asked: any[] = [];
    const { app, store, api, settled } = await fixture(
      {
        filing: {
          version: 1,
          start: "crm.file.opened",
          id: (event) => event.aggregate.id,
          correlate: { "crm.file.closed": (event) => event.aggregate.id },
          async run(ctx, opened) {
            const first = await ctx.waitFor({ kind: "event", name: "crm.file.closed" }, { timeout: 100 });
            await ctx.sleep("pause", 50);
            const reply = await ctx.ask("lookup", { file: opened.aggregate.id });
            await ctx.tell("crm.file.note", { stream: stream(opened.aggregate.id), name: "note", payload: { text: reply.value } });
            const closed = await ctx.waitFor({ kind: "event", name: "crm.file.closed" });
            return { first, reason: closed.payload.reason };
          },
        },
      },
      {
        workers: {
          lookup: {
            handles: ["lookup"],
            execute: ({ payload }: any) => (asked.push(payload), `found ${payload.file}`),
          },
        },
      },
    );
    try {
      await app.sendCommand({ stream: stream("f1"), name: "open", payload: {} });
      await settled("filing", "f1", "running");
      // Times out at 100 ms, sleeps, asks, notes the reply, then waits for the close.
      await vi.waitFor(async () => {
        const events = await store.readStream(stream("f1"));
        expect(events.map((event) => event.name)).toEqual(["opened", "noted"]);
        expect(events[1]!.payload).toEqual({ text: "found f1" });
      }, { timeout: 5000, interval: 20 });
      expect(asked).toEqual([{ file: "f1" }]);
      await app.sendCommand({ stream: stream("f1"), name: "close", payload: { reason: "done" } });
      await settled("filing", "f1", "completed");
      expect((await api.query("filing", "f1"))!.result).toEqual({ first: { timedOut: true }, reason: "done" });
      // An Event-start for a known id is a no-op.
      await app.sendCommand({ stream: stream("f1"), name: "open", payload: {} });
      await app.sendCommand({ stream: stream("f2"), name: "open", payload: {} });
      await settled("filing", "f2", "running");
      expect(await api.query("filing", "f1")).toMatchObject({ status: "completed" });
    } finally {
      await app.close();
    }
  });

  const counter: WorkflowConfig = {
    version: 1,
    signals: { add: {} },
    async run(ctx) {
      let total = 0;
      for (;;) {
        const n = await ctx.waitFor({ kind: "signal", name: "add" });
        if (n === 0) return total;
        await ctx.tell("crm.file.note", { stream: stream("notes"), name: "note", payload: { total: (total += n) } });
      }
    },
  };

  test(`${adapter}: ctx.id names the instance, and ctx.read revives a Readmodel's dates on replay`, async () => {
    // A Workflow started by id needs no id in its input; a `DateTime()` field is a Date
    // on the first run and after the journal round-trips through the store.
    const desk = {
      models: {
        notes: {
          schema: Type.Object({ text: Type.String(), at: DateTime() }),
          reactions: {
            "crm.issue.noted": (event: any, vm: any) =>
              vm.set({ text: event.payload.text, at: new Date(event.timestamp) }),
          },
        },
      },
    };
    const stamped = workflow("stamped", {
      version: 1,
      domain,
      reads: ["desk"],
      signals: { go: signal() },
      async run(ctx) {
        const first = await ctx.read("desk", "notes", ctx.id);
        await ctx.waitFor(ctx.signals.go);
        return { id: ctx.id, at: first?.at instanceof Date ? first.at.toISOString() : typeof first?.at };
      },
    });
    const { app, settled } = await fixture({ stamped }, { readmodels: { desk } });
    try {
      const outcome = await app.sendCommand(domain.crm.issue.note("n1", { text: "hi" }));
      if (outcome.status !== "accepted") throw new Error(JSON.stringify(outcome));
      await vi.waitFor(async () => expect((await app.getReadmodels().desk.notes("n1")).document).toBeTruthy(), {
        timeout: 5000,
      });
      await app.startWorkflow(stamped, "n1");
      await settled("stamped", "n1", "running");
      await app.sendSignal("n1", stamped.signals.go());
      await settled("stamped", "n1", "completed");
      expect((await app.queryWorkflow(stamped, "n1"))!.result).toEqual({
        id: "n1",
        at: new Date(outcome.events[0]!.timestamp).toISOString(),
      });
    } finally {
      await app.close();
    }
  });

  test(`${adapter}: a restart mid-wait resumes from the journal without repeating finished steps`, async () => {
    const store = await persistence();
    const first = await fixture({ counter }, { store });
    await first.api.start("counter", "c1");
    await first.api.signal("counter", "c1", "add", 2);
    await vi.waitFor(async () => expect(await store.readStream(stream("notes"))).toHaveLength(1), { timeout: 5000 });
    await first.app.close();
    const second = await fixture({ counter }, { store });
    try {
      await second.api.signal("counter", "c1", "add", 3);
      await second.api.signal("counter", "c1", "add", 0);
      await second.settled("counter", "c1", "completed");
      expect((await second.api.query("counter", "c1"))!.result).toBe(5);
      // The replayed tell after the restart was not sent again.
      await vi.waitFor(async () =>
        expect((await store.readStream(stream("notes"))).map((event) => event.payload)).toEqual([
          { total: 2 },
          { total: 5 },
        ]),
      );
    } finally {
      await second.app.close();
    }
  });

  test(`${adapter}: a replica takes over the Lease and fires the Deadline the first owner set`, async () => {
    const store = await persistence();
    const napper: WorkflowConfig = {
      version: 1,
      async run(ctx) {
        await ctx.sleep("nap", 300);
        return "rested";
      },
    };
    const first = await fixture({ napper }, { store });
    await first.api.start("napper", "n1");
    await first.settled("napper", "n1", "running");
    // Running beside the owner, it waits for the Lease.
    const second = await fixture({ napper }, { store });
    try {
      await first.app.close();
      await second.settled("napper", "n1", "completed");
      expect((await second.api.query("napper", "n1"))!.result).toBe("rested");
    } finally {
      await second.app.close();
    }
  });

  test(`${adapter}: a drifted instance is parked and its wakes dead-lettered while other instances run on`, async () => {
    const store = await persistence();
    const body = (step: string): WorkflowConfig => ({
      version: 1,
      signals: { go: {}, done: {} },
      async run(ctx, input) {
        await ctx.waitFor({ kind: "signal", name: "go" });
        await ctx.tell(step, { stream: stream(input.id), name: step.split(".")[2], payload: { text: step } });
        await ctx.waitFor({ kind: "signal", name: "done" });
        return step;
      },
    });
    const first = await fixture({ flow: body("crm.file.note") }, { store });
    for (const id of ["a", "b"]) await first.api.start("flow", id, { id });
    await first.api.signal("flow", "a", "go");
    await vi.waitFor(async () => expect(await store.readStream(stream("a"))).toHaveLength(1), { timeout: 5000 });
    await first.app.close();
    // A deploy renames the step instance a already finished, under the same version.
    const second = await fixture({ flow: body("crm.file.open") }, { store });
    try {
      await second.api.signal("flow", "a", "done");
      await second.settled("flow", "a", "drifted");
      await vi.waitFor(async () =>
        expect((await store.deadLetters()).map((letter) => letter.error)).toEqual([
          expect.stringMatching(/^workflow-drifted: new step while finished tell:crm\.file\.note#0 not reached/),
        ]),
      );
      // Instance b never reached the renamed step: it runs the new code, and the Partition is not halted.
      await second.api.signal("flow", "b", "go");
      await second.api.signal("flow", "b", "done");
      await second.settled("flow", "b", "completed");
      expect((await second.api.query("flow", "b"))!.result).toBe("crm.file.open");
      expect((await store.consumers()).filter((status) => status.state === "halted")).toEqual([]);
    } finally {
      await second.app.close();
    }
  });

  test(`${adapter}: instances keep their pinned version, new ones take the highest, and a body exception fails one`, async () => {
    const store = await persistence();
    const version = (n: number): WorkflowConfig & { version: number } => ({
      version: n,
      signals: { go: {} },
      async run(ctx, input) {
        await ctx.waitFor({ kind: "signal", name: "go" });
        if (input?.boom) throw new Error("boom");
        return `v${n}`;
      },
    });
    const first = await fixture({ pinned: version(1) }, { store });
    await first.api.start("pinned", "old");
    await first.settled("pinned", "old", "running");
    await first.app.close();
    const second = await fixture({ pinned: { versions: [version(1), version(2)] } }, { store });
    try {
      for (const id of ["new", "bad", "waiting"]) await second.api.start("pinned", id, { boom: id === "bad" });
      for (const id of ["old", "new", "bad"]) await second.api.signal("pinned", id, "go");
      await second.settled("pinned", "old", "completed");
      await second.settled("pinned", "new", "completed");
      await second.settled("pinned", "bad", "failed");
      expect((await second.api.query("pinned", "old"))!.result).toBe("v1");
      expect((await second.api.query("pinned", "new"))!.result).toBe("v2");
      // A Signal to a finished instance dead-letters as `workflow-missing`.
      await second.api.signal("pinned", "old", "go");
      await vi.waitFor(async () =>
        expect((await store.deadLetters()).map((letter) => letter.error)).toEqual(["workflow-missing"]),
      );
    } finally {
      await second.app.close();
    }
    // A deploy without the version an instance pinned parks it.
    const third = await fixture({ pinned: version(3) }, { store });
    try {
      await third.settled("pinned", "waiting", "running");
      await third.api.signal("pinned", "waiting", "go");
      await third.settled("pinned", "waiting", "drifted");
      expect((await store.loadWorkflow!("workflow:pinned", "waiting"))!.error).toBe("version 2 is not registered");
    } finally {
      await third.app.close();
    }
  });

  test(`${adapter}: a restart takes the highest registered version, under a fresh journal`, async () => {
    const store = await persistence();
    const looper = (n: number): WorkflowConfig & { version: number } => ({
      version: n,
      signals: { go: {} },
      async run(ctx, input) {
        await ctx.waitFor({ kind: "signal", name: "go" });
        if (n === 1) ctx.restart("again");
        return `v${n} ${input}`;
      },
    });
    const first = await fixture({ looping: looper(1) }, { store });
    await first.api.start("looping", "l1", "first");
    await first.settled("looping", "l1", "running");
    await first.app.close();
    const second = await fixture({ looping: { versions: [looper(1), looper(2)] } }, { store });
    try {
      await second.api.signal("looping", "l1", "go");
      await vi.waitFor(async () => expect((await store.loadWorkflow!("workflow:looping", "l1"))!.version).toBe(2), {
        timeout: 5000,
      });
      await second.api.signal("looping", "l1", "go");
      await second.settled("looping", "l1", "completed");
      expect((await second.api.query("looping", "l1"))!.result).toBe("v2 again");
    } finally {
      await second.app.close();
    }
  });

  test(`${adapter}: Events route by every registered version's correlate, and a Signal checks the instance's pinned version`, async () => {
    const store = await persistence();
    const v1: WorkflowConfig & { version: number } = {
      version: 1,
      correlate: { "crm.file.closed": (event) => event.aggregate.id },
      signals: { go: {} },
      async run(ctx) {
        await ctx.waitFor({ kind: "signal", name: "go" });
        const closed = await ctx.waitFor({ kind: "event", name: "crm.file.closed" });
        return `v1 ${closed.payload.reason}`;
      },
    };
    const v2: WorkflowConfig & { version: number } = {
      version: 2,
      signals: { proceed: {} },
      run: async (ctx) => ctx.waitFor({ kind: "signal", name: "proceed" }),
    };
    const first = await fixture({ routed: v1 }, { store });
    await first.api.start("routed", "f1");
    await first.settled("routed", "f1", "running");
    await first.app.close();
    const second = await fixture({ routed: { versions: [v1, v2] } }, { store });
    try {
      await expect(second.api.signal("routed", "f1", "proceed")).rejects.toThrow(/no Signal proceed/);
      // An unknown instance takes a Signal any registered version declares; it dead-letters later.
      await second.api.signal("routed", "nobody", "go");
      await second.api.signal("routed", "f1", "go");
      await second.app.sendCommand({ stream: stream("f1"), name: "close", payload: { reason: "paid" } });
      await second.settled("routed", "f1", "completed");
      expect((await second.api.query("routed", "f1"))!.result).toBe("v1 paid");
    } finally {
      await second.app.close();
    }
  });

  test(`${adapter}: a start to a finished id is a Rejection to its caller, and dead-letters from ctx.start or a recorded start`, async () => {
    const workflows: Record<string, WorkflowConfig> = {
      child: { version: 1, signals: { go: {} }, run: async (_ctx, input) => input },
      parent: {
        version: 1,
        async run(ctx, input) {
          await ctx.start("child", "c1", input.value, input.reuse ? { reuse: true } : undefined);
          return "started";
        },
      },
    };
    const { app, api, store, settled } = await fixture(workflows);
    const letters = async () => (await store.deadLetters()).map((letter) => letter.error).sort();
    try {
      await api.start("child", "c1", "first");
      await settled("child", "c1", "completed");
      await expect(api.start("child", "c1", "again")).rejects.toMatchObject({
        name: "Rejection",
        code: "workflow-finished",
      });
      await api.start("parent", "p1", { value: "late" });
      await settled("parent", "p1", "completed");
      await vi.waitFor(async () => expect(await letters()).toEqual(["workflow-finished"]), { timeout: 5000 });
      // A recorded start that carries a Signal (a Saga's) loses that Signal: `workflow-missing`.
      await api.send("child", "start", { input: null, reuse: false, signal: { name: "go", payload: 1 } }, { to: "c1" });
      await vi.waitFor(async () => expect(await letters()).toEqual(["workflow-finished", "workflow-missing"]), {
        timeout: 5000,
      });
      await api.start("parent", "p2", { value: "reused", reuse: true });
      await vi.waitFor(async () => expect((await api.query("child", "c1"))!.result).toBe("reused"), { timeout: 5000 });
    } finally {
      await app.close();
    }
  });

  /** Runs `event-fabric workflows ...` against `store`, with a config that declares `workflows`. */
  const cli = async (store: Persistence, workflows: Record<string, WorkflowConfig>, args: string[]) => {
    const folder = await mkdtemp(join(tmpdir(), "event-fabric-workflows-cli-")),
      key = `workflowsCli${Math.random().toString(36).slice(2)}`,
      output: string[] = [];
    // The CLI closes what it loaded; the test keeps using the store.
    (globalThis as any)[key] = {
      name: "workflows-cli",
      store: { ...store, close: async () => {} },
      aggregates: [file],
      transport: createInMemoryTransport(),
      workflows,
      logger: pino({ level: "silent" }),
    };
    try {
      await writeFile(join(folder, "config.mjs"), `export const config = () => globalThis.${key};`);
      const code = await runCLI(["workflows", ...args, "--config", "./config.mjs"], {
        cwd: folder,
        out: (text) => void output.push(text),
        err: (text) => void output.push(text),
      });
      return { code, output: output.join("\n") };
    } finally {
      delete (globalThis as any)[key];
      await rm(folder, { recursive: true, force: true });
    }
  };

  test(`${adapter}: workflows cancel ends a waiting instance, whose body sees WorkflowCancelled at its await`, async () => {
    const cancellable: WorkflowConfig = {
      version: 1,
      signals: { go: {} },
      async run(ctx, input) {
        try {
          await ctx.waitFor({ kind: "signal", name: "go" });
          return "went";
        } catch (error) {
          if (error instanceof WorkflowCancelled)
            await ctx.tell("crm.file.note", { stream: stream(input.id), name: "note", payload: { text: "cleaned up" } });
          throw error;
        }
      },
    };
    const { app, store, settled } = await fixture({ cancellable });
    try {
      await app[workflowsApi].start("cancellable", "w1", { id: "w1" });
      await settled("cancellable", "w1", "running");
      expect(await cli(store, { cancellable }, ["cancel", "cancellable", "w1"])).toEqual({
        code: 0,
        output: "Cancel requested for cancellable w1",
      });
      await settled("cancellable", "w1", "cancelled");
      await vi.waitFor(async () =>
        expect((await store.readStream(stream("w1"))).map((event) => event.payload)).toEqual([{ text: "cleaned up" }]),
      );
      expect(await cli(store, { cancellable }, ["cancel", "cancellable", "w1"])).toEqual({
        code: 1,
        output: "Workflow cancellable w1 is not running",
      });
    } finally {
      await app.close();
    }
  });

  test(`${adapter}: workflows resume runs a drifted instance again and replays its dead-lettered wakes; cancel ends one that stays drifted`, async () => {
    const store = await persistence();
    const version = (n: number): WorkflowConfig & { version: number } => ({
      version: n,
      signals: { go: {} },
      async run(ctx) {
        const value = await ctx.waitFor({ kind: "signal", name: "go" });
        return `v${n} ${value}`;
      },
    });
    const first = await fixture({ flow: version(1) }, { store });
    for (const id of ["a", "b"]) await first.api.start("flow", id);
    await first.settled("flow", "a", "running");
    await first.settled("flow", "b", "running");
    await first.app.close();
    // A deploy that dropped version 1 parks both instances on their next wake.
    const second = await fixture({ flow: version(2) }, { store });
    try {
      for (const id of ["a", "b"]) await second.api.signal("flow", id, "go", id);
      await second.settled("flow", "a", "drifted");
      await second.settled("flow", "b", "drifted");
    } finally {
      await second.app.close();
    }
    const fixed = { flow: { versions: [version(1), version(2)] } };
    const third = await fixture(fixed, { store });
    try {
      expect(await cli(store, fixed, ["resume", "flow", "a"])).toEqual({ code: 0, output: "Resume requested for flow a" });
      await third.settled("flow", "a", "completed");
      expect((await third.api.query("flow", "a"))!.result).toBe("v1 a");
      expect(await cli(store, fixed, ["resume", "flow", "a"])).toEqual({
        code: 1,
        output: "Workflow flow a is not running",
      });
      // b's dead letter stays until b is resumed; cancel ends b without running its body.
      expect((await store.deadLetters()).map((letter) => letter.sagaMessage?.instance)).toEqual(["b"]);
      expect(await cli(store, fixed, ["cancel", "flow", "b"])).toMatchObject({ code: 0 });
      await third.settled("flow", "b", "cancelled");
    } finally {
      await third.app.close();
    }
  });

  test(`${adapter}: workflows list filters by status, inspect shows the journal, purge drops instances finished before a date`, async () => {
    const workflows = { greeter: { version: 1, signals: { name: {} }, run: async (ctx: any) => ctx.waitFor({ kind: "signal", name: "name" }) } };
    const { app, store, api, settled } = await fixture(workflows);
    try {
      for (const id of ["done", "waiting"]) await api.start("greeter", id);
      await api.signal("greeter", "done", "name", "Ada");
      await settled("greeter", "done", "completed");
      await settled("greeter", "waiting", "running");
      const list = async (...args: string[]) => {
        const { code, output } = await cli(store, workflows, ["list", ...args]);
        expect(code).toBe(0);
        return JSON.parse(output);
      };
      expect(await list("--status", "running")).toEqual([
        { group: "workflow:greeter", id: "waiting", version: 1, status: "running", startedAt: expect.any(String) },
      ]);
      expect((await list()).map((row: any) => row.id)).toEqual(["done", "waiting"]);
      expect(await cli(store, workflows, ["list", "--status", "asleep"])).toEqual({
        code: 1,
        output: "--status takes running, completed, failed, cancelled or drifted",
      });
      const inspected = await cli(store, workflows, ["inspect", "greeter", "waiting"]);
      expect(JSON.parse(inspected.output)).toMatchObject({
        id: "waiting",
        status: "running",
        journal: { "signal:name#0": { s: "pending" } },
      });
      expect(await cli(store, workflows, ["inspect", "greeter", "nobody"])).toEqual({
        code: 1,
        output: "No Workflow greeter nobody",
      });
      // Only finished instances go, and only those finished before the date.
      expect(await cli(store, workflows, ["purge", "--finished-before", "2000-01-01"])).toEqual({
        code: 0,
        output: "Purged 0 Workflow instances",
      });
      expect(await cli(store, workflows, ["purge", "--finished-before", new Date(Date.now() + 60_000).toISOString()])).toEqual({
        code: 0,
        output: "Purged 1 Workflow instances",
      });
      expect((await list()).map((row: any) => row.id)).toEqual(["waiting"]);
      expect(await cli(store, workflows, ["purge"])).toMatchObject({ code: 1 });
    } finally {
      await app.close();
    }
  });

  test(`${adapter}: a Worker's reply to an earlier run of a reused id is dropped`, async () => {
    const gates: Array<(value: string) => void> = [];
    const slow = worker(lookup, { execute: () => new Promise<string>((resolve) => gates.push(resolve)) });
    const asking: WorkflowConfig = {
      version: 1,
      async run(ctx, input) {
        const reply = await ctx.ask("lookup", { issue: input });
        return reply.value;
      },
    };
    const { app, api, settled } = await fixture({ asking }, { workers: { lookup: slow } });
    try {
      await api.start("asking", "a1", "first");
      await vi.waitFor(() => expect(gates).toHaveLength(1), { timeout: 5000 });
      await api.cancel("asking", "a1");
      await settled("asking", "a1", "cancelled");
      await api.start("asking", "a1", "second", { reuse: true });
      await settled("asking", "a1", "running");
      // The first run's Worker answers only now, while the second run waits on the same step.
      gates[0]!("old");
      await vi.waitFor(() => expect(gates).toHaveLength(2), { timeout: 5000 });
      await new Promise((resolve) => setTimeout(resolve, 300));
      expect(await api.query("asking", "a1")).toMatchObject({ status: "running" });
      gates[1]!("new");
      await settled("asking", "a1", "completed");
      expect((await api.query("asking", "a1"))!.result).toBe("new");
    } finally {
      for (const open of gates) open("closing");
      await app.close();
    }
  });

  test(`${adapter}: arguments read back from the store in another key order do not drift`, async () => {
    const { app, api, settled } = await fixture({
      // The first run sees the Event as the runtime builds it; a replay sees it as the store keeps it.
      ordered: {
        version: 1,
        start: "crm.file.opened",
        id: (event) => event.aggregate.id,
        signals: { go: {} },
        async run(ctx, opened) {
          await ctx.start("noop", `n-${ctx.id}`, opened);
          await ctx.waitFor({ kind: "signal", name: "go" });
          return "done";
        },
      },
      noop: { version: 1, run: async () => "ok" },
    });
    try {
      await app.sendCommand({ stream: stream("o1"), name: "open", payload: { zz: 1, a: 2 } });
      await settled("ordered", "o1", "running");
      await api.signal("ordered", "o1", "go");
      await settled("ordered", "o1", "completed");
    } finally {
      await app.close();
    }
  });

  test(`${adapter}: a restart into a newer version reads what that version declares`, async () => {
    const desk = {
      models: {
        notes: {
          schema: Type.Object({ text: Type.String() }),
          reactions: { "crm.issue.noted": (event: any, vm: any) => vm.set({ text: event.payload.text }) },
        },
      },
    };
    const store = await persistence();
    const v1: WorkflowConfig & { version: number } = {
      version: 1,
      signals: { go: {} },
      async run(ctx) {
        await ctx.waitFor({ kind: "signal", name: "go" });
        ctx.restart("again");
      },
    };
    const v2: WorkflowConfig & { version: number } = {
      version: 2,
      reads: ["desk"],
      signals: { go: {} },
      async run(ctx) {
        const note = await ctx.read("desk", "notes", ctx.id);
        await ctx.waitFor({ kind: "signal", name: "go" });
        return note?.text;
      },
    };
    const first = await fixture({ reading: v1 }, { store });
    await first.api.start("reading", "r1");
    await first.settled("reading", "r1", "running");
    await first.app.close();
    const second = await fixture({ reading: { versions: [v1, v2] } }, { store, readmodels: { desk } });
    try {
      await second.app.sendCommand(domain.crm.issue.note("r1", { text: "hi" }));
      await vi.waitFor(async () => expect((await second.app.getReadmodels().desk.notes("r1")).document).toBeTruthy(), {
        timeout: 5000,
      });
      await second.api.signal("reading", "r1", "go");
      await vi.waitFor(async () => expect((await store.loadWorkflow!("workflow:reading", "r1"))!.version).toBe(2), {
        timeout: 5000,
      });
      await second.api.signal("reading", "r1", "go");
      await second.settled("reading", "r1", "completed");
      expect((await second.api.query("reading", "r1"))!.result).toBe("hi");
    } finally {
      await second.app.close();
    }
  });

  test(`${adapter}: state and a result of null are kept, and Dates are stored as their ISO text`, async () => {
    const stamped = workflow("stamped", {
      version: 1,
      state: Type.Object({ at: DateTime() }),
      async run(ctx) {
        const now = await ctx.now();
        await ctx.set({ at: now });
        return now;
      },
    });
    const { app, api, settled } = await fixture({
      nothing: {
        version: 1,
        async run(ctx) {
          await ctx.set(null);
          return null;
        },
      },
      stamped,
    });
    try {
      await api.start("nothing", "z1");
      await settled("nothing", "z1", "completed");
      expect(await api.query("nothing", "z1")).toEqual({ status: "completed", state: null, result: null });
      await app.startWorkflow(stamped, "s1");
      await settled("stamped", "s1", "completed");
      const { state, result } = (await api.query("stamped", "s1"))!;
      expect(result).toMatch(/^\d{4}-\d\d-\d\dT/);
      expect(state).toEqual({ at: result });
    } finally {
      await app.close();
    }
  });
}
