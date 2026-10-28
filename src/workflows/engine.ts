import { uuidv7 } from "../id.js";
import { LateWorkflowCallError, WorkflowCancelled } from "../errors.js";
import { canonical, toJson } from "../validation.js";

/**
 * The Workflow replay engine: runs a body once per wake against its journal
 * and returns what the wake commits. No store, no Partition loop: the caller
 * loads the instance, calls `runWorkflow`, and commits the returned instance
 * and effects with its Checkpoint.
 */

/** What `waitFor` waits on: a Signal by name or a correlated Event by type. */
export type WorkflowHandle = { kind: "signal" | "event"; name: string };
/** Where the inbox buffers what `waitFor(handle)` reads: `signal:<name>` or `event:<type>`. */
export const inboxKey = ({ kind, name }: WorkflowHandle) => `${kind}:${name}`;
/** The step names of `sleep(name)` and `ask(message)`; a `waitFor` step is named by its `inboxKey`. */
export const stepName = {
  sleep: (name: string) => `sleep:${name}`,
  ask: (message: string) => `ask:${message}`,
};
/** A journal key: the step's name and its occurrence in the run, from 0. Deadlines are named after it. */
export const stepKey = (name: string, occurrence: number) => `${name}#${occurrence}`;

/**
 * One journal entry, keyed `name#n`. `a` is the JSON of the step's arguments
 * (`tell`, `queue`, `ask`, `start`, `read`), compared on replay; `at` is the
 * due time of a pending step's Deadline. The keys are short on purpose: every
 * instance's journal is stored as JSON, one entry per step.
 */
export type WorkflowStep =
  | { s: "done"; v?: unknown; a?: string }
  | { s: "pending"; a?: string; at?: string }
  | { s: "cancelled"; a?: string };

export type WorkflowStatus =
  | "running"
  | "completed"
  | "failed"
  | "cancelled"
  | "drifted";

/** A Workflow instance as the store holds it; plain JSON. */
export type WorkflowInstance = {
  input: unknown;
  journal: Record<string, WorkflowStep>;
  /** Signals (`signal:<name>`) and correlated Events (`event:<type>`) not yet read, oldest first. */
  inbox: Record<string, unknown[]>;
  state?: unknown;
  status: WorkflowStatus;
  result?: unknown;
  error?: string;
  cancelRequested?: boolean;
  /**
   * Names this run: a new instance, a `restart` and a reused id each get a new one. A Worker's reply
   * carries it, so a reply to an earlier run is dropped. Older instances have none.
   */
  run?: string;
};

/** Why the body runs this time. `step` names the journal key a Deadline or reply belongs to. */
export type WorkflowWake =
  | { kind: "start"; at: Date }
  | { kind: "signal" | "event"; name: string; value: unknown; at: Date }
  | { kind: "deadline"; step: string; at: Date }
  | { kind: "reply"; step: string; value: unknown; at: Date }
  | { kind: "cancel"; at: Date }
  /** An operator resumed the instance: the body replays and reads what its inbox gathered. */
  | { kind: "resume"; at: Date };

/** What a wake asks the runtime to do once it commits. Deadlines are named after their step. */
export type WorkflowEffect =
  | { type: "tell"; step: string; command: string; message: unknown }
  /** `run` is the instance's run, so each restart in one wake queues under its own key. */
  | { type: "queue"; step: string; command: string; message: unknown; run?: string }
  | { type: "ask"; step: string; name: string; message: unknown; run?: string }
  | { type: "start"; step: string; name: string; id: string; input: unknown; reuse?: boolean }
  | { type: "deadline"; step: string; at: Date }
  | { type: "cancelDeadline"; step: string };

/** When a wait times out: `timeout` ms after the step first ran, or at the absolute time `at`. */
export type WorkflowDue = { timeout?: number; at?: string | Date };
/** The engine's untyped `ctx`; `event-fabric/workflows` wraps it in the authored `WorkflowContext`. */
export type EngineContext = {
  /** The instance id. */
  readonly id: string;
  /** Resolves with the oldest buffered value; with a `timeout` or `at`, `{ value }` or `{ timedOut: true }`. */
  waitFor(handle: WorkflowHandle, options?: WorkflowDue): Promise<any>;
  /** Waits `ms` milliseconds, or until the absolute time given as a string or Date. */
  sleep(name: string, msOrAt: number | string | Date): Promise<void>;
  /** Resolves with the Worker's reply; with a `timeout`, `{ outcome: "timedOut" }` once it passes. */
  ask(
    name: string,
    message: unknown,
    options?: { timeout?: number },
  ): Promise<any>;
  read(group: string, model: string, id: string): Promise<any>;
  tell(command: string, message: unknown): Promise<void>;
  queue(command: string, message: unknown): Promise<void>;
  start(name: string, id: string, input: unknown, options?: { reuse?: boolean }): Promise<void>;
  now(): Promise<Date>;
  random(): Promise<number>;
  uuid(): Promise<string>;
  set(view: unknown): Promise<void>;
  fail(reason: string): never;
  cancel(): never;
  restart(input: unknown): never;
};

export type WorkflowBody = (ctx: EngineContext, input: any) => unknown;

export type WorkflowOptions = {
  /** The instance id the body sees as `ctx.id`. */
  id?: string;
  reader?: (group: string, model: string, id: string) => Promise<unknown>;
  /** Turns a document `read` loaded, or the journal's JSON of it on replay, into what the body sees. */
  revive?: (group: string, model: string, document: unknown) => Promise<unknown>;
  /** Called for each ctx call after the run was judged suspended; the call throws `LateWorkflowCallError`. */
  onLate?: (step: string) => void;
  /** The body a `restart` runs its fresh journal under (the highest registered version); default the same body. */
  restartBody?: WorkflowBody;
};

export type WorkflowRun = {
  instance: WorkflowInstance;
  effects: WorkflowEffect[];
  /** Pending steps the final run reached. */
  pending: string[];
  /** Why the instance drifted. */
  reason?: string;
  /** Inbox entries dropped at the end. */
  droppedSignals?: number;
  restarts: number;
};

const maxRestarts = 100;

class End extends Error {} // unwinds the body after fail, cancel or restart
type Act = { done: unknown } | { wait: Date | undefined };
type Settled = { ok: unknown } | { err: unknown } | undefined;

// A fresh promise each time: one shared promise would keep every suspended run's continuation alive.
const never = () => new Promise<never>(() => {});
const immediate = () => new Promise<void>((resolve) => setImmediate(resolve));
const isFinished = (step: WorkflowStep) => step.s !== "pending";

export const newWorkflowInstance = (input?: unknown, at = new Date()): WorkflowInstance => ({
  input,
  journal: {},
  inbox: {},
  status: "running",
  run: uuidv7(at.getTime()),
});
/** Arguments as compared on replay: JSON with sorted keys, as a store may reorder them. */
const argumentsOf = (args: unknown) =>
  args === undefined ? undefined : canonical(JSON.parse(JSON.stringify(args)));
// An older journal holds plain JSON.stringify output.
const sameArguments = (recorded: string | undefined, a: string | undefined) =>
  recorded === a || (recorded !== undefined && a !== undefined && canonical(JSON.parse(recorded)) === a);

/**
 * Runs `body` for one wake. Recorded steps resolve from the journal; new ones
 * act on `wake`. The run is judged suspended one macrotask after it started
 * (or after its last new steps ran): a body that awaits only ctx calls has
 * settled every microtask by then. New steps wait for that boundary, so drift
 * is checked once every finished step this code still reaches was reached.
 */
export async function runWorkflow(
  body: WorkflowBody,
  instance: WorkflowInstance,
  wake: WorkflowWake,
  options: WorkflowOptions = {},
  restarts = 0,
): Promise<WorkflowRun> {
  const inst = structuredClone(instance);
  const effects: WorkflowEffect[] = [];
  const reached = new Set<string>();
  const counts = new Map<string, number>();
  const queued: Array<() => Promise<void>> = []; // new steps held for the next boundary
  let live = false; // false while replaying
  let closed = false;
  let end:
    | { status: WorkflowStatus; error?: string; restart?: { input: unknown } }
    | undefined;
  let drift: string | undefined;
  const after = (ms: number) => new Date(wake.at.getTime() + ms);
  const due = ({ timeout, at }: WorkflowDue) =>
    at !== undefined ? new Date(at) : timeout !== undefined ? after(timeout) : undefined;

  if (wake.kind === "signal" || wake.kind === "event")
    (inst.inbox[inboxKey(wake)] ??= []).push(wake.value);
  if (wake.kind === "cancel") inst.cancelRequested = true;

  // Records the outcome of a new or pending step; undefined leaves it pending.
  function settle(
    key: string,
    a: string | undefined,
    act: (key: string, isNew: boolean) => Act,
    isNew: boolean,
  ): Settled {
    const prior = inst.journal[key];
    const cancelDeadline = () => {
      if (prior?.s === "pending" && prior.at !== undefined)
        effects.push({ type: "cancelDeadline", step: key });
    };
    if (inst.cancelRequested) {
      // Recorded at this step, so a replay throws at the same point.
      delete inst.cancelRequested;
      inst.journal[key] = { s: "cancelled", a };
      cancelDeadline();
      return { err: new WorkflowCancelled(`cancelled at ${key}`) };
    }
    const outcome = act(key, isNew);
    if ("done" in outcome) {
      inst.journal[key] = { s: "done", v: outcome.done, a };
      cancelDeadline();
      return { ok: structuredClone(outcome.done) };
    }
    if (isNew) {
      inst.journal[key] = { s: "pending", a, at: outcome.wait?.toISOString() };
      if (outcome.wait)
        effects.push({ type: "deadline", step: key, at: outcome.wait });
    }
    return undefined;
  }
  const toPromise = (s: Settled): Promise<any> =>
    s === undefined
      ? never()
      : "ok" in s
        ? Promise.resolve(s.ok)
        : Promise.reject(s.err);

  function step(
    name: string,
    args: unknown,
    act: (key: string, isNew: boolean) => Act,
    load?: () => Promise<unknown>,
  ): Promise<any> {
    if (closed) {
      options.onLate?.(name);
      throw new LateWorkflowCallError(
        `ctx call ${name} after the run was judged suspended: the body awaited a non-ctx promise`,
      );
    }
    if (end) throw new End();
    const n = counts.get(name) ?? 0;
    counts.set(name, n + 1);
    const key = stepKey(name, n);
    reached.add(key);
    const a = argumentsOf(args);
    const recorded = inst.journal[key];
    if (recorded) {
      if (!sameArguments(recorded.a, a)) {
        drift ??= `${key} was recorded with other arguments`;
        return never();
      }
      // A copy, so the body's changes to it never reach the journal.
      if (recorded.s === "done") return Promise.resolve(structuredClone(recorded.v));
      if (recorded.s === "cancelled")
        return Promise.reject(new WorkflowCancelled(`cancelled at ${key}`));
      return toPromise(settle(key, a, act, false));
    }
    if (live && !load) return toPromise(settle(key, a, act, true));
    // The engine's own I/O (`read`) runs between boundaries, never inside a ctx call.
    return new Promise((resolve, reject) =>
      queued.push(async () => {
        const loaded =
          load && !inst.cancelRequested ? { done: await load() } : undefined;
        let s: Settled;
        try {
          s = settle(key, a, loaded ? () => loaded : act, true);
        } catch (error) {
          s = { err: error }; // e.g. `set` with a value that is not JSON: the body sees it
        }
        if (s && "ok" in s) resolve(s.ok);
        else if (s) reject(s.err);
      }),
    );
  }

  const thisRun = () => (inst.run === undefined ? {} : { run: inst.run });
  const deadlineFired = (key: string) =>
    wake.kind === "deadline" && wake.step === key;
  const send = (type: "tell" | "queue", command: string, message: unknown) =>
    step(`${type}:${command}`, message, (key) => {
      effects.push(
        type === "queue" ? { type, step: key, command, message, ...thisRun() } : { type, step: key, command, message },
      );
      return { done: undefined };
    });

  const ctx: EngineContext = {
    id: options.id ?? "",
    waitFor(handle, timing: WorkflowDue = {}) {
      const timed = timing.timeout !== undefined || timing.at !== undefined;
      const box = inboxKey(handle);
      return step(box, undefined, (key, isNew) => {
        const queue = inst.inbox[box];
        if (queue?.length) {
          const value = queue.shift();
          if (!queue.length) delete inst.inbox[box];
          return { done: timed ? { value } : value };
        }
        if (deadlineFired(key)) return { done: { timedOut: true } };
        return { wait: isNew ? due(timing) : undefined };
      });
    },
    sleep(name, msOrAt) {
      const timing = typeof msOrAt === "number" ? { timeout: msOrAt } : { at: msOrAt };
      return step(stepName.sleep(name), undefined, (key, isNew) =>
        deadlineFired(key)
          ? { done: undefined }
          : { wait: isNew ? due(timing) : undefined },
      );
    },
    ask(name, message, { timeout }: { timeout?: number } = {}) {
      return step(stepName.ask(name), message, (key, isNew) => {
        if (wake.kind === "reply" && wake.step === key)
          return { done: wake.value };
        if (deadlineFired(key)) return { done: { outcome: "timedOut" } };
        if (isNew) effects.push({ type: "ask", step: key, name, message, ...thisRun() });
        return {
          wait: isNew && timeout !== undefined ? after(timeout) : undefined,
        };
      });
    },
    read(group, model, id) {
      const reader = options.reader ?? (async () => undefined);
      const read = step(
        `read:${group}.${model}`,
        id,
        () => ({ done: undefined }),
        () => reader(group, model, id),
      );
      // The journal holds JSON, so a first run and a replay both see the revived document.
      const revive = options.revive;
      return revive
        ? read.then((document) =>
            document == null ? document : revive(group, model, document),
          )
        : read;
    },
    tell: (command, message) => send("tell", command, message),
    queue: (command, message) => send("queue", command, message),
    start(name, id, input, { reuse }: { reuse?: boolean } = {}) {
      // `reuse` is recorded only when set, so journals of starts without it replay unchanged.
      return step(`start:${name}`, reuse ? { id, input, reuse } : { id, input }, (key) => {
        effects.push({ type: "start", step: key, name, id, input, ...(reuse ? { reuse } : {}) });
        return { done: undefined };
      });
    },
    // Journaled as ISO text, so the instance stays plain JSON.
    now: () =>
      step("now", undefined, () => ({ done: wake.at.toISOString() })).then(
        (iso: string) => new Date(iso),
      ),
    random: () => step("random", undefined, () => ({ done: Math.random() })),
    uuid: () =>
      step("uuid", undefined, () => ({ done: uuidv7(wake.at.getTime()) })),
    set(view) {
      return step("set", undefined, () => {
        inst.state = view === undefined ? undefined : toJson(view, "Workflow state");
        return { done: undefined };
      });
    },
    fail(reason) {
      end ??= { status: "failed", error: reason };
      throw new End();
    },
    cancel() {
      end ??= { status: "cancelled" };
      throw new End();
    },
    restart(input) {
      end ??= { status: "running", restart: { input } };
      throw new End();
    },
  };

  let outcome: Settled;
  void (async () => body(ctx, structuredClone(inst.input)))().then(
    (value) => (outcome = { ok: value }),
    (error) => (outcome = { err: error }),
  );

  for (;;) {
    await immediate();
    if (drift || outcome || end || !queued.length) break;
    if (!live) {
      const lost = Object.keys(inst.journal).filter(
        (k) => isFinished(inst.journal[k]!) && !reached.has(k),
      );
      if (lost.length) {
        drift = `new step while finished ${lost.join(", ")} not reached`;
        break;
      }
      live = true;
    }
    for (const run of queued.splice(0)) await run();
  }
  closed = true;

  const pending = () =>
    [...reached].filter((k) => inst.journal[k]?.s === "pending");
  const parked = (reason: string): WorkflowRun => ({
    instance: { ...structuredClone(instance), status: "drifted" },
    effects: [],
    pending: pending(),
    reason,
    restarts,
  });
  const cancelDeadlines = () => {
    for (const [k, e] of Object.entries(inst.journal))
      if (e.s === "pending" && e.at !== undefined)
        effects.push({ type: "cancelDeadline", step: k });
  };

  if (drift) return parked(drift);

  if (end?.restart) {
    if (restarts < maxRestarts) {
      cancelDeadlines();
      const next = await runWorkflow(
        options.restartBody ?? body,
        { ...newWorkflowInstance(end.restart.input, wake.at), inbox: inst.inbox },
        { kind: "start", at: wake.at },
        options,
        restarts + 1,
      );
      return { ...next, effects: [...effects, ...next.effects] };
    }
    // A runtime retry would loop again; the body is at fault, so the instance fails.
    end = {
      status: "failed",
      error: `restart loop: ${maxRestarts} restarts in one wake`,
    };
  }

  if (end || outcome) {
    cancelDeadlines();
    const droppedSignals = Object.values(inst.inbox).reduce(
      (n, queue) => n + queue.length,
      0,
    );
    if (end) {
      inst.status = end.status;
      if (end.error !== undefined) inst.error = end.error;
    } else if (outcome && "ok" in outcome) {
      try {
        inst.status = "completed";
        if (outcome.ok !== undefined) inst.result = toJson(outcome.ok, "Workflow result");
      } catch (error) {
        inst.status = "failed";
        inst.error = String(error);
      }
    } else if (outcome?.err instanceof WorkflowCancelled) {
      inst.status = "cancelled";
    } else {
      inst.status = "failed";
      inst.error = String(outcome?.err);
    }
    return {
      instance: { ...inst, journal: {}, inbox: {} },
      effects,
      pending: [],
      droppedSignals,
      restarts,
    };
  }

  // Suspended. Pending steps this code no longer reaches are dropped with their Deadlines.
  for (const [k, e] of Object.entries(inst.journal))
    if (e.s === "pending" && !reached.has(k)) {
      delete inst.journal[k];
      if (e.at !== undefined) effects.push({ type: "cancelDeadline", step: k });
    }
  if (!pending().length)
    return parked(
      "suspended with no pending ctx step: the body awaited a non-ctx promise",
    );
  return { instance: inst, effects, pending: pending(), restarts };
}
