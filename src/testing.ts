import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import pino from "pino";
import { createInMemoryEventStore } from "./event-store/memory.js";
import { createInMemoryTransport } from "./transport/memory.js";
import { createMemoryReadmodelStore } from "./adapters/memory-readmodel.js";
import { compile } from "./authoring/index.js";
import { resolveComponents, type EventType } from "./authoring/decorators.js";
import { createApplication, type Application } from "./runtime/index.js";
import { workerReply } from "./runtime/delivery.js";
import { comparePositions } from "./position.js";
import type { CommandInput } from "./id.js";
import type { EventName, EventPayload, MessageName, MessagePayload } from "./registry.js";
import type {
  ApplicationConfig,
  DecoratedClass,
  ReadmodelGroupConfig,
  SagaDefinition,
  SagaMessageOptions,
  WorkflowConfig,
} from "./runtime/types.js";
import { inboxKey, stepKey, stepName, type WorkflowStatus } from "./workflows/engine.js";
import type { ResultOf, SentSignal, SignalInput, StartArgs, StartWorkflowOptions, WorkflowRef } from "./workflows/index.js";
import { askedMessage, workflowName, workflowsApi } from "./internal.js";
import type { AggregateDefinition } from "./kernel/index.js";
import type { MessageHandle } from "./authoring/aggregate.js";
import type { JsonObject, Message, OutboxEntry, Persistence, WorkerReply } from "./types.js";

export { createInMemoryEventStore } from "./event-store/memory.js";
export {
  createInMemoryBroker,
  createInMemoryTransport,
} from "./transport/memory.js";
export { createMemoryReadmodelStore } from "./adapters/memory-readmodel.js";
export {
  commandPartition,
  fnv1a,
  partitionFor,
} from "./transport/partition.js";

/**
 * A step for `given` and `when`: an Event recorded, a Saga Message sent, a Worker's reply, time
 * passing, or a Workflow instance started or Signalled.
 */
export type ProcessInput =
  | { kind: "event"; name: string; id: string; payload: unknown }
  | { kind: "sagaMessage"; name: string; payload: unknown; options: SagaMessageOptions }
  | { kind: "reply"; name: string; payload: WorkerReply }
  | { kind: "time"; ms: number }
  | { kind: "start"; workflow: string; id: string; input: unknown; options?: StartWorkflowOptions<any> }
  | { kind: "signal"; workflow: string; id: string; signal: SignalInput };
/**
 * Starts instance `id` of the Workflow under test, named by its Handle or name, as `startWorkflow`
 * does: the input and the options (`reuse` starts a finished id again, `signal` delivers one) are typed by it.
 */
export const start = <W extends WorkflowRef>(workflow: W, id: string, ...[input, options]: StartArgs<W>): ProcessInput => ({
  kind: "start",
  workflow: workflowName(workflow),
  id,
  input,
  options,
});
/** Sends instance `id` of the Workflow under test a Signal it declares, as `sendSignal` does. */
export const signal = (id: string, signal: SentSignal): ProcessInput => ({
  kind: "signal",
  workflow: signal.workflow,
  id,
  signal,
});
/**
 * The journal key of a Workflow step, which names its Deadline (`expectDeadline`) and the request its
 * Worker answers (`replyTo.step`). `occurrence` counts that step's earlier runs in the instance, from 0.
 */
export const workflowStep = {
  /** A `waitFor` of a correlated Event type or of a Signal Handle. */
  waitFor: (awaited: EventName | { kind: "signal"; name: string }, occurrence = 0) =>
    stepKey(inboxKey(typeof awaited === "string" ? { kind: "event", name: awaited } : awaited), occurrence),
  /** A `sleep(name, ...)`. */
  sleep: (name: string, occurrence = 0) => stepKey(stepName.sleep(name), occurrence),
  /** An `ask` of the Worker, as `worker(message, ...)` builds it. */
  ask: (worker: { handles: string[] }, occurrence = 0) => stepKey(stepName.ask(askedMessage(worker)), occurrence),
};
/** An Event of the Stream `id`, by qualified name or Event class, recorded at the fixture's clock. */
export function event<N extends EventName>(name: N, id: string, payload: EventPayload<N>): ProcessInput;
export function event<P>(type: EventType<string, P>, id: string, payload: P): ProcessInput;
export function event(name: string | EventType, id: string, payload: unknown): ProcessInput {
  return { kind: "event", name: typeof name === "string" ? name : name.type, id, payload };
}
/** A Saga Message to the Saga under test, as `application.sendSagaMessage` sends it. */
export const sagaMessage = (
  name: string,
  payload: unknown = {},
  options: SagaMessageOptions = {},
): ProcessInput => ({ kind: "sagaMessage", name, payload, options });
/**
 * The reply of the Worker of the oldest unanswered Message `name` the Saga queued with `replyTo`,
 * as the Worker runtime sends it after `execute` succeeds or the Message dead-letters.
 */
export function reply<N extends MessageName>(message: N | MessageHandle<N, any>, payload: WorkerReply): ProcessInput {
  return { kind: "reply", name: typeof message === "string" ? message : message.name, payload };
}
/**
 * The Outcome of a Command that must be accepted, narrowed to its accepted form; anything else fails
 * the test with the Outcome in the message: `const { events } = await accepted(app.sendCommand(input))`.
 */
export async function accepted<O extends { status: string }>(
  outcome: O | Promise<O>,
): Promise<Extract<O, { status: "accepted" }>> {
  const settled = await outcome;
  assert.equal(settled.status, "accepted", JSON.stringify(settled));
  return settled as Extract<O, { status: "accepted" }>;
}
/** Moves the fixture's clock by `ms`, handling every Saga Message that falls due on the way, at its due time. */
export const timePasses = (ms: number): ProcessInput => ({ kind: "time", ms });

type QueuedExpectation = {
  [N in MessageName]: { name: N; payload: MessagePayload<N> } & Record<string, unknown>;
}[MessageName];
export type ProcessFixtureOptions = {
  /** The Saga's name, as in `sagas`; it names the Consumer group `saga:<name>` and Idempotency keys. Default `process`. */
  name?: string;
  /** Where the clock starts; default now. */
  now?: string | Date;
  services?: Record<string, unknown>;
  /** Default: Aggregates declaring just the Events the Saga names. Pass them with `readmodels`. */
  aggregates?: ApplicationConfig["aggregates"];
  /** The Workers of the Messages the Saga queues; they never run, but queueing needs their declaration. */
  workers?: ApplicationConfig["workers"];
  /** The Readmodel groups the Saga `reads`, kept in memory. */
  readmodels?: Record<string, ReadmodelGroupConfig>;
  readmodelServices?: object;
};

/**
 * A given/when/then fixture for one Saga (object form or Decorated) or Workflow on the in-memory
 * store, with a virtual clock. `given` and `when` record Events, send Saga Messages, start or Signal
 * Workflow instances or let time pass, then let the Partitions run until nothing is due;
 * expectations check what the last `when` did. Every method chains, and awaiting the fixture runs
 * the chain. Dispose it, or `close()`, to stop it.
 */
export function processFixture<D extends SagaDefinition | DecoratedClass | WorkflowConfig>(
  definition: D,
  options: ProcessFixtureOptions = {},
) {
  const workflow: WorkflowConfig | undefined =
    typeof definition === "object" && ("run" in definition || "versions" in definition) ? definition : undefined;
  // A Workflow built by `workflow(name, ...)` runs under its own name.
  const versionName = workflow && ("versions" in workflow ? workflow.versions[0]?.name : workflow.name);
  const name = options.name ?? versionName ?? "process",
    kind = workflow ? "Workflow" : "Saga",
    group = `${workflow ? "workflow" : "saga"}:${name}`;
  let now = new Date(options.now ?? Date.now()).getTime();
  const clock = () => new Date(now);
  // `given` Events are the history the process under test reacts to, whatever its `from` says.
  const fromBeginning = { from: "beginning" } as const;
  const resolved = workflow
    ? undefined
    : { ...resolveComponents({ sagas: { [name]: definition } } as any).sagas![name]!, ...fromBeginning };
  const runs: WorkflowConfig | undefined =
    workflow &&
    ("versions" in workflow
      ? { versions: workflow.versions.map((version) => ({ ...version, ...fromBeginning })) }
      : { ...workflow, ...fromBeginning });
  // The highest version routes, as in the runtime.
  const latest = workflow && ("versions" in workflow ? workflow.versions : [workflow]).reduce((a, b) => (b.version > a.version ? b : a));
  const memory = createInMemoryEventStore({ clock });
  let committed: OutboxEntry[] = [],
    // Queued Messages awaiting a reply; Workers never run here, `reply` steps answer them.
    requests: Message[] = [],
    failures: string[] = [],
    instance: string | undefined;
  const commits = (changes: { id: string }[], outbox: OutboxEntry[]) => {
    committed.push(...outbox);
    for (const entry of outbox)
      if (entry.kind === "message" && (entry.message as Message).replyTo) requests.push(entry.message as Message);
    instance = changes.at(-1)?.id ?? instance;
  };
  const store: Persistence = {
    ...memory,
    async commitSaga(lease, position, changes, outbox, messages) {
      await memory.commitSaga(lease, position, changes, outbox, messages);
      commits(changes, outbox);
    },
    async commitWorkflow(lease, position, changes, outbox, messages) {
      await memory.commitWorkflow!(lease, position, changes, outbox, messages);
      commits(changes, outbox);
    },
  };
  const application: Application = createApplication({
    name: `${name}-fixture`,
    store,
    transport: createInMemoryTransport(),
    aggregates:
      options.aggregates ??
      stubAggregates(
        latest
          ? [latest.start ?? [], ...Object.keys(latest.correlate ?? {})].flat()
          : [...Object.keys(resolved!.reactions), ...Object.keys(resolved!.identity ?? {})],
      ),
    readmodelStore: createMemoryReadmodelStore(),
    readmodels: options.readmodels,
    readmodelServices: options.readmodelServices,
    ...(runs ? { workflows: { [name]: runs } } : { sagas: { [name]: resolved } }),
    workers: options.workers,
    services: options.services,
    clock,
    pollInterval: 10,
    // Handler failures retry quietly; a failed expectation lists them.
    logger: pino({ level: "warn" }, {
      write: (line: string) => {
        const { msg, error } = JSON.parse(line);
        failures.push(error ? `${msg}: ${error}` : msg);
      },
    }),
  } as ApplicationConfig);
  /** Every pending Saga Message of the Saga: due ones on a clock at the end of time. */
  const pending = () => {
    const at = now;
    now = 8.64e15;
    // The in-memory store reads its clock synchronously on entry.
    const read = memory.dueSagaMessages!(group, undefined, 10000);
    now = at;
    return read.then((due) => due.messages);
  };
  const settled = async () => {
    const head = await memory.head();
    for (let partition = 0; partition < ((latest ?? resolved)!.partitions ?? 1); partition++) {
      const checkpoint = await memory.checkpoint(group, partition);
      if (head && (!checkpoint || comparePositions(checkpoint, head) < 0)) return false;
    }
    return !(await memory.dueSagaMessages!(group, undefined, 1)).messages.length;
  };
  /** Lets the Saga's Partitions run until they have passed the log's end and nothing is due. */
  const settle = async () => {
    const until = Date.now() + 5000;
    while (!(await settled())) {
      if (Date.now() > until) {
        const halted = (await application.consumerStatus()).find((status) => status.error);
        throw new Error(
          `${kind} ${name} did not settle within 5 s${halted ? `: ${halted.error}` : ""}\n${failures.join("\n")}`,
        );
      }
      application.wake();
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
  };
  const apply = async (input: ProcessInput) => {
    if (input.kind === "event") {
      const [context, aggregate, eventName] = input.name.split(".");
      const stream = { context: context!, aggregate: aggregate!, id: input.id };
      const revision = (await memory.readStream(stream)).at(-1)?.revision ?? 0;
      const id = randomUUID();
      await memory.appendToStream(stream, revision, [
        {
          id,
          name: eventName!,
          payload: input.payload,
          metadata: {},
          schemaVersion: 1,
          timestamp: clock().toISOString(),
          commandId: id,
          correlationId: id,
        },
      ]);
    } else if (input.kind === "sagaMessage")
      await application.sendSagaMessage(name as never, input.name as never, input.payload as never, input.options);
    else if (input.kind === "reply") {
      const index = requests.findIndex((message) => message.name === input.name);
      if (index < 0) throw new Error(`No queued ${input.name} awaits a reply`);
      const sent = workerReply(requests.splice(index, 1)[0]!, input.payload)!;
      if (sent.workflow !== undefined) await application[workflowsApi].send(sent.workflow, sent.name, sent.payload, sent.options);
      else await application.sendSagaMessage(sent.saga as never, sent.name as never, sent.payload as never, sent.options);
    } else if (input.kind === "start" || input.kind === "signal") {
      if (input.workflow !== name) throw new Error(`The fixture runs ${kind} ${name}, not Workflow ${input.workflow}`);
      if (input.kind === "start") await application[workflowsApi].start(name, input.id, input.input, input.options);
      else await application[workflowsApi].signal(name, input.id, input.signal.name, input.signal.payload);
    } else {
      const target = now + input.ms;
      for (;;) {
        await settle();
        const { nextDelay } = await memory.dueSagaMessages!(group, undefined, 1);
        if (nextDelay === null || now + nextDelay > target) break;
        now += nextDelay;
      }
      now = target;
    }
    await settle();
  };
  const instanceId = (id?: string) => {
    if (id ?? instance) return (id ?? instance)!;
    throw new Error(`No ${kind} instance yet: pass the instance id`);
  };
  const loadWorkflow = (id?: string) => {
    if (!workflow) throw new Error("expectStatus and expectResult check a Workflow");
    return memory.loadWorkflow!(group, instanceId(id));
  };
  /** The effects the last `when` committed, each projected onto the keys its expectation names. */
  const effects = (kind: OutboxEntry["kind"], expected: object[]) =>
    committed
      .filter((entry) => entry.kind === kind)
      .map((entry, index) => {
        const message = entry.message as Message;
        // A Workflow's run is random, so expectations name its step without it.
        const { run: _run, ...replyTo } = (message.replyTo ?? {}) as { run?: string };
        const actual: Record<string, unknown> = { ...message, ...entry, ...(message.replyTo && { replyTo }) };
        return Object.fromEntries(Object.keys(expected[index] ?? actual).map((key) => [key, actual[key]]));
      });
  // Only the Saga and the groups it reads: no Outbox relay, so queued Messages never reach a Worker.
  let chain = application
    .start({ only: [group, ...Object.keys(options.readmodels ?? {}).map((name) => `readmodels:${name}`)] })
    .then(() => {});
  const fixture = {
    application,
    /** The in-memory Event store, on the fixture's clock. */
    store: memory,
    now: clock,
    given(...inputs: ProcessInput[]) {
      return step(async () => {
        for (const input of inputs) await apply(input);
      });
    },
    when(input: ProcessInput) {
      return step(async () => {
        committed = [];
        failures = [];
        await apply(input);
      });
    },
    /** The Commands the last `when` told, in order; each compared on the keys it names. */
    expectCommands(expected: CommandInput[]) {
      return check(async () => assert.deepStrictEqual(effects("command", expected), expected));
    },
    /** The Messages the last `when` queued for Workers, in order; each compared on the keys it names. */
    expectQueued(expected: QueuedExpectation[]) {
      return check(async () => assert.deepStrictEqual(effects("message", expected), expected));
    },
    /** The state of `instance` (default: the last one changed): a Saga's, `null` once destroyed, or a Workflow's `set` view. */
    expectState(state: JsonObject | null, instance?: string) {
      return check(async () => {
        if (workflow) {
          assert.deepStrictEqual((await loadWorkflow(instance))?.state ?? null, state);
          return;
        }
        const loaded = await memory.loadSaga(group, instanceId(instance));
        // `_id` is the instance id the expectation already names.
        const { _id, ...actual } = loaded ?? {};
        assert.deepStrictEqual(loaded && actual, state);
      });
    },
    /** The Workflow status of `instance` (default: the last one changed); `null` when it never started. */
    expectStatus(status: WorkflowStatus | null, instance?: string) {
      return check(async () => assert.deepStrictEqual((await loadWorkflow(instance))?.status ?? null, status));
    },
    /** The Workflow result of `instance` (default: the last one changed), typed by the Workflow; `undefined` until it completes. */
    expectResult(result: ResultOf<D> | undefined, instance?: string) {
      return check(async () => assert.deepStrictEqual((await loadWorkflow(instance))?.result, result));
    },
    /** The pending Deadline `name` of `instance` (default: the last one changed) falls due at `at`; a Workflow's is named after its step, e.g. `sleep:nap#0`. */
    expectDeadline(deadline: string, at: string | Date, instance?: string) {
      return check(async () => {
        const id = instanceId(instance),
          found = (await pending()).find((m) => m.instance === id && m.deadline === deadline);
        assert.deepStrictEqual(
          { instance: id, deadline, dueAt: found?.dueAt ?? null },
          { instance: id, deadline, dueAt: new Date(at).toISOString() },
        );
      });
    },
    expectNoDeadline(deadline: string, instance?: string) {
      return check(async () => {
        const id = instanceId(instance),
          found = (await pending()).find((m) => m.instance === id && m.deadline === deadline);
        assert.deepStrictEqual({ instance: id, deadline, dueAt: found?.dueAt ?? null }, {
          instance: id,
          deadline,
          dueAt: null,
        });
      });
    },
    then<T = void, E = never>(
      resolve?: ((value: void) => T | PromiseLike<T>) | null,
      reject?: ((reason: unknown) => E | PromiseLike<E>) | null,
    ) {
      return chain.then(resolve, reject);
    },
    /** Waits for the chain, then closes the Application. */
    async close() {
      await chain.catch(() => {});
      await application.close();
    },
    [Symbol.asyncDispose]() {
      return fixture.close();
    },
  };
  const step = (work: () => Promise<void>) => {
    chain = chain.then(work);
    return fixture;
  };
  const check = (assertion: () => Promise<void>) =>
    step(() =>
      assertion().catch((error) => {
        if (error instanceof assert.AssertionError && failures.length)
          error.message += `\n\nLogged since the last \`when\`:\n${failures.join("\n")}`;
        throw error;
      }),
    );
  return fixture;
}
export type ProcessFixture = ReturnType<typeof processFixture>;

/** One Aggregate per `context.aggregate` the process names, declaring just those Events. */
const stubAggregates = (names: string[]): AggregateDefinition[] => {
  const events = new Map<string, Record<string, () => void>>();
  for (const name of names) {
    const parts = name.split(".");
    const key = parts.slice(0, 2).join(".");
    events.set(key, { ...events.get(key), [parts[2]!]: () => {} });
  }
  return [...events].map(([key, handlers]) => {
    const [context, aggregate] = key.split(".");
    return compile(context!, aggregate!, { commands: {}, events: handlers });
  });
};
