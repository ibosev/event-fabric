import type { TSchema } from "typebox";
import type { EventsOf, MessageHandle, Static } from "../authoring/aggregate.js";
import type { TellInput } from "../kernel/index.js";
import type {
  EventPayloads,
  IsOpen,
  ReadmodelDocuments,
  ReadmodelModelName,
  Registered,
} from "../registry.js";
import type { ReactionEvent, WorkflowDefinition } from "../runtime/types.js";
import type { ConsumerStart, ReplyOf } from "../types.js";
import { assertPlainSchema } from "../validation.js";
import { askedMessage } from "../internal.js";
import { qualifiedName } from "../observability.js";
import type { EngineContext, WorkflowDue, WorkflowStatus } from "./engine.js";

/**
 * Workflows (ADR-0021): `workflow(name, { ... })` declares one, `signal<T>()` declares its Signals.
 * Its `ctx` and the contexts' `startWorkflow`, `sendSignal` and `queryWorkflow` take typed Handles,
 * as Sagas do (ADR-0013); a Workflow's name as a string is typed by the generated Register (ADR-0012).
 */

declare const types: unique symbol;
type Get<T, K> = K extends keyof T ? T[K] : never;

/** A Signal a Workflow accepts: `signal<P>()` types its payload, `signal(schema)` also checks it when sent. */
export type SignalDeclaration<P = unknown> = {
  readonly schema?: TSchema;
  readonly [types]?: { payload: P };
};
/** A Signal on its way to an instance of Workflow `W`, as a Signal Handle builds it. */
export type SignalInput<N extends string = string, P = unknown, W extends string = string> = {
  workflow: W;
  name: N;
  payload: P;
};
/** `workflow.signals.<name>(payload)` builds a Signal; `ctx.waitFor(ctx.signals.<name>)` waits for one. */
export type SignalHandle<N extends string = string, P = unknown, W extends string = string> = {
  (...payload: null extends P ? [payload?: P] : [payload: P]): SignalInput<N, P, W>;
  readonly kind: "signal";
  readonly name: N;
  readonly schema?: TSchema;
  readonly [types]?: { payload: P };
};
type PayloadOf<T> = NonNullable<T extends { readonly [types]?: infer X } ? X : never> extends {
  payload: infer P;
}
  ? P
  : never;
export type SignalHandles<S, W extends string = string> = {
  readonly [K in Extract<keyof S, string>]: SignalHandle<K, PayloadOf<S[K]>, W>;
};

/** A Workflow as `workflow(...)` returns it: the runtime's definition, its name and its Signal Handles. */
export type Workflow<N extends string = string, I = any, S = any, V = any, R = any> = WorkflowDefinition & {
  readonly name: N;
  readonly signals: SignalHandles<S, N>;
  readonly [types]?: { input: I; signals: S; state: V; result: R };
};
type Info<W> = NonNullable<W extends { readonly [types]?: infer X } ? X : never>;

type Workflows = Registered["workflows"];
/** A Workflow named in the generated Register. */
export type WorkflowName = Extract<keyof Workflows, string>;
/** A Workflow as the contexts name it: its Handle, or its name typed by the Register. */
export type WorkflowRef = Workflow | WorkflowName;
type Signals<W> = W extends Workflow<infer N>
  ? {
      [K in Extract<keyof Info<W>["signals"], string>]: SignalInput<K, PayloadOf<Info<W>["signals"][K]>, N>;
    }
  : IsOpen<Workflows> extends true
    ? Record<string, SignalInput<string, any>>
    : {
        [K in Extract<keyof Get<Get<Workflows, W>, "signals">, string>]: SignalInput<
          K,
          Get<Get<Get<Workflows, W>, "signals">, K>,
          Extract<W, string>
        >;
      };
/** A Signal Workflow `W` accepts. */
export type SignalOf<W> = Signals<W>[keyof Signals<W>];
/**
 * A Signal as `sendSignal` takes it, naming its Workflow: built by a Signal Handle, or any registered
 * Workflow's Signal by name; any while the Register is open.
 */
export type SentSignal = IsOpen<Workflows> extends true
  ? SignalInput<string, unknown>
  : { [W in WorkflowName]: SignalOf<W> }[WorkflowName];
/** The input that starts an instance of `W`. */
export type InputOf<W> = W extends Workflow ? Info<W>["input"] : any;
/** What an instance of `W` returns when it completes. */
export type ResultOf<W> = W extends Workflow ? Info<W>["result"] : any;
/** What `queryWorkflow` reads from the store: status, the `set` view and the result. */
export type WorkflowQuery<W = Workflow> = {
  status: WorkflowStatus;
  state?: W extends Workflow ? Info<W>["state"] : any;
  result?: ResultOf<W>;
};
export type StartWorkflowOptions<W> = {
  /** Starts a finished id again. */
  reuse?: boolean;
  /** A Signal delivered with the start, or to the instance if it already runs. */
  signal?: SignalOf<W>;
};
/** `startWorkflow`'s arguments after the id: the input (optional when `W` takes none) and the options. */
export type StartArgs<W> = undefined extends InputOf<W>
  ? [input?: InputOf<W>, options?: StartWorkflowOptions<W>]
  : [input: InputOf<W>, options?: StartWorkflowOptions<W>];

/**
 * Starting, signalling and querying Workflow instances, on the Gateway, Saga, Worker, Task and Plugin
 * contexts. A Saga records starts and Signals with its state (`R` is `void` there).
 */
export type WorkflowCalls<R = Promise<void>> = {
  /** Starts instance `id`: a no-op while it runs, refused once it finished unless `reuse`. */
  startWorkflow<W extends WorkflowRef>(workflow: W, id: string, ...args: StartArgs<W>): R;
  /** Signals instance `id` of the Signal's Workflow; a missing or finished one dead-letters it as `workflow-missing`. */
  sendSignal(id: string, signal: SentSignal): R;
  /** The committed status, `set` view and result, or null; nothing runs the body to answer. */
  queryWorkflow<W extends WorkflowRef>(workflow: W, id: string): Promise<WorkflowQuery<W> | null>;
};

type Timed<T> = { value: T; timedOut?: undefined } | { timedOut: true; value?: undefined };
/** When a `waitFor` times out: `timeout` ms after its step first ran, or at the absolute time `at`. */
type Due = { timeout: number; at?: undefined } | { at: string | Date; timeout?: undefined };
type Asked = { handles: string[]; execute(message: { payload: any }, ...rest: any[]): unknown };
type AskPayload<W extends Asked> = Parameters<W["execute"]>[0]["payload"];
type View<T> = T extends TSchema ? Static<T> : unknown;
type EventsFor<D> = [D] extends [undefined] ? EventPayloads : EventsOf<D>;
type EventKey<D> = Extract<keyof EventsFor<D>, string>;

/**
 * What a Workflow body receives. Every call is a durable step, named by its name and occurrence;
 * after a wake the body runs again and finished steps return their recorded results (ADR-0021).
 * Await only `ctx` calls, and take time and randomness from `ctx`.
 */
export type WorkflowContext<E = EventPayloads, C extends string = string, S = any, V = any, I = any, G extends string = string> = {
  /** The instance id: what `startWorkflow` was given, or what `id` picked from the start Event. */
  readonly id: string;
  /** This Workflow's Signal Handles, for `waitFor`. */
  readonly signals: SignalHandles<S>;
  /**
   * The oldest correlated Event of type `event` not yet read. With a `timeout` (ms from the step's first
   * run) or an absolute `at`, `{ value }` or `{ timedOut: true }`.
   */
  waitFor<K extends C>(event: K, options: Due): Promise<Timed<ReactionEvent<Get<E, K>>>>;
  waitFor<K extends C>(event: K): Promise<ReactionEvent<Get<E, K>>>;
  /** The oldest Signal of that name not yet read. */
  waitFor<H extends SignalHandles<S>[keyof SignalHandles<S>]>(
    signal: H,
    options: Due,
  ): Promise<Timed<PayloadOf<H>>>;
  waitFor<H extends SignalHandles<S>[keyof SignalHandles<S>]>(signal: H): Promise<PayloadOf<H>>;
  /** Waits `ms`, or until the absolute time `at` as Saga Deadlines take it; `name` names the step. */
  sleep(name: string, msOrAt: number | string | Date): Promise<void>;
  /** Queues the Worker's Message and resolves with its reply; with a `timeout`, `{ outcome: "timedOut" }` once it passes. */
  ask<W extends Asked>(
    worker: W,
    payload: AskPayload<W>,
    options: { timeout: number },
  ): Promise<ReplyOf<W> | { outcome: "timedOut" }>;
  ask<W extends Asked>(worker: W, payload: AskPayload<W>): Promise<ReplyOf<W>>;
  /** One document of a Readmodel in a group the Workflow `reads`, recorded in the journal. */
  read<Gr extends G, M extends ReadmodelModelName<Gr>>(
    group: Gr,
    model: M,
    id: string,
  ): Promise<Get<ReadmodelDocuments, `${Gr}.${M}`> | null>;
  /** Sends a Command built by a Command Handle, through the Outbox. */
  tell(command: TellInput): Promise<void>;
  /** Queues a Message for a Worker, through the Outbox. */
  queue<P>(message: MessageHandle<string, P>, payload: P): Promise<void>;
  /**
   * Starts an instance of another Workflow: a no-op while it runs. A finished id dead-letters the start
   * as `workflow-finished` unless `reuse` starts it again.
   */
  start<W extends Workflow>(
    workflow: W,
    id: string,
    ...args: undefined extends InputOf<W>
      ? [input?: InputOf<W>, options?: { reuse?: boolean }]
      : [input: InputOf<W>, options?: { reuse?: boolean }]
  ): Promise<void>;
  now(): Promise<Date>;
  random(): Promise<number>;
  uuid(): Promise<string>;
  /** The view `queryWorkflow` returns as `state`. */
  set(view: V): Promise<void>;
  /** Ends the instance `failed`. */
  fail(reason: string): never;
  /** Ends the instance `cancelled`. */
  cancel(): never;
  /** Ends this run and starts a fresh journal under the same id. */
  restart(input: I): never;
};

/** Declares a Signal; `signal(schema)` checks its payload when it is sent. */
export function signal<P = null>(): SignalDeclaration<P>;
export function signal<T extends TSchema>(schema: T): SignalDeclaration<Static<T>>;
export function signal(schema?: TSchema): SignalDeclaration {
  if (schema === undefined) return {};
  assertPlainSchema(schema, "Signal");
  return { schema };
}

const signalHandle = (workflow: string, name: string, schema: TSchema | undefined) =>
  Object.defineProperty(
    Object.assign((payload: unknown = null) => ({ workflow, name, payload }), { kind: "signal", schema }),
    "name",
    { value: name },
  );


/**
 * Declares a Workflow. Pass `domain` to type its Events from the domain's Handles; without it they
 * come from the generated Register. `start` and `id` start an instance on an Event, `correlate`
 * routes later Events to the instances that `waitFor` them, `state` types the `set` view.
 */
export function workflow<
  const N extends string,
  R,
  I = undefined,
  D = undefined,
  const St extends EventKey<D> = never,
  const C extends EventKey<D> = never,
  S extends Record<string, SignalDeclaration<any>> = {},
  T extends TSchema | undefined = undefined,
  const G extends string = never,
>(
  name: N,
  definition: {
    version: number;
    /** The domain whose Handles type the Events; the Register's when left out. */
    domain?: D;
    /** The Event that starts an instance; its `id` picks the instance, and the Event is the input. */
    start?: St;
    id?: (event: ReactionEvent<Get<EventsFor<D>, St>>) => string;
    correlate?: {
      [K in C]: (event: ReactionEvent<Get<EventsFor<D>, K>>) => string | string[] | null | undefined;
    };
    signals?: S;
    /** The schema of the `set` view, which types `queryWorkflow`'s `state`. */
    state?: T;
    reads?: readonly G[];
    /** Runs the Workflow as N Partitions; each instance belongs to one. Default 1. */
    partitions?: number;
    /** Where the Workflow starts reading the Event log when it is new; default `"now"`. */
    from?: ConsumerStart;
    run(
      ctx: WorkflowContext<EventsFor<D>, C, S, View<T>, NoInfer<[St] extends [never] ? I : ReactionEvent<Get<EventsFor<D>, St>>>, G>,
      input: [St] extends [never] ? I : ReactionEvent<Get<EventsFor<D>, St>>,
    ): Promise<R>;
  },
): Workflow<N, [St] extends [never] ? I : ReactionEvent<Get<EventsFor<D>, St>>, S, View<T>, R> {
  const { domain: _domain, state: _state, signals = {}, run, reads, ...rest } = definition;
  const handles = Object.fromEntries(
    Object.entries(signals as Record<string, SignalDeclaration>).map(([key, { schema }]) => [
      key,
      signalHandle(name, key, schema),
    ]),
  );
  const typed = (ctx: EngineContext) => ({
    ...ctx,
    signals: handles,
    waitFor: (handle: string | { kind: "signal"; name: string }, options?: WorkflowDue) =>
      ctx.waitFor(typeof handle === "string" ? { kind: "event", name: handle } : handle, options),
    ask: (worker: { handles: string[] }, payload: unknown, options?: { timeout?: number }) =>
      ctx.ask(askedMessage(worker), payload, options),
    tell: (input: TellInput) => ctx.tell(qualifiedName(input), input),
    queue: (message: MessageHandle, payload: unknown) => ctx.queue(message.name, payload),
    start: (workflow: Workflow, id: string, input?: unknown, options?: { reuse?: boolean }) =>
      ctx.start(workflow.name, id, input, options),
  });
  return {
    ...rest,
    ...(reads ? { reads: [...reads] } : {}),
    name,
    signals: handles,
    run: (ctx: EngineContext, input: any) => run(typed(ctx) as never, input),
  } as unknown as Workflow<N, any, S, View<T>, R>;
}
