import type { IncomingMessage } from "node:http";
import type { Duplex } from "node:stream";
import type { createScopedHandle } from "../gateway/access.js";
import type { AggregateDefinition, Tell } from "../kernel/index.js";
import type {
  AuthoringMarker,
  PredicateMarker,
  StateView,
} from "../authoring/index.js";
import type { CommandInput } from "../id.js";
import type { WorkflowBody } from "../workflows/engine.js";
import type { WorkflowCalls } from "../workflows/index.js";
import type { EventPattern } from "../registry.js";
import type { MessageHandle } from "../authoring/aggregate.js";
import type {
  CommandAccessKey,
  SagaMessageAccessKey,
  WorkflowAccessKey,
  SagaMessageName,
  SagaMessagePayload,
  SagaName,
  Domain,
  EventPayloads,
  MessageName,
  MessagePayloads,
  ReadmodelAccessKey,
  ReadmodelDocument,
  ReadmodelDocuments,
  ReadmodelGroupName,
  ReadmodelModelName,
  ReadmodelName,
  RegisteredServices,
  RegisteredReadmodelHelpers,
  RegisteredLegacyApp,
  RegisteredReadmodelServices,
  Register,
  Undeclared,
  IsOpen,
} from "../registry.js";
import type { createReadmodels, ReadOptions } from "./readmodels.js";
import type { Filter, NativeOptions } from "../readmodel-store/types.js";
import type {
  ReadmodelStore,
  ReadmodelIndex,
  SchemaMigration,
} from "../readmodel-store/types.js";
import type {
  Command,
  CommandLane,
  ConsumerStart,
  JsonObject,
  Logger,
  Message,
  Outcome,
  Persistence,
  Position,
  ReadAllOptions,
  RecordedEvent,
  StreamId,
  Schema,
  Transport,
  Validate,
  ApplicationNotification,
  SagaMessage,
} from "../types.js";
export type Identity = string | string[] | JsonObject | null | undefined;
export type Reaction = (event: any, state: any, app: any) => unknown;
/** The Event a Readmodel or Saga Reaction receives. */
export type ReactionEvent<P = any> = Omit<RecordedEvent, "payload"> & {
  payload: P;
  context: { name: string };
  aggregate: { id: string; name: string };
};
/** The Readmodel document or Saga state a Reaction updates; `destroy` deletes it. */
export type ReactionView<D = any> = StateView<D> & {
  id: string;
  destroy(): void;
};
/** The state of one Saga instance: a Reaction view that also sets and cancels its Deadlines. */
export type SagaView<D = any> = ReactionView<D> & {
  /** Sets the instance's Deadline `name`, replacing a pending one of that name, once this attempt commits. */
  deadline(name: string, at: string | Date, payload?: unknown): void;
  cancelDeadline(name: string): void;
};
/** Where a Saga Message goes and when: `to` an instance, `at` a due time (default now). */
export type SagaMessageOptions = {
  to?: string;
  at?: string | Date;
  /** A stable id makes the send idempotent, e.g. an imported scheduler task's. */
  id?: string;
  metadata?: JsonObject;
};
/** Sends a Saga Message to an instance of the Saga `saga`, outside the Event log. */
export type SendSagaMessage<R = void> = <
  S extends SagaName,
  N extends SagaMessageName<S>,
>(
  saga: S,
  name: N,
  payload?: SagaMessagePayload<S, N>,
  options?: SagaMessageOptions,
) => R;
/** A Saga Message or Deadline as its handler receives it. */
export type SagaMessageView<P = any> = Readonly<
  Omit<SagaMessage, "group" | "attempt" | "payload">
> & { saga: string; payload: P };
export type SagaMessageFunction<P = any, State = any> = (
  message: SagaMessageView<P>,
  saga: SagaView<State>,
  app: ReactionApp,
) => unknown;
export type SagaMessageDefinition<P = any, State = any> =
  | SagaMessageFunction<P, State>
  | {
      handle: SagaMessageFunction<P, State>;
      /** Creates the instance when it does not exist; otherwise a Saga Message to a missing instance is dropped. */
      start?: boolean;
      /** The instance a send without `to` reaches: a payload key or a function. Default for `start`: the Saga Message id. */
      identity?: string | ((message: { id: string; payload: P }) => string);
      /** Checked when the Saga Message is sent. */
      schema?: Schema;
    };
type IsAny<T> = 0 extends 1 & T ? true : false;
type QueueOptions<R extends string> = {
  suffix?: string;
  metadata?: JsonObject;
  notBefore?: string;
  replaceKey?: string;
  /**
   * From a Saga Reaction: the Saga's own Saga Message the Worker replies with, to this instance,
   * once it succeeds (`{ outcome: "ok", value }`) or dead-letters (`{ outcome: "failed", error, attempts }`).
   */
  replyTo?: R;
};
type Effecting<M, Loaded = {}, R extends string = never> = {
  /** Records a Command, sent through the Outbox once this attempt commits. */
  tell: Tell;
  /** Records a Message for a Worker, sent through the Outbox once this attempt commits. */
  queue<K extends IsAny<M> extends true ? string : Extract<keyof M, string>>(
    name: K,
    payload: IsAny<M> extends true ? any : M[K & keyof M],
    options?: QueueOptions<R>,
  ): void;
  /** Queues a Message declared with `message(name, schema)`. */
  queue<P>(message: MessageHandle<string, P>, payload: P, options?: QueueOptions<R>): void;
  /** `getDomain().context.aggregate(id).command(payload)` tells that Command. */
  getDomain(): Domain<
    void,
    [options?: { notBefore?: string; replaceKey?: string }],
    Loaded
  >;
};
/**
 * The third Readmodel Reaction argument: it reads, and never tells or queues.
 * `S` defaults to the Readmodel helpers when `Register["readmodelServices"]` is declared,
 * otherwise to the Application services contract for backwards compatibility.
 */
export type ReadmodelApp<S = RegisteredReadmodelServices> = RegisteredLegacyApp & {
  getServices(): S;
  getReadmodels(): Readmodels;
  logger: Logger;
};
/**
 * The third Saga Reaction argument: `tell` and `queue` record effects committed with the Saga state.
 * `R` names the Saga's own Saga Messages a queued Message's `replyTo` may name, e.g.
 * `Extract<keyof typeof messages, string>`; by default any name, checked when the Reaction commits.
 */
export type ReactionApp<
  M = MessagePayloads,
  S = RegisteredServices,
  R extends string = string,
> = ReadmodelApp<S> &
  Effecting<M, {}, R> & {
    /** Records a Saga Message, committed with the Saga state. */
    sendSagaMessage: SendSagaMessage;
  } &
  /** Starts and Signals are recorded with the Saga state; `queryWorkflow` reads the store. */
  WorkflowCalls<void>;
/** The last argument of a Command Step, Precondition, Rule and id generator. */
export type CommandApp<S = RegisteredServices> = RegisteredLegacyApp & Effecting<
  MessagePayloads,
  /** `getDomain().context.aggregate(id).load()` reads another Aggregate's current State. */
  { load(options?: { atRevision?: number }): Promise<any> }
> & {
  /** Creates or replaces a named Command Schedule owned by this Command's Stream. */
  schedule(name: string, dueAt: string | Date, command: CommandInput): void;
  /** Cancels atomically on acceptance; the Outcome reports the cancellation result. */
  cancelSchedule(name: string): void;
  getServices(): S;
  getReadmodels(): Readmodels;
  logger: Logger;
  /** When this attempt started; the same instant as `command.now`. */
  now: Date;
  /** Aborts when the Command's deadline passes; pass it to slow service calls. */
  signal?: AbortSignal;
};
/** Prefixes every key, e.g. `Qualified<"billing.invoice", { issued: P }>` is `{ "billing.invoice.issued": P }`. */
export type Qualified<Prefix extends string, T> = {
  [K in Extract<keyof T, string> as `${Prefix}.${K}`]: T[K];
};
type ReactionFunction<P, View, App> = (
  event: ReactionEvent<P>,
  view: View,
  app: App,
) => unknown;
/** A Reaction map keyed by qualified Event name, each payload typed by its Event. */
export type Reactions<
  Events extends object = EventPayloads,
  View = ReactionView,
  App = ReactionApp,
> = {
  [N in keyof Events]?:
    | ReactionFunction<Events[N], View, App>
    | (
        | AuthoringMarker
        | PredicateMarker
        | ReactionFunction<Events[N], View, App>
      )[];
};
/** Reactions of the Readmodel `"group.model"`; the view is its generated document. */
export type ReadmodelReactions<N extends ReadmodelName> = Reactions<
  EventPayloads,
  ReactionView<ReadmodelDocument<N>>,
  ReadmodelApp
>;
/** Reactions of a Saga whose state is `State`. */
export type SagaReactions<State = any> = Reactions<
  EventPayloads,
  SagaView<State>
>;
/** Which document or Saga ids an Event concerns; the Stream id when an Event has no entry. */
export type Identities<Events extends object = EventPayloads> = {
  [N in keyof Events]?:
    | ((event: ReactionEvent<Events[N]>) => Identity | Promise<Identity>)
    | any[];
};
/** A Readmodel repository: `repository(id)` reads one document; queries return documents. */
export type ReadmodelRepository<D = any> = {
  (
    id: string,
    options?: ReadOptions,
  ): Promise<{ document: D | null; position: Position | null; behind: boolean }>;
  find(filter?: Filter<D>, options?: ReadOptions<D>): Promise<D[]>;
  findOne(filter?: Filter<D>, options?: ReadOptions<D>): Promise<D | null>;
  count(filter?: Filter<D>, options?: ReadOptions<D>): Promise<number>;
  stream(filter?: Filter<D>, options?: ReadOptions<D>): AsyncIterable<D>;
  /**
   * The store's own handle; with `scope`, a read-only one limited to it where the store supports that.
   * `options.joinScope` limits the documents of models a native pipeline joins.
   */
  native(scope?: Filter<D>, options?: NativeOptions): any;
};
/** `getReadmodels()`: `readmodels.group.model(id)` and queries, typed from `Register`. */
export type Readmodels = {
  repository<G extends ReadmodelGroupName, M extends ReadmodelModelName<G>>(
    group: G,
    model: M,
  ): ReadmodelRepository<ReadmodelDocuments[`${G}.${M}`]>;
} & Pick<
  ReturnType<typeof createReadmodels>,
  "waitFor" | "subscribe" | "status" | "changes"
> &
  (IsOpen<ReadmodelDocuments> extends true
  ? { [group: string]: any }
  : {
      [G in ReadmodelGroupName]: {
        [M in ReadmodelModelName<G>]: ReadmodelRepository<
          ReadmodelDocuments[`${G}.${M}`]
        >;
      };
    });
export type ConsumerDefinition = {
  initialState?: JsonObject;
  identity?: Identities;
  reactions: Reactions<EventPayloads, any, any>;
};
export type ReadmodelDefinition = ConsumerDefinition & {
  schema: Schema;
  indexes?: ReadmodelIndex[];
  /**
   * Reaction sets: more Reactions to the model's Events, each set with its own Identities and markers, on the
   * same documents, after the model's own Reactions and in declaration order. The old `<event>:<group>` keys.
   */
  reactionSets?: Record<string, Omit<ConsumerDefinition, "initialState">>;
};
export type ReadmodelGroup = {
  store: ReadmodelStore;
  models: Record<string, ReadmodelDefinition>;
  reads?: string[];
  /** Bump when helpers or captured values used by reconstruction change. Guards pending rebuild resumption. */
  rebuildVersion?: string;
  migrations?: SchemaMigration[];
  validate?: boolean;
  validator?: Validate;
  schemas?: Record<string, Schema>;
  /**
   * Runs the group as N Partitions, each on any replica. Each document belongs to one
   * Partition and sees every Event that touches it in Position order. Reactions cannot
   * read the group's own documents. Default 1.
   */
  partitions?: number;
};
export type SagaDefinition = ConsumerDefinition & {
  reads?: string[];
  /** Runs the Saga as N Partitions; each Saga instance belongs to one. Default 1. */
  partitions?: number;
  /**
   * Where the group starts reading the Event log when it is new (no Partition has a Checkpoint):
   * `"now"` (default) skips every Event recorded before it first starts. Changing it later has no
   * effect; retire the group and deploy it again to replay history.
   */
  from?: ConsumerStart;
  /** Handlers of Saga Messages others send to an instance, by name (ADR-0020). */
  messages?: Record<string, SagaMessageDefinition>;
  /** Handlers of the Deadlines an instance sets with `saga.deadline`, by name. */
  deadlines?: Record<string, SagaMessageFunction>;
};
/**
 * One version of a Workflow as the runtime runs it (ADR-0021); `workflow(...)` builds it.
 * `start` names the Event that starts an instance and `id` picks it; `correlate` routes later Events
 * to the instances that wait for them. The body's `tell(name, input)` takes the Command's qualified name
 * and its `CommandInput`.
 */
export type WorkflowDefinition = {
  /** Set by `workflow(name, ...)`; it must match the Workflow's key in `workflows`. */
  name?: string;
  version: number;
  run: WorkflowBody;
  start?: string;
  id?: (event: any) => string;
  correlate?: Record<string, (event: any) => string | string[] | null | undefined>;
  /** The Signals an instance accepts, each with an optional payload schema. */
  signals?: Record<string, { schema?: Schema }>;
  reads?: string[];
  /** Runs the Workflow as N Partitions; each instance belongs to one. Default 1. */
  partitions?: number;
  /** As a Saga's `from`; it limits Event-started instances and correlated Events only. The highest version's applies. */
  from?: ConsumerStart;
};
/** A Workflow, or several versions of it: new instances take the highest, which also routes. */
export type WorkflowConfig = WorkflowDefinition | { versions: WorkflowDefinition[] };
export type WorkerContext<S = RegisteredServices> = {
  idempotencyKey: string;
  attempt: number;
  signal: AbortSignal;
  /** Sends a Command that inherits the Message's Metadata, Causation and Correlation. */
  send: (command: CommandInput) => Promise<Outcome>;
  /** Sends a Saga Message that inherits the Message's Metadata, Causation and Correlation. */
  sendSagaMessage: SendSagaMessage<Promise<void>>;
  services: S;
  /** The Application's Readmodels, as a Task reads them; they may lag the Message's cause. */
  readmodels: Readmodels;
  logger: Logger;
  // Workflow starts and Signals inherit the Message's Metadata, Causation and Correlation.
} & WorkflowCalls;
/** A Worker performs queued Messages of the types in `handles`; payloads come from `schemas`. */
export type WorkerDefinition<
  N extends MessageName = MessageName,
  S = RegisteredServices,
> = {
  handles: N[];
  /** Message payload schemas: they type `app.queue` and are checked when a Message is recorded. */
  schemas?: { [K in N]?: Schema };
  concurrency?: number;
  timeout?: number;
  retry?: { attempts?: number; baseDelay?: number; maxDelay?: number };
  execute(
    message: Omit<Message, "payload"> & { payload: MessagePayloads[N] },
    ctx: WorkerContext<S>,
  ): Promise<unknown> | unknown;
};
export type Route = {
  method: string;
  path: string;
  handler: any;
  command?: string;
  readmodel?: string;
  schema?: Schema;
};
export type AccessMap<TContext = any> = {
  domain?: {
    [K in CommandAccessKey]?:
      | boolean
      | ((ctx: TContext, command: any) => boolean | Promise<boolean>);
  };
  /** Saga Messages a caller may send, as `<saga>.<message>`; denied without an entry. The send names its instance in `to`. */
  sagaMessages?: {
    [K in SagaMessageAccessKey]?:
      | boolean
      | ((ctx: TContext, message: { payload: any; to?: string }) => boolean | Promise<boolean>);
  };
  /**
   * Workflow calls a caller may make, as `<workflow>.start`, `.query` or `.signals.<name>`; denied without an
   * entry. A start with a Signal needs both entries. `payload` is the start's input or the Signal's payload.
   */
  workflows?: {
    [K in WorkflowAccessKey]?:
      | boolean
      | ((ctx: TContext, request: { id: string; payload?: unknown }) => boolean | Promise<boolean>);
  };
  readmodels?: {
    [K in ReadmodelAccessKey]?:
      | boolean
      | ((
          ctx: TContext,
        ) => boolean | JsonObject | Promise<boolean | JsonObject>);
  };
};
/** What `handle(req)` returns: the caller's context, Access-scoped Readmodels, Commands and live feeds. */
export type ScopedHandle<TContext = any> = ReturnType<
  typeof createScopedHandle<TContext>
>;
export type GatewayDefinition<TContext = any> = {
  port: number;
  host?: string;
  bodyLimit?: string | number;
  /** Keep each JSON body's bytes as `req.rawBody` (a Buffer), e.g. to check a webhook signature. */
  rawBody?: boolean;
  middleware?: any[];
  routes?: Route[];
  context?: (req: IncomingMessage) => TContext | Promise<TContext>;
  commandMetadata?: (
    ctx: TContext,
    command: any,
  ) => JsonObject | Promise<JsonObject>;
  access?: AccessMap<TContext>;
  /** The lane of this Gateway's Commands, `interactive` by default; `bulk` for machine callers such as an import. */
  lane?: CommandLane | ((ctx: TContext) => CommandLane | Promise<CommandLane>);
  waitTimeout?: number;
  serverTiming?: boolean;
  /**
   * Whose `Idempotency-Key` values share one namespace, e.g. `(ctx) => ctx.tenant`. With a scope, a request's
   * Command id derives from (scope, key), so a retry is a Duplicate Command. Without one the header is ignored.
   */
  idempotencyScope?: (ctx: TContext) => string | undefined | Promise<string | undefined>;
  /**
   * Which live Events and Application Notifications a caller may receive (`handle(req).subscribeEvents`,
   * `subscribeNotifications`, the `live()` route). Called for every item and subscriber, so keep it cheap
   * and synchronous. A kind without a rule is denied.
   */
  live?: {
    events?: (ctx: TContext, event: RecordedEvent) => boolean;
    notifications?: (ctx: TContext, notification: ApplicationNotification) => boolean;
  };
  /**
   * Takes HTTP upgrade requests, for example a WebSocket protocol. `scoped(ctx)` is the handle a route gets from
   * `handle(req)`, for the caller the upgrade handler identified. Upgraded sockets are destroyed when the
   * Gateway stops. Without it, upgrades are refused.
   */
  upgrade?: (
    req: IncomingMessage,
    socket: Duplex,
    head: Buffer,
    scoped: (ctx: TContext) => ScopedHandle<TContext>,
  ) => void;
};
/** `sendCommand`'s result: the Outcome, and `.await(pattern)` for the Event it recorded (the first matching any of a list). */
export type SentCommand = Promise<Outcome> & {
  await(pattern: EventPattern | readonly EventPattern[]): Promise<RecordedEvent>;
};
export type SendCommand = (
  input: CommandInput,
  options?: { deadline?: number; lane?: CommandLane },
) => SentCommand;
export type AwaitCorrelated = (
  input: CommandInput,
  pattern: EventPattern,
  options?: {
    predicate?: (event: RecordedEvent) => boolean;
    deadline?: number;
    lane?: CommandLane;
  },
) => Promise<
  | { status: "accepted"; event: RecordedEvent; outcome: Outcome }
  | { status: "rejected" | "failed"; outcome: Outcome }
  | { status: "unknown"; outcome: Outcome }
>;
/** What a plugin receives once at start, after services are ready and required rebuilds ran. */
export type PluginContext<S = RegisteredServices> = {
  logger: Logger;
  config: ResolvedApplicationConfig;
  sendCommand: SendCommand;
  sendSagaMessage: SendSagaMessage<Promise<void>>;
  awaitCorrelated: AwaitCorrelated;
  readmodels: Readmodels;
  services: S;
  /** The declared components, read-only. */
  model: {
    readonly aggregates: readonly AggregateDefinition[];
    readonly readmodels: Readonly<
      Record<string, { models: Record<string, ReadmodelDefinition>; reads: string[] }>
    >;
    readonly sagas: Readonly<Record<string, SagaDefinition>>;
    readonly workers: Readonly<Record<string, { handles: string[] }>>;
    readonly gateways: readonly string[];
  };
  /** `start` after startup, `gateway` per Gateway, `shutdown` in reverse on `stop()`. */
  on(
    hook: "start" | "shutdown" | "gateway",
    callback: (...args: any[]) => unknown,
  ): void;
  subscribeEvents(
    handler: (event: RecordedEvent) => unknown,
    options?: { after?: Position | null },
  ): Promise<() => Promise<void>>;
  /** Publishes an Application Notification to live subscribers on every replica. */
  notify(name: string, payload?: unknown): Promise<void>;
  subscribeNotifications(
    handler: (notification: ApplicationNotification) => unknown,
  ): Promise<() => Promise<void>>;
} & WorkflowCalls;
export type Plugin<S = RegisteredServices> = (
  ctx: PluginContext<S>,
) => void | Promise<void>;
/** What `event-fabric task <module>` passes to the module's `run(ctx, args)`; no consumers or plugins run. */
export type TaskContext<S = RegisteredServices> = {
  config: ResolvedApplicationConfig;
  sendCommand: SendCommand;
  sendSagaMessage: SendSagaMessage<Promise<void>>;
  awaitCorrelated: AwaitCorrelated;
  readmodels: Readmodels;
  services: S;
  readStream(
    stream: StreamId,
    options?: { atRevision?: number },
  ): Promise<RecordedEvent[]>;
  readAll(options?: ReadAllOptions): Promise<RecordedEvent[]>;
  import(
    events: RecordedEvent[],
    options?: { signal?: AbortSignal },
  ): Promise<RecordedEvent[]>;
  logger: Logger;
} & WorkflowCalls;
/** A Task module's `run` export: `export const run: Task = async (ctx, args) => { ... }`. */
export type Task<S = RegisteredServices> = (
  ctx: TaskContext<S>,
  args: string[],
) => unknown;
/**
 * Where `setupServices` registers cleanup; the Application disposes it on `close()`, last
 * acquired first, after work has drained and before its own adapters close.
 */
export type ServiceResources = Pick<AsyncDisposableStack, "use" | "adopt" | "defer">;
/**
 * Builds the Application's services once, before any handler, rebuild, plugin or Task uses
 * them. Register each resource's cleanup right after acquiring it; on failure the cleanup
 * registered so far runs.
 */
export type SetupServices<S = RegisteredServices> = (context: {
  resources: ServiceResources;
  logger: Logger;
}) => S | Promise<S>;
/**
 * `services` (borrowed: the Application never closes them) or `setupServices` (owned:
 * cleaned up on `close()`). Required once `Register["services"]` has a required service.
 */
export type ServiceSupply<S = RegisteredServices> = {} extends S
  ? { services?: S; setupServices?: SetupServices<S> }
  :
      | { services: S; setupServices?: undefined }
      | { services?: undefined; setupServices: SetupServices<S> };
/** A class declared with `@Aggregate`, `@Readmodel` or `@Saga` from `event-fabric/decorators`. */
export type DecoratedClass = abstract new () => unknown;
/** A Readmodel group as an Application declares it; `store` defaults to the Application's `readmodelStore`. */
export type ReadmodelGroupConfig = Omit<ReadmodelGroup, "store" | "models"> & {
  store?: ReadmodelStore;
  models: Record<string, ReadmodelDefinition | DecoratedClass>;
};
type ApplicationOptions<S> = {
  name: string;
  store: Persistence;
  transport: Transport;
  aggregates?: (AggregateDefinition | DecoratedClass)[];
  readmodelStore?: ReadmodelStore;
  readmodels?: Record<string, ReadmodelGroupConfig>;
  sagas?: Record<string, SagaDefinition | DecoratedClass>;
  workflows?: Record<string, WorkflowConfig>;
  workers?: Record<string, WorkerDefinition<any, S>>;
  gateways?: Record<string, GatewayDefinition>;
  plugins?: Plugin<S>[];
  logger?: Logger;
  validate?: Validate;
  schemas?: Record<string, Schema>;
  metadataSchema?: Schema;
  metadataMaxBytes?: number;
  /** "Now" for Commands (`command.now`) and Saga Messages; a test moves it with the in-memory store's `clock`. */
  clock?: () => Date;
  pollInterval?: number;
  /**
   * URL of the module exporting this `config`; the CLI sets it. With it, rebuilds of a group with
   * `partitions > 1` on a store with rebuild slices (Postgres) fold in one worker thread per Partition.
   */
  configModule?: string;
  /**
   * Bytes this process's rebuilds may spend keeping the Readmodel documents they have read and folded,
   * so a document is read from the store once per rebuild while it fits; fold worker threads share it.
   * Default 256 MiB; 0 turns it off.
   */
  rebuildCacheBytes?: number;
  leaseDuration?: number;
  shutdownTimeout?: number;
  commandTimeout?: number;
  /** Milliseconds after which a Command handler or live Reaction logs `Slow Command handler` / `Slow Reaction` (1000). */
  slowHandlerThreshold?: number;
  /** How far, in Event-timestamp milliseconds, the relay may fall behind before it jumps to the head. */
  relayMaxLag?: number;
  /**
   * Members old Application code still calls on the Command and Reaction contexts, such as
   * `app => ({ getCrud: () => app.getServices().crud })`; built-in members win. Gateways take
   * `requestApp`, old Tasks the config module's `legacyTaskApp`.
   */
  legacyApp?: (app: { getServices(): S; getReadmodels(): Readmodels }) => RegisteredLegacyApp;
  only?: string[];
  typesPath?: string;
  /** `"names"` generates checked names with loose payloads and State, for gradual adoption. */
  typesMode?: "full" | "names";
  /** Package to augment during coexistence; defaults to `"event-fabric"`. */
  typesModule?: string;
  env?: string;
};
/** Readmodel helpers are an object, never an array (the runtime also rejects functions). */
type Helpers<R> = R & { [Symbol.iterator]?: never };
/**
 * `S` is the Application services contract and `R` the Readmodel helpers contract; until `R` is
 * declared, Readmodels share `S`. Registered services without declared helpers refuse
 * `readmodelServices`: Readmodel types would still promise the services the runtime withholds.
 */
export type ApplicationConfig<
  S = RegisteredServices,
  R = RegisteredReadmodelHelpers,
> = ApplicationOptions<S> &
  ServiceSupply<S> &
  (Undeclared<R> extends true
    ? {
        readmodelServices?: "services" extends keyof Register
          ? undefined
          : Helpers<object>;
      }
    : { readmodelServices: Helpers<R> });
/** An `ApplicationConfig` whose decorated classes have been replaced by their definitions. */
export type ResolvedApplicationConfig = Omit<
  ApplicationOptions<any>,
  "aggregates" | "readmodels" | "sagas"
> & {
  services?: Record<string, any>;
  /** The Readmodel helpers, which replace Application services in Readmodels; omitted keeps them. */
  readmodelServices?: Record<string, any>;
  setupServices?: SetupServices<any>;
  aggregates?: AggregateDefinition[];
  readmodels?: Record<
    string,
    Omit<ReadmodelGroup, "store"> & { store?: ReadmodelStore }
  >;
  sagas?: Record<string, SagaDefinition>;
};
/** One per committed Readmodel batch on the transport; subscribers see it per model as a Notification. */
export type BatchNotification = {
  kind: "readmodel";
  group: string;
  position: import("../types.js").Position;
  changes: {
    model: string;
    id: string;
    before: JsonObject | null;
    after: JsonObject | null;
    /** The correlations of the Events that made the change, so a client can find its own Command's writes. */
    correlationIds: string[];
  }[];
};
export type Notification = {
  kind: "readmodel";
  group: string;
  model: string;
  ids: string[];
  position: import("../types.js").Position;
  changes: {
    id: string;
    before: JsonObject | null;
    after: JsonObject | null;
    /** The correlations of the Events that made the change, so a client can find its own Command's writes. */
    correlationIds: string[];
  }[];
};
