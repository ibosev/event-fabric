import { createHash, randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { availableParallelism } from "node:os";
import { Worker } from "node:worker_threads";
import { createRequire } from "node:module";
import { createKernel, AggregateState, buildDomain } from "../kernel/index.js";
import type { AggregateDefinition } from "../kernel/index.js";
import { recordEffects } from "../effects.js";
import type { Effects, MessageRouting } from "../effects.js";
import { createCommand, uuidv7 } from "../id.js";
import type { CommandInput } from "../id.js";
import {
  GenerationBehindError,
  LeaseLostError,
  Rejection,
  RebuildRunningError,
  StaleSagaMessageError,
  StaleGenerationError,
} from "../errors.js";
import { comparePositions, decodePosition, encodePosition } from "../position.js";
import { pageComplete, readEventPage } from "../event-store/pages.js";
import {
  createValidator,
  assertMetadata,
  canonical,
  toJson,
} from "../validation.js";
import {
  createLogger,
  createTimings,
  count,
  measure,
  gauge,
  qualifiedName,
  recovery,
  streamName,
  withSpan,
  traceparent,
} from "../observability.js";
import { readmodelOrder } from "./dependencies.js";
import { memoryIndexes } from "./filter-index.js";
import { backoff, consumerId, createConsumerGroups } from "./consumer-groups.js";
import { keyPartition } from "../transport/partition.js";
import { createOutboxRelay, createWorkerRunner } from "./delivery.js";
import { createReadmodels } from "./readmodels.js";
import type {
  Domain,
  EventPattern,
  RegisteredServices,
  RegisteredReadmodelHelpers,
} from "../registry.js";
import type {
  ApplicationConfig,
  AwaitCorrelated,
  ResolvedApplicationConfig,
  Readmodels,
  ConsumerDefinition,
  ReadmodelDefinition,
  Identity,
  BatchNotification,
  ReadmodelGroup,
  Reaction,
  SagaDefinition,
  SagaMessageDefinition,
  SagaMessageOptions,
  SendSagaMessage,
} from "./types.js";
import type {
  Command,
  CommandLane,
  ConsumerStart,
  ConsumerStatus,
  EventPage,
  JsonObject,
  Lease,
  Outcome,
  Message,
  OutboxEntry,
  Position,
  PoolStats,
  RecordedEvent,
  Schema,
  Stop,
  StreamId,
  ApplicationNotification,
  Metadata,
  SagaMessage,
  SagaMessageChange,
  WorkflowRecord,
  RecordedStart,
} from "../types.js";
import { sharedSubscriptions } from "../transport/shared.js";
import type { ReadmodelChange } from "../readmodel-store/types.js";
import type { EventStore } from "../types.js";
import { stagedReads } from "./staged-reads.js";
import { foldSlice, parseProcessGroup, parseTime, workflowGroup, workflowName, workflowWake, workflowsApi } from "../internal.js";
import { inboxKey, newWorkflowInstance, runWorkflow } from "../workflows/engine.js";
import type { SignalInput, WorkflowCalls } from "../workflows/index.js";
import type { WorkflowStatus, WorkflowWake } from "../workflows/engine.js";
import { resolveComponents } from "../authoring/decorators.js";
import { matches } from "../readmodel-store/query.js";
import { normalizeDocument, resolveSchema } from "../readmodel-store/schema.js";
import {
  cachedReads,
  defaultRebuildCacheBytes,
  otherGroupReads,
  rebuildCache,
  withRebuildMemo,
} from "./rebuild-cache.js";

export * from "./types.js";
export { PermanentError, permanent } from "./delivery.js";
export { rebuildMemo } from "./rebuild-cache.js";
const errorMessage = (error: unknown) =>
  error instanceof Error ? error.message : String(error);
const eventName: (event: RecordedEvent) => string = qualifiedName;
// No copy: the view is frozen with `immutable`, which freezes the read Event's parts in place. One per read
// Event, shared by every Identity and Reaction that sees it.
const views = new WeakMap<RecordedEvent, any>();
const authoringView = (event: RecordedEvent) => {
  let view = views.get(event);
  if (!view)
    views.set(
      event,
      (view = immutable({
        ...event,
        context: { name: event.stream.context },
        aggregate: { id: event.stream.id, name: event.stream.aggregate },
      })),
    );
  return view;
};
const items = (value: any): any[] =>
  Array.isArray(value) ? value.flatMap(items) : [value];
const functions = (value: any): ((...args: any[]) => any)[] =>
  items(value)
    .map((item) =>
      typeof item === "function"
        ? item
        : (item?.preCondition ?? item?.businessRule),
    )
    .filter(Boolean);
const consumerKeys = {
  Readmodel: [
    "initialState",
    "identity",
    "reactions",
    "schema",
    "indexes",
    "reactionSets",
  ],
  Saga: [
    "initialState",
    "identity",
    "reactions",
    "reads",
    "partitions",
    "from",
    "messages",
    "deadlines",
  ],
};
/** A Saga's or Workflow's `from`, checked, in the form its `layout:<group>` row records. */
const recordedStart = (
  what: string,
  from: ConsumerStart = "now",
): RecordedStart => {
  if (from === "beginning" || from === "now") return from;
  if (from instanceof Date) {
    if (Number.isNaN(from.getTime()))
      throw new Error(`${what}: from must be a valid Date`);
    return { date: from.toISOString() };
  }
  if (typeof from === "object" && typeof from?.position === "string") {
    try {
      decodePosition(from.position);
    } catch {
      throw new Error(
        `${what}: from.position must be a Position token as EventFabric-Position returns it`,
      );
    }
    return { position: from.position };
  }
  throw new Error(`${what}: from must be "beginning", "now", a Date or { position }`);
};
/** A recorded `from` as logs show it: `beginning`, `now`, the ISO date or the Position token. */
const describeStart = (from: RecordedStart) =>
  typeof from === "string" ? from : "date" in from ? from.date : from.position;
/** A Saga Message handler in its long form. */
const messageSpec = (handler: SagaMessageDefinition) =>
  typeof handler === "function" ? { handle: handler } : handler;
const handlesSagaMessages = (definition: SagaDefinition) =>
  !!(
    Object.keys(definition.messages ?? {}).length ||
    Object.keys(definition.deadlines ?? {}).length
  );
/** An instant with a timezone, as a Schedule's due time is. */
const instant = (at: string | Date, what: string) => {
  if (
    !(at instanceof Date) &&
    (typeof at !== "string" || !/T.*(?:Z|[+-]\d\d:\d\d)$/i.test(at))
  )
    throw new Error(`${what} must be an instant with a timezone`);
  const due = new Date(at);
  if (Number.isNaN(due.getTime())) throw new Error(`${what} is not a valid time`);
  return due.toISOString();
};
// `{ settings: { autoCreate: false } }` reads as `settings.autoCreate`, so the old marker is recognisable.
const describe = (item: any): string =>
  item && typeof item === "object"
    ? Object.entries(item)
        .flatMap(([key, value]) =>
          value && typeof value === "object" && !Array.isArray(value)
            ? Object.keys(value).map((inner) => `${key}.${inner}`)
            : [key],
        )
        .join(", ")
    : JSON.stringify(item);
/** A Reaction's or Identity's markers and functions, parsed once: declarations do not change once loaded. */
const compiled = new WeakMap<
  object,
  { markers: ReturnType<typeof reactionMarker>[]; fns: ((...args: any[]) => any)[] }
>();
const compiledReaction = (reaction: any) => {
  if (!reaction || (typeof reaction !== "object" && typeof reaction !== "function"))
    return { markers: [], fns: [] };
  let parsed = compiled.get(reaction);
  if (!parsed)
    compiled.set(
      reaction,
      (parsed = {
        markers: items(reaction)
          .filter((item) => item && typeof item !== "function")
          .map(reactionMarker),
        fns: functions(reaction),
      }),
    );
  return parsed;
};
/**
 * A Reaction marker: `only.ifExists()`, `only.ifNotExists()` or `only.if(predicate)`. The old toolkit's
 * `settings.autoCreate`, `settings.shouldHandleEvent` and Saga `shouldHandle` keep their old meaning.
 */
const reactionMarker = (
  item: any,
): { existing?: boolean; when?: (...args: any[]) => unknown } | undefined => {
  if (!item || typeof item !== "object" || Object.keys(item).length !== 1)
    return undefined;
  const settings =
    typeof item.shouldHandle === "function"
      ? { shouldHandleEvent: item.shouldHandle }
      : item.settings;
  const [entry, ...rest] = Object.entries(settings ?? {});
  if (!entry || rest.length) return undefined;
  const [key, value] = entry;
  if (key === "existing" && typeof value === "boolean")
    return { existing: value };
  // autoCreate: true was the default; false only handles existing documents.
  if (key === "autoCreate" && typeof value === "boolean")
    return value ? {} : { existing: true };
  if (key === "shouldHandleEvent" && typeof value === "function")
    return { when: value as (...args: any[]) => unknown };
  return undefined;
};
/**
 * Refuses declarations the runtime would ignore, since ignoring them silently targets
 * another document or Saga instance (the Stream id) or runs a Reaction meant to be skipped.
 */
const assertConsumerShape = (
  kind: "Readmodel" | "Saga",
  name: string,
  definition: ConsumerDefinition,
) => {
  for (const key of Object.keys(definition))
    if (!consumerKeys[kind].includes(key))
      throw new Error(
        `${kind} ${name}: unsupported key ${key}${key === "query" ? "; return the filter from an identity function instead" : ""}`,
      );
  for (const [set, part] of Object.entries(
    (definition as ReadmodelDefinition).reactionSets ?? {},
  ))
    for (const key of Object.keys(part ?? {}))
      if (!["identity", "reactions"].includes(key))
        throw new Error(
          `${kind} ${name}: Reaction set ${set} has unsupported key ${key}; a Reaction set has only identity and reactions`,
        );
  for (const [event, identity] of Object.entries(definition.identity ?? {}))
    if (!items(identity).every((item) => typeof item === "function"))
      throw new Error(
        `${kind} ${name}: identity for ${event} must be a function, e.g. (event) => event.payload.id; anything else would fall back to the Stream id`,
      );
  for (const [event, reaction] of Object.entries(definition.reactions ?? {}))
    for (const item of items(reaction))
      if (
        typeof item !== "function" &&
        !reactionMarker(item) &&
        typeof item?.preCondition !== "function" &&
        typeof item?.businessRule !== "function"
      )
        throw new Error(
          `${kind} ${name}: Reaction for ${event} has an unsupported item ${describe(item)}; only functions, only.ifExists(), only.ifNotExists() and only.if() apply`,
        );
  const saga = definition as SagaDefinition;
  for (const [message, handler] of Object.entries(saga.messages ?? {})) {
    const spec = messageSpec(handler);
    if (typeof spec?.handle !== "function")
      throw new Error(`${kind} ${name}: Saga Message ${message} needs a handler function`);
    for (const key of Object.keys(spec))
      if (!["handle", "start", "identity", "schema"].includes(key))
        throw new Error(`${kind} ${name}: Saga Message ${message} has unsupported key ${key}`);
  }
  for (const [deadline, handler] of Object.entries(saga.deadlines ?? {}))
    if (typeof handler !== "function")
      throw new Error(`${kind} ${name}: Deadline ${deadline} needs a handler function`);
};
// Each model's own Reactions, then its Reaction sets: [model, definition, Identity cache key].
const parts = new WeakMap<object, [string, ConsumerDefinition, string][]>();
const reactionParts = (
  models: Record<string, ReadmodelDefinition>,
): [string, ConsumerDefinition, string][] => {
  if (!parts.has(models))
    parts.set(
      models,
      Object.entries(models).flatMap(([model, definition]) => [
        [model, definition, model] as [string, ConsumerDefinition, string],
        ...Object.entries(definition.reactionSets ?? {}).map(
          ([set, part]): [string, ConsumerDefinition, string] => [
            model,
            {
              initialState: definition.initialState,
              identity: part.identity,
              reactions: part.reactions ?? {},
            },
            `${model}:${set}`,
          ],
        ),
      ]),
    );
  return parts.get(models)!;
};
const immutable = (value: any): any => {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const key of Object.keys(value)) immutable(value[key]);
  }
  return value;
};
const { version } = createRequire(import.meta.url)("../../package.json") as {
  version: string;
};
const selected = (only: string[] | undefined, kind: string, name: string) =>
  !only?.length ||
  only.some(
    (selector) => selector === `${kind}:${name}` || selector === `${kind}:*`,
  );

/** Refuses a log holding Event names no Aggregate declares: Event names are permanent, so a rename breaks history. */
export async function assertDeclaredEvents(
  store: Pick<EventStore, "eventNames">,
  aggregates: AggregateDefinition[],
) {
  const declared = new Set(
    aggregates.flatMap((a) =>
      Object.keys(a.events).map((name) => `${a.context}.${a.name}.${name}`),
    ),
  );
  const undeclared = (await store.eventNames())
    .filter((name) => !declared.has(name))
    .sort();
  if (undeclared.length)
    throw new Error(
      `The Event log holds Events no Aggregate declares: ${undeclared.join(", ")}. Event names are permanent: keep a retired Event declared.`,
    );
}
/** Resolves true once `lost()` reports the broker gone, false on a shutdown signal. */
const waitForShutdown = (signal?: AbortSignal, lost?: () => boolean) =>
  new Promise<boolean>((resolve) => {
    const stop = () => done(false);
    const done = (brokerLost: boolean) => {
      clearInterval(timer);
      process.off("SIGTERM", stop);
      process.off("SIGINT", stop);
      signal?.removeEventListener("abort", stop);
      resolve(brokerLost);
    };
    const timer = setInterval(() => lost?.() && done(true), 1000);
    process.once("SIGTERM", stop);
    process.once("SIGINT", stop);
    signal?.addEventListener("abort", stop, { once: true });
    if (signal?.aborted) stop();
  });
/**
 * Service contracts default to the Register and are never inferred from the config,
 * so a config cannot leave out a registered service or Readmodel helper.
 */
export function createApplication<
  S = RegisteredServices,
  R = RegisteredReadmodelHelpers,
>(input: ApplicationConfig<NoInfer<S>, NoInfer<R>>) {
  const config: ResolvedApplicationConfig = resolveComponents(input as any);
  const store = config.store,
    transport = config.transport,
    logger = config.logger ?? createLogger(),
    // Commands read it as `command.now`; Saga Messages are stamped, retried and due on it.
    clock = config.clock ?? (() => new Date());
  // Live subscribers in this process share one Transport subscription per topic.
  const live = sharedSubscriptions(transport);
  // Debug summaries of where each Consumer group, the Outbox and Readmodel reads spend time.
  const consumersLogger = logger.child({ component: "consumers" }),
    commandsLogger = logger.child({ component: "commands" }),
    outboxLogger = logger.child({ component: "outbox" }),
    readmodelsLogger = logger.child({ component: "readmodels" }),
    storeLogger = logger.child({ component: "store" });
  const timings = {
    consumers: createTimings(consumersLogger, "Consumer group timings", "consumerGroup"),
    outbox: createTimings(outboxLogger, "Outbox timings", "consumerGroup"),
    reads: createTimings(
      readmodelsLogger,
      "Readmodel read timings",
      "readmodelGroup",
    ),
  };
  if (config.services !== undefined && config.setupServices)
    throw new Error("Configure services or setupServices, not both");
  if (
    config.readmodelServices !== undefined &&
    (!config.readmodelServices ||
      typeof config.readmodelServices !== "object" ||
      Array.isArray(config.readmodelServices))
  )
    throw new Error("readmodelServices must be an object of Readmodel helpers");
  // Read-only, so a Reaction cannot keep state there that a rebuild would see differently.
  const readOnly = () => {
    throw new TypeError(
      "Readmodel helpers are read-only: keep state in the document, not in readmodelServices",
    );
  };
  const readmodelServices =
    config.readmodelServices &&
    new Proxy(config.readmodelServices, {
      set: readOnly,
      defineProperty: readOnly,
      deleteProperty: readOnly,
    });
  // Application service contexts read through `currentServices`, so setup can fill them in later.
  let services: Record<string, any> | undefined = config.setupServices
    ? undefined
    : (config.services ?? {});
  const resources = new AsyncDisposableStack();
  let servicesReady: Promise<void> | undefined,
    preparing: Promise<void> | undefined,
    starting: Promise<unknown> | undefined,
    closing: Promise<void> | undefined;
  const currentServices = (): Record<string, any> => {
    if (services) return services;
    throw new Error(
      closing
        ? "Application is closed; its services were cleaned up"
        : "Application services are not ready; await initialize() first",
    );
  };
  const setupServices = () =>
    (servicesReady ??= (async () => {
      if (!config.setupServices) return;
      try {
        const ready = await config.setupServices({ resources, logger });
        if (!ready || typeof ready !== "object")
          throw new Error("setupServices must return the services object");
        services = ready;
      } catch (error) {
        // A failed setup is not retried: cleanup has run and its resources are gone.
        const cleanup = await resources.disposeAsync().then(
          () => undefined,
          (failure: unknown) => ({ failure }),
        );
        throw cleanup
          ? new AggregateError(
              [error, cleanup.failure],
              "Application service setup failed and so did its cleanup",
            )
          : error;
      }
    })());
  const groups: Record<string, ReadmodelGroup> = Object.fromEntries(
      Object.entries(config.readmodels ?? {}).map(([name, group]) => {
        const store = group.store ?? config.readmodelStore;
        if (!store)
          throw new Error(
            `Readmodel group ${name} requires a store or Application readmodelStore`,
          );
        return [name, { ...group, store }];
      }),
    ),
    sagas = config.sagas ?? {},
    workers = config.workers ?? {};
  /*
   * The highest version starts instances and runs restarts; every version runs the instances pinned to
   * it. Correlated Events route by every version's `correlate`, and `reads` waits on every version's groups.
   */
  const workflows = Object.fromEntries(
    Object.entries(config.workflows ?? {}).map(([name, entry]) => {
      const versions = "versions" in entry ? entry.versions : [entry];
      for (const version of versions)
        if (version.name !== undefined && version.name !== name)
          throw new Error(`Workflow ${version.name} is registered as ${name}`);
      // Newest first, so a lookup across versions finds the newest declaration.
      const byVersion = new Map(
        versions.toSorted((a, b) => b.version - a.version).map((version) => [version.version, version]),
      );
      if (!versions.length || byVersion.size !== versions.length)
        throw new Error(`Workflow ${name} needs distinct versions`);
      const latest = byVersion.values().next().value!;
      const correlated = versions.flatMap((version) => Object.keys(version.correlate ?? {}));
      const events = [...new Set([latest.start, ...correlated])].filter((event) => event !== undefined);
      const reads = [...new Set(versions.flatMap((version) => version.reads ?? []))];
      return [name, { ...latest, versions: byVersion, events, reads }];
    }),
  );
  type Workflow = (typeof workflows)[string];
  const readmodelEvents = Object.fromEntries(
    Object.entries(groups).map(([name, group]) => [
      name,
      [
        ...new Set(
          reactionParts(group.models).flatMap(([, part]) =>
            Object.keys(part.reactions),
          ),
        ),
      ],
    ]),
  );
  // A Saga reads only the Events it reacts to; `through` proves progress across the others.
  const sagaEvents = Object.fromEntries(
    Object.entries(sagas).map(([name, saga]) => [
      name,
      Object.keys(saga.reactions),
    ]),
  );
  // Computed on a group's first rebuild; other startups never serialize definitions.
  const reconstructions: Record<string, string> = {};
  const reconstruction = (name: string) => {
    if (reconstructions[name]) return reconstructions[name];
    const group = groups[name]!;
    const schemas = { ...config.schemas, ...group.schemas };
    const source = JSON.stringify(
      {
        // Resolved, so unrelated Application schemas do not invalidate a pending rebuild.
        models: Object.fromEntries(
          Object.entries(group.models).map(([model, definition]) => [
            model,
            { ...definition, schema: resolveSchema(definition.schema, schemas) },
          ]),
        ),
        declarations: input.readmodels?.[name]?.models,
        reads: group.reads ?? [],
        version: group.rebuildVersion,
        events: Object.fromEntries(
          (config.aggregates ?? []).flatMap((aggregate) =>
            Object.entries(aggregate.eventSchemas ?? {}).filter(([event]) =>
              readmodelEvents[name]!.includes(event),
            ),
          ),
        ),
      },
      (_key, value) =>
        typeof value === "function" ? value.toString() : value,
    );
    return (reconstructions[name] = createHash("sha256")
      .update(canonical(JSON.parse(source)))
      .digest("hex"));
  };
  const owner = randomUUID(),
    validate = config.validate ?? createValidator({ schemas: config.schemas }),
    storeValidate =
      config.validate ??
      createValidator({ schemas: config.schemas, mutate: false });
  const workerByType = new Map<string, string>();
  // Groups whose schema changed, with the active generation they had at registration.
  const requiredRebuilds = new Map<string, string | null>();
  const stops: Stop[] = [],
    // Stopped after everything else: Gateways, Workers and the Outbox may still be waiting on a Command's reply.
    commandStops: Stop[] = [],
    hooks = new Map<string, ((...args: any[]) => unknown)[]>();
  // Rebuilds and direct kernel.handle() calls, which close() drains before cleaning up services.
  const active = new Set<Promise<unknown>>();
  const track = <T>(work: Promise<T>) => {
    const done = () => void active.delete(work);
    active.add(work);
    work.then(done, done);
    return work;
  };
  const wakeups = new Set<() => void>();
  let running = false,
    started = false;
  const shutdownTimeout = config.shutdownTimeout ?? 10000,
    shutdownReserve = Math.min(2000, shutdownTimeout / 5),
    // A Command handler or live Reaction slower than this logs a warning.
    slowHandlerThreshold = config.slowHandlerThreshold ?? 1000;
  let waking: ReturnType<typeof setTimeout> | undefined;
  let wakeVersion = 0;
  // Every append notifies, and a wake costs each idle loop a tick (metadata reads and a log
  // read), so wakes within 10 ms are merged: ticks stop growing with the append rate.
  const wake = () => {
    waking ??= setTimeout(() => {
      waking = undefined;
      // Counts delivered wakes, not calls: under load calls never stop, so the Outbox would never pause.
      wakeVersion++;
      for (const resolve of [...wakeups]) resolve();
    }, 10);
  };
  const pause = (ms = config.pollInterval ?? 1000) =>
    new Promise<void>((resolve) => {
      const done = () => {
        clearTimeout(timer);
        wakeups.delete(done);
        resolve();
      };
      const timer = setTimeout(done, ms);
      wakeups.add(done);
    });
  /** One Partition of a partitioned Consumer group; absent for a group of one Partition. */
  type Slice = { partition: number; count: number };
  const partitionsOf = (definition: { partitions?: number }) =>
    definition.partitions ?? 1;
  const sliceOf = (count: number, partition: number): Slice | undefined =>
    count > 1 ? { partition, count } : undefined;
  /** Whether this Partition owns a document (`model/id`) or Saga instance. */
  const owns = (slice: Slice | undefined, key: string) =>
    !slice || keyPartition(key, slice.count) === slice.partition;
  const leaseId = (lease: Lease) => consumerId(lease.group, lease.partition);
  for (const [kind, definitions] of [
    ["Readmodel group", groups],
    ["Saga", sagas],
    ["Workflow", workflows],
  ] as const)
    for (const [name, definition] of Object.entries<{ partitions?: number }>(definitions)) {
      // `#` separates a Consumer group from its Partition in ids and stored records.
      if (name.includes("#"))
        throw new Error(`${kind} name ${name} must not contain "#"`);
      const count = partitionsOf(definition);
      if (!Number.isInteger(count) || count < 1 || count > 128)
        throw new Error(
          `${kind} ${name} partitions must be an integer from 1 to 128`,
        );
    }
  /** Each Saga's and Workflow's `from`, checked and in the form its layout row records. */
  const starts = new Map<string, RecordedStart>([
    ...Object.entries(sagas).map(
      ([name, saga]) => [`saga:${name}`, recordedStart(`Saga ${name}`, saga.from)] as const,
    ),
    ...Object.entries(workflows).map(
      ([name, workflow]) =>
        [workflowGroup(name), recordedStart(`Workflow ${name}`, workflow.from)] as const,
    ),
  ]);
  /** The log's head when this instance started: `now` for a group it starts, so no Event recorded after `start()` is skipped. */
  let startedAt: Position | null = null;
  /** The Position a new group starts after; null is before the first Event. */
  const startPosition = async (from: RecordedStart) =>
    from === "beginning"
      ? null
      : from === "now"
        ? startedAt
        : "date" in from
          ? store.positionAt(new Date(from.date))
          : decodePosition(from.position);
  const consumers = createConsumerGroups({
    store,
    owner,
    leaseDuration: config.leaseDuration ?? 30000,
    logger: consumersLogger,
    timings: timings.consumers,
    pause,
    wakes: () => wakeVersion,
    running: () => running,
  });
  const activeGenerations = new Map<string, string>();
  /*
   * Each Partition's Checkpoint as this instance last read or committed it, by Consumer group id.
   * Only the Lease holder writes it at that epoch (and Readmodel generation): a skip or a new owner
   * raises the epoch first, a swap changes the generation, and a layout switch forgets it. So status
   * updates and ticks answer from here instead of a store transaction each.
   */
  const checkpoints = new Map<
    string,
    { epoch: number; generation?: string; position: Position | null }
  >();
  const knownCheckpoint = (id: string, epoch?: number, generation?: string) => {
    const known = checkpoints.get(id);
    return known && known.epoch === epoch && known.generation === generation
      ? known
      : undefined;
  };
  /** A kind's Checkpoint: from memory while this instance holds the Lease, else from `read`. */
  const rememberedCheckpoint =
    (
      id: string,
      read: () => Promise<Position | null>,
      generation?: () => string | undefined,
    ) =>
    async () => {
      const known = knownCheckpoint(id, consumers.lease(id)?.epoch, generation?.());
      return known ? known.position : read();
    };
  const forgetCheckpoints = (group: string) => {
    for (const id of checkpoints.keys())
      if (id === group || id.startsWith(`${group}#`)) checkpoints.delete(id);
  };
  for (const [name, group] of Object.entries(groups))
    for (let p = 0; p < partitionsOf(group); p++) {
      const partition = partitionsOf(group) > 1 ? p : undefined;
      const id = consumerId(`readmodels:${name}`, p);
      consumers.define(id, {
        checkpoint: rememberedCheckpoint(
          id,
          () => group.store.checkpoint(name, { partition }),
          () => activeGenerations.get(name),
        ),
        store: async () => group.store.storeId?.(),
        fence: (epoch) => group.store.fence(name, epoch, partition),
        skip: {
          prepare: (epoch, position) =>
            group.store.prepareSkip(name, epoch, position, partition),
          finish: (epoch, position) =>
            group.store.batch(name, epoch, position, [], { partition }),
        },
      });
    }
  for (const [group, definition] of [
    ...Object.entries(sagas).map(([name, saga]) => [`saga:${name}`, saga] as const),
    ...Object.entries(workflows).map(([name, workflow]) => [workflowGroup(name), workflow] as const),
  ])
    for (let p = 0; p < partitionsOf(definition); p++) {
      const id = consumerId(group, p);
      consumers.define(id, {
        checkpoint: rememberedCheckpoint(id, () => store.checkpoint(group, p)),
      });
    }
  consumers.define("relay", {
    checkpoint: rememberedCheckpoint("relay", () => store.checkpoint("relay")),
  });
  // The Outbox delivers rows, not Events: it has no Checkpoint to read.
  consumers.define("outbox", { checkpoint: async () => null });
  const readmodels = createReadmodels(
    groups,
    live,
    async (name) =>
      (await store.consumers()).some(
        (status) =>
          status.group === `readmodels:${name}` && status.state === "halted",
      ),
    (name) => requiredRebuilds.has(name),
    timings.reads,
  );
  const messageSchemas = new Map<string, Schema>();
  // A Worker's Message schema is checked before the Message is recorded, never after.
  const assertMessage = async (entry: OutboxEntry) => {
    const replyTo = entry.kind === "message" && (entry.message as Message).replyTo;
    // A Workflow's `ask` sets its own reply; only a Saga names a handler that may be missing.
    if (replyTo && "saga" in replyTo && !sagas[replyTo.saga]?.messages?.[replyTo.name])
      throw new Error(`Saga ${replyTo.saga} handles no Saga Message ${replyTo.name} for a Worker's reply`);
    const schema =
      entry.kind === "message" && messageSchemas.get(entry.message.name);
    if (!schema) return;
    try {
      await storeValidate(schema, entry.message.payload);
    } catch (error) {
      throw new Error(
        `Queued Message ${entry.message.name} payload is invalid: ${errorMessage(error)}`,
      );
    }
  };
  // The Message routing table: Message type, then Worker, then queue.
  const workerQueue = (worker: string) => `${config.name}.workers.${worker}`;
  const routing: MessageRouting = {
    queue: (name) => {
      const worker = workerByType.get(name);
      if (!worker) throw new Error(`Queued Message ${name} has no Worker`);
      return workerQueue(worker);
    },
    check: assertMessage,
  };
  const checkMetadata = (metadata: JsonObject) =>
    assertMetadata(metadata, {
      validate,
      schema: config.metadataSchema,
      maxBytes: config.metadataMaxBytes,
    });
  // Built-in members win over the Application's own.
  const withLegacyApp = <T extends { getServices(): any; getReadmodels(): Readmodels }>(app: T): T =>
    config.legacyApp ? { ...config.legacyApp(app), ...app } : app;
  const kernel = createKernel({
    definitions: config.aggregates ?? [],
    store,
    routing,
    services: () =>
      withLegacyApp({
        getServices: currentServices,
        getReadmodels: () => readmodels as unknown as Readmodels,
      }),
    readmodels,
    validate,
    metadataSchema: config.metadataSchema,
    metadataMaxBytes: config.metadataMaxBytes,
    clock,
    logger: commandsLogger,
  });
  const model = immutable({
    aggregates: config.aggregates ?? [],
    readmodels: Object.fromEntries(
      Object.entries(groups).map(([name, group]) => [
        name,
        { models: group.models, reads: group.reads ?? [] },
      ]),
    ),
    sagas,
    workers: Object.fromEntries(
      Object.entries(workers).map(([name, worker]) => [
        name,
        { handles: worker.handles },
      ]),
    ),
    gateways: Object.keys(config.gateways ?? {}),
  });
  const on = (hook: string, callback: (...args: any[]) => unknown) => {
    if (!["start", "shutdown", "gateway"].includes(hook))
      throw new Error(`Unknown hook ${hook}`);
    if (!hooks.has(hook)) hooks.set(hook, []);
    hooks.get(hook)!.push(callback);
  };
  const invoke = async (hook: string, ...args: any[]) => {
    const callbacks = [...(hooks.get(hook) ?? [])];
    if (hook === "shutdown") callbacks.reverse();
    for (const callback of callbacks) await callback(...args);
  };
  const sendCommand = (
    input: CommandInput,
    options?: { deadline?: number; lane?: CommandLane },
  ) => {
    const promise = (async (): Promise<Outcome> => {
      const command = createCommand({
        ...input,
        traceparent: input.traceparent ?? traceparent(),
      });
      try {
        await assertMetadata(command.metadata, {
          validate,
          schema: config.metadataSchema,
          maxBytes: config.metadataMaxBytes,
        });
      } catch (error) {
        throw new Rejection("validation", errorMessage(error));
      }
      const start = Date.now();
      const outcome = await withSpan("eventfabric.send", command, () =>
        transport.send(command, options),
      );
      count("eventfabric.commands", { outcome: outcome.status });
      measure("eventfabric.command.duration", Date.now() - start);
      wake();
      return outcome;
    })();
    return Object.assign(promise, {
      await: (pattern: EventPattern | readonly EventPattern[]) =>
        promise.then((outcome) => awaitEvent(outcome, pattern)),
    });
  };
  /** The cause a Saga Message inherits Metadata, Correlation and trace from. */
  type MessageCause = {
    id: string;
    correlationId: string;
    metadata: Metadata;
    traceparent?: string;
  };
  /** Builds a Saga Message to a Saga's handler; `to`, the handler's identity or, for `start`, its own id picks the instance. */
  const sagaMessage = async (
    saga: string,
    name: string,
    payload: unknown,
    options: SagaMessageOptions = {},
    cause?: MessageCause,
  ): Promise<SagaMessage> => {
    const handler = sagas[saga]?.messages?.[name];
    if (!handler)
      throw new Error(
        sagas[saga]
          ? `Saga ${saga} handles no Saga Message ${name}`
          : `Unknown Saga ${saga}`,
      );
    const spec = messageSpec(handler),
      id = options.id ?? uuidv7(),
      body = toJson(payload ?? {}, "Payload");
    if (spec.schema) await validate(spec.schema, body);
    const instance =
      options.to ??
      (typeof spec.identity === "string"
        ? (body as JsonObject)?.[spec.identity]
        : spec.identity?.(immutable({ id, payload: structuredClone(body) }))) ??
      (spec.start ? id : undefined);
    if (typeof instance !== "string" || !instance)
      throw new Error(`Saga Message ${saga}.${name} needs an instance id: pass \`to\``);
    return envelope(`saga:${saga}`, instance, name, id, body, options, cause);
  };
  /** A Saga Message to `group`'s `instance`: it inherits `cause`'s Metadata, Correlation and trace. */
  const envelope = async (
    group: string,
    instance: string,
    name: string,
    id: string,
    payload: unknown,
    options: SagaMessageOptions,
    cause?: MessageCause,
  ): Promise<SagaMessage> => {
    const metadata = toJson({ ...cause?.metadata, ...options.metadata }, "Metadata");
    await checkMetadata(metadata);
    const trace = cause ? cause.traceparent : traceparent();
    return {
      id,
      group,
      instance,
      name,
      payload,
      metadata,
      correlationId: cause?.correlationId ?? id,
      ...(cause ? { causationId: cause.id } : {}),
      timestamp: clock().toISOString(),
      ...(trace ? { traceparent: trace } : {}),
      dueAt: options.at === undefined ? clock().toISOString() : instant(options.at, "Saga Message due time"),
    };
  };
  /** Sends a Saga Message outside any Saga commit: from a Worker, a Task, a Gateway or a plugin. */
  const sendSagaMessage = async (
    saga: string,
    name: string,
    payload?: unknown,
    options?: SagaMessageOptions,
    cause?: MessageCause,
  ) => {
    if (!store.recordSagaMessages)
      throw new Error("This Event store does not keep Saga Messages");
    await store.recordSagaMessages([
      await sagaMessage(saga, name, payload, options, cause),
    ]);
    count("eventfabric.saga_messages.sent", { saga });
    wake();
  };
  const workflowOf = (name: string) => {
    const workflow = workflows[name];
    if (!workflow) throw new Error(`Unknown Workflow ${name}`);
    return workflow;
  };
  /**
   * A Saga Message to a Workflow instance: `start`, `signal:<name>`, `reply:<step>`, or a Deadline
   * named after its step (ADR-0021).
   */
  const workflowMessage = (
    workflow: string,
    instance: string,
    name: string,
    payload: unknown,
    options: SagaMessageOptions = {},
    cause?: MessageCause,
  ) =>
    envelope(
      workflowGroup(workflow),
      instance,
      name,
      options.id ?? uuidv7(),
      toJson(payload ?? null, "Payload"),
      options,
      cause,
    );
  const sendWorkflowMessage = async (
    workflow: string,
    name: string,
    payload: unknown,
    options: SagaMessageOptions & { to: string },
    cause?: MessageCause,
  ) => {
    workflowOf(workflow);
    if (!store.recordSagaMessages)
      throw new Error("This Event store does not keep Saga Messages");
    await store.recordSagaMessages([
      await workflowMessage(workflow, options.to, name, payload, options, cause),
    ]);
    wake();
  };
  const finished = (status: WorkflowStatus) =>
    status !== "running" && status !== "drifted";
  const loadWorkflow = (name: string, id: string) => {
    workflowOf(name);
    if (!store.loadWorkflow) throw new Error("This Event store does not keep Workflows");
    return store.loadWorkflow(workflowGroup(name), id);
  };
  /**
   * A Signal's payload as sent to instance `id`: the version a running instance pinned must declare it
   * (any registered version, for an instance not running yet), and that declaration's schema checks it.
   */
  const signalPayload = async (name: string, id: string, signal: SignalInput) => {
    if (signal.workflow !== name)
      throw new Error(`Workflow ${name} cannot take a Signal of Workflow ${signal.workflow}`);
    const workflow = workflowOf(name),
      record = await loadWorkflow(name, id),
      pinned = record && !finished(record.status) ? workflow.versions.get(record.version) : undefined;
    const declared = pinned
      ? pinned.signals?.[signal.name]
      : [...workflow.versions.values()].map((version) => version.signals?.[signal.name]).find(Boolean);
    if (!declared) throw new Error(`Workflow ${name} has no Signal ${signal.name}`);
    const payload = toJson(signal.payload ?? null, "Signal");
    // A Rejection, so a Gateway answers 400 for a Signal its caller got wrong.
    if (declared.schema)
      try {
        await validate(declared.schema, payload);
      } catch (error) {
        throw new Rejection("validation", `Invalid Signal ${signal.name}: ${errorMessage(error)}`);
      }
    return payload;
  };
  /**
   * A context's `startWorkflow`, `sendSignal` and `queryWorkflow`, caused by `cause`. With `record`, a Saga
   * records starts and Signals with its commit, and its start skips the sender's check; the owner's still applies.
   */
  const workflowCalls = (
    cause?: MessageCause,
    record?: (message: Promise<SagaMessage>) => void,
  ): WorkflowCalls<any> => {
    const send = async (message: Promise<SagaMessage>) => {
      if (!store.recordSagaMessages)
        throw new Error("This Event store does not keep Saga Messages");
      await store.recordSagaMessages([await message]);
      wake();
    };
    const startMessage = async (
      name: string,
      id: string,
      input: unknown,
      reuse: boolean,
      signal?: SignalInput,
    ) =>
      workflowMessage(
        name,
        id,
        "start",
        {
          input: input ?? null,
          reuse,
          ...(signal ? { signal: { name: signal.name, payload: await signalPayload(name, id, signal) } } : {}),
        },
        {},
        cause,
      );
    const calls = {
      startWorkflow(
        workflow: string | { name: string },
        id: string,
        input?: unknown,
        { reuse = false, signal }: { reuse?: boolean; signal?: SignalInput } = {},
      ) {
        const name = workflowName(workflow);
        workflowOf(name); // a Saga's recorded start is checked here too, not only by the owner
        if (record) return record(startMessage(name, id, input, reuse, signal));
        return (async () => {
          const existing = await loadWorkflow(name, id);
          // A live instance is a no-op, unless the start carries a Signal for it.
          if (existing && !finished(existing.status) && !signal) return;
          // A Rejection, so a Gateway answers 4xx to a caller that names a finished id.
          if (existing && finished(existing.status) && !reuse)
            throw new Rejection(
              "precondition",
              `Workflow ${name} ${id} has finished; pass { reuse: true } to start it again`,
              undefined,
              "workflow-finished",
            );
          // The instance's owner checks again: a start already pending or handled makes this one a no-op.
          await send(startMessage(name, id, input, reuse, signal));
        })();
      },
      sendSignal(id: string, signal: SignalInput) {
        const name = signal.workflow;
        const message = signalPayload(name, id, signal).then((payload) =>
          workflowMessage(name, id, workflowWake.signal(signal.name), payload, {}, cause),
        );
        return record ? record(message) : send(message);
      },
      /** The committed status, `set` view and result; nothing runs the body to answer. */
      async queryWorkflow(workflow: string | { name: string }, id: string) {
        const record = await loadWorkflow(workflowName(workflow), id);
        return (
          record && {
            status: record.status,
            ...(record.state === undefined ? {} : { state: record.state }),
            ...(record.result === undefined ? {} : { result: record.result }),
          }
        );
      },
    };
    return calls as WorkflowCalls<any>;
  };
  /** Sends a live instance the operator's `cancel` or `resume`; its owner acts on it. */
  const operateWorkflow = async (action: "cancel" | "resume", name: string, id: string) => {
    const record = await loadWorkflow(name, id);
    if (!record || finished(record.status)) throw new Error(`Workflow ${name} ${id} is not running`);
    await sendWorkflowMessage(name, action, null, { to: id });
  };
  const applicationWorkflows: WorkflowCalls = workflowCalls();
  const subscribeEvents = (
    handler: (event: RecordedEvent) => unknown,
    { after = null }: { after?: Position | null } = {},
  ): Promise<Stop> => {
    let last = after;
    return live.subscribe("events", (event: RecordedEvent) => {
      // A deposed relay may republish; drop anything at or below the last Position seen.
      if (last && comparePositions(event.position, last) <= 0) return;
      last = event.position;
      return handler(event) as void | Promise<void>;
    });
  };
  /** Publishes an Application Notification to every live subscriber, on every replica; nothing stores it. */
  const notify = async (name: string, payload: unknown = null) => {
    if (typeof name !== "string" || !name)
      throw new Error("A Notification needs a name");
    await transport.publish("notifications", { name, payload });
  };
  const subscribeNotifications = (
    handler: (notification: ApplicationNotification) => unknown,
  ): Promise<Stop> =>
    live.subscribe("notifications", (notification: ApplicationNotification) => {
      handler(notification);
    });
  const pluginContext = {
    ...applicationWorkflows,
    logger,
    config,
    sendCommand,
    sendSagaMessage: ((saga, name, payload, options) =>
      sendSagaMessage(saga, name, payload, options)) as SendSagaMessage<Promise<void>>,
    readmodels: readmodels as unknown as Readmodels,
    get services() {
      return currentServices();
    },
    model,
    on,
    subscribeEvents,
    notify,
    subscribeNotifications,
    awaitCorrelated: (...args: Parameters<typeof api.awaitCorrelated>) =>
      api.awaitCorrelated(...args),
  };
  // Checks the declarations and registers Readmodel schemas; a failure may be retried.
  const prepare = () =>
    (preparing ??= prepareOnce().catch((error) => {
      preparing = undefined;
      throw error;
    }));
  /** Prepares the Application and sets up its services, once however many callers wait. */
  const initialize = async () => {
    if (closing) throw new Error("Application is closed");
    await prepare();
    // close() waits for a setup already running, but none may begin once it has started.
    if (closing) throw new Error("Application is closed");
    await setupServices();
  };
  const prepareOnce = async () => {
    // Startup never migrates; without the Event-type index every Readmodel poll scans the log.
    await store.assertMigrated?.();
    const knownEvents = new Set(
      (config.aggregates ?? []).flatMap((a) =>
        Object.keys(a.events).map((name) => `${a.context}.${a.name}.${name}`),
      ),
    );
    readmodelOrder(groups);
    for (const aggregate of config.aggregates ?? [])
      if (aggregate.eventSchemas)
        store.registerUpcasters(aggregate.eventSchemas);
    for (const [name, definition, kind] of [
      ...Object.entries(sagas).map(
        ([name, saga]) => [name, saga, "Saga"] as const,
      ),
      ...Object.entries(groups).flatMap(([group, definition]) =>
        reactionParts(definition.models).map(
          ([, model, name]) =>
            [`${group}.${name}`, model, "Readmodel"] as const,
        ),
      ),
    ]) {
      assertConsumerShape(kind, name, definition);
      for (const event of [
        ...Object.keys(definition.reactions),
        ...Object.keys(definition.identity ?? {}),
      ])
        if (!knownEvents.has(event))
          throw new Error(`${name} names unknown Event ${event}`);
    }
    for (const [name, saga] of Object.entries(sagas)) {
      for (const dependency of saga.reads ?? [])
        if (!groups[dependency])
          throw new Error(`Unknown declared Readmodel group ${dependency}`);
      if (handlesSagaMessages(saga) && !store.dueSagaMessages)
        throw new Error(`Saga ${name} handles Saga Messages, which this Event store does not keep`);
    }
    for (const [name, workflow] of Object.entries(workflows)) {
      for (const dependency of workflow.reads ?? [])
        if (!groups[dependency])
          throw new Error(`Unknown declared Readmodel group ${dependency}`);
      for (const event of workflow.events)
        if (!knownEvents.has(event))
          throw new Error(`Workflow ${name} names unknown Event ${event}`);
      if (workflow.start && typeof workflow.id !== "function")
        throw new Error(`Workflow ${name} starts on ${workflow.start} and needs an id function`);
      if (!store.commitWorkflow || !store.dueSagaMessages)
        throw new Error(`Workflow ${name} needs an Event store that keeps Workflows and Saga Messages`);
    }
    for (const [name, worker] of Object.entries(workers)) {
      if (!worker.handles.length)
        throw new Error(`Worker ${name} handles no Message types`);
      for (const type of worker.handles) {
        if (workerByType.has(type))
          throw new Error(`Message ${type} has multiple Workers`);
        workerByType.set(type, name);
      }
      for (const [type, schema] of Object.entries(worker.schemas ?? {})) {
        if (!worker.handles.includes(type))
          throw new Error(`Worker ${name} has a schema for unhandled ${type}`);
        if (schema !== undefined) messageSchemas.set(type, schema as Schema);
      }
    }
    await assertDeclaredEvents(store, config.aggregates ?? []);
    for (const [name, group] of Object.entries(groups)) {
      const registered = await group.store.register(name, group.models, {
        validate: group.validate,
        validator: group.validator ?? storeValidate,
        migrations: group.migrations,
        schemas: { ...config.schemas, ...group.schemas },
      });
      if (registered.rebuildRequired)
        requiredRebuilds.set(name, await group.store.activeGeneration(name));
    }
  };
  const scopedReads = (allowed: string[], local: Record<string, any> = {}) =>
    new Proxy(readmodels, {
      get(target, key: string) {
        if (key === "repository")
          return (group: string, name: string) => {
            if (!allowed.includes(group))
              throw new Error(`Undeclared cross-group read ${group}`);
            return local[group]?.[name] ?? target.repository(group, name);
          };
        if (key === "waitFor")
          return (position: Position, names: string[], options?: any) => {
            if (names.some((name) => !allowed.includes(name)))
              throw new Error("Undeclared cross-group read");
            if (names.some((name) => Object.hasOwn(local, name)))
              throw new Error(
                "Cannot wait for the same Readmodel group inside a Reaction",
              );
            return target.waitFor(position, names, options);
          };
        if (!allowed.includes(key))
          throw new Error(`Undeclared cross-group read ${key}`);
        return local[key] ?? target[key];
      },
    });
  // Readmodel Reactions get no `effects`: they may read, never tell or queue.
  const reactionApp = (
    group: string,
    event: Pick<RecordedEvent, "id" | "causationId" | "correlationId">,
    allowed: string[],
    effects?: Effects,
    localReads?: Record<string, any>,
    sendToSaga?: (...args: Parameters<typeof sagaMessage>) => void,
    record?: (message: Promise<SagaMessage>) => void,
  ) => {
    const tell: any = (
      input?: CommandInput,
      options?: { notBefore?: string; replaceKey?: string },
    ) => {
      if (!effects) throw new Error("Readmodel Reactions cannot tell Commands");
      if (!input) return domain;
      effects.tell(input, options);
    };
    const domain = buildDomain(config.aggregates ?? [], tell);
    return withLegacyApp({
      tell,
      getDomain: () => domain,
      getServices: !effects && readmodelServices !== undefined
        ? () => readmodelServices
        : currentServices,
      getReadmodels: () => scopedReads(allowed, localReads),
      logger: consumersLogger.child({
        consumerGroup: group,
        eventId: event.id,
        causationId: event.causationId,
        correlationId: event.correlationId,
      }),
      queue: (...args: Parameters<Effects["queue"]>) => {
        if (!effects)
          throw new Error("Readmodel Reactions cannot queue Messages");
        effects.queue(...args);
      },
      sendSagaMessage: (
        saga: string,
        name: string,
        payload?: unknown,
        options?: SagaMessageOptions,
      ) => {
        if (!sendToSaga)
          throw new Error("Readmodel Reactions cannot send Saga Messages");
        sendToSaga(saga, name, payload, options);
      },
      // Readmodel Reactions get none: they never record effects.
      ...(record ? workflowCalls(event as MessageCause, record) : {}),
    });
  };
  /**
   * Records a Saga commit's Saga Message changes in call order: Deadlines set and cancelled,
   * Saga Messages sent, and a destroyed instance's Deadlines cancelled.
   */
  const sagaMessageOps = () => {
    const ops: Promise<SagaMessageChange>[] = [];
    const add = (op: Promise<SagaMessageChange>) => {
      // Awaited by `settle`; a rejection fails the commit, not the process.
      op.catch(() => {});
      ops.push(op);
    };
    return {
      settle: () => Promise.all(ops),
      for: (
        group: string,
        definition: SagaDefinition,
        cause: MessageCause,
        instance: string,
      ) => {
        let set = false;
        return {
          view: {
            deadline(name: string, at: string | Date, payload: unknown = {}) {
              if (typeof name !== "string" || !definition.deadlines?.[name])
                throw new Error(`Saga ${parseProcessGroup(group)!.name} has no Deadline handler ${name}`);
              set = true;
              add(
                Promise.resolve({
                  send: {
                    id: uuidv7(),
                    group,
                    instance,
                    name,
                    deadline: name,
                    payload: toJson(payload ?? {}, "Payload"),
                    metadata: toJson(cause.metadata, "Metadata"),
                    correlationId: cause.correlationId,
                    causationId: cause.id,
                    timestamp: clock().toISOString(),
                    ...(cause.traceparent ? { traceparent: cause.traceparent } : {}),
                    dueAt: instant(at, "Deadline due time"),
                  },
                }),
              );
            },
            cancelDeadline(name: string) {
              add(Promise.resolve({ cancel: { group, instance, deadline: name } }));
            },
          },
          /** A Workflow start or Signal, recorded with the commit. */
          record: (message: Promise<SagaMessage>) => {
            if (!store.recordSagaMessages)
              throw new Error("This Event store does not keep Saga Messages");
            add(message.then((send) => ({ send })));
          },
          send: (
            saga: string,
            name: string,
            payload?: unknown,
            options?: SagaMessageOptions,
          ) => {
            if (!store.recordSagaMessages)
              throw new Error("This Event store does not keep Saga Messages");
            add(
              sagaMessage(saga, name, payload, options, cause).then((send) => ({
                send,
              })),
            );
          },
          /** A destroyed instance's Deadlines go with it; one that never existed has none. */
          destroyed: (existed: boolean) => {
            if ((existed || set) && Object.keys(definition.deadlines ?? {}).length)
              add(Promise.resolve({ cancel: { group, instance } }));
          },
        };
      },
    };
  };
  const eventIdentity = async (
    definition: ConsumerDefinition,
    event: RecordedEvent,
  ): Promise<Identity> => {
    const handlers = compiledReaction(definition.identity?.[eventName(event)]).fns;
    return handlers.length
      ? await handlers[0]!(authoringView(event))
      : event.stream.id;
  };
  // One Identity rule for every Consumer: a query object only resolves where `query` is given.
  const identityIds = async (
    kind: "Readmodel" | "Saga" | "Workflow",
    selected: Identity,
    query?: (filter: JsonObject) => Promise<string[]>,
  ): Promise<string[]> => {
    if (selected == null) return [];
    const ids =
      query && typeof selected === "object" && !Array.isArray(selected)
        ? await query(selected)
        : Array.isArray(selected)
          ? selected
          : [selected];
    for (const id of ids)
      if (typeof id !== "string")
        throw new Error(`${kind} Identity must return string ids`);
    return ids as string[];
  };
  const react = async (
    definition: ConsumerDefinition,
    event: RecordedEvent,
    id: string,
    document: JsonObject | null,
    app: any,
    // The caller owns `document` and drops it for the result, so it is folded in place.
    owned = false,
    // A Saga instance's `deadline` and `cancelDeadline`; a Saga Message's handler and view.
    saga: { view?: object; reaction?: Reaction; input?: object } = {},
  ): Promise<JsonObject | null> => {
    const reaction = saga.reaction ?? definition.reactions[eventName(event)];
    const input = () => saga.input ?? authoringView(event);
    const { markers, fns } = compiledReaction(reaction);
    if (markers.some((marker) => marker?.existing === true) && !document)
      return document;
    if (markers.some((marker) => marker?.existing === false) && document)
      return document;
    const state = new AggregateState(
      id,
      document ?? definition.initialState ?? {},
      !(owned && document),
    );
    state.attributes._id = id;
    let destroyed = false;
    const api = {
      id,
      get: (path?: string) => state.get(path),
      set: (path: string | JsonObject, value?: any) => state.set(path, value),
      destroy: () => {
        destroyed = true;
      },
      ...saga.view,
    };
    // A false predicate skips the whole Reaction: no function runs and nothing is created.
    for (const marker of markers)
      if (
        marker?.when &&
        !(await marker.when(input(), api, app))
      )
        return document;
    for (const fn of fns) await fn(input(), api, app);
    return destroyed ? null : { ...state.attributes, _id: id };
  };
  /** Whether the `reads` groups have processed the batch's last Event that has a Reaction. */
  const dependenciesReady = async (
    lease: Lease,
    events: RecordedEvent[],
    reads: string[],
    reacts: (event: RecordedEvent) => boolean,
  ) => {
    const last = reads.length ? events.findLast(reacts) : undefined;
    if (
      last &&
      (await readmodels.waitFor(last.position, reads, { timeout: 0 })).behind
    ) {
      await consumers.status(lease, { state: "waiting", waitingOn: reads });
      return false;
    }
    if (consumers.statuses.get(leaseId(lease))?.state === "waiting")
      await consumers.status(lease, { state: "running", waitingOn: undefined });
    return true;
  };
  // `carried` marks a change of the previous rebuild batch, still committing: visible, but not written again.
  type StagedChange = ReadmodelChange & {
    before: JsonObject | null;
    carried?: boolean;
    // The correlations of the Events that made this change; live batches only.
    correlationIds?: string[];
  };
  type Folded = { position: Position | null; changes: StagedChange[] };
  // A retained rebuild page keeps its Event objects, so its tail reuses Identities across folds.
  const identities = new WeakMap<
    RecordedEvent,
    Map<string, PromiseSettledResult<Identity>>
  >();
  // Folds up to `budget` ms of Events into staged changes; committing is the caller's choice.
  /** Runs one Reaction; over the threshold it logs the group and Event (`false`: a rebuild, not logged). */
  const slowReaction = async <T>(
    consumerGroup: string | false,
    event: RecordedEvent,
    run: () => Promise<T>,
  ): Promise<T> => {
    const start = performance.now();
    try {
      return await run();
    } finally {
      const durationMs = Math.round(performance.now() - start);
      if (consumerGroup && durationMs >= slowHandlerThreshold)
        consumersLogger.warn(
          {
            consumerGroup,
            event: eventName(event),
            eventId: event.id,
            stream: streamName(event.stream),
            correlationId: event.correlationId,
            durationMs,
          },
          "Slow Reaction",
        );
    }
  };
  const foldReadmodelBatch = async (
    name: string,
    group: ReadmodelGroup,
    consumerGroup: string,
    page: EventPage,
    generation?: string,
    slice?: Slice,
    {
      budget = 50,
      // Live batches publish each document's `before` with the change; rebuilds publish nothing.
      keepBefore = true,
      carried,
      others,
      checkpoint,
    }: {
      budget?: number;
      keepBefore?: boolean;
      carried?: Folded;
      // A rebuild's cached repositories of the groups it reads.
      others?: Record<string, any>;
      // The Checkpoint the page was read from, when the caller knows it.
      checkpoint?: Position | null;
    } = {},
  ) => {
    const events = page.events;
    if (!events.length)
      return { position: page.through, changes: [], remaining: undefined };
    const changes = new Map<string, StagedChange>(
      carried?.changes.map((change) => [
        JSON.stringify([change.model, change.id]),
        { ...change, carried: true },
      ]),
    );
    // Query Identities check staged documents through an index of them, not one by one.
    const staged = memoryIndexes(
        (model, id) => changes.get(JSON.stringify([model, id]))?.document ?? null,
      ),
      sequence = new Map<string, number>();
    for (const [key, change] of changes) {
      sequence.set(key, sequence.size);
      staged.touch(change.model, change.id);
    }
    const localReads = {
      ...others,
      // Another Partition's documents may be ahead of or behind this Event.
      [name]: slice
        ? new Proxy(
            {},
            {
              get: () => {
                throw new Error(
                  `Readmodel group ${name} has ${slice.count} Partitions, so its Reactions cannot read its own documents`,
                );
              },
            },
          )
        : stagedReads(
            name,
            group,
            config.schemas,
            changes,
            carried
              ? carried.position
              : checkpoint !== undefined
                ? checkpoint
                : await group.store.checkpoint(name, { generation }),
            generation,
          ),
    };
    // Resolve Event-only Identities once per fold; query filters still run against chronological state.
    const prefetched = new Map<string, JsonObject | null>();
    for (const [modelName, definition, part] of reactionParts(group.models)) {
      const ids = new Set<string>();
      for (const event of events) {
        if (!definition.reactions[eventName(event)]) continue;
        const cached = identities.get(event) ?? new Map();
        identities.set(event, cached);
        if (!cached.has(part))
          try {
            cached.set(part, {
              status: "fulfilled",
              value: await eventIdentity(definition, event),
            });
          } catch (reason) {
            // A later failing Identity must not prevent an earlier time-bounded fold from committing.
            cached.set(part, { status: "rejected", reason });
          }
        const selected = cached.get(part)!;
        if (selected.status === "rejected") continue;
        for (const id of Array.isArray(selected.value)
          ? selected.value
          : [selected.value])
          if (typeof id === "string" && owns(slice, `${modelName}/${id}`))
            ids.add(id);
      }
      if (!ids.size) continue;
      for (const id of ids)
        prefetched.set(JSON.stringify([modelName, id]), null);
      for (const document of await group.store.find(
        name,
        modelName,
        { _id: { in: [...ids] } },
        { generation },
      ))
        prefetched.set(
          JSON.stringify([modelName, String(document._id)]),
          document,
        );
    }
    const startedAt = Date.now();
    let last = events[0]!;
    for (const event of events) {
      for (const [modelName, definition, part] of reactionParts(group.models)) {
        if (!definition.reactions[eventName(event)]) continue;
        const selected = identities.get(event)!.get(part)!;
        if (selected.status === "rejected") {
          // A retry re-runs it, even when an adapter returns the same Event objects again.
          identities.get(event)!.delete(part);
          throw selected.reason;
        }
        const ids = await identityIds(
          "Readmodel",
          selected.value,
          async (filter) => {
            const byId = new Map(
              (
                await group.store.find(name, modelName, filter, { generation })
              ).map((document) => [String(document._id), document]),
            );
            for (const [id, document] of byId)
              prefetched.set(JSON.stringify([modelName, id]), document);
            const found = staged.candidates(modelName, filter);
            if (!found) {
              for (const entry of changes.values())
                if (entry.model === modelName) {
                  if (entry.document && matches(entry.document, filter))
                    byId.set(entry.id, entry.document);
                  else byId.delete(entry.id);
                }
              return [...byId.keys()];
            }
            // The same result as the loop above: stored matches in place, then staged ones in staging order.
            const keyOf = (id: string) => JSON.stringify([modelName, id]);
            for (const id of [...byId.keys()]) {
              const entry = changes.get(keyOf(id));
              if (!entry) continue;
              if (entry.document && matches(entry.document, filter))
                byId.set(id, entry.document);
              else byId.delete(id);
            }
            const added = [...found]
              .filter((id) => !byId.has(id))
              .map(keyOf)
              .filter((key) => {
                const document = changes.get(key)?.document;
                return document && matches(document, filter);
              })
              .sort((a, b) => sequence.get(a)! - sequence.get(b)!);
            for (const key of added) {
              const entry = changes.get(key)!;
              byId.set(entry.id, entry.document!);
            }
            return [...byId.keys()];
          },
        );
        for (const id of ids) {
          if (!owns(slice, `${modelName}/${id}`)) continue;
          const key = JSON.stringify([modelName, id]);
          const previous = changes.get(key);
          const before = previous
            ? previous.document
            : prefetched.has(key)
              ? prefetched.get(key)!
              : await group.store.findOne(
                  name,
                  modelName,
                  { _id: id },
                  { generation },
                );
          const app = reactionApp(
            consumerGroup,
            event,
            [...(group.reads ?? []), name],
            undefined,
            localReads,
          );
          const document = await slowReaction(
            keepBefore && consumerGroup,
            event,
            () =>
              withSpan("eventfabric.reaction", event, () =>
                react(
                  definition,
                  event,
                  id,
                  before,
                  app,
                  previous ? !previous.carried : !keepBefore,
                ),
              ),
          );
          const correlationIds =
            previous && !previous.carried ? previous.correlationIds : undefined;
          changes.set(key, {
            model: modelName,
            id,
            document,
            before: previous ? previous.before : before,
            ...(keepBefore && {
              correlationIds: correlationIds?.includes(event.correlationId)
                ? correlationIds
                : [...(correlationIds ?? []), event.correlationId],
            }),
          });
          if (!sequence.has(key)) sequence.set(key, sequence.size);
          staged.touch(modelName, id);
        }
      }
      last = event;
      if (Date.now() - startedAt >= budget) break;
    }
    const incomplete = last !== events.at(-1);
    return {
      position: incomplete ? last.position : page.through,
      changes: [...changes.values()].filter((change) => !change.carried),
      remaining: incomplete
        ? { ...page, events: events.slice(events.indexOf(last) + 1) }
        : undefined,
    };
  };
  const stripBefore = (changes: StagedChange[]) =>
    changes.map(({ before, correlationIds, ...change }) => change);
  // Live processing renews the Lease before commit and notifies subscribers after it.
  // Readmodel groups that other groups or Sagas declare in `reads`.
  let readBy: Set<string> | undefined;
  const readGroups = () =>
    (readBy ??= new Set(
      [...Object.values(groups), ...Object.values(sagas)].flatMap(
        (definition) => definition.reads ?? [],
      ),
    ));
  const readmodelBatch = async (
    name: string,
    group: ReadmodelGroup,
    lease: Lease,
    page: EventPage,
    generation: string,
    checkpoint: Position | null,
    slice?: Slice,
  ) => {
    const { position, changes } = await foldReadmodelBatch(
      name,
      group,
      lease.group,
      page,
      generation,
      slice,
      { checkpoint },
    );
    if (!position) return;
    // The store refuses a commit under an older epoch, so this only spares one the renewal timer
    // already knows is late, without a renewal round trip per batch.
    const renewed = consumers.lease(leaseId(lease));
    if (
      parseTime(
        renewed?.epoch === lease.epoch ? renewed.expiresAt : lease.expiresAt,
      ) <= Date.now()
    )
      throw new LeaseLostError("Readmodel Lease lost before commit");
    try {
      await group.store.batch(
        name,
        lease.epoch,
        position,
        stripBefore(changes),
        { generation, partition: slice?.partition, requireActive: true },
      );
    } catch (error) {
      // A swap published another generation: the next tick folds these Events again against it.
      if (error instanceof StaleGenerationError) {
        activeGenerations.delete(name);
        return;
      }
      throw error;
    }
    checkpoints.set(leaseId(lease), {
      epoch: lease.epoch,
      generation,
      position,
    });
    // A group or Saga that reads this group waits for it to pass its Events: wake it here, and on other
    // replicas through the notification, which is sent for that even without changes.
    const read = readGroups().has(name);
    if (read) wake();
    if (!changes.length && !read) return;
    const notification: BatchNotification = {
      kind: "readmodel",
      group: name,
      position,
      changes: changes.map((c) => ({
        model: c.model,
        id: c.id,
        before: c.before,
        after: c.document,
        correlationIds: c.correlationIds ?? [],
      })),
    };
    await transport
      .publish("readmodels", notification)
      .catch((error) =>
        readmodelsLogger.warn(
          { consumerGroup: `readmodels:${name}`, error: errorMessage(error) },
          "Notification delivery failed",
        ),
      );
  };
  /*
   * Nobody reads a shadow generation, so rebuild batches are larger than live ones: fewer
   * Checkpoint reads, transactions and prefetches per Event (about twice as fast).
   */
  const rebuildPage = 5000,
    rebuildFoldBudget = 2000;
  // Awaited later; until then a rejection must not count as unhandled.
  const background = <T>(promise: Promise<T>) => {
    promise.catch(() => {});
    return promise;
  };
  type Committing = { folded: Folded; commit: Promise<void> };
  const rebuildDocuments =
    config.rebuildCacheBytes === 0
      ? undefined
      : rebuildCache(config.rebuildCacheBytes ?? defaultRebuildCacheBytes);
  const rebuildScope = (name: string, generation: string, slice?: Slice) =>
    JSON.stringify([name, generation, slice?.partition ?? null]);
  /*
   * A rebuild writes an unpublished generation: its caller renews the Leases and nobody is notified.
   * It folds against the previous batch while that one commits, then starts its own commit once
   * the previous one has, so at most one commit is in flight and commits stay in order.
   */
  const rebuildBatch = async (
    name: string,
    group: ReadmodelGroup,
    lease: Lease,
    page: EventPage,
    generation: string,
    previous?: Committing,
    slice?: Slice,
  ) => {
    const scope = rebuildScope(name, generation, slice),
      key = (change: StagedChange) =>
        scope + JSON.stringify([change.model, change.id]);
    const last = page.events.at(-1)?.position;
    const fold = () =>
      foldReadmodelBatch(
        name,
        rebuildDocuments
          ? cachedReads(
              rebuildDocuments,
              scope,
              name,
              group,
              config.schemas,
              generation,
              new Set(previous?.folded.changes.map(key)),
            )
          : group,
        lease.group,
        page,
        generation,
        slice,
        {
          budget: rebuildFoldBudget,
          keepBefore: false,
          carried: previous?.folded,
          others:
            rebuildDocuments && last && group.reads?.length
              ? otherGroupReads(
                  rebuildDocuments,
                  scope,
                  readmodels,
                  Object.fromEntries(
                    group.reads.map((read) => [
                      read,
                      Object.keys(groups[read]!.models),
                    ]),
                  ),
                  last,
                )
              : undefined,
        },
      );
    const folded = rebuildDocuments
      ? await withRebuildMemo(rebuildDocuments, scope, fold)
      : await fold();
    for (const change of folded.changes)
      rebuildDocuments?.set(scope, key(change), change.document, {
        own: true,
        folded: true,
        document: { model: change.model, id: change.id },
      });
    await previous?.commit;
    const commit = folded.position
      ? background(
          group.store.batch(
            name,
            lease.epoch,
            folded.position,
            stripBefore(folded.changes),
            {
              generation,
              slice: slice?.partition,
              reconstruction: reconstruction(name),
            },
          ),
        )
      : Promise.resolve();
    return { remaining: folded.remaining, committing: { folded, commit } };
  };
  /** Runs a batch of Events through a Saga and commits states, Outbox and Checkpoint in one transaction. */
  const sagaBatch = async (
    definition: (typeof sagas)[string],
    lease: Lease,
    events: RecordedEvent[],
    // The page's `through`: past these Events and any it excluded.
    position: Position,
    slice?: Slice,
  ) => {
    const changes: { id: string; state: JsonObject | null }[] = [],
      outbox: OutboxEntry[] = [],
      messages = sagaMessageOps(),
      // Later Events in the batch see the states earlier ones wrote.
      states = new Map<string, JsonObject | null>();
    for (const event of events) {
      if (!definition.reactions[eventName(event)]) continue;
      consumers.observe(leaseId(lease), event);
      // One recorder per instance: its effects' keys don't depend on which other instances share the batch.
      const recorders = new Map<string, Effects>();
      for (const id of await identityIds("Saga", await eventIdentity(definition, event))) {
        if (!owns(slice, id)) continue;
        const effects =
          recorders.get(id) ??
          recordEffects(event, lease.group, { routing, instance: id });
        recorders.set(id, effects);
        const current = states.has(id)
          ? states.get(id)!
          : await timings.consumers.time(leaseId(lease), "loadState", () =>
              store.loadSaga(lease.group, id),
            );
        const instance = messages.for(lease.group, definition, event, id);
        const app = reactionApp(
          lease.group,
          event,
          definition.reads ?? [],
          effects,
          undefined,
          instance.send,
          instance.record,
        );
        const reacted = await timings.consumers.time(leaseId(lease), "react", () =>
          slowReaction(lease.group, event, () =>
            withSpan(
              "eventfabric.reaction",
              event,
              () =>
                react(definition, event, id, current, app, false, {
                  view: instance.view,
                }),
              true,
            ),
          ),
        );
        // Saga state is stored as JSON: a Date becomes its ISO text for every later Event, as it is after a reload.
        const state = reacted && toJson(reacted, "Saga state");
        if (!state) instance.destroyed(!!current);
        changes.push({ id, state });
        states.set(id, state);
      }
      for (const effects of recorders.values())
        outbox.push(...(await effects.seal(checkMetadata)));
    }
    const sagaMessages = await messages.settle();
    await timings.consumers.time(leaseId(lease), "commit", () =>
      sagaMessages.length
        ? store.commitSaga(lease, position, changes, outbox, {
            changes: sagaMessages,
          })
        : store.commitSaga(lease, position, changes, outbox),
    );
    checkpoints.set(leaseId(lease), { epoch: lease.epoch, position });
    timings.consumers.add(leaseId(lease), "events", events.length);
    // Lets the Outbox deliver what the batch recorded; a batch that only moved its Checkpoint has nothing to send.
    if (changes.length || outbox.length || sagaMessages.length) wake();
  };
  /** A Checkpoint kept in the Event store (Sagas, the relay), from memory while this instance holds its Lease. */
  const storedCheckpoint = async (lease: Lease) => {
    const id = leaseId(lease);
    const known = knownCheckpoint(id, lease.epoch);
    if (known) return known.position;
    const position = await store.checkpoint(lease.group, lease.partition);
    checkpoints.set(id, { epoch: lease.epoch, position });
    return position;
  };
  // The epoch whose operator skips each Readmodel Partition has read.
  const skipsRead = new Map<string, number>();
  /**
   * Whether an operator skipped `event` but the skip's final batch never ran (the store recorded
   * it, then the skipping process stopped). A skip is only allowed on the Event a halted Partition
   * retries, and it raises the epoch, so only a failing Partition or the first page under a new
   * Lease can meet one: other pages read no dead letters.
   */
  const skipped = async (lease: Lease, event: RecordedEvent) => {
    const id = leaseId(lease);
    if (
      !consumers.statuses.get(id)?.attempts &&
      skipsRead.get(id) === lease.epoch
    )
      return false;
    const letters = await store.deadLetters();
    skipsRead.set(id, lease.epoch);
    return letters.some(
      (letter) =>
        letter.kind === "skipped-event" &&
        letter.group === lease.group &&
        (letter.partition ?? 0) === lease.partition &&
        letter.eventId === event.id,
    );
  };
  /** One Readmodel Partition's step: up to 100 Events after its Checkpoint, or up to `upTo`. */
  const readmodelTick = async (
    name: string,
    group: ReadmodelGroup,
    lease: Lease,
    slice?: Slice,
    upTo?: Position,
  ): Promise<boolean> => {
    // One generation for the whole batch: reads, fold and commit. It changes only on a swap,
    // which refuses a batch for the replaced generation (`requireActive`) or drops it; any
    // failure on a cached generation forgets it and retries once against the active one.
    const cached = activeGenerations.get(name);
    const generation = cached ?? (await group.store.activeGeneration(name));
    activeGenerations.set(name, generation);
    try {
      return await readmodelStep(name, group, lease, generation, slice, upTo);
    } catch (error) {
      activeGenerations.delete(name);
      checkpoints.delete(leaseId(lease));
      if (cached) return readmodelTick(name, group, lease, slice, upTo);
      throw error;
    }
  };
  const readmodelStep = async (
    name: string,
    group: ReadmodelGroup,
    lease: Lease,
    generation: string,
    slice?: Slice,
    upTo?: Position,
  ) => {
    const partition = slice?.partition,
      id = leaseId(lease);
    const known = knownCheckpoint(id, lease.epoch, generation);
    const checkpoint = known
      ? known.position
      : await group.store.checkpoint(name, { partition, generation });
    if (!known)
      checkpoints.set(id, { epoch: lease.epoch, generation, position: checkpoint });
    const page = await timings.consumers.time(leaseId(lease), "read", () =>
      readEventPage(store, {
        fromPosition: checkpoint,
        eventNames: readmodelEvents[name],
        ...(upTo ? { toPosition: upTo } : {}),
        limit: consumers.statuses.get(leaseId(lease))?.attempts ? 1 : 100,
      }),
    );
    if (
      !page.through ||
      (checkpoint && comparePositions(page.through, checkpoint) <= 0)
    )
      return false;
    const events = page.events;
    const first = events[0];
    if (first) consumers.observe(leaseId(lease), first);
    if (first && (await skipped(lease, first))) {
      await group.store.batch(name, lease.epoch, first.position, [], {
        partition,
      });
      checkpoints.delete(id);
      return true;
    }
    if (
      !(await timings.consumers.time(leaseId(lease), "checkReads", () =>
        dependenciesReady(lease, events, group.reads ?? [], (event) =>
          reactionParts(group.models).some(
            ([, model]) => model.reactions[eventName(event)],
          ),
        ),
      ))
    )
      return false;
    if (first)
      await consumers.status(lease, {
        eventId: first.id,
        position: first.position,
      });
    await timings.consumers.time(leaseId(lease), "apply", () =>
      readmodelBatch(name, group, lease, page, generation, checkpoint, slice),
    );
    timings.consumers.add(leaseId(lease), "events", events.length);
    return true;
  };
  /** What a Saga or Workflow Partition reads, and how it handles its Events and Saga Messages. */
  type Process = {
    events: string[];
    reads: string[];
    messages: boolean;
    reacts(event: RecordedEvent): boolean;
    batch(lease: Lease, events: RecordedEvent[], through: Position, slice?: Slice): Promise<void>;
    message(lease: Lease, message: SagaMessage): Promise<void>;
  };
  const sagaProcess = (name: string, definition: SagaDefinition): Process => ({
    events: sagaEvents[name]!,
    reads: definition.reads ?? [],
    messages: handlesSagaMessages(definition),
    reacts: (event) => !!definition.reactions[eventName(event)],
    batch: (lease, events, through, slice) =>
      sagaBatch(definition, lease, events, through, slice),
    message: (lease, message) => sagaMessageStep(definition, lease, message),
  });
  const workflowProcess = (name: string, workflow: Workflow): Process => ({
    events: workflow.events,
    reads: workflow.reads ?? [],
    messages: true,
    reacts: (event) => workflow.events.includes(eventName(event)),
    batch: (lease, events, through, slice) =>
      workflowBatch(name, workflow, lease, events, through, slice),
    message: (lease, message) => workflowMessageStep(name, workflow, lease, message),
  });
  /** One Saga or Workflow Partition's step; after a failure, one Event at a time, so the halt names the failing Event. */
  const processTick = async (
    process: Process,
    lease: Lease,
    slice?: Slice,
    upTo?: Position,
  ) => {
    const id = leaseId(lease);
    // Read before the log: once the read below reaches the log's end, the Checkpoint has passed
    // every Event recorded before each of these fell due (ADR-0020).
    const due =
      !upTo && store.dueSagaMessages && process.messages
        ? await timings.consumers.time(id, "dueMessages", () =>
            store.dueSagaMessages!(lease.group, slice, 100),
          )
        : undefined;
    const limit = consumers.statuses.get(id)?.attempts ? 1 : 100;
    const checkpoint = await storedCheckpoint(lease);
    const page = await timings.consumers.time(id, "read", () =>
      readEventPage(store, {
        fromPosition: checkpoint,
        eventNames: process.events,
        ...(upTo ? { toPosition: upTo } : {}),
        limit,
      }),
    );
    const { events } = page;
    const advanced =
      !!page.through &&
      (!checkpoint || comparePositions(page.through, checkpoint) > 0);
    if (events.length) {
      consumers.observe(id, events[0]!);
      if (
        !(await timings.consumers.time(id, "checkReads", () =>
          dependenciesReady(lease, events, process.reads, process.reacts),
        ))
      )
        return false;
      await consumers.status(lease, {
        eventId: events[0]!.id,
        position: events[0]!.position,
      });
    }
    if (advanced) await process.batch(lease, events, page.through!, slice);
    // A page short of the log's end leaves it unknown: the Saga Messages wait for a later tick.
    if (!due || !pageComplete(page, limit)) return advanced;
    sagaWake(id, due.nextDelay);
    if (!due.messages.length) return advanced;
    if (process.reads.length) {
      const checkpoint = await storedCheckpoint(lease);
      if (
        checkpoint &&
        (await readmodels.waitFor(checkpoint, process.reads, { timeout: 0 }))
          .behind
      ) {
        sagaWake(id, 100);
        return advanced;
      }
    }
    for (const message of due.messages)
      await timings.consumers.time(id, "message", () =>
        process.message(lease, message),
      );
    return true;
  };
  // A timer per Saga Partition at its next Saga Message's due time; the poll interval backs it up.
  const sagaWakes = new Map<string, ReturnType<typeof setTimeout>>();
  const sagaWake = (id: string, delay: number | null) => {
    clearTimeout(sagaWakes.get(id));
    sagaWakes.delete(id);
    if (delay === null) return;
    const timer = setTimeout(
      () => {
        sagaWakes.delete(id);
        wake();
      },
      Math.min(delay, 2 ** 31 - 1),
    );
    timer.unref?.();
    sagaWakes.set(id, timer);
  };
  /**
   * Handles one due Saga Message against its instance's state, then commits the state, its
   * effects, its Saga Message changes and the message's consumption under the Lease, keeping
   * the Checkpoint. A failure retries with backoff and dead-letters on the fifth attempt; the
   * Saga's Events keep flowing either way.
   */
  const sagaMessageStep = async (
    definition: SagaDefinition,
    lease: Lease,
    message: SagaMessage,
  ) => {
    const saga = lease.group.slice("saga:".length);
    const handler =
      message.deadline === undefined
        ? definition.messages?.[message.name]
        : definition.deadlines?.[message.deadline];
    const spec = handler && messageSpec(handler);
    const log = consumersLogger.child({
      consumerGroup: lease.group,
      sagaMessageId: message.id,
      instance: message.instance,
      correlationId: message.correlationId,
    });
    measure("eventfabric.saga_message.lateness", clock().getTime() - parseTime(message.dueAt), {
      saga,
    });
    try {
      const current = spec
        ? await store.loadSaga(lease.group, message.instance)
        : null;
      if (!spec || (!current && !spec.start)) {
        log.debug(
          { name: message.name },
          spec
            ? "Saga Message to a missing instance dropped"
            : "Saga Message without a handler dropped",
        );
        await store.commitSaga(lease, null, [], [], { consumed: message.id });
        return;
      }
      const effects = recordEffects(message, lease.group, {
        routing,
        instance: message.instance,
      });
      const messages = sagaMessageOps(),
        instance = messages.for(lease.group, definition, message, message.instance);
      const app = reactionApp(
        lease.group,
        message,
        definition.reads ?? [],
        effects,
        undefined,
        instance.send,
        instance.record,
      );
      const { group: _, attempt: __, ...view } = message;
      const reacted = await withSpan(
        "eventfabric.saga_message",
        message,
        () =>
          react(definition, message as any, message.instance, current, app, false, {
            view: instance.view,
            reaction: spec.handle,
            input: immutable({ ...structuredClone(view), saga }),
          }),
        true,
      );
      const state = reacted && toJson(reacted, "Saga state");
      if (!state) instance.destroyed(!!current);
      const outbox = await effects.seal(checkMetadata);
      await store.commitSaga(
        lease,
        null,
        [{ id: message.instance, state }],
        outbox,
        { changes: await messages.settle(), consumed: message.id },
      );
      count("eventfabric.saga_messages.handled", { saga });
      wake();
    } catch (error) {
      await failMessage(lease, message, error, log, { saga });
    }
  };
  /** Retries a failed Saga Message with backoff, or dead-letters it on the fifth attempt. */
  const failMessage = async (
    lease: Lease,
    message: SagaMessage,
    error: unknown,
    log: typeof consumersLogger,
    labels: Record<string, string>,
  ) => {
    // Replaced, cancelled or handled since it was read: nothing to do.
    if (error instanceof StaleSagaMessageError) return;
    if (error instanceof LeaseLostError) throw error;
    const attempt = (message.attempt ?? 0) + 1;
    count("eventfabric.saga_messages.failures", labels);
    if (attempt >= 5) {
      log.error(
        {
          error: errorMessage(error),
          attempts: attempt,
          deadLetterId: message.id,
          recovery: recovery.deadLetter(message.id),
        },
        "Saga Message dead-lettered",
      );
      await store.deadLetterSagaMessage!(lease, message.id, errorMessage(error));
    } else {
      log.warn(
        { error: errorMessage(error), attempts: attempt },
        "Saga Message failed; retrying",
      );
      await store.retrySagaMessage!(
        lease,
        message.id,
        attempt,
        new Date(clock().getTime() + backoff(attempt)).toISOString(),
      );
    }
  };
  /** A document `ctx.read` journaled, typed as stores return it: `DateTime()` fields come back as Dates. */
  const reviveDocument = (group: string, model: string, document: unknown) => {
    const definition = groups[group]!;
    const schema = resolveSchema(definition.models[model]!.schema, { ...config.schemas, ...definition.schemas });
    const { _id: id, ...rest } = document as JsonObject;
    return normalizeDocument(schema, rest, String(id), false, undefined, false);
  };
  const newWorkflowRecord = (workflow: Workflow, id: string, input: unknown, at: Date): WorkflowRecord => ({
    ...newWorkflowInstance(input, at),
    id,
    version: workflow.version,
    startedAt: at.toISOString(),
  });
  /** Keeps an Event for a drifted instance, so its run sees it once an operator resumes it. */
  const buffered = (record: WorkflowRecord, type: string, value: unknown): WorkflowRecord => {
    const key = inboxKey({ kind: "event", name: type });
    return { ...record, inbox: { ...record.inbox, [key]: [...(record.inbox[key] ?? []), value] } };
  };
  /**
   * Runs one wake of an instance under its pinned version (ADR-0021) and records what it commits:
   * Commands and Messages in `effects`, Deadlines and started Workflows in `changes`. A drifted
   * run returns the stored instance, parked `drifted` with the reason as its error.
   */
  const wakeWorkflow = async (
    name: string,
    workflow: Workflow,
    record: WorkflowRecord,
    wake: WorkflowWake,
    cause: MessageCause,
    effects: Effects,
    changes: SagaMessageChange[],
  ): Promise<{ record: WorkflowRecord; drift?: string }> => {
    const group = workflowGroup(name);
    const pinned = workflow.versions.get(record.version);
    if (!pinned) {
      const drift = `version ${record.version} is not registered`;
      return { record: { ...record, status: "drifted", error: drift }, drift };
    }
    // The version whose body runs: the pinned one, until a restart moves to the highest.
    let running = pinned;
    const run = await runWorkflow(pinned.run, record, wake, {
      id: record.id,
      revive: reviveDocument,
      // A restart's fresh journal runs the highest version, as a new instance would.
      restartBody: (ctx, input) => {
        running = workflow.versions.get(workflow.version)!;
        return running.run(ctx, input);
      },
      reader: async (from, model, id) => {
        if (!(running.reads ?? []).includes(from))
          throw new Error(`Undeclared cross-group read ${from}`);
        // Journaled as JSON, as the store keeps it; `revive` turns it back into the document.
        const { document } = await readmodels.repository(from as never, model as never)(id);
        return document && toJson(document, "Readmodel document");
      },
      onLate: (step) => {
        consumersLogger.warn(
          { consumerGroup: group, instance: record.id, step },
          "Workflow ctx call after its run was judged suspended",
        );
        count("eventfabric.workflow.late_calls", { workflow: name });
      },
    });
    // A restart's run starts from a fresh instance under the highest version; the row keeps its id and start.
    const next: WorkflowRecord = {
      id: record.id,
      version: run.restarts ? workflow.version : record.version,
      startedAt: record.startedAt,
      ...run.instance,
    };
    if (run.reason) return { record: { ...next, error: run.reason }, drift: run.reason };
    if (finished(next.status)) next.finishedAt = wake.at.toISOString();
    for (const effect of run.effects)
      if (effect.type === "tell") effects.tell(effect.message as CommandInput);
      else if (effect.type === "queue")
        effects.queue(effect.command, effect.message, { suffix: workflowWake.step(effect.step, effect.run) });
      else if (effect.type === "ask")
        // The run names the reply too, so a reply to an earlier run of this id is dropped.
        effects.queue(effect.name, effect.message, {
          suffix: workflowWake.step(effect.step, effect.run),
          replyTo: effect.step,
          run: effect.run,
        });
      else if (effect.type === "start") {
        workflowOf(effect.name);
        changes.push({
          send: await workflowMessage(
            effect.name,
            effect.id,
            "start",
            { input: effect.input ?? null, reuse: effect.reuse ?? false },
            {},
            cause,
          ),
        });
      } else if (effect.type === "deadline")
        changes.push({
          send: {
            ...(await workflowMessage(name, record.id, effect.step, {}, { at: effect.at }, cause)),
            deadline: effect.step,
          },
        });
      else changes.push({ cancel: { group, instance: record.id, deadline: effect.step } });
    return { record: next };
  };
  /** Runs a batch of Events through a Workflow: starts, correlated Events, and the Checkpoint in one commit. */
  const workflowBatch = async (
    name: string,
    workflow: Workflow,
    lease: Lease,
    events: RecordedEvent[],
    position: Position,
    slice?: Slice,
  ) => {
    const group = lease.group,
      outbox: OutboxEntry[] = [],
      changes: SagaMessageChange[] = [],
      // Later Events in the batch see the instances earlier ones wrote.
      records = new Map<string, WorkflowRecord | null>(),
      changed = new Set<string>();
    for (const event of events) {
      const type = eventName(event);
      if (!workflow.events.includes(type)) continue;
      consumers.observe(leaseId(lease), event);
      const view = authoringView(event),
        value = toJson(view, "Event"),
        at = clock();
      // Each instance id with the versions whose `correlate` routes the Event to it.
      const wakes: [string, WorkflowWake, number[]][] = [];
      if (type === workflow.start) wakes.push([workflow.id!(view), { kind: "start", at }, []]);
      const routed = new Map<string, number[]>();
      for (const version of workflow.versions.values())
        for (const id of await identityIds("Workflow", (await version.correlate?.[type]?.(view)) ?? null))
          routed.set(id, [...(routed.get(id) ?? []), version.version]);
      for (const [id, versions] of routed) wakes.push([id, { kind: "event", name: type, value, at }, versions]);
      for (const [id, wake, versions] of wakes) {
        if (typeof id !== "string" || !id)
          throw new Error(`Workflow ${name} needs a string instance id for ${type}`);
        if (!owns(slice, id)) continue;
        let record = records.has(id) ? records.get(id)! : await store.loadWorkflow!(group, id);
        if (wake.kind === "start") {
          // Started once: an Event-start for a known id is a no-op, as `startWorkflow` is.
          if (record) continue;
          record = newWorkflowRecord(workflow, id, value, at);
        } else if (!record || finished(record.status)) continue;
        // An instance takes the Events its pinned version routes; one of an unregistered version, the highest's.
        else if (!versions.includes(workflow.versions.has(record.version) ? record.version : workflow.version)) continue;
        if (record.status === "drifted") record = buffered(record, type, value);
        else {
          const effects = recordEffects(event, group, { routing, instance: id });
          const woken = await wakeWorkflow(name, workflow, record, wake, event, effects, changes);
          record = woken.drift ? buffered(woken.record, type, value) : woken.record;
          if (woken.drift) parked(group, id, woken.drift);
          else outbox.push(...(await effects.seal(checkMetadata)));
        }
        records.set(id, record);
        changed.add(id);
      }
    }
    const rows = [...changed].map((id) => records.get(id)!);
    await timings.consumers.time(leaseId(lease), "commit", () =>
      store.commitWorkflow!(lease, position, rows, outbox, { changes }),
    );
    checkpoints.set(leaseId(lease), { epoch: lease.epoch, position });
    timings.consumers.add(leaseId(lease), "events", events.length);
    if (rows.length || outbox.length || changes.length) wake();
  };
  const parked = (group: string, instance: string, reason: string) => {
    consumersLogger.error(
      { consumerGroup: group, instance, reason },
      "Workflow drifted; instance parked until an operator resumes it",
    );
    count("eventfabric.workflow.drifted", { workflow: parseProcessGroup(group)!.name });
  };
  /**
   * Handles one due Saga Message of a Workflow: a start, a Signal, a Worker's reply or a Deadline.
   * The wake commits the instance, its effects and the message's consumption under the Lease. A
   * Signal to a missing or finished instance dead-letters as `workflow-missing`, a start of a finished
   * one without `reuse` as `workflow-finished` (`workflow-missing` if it carries a Signal); any wake of a
   * drifted instance dead-letters, so a resumed instance can replay it. Errors retry as a Saga
   * Message's do.
   */
  const workflowMessageStep = async (
    name: string,
    workflow: Workflow,
    lease: Lease,
    message: SagaMessage,
  ) => {
    const group = lease.group;
    const log = consumersLogger.child({
      consumerGroup: group,
      sagaMessageId: message.id,
      instance: message.instance,
      correlationId: message.correlationId,
    });
    measure("eventfabric.saga_message.lateness", clock().getTime() - parseTime(message.dueAt), {
      workflow: name,
    });
    try {
      let record = await store.loadWorkflow!(group, message.instance);
      const at = clock();
      let trigger: WorkflowWake;
      const signal: { name: string; payload: unknown } | undefined =
        message.name === "start" ? message.payload.signal : undefined;
      const wakeName = workflowWake.parse(message.name);
      if (message.name === "start" && record && !finished(record.status) && signal)
        // A start that carries a Signal signals the instance it finds running.
        trigger = { kind: "signal", name: signal.name, value: signal.payload, at };
      else if (message.name === "start") {
        if (record && !finished(record.status)) {
          await store.commitWorkflow!(lease, null, [], [], { consumed: message.id });
          return;
        }
        if (record && !message.payload.reuse) {
          // Refused, not dropped: a Saga's or `ctx.start`'s start (or the Signal it carries) is lost otherwise.
          const reason = signal ? "workflow-missing" : "workflow-finished";
          log.warn({ name: message.name, reason }, "Start of a finished Workflow instance dead-lettered");
          await store.deadLetterSagaMessage!(lease, message.id, reason);
          return;
        }
        record = newWorkflowRecord(workflow, message.instance, message.payload.input, at);
        if (signal) record.inbox[inboxKey({ kind: "signal", name: signal.name })] = [signal.payload];
        trigger = { kind: "start", at };
      } else if (!record || finished(record.status)) {
        if (wakeName?.kind === "signal") {
          log.warn({ name: message.name }, "Signal to a missing or finished Workflow instance dead-lettered");
          await store.deadLetterSagaMessage!(lease, message.id, "workflow-missing");
        } else {
          log.debug({ name: message.name }, "Saga Message to a finished Workflow instance dropped");
          await store.commitWorkflow!(lease, null, [], [], { consumed: message.id });
        }
        return;
      } else if (wakeName?.kind === "reply" && wakeName.run !== record.run) {
        log.debug({ name: message.name }, "Worker reply to an earlier run of a Workflow instance dropped");
        await store.commitWorkflow!(lease, null, [], [], { consumed: message.id });
        return;
      } else if (message.name === "cancel" && record.status === "drifted") {
        // A drifted body cannot run: the instance ends cancelled without it, and its Deadlines go too.
        await store.commitWorkflow!(
          lease,
          null,
          [{ ...record, status: "cancelled", journal: {}, inbox: {}, finishedAt: at.toISOString() }],
          [],
          { changes: [{ cancel: { group, instance: message.instance } }], consumed: message.id },
        );
        return;
      } else
        trigger =
          message.name === "cancel" || message.name === "resume"
            ? { kind: message.name, at }
            : message.deadline !== undefined
            ? { kind: "deadline", step: message.deadline, at }
            : wakeName?.kind === "reply"
              ? { kind: "reply", step: wakeName.step, value: message.payload, at }
              : { kind: "signal", name: wakeName?.name ?? message.name, value: message.payload, at };
      if (record.status === "drifted" && trigger.kind !== "resume") {
        await store.deadLetterSagaMessage!(lease, message.id, `workflow-drifted: ${record.error}`);
        return;
      }
      // Resumed: the instance runs again from its journal; a body that still drifts parks it again.
      const { error: _error, ...resumed } = record;
      const current: WorkflowRecord = trigger.kind === "resume" ? { ...resumed, status: "running" } : record,
        effects = recordEffects(message, group, { routing, instance: message.instance }),
        changes: SagaMessageChange[] = [];
      const woken = await withSpan(
        "eventfabric.workflow",
        message,
        () => wakeWorkflow(name, workflow, current, trigger, message, effects, changes),
        true,
      );
      if (woken.drift) {
        // Parked first: if the dead letter is lost, the next tick finds the instance drifted and dead-letters again.
        await store.commitWorkflow!(lease, null, [woken.record], []);
        parked(group, message.instance, woken.drift);
        await store.deadLetterSagaMessage!(lease, message.id, `workflow-drifted: ${woken.drift}`);
        return;
      }
      await store.commitWorkflow!(
        lease,
        null,
        [woken.record],
        await effects.seal(checkMetadata),
        { changes, consumed: message.id },
      );
      // Replayed only once the instance runs again, so they do not dead-letter as drifted once more.
      if (trigger.kind === "resume")
        for (const letter of await store.deadLetters())
          if (letter.kind === "saga-message" && letter.group === group && letter.sagaMessage?.instance === message.instance)
            await store.replayDeadLetter(letter.id);
      wake();
    } catch (error) {
      await failMessage(lease, message, error, log, { workflow: name });
    }
  };
  /** What a layout switch needs from a Consumer group kind. */
  type LayoutSpec = {
    group: string;
    count: number;
    /** Fences the writes of the Lease's Partition. */
    fence(lease: Lease): Promise<void>;
    checkpoint(slice?: Slice): Promise<Position | null>;
    tick(
      lease: Lease,
      slice: Slice | undefined,
      upTo: Position,
    ): Promise<boolean>;
    /**
     * Starts Partitions 0..count-1 at `position`. `leases` are every Partition's of both
     * layouts, in order; it fails if any of them no longer fences its Partition.
     */
    reset(leases: Lease[], position: Position): Promise<void>;
    /** A Saga's or Workflow's `from`: where the group starts while it is new. */
    start?: RecordedStart;
  };
  const readmodelLayout = (
    name: string,
    group: ReadmodelGroup,
  ): LayoutSpec => ({
    group: `readmodels:${name}`,
    count: partitionsOf(group),
    fence: (lease) => group.store.fence(name, lease.epoch, lease.partition),
    checkpoint: (slice) =>
      group.store.checkpoint(name, { partition: slice?.partition }),
    tick: (lease, slice, upTo) =>
      readmodelTick(name, group, lease, slice, upTo),
    reset: async (leases, position) => {
      await group.store.resetPartitions(
        name,
        leases.map((lease) => lease.epoch),
        position,
      );
      forgetCheckpoints(`readmodels:${name}`);
    },
  });
  /** A Saga's or Workflow's layout: its Checkpoints live in the Event store. */
  const processLayout = (
    group: string,
    count: number,
    process: Process,
  ): LayoutSpec => ({
    group,
    count,
    start: starts.get(group),
    fence: async () => {},
    checkpoint: (slice) => store.checkpoint(group, slice?.partition),
    tick: (lease, slice, upTo) => processTick(process, lease, slice, upTo),
    // Each commit is fenced by its Lease; Partitions 0..from-1 already stand at `position`.
    reset: async (leases, position) => {
      for (const lease of leases.slice(0, count))
        await store.commitCheckpoint(lease, position);
      forgetCheckpoints(group);
    },
  });
  /**
   * A Consumer group's Partition count is recorded in its `layout:<group>` row (1 when absent).
   * Partition 0's holder switches a group to its configured count: it takes and fences every
   * old and new Partition's Lease, runs each old Partition up to the newest visible Event P, and
   * starts every new Partition at P. No Event can still commit at or before P, so each document
   * or Saga instance changes owner exactly at P: nothing is skipped and nothing runs twice.
   * The reset and the publication each check that every one of those Leases still fences its
   * Partition, so a switch whose Leases expired fails instead of overwriting newer progress or a
   * newer layout. A failed or interrupted switch leaves the old layout running from where its
   * Partitions stand; the next holder of Partition 0 starts the switch again.
   */
  /** A new group's `from` and the Position it resolved to. */
  type Start = { start: RecordedStart; position: Position | null };
  const layout = (() => {
    const settledEpochs = new Map<string, number>(),
      warned = new Set<string>();
    const row = async (group: string) =>
      (await store.consumers()).find((s) => s.group === `layout:${group}`);
    const recorded = async (group: string) => (await row(group))?.partitions ?? 1;
    const renew = async (held: Lease[]) => {
      const renewed = await store.renewLeases(
        held,
        config.leaseDuration ?? 30000,
      );
      if (renewed.some((lease) => !lease))
        throw new LeaseLostError("Lease lost during a Partition layout switch");
      return renewed as Lease[];
    };
    /** Starts a new group at `position` on every Partition, in one transaction, under every Partition's Lease. */
    const begin = async (spec: LayoutSpec, held: Lease[], { start, position }: Start) => {
      if (!(await store.startConsumer(spec.group, start, position, held)))
        throw new LeaseLostError("A Lease was lost or the group started elsewhere");
      forgetCheckpoints(spec.group);
      consumersLogger.info(
        { consumerGroup: spec.group, from: describeStart(start), position: position && encodePosition(position) },
        "Consumer group started",
      );
    };
    const change = async (
      spec: LayoutSpec,
      first: Lease,
      from: number,
      start?: Start,
    ) => {
      let held: Lease[] = [first];
      try {
        for (let p = 1; p < Math.max(from, spec.count); p++) {
          const lease = await store.acquireLease(
            spec.group,
            owner,
            config.leaseDuration ?? 30000,
            p,
          );
          if (!lease) {
            consumersLogger.info(
              { consumerGroup: spec.group, partition: p, from, to: spec.count },
              "Partition layout switch waits for another instance's Lease",
            );
            return false;
          }
          held.push(lease);
        }
        // From here on, only this instance's Leases can write to any Partition of either layout.
        for (const lease of held) await spec.fence(lease);
        if (start) {
          await begin(spec, held, start);
          return true;
        }
        // A group that never ran has nothing to hand over: every Partition starts at the beginning.
        const fresh = from === 1 && !(await spec.checkpoint(undefined));
        const [newest] = fresh
          ? []
          : await store.readAll({ reverse: true, limit: 1 });
        if (newest) {
          for (let q = 0; q < from; q++) {
            const slice = sliceOf(from, q);
            for (;;) {
              held = await renew(held);
              const checkpoint = await spec.checkpoint(slice);
              if (
                checkpoint &&
                comparePositions(checkpoint, newest.position) >= 0
              )
                break;
              if (!(await spec.tick(held[q]!, slice, newest.position)))
                await pause(100);
            }
          }
          held = await renew(held);
          await spec.reset(held, newest.position);
        }
        if (
          !(await store.publishLayout(
            spec.group,
            from,
            spec.count,
            newest?.position ?? null,
            held,
          ))
        )
          throw new LeaseLostError(
            "Partition layout changed or a Lease was lost during the switch",
          );
        consumersLogger.info(
          {
            consumerGroup: spec.group,
            from,
            to: spec.count,
            position: newest?.position,
          },
          "Consumer group switched its Partition layout",
        );
        return true;
      } finally {
        for (const lease of held.slice(1))
          await store.releaseLease(lease).catch(() => {});
      }
    };
    return {
      /** Gates Partitions 1..N-1: their Checkpoints are laid out for N Partitions. */
      ready: async (group: string, count: number) =>
        (await recorded(group)) === count,
      /** Partition 0 lays its group out for the configured count before it runs. */
      async settled(spec: LayoutSpec, lease: Lease) {
        if (lease.partition !== 0) return true;
        if (settledEpochs.get(spec.group) === lease.epoch) return true;
        const current = await row(spec.group);
        const from = current?.partitions ?? 1;
        // New: no layout row, so no Partition but 0 can hold a Checkpoint.
        // Resolved before taking the other Leases: a Date may scan the log.
        const start: Start | undefined =
          spec.start && !current && !(await spec.checkpoint(undefined))
            ? { start: spec.start, position: await startPosition(spec.start) }
            : undefined;
        if (
          spec.start &&
          current?.from &&
          describeStart(current.from) !== describeStart(spec.start) &&
          !warned.has(spec.group)
        ) {
          warned.add(spec.group);
          consumersLogger.warn(
            {
              consumerGroup: spec.group,
              from: describeStart(spec.start),
              startedFrom: describeStart(current.from),
            },
            "from changed on a group that already started; it keeps its Checkpoints. Retire it and deploy it again to start over",
          );
        }
        if (
          (from !== spec.count || start) &&
          !(await change(spec, lease, from, start))
        )
          return false;
        settledEpochs.set(spec.group, lease.epoch);
        return true;
      },
    };
  })();
  const relay = createOutboxRelay({
    store,
    transport,
    routing,
    logger: outboxLogger,
    timings: timings.outbox,
    held: (group) => !!consumers.lease(group),
    running: () => running,
    wake,
  });
  const workerAttempt = createWorkerRunner({
    store,
    logger: logger.child({ component: "worker" }),
    services: currentServices,
    readmodels: readmodels as unknown as Readmodels,
    sendCommand,
    sendSagaMessage,
    workflowCalls: (cause: Message) => workflowCalls(cause),
    sendWorkflowMessage,
    workerQueue,
    wake,
  });
  const readRebuildPage = (name: string, fromPosition: Position | null) =>
    background(
      readEventPage(store, {
        fromPosition,
        eventNames: readmodelEvents[name],
        limit: rebuildPage,
      }),
    );
  type SliceTask = {
    name: string;
    generation: string;
    epoch: number;
    consumerGroup: string;
    slice: Slice;
  };
  /** One fold worker of a sliced rebuild: folds and commits its Partition's documents of each page it is given. */
  const rebuildSlice = async (
    { name, generation, epoch, consumerGroup, slice }: SliceTask,
    pages: () => Promise<EventPage | undefined>,
    folded: () => void,
  ) => {
    await initialize();
    const group = groups[name]!;
    const lease = { group: consumerGroup, epoch } as Lease;
    const checkpoint = (await group.store.slices!(name, generation))![
      slice.partition
    ];
    let committing: Committing | undefined;
    const scope = rebuildScope(name, generation, slice);
    rebuildDocuments?.begin(scope, !checkpoint);
    try {
      for (let page = await pages(); page; page = await pages()) {
        // A resumed rebuild reads from its lowest slice; this one has committed up to `checkpoint`.
        let rest: EventPage | undefined = {
          ...page,
          events: page.events.filter(
            (event) =>
              !checkpoint || comparePositions(event.position, checkpoint) > 0,
          ),
        };
        while (rest)
          ({ remaining: rest, committing } = await rebuildBatch(
            name,
            group,
            lease,
            rest,
            generation,
            committing,
            slice,
          ));
        folded();
      }
      await committing?.commit;
    } finally {
      await committing?.commit.catch(() => {});
      rebuildDocuments?.drop(scope);
    }
  };
  /*
   * Folds a generation without a Checkpoint in one worker thread per Partition, when the group is
   * partitioned, its store has rebuild slices, the configuration module is known and more than one
   * CPU is available.
   * The rebuild reads and decodes each page once and sends each Event only to the workers owning
   * one of its documents, at most two pages ahead of the slowest; every worker gets every page's
   * Position. Once every slice has committed through the same Position, that Position becomes
   * the generation's Checkpoint and the single loop continues from it.
   */
  const foldInWorkers = async (
    name: string,
    group: ReadmodelGroup,
    lease: Lease,
    generation: string,
    proceed: () => void,
  ) => {
    const count = partitionsOf(group),
      started = await group.store.slices?.(name, generation),
      eligible = count > 1 && !!group.store.slices && !!config.configModule;
    // On one CPU, workers cost more than they fold.
    if (!started && (!eligible || availableParallelism() < 2)) return;
    if (!eligible)
      throw new Error(
        `The pending rebuild of ${name} folds in ${started!.length} worker threads; resume it with partitions: ${started!.length} and configModule set (the CLI sets it; an entry point passes import.meta.resolve of its config module)`,
      );
    const checkpoints = (await group.store.slices!(name, generation, count))!;
    let through: Position | null = null;
    if (checkpoints.every(Boolean))
      for (const checkpoint of checkpoints as Position[])
        if (!through || comparePositions(checkpoint, through) < 0)
          through = checkpoint;
    const workers = checkpoints.map(
      (_, partition) =>
        new Worker(new URL("./rebuild-worker.js", import.meta.url), {
          workerData: {
            module: config.configModule,
            // The fold workers share this process's cache budget.
            rebuildCacheBytes: Math.floor(
              (config.rebuildCacheBytes ?? defaultRebuildCacheBytes) / count,
            ),
            task: {
              name,
              generation,
              epoch: lease.epoch,
              consumerGroup: lease.group,
              slice: { partition, count },
            } satisfies SliceTask,
          },
        }),
    );
    const outstanding = workers.map(() => 0),
      done = new Set<number>();
    let failed: unknown,
      wake = () => {};
    workers.forEach((worker, i) => {
      worker.on("message", (message) => {
        if (message === "folded") outstanding[i]!--;
        if (message === "done") done.add(i);
        wake();
      });
      worker.on("error", (error) => {
        failed ??= error;
        wake();
      });
      worker.on("exit", (code) => {
        if (!done.has(i))
          failed ??= new Error(`Rebuild worker ${i} exited with code ${code}`);
        wake();
      });
    });
    // Query Identities and failing Identities go to every worker, which resolves them as Partitions do.
    const owners = async (event: RecordedEvent) => {
      const found = new Set<number>();
      for (const [model, definition] of reactionParts(group.models)) {
        if (!definition.reactions[eventName(event)]) continue;
        let selected: Identity;
        try {
          selected = await eventIdentity(definition, event);
        } catch {
          return workers.map((_, i) => i);
        }
        if (selected == null) continue;
        for (const id of Array.isArray(selected) ? selected : [selected]) {
          if (typeof id !== "string") return workers.map((_, i) => i);
          found.add(keyPartition(`${model}/${id}`, count));
        }
      }
      return [...found];
    };
    const until = async (ready: () => boolean) => {
      for (;;) {
        if (failed) throw failed;
        if (ready()) return;
        await new Promise<void>((resolve) => (wake = resolve));
      }
    };
    try {
      let next = readRebuildPage(name, through);
      for (;;) {
        proceed();
        const page = await next;
        if (page.events.length) {
          while (
            (
              await readmodels.waitFor(
                page.events.at(-1)!.position,
                group.reads ?? [],
                { timeout: 100 },
              )
            ).behind
          )
            proceed();
          next = readRebuildPage(name, page.through);
        }
        const routed = workers.map((): RecordedEvent[] => []);
        for (const event of page.events)
          for (const i of await owners(event)) routed[i]!.push(event);
        await until(() => outstanding.every((pages) => pages < 2));
        workers.forEach((worker, i) => {
          outstanding[i]!++;
          worker.postMessage({ events: routed[i], through: page.through });
        });
        through = page.through ?? through;
        if (!page.events.length) break;
      }
      for (const worker of workers) worker.postMessage("finish");
      await until(() => done.size === count);
      if (through)
        await group.store.batch(name, lease.epoch, through, [], {
          generation,
          reconstruction: reconstruction(name),
        });
    } finally {
      await Promise.all(workers.map((worker) => worker.terminate()));
    }
  };
  const rebuild = async (
    name: string,
    {
      swap = "auto",
      signal,
    }: { swap?: "auto" | "manual"; signal?: AbortSignal } = {},
    existingGeneration?: string,
  ) => {
    await initialize();
    const group = groups[name];
    if (!group)
      throw new Error(
        `Unknown Readmodel group ${name}; Sagas cannot be rebuilt`,
      );
    if (group.reads?.length)
      readmodelsLogger.warn(
        { consumerGroup: `readmodels:${name}`, reads: group.reads },
        "Rebuild cross-group reads observe the current document and can differ from live processing",
      );
    const work = async (lease: Lease, alive: () => void) => {
      await group.store.fence(name, lease.epoch);
      if (!existingGeneration && (await group.store.pendingGeneration(name)))
        throw new Error(
          "A pending rebuild exists; swap it before starting another",
        );
      const generation =
        existingGeneration ??
        (await group.store.createGeneration(name, reconstruction(name)));
      const previousGeneration = await group.store.activeGeneration(name);
      const read = (fromPosition: Position | null) =>
        readRebuildPage(name, fromPosition);
      if (!(await group.store.checkpoint(name, { generation })))
        await foldInWorkers(name, group, lease, generation, () => {
          if (closing)
            throw new Error("Application is closing; rebuild stopped");
          signal?.throwIfAborted();
          alive();
        });
      // A generation nothing has committed to is empty, so the rebuild's own cache knows every document.
      const scope = rebuildScope(name, generation);
      rebuildDocuments?.begin(
        scope,
        !(await group.store.checkpoint(name, { generation })) &&
          !(await group.store.slices?.(name, generation))?.some(Boolean),
      );
      // The next page is read while a batch folds, and a batch commits while the next one folds.
      let page: EventPage | undefined,
        next: Promise<EventPage> | undefined,
        ahead = false,
        committing: Committing | undefined;
      const settle = async () => {
        await committing?.commit;
        committing = undefined;
      };
      try {
        for (;;) {
          // Cooperative: close() stops a rebuild between batches; it resumes from its checkpoint.
          if (closing)
            throw new Error("Application is closing; rebuild stopped");
          signal?.throwIfAborted();
          alive();
          if (!page) {
            ahead = !!next;
            if (!next) {
              await settle();
              next = read(
                await group.store.checkpoint(name, { generation }),
              );
            }
            page = await next;
            next = undefined;
          }
          const events = page.events;
          // A page read ahead can predate Events appended meanwhile, so only a fresh read ends the rebuild.
          if (!events.length && ahead) {
            page = undefined;
            continue;
          }
          if (!events.length) {
            await settle();
            const checkpoint = await group.store.checkpoint(name, {
              generation,
            });
            if (
              page.through &&
              (!checkpoint || comparePositions(page.through, checkpoint) > 0)
            )
              await (await rebuildBatch(name, group, lease, page, generation))
                .committing.commit;
            page = undefined;
            if (swap === "auto") {
              try {
                await group.store.swapGeneration(
                  name,
                  generation,
                  (await store.head()) ?? undefined,
                  reconstruction(name),
                );
              } catch (error) {
                if (error instanceof GenerationBehindError) continue;
                throw error;
              }
              await group.store.dropGeneration(name, previousGeneration);
              requiredRebuilds.delete(name);
            }
            break;
          }
          const dependency = await readmodels.waitFor(
            events.at(-1)!.position,
            group.reads ?? [],
            { timeout: 100 },
          );
          if (dependency.behind) continue;
          if (page.through) next ??= read(page.through);
          ({ remaining: page, committing } = await rebuildBatch(
            name,
            group,
            lease,
            page,
            generation,
            committing,
          ));
        }
      } finally {
        await committing?.commit.catch(() => {});
        rebuildDocuments?.drop(scope);
      }
      return { group: name, generation, swapped: swap === "auto" };
    };
    return underGroupLease(name, work);
  };
  // Writes go under the group's Lease: this instance's, a fresh one, or a live one another instance holds.
  const underGroupLease = <T>(
    name: string,
    work: (lease: Lease, alive: () => void) => Promise<T>,
  ) =>
    consumers.hold(
      `rebuild:${name}`,
      (_maintenance, maintained) =>
        consumers.hold(
          `readmodels:${name}`,
          (lease, alive) =>
            work(lease, () => {
              maintained();
              alive();
            }),
          async () => {
            const lease = (await store.consumers()).find(
              (status) =>
                status.group === `readmodels:${name}` &&
                status.lease &&
                parseTime(status.lease.expiresAt) > Date.now(),
            )?.lease;
            if (!lease)
              throw new Error("Readmodel Lease changed; retry the rebuild");
            return work(lease, maintained);
          },
        ),
      async () => {
        throw new RebuildRunningError();
      },
    );
  /*
   * Takes over documents another store already folded, at a cutover, instead of rebuilding them:
   * loads them into a pending generation whose Checkpoint is `position` (the log head
   * by default), then resumes it as a rebuild, which folds any later Events and, with `swap: "auto"`,
   * swaps it in. A failed load drops the generation.
   */
  const importReadmodels = async (
    name: string,
    models: Record<string, Iterable<JsonObject> | AsyncIterable<JsonObject>>,
    {
      position,
      swap = "auto",
      chunk = 1000,
      concurrency = 4,
    }: {
      position?: Position;
      swap?: "auto" | "manual";
      chunk?: number;
      /** Chunks written at once; a pending generation's documents have no order between them. */
      concurrency?: number;
    } = {},
  ) => {
    if (!Number.isInteger(concurrency) || concurrency < 1)
      throw new Error("A Readmodel import's concurrency must be a positive integer");
    await initialize();
    const group = groups[name];
    if (!group) throw new Error(`Unknown Readmodel group ${name}`);
    if (!group.store.load)
      throw new Error(`The Readmodel store of ${name} cannot import documents`);
    for (const model of Object.keys(models))
      if (!group.models[model])
        throw new Error(`Unknown Readmodel ${name}.${model}`);
    const at = position ?? (await store.head());
    if (!at)
      throw new Error("A Readmodel import needs a Position; the Event log is empty");
    const counts: Record<string, number> = {};
    const generation = await underGroupLease(name, async (lease, alive) => {
      await group.store.fence(name, lease.epoch);
      if (await group.store.pendingGeneration(name))
        throw new Error(
          "A pending rebuild exists; swap or drop it before importing",
        );
      const options = { reconstruction: reconstruction(name) };
      const generation = await group.store.createGeneration(
        name,
        options.reconstruction,
      );
      const writing = new Set<Promise<void>>();
      try {
        for (const [model, documents] of Object.entries(models)) {
          counts[model] = 0;
          let changes: ReadmodelChange[] = [];
          const flush = async () => {
            const loading = changes;
            changes = [];
            const written = group.store
              .load!(name, lease.epoch, generation, loading, options)
              .then(() => {
                counts[model]! += loading.length;
                alive();
              });
            writing.add(written);
            written.finally(() => writing.delete(written)).catch(() => {});
            if (writing.size >= concurrency) await Promise.race(writing);
          };
          for await (const { _id, ...document } of documents) {
            if (typeof _id !== "string")
              throw new Error(
                `A Readmodel import of ${name}.${model} needs string _id values`,
              );
            changes.push({ model, id: _id, document });
            if (changes.length >= chunk) await flush();
          }
          if (changes.length) await flush();
        }
        await Promise.all(writing);
        await group.store.batch(name, lease.epoch, at, [], {
          generation,
          ...options,
        });
      } catch (error) {
        // A write still in flight would land after the drop.
        await Promise.allSettled(writing);
        await group.store.dropGeneration(name, generation).catch(() => {});
        throw error;
      }
      return generation;
    });
    const swapped =
      swap === "auto" && (await rebuild(name, { swap }, generation)).swapped;
    return { group: name, generation, position: at, counts, swapped };
  };
  // One group's whole rebuild in a worker thread that loads the Application from its configuration module.
  const rebuildInWorker = (
    name: string,
    swap: "auto" | "manual",
    rebuildCacheBytes: number,
    signal?: AbortSignal,
  ) =>
    new Promise<Awaited<ReturnType<typeof rebuild>>>((resolve, reject) => {
      const worker = new Worker(new URL("./rebuild-worker.js", import.meta.url), {
        workerData: {
          module: config.configModule,
          rebuildCacheBytes,
          rebuild: { name, swap },
        },
      });
      const abort = () => worker.postMessage("abort");
      signal?.addEventListener("abort", abort, { once: true });
      let outcome: { result?: any; error?: any } = {},
        failure: unknown;
      worker.on("message", (message) => (outcome = message));
      worker.on("error", (error) => (failure ??= error));
      worker.on("exit", (code) => {
        signal?.removeEventListener("abort", abort);
        if (outcome.result) return resolve(outcome.result);
        reject(
          outcome.error
            ? Object.assign(new Error(outcome.error.message), outcome.error)
            : (failure ??
                new Error(`Rebuild worker of ${name} exited with code ${code}`)),
        );
      });
    });
  /*
   * Rebuilds several groups at once. With the configuration module known and more
   * than one CPU, each group rebuilds in its own worker thread, so groups fold on separate CPUs.
   * A group that reads another group of the call starts after that one ends, and does not start
   * when it failed. The other groups still finish; then the failures are thrown together.
   */
  const rebuildGroups = async (
    names?: string[],
    {
      swap = "auto",
      concurrency = availableParallelism(),
      signal,
    }: {
      swap?: "auto" | "manual";
      concurrency?: number;
      signal?: AbortSignal;
    } = {},
  ) => {
    await initialize();
    for (const name of names ?? [])
      if (!groups[name])
        throw new Error(`Unknown Readmodel group ${name}; Sagas cannot be rebuilt`);
    const queue = readmodelOrder(groups).filter(
        (name) => !names || names.includes(name),
      ),
      width = Math.max(1, Math.min(concurrency, queue.length)),
      threads =
        width > 1 && !!config.configModule && availableParallelism() > 1;
    const results: Record<string, Awaited<ReturnType<typeof rebuild>>> = {},
      failed = new Map<string, unknown>(),
      ended: Record<string, Promise<void>> = {};
    const one = async (name: string) => {
      const reads = (groups[name]!.reads ?? []).filter((read) => read in ended);
      await Promise.all(reads.map((read) => ended[read]));
      const blocked = reads.find((read) => failed.has(read));
      if (blocked) throw new Error(`Not rebuilt: ${blocked}, which it reads, failed`);
      results[name] = threads
        ? await rebuildInWorker(
            name,
            swap,
            // The worker threads share this process's cache budget.
            Math.floor((config.rebuildCacheBytes ?? defaultRebuildCacheBytes) / width),
            signal,
          )
        : await rebuild(name, { swap, signal });
    };
    // In dependency order, so a group a runner waits for has already started.
    const runner = async () => {
      for (let name; (name = queue.shift()); )
        await (ended[name] = one(name).catch((error) => {
          failed.set(name, error);
        }));
    };
    await Promise.all(Array.from({ length: width }, runner));
    if (failed.size)
      throw new AggregateError(
        [...failed.values()],
        `Rebuild failed for ${[...failed.keys()].join(", ")}`,
      );
    return results;
  };
  /*
   * Replicas that start together all see the schema change. One rebuilds; the others
   * wait until it swaps the new generation in, instead of rebuilding again.
   */
  const startupRebuild = async (
    name: string,
    here: boolean,
    signal: AbortSignal,
  ) => {
    const store = groups[name]!.store;
    const before = requiredRebuilds.get(name);
    for (;;) {
      signal.throwIfAborted();
      if ((await store.activeGeneration(name)) !== before) {
        requiredRebuilds.delete(name);
        return;
      }
      // A replica that does not run the group only waits for the replica that does.
      if (!here) {
        await pause();
        continue;
      }
      try {
        // Resume a generation an interrupted or failed attempt left; its fingerprint must still match.
        return await rebuild(
          name,
          { signal },
          (await store.pendingGeneration(name)) ?? undefined,
        );
      } catch (error) {
        if (!(error instanceof RebuildRunningError)) throw error;
        readmodelsLogger.info(
          { consumerGroup: `readmodels:${name}` },
          "Waiting for another instance's rebuild",
        );
        await pause();
      }
    }
  };
  const swapRebuild = async (name: string) => {
    await initialize();
    const group = groups[name];
    if (!group) throw new Error(`Unknown Readmodel group ${name}`);
    const pending = await group.store.pendingGeneration(name);
    if (!pending)
      throw new Error("No pending rebuild for this Readmodel group");
    return rebuild(name, { swap: "auto" }, pending);
  };
  const start = (options?: { only?: string[] }) => {
    if (closing) return Promise.reject(new Error("Application is closed"));
    const run = startOnce(options);
    starting = Promise.allSettled([starting, run]);
    return run;
  };
  const startOnce = async ({ only = config.only }: { only?: string[] } = {}) => {
    if (started) throw new Error("Application already started");
    await initialize();
    // Health and `run` read `connected()`, so the broker is opened before anything starts.
    await transport.connect?.();
    const contexts = [
      ...new Set((config.aggregates ?? []).map((a) => a.context)),
    ];
    const components: Record<string, string[]> = {
      commands: [config.name, ...contexts.map((c) => `${config.name}/${c}`)],
      outbox: [config.name],
      relay: [config.name],
      gateway: Object.keys(config.gateways ?? {}),
      readmodels: Object.keys(groups),
      saga: Object.keys(sagas),
      workflow: Object.keys(workflows),
      worker: Object.keys(workers),
    };
    for (const selector of only ?? []) {
      if (
        !/^(gateway|commands|readmodels|saga|workflow|worker|outbox|relay):[^:]+$/.test(
          selector,
        )
      )
        throw new Error(`Invalid component selector ${selector}`);
      const [kind, name] = selector.split(":");
      if (name !== "*" && !components[kind!]!.includes(name!))
        throw new Error(`Unknown component selector ${selector}`);
    }
    try {
      for (const plugin of config.plugins ?? []) await plugin(pluginContext);
      await invoke("start", pluginContext);
      running = true;
      started = true;
      // `instance` is this replica's Lease owner in `consumers` rows and status output.
      consumersLogger.info({ instance: owner }, "Application starting consumers");
      if ([...starts.values()].includes("now")) startedAt = await store.head();
      if (store.listen) stops.push(await store.listen(wake));
      // Groups and Sagas waiting on a group they read resume when another replica commits it.
      if (readGroups().size)
        stops.push(
          await live.subscribe("readmodels", (batch: BatchNotification) => {
            if (readGroups().has(batch.group)) wake();
          }),
        );
      const observations = setInterval(() => {
        void Promise.all([
          consumerStatus().then((statuses) => {
            if (consumersLogger.isLevelEnabled?.("debug") === false) return;
            const held = statuses.filter(
              (status) =>
                status.lease?.owner === owner && status.lagMs !== undefined,
            );
            if (held.length)
              consumersLogger.debug(
                {
                  lagMs: Object.fromEntries(
                    held.map((status) => [
                      consumerId(status.group, status.partition),
                      status.lagMs,
                    ]),
                  ),
                },
                "Consumer group lag",
              );
          }),
          (store as typeof store & { readerLag?: () => Promise<number> })
            .readerLag?.()
            .then((value) => measure("eventfabric.reader.lag", value)),
        ]).catch((error) =>
          storeLogger.warn(
            { error: errorMessage(error) },
            "Metric observation failed",
          ),
        );
      }, 10000);
      stops.push(async () => {
        clearInterval(observations);
      });
      // Pools are sampled every 250 ms, so a queue shorter than the 10 s summary still shows.
      const pools = [
        { name: "events", port: store as unknown },
        ...[...new Set(Object.values(groups).map((group) => group.store))].map(
          (port) => ({
            name: `readmodels:${Object.keys(groups)
              .filter((name) => groups[name]!.store === port)
              .join(",")}`,
            port: port as unknown,
          }),
        ),
      ].flatMap(({ name, port }) => {
        const source = port as { poolStats?: () => PoolStats };
        return source.poolStats ? [{ name, stats: () => source.poolStats!() }] : [];
      });
      const occupancy = new Map<
        string,
        { samples: number; waiting: number; waitingMax: number; busyMax: number }
      >();
      const sampling = setInterval(() => {
        if (storeLogger.isLevelEnabled?.("debug") === false) return;
        for (const { name, stats } of pools) {
          const { total, idle, waiting } = stats(),
            entry = occupancy.get(name);
          occupancy.set(name, {
            samples: (entry?.samples ?? 0) + 1,
            waiting: (entry?.waiting ?? 0) + waiting,
            waitingMax: Math.max(entry?.waitingMax ?? 0, waiting),
            busyMax: Math.max(entry?.busyMax ?? 0, total - idle),
          });
        }
      }, 250);
      const summaries = setInterval(() => {
        for (const timing of Object.values(timings)) timing.flush();
        if (storeLogger.isLevelEnabled?.("debug") === false) {
          occupancy.clear();
          return;
        }
        for (const [name, entry] of occupancy)
          storeLogger.debug(
            {
              pool: name,
              waitingAvg: Math.round((entry.waiting / entry.samples) * 10) / 10,
              waitingMax: entry.waitingMax,
              busyMax: entry.busyMax,
            },
            "Pool occupancy",
          );
        occupancy.clear();
      }, 10000);
      const removePoolGauge = gauge("eventfabric.pool.waiting", () =>
        pools.map(({ name, stats }) => ({
          value: stats().waiting,
          attributes: { pool: name },
        })),
      );
      stops.push(async () => {
        clearInterval(sampling);
        clearInterval(summaries);
        removePoolGauge();
      });
      stops.push(consumers.startRenewals());
      // A Partition's two lanes run at once, so one Stream's Commands still take turns here.
      const turns = new Map<string, Promise<void>>();
      const inTurn = <T>(key: string, run: () => Promise<T>) => {
        const current = (turns.get(key) ?? Promise.resolve()).then(run, run);
        const settled = current.then(
          () => {},
          () => {},
        );
        turns.set(key, settled);
        void settled.then(() => {
          if (turns.get(key) === settled) turns.delete(key);
        });
        return current;
      };
      const handleCommand = async (command: Command): Promise<Outcome> => {
        const controller = new AbortController();
        let timer: ReturnType<typeof setTimeout> | undefined;
        const start = Date.now();
        try {
          return await withSpan("eventfabric.command.handle", command, () =>
            Promise.race([
              kernel.handle(command, {
                signal: controller.signal,
                deadline: performance.now() + (config.commandTimeout ?? 30000),
              }),
              new Promise<Outcome>((resolve) => {
                timer = setTimeout(() => {
                  controller.abort();
                  resolve({
                    status: "failed",
                    error: {
                      code: "timeout",
                      message: "Command handler timed out",
                    },
                  });
                }, config.commandTimeout ?? 30000);
              }),
            ]),
          );
        } finally {
          if (timer) clearTimeout(timer);
          const durationMs = Date.now() - start;
          measure("eventfabric.command.handle_duration", durationMs);
          if (durationMs >= slowHandlerThreshold)
            commandsLogger.warn(
              {
                command: command.name,
                stream: streamName(command.stream),
                commandId: command.id,
                correlationId: command.correlationId,
                durationMs,
              },
              "Slow Command handler",
            );
          wake();
        }
      };
      for (const context of contexts)
        if (
          selected(only, "commands", config.name) ||
          selected(only, "commands", `${config.name}/${context}`)
        )
          commandStops.push(
            await transport.handle(
              context,
              Array.from({ length: 128 }, (_, i) => i),
              (command) =>
                command.stream.id
                  ? inTurn(JSON.stringify(command.stream), () =>
                      handleCommand(command),
                    )
                  : handleCommand(command),
            ),
          );
      // A group waiting for its rebuild, or reading one that is, starts once that group swaps in.
      const startedConsumers = new Set<string>();
      const startable = (id: string, reads: string[] = []) =>
        running &&
        !startedConsumers.has(id) &&
        !reads.some((group) => requiredRebuilds.has(group));
      const startConsumers = () => {
        for (const [name, group] of Object.entries(groups)) {
          const id = `readmodels:${name}`;
          if (
            !selected(only, "readmodels", name) ||
            requiredRebuilds.has(name) ||
            !startable(id, group.reads)
          )
            continue;
          startedConsumers.add(id);
          for (let p = 0; p < partitionsOf(group); p++)
            consumers.run(
              consumerId(id, p),
              async (lease) => {
                if (
                  !(await layout.settled(readmodelLayout(name, group), lease))
                )
                  return false;
                return readmodelTick(
                  name,
                  group,
                  lease,
                  sliceOf(partitionsOf(group), p),
                );
              },
              { ready: p ? () => layout.ready(id, partitionsOf(group)) : undefined },
            );
        }
        for (const [kind, name, definition, process] of [
          ...Object.entries(sagas).map(
            ([name, saga]) => ["saga", name, saga, sagaProcess(name, saga)] as const,
          ),
          ...Object.entries(workflows).map(
            ([name, workflow]) =>
              ["workflow", name, workflow, workflowProcess(name, workflow)] as const,
          ),
        ]) {
          const id = `${kind}:${name}`;
          if (!selected(only, kind, name) || !startable(id, definition.reads))
            continue;
          startedConsumers.add(id);
          const count = partitionsOf(definition);
          for (let p = 0; p < count; p++)
            consumers.run(
              consumerId(id, p),
              async (lease) => {
                if (
                  !(await layout.settled(processLayout(id, count, process), lease))
                )
                  return false;
                return processTick(process, lease, sliceOf(count, p));
              },
              {
                ready: p ? () => layout.ready(id, count) : undefined,
              },
            );
        }
      };
      startConsumers();
      // Startup rebuilds run in dependency order behind live traffic; each resumes from its Checkpoint.
      const rebuilds = new AbortController();
      const rebuilding = (async () => {
        for (const name of readmodelOrder(groups))
          while (requiredRebuilds.has(name)) {
            try {
              await startupRebuild(
                name,
                selected(only, "readmodels", name),
                rebuilds.signal,
              );
            } catch (error) {
              if (rebuilds.signal.aborted) return;
              readmodelsLogger.error(
                { consumerGroup: `readmodels:${name}`, error: errorMessage(error) },
                "Startup rebuild failed; retrying from its Checkpoint",
              );
              await delay(5000, undefined, { signal: rebuilds.signal }).catch(
                () => {},
              );
            }
            startConsumers();
          }
      })();
      stops.push(async () => {
        rebuilds.abort();
        wake();
        await rebuilding;
      });
      if (selected(only, "outbox", config.name))
        consumers.run("outbox", (lease) => relay.dispatch(lease), {
          idle: async () => {
            const version = wakeVersion;
            const delay = await relay.nextDelay();
            if (version !== wakeVersion || !running) return;
            await pause(Math.min(config.pollInterval ?? 1000, delay ?? Infinity));
          },
          requires: () => transport.connected(),
        });
      if (selected(only, "relay", config.name))
        consumers.run("relay", async (lease) => {
          const events = await store.readAll({
            fromPosition: await storedCheckpoint(lease),
            limit: 100,
          });
          if (!events.length) return false;
          // Both timestamps come from the log, so host clock skew cannot trigger a jump.
          const [latest] = await store.readAll({ reverse: true, limit: 1 });
          if (
            latest &&
            parseTime(latest.timestamp) - parseTime(events[0]!.timestamp) >
              (config.relayMaxLag ?? 60000)
          ) {
            await store.commitCheckpoint(lease, latest.position);
            checkpoints.set("relay", { epoch: lease.epoch, position: latest.position });
            return true;
          }
          // One Checkpoint per batch: after a crash, subscribers drop what they already saw.
          await Promise.all(
            events.map((event) => transport.publish("events", event)),
          );
          await store.commitCheckpoint(lease, events.at(-1)!.position);
          checkpoints.set("relay", {
            epoch: lease.epoch,
            position: events.at(-1)!.position,
          });
          return true;
        }, { requires: () => transport.connected() });
      for (const [name, worker] of Object.entries(workers))
        if (selected(only, "worker", name))
          stops.push(
            await transport.consume(
              workerQueue(name),
              (message) => workerAttempt(name, worker, message),
              { concurrency: worker.concurrency ?? 1 },
            ),
          );
      for (const [name, definition] of Object.entries(config.gateways ?? {}))
        if (selected(only, "gateway", name)) {
          const { startGateway } = await import("../gateway/index.js");
          stops.push(
            await startGateway(name, definition, {
              application: api,
              invoke: (...args: any[]) =>
                invoke("gateway", pluginContext, ...args),
              logger: logger.child({ component: "gateway" }),
              timeout: shutdownTimeout - shutdownReserve,
            }),
          );
        }
      stops.push(async () => {
        removeGauge();
      });
      // What this replica runs, for an operator reading the start; no addresses or credentials.
      logger.child({ component: "application" }).info(
        {
          instance: owner,
          application: config.name,
          version,
          node: process.version,
          roles: Object.fromEntries(
            Object.entries(components)
              .map(
                ([kind, names]) =>
                  [kind, names.filter((name) => selected(only, kind, name))] as const,
              )
              .filter(([, names]) => names.length),
          ),
          readmodels: Object.fromEntries(
            await Promise.all(
              Object.entries(groups).map(async ([name, group]) => [
                name,
                {
                  partitions: partitionsOf(group),
                  store: await Promise.resolve(group.store.storeId?.()).catch(
                    () => undefined,
                  ),
                },
              ]),
            ),
          ),
          sagas: Object.keys(sagas),
          workers: Object.keys(workers),
          slowHandlerThreshold,
        },
        "Application started",
      );
    } catch (error) {
      running = false;
      wake();
      await Promise.allSettled(
        stops
          .splice(0)
          .reverse()
          .map((stop) => stop()),
      );
      await Promise.allSettled(consumers.loops);
      await Promise.allSettled(commandStops.splice(0).map((stop) => stop()));
      await consumers.release();
      throw error;
    }
    return api;
  };
  const removeGauge = gauge("eventfabric.consumer.state", () =>
    [...consumers.statuses.values()].flatMap((entry) =>
      ["halted", "waiting"].map((state) => ({
        value: entry.state === state ? 1 : 0,
        attributes: { group: entry.group, partition: entry.partition, state },
      })),
    ),
  );
  const removeDurationGauge = gauge("eventfabric.consumer.state_duration", () =>
    [...consumers.statuses.values()].map((entry) => ({
      value: consumers.stateDuration(entry),
      attributes: {
        group: entry.group,
        partition: entry.partition,
        state: entry.state,
      },
    })),
  );
  const stop = async () => {
    running = false;
    wake();
    // Only a started replica reports its shutdown; a CLI rebuild or Task never started one.
    const stopping = Date.now(),
      lifecycle = started
        ? logger.child({ component: "application" })
        : createLogger({ level: "silent" });
    lifecycle.info({ instance: owner }, "Application stopping");
    const deadline = Date.now() + shutdownTimeout;
    const errors: unknown[] = [];
    const within = async (until: number, work: Promise<unknown>) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        return await Promise.race([
          work.then(() => true),
          new Promise<false>((resolve) => {
            timer = setTimeout(
              () => resolve(false),
              Math.max(0, until - Date.now()),
            );
          }),
        ]);
      } finally {
        if (timer) clearTimeout(timer);
      }
    };
    try {
      // Draining keeps a reserve so hooks and Lease release still run when a request hangs.
      const drained = await within(
        deadline - shutdownReserve,
        (async () => {
          for (const result of await Promise.allSettled(
            stops
              .splice(0)
              .reverse()
              .map((stop) => stop()),
          ))
            if (result.status === "rejected") errors.push(result.reason);
          await Promise.allSettled([
            ...consumers.loops,
            ...(closing ? active : []),
          ]);
          // The Outbox loop's last pass can start deliveries, so they are collected after it ends.
          await Promise.allSettled(relay.inFlight());
          for (const result of await Promise.allSettled(
            commandStops.splice(0).map((stop) => stop()),
          ))
            if (result.status === "rejected") errors.push(result.reason);
        })(),
      );
      if (!drained) {
        errors.push(new Error("Application shutdown timed out"));
        // Out of budget: the Command handlers stop without being waited on, as a hung stop above is not.
        for (const stop of commandStops.splice(0)) stop().catch(() => {});
      }
      const callbacks = [...(hooks.get("shutdown") ?? [])].reverse();
      for (const [index, callback] of callbacks.entries()) {
        // Share the remaining budget with later hooks and Lease release.
        const until =
          Date.now() +
          Math.max(0, deadline - Date.now()) / (callbacks.length - index + 1);
        if (
          !(await within(
            until,
            Promise.resolve()
              .then(() => callback(pluginContext))
              .catch((error) => errors.push(error)),
          ))
        )
          errors.push(new Error("Application shutdown hook timed out"));
      }
      const released = await within(
        deadline,
        consumers.release().then((results) => {
          for (const result of results)
            if (result.status === "rejected") errors.push(result.reason);
        }),
      );
      if (!released)
        errors.push(new Error("Application shutdown Lease release timed out"));
      if (errors.length) {
        lifecycle.error(
          { instance: owner, errors: errors.map(errorMessage) },
          "Application shutdown failed",
        );
        throw errors.length === 1
          ? errors[0]
          : new AggregateError(errors, "Application shutdown failed");
      }
      lifecycle.info(
        { instance: owner, durationMs: Date.now() - stopping },
        "Application stopped",
      );
    } finally {
      removeGauge();
      removeDurationGauge();
      consumers.clear();
      started = false;
    }
  };
  const health = async () => {
    let postgres = true;
    try {
      await store.ping();
    } catch {
      postgres = false;
    }
    let active: ConsumerStatus[] = [];
    if (postgres) {
      try {
        active = await store.consumers();
      } catch {
        postgres = false;
      }
    }
    const held = consumers.held(active);
    return {
      // The transport does not reconnect: a lost broker needs a restart.
      liveness: transport.connected(),
      readiness: running && postgres && transport.connected() && held,
      postgres,
      broker: transport.connected(),
      leases: held,
      // Reads of these groups fail with ReadmodelRebuildingError until their new generation swaps in.
      rebuilding: [...requiredRebuilds.keys()],
    };
  };
  const consumerStatus = consumers.consumers;
  const skipConsumer = async (
    groupName: string,
    eventId: string,
    who: string,
  ) => {
    await prepare();
    await consumers.skip(groupName, eventId, who);
    wake();
  };
  const domain = buildDomain(
    config.aggregates ?? [],
    ({ notBefore: _, replaceKey: __, ...input }) => sendCommand(input),
    store,
  );
  /**
   * An entry point's whole lifetime: starts unless already started, waits for SIGTERM, SIGINT or
   * `signal`, then closes. Resolves the exit code: 1 when the broker connection was lost, since the
   * Transport does not reconnect and the supervisor should restart the process.
   */
  const run = async ({
    only,
    signal,
  }: { only?: string[]; signal?: AbortSignal } = {}) => {
    try {
      if (!started) await start({ only });
      if (!(await waitForShutdown(signal, () => !transport.connected())))
        return 0;
      logger.error(
        {
          component: "application",
          recovery:
            "the supervisor restarts this instance; if it keeps exiting, check the broker and its address",
        },
        "Broker connection lost; exiting",
      );
      return 1;
    } finally {
      await api.close();
    }
  };
  const api = {
    start,
    stop,
    run,
    sendCommand,
    /** Sends a Saga Message to an instance, now or `at` a due time (ADR-0020). */
    sendSagaMessage: ((saga, name, payload, options) =>
      sendSagaMessage(saga, name, payload, options)) as SendSagaMessage<Promise<void>>,
    async getSchedule(stream: StreamId, name: string) {
      const current = await store.getSchedule(stream, name);
      if (!current) return null;
      const { fingerprint: _, ...schedule } = current;
      return schedule;
    },
    readmodels: readmodels as unknown as Readmodels,
    model,
    config: { ...config, readmodels: groups },
    initialize,
    rebuild: (...args: Parameters<typeof rebuild>) => track(rebuild(...args)),
    rebuildGroups: (...args: Parameters<typeof rebuildGroups>) =>
      track(rebuildGroups(...args)),
    importReadmodels: (...args: Parameters<typeof importReadmodels>) =>
      track(importReadmodels(...args)),
    [foldSlice]: rebuildSlice,
    ...applicationWorkflows,
    [workflowsApi]: {
      start: (name: string, id: string, input?: unknown, options?: { reuse?: boolean; signal?: SignalInput }) =>
        applicationWorkflows.startWorkflow(name as never, id, input as never, options as never),
      signal: (name: string, id: string, signal: string, payload?: unknown) =>
        applicationWorkflows.sendSignal(id, { workflow: name, name: signal, payload }),
      query: (name: string, id: string) => applicationWorkflows.queryWorkflow(name as never, id),
      cancel: (name: string, id: string) => operateWorkflow("cancel", name, id),
      resume: (name: string, id: string) => operateWorkflow("resume", name, id),
      /** A raw Saga Message to an instance, as a Worker's reply is sent (the process fixture's `reply`). */
      send: sendWorkflowMessage,
    },
    swapRebuild: (name: string) => track(swapRebuild(name)),
    health,
    kernel: {
      ...kernel,
      handle: (...args: Parameters<typeof kernel.handle>) =>
        track(kernel.handle(...args)),
    },
    wake,
    statuses: consumers.statuses,
    consumerStatus,
    retireConsumer: consumers.retire,
    skipConsumer,
    subscribeEvents,
    notify,
    subscribeNotifications,
    /** The ready services; throws before `initialize()` resolves when `setupServices` builds them. */
    getServices: () => currentServices() as S,
    getReadmodels: () => readmodels as unknown as Readmodels,
    getDomain: (): Domain<
      ReturnType<typeof sendCommand>,
      [],
      { load(options?: { atRevision?: number }): Promise<any> }
    > => domain,
    /**
     * Stops, cleans up what `setupServices` acquired, then closes the configured stores and
     * Transport. Terminal: later calls return the same result, and the Application cannot start again.
     */
    close(): Promise<void> {
      return (closing ??= (async () => {
        const errors: unknown[] = [];
        // A start or setup in progress finishes first, so what it acquires is cleaned up too.
        await Promise.allSettled([starting, preparing, servicesReady]);
        await stop().catch((error) => errors.push(error));
        // Shutdown hooks ran inside stop() and could still use services.
        await resources.disposeAsync().catch((error) => errors.push(error));
        if (config.setupServices) services = undefined;
        const adapters = new Set([
          ...Object.values(groups).map((group) => group.store),
          transport,
          store,
        ]);
        const closed = await Promise.allSettled(
          [...adapters].map((adapter) =>
            Promise.resolve().then(() => adapter.close()),
          ),
        );
        for (const result of closed)
          if (result.status === "rejected") errors.push(result.reason);
        if (errors.length)
          throw errors.length === 1
            ? errors[0]
            : new AggregateError(errors, "Application close failed");
      })());
    },
    async awaitCorrelated(
      input: CommandInput,
      pattern: EventPattern,
      options: {
        predicate?: (event: RecordedEvent) => boolean;
        deadline?: number;
        lane?: CommandLane;
      } = {},
    ): ReturnType<AwaitCorrelated> {
      const command = createCommand(input);
      const expiresAt = Date.now() + (options.deadline ?? 10000);
      let buffered: RecordedEvent | undefined;
      let resolveEvent: ((event: RecordedEvent) => void) | undefined;
      const matcher = (event: RecordedEvent) =>
        event.correlationId === command.correlationId &&
        matchPattern(eventName(event), pattern) &&
        (!options.predicate || options.predicate(event));
      const stop = await subscribeEvents((event) => {
        if (matcher(event)) {
          buffered ??= event;
          resolveEvent?.(buffered);
        }
      });
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        const before = await store.head();
        const outcome = await sendCommand(command, {
          lane: options.lane,
          deadline: Math.max(0, expiresAt - Date.now()),
        });
        if (outcome.status === "rejected" || outcome.status === "failed")
          return { outcome, status: outcome.status };
        const direct =
          (outcome.status === "accepted"
            ? outcome.events.find(matcher)
            : undefined) ?? buffered;
        if (direct) return { status: "accepted", event: direct, outcome };
        const event = await Promise.race([
          new Promise<RecordedEvent>((resolve) => {
            resolveEvent = resolve;
          }),
          new Promise<null>((resolve) => {
            timer = setTimeout(
              () => resolve(null),
              Math.max(0, expiresAt - Date.now()),
            );
          }),
        ]);
        if (event) return { status: "accepted", event, outcome };
        let fromPosition: Position | undefined = before ?? undefined;
        let found: RecordedEvent | undefined;
        for (;;) {
          const stored = await store.readAll({
            fromPosition,
            correlationId: command.correlationId,
          });
          found = stored.find(matcher);
          if (found || stored.length < 1000) break;
          fromPosition = stored.at(-1)!.position;
        }
        return found
          ? { status: "accepted", event: found, outcome }
          : { status: "unknown", outcome };
      } finally {
        if (timer) clearTimeout(timer);
        await stop();
      }
    },
  };
  return api;
}
/** A name test for `pattern`, where `*` matches any characters; compile once for repeated tests. */
export const patternMatcher = (pattern: string): ((name: string) => boolean) => {
  const regex = new RegExp(
    `^${pattern
      .split("*")
      .map((piece) => piece.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"))
      .join(".*")}$`,
  );
  return (name) => regex.test(name);
};
export const matchPattern = (name: string, pattern: string): boolean =>
  patternMatcher(pattern)(name);
/** The Command's first Event matching `pattern` (or any of a list); a Rejection throws it. */
export const awaitEvent = (
  outcome: Outcome,
  pattern: string | readonly string[],
): RecordedEvent => {
  if (outcome.status === "rejected") {
    const { kind, code, message, details, thrown } = outcome.error;
    throw Object.assign(new Rejection(kind ?? code, message, details, code), {
      thrown,
    });
  }
  if (outcome.status !== "accepted")
    throw new Error(`Command ${outcome.status}`);
  const event = outcome.events.find((event) =>
    [pattern].flat().some((one) => matchPattern(eventName(event), one)),
  );
  if (!event) throw new Error("Command accepted without awaited Event");
  return event;
};
export type Application<S = RegisteredServices> = ReturnType<
  typeof createApplication<S>
>;
