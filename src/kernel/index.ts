import {
  ConcurrencyError,
  DuplicateCommandError,
  Rejection,
} from "../errors.js";
import { commandFingerprint } from "../event-store/rules.js";
import { createCommand, uuidv7 } from "../id.js";
import { assertScheduleAttempt } from "../schedules.js";
import { setTimeout as sleep } from "node:timers/promises";
import { recordEffects } from "../effects.js";
import type { MessageRouting, QueueOptions } from "../effects.js";
import type { CommandInput } from "../id.js";
import type { UpcasterDefinition } from "../event-store/upcasters.js";
import type {
  Command,
  EventStore,
  Logger,
  Metadata,
  Outcome,
  PendingEvent,
  RecordedEvent,
  Schema,
  Validate,
} from "../types.js";
import {
  assertMetadata,
  createValidator,
  toJson,
  ValidationError,
} from "../validation.js";
import {
  createLogger,
  measure,
  count,
  streamName,
  withSpan,
} from "../observability.js";
import { legacyEnrichers, parseTime } from "../internal.js";

export type LegacyCommand = Omit<Command, "stream"> & {
  context: string;
  aggregate: { id: string; name: string };
  now: Date;
};
export type LegacyEvent = PendingEvent & {
  context: { name: string };
  aggregate: { id: string; name: string };
  revision: number;
  stream: Command["stream"];
};
export type Step = (
  command: LegacyCommand,
  aggregate: AggregateApi,
  app: any,
) => unknown;
export type Rule = (
  current: AggregateState,
  previous: AggregateState,
  events: readonly PendingEvent[],
  command: LegacyCommand,
  app: any,
) => unknown;
export type PreLoad = (command: LegacyCommand, app: any) => unknown;
export type Fold = (event: LegacyEvent, state: AggregateState) => void;
export type AggregateDefinition = {
  context: string;
  name: string;
  initialState: Record<string, any>;
  newId?: (command: LegacyCommand, app: any) => string | Promise<string>;
  commands: Record<
    string,
    {
      existence?: "exists" | "notExists";
      schema?: Schema;
      preLoad: PreLoad[];
      steps: Step[];
      rules: Rule[];
    }
  >;
  events: Record<string, Fold>;
  eventVersions?: Record<string, number>;
  eventSchemas?: Record<string, UpcasterDefinition>;
  enrich?: (
    event: Readonly<LegacyEvent>,
    state: AggregateState,
    command: LegacyCommand,
  ) => Metadata | void;
};
const safePath = (path: string): string[] => {
  const keys = path.split(".");
  if (
    keys.some((key) => ["__proto__", "constructor", "prototype"].includes(key))
  )
    throw Error("Unsafe state path");
  return keys;
};
export class AggregateState {
  revision = 0;
  locals: Record<string, any> = {};
  attributes: Record<string, any>;
  private readonly eventCopies = new WeakMap<object, any>();
  constructor(
    public id: string,
    attributes: Record<string, any>,
    /** False when the caller hands over attributes nobody else holds. */
    copy = true,
  ) {
    this.attributes = copy ? structuredClone(attributes) : attributes;
  }
  /** Reads expose the Stream id as `id` unless the state has its own; it is never stored in attributes. */
  get(path?: string): any {
    const own = this.attributes.id !== undefined;
    if (path === "id") return own ? this.attributes.id : this.id;
    if (path)
      return safePath(path).reduce(
        (value, key) => value?.[key],
        this.attributes,
      );
    return own ? this.attributes : { ...this.attributes, id: this.id };
  }
  set(path: string | Record<string, any>, value?: any): void {
    if (typeof path !== "string") {
      for (const [key, entry] of Object.entries(path)) this.set(key, entry);
      return;
    }
    const keys = safePath(path),
      leaf = keys.pop()!;
    let target = this.attributes;
    for (const key of keys) {
      if (target[key] == null) target[key] = {};
      target = target[key];
    }
    target[leaf] = this.mutable(value);
  }
  private mutable(value: any, seen = new WeakSet<object>()): any {
    if (!value || typeof value !== "object") return value;
    value = this.eventCopies.get(value) ?? value;
    if (seen.has(value)) return value;
    if (Object.isFrozen(value)) {
      // Detach Event data once, preserving aliases between State fields that share it.
      const copy = Array.isArray(value)
        ? [...value]
        : Object.getPrototypeOf(value) === Object.prototype
          ? { ...value }
          : structuredClone(value);
      this.eventCopies.set(value, copy);
      value = copy;
    }
    seen.add(value);
    for (const [key, child] of Object.entries(value))
      value[key] = this.mutable(child, seen);
    return value;
  }
  clone(): AggregateState {
    const copy = new AggregateState(this.id, this.attributes);
    copy.revision = this.revision;
    copy.locals = structuredClone(this.locals);
    return copy;
  }
}
export type AggregateApi = {
  id: string;
  readonly revision: number;
  locals: Record<string, any>;
  now: Date;
  get: (path?: string) => any;
  apply: Record<string, (payload?: any, metadata?: Metadata) => void>;
};
const freeze = <T>(value: T): T => {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
};
const draft = (
  definition: AggregateDefinition,
  id: string,
  event: PendingEvent,
  revision: number,
  /** True when nobody else holds `event`'s data, so it can be frozen in place. */
  owned = false,
): LegacyEvent => ({
  ...(owned ? event : structuredClone(event)),
  context: { name: definition.context },
  aggregate: { id, name: definition.name },
  stream: { context: definition.context, aggregate: definition.name, id },
  revision,
});
const envelope = (...args: Parameters<typeof draft>): LegacyEvent =>
  freeze(draft(...args));
/**
 * A `legacyEnricher` runs as the old one did: inside `apply`, before the fold, on a mutable Event
 * and the State before it. The Event it returns, or the one it changed, is recorded and folded.
 */
const enrichOnApply = (
  definition: AggregateDefinition,
  state: AggregateState,
  event: PendingEvent,
  command: LegacyCommand,
): PendingEvent => {
  // The envelope apart from payload and metadata, to refuse changes to it; only the enricher's Event is copied whole.
  const { payload: _, metadata: __, ...frame } = event;
  const before = draft(definition, state.id, frame as PendingEvent, state.revision + 1);
  const mutable = draft(definition, state.id, event, state.revision + 1);
  const returned = definition.enrich!(mutable, state, command);
  const enriched: any =
    returned && typeof returned === "object" ? returned : mutable;
  // Only the payload and metadata are the Application's; the rest of the envelope is recorded by EventFabric.
  for (const key of new Set([...Object.keys(before), ...Object.keys(enriched)]))
    if (
      !["payload", "metadata"].includes(key) &&
      JSON.stringify((before as any)[key]) !== JSON.stringify(enriched[key])
    )
      throw Error(
        `eventEnricher changed the Event's ${key}; only payload and metadata are recorded`,
      );
  return {
    ...event,
    payload: toJson(enriched.payload ?? {}, "Payload"),
    metadata: toJson(enriched.metadata ?? {}, "Metadata"),
  };
};
const fold = (
  definition: AggregateDefinition,
  state: AggregateState,
  event: PendingEvent,
  revision: number,
  owned = false,
): void => {
  const handler = definition.events[event.name];
  if (!handler)
    throw Error(
      `Unknown Event ${definition.context}.${definition.name}.${event.name}`,
    );
  handler(envelope(definition, state.id, event, revision, owned), state);
  state.revision = revision;
};
export const loadAggregate = (
  definition: AggregateDefinition,
  id: string,
  history: RecordedEvent[],
): AggregateState => fromHistory(definition, id, history, false);
/** A Stream read hands over fresh Events (the Upcaster codec takes ownership), so the load freezes them instead of copying. */
const fromHistory = (
  definition: AggregateDefinition,
  id: string,
  history: RecordedEvent[],
  owned: boolean,
): AggregateState => {
  const state = new AggregateState(id, definition.initialState);
  for (const event of history)
    fold(definition, state, event, event.revision, owned);
  return state;
};
export type TellInput = CommandInput & {
  notBefore?: string;
  replaceKey?: string;
};
export type Tell = { (input: TellInput): void; (): any };
export type KernelOptions = {
  definitions: AggregateDefinition[];
  store: EventStore;
  validate?: Validate;
  metadataSchema?: Schema;
  metadataMaxBytes?: number;
  services?:
    Record<string, any> | ((command: LegacyCommand, tell: Tell) => any);
  readmodels?: any;
  /** Routes and checks queued Messages before commit; without it a Message goes to the queue named after it. */
  routing?: MessageRouting;
  clock?: () => Date;
  logger?: Logger;
  retries?: number;
  development?: boolean;
};
/** Old Application errors are often plain objects with a message rather than Errors. */
const messageOf = (error: unknown): string =>
  error instanceof Error
    ? error.message
    : typeof (error as any)?.message === "string"
      ? (error as any).message
      : String(error);
/** An error's name and own fields that JSON can carry, so a Rejection crosses the Transport with them. */
const thrownOf = (error: unknown) => {
  if (!error || typeof error !== "object") return undefined;
  const fields: Record<string, unknown> = { name: (error as Error).name };
  for (const [key, value] of Object.entries(error))
    if (key !== "stack")
      try {
        fields[key] = toJson(value);
      } catch {
        // A field JSON cannot carry stays behind.
      }
  return fields;
};
export const createKernel = (options: KernelOptions) => {
  const { store, clock = () => new Date(), retries = 3 } = options;
  const validate = options.validate ?? createValidator();
  const logger = options.logger ?? createLogger({ level: "silent" });
  const definitions = new Map<string, AggregateDefinition>();
  for (const definition of options.definitions) {
    const key = JSON.stringify([definition.context, definition.name]);
    if (definitions.has(key))
      throw Error(
        `Duplicate Aggregate ${definition.context}.${definition.name}`,
      );
    definitions.set(key, definition);
  }
  const checked = async (kind: string, run: () => unknown) => {
    try {
      const value = await run();
      if (
        (options.development ?? process.env.NODE_ENV !== "production") &&
        (value === false || typeof value === "string")
      )
        options.logger?.warn(
          { kind },
          "Only throwing rejects a Command; check returned a value",
        );
    } catch (error) {
      if (error instanceof Rejection) throw error;
      const rejection = new Rejection(
        kind,
        messageOf(error),
        (error as any)?.details ?? (error as any)?.errors,
      );
      // EventFabric's own schema rejection is no Application error; its issues are the details.
      if (!(error instanceof ValidationError))
        rejection.thrown = thrownOf(error);
      throw rejection;
    }
  };
  const metadata = (value: Metadata) =>
    assertMetadata(value, {
      validate,
      schema: options.metadataSchema,
      maxBytes: options.metadataMaxBytes,
    });
  const handle = async (
    incoming: Command,
    settings: { signal?: AbortSignal; deadline?: number } = {},
  ): Promise<Outcome> => {
    let canRecheckSchedule = false;
    // Lateness is reported once, when the Schedule attempt reaches a final outcome.
    let lateness: (() => void) | undefined;
    const checkDeadline = () => {
      settings.signal?.throwIfAborted();
      if (
        settings.deadline !== undefined &&
        performance.now() >= settings.deadline
      )
        throw new Error("Command handler timed out");
    };
    try {
      checkDeadline();
      const json = (value: unknown, label: string): any => {
        try {
          return value === undefined ? undefined : toJson(value, label);
        } catch (error) {
          throw new Rejection("validation", messageOf(error));
        }
      };
      const { payload, metadata: meta, schedule, ...address } = incoming;
      const validated: Command = createCommand({
        ...structuredClone(address),
        payload: json(payload, "Payload"),
        metadata: json(meta, "Metadata"),
      });
      if (schedule !== undefined) {
        await checked("validation", () => {
          assertScheduleAttempt(schedule);
          if (!validated.stream.id) throw new Error("Invalid Schedule attempt");
        });
        validated.schedule = { name: schedule.name, dueAt: schedule.dueAt };
      }
      if (validated.schedule) {
        await checked("validation", () =>
          assertMetadata(validated.metadata, {
            maxBytes: options.metadataMaxBytes,
          }),
        );
        canRecheckSchedule = true;
      }
      const definition = definitions.get(
        JSON.stringify([validated.stream.context, validated.stream.aggregate]),
      );
      const spec = definition?.commands[validated.name];
      // The Command as sent: a creating Command's Stream id is still empty here, so a retry matches.
      const fingerprint = commandFingerprint(validated);
      const initialSchedule = validated.schedule
        ? await store.getSchedule(validated.stream, validated.schedule.name)
        : null;
      const obsoleteInitially =
        !!validated.schedule && initialSchedule?.commandId !== validated.id;
      if ((!definition || !spec) && !obsoleteInitially)
        throw new Rejection(
          "unknown-command",
          `Unknown Command ${validated.stream.context}.${validated.stream.aggregate}.${validated.name}`,
        );
      if (!obsoleteInitially)
        await checked("validation", () => metadata(validated.metadata));
      if (!obsoleteInitially && spec?.schema !== undefined)
        await checked("validation", () =>
          validate(spec.schema!, validated.payload),
        );
      const duplicate = async (): Promise<Outcome | undefined> => {
        const accepted = await store.acceptedCommand(validated.id);
        if (!accepted) return undefined;
        if (accepted.fingerprint !== fingerprint)
          throw new Rejection(
            "conflict",
            "Command id was already accepted for a different Command",
          );
        return {
          status: "accepted",
          events: accepted.events,
          duplicate: true,
          ...(accepted.cancellations
            ? { cancellations: accepted.cancellations }
            : {}),
        };
      };
      if (validated.schedule && !obsoleteInitially) {
        // Clock skew between relay and kernel: wait out a short early arrival instead of a one-second Outbox retry.
        const early = parseTime(validated.schedule.dueAt) - clock().getTime();
        if (
          early > 0 &&
          early <= 1000 &&
          (settings.deadline === undefined ||
            performance.now() + early < settings.deadline)
        )
          await sleep(early, undefined, { signal: settings.signal });
      }
      for (let attempt = 0; attempt <= retries; attempt++) {
        // Reading the Stream runs no Application code, so it overlaps the Duplicate check: one
        // round trip instead of two. A creating Command learns its id later.
        const reading =
          validated.stream.id && !validated.schedule
            ? { at: Date.now(), history: store.readStream(validated.stream) }
            : undefined;
        reading?.history.catch(() => {});
        const replay = await duplicate();
        if (replay) return replay;
        // Step mutations of payload/metadata must not leak into a retry
        const command: Command = {
          ...validated,
          payload: structuredClone(validated.payload),
          metadata: structuredClone(validated.metadata),
        };
        const now = new Date(clock());
        if (!Number.isFinite(now.getTime()))
          throw Error("Invalid kernel clock");
        const timestamp = now.toISOString();
        const scheduled =
          attempt === 0
            ? initialSchedule
            : command.schedule
              ? await store.getSchedule(command.stream, command.schedule.name)
              : null;
        const obsolete =
          !!command.schedule && scheduled?.commandId !== command.id;
        if (
          command.schedule &&
          !obsolete &&
          scheduled?.fingerprint !== fingerprint
        )
          throw new Rejection(
            "conflict",
            "Scheduled Command does not match its recorded attempt",
          );
        const scheduleGuard = command.schedule
          ? {
              name: command.schedule.name,
              commandId: command.id,
              fingerprint,
              obsolete,
            }
          : undefined;
        if (obsolete) {
          try {
            const history = await store.readStream(command.stream);
            await store.appendToStream(
              command.stream,
              history.at(-1)?.revision ?? 0,
              [],
              command,
              { fingerprint, scheduleGuard, signal: settings.signal },
            );
            return { status: "accepted", events: [] };
          } catch (error) {
            if (error instanceof DuplicateCommandError) {
              const replay = await duplicate();
              if (replay) return replay;
            }
            if (error instanceof ConcurrencyError && attempt < retries)
              continue;
            throw error;
          }
        }
        if (!definition || !spec)
          throw new Error("Missing Aggregate definition");
        const legacy = (): LegacyCommand => ({
          ...command,
          context: command.stream.context,
          aggregate: { id: command.stream.id, name: command.stream.aggregate },
          now: new Date(now),
        });
        const effects = recordEffects(
          command,
          `${definition.context}.${definition.name}`,
          { now, routing: options.routing, owner: command.stream },
        );
        let loaded = false;
        const tell: Tell = ((input?: TellInput): any => {
          if (!input) return buildDomain(definitions.values(), tell, store);
          if (!loaded) throw Error("tell() is unavailable before Stream load");
          effects.tell(input);
        }) as Tell;
        const app = {
          ...(typeof options.services === "function"
            ? options.services(legacy(), tell)
            : {
                getServices: () => options.services ?? {},
                getReadmodels: () => options.readmodels ?? {},
              }),
        };
        app.tell = tell;
        app.now = new Date(now);
        app.signal = settings.signal;
        app.queue = (name: string, payload: any, settings?: QueueOptions) => {
          if (!loaded) throw Error("queue() is unavailable before Stream load");
          effects.queue(name, payload, settings);
        };
        app.schedule = (...args: Parameters<typeof effects.schedule>) => {
          if (!loaded)
            throw Error("schedule() is unavailable before Stream load");
          effects.schedule(...args);
        };
        app.cancelSchedule = (name: string) => {
          if (!loaded)
            throw Error("cancelSchedule() is unavailable before Stream load");
          effects.cancelSchedule(name);
        };
        app.getDomain ??= () => buildDomain(definitions.values(), tell, store);
        app.getReadmodels ??= () => options.readmodels ?? {};
        app.getServices ??= () => options.services ?? {};
        app.logger ??= logger;
        for (const check of spec.preLoad)
          await checked("precondition", () => check(legacy(), app));
        if (!command.stream.id)
          command.stream.id = definition.newId
            ? await definition.newId(legacy(), app)
            : uuidv7(now.getTime());
        const loadedAt = reading?.at ?? Date.now();
        const history = await (reading?.history ??
          store.readStream(command.stream));
        const state = fromHistory(definition, command.stream.id, history, true);
        measure("eventfabric.aggregate.load_events", history.length, {
          aggregate: `${definition.context}.${definition.name}`,
        });
        measure("eventfabric.aggregate.load_duration", Date.now() - loadedAt, {
          aggregate: `${definition.context}.${definition.name}`,
        });
        const expected = state.revision;
        loaded = true;
        if (!obsolete && spec.existence === "exists" && expected === 0)
          throw new Rejection("existence", "Aggregate does not exist");
        if (!obsolete && spec.existence === "notExists" && expected !== 0)
          throw new Rejection("existence", "Aggregate already exists");
        const previous = state.clone(),
          pending: PendingEvent[] = [];
        const apply = Object.fromEntries(
          Object.keys(definition.events).map((name) => [
            name,
            (payload: any = {}, extra: Metadata = {}) => {
              let event: PendingEvent = {
                id: uuidv7(now.getTime()),
                name,
                payload: toJson(payload, "Payload"),
                metadata: toJson({ ...command.metadata, ...extra }, "Metadata"),
                schemaVersion: definition.eventVersions?.[name] ?? 1,
                timestamp,
                commandId: command.id,
                causationId: command.id,
                correlationId: command.correlationId,
                ...(command.traceparent
                  ? { traceparent: command.traceparent }
                  : {}),
              };
              if (definition.enrich && legacyEnrichers.has(definition.enrich))
                event = enrichOnApply(definition, state, event, legacy());
              fold(definition, state, event, state.revision + 1);
              pending.push(event);
            },
          ]),
        );
        const api: AggregateApi = {
          id: state.id,
          get revision() {
            return state.revision;
          },
          locals: state.locals,
          now: new Date(now),
          get: (path) => state.get(path),
          apply,
        };
        if (command.schedule) {
          const { dueAt, name } = command.schedule;
          const late = clock().getTime() - parseTime(dueAt);
          if (!Number.isFinite(late)) throw new Error("Invalid kernel clock");
          // Never early: the business check runs only once the kernel's clock reaches the due time.
          if (late < 0) return { status: "unknown", commandId: command.id };
          lateness = () => {
            const attributes = {
              aggregate: `${definition.context}.${definition.name}`,
              schedule: name,
            };
            measure("eventfabric.schedule.lateness", late, attributes);
            if (late <= 100) return;
            count("eventfabric.schedule.missed", attributes);
            options.logger?.warn(
              {
                ...attributes,
                commandId: command.id,
                correlationId: command.correlationId,
                dueAt,
                lateness: late,
              },
              "Schedule business check missed its 100 ms target",
            );
          };
        }
        for (const step of spec.steps) {
          checkDeadline();
          await checked("precondition", () => step(legacy(), api, app));
        }
        for (const rule of spec.rules) {
          checkDeadline();
          await checked("rule", () =>
            rule(
              state,
              previous,
              freeze(structuredClone(pending)),
              legacy(),
              app,
            ),
          );
        }
        for (let i = 0; i < pending.length; i++) {
          const event = pending[i]!;
          if (definition.enrich && !legacyEnrichers.has(definition.enrich)) {
            // Old enrichers set fields on event.metadata and return nothing; keep both forms.
            const own = structuredClone(event.metadata);
            const enriched = definition.enrich(
              Object.freeze({
                ...envelope(definition, state.id, event, expected + i + 1),
                metadata: own,
              }),
              state,
              legacy(),
            );
            // `(event) => (event.metadata.x = value)` returns the value, not metadata fields.
            event.metadata = toJson(
              { ...own, ...(typeof enriched === "object" ? enriched : {}) },
              "Metadata",
            );
          }
          await metadata(event.metadata);
        }
        const outbox = await effects.seal(metadata);
        try {
          checkDeadline();
          const events = await withSpan("eventfabric.append", command, () =>
            store.appendToStream(command.stream, expected, pending, command, {
              outbox,
              signal: settings.signal,
              fingerprint,
              schedules: effects.schedules,
              scheduleGuard,
            }),
          );
          // The append committed: a failed lookup must not report the recorded Command as failed.
          const cancellations = effects.schedules.some(
            (change) => !change.entry,
          )
            ? await store.acceptedCommand(command.id).then(
                (accepted) => accepted?.cancellations,
                (error) => {
                  options.logger?.warn(
                    { error: messageOf(error), commandId: command.id },
                    "Command accepted; its Schedule cancellations could not be read",
                  );
                  return undefined;
                },
              )
            : undefined;
          lateness?.();
          return {
            status: "accepted",
            events,
            ...(cancellations ? { cancellations } : {}),
          };
        } catch (error) {
          if (error instanceof DuplicateCommandError) {
            const replay = await duplicate();
            if (replay) return replay;
          }
          if (!(error instanceof ConcurrencyError)) throw error;
          count("eventfabric.command.revision_conflicts", {
            aggregate: `${definition.context}.${definition.name}`,
          });
          if (attempt === retries)
            throw new Rejection(
              "concurrency",
              "Command conflicted after three retries",
            );
        }
      }
      throw Error("Invalid retry count");
    } catch (error) {
      if (
        canRecheckSchedule &&
        error instanceof Rejection &&
        incoming.schedule &&
        [
          "validation",
          "unknown-command",
          "precondition",
          "rule",
          "existence",
          "concurrency",
        ].includes(error.kind)
      ) {
        try {
          const current = await store.getSchedule(
            incoming.stream,
            incoming.schedule.name,
          );
          if (current?.commandId !== incoming.id)
            return handle(incoming, settings);
        } catch (lookupError) {
          return {
            status: "failed",
            error: {
              code: "schedule-lookup-failed",
              message: messageOf(lookupError),
            },
          };
        }
      }
      if (error instanceof Rejection) {
        lateness?.();
        return {
          status: "rejected",
          error: {
            code: error.code,
            kind: error.kind,
            message: error.message,
            details: error.details,
            ...(error.thrown ? { thrown: error.thrown } : {}),
          },
        };
      }
      options.logger?.error(
        {
          error: messageOf(error),
          command: incoming.name,
          stream: streamName(incoming.stream),
          commandId: incoming.id,
          correlationId: incoming.correlationId,
        },
        "Command failed",
      );
      return {
        status: "failed",
        error: { code: "command-failed", message: messageOf(error) },
      };
    }
  };
  return { handle, definitions: options.definitions };
};
/** `domain.<context>.<aggregate>(id).<command>(payload, metadata, settings)` sends through `send`; with a store, `.load()` folds the Stream. */
export const buildDomain = (
  definitions: Iterable<AggregateDefinition>,
  send: (input: TellInput) => any,
  store?: EventStore,
): any => {
  const domain: Record<string, any> = {};
  for (const definition of definitions) {
    domain[definition.context] ??= {};
    domain[definition.context][definition.name] = (id = "") => ({
      ...Object.fromEntries(
        Object.keys(definition.commands).map((name) => [
          name,
          (
            payload?: any,
            metadata?: Metadata,
            settings?: { notBefore?: string; replaceKey?: string },
          ) =>
            send({
              stream: {
                context: definition.context,
                aggregate: definition.name,
                id,
              },
              name,
              payload,
              metadata,
              ...settings,
            }),
        ]),
      ),
      ...(store && {
        load: async (options?: { atRevision?: number }) =>
          fromHistory(
            definition,
            id,
            await store.readStream(
              { context: definition.context, aggregate: definition.name, id },
              options,
            ),
            true,
          ),
      }),
    });
  }
  return domain;
};
