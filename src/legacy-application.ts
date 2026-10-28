import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { isDeepStrictEqual } from "node:util";
import { compile } from "./authoring/index.js";
import { AggregateState } from "./kernel/index.js";
import { legacyQueryIdentity } from "./compat.js";

/**
 * An old-framework Application's folders as EventFabric declarations: Aggregates (`app/domain/<context>/*.js`),
 * Readmodels (`app/readmodels/<group>/*.js`) and Sagas (`app/sagas/<group>/*.js`), with the old Event envelope,
 * view and Saga semantics kept, old `options` Aggregates and scheduled tasks run by an orchestration Worker, and
 * Saga Reactions with external effects run in Workers. What no convention tells is an option.
 */

const require = createRequire(import.meta.url);
type Fn = (...args: any[]) => any;
type Table = Record<string, string[] | "*">;

// ——— Orchestration: old `options` Aggregates and the external scheduler ———

export const LEGACY_ORCHESTRATION_MESSAGE = "orchestration.command";
export const LEGACY_SCHEDULER = "system.scheduler";
const blocked = (name: string) => () => {
  throw new Error(`${name} is disabled in the local migration`);
};
/** The old publisher's command string: `context.aggregate.name`, with or without a leading `domain.`. */
export const legacyCommandOf = (value: unknown): string[] => {
  const parts = String(value).split(".");
  if (parts.length < 3) throw new Error("Invalid command string.");
  if (parts.length === 4) parts.shift();
  return parts;
};
const instant = (value: unknown) => {
  const date = new Date((value as any) ?? Date.now());
  return isNaN(date as any) ? null : date.toISOString();
};
/**
 * The scheduler Saga: one instance per Stream and Command, so a second task replaces the first, whose Deadline sends
 * the Command with the old `{ automation: true }` metadata when it falls due.
 */
export const legacyScheduler = {
  reads: [],
  identity: {},
  reactions: {},
  messages: {
    schedule: {
      start: true,
      identity: ({ payload }: any) => `${legacyCommandOf(payload.commandToTrigger).join(".")}:${payload.id ?? ""}`,
      // The old scheduler fired a task with no valid date at once.
      handle: ({ payload }: any, saga: any) => saga.deadline("due", payload.date ?? new Date(), payload),
    },
  },
  deadlines: {
    due: ({ payload }: any, saga: any, app: any) => {
      const [context, aggregate, name] = legacyCommandOf(payload.commandToTrigger);
      app.tell({ stream: { context, aggregate, id: payload.id ?? "" }, name, payload: payload.payload || {}, metadata: { automation: true } });
      saga.destroy();
    },
  },
};

export type LegacyOrchestrationOptions = {
  /** `external.publisher()` calls that send work outside (address validators, say): refused in the migration. */
  publisherEffects?: string[];
  /** By `<context>.<aggregate>.<command>`, Command steps the Worker skips (checks that load another Aggregate). */
  skipped?: Record<string, (item: any) => boolean>;
  /** By Event, a check a Saga Message runs before its Reaction (in place of a skipped one). */
  checks?: Record<string, (event: any, view: any, app: any) => Promise<boolean> | boolean>;
  /** By Command, Commands whose Events reached no Saga: the Worker drops them. */
  silent?: Record<string, (command: any) => boolean>;
};

/**
 * Old Aggregates with an `options` block published their Events without storing them. Their Commands are accepted
 * with no Events and queue the orchestration Worker, which runs the old Command on an empty State; each Event it
 * applies goes as a Saga Message to the Sagas reacting to it, never into the log.
 */
export const legacyOrchestration = (options: LegacyOrchestrationOptions = {}) => {
  const { skipped = {}, checks = {}, silent = {} } = options;
  const validators = Object.fromEntries((options.publisherEffects ?? []).map((name) => [name, blocked(name)]));
  // `getServices().external.publisher()` as the old service gave it, with `scheduleTask` sending through `send` (a
  // Saga's, a Worker's or the Application's sendSagaMessage) and the rest from `base`.
  const publisher = (send: Fn, base: object = validators) =>
    Object.create(base, {
      scheduleTask: {
        value: async ({ id, account, accountName, date, commandToTrigger, payload, territory }: any) =>
          send(LEGACY_SCHEDULER, "schedule", { id, account, accountName, date: instant(date), commandToTrigger, payload, territory }),
        enumerable: true,
      },
    });
  const withPublisher = (services: any, send: Fn) =>
    Object.create(services ?? {}, {
      external: {
        value: { ...services?.external, publisher: async () => publisher(send, (await services?.external?.publisher?.()) ?? undefined) },
        enumerable: true,
      },
    });
  const authored = new Map<string, { commands: Record<string, unknown>; events: string[] }>(),
    targets = new Map<string, string[]>();
  /** An old `options` Aggregate: its Commands keep their schema and queue the Worker; its Event handlers stay. */
  const aggregate = (context: string, name: string, { options: _, commands = {}, ...history }: any) => {
    authored.set(`${context}.${name}`, { commands, events: Object.keys(history.events ?? {}) });
    return compile(context, name, {
      ...history,
      commands: Object.fromEntries(Object.entries(commands).map(([command, items]) => [command, [
        ...[items].flat(Infinity).filter((item: any) => item && Object.hasOwn(item, "schema")),
        ({ id, stream, payload, metadata, timestamp }: any, __: any, app: any) =>
          app.queue(LEGACY_ORCHESTRATION_MESSAGE, { id, stream, name: command, payload, metadata, timestamp }),
      ]])),
    } as any);
  };
  const wakeUp = (event: string) => authored.has(event.split(".").slice(0, 2).join("."));
  /**
   * A Saga's Reactions to these Events become Saga Messages of the same name: the old Reaction gets the old Event,
   * the instance comes from its Identity (default the Stream id), and one without only.ifExists() starts it.
   */
  const sagaMessages = (saga: string, identity: Record<string, any>, reactions: Record<string, any>) => {
    const messages: Record<string, unknown> = {};
    for (const [event, entry] of Object.entries(reactions)) {
      if (!wakeUp(event)) continue;
      const items = [entry].flat(Infinity),
        select = identity[event] && [identity[event]].flat(Infinity)[0];
      messages[event] = {
        start: !items.some((item: any) => item?.settings?.existing === true),
        identity: ({ payload }: any) => (select ? select(payload.event) : payload.event.aggregate.id),
        handle: async ({ payload }: any, view: any, app: any) => {
          const { event: published } = payload;
          if (checks[event] && !(await checks[event](published, view, app))) return;
          for (const item of items)
            if (typeof item?.settings?.shouldHandleEvent === "function" && !(await item.settings.shouldHandleEvent(published, view, app))) return;
          for (const item of items) if (typeof item === "function") await item(published, view, app);
        },
      };
      (targets.get(event) ?? targets.set(event, []).get(event)!).push(saga);
      delete reactions[event];
      delete identity[event];
    }
    return messages;
  };
  // The old Command API, for a Command run in the Worker: Commands go out as the Worker's, and nothing loads.
  const commandApp = (ctx: any) => {
    const handle = (context: string, aggregate: string) => (id = "") =>
      new Proxy({}, {
        get: (_, command) =>
          typeof command === "symbol" || command === "then"
            ? undefined
            : command === "load"
              ? () => { throw new Error(`An orchestration Command cannot load ${context}.${aggregate}`); }
              : (payload: unknown, metadata: unknown) => ctx.send({ stream: { context, aggregate, id }, name: command, payload: payload ?? {}, metadata: metadata ?? {} }),
      });
    const domain = new Proxy({}, {
      get: (_, context) =>
        typeof context === "symbol" || context === "then"
          ? undefined
          : new Proxy({}, { get: (__, aggregate) => (typeof aggregate === "symbol" ? undefined : handle(context, aggregate)) }),
    });
    const services = withPublisher(ctx.services, ctx.sendSagaMessage);
    return { getServices: () => services, getCrud: () => services.crud, getDomain: () => domain, tell: () => domain, getLogger: () => ctx.logger };
  };
  const worker = {
    handles: [LEGACY_ORCHESTRATION_MESSAGE],
    async execute({ payload: command }: any, ctx: any) {
      const { stream, name } = command,
        key = `${stream.context}.${stream.aggregate}`,
        full = `${key}.${name}`;
      if (silent[full]?.(command)) return;
      const { commands, events } = authored.get(key)!;
      const applied: { name: string; payload: unknown; metadata: unknown }[] = [],
        rules: Fn[] = [];
      const state = {
        id: stream.id, locals: {}, get: () => undefined,
        // The old bus added the Command id as causation and correlation; an absent payload stayed absent.
        apply: Object.fromEntries(events.map((event) => [event, (payload: unknown, extra = {}) =>
          void applied.push({ name: event, payload, metadata: { ...command.metadata, ...extra, causationId: command.id, correlationId: command.id } })])),
      };
      const old = { ...command, context: stream.context, aggregate: { id: stream.id, name: stream.aggregate } };
      const app = commandApp(ctx);
      try {
        for (const item of [commands[name]].flat(Infinity) as any[]) {
          if (skipped[full]?.(item)) continue;
          if (typeof item === "function") await item(old, state, app);
          else if (typeof item?.preCondition === "function") await item.preCondition(old, state, app);
          else if (typeof item?.businessRule === "function") rules.push(item.businessRule);
        }
        for (const rule of rules) await rule(state, { id: stream.id, locals: {}, get: () => undefined }, applied, old, app);
      } catch (error: any) {
        // The old domain rejected the Command: nothing was published, and nothing tried it again.
        ctx.logger.info({ command: full, error: error?.message }, "Orchestration Command rejected");
        return;
      }
      for (const [index, { name: event, payload, metadata }] of applied.entries()) {
        const fullname = `${key}.${event}`;
        // As the runtime gives a Reaction an Event; the id is stable across retries of this Message.
        const published = {
          id: `${command.id}:${index}`, name: event, context: { name: stream.context }, aggregate: { id: stream.id, name: stream.aggregate },
          stream, revision: index + 1, payload, metadata, timestamp: command.timestamp, commandId: command.id, correlationId: command.id,
        };
        for (const saga of targets.get(fullname) ?? []) await ctx.sendSagaMessage(saga, fullname, { event: published }, { id: `${saga}:${published.id}` });
      }
    },
  };
  return { aggregate, sagaMessages, worker, withPublisher, publisher, wakeUp, targets };
};

// ——— Effects: Saga Reactions with external effects run in Workers ———

export const LEGACY_EFFECTS_STATE = "effects.state";
export type LegacyEffectsOptions = {
  /** By Saga (`<group>.<file>`), the Events (or orchestration Saga Messages) whose Reaction runs in its Worker; `*` for all. */
  reactions?: Table;
  /** Provider services by name, with the calls a Saga may still make: reads that change nothing. */
  providers?: Record<string, string[]>;
  /** `external.publisher()` calls that change something: refused in a Saga. */
  publisherEffects?: string[];
};
const refused = (name: string) => () => {
  throw new Error(`${name} has an external effect: a Saga runs it in its Worker`);
};
// A client whose calls other than `reads` throw.
const guard = (client: any, name: string, reads: string[]) =>
  (client && typeof client === "object") || typeof client === "function"
    ? new Proxy(client, {
        get: (target, key) => {
          const value = Reflect.get(target, key);
          if (typeof key === "symbol" || key === "then" || typeof value !== "function") return value;
          return reads.includes(key) ? (...args: unknown[]) => Reflect.apply(value, target, args) : refused(`${name}.${String(key)}`);
        },
      })
    : client;
// Providers are factories (`mailer()`, `await files()`) or objects of them (`payment.capture()`).
const guardFactory = (factory: Fn, name: string, reads: string[]) => (...args: unknown[]) => {
  const client = factory(...args);
  return client instanceof Promise ? client.then((value) => guard(value, `${name}()`, reads)) : guard(client, `${name}()`, reads);
};
const guardProvider = (provider: any, name: string, reads: string[]) =>
  typeof provider === "function"
    ? guardFactory(provider, name, reads)
    : provider && typeof provider === "object"
      ? Object.fromEntries(Object.entries(provider).map(([key, value]) => [key, typeof value === "function" ? guardFactory(value as Fn, `${name}.${key}`, reads) : value]))
      : provider;
const json = (value: unknown) => JSON.parse(JSON.stringify(value ?? null));
// The old Saga app in a Worker: Commands are collected for the state message; services are the Worker's, with every provider.
const workerApp = (ctx: any, commands: unknown[]) => {
  const handle = (context: string, aggregate: string) => (id = "") =>
    new Proxy({}, {
      get: (_, command) =>
        typeof command === "symbol" || command === "then"
          ? undefined
          : (payload: unknown, metadata: unknown) => (commands.push({ stream: { context, aggregate, id }, name: command, payload: payload ?? {}, metadata: metadata ?? {} }), Promise.resolve()),
    });
  const domain = new Proxy({}, {
    get: (_, context) =>
      typeof context === "symbol" || context === "then"
        ? undefined
        : new Proxy({}, { get: (__, aggregate) => (typeof aggregate === "symbol" ? undefined : handle(context, aggregate)) }),
  });
  return {
    worker: true,
    logger: ctx.logger,
    getLogger: () => ctx.logger,
    getServices: () => ctx.services,
    getCrud: () => ctx.services.crud,
    getReadmodels: () => ctx.readmodels,
    getDomain: () => domain,
    tell: (input: unknown) => (input ? void commands.push(input) : domain),
    sendSagaMessage: ctx.sendSagaMessage,
    queue: refused("queue"),
  };
};

/**
 * EventFabric may run a Saga Reaction again (a retry, a halt and replay), so a Reaction that sends mail or posts a
 * webhook would repeat it. Each listed Reaction queues one Message with the Event and the instance's state, committed
 * with the Saga's Checkpoint; its Saga's Worker runs the old Reaction and sends the state and the Reaction's Commands
 * back as one Saga Message, which the instance commits together, as the old engine committed a Saga's state and then
 * sent its Commands. A Saga's Worker handles one Message at a time and tries it once, as the old engine logged a
 * failure and moved on; a failure is a dead letter, replayable. Elsewhere in a Saga, provider calls that change
 * something are refused; reads pass.
 */
export const legacyEffects = (options: LegacyEffectsOptions = {}) => {
  const table = options.reactions ?? {}, providers = options.providers ?? {};
  const messageOf = (saga: string) => `effects.${saga}`;
  const runsInWorker = (saga: string, event: string) => table[saga] === "*" || table[saga]?.includes(event);
  const sagaServices = (services: any) =>
    new Proxy(services, {
      get: (target, key) =>
        typeof key === "string" && Object.hasOwn(providers, key) && target[key] !== undefined
          ? guardProvider(target[key], key, providers[key]!)
          : target[key],
    });
  const sagaPublisher = (publisher: any) =>
    Object.create(publisher, Object.fromEntries((options.publisherEffects ?? []).map((name) => [name, { value: refused(`external.publisher().${name}`) }])));
  const steps = new Map<string, Fn[]>();
  /** Replaces the listed Reactions' functions with one that queues the Saga's Worker; markers stay in the Saga. */
  const reactions = (saga: string, map: Record<string, any>) => {
    let moved = false;
    for (const [event, items] of Object.entries(map)) {
      if (!runsInWorker(saga, event)) continue;
      const list = [items].flat(Infinity),
        fns = list.filter((item) => typeof item === "function");
      if (!fns.length) continue;
      steps.set(`${saga}:${event}`, fns);
      map[event] = [
        ...list.filter((item) => typeof item !== "function"),
        (published: unknown, view: any, app: any) => app.queue(messageOf(saga), { saga, event, instance: view.id, state: json(view.get()), published: json(published) }),
      ];
      moved = true;
    }
    return moved;
  };
  // The state the Worker's run left and its Commands, committed together by the instance.
  // shortcut: a start handler, so a state message for an instance another Event destroyed meanwhile recreates it with
  // the Worker's changes; keys a Reaction removed stay. Upgrade when a moved Reaction destroys or removes keys of
  // instances other Events also reach.
  const stateMessage = {
    start: true,
    handle: ({ payload: { set = {}, commands = [], destroy } }: any, view: any, app: any) => {
      for (const [key, value] of Object.entries(set)) view.set(key, value);
      for (const command of commands) app.tell(command);
      if (destroy) view.destroy();
    },
  };
  const execute = async ({ payload, idempotencyKey }: any, ctx: any) => {
    const { saga, event, instance, state, published } = payload;
    const draft = new AggregateState(instance, state ?? {}),
      commands: unknown[] = [];
    let destroyed = false;
    const view = { id: instance, get: (at?: string) => draft.get(at as any), set: (at: any, value: unknown) => draft.set(at, value), destroy: () => { destroyed = true; } };
    const app = workerApp(ctx, commands);
    for (const fn of steps.get(`${saga}:${event}`) ?? []) await fn(published, view, app);
    // get() adds the instance id as `id`; it goes back only where a Reaction changed it.
    const set = Object.fromEntries(Object.entries(draft.attributes).filter(([key, value]) => !(key === "id" && value === instance) && !isDeepStrictEqual(value, state?.[key])));
    if (destroyed || commands.length || Object.keys(set).length)
      await ctx.sendSagaMessage(saga, LEGACY_EFFECTS_STATE, json({ set, commands, ...(destroyed ? { destroy: true } : {}) }), { to: instance, id: `${idempotencyKey}:state` });
  };
  const workers = () =>
    Object.fromEntries(Object.keys(table).map((saga) => [`effects:${saga}`, {
      handles: [messageOf(saga)], concurrency: 1, retry: { attempts: 1 }, timeout: 300000, execute,
    }]));
  return { reactions, stateMessage, execute, workers, messageOf, runsInWorker, sagaServices, sagaPublisher, STATE: LEGACY_EFFECTS_STATE };
};

// ——— Declarations: the old folders, envelope, view and Sagas ———

export type LegacyApplicationOptions = {
  /** The old Application's folder. */
  root: string;
  /** The Readmodel and Saga groups its configuration loads (`denormalizers`, `sagas`). */
  readmodelGroups: string[];
  sagaGroups: string[];
  /** By Saga group, the Readmodel group whose Events its Sagas received (the old binding). */
  sagaBindings?: Record<string, string | undefined>;
  /** A Readmodel's reviewed schema and indexes, in place of its `repositorySettings` and old API schema. */
  readmodelStorage: (group: string, name: string, repositorySettings: unknown) => { schema: object; indexes: unknown[] };
  /** By Readmodel group, the other groups its Reactions read. */
  readmodelReads?: Record<string, string[]>;
  /** By `<group>/<model>`, Reaction keys the old runtime never matched (built from constants that do not exist). */
  unmatchedReactions?: Record<string, string[]>;
  /** By Saga group, the Readmodel groups its Sagas read, and the groups every Saga reads through shared helpers. */
  sagaReads?: Record<string, string[]>;
  sharedSagaReads?: string[];
  /** Event names in the imported log that no Aggregate declares; their handlers throw until recovered. */
  historicalEvents?: string[];
  /** Contexts whose `options` Aggregates keep only their Event handlers, with Commands off. */
  disabledContexts?: string[];
  /** Restores the old envelope's fields the import moved (copies of the payload and metadata; change them in place). */
  restoreEvent?: (event: any, legacy: { payload: any; metadata: any }) => void;
  orchestration?: LegacyOrchestrationOptions;
  effects?: LegacyEffectsOptions;
};

const copyJson = (value: any): any => {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(copyJson);
  if (Object.getPrototypeOf(value) !== Object.prototype) return structuredClone(value);
  const copy: any = {};
  for (const key of Object.keys(value))
    if (key === "__proto__") Object.defineProperty(copy, key, { value: copyJson(value[key]), enumerable: true, writable: true, configurable: true });
    else copy[key] = copyJson(value[key]);
  return copy;
};
// The old view took array paths, ignored `undefined` in set(path, value) and merged an object into a stored object.
const keyPath = (at: any) => (Array.isArray(at) ? at.join(".") : at);
// The old store wrote an Invalid Date as the epoch: the driver serialized its NaN time as 0.
const epoch = (value: any): any => {
  if (value instanceof Date) return isNaN(value as any) ? new Date(0) : value;
  if (value && typeof value === "object")
    for (const [key, child] of Object.entries(value)) {
      const kept = epoch(child);
      if (kept !== child) value[key] = kept;
    }
  return value;
};
const mergeable = (value: unknown) => value !== null && typeof value === "object" && !Array.isArray(value) && !(value instanceof Date);
const legacyView = (view: any) =>
  Object.create(view, {
    attributes: { get: () => view.get() },
    get: { value: (...args: any[]) => view.get(...args.map(keyPath)) },
    set: {
      value: (at: any, value: unknown) => {
        const path = keyPath(at);
        if (typeof path !== "string") return view.set(epoch(path));
        if (value === undefined) return;
        const current = view.get(path);
        view.set(path, epoch(mergeable(current) && mergeable(value) ? Object.assign(current, value) : value));
      },
    },
  });
// The old Saga model's get() without a path was undefined.
const legacySagaView = (view: any) => Object.create(view, { get: { value: (at: any) => (at === undefined ? undefined : view.get(keyPath(at))) } });
// The old ViewModel stored each document's id in `id`; get('id') falls back to the document id, so set it on every Reaction.
const keepId = (_: unknown, view: any) => view.set("id", view.id);
const rejection = (key: string) => /\.commandRejected$/.test(key.replace(/^domain\./, ""));

/** The old declarations of an Application's folders as EventFabric's Aggregates, Readmodels, Sagas and Workers. */
export const legacyApplication = (options: LegacyApplicationOptions) => {
  const { root, readmodelReads = {}, unmatchedReactions = {}, sagaReads = {}, sharedSagaReads = [], sagaBindings = {} } = options;
  const folders = (directory: string) =>
    fs.readdirSync(path.join(root, directory), { withFileTypes: true }).filter((entry) => entry.isDirectory()).map((entry) => entry.name).sort();
  const definitions = (directory: string): [string, any][] =>
    fs.readdirSync(path.join(root, directory), { withFileTypes: true })
      .filter((entry) => entry.isFile() && entry.name.endsWith(".js"))
      .sort((a, b) => a.name.localeCompare(b.name))
      .map((entry) => [entry.name.split(".")[0]!, require(path.join(root, directory, entry.name))]);
  const orchestration = legacyOrchestration({ publisherEffects: options.effects?.publisherEffects, ...options.orchestration });
  const effects = legacyEffects(options.effects);
  const deferred: { context: string; name: string; commands: string[]; reason: string }[] = [];
  const aggregates = folders("app/domain").flatMap((context) =>
    definitions(`app/domain/${context}`).map(([name, authored]) => {
      if (!authored.options) return compile(context, name, authored);
      const { options: _, commands, idGenerator: __, ...history } = authored;
      const disabled = options.disabledContexts?.includes(context) ?? false;
      deferred.push({ context, name, commands: Object.keys(commands || {}), reason: disabled ? `disabled ${context}` : "runs in the orchestration Worker" });
      return disabled ? compile(context, name, history) : orchestration.aggregate(context, name, authored);
    }),
  );
  for (const name of options.historicalEvents ?? []) {
    const [context, aggregate, event] = name.split(".") as [string, string, string];
    let definition = aggregates.find((entry) => entry.context === context && entry.name === aggregate);
    if (!definition) aggregates.push((definition = compile(context, aggregate, {} as any)));
    (definition.events as any)[event] = () => {
      throw new Error("Historical Event handler is unavailable; recover it before Aggregate replay");
    };
  }
  // Readmodel and Saga code reads the old Event envelope: Context as a string, `fullname`, the Stream Revision on
  // `aggregate`, and the commit time (a Date), `position` and `correlationId` in metadata. Old Reactions also wrote
  // into the payload, so they get a copy.
  const legacyEvent = (event: any) => {
    const legacy = { payload: copyJson(event.payload), metadata: copyJson(event.metadata) };
    options.restoreEvent?.(event, legacy);
    return {
      ...event, context: event.context.name, fullname: `domain.${event.context.name}.${event.aggregate.name}.${event.name}`,
      aggregate: { ...event.aggregate, revision: event.revision }, payload: legacy.payload,
      metadata: { ...legacy.metadata, timestamp: new Date(event.timestamp), position: event.position && Number(event.position.sequence), correlationId: event.commandId },
    };
  };
  // The old runtime handed every action of a group the same Event object, so one copy per Event and group is shared,
  // as are the writes Reactions make into it. A failure drops it, so a retry starts from the recorded Event.
  // Keyed by the read Event's payload: a group's Partitions fold different Events at once, and a batch that runs
  // again reads its Events again, so it never sees the writes of its failed attempt.
  const shared = new Map<string, WeakMap<object, any>>();
  const legacyEventOf = (group: string, event: any) => {
    if (!event.payload || typeof event.payload !== "object") return legacyEvent(event);
    let read = shared.get(group);
    if (!read) shared.set(group, (read = new WeakMap()));
    let copy = read.get(event.payload);
    if (!copy) read.set(event.payload, (copy = legacyEvent(event)));
    return copy;
  };
  // Diagnostics name the Event by id, never by its data.
  const located = (error: any, event: any, group: string) => {
    if (event?.payload && typeof event.payload === "object") shared.get(group)?.delete(event.payload);
    if (error && typeof error === "object" && !error.eventId) error.eventId = event?.id;
    return error;
  };
  const guarded = (group: string, event: any, run: () => any) => {
    try {
      const result = run();
      return result?.catch ? result.catch((error: unknown) => { throw located(error, event, group); }) : result;
    } catch (error) {
      throw located(error, event, group);
    }
  };
  const legacy = (fn: Fn, group: string): Fn => (event: any, view: any, ...rest: any[]) =>
    guarded(group, event, () => fn(legacyEventOf(group, event), view && legacyView(view), ...rest));
  const items = (value: unknown, wrapped: (fn: Fn) => Fn) =>
    [value].flat(Infinity).map((item: any) =>
      typeof item === "function" ? wrapped(item)
        : item?.settings?.shouldHandleEvent ? { settings: { shouldHandleEvent: wrapped(item.settings.shouldHandleEvent) } } : item);
  const legacyItems = (value: unknown, group: string) => items(value, (fn) => legacy(fn, group));
  // The old builder took Event names with or without the `domain.` prefix that Event constants carry.
  const named = (owner: string, entries: Record<string, any>) => {
    const renamed = Object.fromEntries(Object.entries(entries).map(([key, value]) => [key.replace(/^domain\./, ""), value]));
    assert.equal(Object.keys(renamed).length, Object.keys(entries).length, `${owner}: an Event is named with and without domain.`);
    return renamed;
  };
  /**
   * The old declaration as EventFabric's, as migrateSource renders inline ones: query entries become Identities,
   * grouped `<event>:<group>` keys with their own Identity or markers become Reaction sets and the others merge into
   * their Event's Reaction; the reviewed schema and indexes replace repositorySettings and the old API schema.
   */
  const readmodel = (group: string, name: string, { repositorySettings, schema: _, query: authoredQuery = {}, identity: authoredIdentity = {}, reactions: authoredReactions, ...rest }: any) => {
    const owner = `${group}/${name}`;
    const live = ([key]: [string, unknown]) => !unmatchedReactions[owner]?.includes(key);
    const identity = named(owner, Object.fromEntries(Object.entries(authoredIdentity).filter(live))),
      reactions = named(owner, Object.fromEntries(Object.entries(authoredReactions).filter(live)));
    const query = named(owner, authoredQuery);
    assert.deepEqual(Object.keys(rest).filter((key) => key !== "initialState"), [], `${owner}: unreviewed Readmodel key`);
    const selectors: Record<string, any> = { ...identity };
    for (const [key, entry] of Object.entries(query)) {
      assert.ok(!(key in identity), `${owner}: ${key} has both an Identity and a query entry`);
      selectors[key] = legacyQueryIdentity(entry);
    }
    const own = { identity: {} as Record<string, any>, reactions: {} as Record<string, any[]> },
      reactionSets: Record<string, typeof own> = {},
      slots = new Map<string, string>();
    for (const [key, reaction] of Object.entries(reactions)) {
      const [, base, set] = /^([^:\s]+\.[^:\s]+):([^:\s]+)$/.exec(key) ?? [];
      const target = !base ? own
        : key in selectors || [reaction].flat(Infinity).some((item) => typeof item !== "function") ? (reactionSets[set!] ??= { identity: {}, reactions: {} })
          : own;
      const event = base ?? key,
        selector = selectors[key] ?? selectors[event];
      assert.ok(!base || key in selectors || event in identity || !(event in query), `${owner}: ${key} selects by its Event's query entry`);
      // The old builder merged an entry's settings wherever they stood: only.ifNotExists() set autoCreate: true, the
      // default, and only.ifExists() cleared it; only.if(predicate) applied only to one-argument predicates.
      const kept = legacyItems(reaction, group).filter((item: any) => item?.settings?.existing !== false);
      const predicates = [reaction].flat(Infinity).filter((item: any) => item?.settings?.shouldHandleEvent).map((item: any) => item.settings.shouldHandleEvent);
      assert.ok(predicates.every((predicate: Fn) => predicate.length === 1), `${owner}: ${key} has a predicate the old runtime ignored`);
      // Each old entry kept its own settings, so entries share a Reaction only where those agree and none has a predicate.
      const slot = `${target === own ? "" : set}:${event}`,
        settings = `${kept.some((item: any) => item?.settings?.existing === true)}:${predicates.length > 0}`;
      assert.ok(!slots.has(slot) || (slots.get(slot) === settings && !predicates.length), `${owner}: ${key} merges entries with different settings`);
      slots.set(slot, settings);
      if (predicates.length) {
        // The old runtime asked shouldHandleEvent with the Event alone, before its query, and skipped the entry.
        const legacyPredicates = predicates.map((predicate: Fn) => legacy(predicate, group));
        const select = selector ? legacyItems(selector, group)[0] : (event: any) => event.aggregate.id;
        target.identity[event] = [async (event: any, ...rest: any[]) => {
          for (const predicate of legacyPredicates) if (!(await predicate(event))) return null;
          return select(event, ...rest);
        }];
      } else if (selector) target.identity[event] = legacyItems(selector, group);
      target.reactions[event] = [...(target.reactions[event] ?? [keepId]), ...kept.filter((item: any) => !item?.settings?.shouldHandleEvent)];
    }
    const { schema, indexes } = options.readmodelStorage(group, name, repositorySettings);
    return {
      ...rest, ...own, ...(Object.keys(reactionSets).length ? { reactionSets } : {}),
      // The old stack stored keys a Readmodel does not declare (Command payloads copied into documents); a Command
      // with such a key would halt the group on EventFabric's closed documents, so they are dropped with a warning.
      schema: { ...schema, "x-undeclared": "drop" },
      indexes,
    };
  };
  const authoredReadmodels = Object.fromEntries(folders("app/readmodels").filter((group) => options.readmodelGroups.includes(group))
    .map((group) => [group, { models: Object.fromEntries(definitions(`app/readmodels/${group}`)) }]));
  const readmodels = Object.fromEntries(Object.entries(authoredReadmodels).map(([group, { models }]) => [group, {
    ...(readmodelReads[group] ? { reads: readmodelReads[group] } : {}),
    models: Object.fromEntries(Object.entries(models).map(([name, authored]) => [name, readmodel(group, name, authored)])),
  }]));
  // `getServices().external.publisher()` schedules through the scheduler Saga, committed with the Reaction. In a
  // Saga, provider calls that change something are refused: those Reactions run in their Saga's Worker.
  const sagaApp = (app: any) => {
    const services = orchestration.withPublisher(app.getServices(), app.sendSagaMessage);
    if (app.worker) return services;
    const publisher = services.external.publisher;
    return effects.sagaServices(Object.create(services, {
      external: { value: { ...services.external, publisher: async () => effects.sagaPublisher(await publisher()) }, enumerable: true },
    }));
  };
  // Saga Reactions also kept the Application's getLogger().
  const legacySagaApp = (app: any) => app && Object.create(app, { getLogger: { value: () => app.logger }, getServices: { value: () => sagaApp(app) } });
  // The old bus published a `commandRejected` pseudo-Event for every rejected Command, never stored; EventFabric
  // reports a rejection as the Command's outcome, so Reactions to it are dropped and listed.
  const rejections: string[] = [];
  const saga = (group: string, name: string, { identity: authoredIdentity = {}, reactions: authoredReactions, ...rest }: any) => {
    assert.deepEqual(Object.keys(rest).filter((key) => key !== "initialState"), [], `${group}/${name}: unreviewed Saga key`);
    const kept = (map: Record<string, unknown>) => Object.fromEntries(Object.entries(map).filter(([event]) => !rejection(event)));
    rejections.push(...Object.keys(authoredReactions).filter(rejection).map((event) => `${group}.${name}: ${event.replace(/^domain\./, "")}`));
    const key = `saga:${group}.${name}`,
      bound = sagaBindings[group],
      binding = bound && options.readmodelGroups.includes(bound) ? [bound] : [];
    const legacySaga = (fn: Fn): Fn => (event: any, view: any, app: any, ...rest: any[]) =>
      guarded(key, event, () => fn(legacyEventOf(key, event), view && legacySagaView(view), legacySagaApp(app), ...rest));
    const sagaItems = (value: unknown) => items(value, legacySaga);
    const wrapped = {
      identity: Object.fromEntries(Object.entries(named(key, kept(authoredIdentity))).map(([event, entry]) => [event, sagaItems(entry)])),
      reactions: Object.fromEntries(Object.entries(named(key, kept(authoredReactions))).map(([event, entry]) => [event, sagaItems(entry)])),
    };
    const moved = effects.reactions(`${group}.${name}`, wrapped.reactions);
    const messages = { ...orchestration.sagaMessages(`${group}.${name}`, wrapped.identity, wrapped.reactions), ...(moved ? { [LEGACY_EFFECTS_STATE]: effects.stateMessage } : {}) };
    return {
      reads: [...new Set([...binding, ...(sagaReads[group] ?? []), ...sharedSagaReads])].sort(),
      ...wrapped,
      ...(Object.keys(messages).length ? { messages } : {}),
    };
  };
  // The old store kept one state collection per Saga file, so each file is a Saga named `<group>.<file>`.
  const sagas = Object.fromEntries(folders("app/sagas").filter((group) => options.sagaGroups.includes(group))
    .flatMap((group) => definitions(`app/sagas/${group}`).map(([name, authored]) => [`${group}.${name}`, saga(group, name, authored)])));
  return {
    aggregates, readmodels, sagas,
    // The scheduler Saga and the orchestration Worker.
    orchestrationSagas: { [LEGACY_SCHEDULER]: legacyScheduler },
    workers: { orchestration: orchestration.worker, ...effects.workers() },
    orchestration, effects, rejections, authoredReadmodels, readmodel, deferred, legacyEvent,
    historical: options.historicalEvents ?? [],
  };
};
