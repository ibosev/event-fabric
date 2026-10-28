import type { TSchema } from "typebox";
import type { CommandInput } from "../id.js";
import type {
  AggregateDefinition,
  AggregateState,
  LegacyCommand,
} from "../kernel/index.js";
import type { ConsumerStart, JsonObject, Metadata } from "../types.js";
import type {
  ApplicationConfig,
  CommandApp,
  Identity,
  ReactionApp,
  ReadmodelApp,
  ReactionEvent,
  ReactionView,
  ReadmodelDefinition,
  ResolvedApplicationConfig,
  SagaDefinition,
  SagaMessageView,
  SagaView,
} from "../runtime/types.js";
import type { ReadmodelIndex } from "../readmodel-store/types.js";
import { assertPlainSchema } from "../validation.js";
import {
  aggregate,
  type AggregateInfo,
  type Document,
  type Static,
} from "./aggregate.js";
import type {
  AggregateCommand,
  AggregateEvent,
  AggregateFoldState,
  AggregateView,
  AuthoringMarker,
  CommandRule,
} from "./index.js";

declare const brand: unique symbol;
type Upcasters = Record<number, (payload: any) => any | Promise<any>>;
type SendOptions = Omit<CommandInput, "stream" | "name" | "payload">;

/** A Command type made with `command()`; its instances are the Command a `@CommandHandler` receives. */
export type CommandType<N extends string = string, P = any> = {
  readonly kind: "command";
  readonly type: N;
  readonly schema: TSchema;
  /** Builds the Command to send: `app.sendCommand(OpenAccount.to("a-1", { owner }))`. */
  to(id: string | undefined, payload: P, options?: SendOptions): CommandInput;
  new (): AggregateCommand<P> & { readonly [brand]?: N };
};
/** An Event type made with `event()` or `eventOf()`; its instances are the Event a handler receives. */
export type EventType<N extends string = string, P = any> = {
  readonly kind: "event";
  readonly type: N;
  readonly schema: TSchema;
  readonly schemaVersion: number;
  readonly upcasters: Upcasters;
  new (): AggregateEvent<P> & { readonly [brand]?: N };
};
/** A Saga Message type made with `sagaMessage()`; its instances are what a `@SagaMessageHandler` receives. */
export type SagaMessageType<N extends string = string, P = any> = {
  readonly kind: "sagaMessage";
  readonly type: N;
  readonly schema: TSchema;
  new (): SagaMessageView<P> & { readonly [brand]?: N };
};
type Qualified = `${string}.${string}.${string}`;
const split = (type: string, what: string) => {
  const parts = type.split(".");
  if (parts.length !== 3 || parts.some((part) => !part))
    throw Error(`${what} ${type}: name it <context>.<aggregate>.<name>`);
  return parts as [string, string, string];
};

/**
 * Declares a Command type, as Axon's Command classes do: `class OpenAccount extends
 * command("bank.account.open", Opened) {}`. The class carries the qualified name and the
 * payload schema, types `@CommandHandler(OpenAccount)` methods and builds Commands with `to`.
 */
export const command = <const N extends Qualified, S extends TSchema>(
  type: N,
  schema: S,
): CommandType<N, Static<S>> => {
  const [context, aggregateName, name] = split(type, "Command");
  assertPlainSchema(schema, type);
  return class {
    static readonly kind = "command";
    static readonly type = type;
    static readonly schema = schema;
    static to(
      id: string | undefined,
      payload: unknown,
      options: SendOptions = {},
    ): CommandInput {
      return {
        ...options,
        stream: { context, aggregate: aggregateName, id },
        name,
        payload,
      };
    }
  } as unknown as CommandType<N, Static<S>>;
};
const eventType = (
  type: string,
  schema: TSchema,
  schemaVersion: number,
  upcasters: Upcasters,
) =>
  class {
    static readonly kind = "event";
    static readonly type = type;
    static readonly schema = schema;
    static readonly schemaVersion = schemaVersion;
    static readonly upcasters = upcasters;
  };
/**
 * Declares an Event type: `class AccountOpened extends event("bank.account.opened", Opened) {}`.
 * `schemaVersion` and `upcasters` read older stored payloads, as in `aggregate()`.
 */
export const event = <const N extends Qualified, S extends TSchema>(
  type: N,
  schema: S,
  options: { schemaVersion?: number; upcasters?: Upcasters } = {},
): EventType<N, Static<S>> => {
  split(type, "Event");
  assertPlainSchema(schema, type);
  return eventType(
    type,
    schema,
    options.schemaVersion ?? 1,
    options.upcasters ?? {},
  ) as unknown as EventType<N, Static<S>>;
};
/**
 * Declares a Saga Message type, a System Event outside the Event log:
 * `class DoneRequested extends sagaMessage("doneRequested", Done) {}`. Its name is the one
 * senders use with `sendSagaMessage(saga, name, payload)`; the schema checks it when sent.
 */
export const sagaMessage = <const N extends string, S extends TSchema>(
  type: N,
  schema: S,
): SagaMessageType<N, Static<S>> => {
  if (!type) throw Error("A Saga Message type needs a name");
  assertPlainSchema(schema, type);
  return class {
    static readonly kind = "sagaMessage";
    static readonly type = type;
    static readonly schema = schema;
  } as unknown as SagaMessageType<N, Static<S>>;
};
/**
 * The Event type of an Aggregate declared with `aggregate()`, so decorated Readmodels and
 * Sagas react to it: `class InvoiceSettled extends eventOf(invoice, "settled") {}`.
 */
type EventNames<T> =
  AggregateInfo<T> extends { events: infer E }
    ? Extract<keyof E, string>
    : never;
type EventOf<T, N extends string> =
  AggregateInfo<T> extends {
    context: infer C extends string;
    name: infer A extends string;
    events: infer E;
  }
    ? EventType<`${C}.${A}.${N}`, E[N & keyof E]>
    : never;
export const eventOf = <
  T extends AggregateDefinition,
  const N extends EventNames<T>,
>(
  definition: T,
  name: N,
): EventOf<T, N> => {
  const type = `${definition.context}.${definition.name}.${name}`;
  if (!definition.events[name])
    throw Error(
      `${definition.context}.${definition.name} declares no Event ${name}`,
    );
  const stored = definition.eventSchemas?.[type];
  return eventType(
    type,
    stored?.schema ?? {},
    stored?.version ?? 1,
    stored?.upcasters ?? {},
  ) as any;
};

type PayloadOf<T> = T extends abstract new () => { payload: infer P }
  ? P
  : never;
/**
 * Event payloads keyed by qualified name for a union of Event types, for object Reactions
 * that mix with decorated ones: `Reactions<EventsOf<typeof domain> & EventTypes<typeof A | typeof B>>`.
 */
export type EventTypes<T extends EventType> = {
  [N in T["type"]]: PayloadOf<Extract<T, { type: N }>>;
};

/** What a `@CommandHandler` decides on: the Aggregate's State and `apply(EventType, payload)`. */
export type AggregateOf<S extends TSchema> = Omit<
  AggregateView<any, Static<S>>,
  "apply"
> & {
  /** Records an Event of this Aggregate, as Axon's `apply(new AccountOpened(...))`. */
  apply<E extends EventType>(
    event: E,
    payload: PayloadOf<E>,
    metadata?: Metadata,
  ): void;
};
/** The State an `@EventSourcingHandler` folds into. */
export type StateOf<S extends TSchema> = AggregateFoldState<Static<S>>;
/** The document an `@EventHandler` updates in a `@Readmodel`. */
export type DocumentOf<S extends TSchema> = ReactionView<Document<S>>;
/** The state of one Saga instance a `@SagaEventHandler` updates. */
export type SagaOf<S extends TSchema> = SagaView<Static<S>>;

type Role =
  | "CommandHandler"
  | "EventSourcingHandler"
  | "EventHandler"
  | "SagaEventHandler"
  | "SagaMessageHandler"
  | "SagaDeadlineHandler";
type Token =
  | CommandType
  | EventType
  | SagaMessageType
  | { kind: "deadline"; type: string };
type Mark = {
  role: Role;
  token: Token;
  options: any;
  key: string;
};
// Keyed by the method function itself: no global registry, and no class is ever constructed.
const marks = new WeakMap<Function, Mark[]>();
// The definition rides on the class under a registered symbol, so a second copy of this module
// (a bundler, a test runner, two installs of the package) still finds it.
const component = Symbol.for("event-fabric.component");
type Component = { kind: string; definition: unknown };

const expectedKinds: Partial<Record<Role, [Token["kind"], string]>> = {
  CommandHandler: ["command", "a command()"],
  SagaMessageHandler: ["sagaMessage", "a sagaMessage()"],
  SagaDeadlineHandler: ["deadline", "a Deadline name"],
};
const mark =
  (role: Role, token: Token, options: unknown) =>
  (method: Function, context: ClassMethodDecoratorContext) => {
    if (context.kind !== "method" || context.static || context.private)
      throw Error(
        `@${role}(${token?.type}) on ${String(context.name)}: only public instance methods can be handlers`,
      );
    const [expected, what] = expectedKinds[role] ?? ["event", "an event()"];
    if (token?.kind !== expected || !token.type)
      throw Error(`@${role} on ${String(context.name)}: expected ${what} type`);
    marks.set(method, [
      ...(marks.get(method) ?? []),
      { role, token, options, key: String(context.name) },
    ]);
  };

/** Selects the method that decides a Command; `before` holds `only.*` checks and earlier Steps, `rules` run after. */
export const CommandHandler =
  <T extends CommandType>(
    command: T,
    options: {
      before?: (
        | AuthoringMarker
        | ((command: InstanceType<T>, aggregate: any, app: CommandApp) => unknown)
      )[];
      rules?: CommandRule<any, PayloadOf<T>>[];
    } = {},
  ) =>
  <F extends (command: InstanceType<T>, aggregate: any, app: CommandApp) => unknown>(
    method: F,
    context: ClassMethodDecoratorContext,
  ) =>
    mark("CommandHandler", command, options)(method, context);
/** Selects the method that folds an Event into the State. It runs on every load, so it must be deterministic. */
export const EventSourcingHandler =
  <T extends EventType>(event: T) =>
  <F extends (event: InstanceType<T>, state: any) => void>(
    method: F,
    context: ClassMethodDecoratorContext,
  ) =>
    mark("EventSourcingHandler", event, {})(method, context);
type ReactionOptions<T extends EventType> = {
  /**
   * Which document or Saga instance the Event concerns: a payload key, or a function returning
   * an id, ids, a filter or `null` to skip. Default: the Event's Stream id.
   */
  associationProperty?:
    | Extract<keyof PayloadOf<T>, string>
    | ((event: ReactionEvent<PayloadOf<T>>) => Identity | Promise<Identity>);
  /** `only.ifExists()`/`only.ifNotExists()` before the method. */
  before?: AuthoringMarker[];
};
type Reaction<T extends EventType, App> = (
  event: InstanceType<T> & ReactionEvent<PayloadOf<T>>,
  view: any,
  app: App,
) => unknown;
/** Selects a `@Readmodel` method that updates a document from an Event. It must be deterministic. */
export const EventHandler =
  <T extends EventType>(event: T, options: ReactionOptions<T> = {}) =>
  <F extends Reaction<T, ReadmodelApp>>(
    method: F,
    context: ClassMethodDecoratorContext,
  ) => mark("EventHandler", event, options)(method, context);
/** Selects a `@Saga` method that reacts to an Event: it may `tell` Commands and `queue` Messages through the Outbox. */
export const SagaEventHandler =
  <T extends EventType>(event: T, options: ReactionOptions<T> = {}) =>
  <F extends Reaction<T, ReactionApp>>(
    method: F,
    context: ClassMethodDecoratorContext,
  ) => mark("SagaEventHandler", event, options)(method, context);

/**
 * Selects a `@Saga` method that handles a Saga Message others send to an instance. `start`
 * creates a missing instance; `associationProperty` picks the instance when a send names none.
 */
export const SagaMessageHandler =
  <T extends SagaMessageType>(
    message: T,
    options: {
      start?: boolean;
      associationProperty?:
        | Extract<keyof InstanceType<T>["payload"], string>
        | ((message: { id: string; payload: InstanceType<T>["payload"] }) => string);
    } = {},
  ) =>
  <F extends (message: InstanceType<T>, saga: any, app: ReactionApp) => unknown>(
    method: F,
    context: ClassMethodDecoratorContext,
  ) =>
    mark("SagaMessageHandler", message, options)(method, context);
/** Selects a `@Saga` method that handles the instance's Deadline `name`, set with `saga.deadline(name, at)`. */
export const SagaDeadlineHandler =
  (name: string) =>
  <F extends (message: SagaMessageView, saga: any, app: ReactionApp) => unknown>(
    method: F,
    context: ClassMethodDecoratorContext,
  ) =>
    mark("SagaDeadlineHandler", { kind: "deadline", type: name }, {})(method, context);

/**
 * The restricted class model: no superclass, no static members, and every prototype member a
 * decorated handler of an allowed role. Handlers run unbound (`this` is undefined), so instance
 * state cannot creep in; the `decorated-class` lint rule reports fields and constructors.
 */
const collect = (cls: Function, decorator: string, roles: Role[]) => {
  const where = `@${decorator} ${cls.name || "class"}`;
  if (Object.getPrototypeOf(cls) !== Function.prototype)
    throw Error(
      `${where}: extends another class; inheritance is not supported`,
    );
  // TypeScript adds `Symbol.metadata` to decorated classes where the runtime defines it.
  const statics = Reflect.ownKeys(cls).filter(
    (key) =>
      !["length", "name", "prototype"].includes(key as string) &&
      key !== (Symbol as any).metadata &&
      key !== component,
  );
  if (statics.length)
    throw Error(
      `${where}: static members are not supported (${statics.map(String).join(", ")})`,
    );
  const found: (Mark & { method: Function })[] = [];
  const descriptors = Object.getOwnPropertyDescriptors(cls.prototype);
  for (const key of Reflect.ownKeys(descriptors)) {
    if (key === "constructor") continue;
    const { value } = descriptors[key as string]!;
    const own = typeof value === "function" ? marks.get(value) : undefined;
    if (!own?.length)
      throw Error(`${where}: ${String(key)} is not a decorated handler`);
    for (const item of own) {
      if (!roles.includes(item.role))
        throw Error(`${where}: @${item.role} is not allowed on ${item.key}`);
      found.push({ ...item, method: value });
    }
  }
  if (!found.length) throw Error(`${where}: has no handlers`);
  const seen = new Map<string, string>();
  for (const { role, token, key } of found) {
    const id = `${role} ${token.type}`;
    if (seen.has(id))
      throw Error(
        `${where}: ${token.type} is handled by both ${seen.get(id)} and ${key}`,
      );
    seen.set(id, key);
  }
  return { where, found };
};
const register = (cls: Function, kind: string, definition: unknown) => {
  if (Object.hasOwn(cls, component))
    throw Error(`${cls.name}: decorated twice`);
  Object.defineProperty(cls, component, {
    value: { kind, definition } satisfies Component,
  });
};
/** Runs after static fields exist, so the class is checked whole; still throws where the class is declared. */
const onDeclared = (
  context: ClassDecoratorContext,
  convert: (cls: Function) => void,
) =>
  context.addInitializer(function (this: Function) {
    convert(this);
  });

/**
 * Declares an Aggregate, as Axon's `@Aggregate`, from its `@CommandHandler` and
 * `@EventSourcingHandler` methods. Context and name come from the qualified names of the
 * Commands and Events it handles, which must all belong to one Aggregate. List the class in
 * the Application's `aggregates`.
 */
export const Aggregate =
  <S extends TSchema>(options: {
    /** The State's schema; annotate handlers with `AggregateOf<typeof S>` and `StateOf<typeof S>`. */
    state: S;
    initialState: NoInfer<Static<S>>;
    /** Rules for every Command, run after its Steps. */
    rules?: CommandRule<NoInfer<Static<S>>>[];
    idGenerator?: (
      command: LegacyCommand,
      app: CommandApp,
    ) => string | Promise<string>;
    eventEnricher?: (
      event: Readonly<any>,
      state: AggregateState,
      command: LegacyCommand,
    ) => Metadata | void;
  }) =>
  (_cls: Function, context: ClassDecoratorContext) =>
    onDeclared(context, (cls) => {
      const { where, found } = collect(cls, "Aggregate", [
        "CommandHandler",
        "EventSourcingHandler",
      ]);
      const streams = [
        ...new Set(
          found.map(({ token }) => split(token.type, "").slice(0, 2).join(".")),
        ),
      ];
      if (streams.length !== 1)
        throw Error(
          `${where}: handles messages of ${streams.join(", ")}; an Aggregate handles its own only`,
        );
      const [context, name] = streams[0]!.split(".") as [string, string];
      const short = (token: { type: string }) => token.type.split(".")[2]!;
      // A Step sees `apply(EventType, payload)`; the kernel's `apply.<event>` stays underneath.
      const step =
        (fn: Function) => (command: unknown, view: any, app: unknown) =>
          fn(
            command,
            Object.create(view, {
              apply: {
                value: (
                  token: EventType,
                  payload: unknown,
                  metadata?: Metadata,
                ) => {
                  const apply =
                    token?.kind === "event" &&
                    token.type.startsWith(`${streams[0]}.`) &&
                    view.apply[short(token)];
                  if (!apply)
                    throw Error(
                      `${streams[0]} has no @EventSourcingHandler for ${token?.type}`,
                    );
                  apply(payload, metadata);
                },
              },
            }),
            app,
          );
      const events = found.filter(
        ({ role }) => role === "EventSourcingHandler",
      );
      const commands = found.filter(({ role }) => role === "CommandHandler");
      register(
        cls,
        "Aggregate",
        aggregate(context, name, {
          state: options.state,
          initialState: options.initialState,
          rules: options.rules,
          idGenerator: options.idGenerator,
          eventEnricher: options.eventEnricher,
          events: Object.fromEntries(
            events.map(({ token, method }) => {
              const { schema, schemaVersion, upcasters } = token as EventType;
              return [
                short(token),
                { schema, handler: method, schemaVersion, upcasters },
              ];
            }),
          ),
          commands: Object.fromEntries(
            commands.map(({ token, method, options }) => [
              short(token),
              {
                schema: (token as CommandType).schema,
                steps: [...(options.before ?? []), method].map((item) =>
                  typeof item === "function" ? step(item) : item,
                ),
                rules: options.rules,
              },
            ]),
          ),
          // Untyped on purpose: checking this call against generic TypeBox types costs over a million instantiations.
        } as any),
      );
    });

const consumer = (
  cls: Function,
  decorator: string,
  role: Role,
  others: Role[] = [],
) => {
  const reactions: Record<string, any> = {};
  const identity: Record<string, (event: any) => Identity | Promise<Identity>> =
    {};
  const messages: NonNullable<SagaDefinition["messages"]> = {},
    deadlines: NonNullable<SagaDefinition["deadlines"]> = {};
  for (const { role: found, token, method, options } of collect(cls, decorator, [
    role,
    ...others,
  ]).found) {
    if (found === "SagaDeadlineHandler") {
      deadlines[token.type] = method as any;
      continue;
    }
    if (found === "SagaMessageHandler") {
      messages[token.type] = {
        handle: method as any,
        schema: (token as SagaMessageType).schema,
        ...(options.start ? { start: true } : {}),
        ...(options.associationProperty !== undefined
          ? { identity: options.associationProperty }
          : {}),
      };
      continue;
    }
    reactions[token.type] = options.before?.length
      ? [...options.before, method]
      : method;
    const association = options.associationProperty;
    if (typeof association === "string")
      identity[token.type] = (event) => event.payload?.[association];
    else if (typeof association === "function")
      identity[token.type] = association;
  }
  return {
    reactions,
    ...(Object.keys(identity).length ? { identity } : {}),
    ...(Object.keys(messages).length ? { messages } : {}),
    ...(Object.keys(deadlines).length ? { deadlines } : {}),
  };
};
/**
 * Declares a Readmodel from its `@EventHandler` methods. List the class in a Readmodel group's
 * `models`; the key there is its stable name.
 */
export const Readmodel =
  (options: {
    schema: TSchema;
    indexes?: ReadmodelIndex[];
    initialState?: JsonObject;
  }) =>
  (_cls: Function, context: ClassDecoratorContext) =>
    onDeclared(context, (cls) =>
      register(cls, "Readmodel", {
        ...options,
        ...consumer(cls, "Readmodel", "EventHandler"),
      } satisfies ReadmodelDefinition),
    );
/**
 * Declares a Saga, as Axon's `@Saga`, from its `@SagaEventHandler`, `@SagaMessageHandler` and
 * `@SagaDeadlineHandler` methods. List the class in
 * the Application's `sagas`; the key there is its stable Consumer group name.
 */
export const Saga =
  (
    options: {
      reads?: string[];
      partitions?: number;
      /** Where the Saga starts reading the Event log when it is new; default `"now"`. */
      from?: ConsumerStart;
      initialState?: JsonObject;
    } = {},
  ) =>
  (_cls: Function, context: ClassDecoratorContext) =>
    onDeclared(context, (cls) =>
      register(cls, "Saga", {
        ...options,
        ...consumer(cls, "Saga", "SagaEventHandler", [
          "SagaMessageHandler",
          "SagaDeadlineHandler",
        ]),
      } satisfies SagaDefinition),
    );

const componentKey = component;
const resolve = <T>(component: unknown, kind: string, where: string): T => {
  if (typeof component !== "function") return component as T;
  // Own metadata only: an undecorated subclass would otherwise inherit its parent's definition.
  const found = Object.hasOwn(component, componentKey)
    ? ((component as any)[componentKey] as Component)
    : undefined;
  if (!found)
    throw Error(
      componentKey in component
        ? `${where}: ${component.name || "class"} extends a decorated class; inheritance is not supported`
        : `${where}: ${component.name || "class"} is not decorated with @${kind}`,
    );
  if (found.kind !== kind)
    throw Error(
      `${where}: ${component.name} is a @${found.kind}, not a @${kind}`,
    );
  return found.definition as T;
};
/** Replaces decorated classes in a config with the definitions they declare; other values pass through. */
export const resolveComponents = <C extends ApplicationConfig<any, any>>(
  config: C,
): Omit<C, "aggregates" | "readmodels" | "sagas"> &
  ResolvedApplicationConfig => ({
  ...config,
  ...(config.aggregates && {
    aggregates: config.aggregates.map((item) =>
      resolve<AggregateDefinition>(item, "Aggregate", "aggregates"),
    ),
  }),
  ...(config.readmodels && {
    readmodels: Object.fromEntries(
      Object.entries(config.readmodels).map(([group, definition]) => [
        group,
        {
          ...definition,
          models: Object.fromEntries(
            Object.entries(definition.models).map(([model, item]) => [
              model,
              resolve<ReadmodelDefinition>(
                item,
                "Readmodel",
                `readmodels.${group}.${model}`,
              ),
            ]),
          ),
        },
      ]),
    ),
  }),
  ...(config.sagas && {
    sagas: Object.fromEntries(
      Object.entries(config.sagas).map(([name, item]) => [
        name,
        resolve<SagaDefinition>(item, "Saga", `sagas.${name}`),
      ]),
    ),
  }),
});
