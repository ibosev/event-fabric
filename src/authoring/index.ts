import type { Metadata, PendingEvent, Schema } from "../types.js";
import type { AggregateTypes, Registered } from "../registry.js";
import type { CommandApp } from "../runtime/types.js";
import type {
  AggregateApi,
  AggregateDefinition,
  LegacyEvent,
  Step,
  Rule,
  PreLoad,
  LegacyCommand,
  AggregateState,
} from "../kernel/index.js";

type Marker =
  | { settings: { existing: boolean } }
  | { schema: Schema }
  | { preLoadCondition: PreLoad }
  | { preCondition: Step }
  | { businessRule: Rule };
/** A Precondition, Rule or existence check from `only`. */
export type AuthoringMarker = Marker;
/** `only.ifExists()` or `only.ifNotExists()`. */
export type ExistenceMarker = { settings: { existing: boolean } };
/** `only.if(predicate)`: a Readmodel or Saga Reaction runs only when it returns true. */
export type PredicateMarker = {
  settings: {
    shouldHandleEvent: (
      event: any,
      view: any,
      app: any,
    ) => boolean | Promise<boolean>;
  };
};
type IsAny<T> = 0 extends 1 & T ? true : false;
type Key<S> = IsAny<S> extends true ? string : Extract<keyof S, string>;
type Value<S, K> =
  IsAny<S> extends true ? any : K extends keyof S ? S[K] : never;
/** Typed `get`/`set` over State `S`; top-level keys only (dotted paths need an untyped State). */
export interface StateView<S = any> {
  get(): IsAny<S> extends true ? any : S & { id: string };
  get<K extends Key<S>>(path: K): Value<S, K>;
  set<K extends Key<S>>(path: K, value: Value<S, K>): void;
  set(values: IsAny<S> extends true ? Record<string, any> : Partial<S>): void;
}
/** Applies an Event; a payload-less Event may be applied without one. */
type ApplyEvent<P> = undefined extends P
  ? (payload?: P, metadata?: Metadata) => void
  : (payload: P, metadata?: Metadata) => void;
/** The Aggregate a Step receives: `apply` has one typed function per declared Event. */
export type AggregateView<E = any, S = any> = Omit<
  AggregateApi,
  "get" | "apply"
> &
  Pick<StateView<S>, "get"> & {
    apply: IsAny<E> extends true
      ? AggregateApi["apply"]
      : { [K in keyof E]: ApplyEvent<E[K]> };
  };
/** The State an Event handler folds into. */
export type AggregateFoldState<S = any> =
  IsAny<S> extends true
    ? AggregateState
    : Omit<AggregateState, "get" | "set" | "attributes" | "clone"> &
        StateView<S>;
export type AggregateCommand<P = any> = Omit<LegacyCommand, "payload"> & {
  payload: P;
};
export type AggregateEvent<P = any> = Omit<LegacyEvent, "payload"> & {
  payload: P;
};
export type CommandStep<E = any, S = any, P = any> = (
  command: AggregateCommand<P>,
  aggregate: AggregateView<E, S>,
  app: CommandApp,
) => unknown;
/** A Rule (`only.after`): runs after the Steps with the State before and after them. */
export type CommandRule<S = any, P = any> = (
  current: AggregateFoldState<S>,
  previous: AggregateFoldState<S>,
  events: readonly PendingEvent[],
  command: AggregateCommand<P>,
  app: CommandApp,
) => unknown;
export type CommandAuthoring<E = any, S = any, P = any> =
  | Marker
  | CommandStep<E, S, P>
  | CommandAuthoring<E, S, P>[];
export type EventHandler<S = any, P = any> = (
  event: AggregateEvent<P>,
  state: AggregateFoldState<S>,
) => void;
export type EventAuthoring<S = any, P = any> =
  | EventHandler<S, P>
  | EventHandler<S, P>[]
  | {
      handler: EventHandler<S, P>;
      schemaVersion?: number;
      schema?: Schema;
      upcasters?: Record<number, (payload: any) => any | Promise<any>>;
    };
/**
 * `E` maps Event names to payloads, `S` is the State, `C` maps Command names to payloads.
 * Each defaults to `any`, which keeps that part as permissive as untyped authoring.
 */
export type AggregateAuthoring<E = any, S = any, C = any> = {
  initialState?: IsAny<S> extends true ? Record<string, any> : S;
  commands?: IsAny<C> extends true
    ? Record<string, CommandAuthoring<E, S>>
    : { [K in keyof C]: CommandAuthoring<E, S, C[K]> };
  events?: IsAny<E> extends true
    ? Record<string, EventAuthoring<S>>
    : { [K in keyof E]: EventAuthoring<S, E[K]> };
  idGenerator?: (
    command: LegacyCommand,
    app: CommandApp,
  ) => string | Promise<string>;
  eventEnricher?: (
    event: Readonly<any>,
    state: AggregateState,
    command: LegacyCommand,
  ) => Metadata | void;
  rules?: Rule[];
};
export const only = {
  ifExists: (): ExistenceMarker => ({ settings: { existing: true } }),
  ifNotExists: (): ExistenceMarker => ({ settings: { existing: false } }),
  /** Readmodel and Saga Reactions only: a false result skips the whole Reaction, so no document or instance is created. */
  if: (
    predicate: PredicateMarker["settings"]["shouldHandleEvent"],
  ): PredicateMarker => ({ settings: { shouldHandleEvent: predicate } }),
  ifValidatedBy: (schema: Schema | PreLoad): Marker =>
    typeof schema === "function"
      ? { preLoadCondition: schema as PreLoad }
      : { schema },
  ifState: (check: Step): Marker => ({ preCondition: check }),
  after: (check: Rule): Marker => ({ businessRule: check }),
};
const flat = (value: any): any[] =>
  Array.isArray(value) ? value.flatMap(flat) : [value];
/** Names declared here but not generated yet stay loose until `event-fabric types` runs. */
type WithLocal<T, X extends string> =
  IsAny<T> extends true ? any : T & { [K in Exclude<X, keyof T>]: any };
/** State is inferred from `initialState` once the Aggregate is registered; `mode: "names"` keeps it loose. */
type StateFor<Types, S> = Types extends { registered: true }
  ? Registered["mode"] extends "names"
    ? any
    : S
  : any;
/**
 * Declares an Aggregate. Context and name are inferred, and Event and Command payloads
 * come from the generated `Register` (ADR-0012), so typed authoring needs no type arguments.
 */
export const compile = <
  const C extends string,
  const A extends string,
  S extends object = any,
  XE extends string = never,
  XC extends string = never,
>(
  context: C,
  name: A,
  typed: AggregateAuthoring<
    WithLocal<AggregateTypes<C, A>["events"], NoInfer<XE>>,
    NoInfer<StateFor<AggregateTypes<C, A>, S>>,
    WithLocal<AggregateTypes<C, A>["commands"], NoInfer<XC>>
  > & {
    initialState?: S;
    events?: Record<XE, unknown>;
    commands?: Record<XC, unknown>;
  },
): AggregateDefinition => {
  const authored = typed as AggregateAuthoring;
  const known = ["initialState", "commands", "events", "idGenerator", "eventEnricher", "rules"];
  for (const key of Object.keys(authored))
    if (!known.includes(key))
      throw Error(
        `${context}.${name}: unsupported Aggregate option ${key}${key === "options" ? " (Events are always persisted and history always loaded)" : ""}; remove it after review`,
      );
  (authored.rules ?? []).forEach((rule, index) => {
    if (typeof rule !== "function")
      throw Error(
        `${context}.${name}: rule ${index} is not a function; pass the function itself and keep a named Rule's rejection code in its Rejection`,
      );
  });
  const commands: AggregateDefinition["commands"] = {};
  for (const [command, authoring] of Object.entries(authored.commands ?? {})) {
    const spec: AggregateDefinition["commands"][string] = {
      preLoad: [],
      steps: [],
      rules: [...(authored.rules ?? [])],
    };
    const fail = (message: string): never => {
      throw Error(`${context}.${name}.${command}: ${message}`);
    };
    for (const item of flat(authoring)) {
      if (typeof item === "function") spec.steps.push(item);
      else if (
        item?.settings &&
        typeof item.settings.existing === "boolean" &&
        Object.keys(item.settings).length === 1
      ) {
        const existence = item.settings.existing ? "exists" : "notExists";
        if (spec.existence && spec.existence !== existence)
          fail("contradictory existence checks");
        spec.existence = existence;
      } else if (item && Object.hasOwn(item, "schema")) {
        if (spec.schema !== undefined) fail("two payload schemas");
        spec.schema = item.schema;
      } else if (typeof item?.preLoadCondition === "function") {
        if (spec.steps.length) fail("pre-load check must precede Steps");
        spec.preLoad.push(item.preLoadCondition);
      } else if (typeof item?.preCondition === "function")
        spec.steps.push(item.preCondition);
      else if (typeof item?.businessRule === "function")
        spec.rules.push(item.businessRule);
      else fail("unrecognised authoring item");
    }
    commands[command] = spec;
  }
  const events: AggregateDefinition["events"] = {},
    eventVersions: Record<string, number> = {},
    eventSchemas: NonNullable<AggregateDefinition["eventSchemas"]> = {};
  for (const [event, value] of Object.entries(authored.events ?? {})) {
    const handlers =
      typeof value === "object" && !Array.isArray(value)
        ? [value.handler]
        : flat(value);
    if (
      !handlers.length ||
      handlers.some((handler) => typeof handler !== "function")
    )
      throw Error(`${context}.${name}.${event}: Event requires handlers`);
    events[event] = (envelope, state) => {
      for (const handler of handlers) handler(envelope, state);
    };
    eventVersions[event] =
      typeof value === "object" && !Array.isArray(value)
        ? (value.schemaVersion ?? 1)
        : 1;
    if (
      !Number.isSafeInteger(eventVersions[event]) ||
      eventVersions[event]! < 1
    )
      throw Error(`${context}.${name}.${event}: invalid Schema version`);
    eventSchemas[`${context}.${name}.${event}`] = {
      version: eventVersions[event]!,
      upcasters:
        typeof value === "object" && !Array.isArray(value)
          ? (value.upcasters ?? {})
          : {},
      schema:
        typeof value === "object" && !Array.isArray(value)
          ? value.schema
          : undefined,
    };
  }
  return {
    context,
    name,
    initialState: authored.initialState ?? {},
    commands,
    events,
    eventVersions,
    eventSchemas,
    newId: authored.idGenerator,
    enrich: authored.eventEnricher,
  };
};
