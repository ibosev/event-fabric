/**
 * The Application's names and payload types, filled by `event-fabric types` through
 * `declare module "event-fabric" { interface Register { ... } }` (ADR-0012). Types only.
 *
 * - `aggregates[context][aggregate]`: `events` and `commands`, name to payload
 * - `readmodels[group][model]`: the document
 * - `messages[name]`: the queued Message payload
 * - `sagaMessages[saga][name]`: a Saga Message payload
 * - `workflows[workflow].signals[name]`: a Signal payload
 * - `mode`: `"names"` checks names only and leaves payloads and State loose
 * - `services`: the Application's services; written by hand, never generated
 * - `readmodelServices`: the Readmodel helpers; written by hand
 * - `legacyApp`: the old accessors `legacyApp` and `requestApp` add to the Command, Reaction and
 *   `req.getApp()` contexts, such as `{ getCrud(): Crud }`; written by hand
 *
 * Parts left out fall back to untyped behaviour. Without a separate
 * `readmodelServices` contract, Readmodels retain the Application services contract.
 */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface Register {}

type Loose = {
  aggregates: Record<
    string,
    Record<
      string,
      { events: Record<string, any>; commands: Record<string, any> }
    >
  >;
  readmodels: Record<string, Record<string, any>>;
  messages: Record<string, any>;
  sagaMessages: Record<string, Record<string, any>>;
  workflows: Record<string, { signals: Record<string, any> }>;
  mode: "full";
  services: Record<string, any>;
  readmodelServices: undefined;
  legacyApp: object;
};
type Part<K extends keyof Loose> = K extends keyof Register
  ? Register[K]
  : Loose[K];
/** `Register` with every missing part replaced by its fallback. */
export type Registered = { [K in keyof Loose]: Part<K> };

type Aggregates = Registered["aggregates"];
/** `T[K]` when `K` is a key of `T`; avoids unprovable index errors in generic lookups. */
type Get<T, K> = K extends keyof T ? T[K] : never;
type Readmodels = Registered["readmodels"];
/** True while `T` is an open record (nothing generated for it). */
export type IsOpen<T> = string extends keyof T ? true : false;
type Payloads<Kind extends "events" | "commands"> =
  IsOpen<Aggregates> extends true
    ? Record<string, any>
    : {
        [Q in {
          [C in Extract<keyof Aggregates, string>]: {
            [A in Extract<keyof Aggregates[C], string>]: `${C}.${A}.${Extract<keyof Get<Aggregates[C][A], Kind>, string>}`;
          }[Extract<keyof Aggregates[C], string>];
        }[Extract<keyof Aggregates, string>]]: Q extends `${infer C}.${infer A}.${infer N}`
          ? Get<Get<Get<Get<Aggregates, C>, A>, Kind>, N>
          : never;
      };
/** Event payloads keyed by qualified name, `"context.aggregate.event"`. */
export type EventPayloads = Payloads<"events">;
/** Command payloads keyed by qualified name, `"context.aggregate.command"`. */
export type CommandPayloads = Payloads<"commands">;
/** Readmodel documents keyed by `"group.model"`. */
export type ReadmodelDocuments =
  IsOpen<Readmodels> extends true
    ? Record<string, any>
    : {
        [Q in {
          [G in Extract<keyof Readmodels, string>]: `${G}.${Extract<keyof Readmodels[G], string>}`;
        }[Extract<keyof Readmodels, string>]]: Q extends `${infer G}.${infer M}`
          ? Get<Get<Readmodels, G>, M>
          : never;
      };
export type MessagePayloads = Registered["messages"];
type SagaMessages = Registered["sagaMessages"];
/** A Saga that handles Saga Messages. */
export type SagaName = Extract<keyof SagaMessages, string>;
/** The Saga Messages Saga `S` handles. */
export type SagaMessageName<S extends string> =
  IsOpen<SagaMessages> extends true
    ? string
    : S extends keyof SagaMessages
      ? Extract<keyof SagaMessages[S], string>
      : never;
export type SagaMessagePayload<S extends string, N extends string> =
  IsOpen<SagaMessages> extends true ? any : Get<Get<SagaMessages, S>, N>;
/** Access map `sagaMessages` keys: a Saga Message, a Saga with `.*`, or `*`. */
export type SagaMessageAccessKey =
  IsOpen<SagaMessages> extends true
    ? string
    :
        | "*"
        | {
            [S in SagaName]: `${S}.*` | `${S}.${SagaMessageName<S>}`;
          }[SagaName];
type Workflows = Registered["workflows"];
/** Access map `workflows` keys: `<workflow>.start`, `.query`, `.signals.<name>`, a Workflow with `.*`, or `*`. */
export type WorkflowAccessKey =
  IsOpen<Workflows> extends true
    ? string
    :
        | "*"
        | {
            [W in Extract<keyof Workflows, string>]:
              | `${W}.${"*" | "start" | "query"}`
              | `${W}.signals.${Extract<keyof Get<Workflows[W], "signals">, string>}`;
          }[Extract<keyof Workflows, string>];
/** Application services; also the default for Readmodels that have not opted into isolation. */
export type RegisteredServices = Registered["services"];
/** The Readmodel helpers contract, `Register["readmodelServices"]`; `undefined` until declared. */
export type RegisteredReadmodelHelpers = Registered["readmodelServices"];
/** The old accessors added to Command, Reaction and `req.getApp()` contexts, `Register["legacyApp"]`. */
export type RegisteredLegacyApp = Registered["legacyApp"];
/** Whether a Readmodel helpers contract is left undeclared. */
export type Undeclared<T> = [T] extends [undefined] ? true : false;
/** What Readmodel Reactions get from `getServices()`: the Readmodel helpers once declared, else the Application services. */
export type RegisteredReadmodelServices =
  Undeclared<RegisteredReadmodelHelpers> extends true
    ? RegisteredServices
    : RegisteredReadmodelHelpers;
export type EventName = Extract<keyof EventPayloads, string>;
export type CommandName = Extract<keyof CommandPayloads, string>;
export type ReadmodelName = Extract<keyof ReadmodelDocuments, string>;
export type MessageName = Extract<keyof MessagePayloads, string>;
export type EventPayload<N extends EventName> = EventPayloads[N];
export type CommandPayload<N extends CommandName> = CommandPayloads[N];
export type ReadmodelDocument<N extends ReadmodelName> = ReadmodelDocuments[N];
export type MessagePayload<N extends MessageName> = MessagePayloads[N];

/** The Events and Commands registered for one Aggregate; open (untyped) when it is not registered yet. */
export type AggregateTypes<C extends string, A extends string> =
  IsOpen<Aggregates> extends true
    ? { events: any; commands: any; registered: false }
    : C extends keyof Aggregates
    ? A extends keyof Aggregates[C]
      ? Aggregates[C][A] & { registered: true }
      : { events: any; commands: any; registered: false }
    : { events: any; commands: any; registered: false };
export type ReadmodelGroupName = Extract<keyof Readmodels, string>;
export type ReadmodelModelName<G extends string> = G extends keyof Readmodels
  ? Extract<keyof Readmodels[G], string>
  : string;
/** An `.await` / `awaitCorrelated` pattern: a qualified Event name, or one with `*` wildcards. */
export type EventPattern =
  IsOpen<Aggregates> extends true ? string : EventName | `${string}*${string}`;
/** Access map `domain` keys: a Command, an Aggregate, a Context with `.*`, or `*`. */
export type CommandAccessKey =
  IsOpen<Aggregates> extends true
    ? string
    :
        | "*"
        | CommandName
        | {
            [C in Extract<keyof Aggregates, string>]:
              | `${C}.*`
              | `${C}.${Extract<keyof Aggregates[C], string>}`;
          }[Extract<keyof Aggregates, string>];
/** Access map `readmodels` keys: a Readmodel, a group with `.*`, or `*`. */
export type ReadmodelAccessKey =
  IsOpen<Readmodels> extends true
    ? string
    : "*" | ReadmodelName | `${ReadmodelGroupName}.*`;
type CommandCall<P, R, Extra extends unknown[] = []> = undefined extends P
  ? (payload?: P, metadata?: Record<string, unknown>, ...extra: Extra) => R
  : (payload: P, metadata?: Record<string, unknown>, ...extra: Extra) => R;
/** `getDomain()`: `domain.context.aggregate(id).command(payload, metadata)`, typed from `Register`. */
export type Domain<R, Extra extends unknown[] = [], Loaded = {}> =
  IsOpen<Aggregates> extends true
    ? any
    : {
        [C in keyof Aggregates]: {
          [A in keyof Aggregates[C]]: (id?: string) => {
            [K in keyof Get<Aggregates[C][A], "commands">]: CommandCall<
              Get<Get<Aggregates[C][A], "commands">, K>,
              R,
              Extra
            >;
          } & Loaded;
        };
      };
