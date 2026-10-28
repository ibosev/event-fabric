import { Type, type Static as TypeBoxStatic, type TSchema } from "typebox";
import type { CommandInput } from "../id.js";
import type {
  AggregateDefinition,
  AggregateState,
  LegacyCommand,
} from "../kernel/index.js";
import type { Message, Metadata } from "../types.js";
import type { CommandApp, WorkerContext } from "../runtime/types.js";
import { assertPlainSchema } from "../validation.js";
import {
  compile,
  type CommandRule,
  type ExistenceMarker,
  type CommandStep,
  type EventHandler,
} from "./index.js";

/**
 * The type a TypeBox schema describes. Flattened once, so TypeScript does not re-expand
 * TypeBox's deferred types wherever a payload flows.
 *
 * For a Command this is the validated payload the Steps receive. Ajv fills `default`s
 * before the schema check, but TypeBox's types do not record them, so senders must
 * supply a defaulted field too, or declare it `Type.Optional`.
 */
export type Static<T extends TSchema> =
  TypeBoxStatic<T> extends infer R ? { [K in keyof R]: R[K] } : never;

type Stored<T> = T extends Date
  ? T
  : T extends readonly (infer I)[]
    ? Stored<I>[]
    : T extends object
      ? {
          [K in keyof T]: undefined extends T[K]
            ? Stored<Exclude<T[K], undefined>> | null
            : Stored<T[K]>;
        }
      : T;
/**
 * A Readmodel document as stores return it: an optional field may be absent or `null` (the
 * Postgres store reads an absent field as `null`), and the document carries its `_id`. Declare date-time fields with `DateTime()`.
 */
export type Document<T extends TSchema> = Stored<Static<T>> & { _id: string };
/** A Readmodel date-time field: stored as a timestamp and read back as a `Date`. */
export const DateTime = (options: { description?: string } = {}) =>
  Type.Unsafe<Date>({ ...options, type: "string", format: "date-time" });

declare const types: unique symbol;
/** Builds a typed `CommandInput`; pass `undefined` as `id` to let the Aggregate generate one. */
export type CommandHandle<P = any> = (
  id: string | undefined,
  payload: P,
  options?: Omit<CommandInput, "stream" | "name" | "payload">,
) => CommandInput;
/** A queued Message type: its name, schema and payload type. */
export type MessageHandle<N extends string = string, P = any> = {
  readonly name: N;
  readonly schema: TSchema;
  readonly [types]?: P;
};

type Payloads<E> = { [K in keyof E]: E[K] extends TSchema ? Static<E[K]> : never };
type CommandAuthoring<E, S, P> = {
  /** The payload schema, checked before any Step runs. */
  schema: TSchema;
  /**
   * `only.ifExists()`/`only.ifNotExists()` and Steps, in order. A Precondition is a Step
   * that throws; Steps apply Events.
   */
  steps: (ExistenceMarker | CommandStep<E, S, P>)[];
  /** Rules: run after the Steps, with the State before and after them. */
  rules?: CommandRule<S, P>[];
};
type EventAuthoring<S, P> = {
  schema: TSchema;
  handler: EventHandler<S, P>;
  schemaVersion?: number;
  upcasters?: Record<number, (payload: any) => any | Promise<any>>;
};
/** A compiled Aggregate that also records its types, for `defineDomain`. */
export type Aggregate<
  C extends string = string,
  A extends string = string,
  E = any,
  K = any,
> = AggregateDefinition & {
  readonly [types]?: { context: C; name: A; events: E; commands: K };
};

/**
 * Declares an Aggregate whose types all come from this call: TypeBox schemas give each
 * Event, Command and the State their types. Collect Aggregates with `defineDomain` to
 * reference their Events and send their Commands from other files.
 */
export function aggregate<
  const C extends string,
  const A extends string,
  S extends TSchema,
  E extends Record<string, TSchema>,
  K extends Record<string, TSchema>,
>(
  context: C,
  name: A,
  authored: {
    /** The State's schema; it types `get`, `set` and `initialState`. */
    state: S;
    initialState: NoInfer<Static<S>>;
    events: {
      [N in keyof E]: EventAuthoring<NoInfer<Static<S>>, Static<E[N]>> & {
        schema: E[N];
      };
    };
    commands: {
      [N in keyof K]: CommandAuthoring<
        NoInfer<Payloads<E>>,
        NoInfer<Static<S>>,
        Static<K[N]>
      > & { schema: K[N] };
    };
    idGenerator?: (
      command: LegacyCommand,
      app: CommandApp,
    ) => string | Promise<string>;
    eventEnricher?: (
      event: Readonly<any>,
      state: AggregateState,
      command: LegacyCommand,
    ) => Metadata | void;
    /** Rules for every Command, run after its Steps. */
    rules?: CommandRule<NoInfer<Static<S>>>[];
  },
): Aggregate<C, A, Payloads<E>, Payloads<K>> {
  const where = `${context}.${name}`;
  const { state: _state, commands, ...rest } = authored;
  for (const [event, { schema }] of Object.entries(authored.events))
    assertPlainSchema(schema, `${where}.${event}`);
  for (const [command, { schema }] of Object.entries(commands))
    assertPlainSchema(schema, `${where}.${command}`);
  return compile(context, name, {
    ...rest,
    commands: Object.fromEntries(
      Object.entries(commands).map(([command, { schema, steps, rules }]) => [
        command,
        [{ schema }, ...steps, ...(rules ?? []).map((rule: unknown) => ({ businessRule: rule }))],
      ]),
    ),
  } as any);
}

type UnionToIntersection<U> = (
  U extends unknown ? (value: U) => void : never
) extends (value: infer I) => void
  ? I
  : never;
type Types<T> = T extends { readonly [types]?: infer X } ? NonNullable<X> : never;
/** The context, name, Event and Command payloads an `aggregate()` recorded. */
export type AggregateInfo<T> = Types<T>;
type Members<X> = X extends {
  context: string;
  name: string;
  events: infer E;
  commands: infer K;
}
  ? { readonly [N in keyof E]: `${X["context"]}.${X["name"]}.${N & string}` } & {
      readonly [N in keyof K]: CommandHandle<K[N]>;
    }
  : never;
// `T extends unknown` distributes over a union of Aggregates, one entry each.
type Entry<T> = T extends unknown
  ? Types<T> extends infer X extends { context: string; name: string }
    ? { readonly [C in X["context"]]: { readonly [A in X["name"]]: Members<X> } }
    : never
  : never;
type QualifiedEvents<T> = T extends unknown ? Types<T> extends infer X extends {
  context: string;
  name: string;
  events: unknown;
}
  ? {
      [N in Extract<keyof X["events"], string>as `${X["context"]}.${X["name"]}.${N}`]: X["events"][N];
    }
  : never : never;
/**
 * `domain.<context>.<aggregate>.<event>` is the Event's qualified name and
 * `domain.<context>.<aggregate>.<command>(id, payload)` builds a typed Command.
 */
export type DomainHandles<T extends readonly Aggregate[]> = UnionToIntersection<
  Entry<T[number]>
> & { readonly [types]?: UnionToIntersection<QualifiedEvents<T[number]>> };

/**
 * Collects Aggregates into one namespace: `domain.billing.invoice.settled` is the qualified
 * Event name `"billing.invoice.settled"`, and `domain.billing.invoice.issue(id, payload)`
 * builds a typed Command.
 */
export const defineDomain = <const T extends readonly Aggregate[]>(
  ...aggregates: T
): DomainHandles<T> => {
  const domain: Record<string, Record<string, Record<string, unknown>>> = {};
  for (const { context, name, events, commands } of aggregates) {
    const contextMembers = (domain[context] ??= {});
    if (contextMembers[name])
      throw new Error(`Duplicate Aggregate ${context}.${name}`);
    const members: Record<string, unknown> = (contextMembers[name] = {});
    for (const event of Object.keys(events))
      members[event] = `${context}.${name}.${event}`;
    for (const command of Object.keys(commands)) {
      if (Object.hasOwn(members, command))
        throw new Error(
          `${context}.${name}: Command and Event are both named ${command}`,
        );
      members[command] = (
        id: string | undefined,
        payload: unknown,
        options = {},
      ): CommandInput => ({
        ...options,
        stream: { context, aggregate: name, id },
        name: command,
        payload,
      });
    }
  }
  return domain as unknown as DomainHandles<T>;
};

/**
 * Event payloads keyed by qualified name, for a domain or a single Aggregate: type a
 * Reaction map keyed by `[domain.billing.invoice.settled]` with
 * `Reactions<EventsOf<typeof domain>, ReactionView<Document>>`.
 */
export type EventsOf<T> = T extends { readonly [types]?: infer X }
  ? X extends { context: string }
    ? QualifiedEvents<T>
    : NonNullable<X>
  : never;

/** Declares a queued Message type; `app.queue(handle, payload)` and `worker(handle, ...)` take it. */
export const message = <const N extends string, S extends TSchema>(
  name: N,
  schema: S,
): MessageHandle<N, Static<S>> => {
  assertPlainSchema(schema, `Message ${name}`);
  return { name, schema };
};

/**
 * A Worker for one Message type; the Message's schema is checked when the Message is queued.
 * What `execute` returns is the `value` of its reply (`ReplyOf<typeof worker>`).
 */
export const worker = <N extends string, P, R = unknown>(
  handle: MessageHandle<N, P>,
  definition: {
    concurrency?: number;
    timeout?: number;
    retry?: { attempts?: number; baseDelay?: number; maxDelay?: number };
    execute(
      message: { payload: P } & Omit<Message, "payload">,
      ctx: WorkerContext,
    ): Promise<R> | R;
  },
) => ({
  ...definition,
  handles: [handle.name],
  schemas: { [handle.name]: handle.schema },
});
