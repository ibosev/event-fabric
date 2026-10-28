import type { JsonObject, Position, Schema, Validate } from "../types.js";
export type ReadmodelIndex = {
  fields: string[] | Record<string, 1 | -1>;
  sparse?: boolean;
  text?: boolean;
};
export type ReadmodelDefinition = {
  schema: Schema;
  indexes?: ReadmodelIndex[];
};
export type ReadmodelDefinitions = Record<string, ReadmodelDefinition>;
/** How a native handle joins other models' documents (Mongo `$lookup`, `$graphLookup`, `$unionWith`). */
export type NativeOptions = {
  /** The caller's filter for a joined model's documents; throwing `ForbiddenError` refuses the join. */
  joinScope?: (group: string, model: string) => Promise<Filter>;
};
type IsAny<T> = 0 extends 1 & T ? true : false;
type Element<T> = T extends readonly (infer E)[] ? E : T;
// A nested object a dotted path can enter; dates and arrays of scalars are leaves.
type Nested<T> = T extends Date ? never : T extends object ? T : never;
// A date-time field also accepts its ISO string in a query.
type QueryValue<T> = T extends Date ? Date | string : T;
type Equality<T> = QueryValue<T> | QueryValue<Element<T>>;
type Ranged<T> = [Exclude<Element<T>, null>] extends [string | number | Date]
  ? {
      gt?: QueryValue<Element<T>>;
      gte?: QueryValue<Element<T>>;
      lt?: QueryValue<Element<T>>;
      lte?: QueryValue<Element<T>>;
    }
  : {};
type ElementMatch<T> =
  NonNullable<T> extends readonly (infer E)[]
    ? { elemMatch?: [Nested<E>] extends [never] ? Condition<E> : TypedFilter<E> }
    : {};
/** A condition on one field: a value (an array field also matches one element) or operators. */
export type Condition<T> =
  | Equality<T>
  | ({
      eq?: Equality<T>;
      ne?: Equality<T>;
      in?: Equality<T>[];
      nin?: Equality<T>[];
      oneOf?: Condition<T>[];
      exists?: boolean;
    } & Ranged<T> &
      ElementMatch<T>);
type Depth = [never, 0, 1, 2, 3];
/** Every field path of a document, dotted into nested objects and arrays of objects. */
export type FieldPath<D, N extends number = 4> = [N] extends [never]
  ? never
  : {
      [K in keyof D & string]:
        | K
        | `${K}.${FieldPath<Nested<Element<NonNullable<D[K]>>>, Depth[N]>}`;
    }[keyof D & string];
/** The value at a dotted path; `null` when a parent on the way may be null. */
export type FieldValue<D, P extends string> = P extends keyof D
  ? D[P]
  : P extends `${infer K}.${infer R}`
    ? K extends keyof D
      ?
          | FieldValue<Nested<Element<NonNullable<D[K]>>>, R>
          | (null extends D[K] ? null : never)
      : never
    : never;
/** A query filter typed by the document: known paths, their value types and fitting operators. */
export type TypedFilter<D> = { [P in FieldPath<D>]?: Condition<FieldValue<D, P>> } & {
  oneOf?: TypedFilter<D>[];
  and?: TypedFilter<D>[];
};
/** A Readmodel query filter; loose when the document type is unknown. */
export type Filter<D = any> =
  IsAny<D> extends true ? Record<string, any> : TypedFilter<D>;
/** Sort fields by direction; ties break by `_id`. */
export type Sort<D = any> =
  IsAny<D> extends true
    ? Record<string, 1 | -1>
    : { [P in FieldPath<D>]?: 1 | -1 };
export type QueryOptions<D = any> = {
  sort?: Sort<D>;
  from?: number;
  size?: number;
  q?: string;
  generation?: string;
};
export type ReadmodelChange = {
  model: string;
  id: string;
  document: JsonObject | null;
};
export type SchemaMigration = {
  from: string;
  to: string;
  /**
   * Runs in one transaction with the hash update; throwing rolls it back. `native` depends on the adapter:
   * - memory: the `ReadmodelStore` itself (changes to the generation are restored on failure);
   * - Mongo: `{ session, collection(model), database }`, whose collection and database handles inject `session`;
   * - Postgres: `{ schema, tableFor(model), query(text, values), pool }`, where `query` runs in the transaction and `pool` does not.
   */
  up: (native: any) => Promise<void>;
};
export type RegisterOptions = {
  validator?: Validate;
  validate?: boolean;
  schemas?: Record<string, Schema>;
  migrations?: SchemaMigration[];
};
export interface ReadmodelStore<TNative = unknown> {
  register(
    group: string,
    definitions: ReadmodelDefinitions,
    options?: RegisterOptions,
  ): Promise<{ rebuildRequired: boolean; hash: string }>;
  /** `partition`: one Partition of a partitioned group, fenced by its own Lease epoch. */
  fence(group: string, epoch: number, partition?: number): Promise<void>;
  /**
   * Starts Partitions 0..N-1 at `position`, N = `epochs.length`, if each is still fenced by
   * the given epoch; throws StaleEpochError otherwise. Every other Partition's Checkpoint is
   * invalidated as well.
   */
  resetPartitions(
    group: string,
    epochs: number[],
    position: Position,
  ): Promise<void>;
  prepareSkip(
    group: string,
    epoch: number,
    position: Position,
    partition?: number,
  ): Promise<void>;
  /**
   * The group's Checkpoint; with `partition`, that Partition's; with `partitions: N`, the
   * lowest of Partitions 0..N-1, which every one of them has passed.
   */
  checkpoint(
    group: string,
    options?: { generation?: string; partition?: number; partitions?: number },
  ): Promise<Position | null>;
  /**
   * Writes documents into a pending generation that has no Checkpoint yet, for a Readmodel import;
   * the group's Lease epoch fences it. A later `batch` with no changes sets the Checkpoint.
   */
  load?(
    group: string,
    epoch: number,
    generation: string,
    changes: ReadmodelChange[],
    options?: { reconstruction?: string },
  ): Promise<void>;
  /**
   * Commits documents and the Checkpoint; a position at or before the Checkpoint is a no-op.
   * `requireActive`: the changes were folded against `generation` as the active generation,
   * so the batch throws StaleGenerationError if another generation became active.
   */
  batch(
    group: string,
    epoch: number,
    position: Position,
    changes: ReadmodelChange[],
    options?: {
      generation?: string;
      partition?: number;
      /** One fold worker of a sliced rebuild of `generation`: commits that slice's Checkpoint instead. */
      slice?: number;
      requireActive?: boolean;
      /** Must match the reconstruction recorded when creating the generation. */
      reconstruction?: string;
    },
  ): Promise<void>;
  /**
   * The slices of a rebuild folded by workers, one per Partition; only stores whose slices can
   * commit in parallel have it. With `count`, starts that many slices of a generation without a
   * Checkpoint, or resumes them (another count throws). Returns each slice's Checkpoint, or null
   * when the generation has no slices.
   */
  slices?(
    group: string,
    generation: string,
    count?: number,
  ): Promise<(Position | null)[] | null>;
  find(
    group: string,
    model: string,
    filter?: Filter,
    options?: QueryOptions,
  ): Promise<JsonObject[]>;
  findOne(
    group: string,
    model: string,
    filter?: Filter,
    options?: QueryOptions,
  ): Promise<JsonObject | null>;
  count(
    group: string,
    model: string,
    filter?: Filter,
    options?: QueryOptions,
  ): Promise<number>;
  stream(
    group: string,
    model: string,
    filter?: Filter,
    options?: QueryOptions,
  ): AsyncIterable<JsonObject>;
  pendingGeneration(group: string): Promise<string | null>;
  activeGeneration(group: string): Promise<string>;
  createGeneration(group: string, reconstruction?: string): Promise<string>;
  swapGeneration(
    group: string,
    generation: string,
    minimumPosition?: Position,
    reconstruction?: string,
  ): Promise<void>;
  dropGeneration(group: string, generation: string): Promise<void>;
  /**
   * The adapter's own handle for one model. With `scope`, a read-only handle that selects only documents
   * matching it, where the adapter supports that (Mongo); otherwise `ForbiddenError`.
   */
  native(
    group: string,
    model: string,
    scope?: Filter,
    options?: NativeOptions,
  ): Promise<TNative>;
  /**
   * Names the physical store, the same for every instance that opens it: the runtime refuses a
   * group's Leases to an instance whose store differs from the other live instances'.
   */
  storeId?(): Promise<string>;
  close(): Promise<void>;
}
