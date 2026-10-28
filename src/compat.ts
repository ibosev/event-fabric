import { Readable } from "node:stream";
import type { Filter, QueryOptions } from "./readmodel-store/types.js";
import type { JsonObject, Metadata } from "./types.js";
import type {
  AggregateState,
  LegacyCommand,
  LegacyEvent,
} from "./kernel/index.js";
import { legacyEnrichers } from "./internal.js";
import type { Request, RequestHandler } from "express";
import { Rejection } from "./errors.js";
import { legacyListPage } from "./legacy-gateway.js";
import { handle } from "./gateway/index.js";
import type {
  ReadmodelDocuments,
  ReadmodelGroupName,
  ReadmodelModelName,
} from "./registry.js";

/**
 * Old-framework behavior for code that `migrateSource` translated. Each helper keeps
 * what the old stack did at one call site; new code uses the plain API instead.
 */

const isPlain = (value: unknown): value is Record<string, any> =>
  !!value && typeof value === "object" && !Array.isArray(value);
// lodash.merge: plain objects and arrays merge key by key; undefined never overwrites.
// Metadata can come from clients, so prototype keys are skipped and only own values merge.
const merge = (target: any, source: any): any => {
  for (const [key, value] of Object.entries(source)) {
    if (
      value === undefined ||
      ["__proto__", "constructor", "prototype"].includes(key)
    )
      continue;
    const current = Object.hasOwn(target, key) ? target[key] : undefined;
    target[key] =
      (isPlain(value) && isPlain(current)) ||
      (Array.isArray(value) && Array.isArray(current))
        ? merge(current, value)
        : value !== null && typeof value === "object"
          ? structuredClone(value)
          : value;
  }
  return target;
};

/**
 * The old applied Event's metadata: the Command's deep-merged over the Event's, Command values winning.
 * Like lodash.merge, it merges into `event` and returns it, so code reading that object afterwards sees the merge.
 */
export const legacyMetadata = (
  event: Metadata = {},
  command: Metadata = {},
): Metadata => merge(event, command);

/**
 * An old `eventEnricher`: it runs inside each `apply`, before that Event's fold, sees the State before it,
 * and may change or return the whole Event; the payload and metadata it leaves are recorded and folded,
 * so later Steps and Rules see them. Without it, an enricher runs once on the final State and adds metadata.
 */
export const legacyEnricher = (
  enricher: (
    event: LegacyEvent & Record<string, any>,
    state: AggregateState,
    command: LegacyCommand,
  ) => unknown,
): ((
  event: Readonly<LegacyEvent>,
  state: AggregateState,
  command: LegacyCommand,
) => Metadata | void) => {
  legacyEnrichers.add(enricher);
  // Typed as an `eventEnricher`; the kernel treats what a marked enricher returns as the Event.
  return enricher as any;
};

export type LegacyQuery = {
  filter?: Record<string, any>;
  sort?: Record<string, any> | Record<string, any>[];
  from?: number;
  size?: number;
  q?: string;
};
const legacyCondition = (value: any): any => {
  if (Array.isArray(value))
    return value.length === 1 ? value[0] : { in: value };
  if (isPlain(value) && Array.isArray(value.ne)) {
    const { ne, ...rest } = value;
    return { ...rest, ...(ne.length === 1 ? { ne: ne[0] } : { nin: ne }) };
  }
  return value;
};
const legacyFilter = (filter: Record<string, any>): Filter =>
  Object.fromEntries(
    Object.entries(filter).map(([key, value]) =>
      key === "$oneOf"
        ? ["oneOf", value.map(legacyFilter)]
        : // The old documents stored their id as `id`; the store keys them by `_id`.
          [key === "id" ? "_id" : key, legacyCondition(value)],
    ),
  );
const direction = (value: unknown): 1 | -1 => {
  if (value === 1 || value === "1" || value === "asc") return 1;
  if (value === -1 || value === "-1" || value === "desc") return -1;
  throw Error(`Unknown old sort direction ${JSON.stringify(value)}`);
};

/**
 * The old QueryBuilder envelope `{ filter, sort, from, size, q }` as `find(filter, options)` arguments:
 * an array value is membership, `ne` with an array is `nin`, `$oneOf` is `oneOf`, a field whose value is
 * `undefined` sets no condition (the old translation skipped it), sort records take string directions, an
 * omitted size is the old default of 100 and size 0 is unlimited.
 */
export const legacyQuery = ({
  filter = {},
  sort,
  from,
  size = 100,
  q,
}: LegacyQuery = {}): [Filter, QueryOptions] => [
  legacyFilter(
    Object.fromEntries(
      Object.entries(filter).filter(([, value]) => value !== undefined),
    ),
  ),
  {
    ...(sort && {
      sort: Object.fromEntries(
        (Array.isArray(sort) ? sort : [sort]).flatMap((record) =>
          Object.entries(record).map(([key, value]) => [
            key === "id" ? "_id" : key,
            direction(value),
          ]),
        ),
      ),
    }),
    ...(from !== undefined && { from }),
    ...(size !== 0 && { size }),
    ...(q !== undefined && { q }),
  },
];

/** The old id read resolved to an array: the document, or empty when absent. */
export const legacyOne = async (
  read: Promise<{ document: JsonObject | null }>,
): Promise<JsonObject[]> => {
  const { document } = await read;
  return document ? [document] : [];
};

/** The old `.stream()` was a Node object stream; consumers using `.pipe` or `.on('data')` keep working. */
export const legacyStream = <T>(documents: AsyncIterable<T>): Readable =>
  Readable.from(documents);

/** An old Readmodel `query` entry as an Identity function: it returns the old filter in portable form. */
export const legacyQueryIdentity =
  <A extends unknown[]>(query: (...args: A) => any) =>
  async (...args: A): Promise<Filter> => {
    const filter = await query(...args);
    return isPlain(filter) ? legacyFilter(filter) : filter;
  };

const ranges = ["gt", "gte", "lt", "lte"];
const listCondition = (value: unknown): unknown => {
  // Membership: several values are `in`, one is equality, none drops the field.
  if (Array.isArray(value))
    return value.length > 1
      ? { in: value }
      : value.length
        ? value[0]
        : undefined;
  if (!isPlain(value)) return value;
  // A range wins over `ne`, as the old translator checked it first.
  const range = Object.entries(value).filter(([key]) => ranges.includes(key));
  if (range.length) return Object.fromEntries(range);
  if ("ne" in value) {
    const ne = [value.ne].flat();
    return ne.length > 1 ? { nin: ne } : ne.length ? { ne: ne[0] } : undefined;
  }
  // Any other object was an equality against the object itself.
  return { eq: value };
};
const listFilter = (fields: Record<string, unknown>): Filter =>
  Object.fromEntries(
    Object.entries(fields).flatMap(([key, value]) => {
      if (key === "$oneOf" && Array.isArray(value))
        return [
          [
            "oneOf",
            value.map((group) => listFilter(isPlain(group) ? group : {})),
          ],
        ];
      const condition = listCondition(value);
      return condition === undefined
        ? []
        : [[key === "id" ? "_id" : key, condition]];
    }),
  );
const badRequest = () => new Rejection("validation", "Bad Request");
// The old parser: base-10 parseInt, so "12rows" is 12 and ["2", "6"] is 2.
const integer = (value: unknown): number => {
  const parsed = Number.parseInt(String(value), 10);
  // Negative paging is refused; the old parser passed it to Mongo.
  if (!Number.isInteger(parsed) || parsed < 0) throw badRequest();
  return parsed;
};

/**
 * The old list query object as `find(filter, options)` arguments. `from` (0), `size` (100; 0 is unlimited),
 * `sort` (`-a,+b,c`), `q` and `aggs` (ignored) are reserved; every other key filters. A string value is
 * comma-split membership, a `.gt`/`.gte`/`.lt`/`.lte` suffix is a range, `{ ne }` is a negation and
 * `$oneOf` alternatives. Values stay strings: nothing is coerced.
 */
export const legacyListQuery = (
  input: Record<string, unknown>,
): [Filter, QueryOptions] => {
  const {
    from = 0,
    size = 100,
    sort = "",
    q = null,
    aggs: _aggs,
    ...rest
  } = input;
  const fields: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(rest)) {
    const [, field, op] = /^(.+)\.(gt|gte|lt|lte)$/.exec(key) ?? [];
    if (field && op)
      fields[field] = {
        ...(isPlain(fields[field]) ? fields[field] : {}),
        [op]: value,
      };
    else
      fields[key] =
        typeof value === "string"
          ? value.split(",").map((part) => part.trim())
          : value;
  }
  const tokens =
    typeof sort === "string"
      ? sort.split(",").map((token) => token.trim())
      : Array.isArray(sort)
        ? sort
        : [];
  if (tokens.some((token) => typeof token !== "string")) throw badRequest();
  const order: Record<string, 1 | -1> = {};
  for (const token of tokens as string[]) {
    const field = token.replace(/^[-+]/, "");
    // Last direction wins for a repeated field, as the old record did; empty names are dropped.
    if (field)
      order[field === "id" ? "_id" : field] = token.startsWith("-") ? -1 : 1;
  }
  if (q != null && typeof q !== "string") throw badRequest();
  const skip = integer(from),
    limit = integer(size);
  return [
    listFilter(fields),
    {
      ...(Object.keys(order).length && { sort: order }),
      ...(skip && { from: skip }),
      ...(limit !== 0 && { size: limit }),
      ...(q && { q }),
    },
  ];
};

/**
 * The old Gateway list route: the request's query string (parsed as the old bracket syntax), overridden
 * key by key by the route's `query`, answered as `{ total, data }`. `total` counts every matching document
 * the caller may read, before paging and transforms. Transforms run in order on each document with the
 * request handle; one returning null or undefined drops the document.
 */
export const legacyList =
  <G extends ReadmodelGroupName, M extends ReadmodelModelName<G>>(
    group: G,
    model: M,
    options: NoInfer<{
      query?: Record<string, unknown>;
      transforms?: ((
        document: ReadmodelDocuments[`${G}.${M}`],
        request: ReturnType<typeof handle>,
      ) => unknown)[];
      waitTimeout?: number;
    }> = {},
  ): RequestHandler =>
  async (req: Request, res) => {
    const request = handle(req);
    const { total, documents } = await legacyListPage(
      req,
      res,
      group,
      model,
      options.query,
      options.waitTimeout,
    );
    const data: unknown[] = [];
    for (let document of documents) {
      for (const transform of options.transforms ?? []) {
        document = await transform(document, request);
        if (document == null) break;
      }
      if (document != null) data.push(document);
    }
    res.json({ total, data });
  };

export {
  legacyGateway,
  legacyRest,
  legacyPath,
  legacyServices,
} from "./legacy-gateway.js";
export type {
  LegacyGatewayOptions,
  LegacyRequest,
  LegacyRequestApp,
} from "./legacy-gateway.js";
export {
  legacySocket,
  legacyEvent,
  legacyChange,
  legacyNotification,
  partOf,
} from "./legacy-socket.js";
export type { LegacySocketOptions, LegacySocketSetup } from "./legacy-socket.js";
export {
  legacyApplication,
  legacyOrchestration,
  legacyEffects,
  legacyScheduler,
  legacyCommandOf,
  LEGACY_ORCHESTRATION_MESSAGE,
  LEGACY_SCHEDULER,
  LEGACY_EFFECTS_STATE,
} from "./legacy-application.js";
export type {
  LegacyApplicationOptions,
  LegacyOrchestrationOptions,
  LegacyEffectsOptions,
} from "./legacy-application.js";
