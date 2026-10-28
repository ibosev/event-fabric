import { jsonField, scalarSchema, schemaType } from "./schema.js";
import type { JsonObject } from "../types.js";
import type { Filter, QueryOptions } from "./types.js";
export const pathValues = (value: any, path: string[]): any[] =>
  path.length === 0
    ? [value]
    : Array.isArray(value)
      ? value.flatMap((v) => pathValues(v, path))
      : value == null
        ? [undefined]
        : pathValues(value[path[0]!], path.slice(1));
// An absent field equals `null`, as in Mongo: stores keep absent fields absent.
const equal = (a: any, b: any): boolean =>
  a == null && b == null
    ? true
    : a instanceof Date || b instanceof Date
      ? new Date(a).valueOf() === new Date(b).valueOf()
      : a && b && typeof a === "object" && typeof b === "object"
        ? Array.isArray(a) || Array.isArray(b)
          ? Array.isArray(a) &&
            Array.isArray(b) &&
            a.length === b.length &&
            a.every((v, i) => equal(v, b[i]))
          : Object.keys(a).length === Object.keys(b).length &&
            Object.keys(a).every(
              (key) => Object.hasOwn(b, key) && equal(a[key], b[key]),
            )
        : a === b;
const contains = (a: any, b: any): boolean =>
  Array.isArray(a) && !Array.isArray(b)
    ? a.some((v) => equal(v, b))
    : equal(a, b);
const operators = new Set([
  "eq",
  "in",
  "ne",
  "nin",
  "gt",
  "gte",
  "lt",
  "lte",
  "oneOf",
  "exists",
  "elemMatch",
]);
// UTF-8 binary order agrees with Postgres C collation and Mongo simple collation.
const stringOrder = (a: string, b: string) =>
  Buffer.compare(Buffer.from(a), Buffer.from(b));
function rangeOrder(actual: any, expected: any): number {
  if (actual == null) return NaN;
  if (typeof actual === "string" && typeof expected === "string")
    return stringOrder(actual, expected);
  const bound = actual instanceof Date ? new Date(expected) : expected;
  return actual < bound ? -1 : actual > bound ? 1 : actual >= bound ? 0 : NaN;
}
function test(actual: any, condition: any): boolean {
  if (
    !condition ||
    typeof condition !== "object" ||
    Array.isArray(condition) ||
    condition instanceof Date
  )
    return contains(actual, condition);
  if (!Object.keys(condition).some((k) => operators.has(k.replace(/^\$/, ""))))
    return equal(actual, condition);
  return Object.entries(condition).every(([key, expected]) => {
    if (
      Array.isArray(actual) &&
      ["gt", "gte", "lt", "lte", "$gt", "$gte", "$lt", "$lte"].includes(key)
    )
      return actual.some((value) => test(value, { [key]: expected }));
    switch (key.replace(/^\$/, "")) {
      case "eq":
        return contains(actual, expected);
      case "in":
        return (expected as any[]).some((v) => contains(actual, v));
      case "oneOf":
        return (expected as any[]).some((v) => test(actual, v));
      case "ne":
        return !contains(actual, expected);
      case "nin":
        return !(expected as any[]).some((v) => contains(actual, v));
      case "gt":
        return rangeOrder(actual, expected) > 0;
      case "gte":
        return rangeOrder(actual, expected) >= 0;
      case "lt":
        return rangeOrder(actual, expected) < 0;
      case "lte":
        return rangeOrder(actual, expected) <= 0;
      case "exists":
        return expected
          ? actual !== undefined && actual !== null
          : actual === undefined || actual === null;
      case "elemMatch":
        return (
          Array.isArray(actual) &&
          actual.some((v) =>
            v && typeof v === "object"
              ? matches(v, expected as Filter)
              : test(v, expected),
          )
        );
      default:
        throw new Error(`Unknown Readmodel query operator ${key}`);
    }
  });
}
export function matches(document: JsonObject, filter: Filter = {}): boolean {
  return Object.entries(filter).every(([path, condition]) => {
    if (path === "oneOf" || path === "$or")
      return (condition as Filter[]).some((f) => matches(document, f));
    if (path === "and" || path === "$and")
      return (condition as Filter[]).every((f) => matches(document, f));
    const values = pathValues(document, path.split("."));
    const negative =
      condition &&
      typeof condition === "object" &&
      Object.keys(condition).some((k) =>
        ["ne", "nin", "$ne", "$nin"].includes(k),
      );
    return negative
      ? values.every((value) => test(value, condition))
      : values.some((value) => test(value, condition));
  });
}
// Mirrors the BSON type order so mixed-type json fields sort the same on every store.
const typeRank = (v: any): number =>
  v == null
    ? 0
    : typeof v === "number"
      ? 1
      : typeof v === "string"
        ? 2
        : typeof v === "boolean"
          ? 4
          : v instanceof Date
            ? 5
            : 3;
export function compareValues(x: any, y: any): number {
  const rank = typeRank(x) - typeRank(y);
  if (rank || x == null) return Math.sign(rank);
  if (typeof x === "string") return stringOrder(x, y);
  const [a, b] =
    x instanceof Date
      ? [x.getTime(), y.getTime()]
      : typeof x === "object"
        ? [JSON.stringify(x), JSON.stringify(y)]
        : [x, y];
  return a < b ? -1 : a > b ? 1 : 0;
}
/*
 * A copy of a document as `structuredClone` makes it, several times faster for what documents hold:
 * plain objects, arrays, Dates and scalars. Anything else, or nesting deep enough to be
 * a cycle, copies the whole value with `structuredClone`, so its results and errors stay the same.
 * Unlike it, a value referenced twice is copied twice.
 */
const cloneFallback = Symbol("structuredClone");
const copy = (value: any, depth: number): any => {
  if (typeof value !== "object" || value === null) {
    if (typeof value === "function" || typeof value === "symbol")
      throw cloneFallback;
    return value;
  }
  if (depth > 200) throw cloneFallback;
  if (Array.isArray(value)) {
    if (Object.getPrototypeOf(value) !== Array.prototype) throw cloneFallback;
    return value.map((item) => copy(item, depth + 1));
  }
  if (value instanceof Date) {
    if (Object.getPrototypeOf(value) !== Date.prototype) throw cloneFallback;
    return new Date(value.valueOf());
  }
  const prototype = Object.getPrototypeOf(value);
  if (prototype !== Object.prototype && prototype !== null) throw cloneFallback;
  const result: Record<string, any> = {};
  for (const key of Object.keys(value)) {
    const item = copy(value[key], depth + 1);
    if (key === "__proto__")
      Object.defineProperty(result, key, {
        value: item,
        enumerable: true,
        writable: true,
        configurable: true,
      });
    else result[key] = item;
  }
  return result;
};
export function copyDocument<T>(value: T): T {
  try {
    return copy(value, 0);
  } catch (error) {
    if (error !== cloneFallback) throw error;
    return structuredClone(value);
  }
}
export function queryDocuments(
  documents: JsonObject[],
  filter: Filter = {},
  options: QueryOptions = {},
  textFields: string[] = [],
): JsonObject[] {
  let result = documents.filter((d) => matches(d, filter));
  if (options.q) {
    if (!textFields.length)
      throw new Error("Readmodel has no declared text index");
    const words = options.q.toLowerCase().match(/-?"[^"]+"|\S+/g) ?? [];
    result = result.filter((d) => {
      const text = textFields
        .flatMap((p) => pathValues(d, p.split(".")))
        .flat(Infinity)
        .join(" ")
        .toLowerCase();
      return words.every((w) =>
        w.startsWith("-")
          ? !text.includes(w.slice(1).replaceAll('"', ""))
          : text.includes(w.replaceAll('"', "")),
      );
    });
  }
  if (options.sort)
    result.sort((a, b) => {
      for (const [path, direction] of Object.entries(options.sort!)) {
        const x = pathValues(a, path.split("."))[0],
          y = pathValues(b, path.split("."))[0];
        const order = compareValues(x, y);
        if (order) return order * direction;
      }
      return compareValues(String(a._id), String(b._id));
    });
  const from = options.from ?? 0,
    size = options.size ?? result.length;
  if (
    !Number.isInteger(from) ||
    from < 0 ||
    !Number.isInteger(size) ||
    size < 0
  )
    throw new Error("Readmodel paging requires non-negative integers");
  return copyDocument(result.slice(from, from + size));
}
// A string that is not a date: `matches` compares a stored Date with a date string, Mongo never does.
// shortcut: Date.parse also reads many id-like strings, which a json field holding a Date of that
// time would equal in `matches` only (never a wider native selection); type such fields if that matters.
const plainString = (v: unknown) =>
  typeof v === "string" && !v.includes("\0") && !/^\d{4}-\d{2}-\d{2}/.test(v);
const isCondition = (condition: unknown): condition is Record<string, unknown> =>
  !!condition &&
  typeof condition === "object" &&
  !Array.isArray(condition) &&
  !(condition instanceof Date);
/**
 * Whether the durable stores' native predicates select exactly what `matches` does, so they may
 * count in the database: comparisons of scalar fields outside arrays and json fields with
 * non-null values of the field's own type. Anything else is verified by the shared evaluator.
 * With `jsonEquality` (a store whose equality on a json field matches the value or an element of
 * its array, as `matches` does: Mongo), equality to strings that are not dates on json fields too.
 */
export function exactFilter(
  filter: Filter,
  schema: Record<string, any>,
  options: { jsonEquality?: boolean } = {},
): boolean {
  return Object.entries(filter).every(([path, condition]) => {
    if (["oneOf", "$or", "and", "$and"].includes(path))
      return (
        Array.isArray(condition) &&
        condition.every((f) => exactFilter(f, schema, options))
      );
    // An empty condition object selects everything natively, and nothing in `matches`.
    if (isCondition(condition) && !Object.keys(condition).length) return false;
    const s = scalarSchema(schema as any, path);
    if (!s)
      return (
        !!options.jsonEquality &&
        jsonField(schema as any, path) &&
        (isCondition(condition)
          ? Object.entries(condition).every(([key, value]) => {
              const op = key.replace(/^\$/, "");
              return op === "eq"
                ? plainString(value)
                : op === "in" && Array.isArray(value) && value.every(plainString);
            })
          : plainString(condition))
      );
    const type = schemaType(s);
    const fits = (v: unknown) =>
      s.format === "date-time"
        ? v instanceof Date && !Number.isNaN(v.valueOf())
        : type === "integer"
          ? Number.isSafeInteger(v)
          : type === "number"
            ? Number.isFinite(v)
            : type === "boolean"
              ? typeof v === "boolean"
              : type === "string" && typeof v === "string" && !v.includes("\0");
    if (
      !condition ||
      typeof condition !== "object" ||
      Array.isArray(condition) ||
      condition instanceof Date
    )
      return fits(condition);
    return Object.entries(condition).every(([key, value]) => {
      const op = key.replace(/^\$/, "");
      if (["eq", "ne", "gt", "gte", "lt", "lte"].includes(op))
        return fits(value);
      if (op === "in" || op === "nin")
        return Array.isArray(value) && value.every(fits);
      return op === "exists" && typeof value === "boolean";
    });
  });
}
export const textFields = (definition: {
  indexes?: { fields: string[] | Record<string, 1 | -1>; text?: boolean }[];
}): string[] =>
  definition.indexes
    ?.filter((i) => i.text)
    .flatMap((i) =>
      Array.isArray(i.fields) ? i.fields : Object.keys(i.fields),
    ) ?? [];
/** Native Mongo narrowing; the shared evaluator verifies every selected document. */
export function mongoSelector(
  filter: Filter,
  schema: Record<string, any>,
): Record<string, any> {
  const logical = (operator: "$and" | "$or", clauses: Record<string, any>[]) =>
    clauses.length
      ? { [operator]: clauses }
      : operator === "$and"
        ? {}
        : { $expr: false };
  function fieldSchema(path: string): any {
    let s = schema;
    for (const segment of path.split(".")) {
      while (s && schemaType(s) === "array") s = s.items;
      s = s?.properties?.[segment];
    }
    return s;
  }
  function term(path: string, value: any): any {
    const s = fieldSchema(path);
    const scalar = s && schemaType(s) === "array" ? s.items : s;
    const convert = (v: any): any =>
      Array.isArray(v)
        ? v.map(convert)
        : scalar?.format === "date-time" && v != null
          ? new Date(v)
          : v;
    if (
      Array.isArray(value) &&
      value.some(
        (item) => item && typeof item === "object" && !(item instanceof Date),
      )
    )
      return {};
    if (
      value === null ||
      typeof value !== "object" ||
      Array.isArray(value) ||
      value instanceof Date
    )
      return { [path]: { $eq: convert(value) } };
    const parts = Object.entries(value).map(([raw, v]) => {
      const op = raw.replace(/^\$/, "");
      if (op === "oneOf")
        return logical(
          "$or",
          (v as any[]).map((x) => term(path, x)),
        );
      if (op === "exists")
        return { [path]: v ? { $ne: null } : { $in: [null] } };
      if (op === "elemMatch")
        return s?.items && schemaType(s.items) === "object"
          ? { [path]: { $elemMatch: mongoSelector(v as Filter, s.items) } }
          : {};
      if (
        (op === "eq" || op === "in") &&
        (Array.isArray(v) ? v : [v]).some(
          (item) => item && typeof item === "object" && !(item instanceof Date),
        )
      )
        return {};
      if (["eq", "ne", "gt", "gte", "lt", "lte", "in", "nin"].includes(op))
        return {
          [path]: {
            [`$${op}`]: Array.isArray(v) ? v.map(convert) : convert(v),
          },
        };
      return null;
    });
    return parts.some((p) => p === null)
      ? {}
      : logical("$and", parts as Record<string, any>[]);
  }
  if (Object.keys(filter).length === 0) return {};
  return logical(
    "$and",
    Object.entries(filter).map(([path, value]) =>
      path === "oneOf" || path === "$or"
        ? logical(
            "$or",
            (value as Filter[]).map((f) => mongoSelector(f, schema)),
          )
        : path === "and" || path === "$and"
          ? logical(
              "$and",
              (value as Filter[]).map((f) => mongoSelector(f, schema)),
            )
          : term(path, value),
    ),
  );
}
