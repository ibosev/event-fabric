import { expect, test } from "vitest";
import { copyDocument, matches } from "../src/readmodel-store/query.js";
import {
  normalizeDocument,
  nullable,
  resolveSchema,
  schemaType,
  type StructuralSchema,
} from "../src/readmodel-store/schema.js";

// The walker `normalizeDocument` once used, as the oracle for the generated normalizer.
function walk(
  schema: StructuralSchema,
  document: Record<string, any>,
  checkRequired: boolean,
) {
  function normalize(s: any, v: any, path: string, required = false): any {
    const json = s["x-store"] === "json",
      type = schemaType(s);
    if (checkRequired && v == null && required && !nullable(s) && !json)
      throw new Error(
        `Invalid Readmodel document: ${path.slice(1)} is required`,
      );
    if (v === undefined) return null;
    if (json) return structuredClone(v);
    if (v === null) return null;
    if (type === "object") {
      if (typeof v !== "object" || Array.isArray(v) || v instanceof Date)
        throw new Error(`Readmodel ${path} must be an object`);
      for (const key of Object.keys(v))
        if (!Object.hasOwn(s.properties, key))
          throw new Error(`Undeclared Readmodel path ${path}.${key}`);
      const result: Record<string, any> = {};
      const required = new Set(s.required ?? []);
      for (const [key, child] of Object.entries(s.properties)) {
        const value = normalize(
          child,
          v[key],
          `${path}.${key}`,
          required.has(key),
        );
        // A key the document lacks stays absent.
        if (key in v)
          Object.defineProperty(result, key, {
            value,
            enumerable: true,
            writable: true,
            configurable: true,
          });
      }
      return result;
    }
    if (type === "array") {
      if (!Array.isArray(v))
        throw new Error(`Readmodel ${path} must be an array`);
      return v.map((x, i) => normalize(s.items, x, `${path}.${i}`, true));
    }
    if (type === "integer" && !Number.isSafeInteger(v))
      throw new Error(`Readmodel ${path} requires a safe integer`);
    if (s.format === "date-time" && type === "string") {
      const date = new Date(v);
      if (Number.isNaN(date.valueOf()))
        throw new Error(`Invalid date at ${path}`);
      return date;
    }
    return v;
  }
  const input = { ...document };
  delete input._id;
  return normalize(schema, input, "");
}

const schemas: Record<string, any> = {
  wide: {
    type: "object",
    required: ["name", "count"],
    properties: {
      name: { type: "string" },
      count: { type: "integer" },
      ratio: { type: ["number", "null"] },
      when: { type: ["string", "null"], format: "date-time" },
      dates: { type: "array", items: { type: "string", format: "date-time" } },
      owner: {
        type: ["object", "null"],
        required: ["account"],
        properties: { account: { type: "string" }, file: { type: "string" } },
      },
      items: {
        type: "array",
        items: {
          type: "object",
          required: ["sku"],
          properties: {
            sku: { type: "string" },
            cost: { type: "number" },
            parts: {
              type: "array",
              items: { type: "object", properties: { n: { type: "integer" } } },
            },
          },
        },
      },
      tags: { type: "array", items: { type: ["string", "null"] } },
      extra: { "x-store": "json", type: "object" },
      flag: { type: "boolean" },
    },
  },
  shallow: {
    type: "object",
    properties: {
      a: { type: "string" },
      b: { anyOf: [{ type: "integer" }, { type: "null" }] },
    },
  },
  // The walker reads an absent `toString` through the prototype; such a schema keeps the walker.
  inheritedKey: {
    type: "object",
    properties: { toString: { type: "string" }, a: { type: "string" } },
  },
};

let seed = 5;
const random = () => (seed = (seed * 1103515245 + 12345) % 2 ** 31) / 2 ** 31;
const pick = <T>(items: T[]): T => items[Math.floor(random() * items.length)]!;
class Instance {
  name = "x";
  get count() {
    return 3;
  }
}
const scalar = () =>
  pick<any>([
    "s",
    "",
    "2026-10-21T10:00:00.000Z",
    "not a date",
    0,
    1.5,
    7,
    2 ** 60,
    true,
    null,
    undefined,
    new Date("2026-10-21T00:00:00.000Z"),
    new Date(NaN),
    [],
    {},
  ]);
const value = (schema: any, depth = 0): any => {
  if (random() < 0.12 || depth > 3) return scalar();
  const type = schemaType(schema);
  if (schema["x-store"] === "json")
    return pick([{ any: [1, { b: new Date(0) }] }, null, undefined, "text"]);
  if (type === "object") return object(schema, depth + 1);
  if (type === "array")
    return Array.from({ length: Math.floor(random() * 3) }, () =>
      value(schema.items, depth + 1),
    );
  if (type === "integer") return pick([1, 2, -4, 1.5, 2 ** 60, null]);
  if (schema.format === "date-time")
    return pick([
      "2026-10-21T10:00:00.000Z",
      new Date("2026-01-01T00:00:00.000Z"),
      "nope",
      86400000,
      null,
    ]);
  return scalar();
};
const object = (schema: any, depth: number): any => {
  const result: Record<string, any> =
    random() < 0.05 ? Object.create(null) : {};
  for (const key of Object.keys(schema.properties ?? {}))
    if (random() < 0.7) result[key] = value(schema.properties[key], depth);
  if (random() < 0.05) result.undeclared = 1;
  if (depth === 1 && random() < 0.3) result._id = pick(["doc", "other"]);
  return result;
};
const documents = (schema: any) => [
  ...Array.from({ length: 1500 }, () => object(schema, 1)),
  new Instance(),
  JSON.parse('{"__proto__": {"a": 1}, "name": "p"}'),
  { name: "n", count: 1, owner: new Instance() },
  // A nested object whose declared key is a getter on its prototype, which the walker reads.
  {
    name: "n",
    count: 1,
    owner: Object.assign(
      Object.create({
        get file() {
          return "f";
        },
      }),
      { account: "a" },
    ),
  },
];

// Key order and Date values, which `toStrictEqual` alone does not compare.
const shape = (value: unknown): string =>
  JSON.stringify(value, function (key, v) {
    return this[key] instanceof Date
      ? `Date(${(this[key] as Date).valueOf()})`
      : v === undefined
        ? "undefined"
        : v;
  });

test("the generated normalizer gives the walker's documents and errors", async () => {
  for (const [name, raw] of Object.entries(schemas)) {
    const schema = resolveSchema(raw);
    for (const document of documents(raw))
      for (const checkRequired of [true, false]) {
        const id = typeof document._id === "string" ? document._id : "doc";
        let expected: unknown, actual: unknown;
        try {
          expected = {
            result: { _id: id, ...walk(schema, document, checkRequired) },
          };
        } catch (error) {
          expected = { error: (error as Error).message };
        }
        try {
          actual = {
            result: await normalizeDocument(
              schema,
              document,
              id,
              false,
              undefined,
              checkRequired,
            ),
          };
        } catch (error) {
          actual = { error: (error as Error).message };
        }
        expect(shape(actual), name).toBe(shape(expected));
        expect(actual).toStrictEqual(expected);
      }
  }
});

// The validation input `normalizeDocument` once built, as the oracle for the generated JSON form.
function validationJson(schema: any, document: Record<string, any>) {
  function input(s: any, value: any): any {
    if (value == null || s["x-store"] === "json") return value;
    const type = schemaType(s);
    if (type === "array") return value.map((v: any) => input(s.items, v));
    if (type !== "object") return value;
    const result: Record<string, any> = {};
    for (const key of Object.keys(value)) {
      const child = s.properties[key];
      if (
        value[key] !== null ||
        s.required?.includes(key) ||
        child?.type?.includes?.("null")
      )
        Object.defineProperty(result, key, {
          value: input(child, value[key]),
          enumerable: true,
          writable: true,
          configurable: true,
        });
    }
    return result;
  }
  const { _id, ...fields } = document;
  return JSON.parse(
    JSON.stringify(
      input(schema, Object.hasOwn(document, "_id") ? fields : document),
    ),
  );
}

test("the generated validation input is the copy and JSON round trip it replaces", async () => {
  const odd = [
    NaN,
    -Infinity,
    () => 1,
    Symbol("s"),
    10n,
    [1, , undefined, () => 1, new Date(NaN)],
    { toJSON: (key: string) => `json ${key}` },
    Object.assign(Object.create(null), { a: undefined, b: new Date(0) }),
    new Map([[1, 2]]),
  ];
  const schema = resolveSchema({
    type: "object",
    required: ["name"],
    properties: {
      ...schemas.wide.properties,
      nested: { type: "array", items: { type: ["number", "null"] } },
    },
  });
  let checked = 0;
  for (const base of documents(schema)) {
    const variants = [base];
    for (const key of ["extra", "ratio", "name", "flag", "nested"])
      variants.push({ ...base, [key]: odd[Math.floor(random() * odd.length)] });
    variants.push({ ...base, nested: [1, NaN, , null, undefined, 2] });
    for (const document of variants) {
      let seen: unknown;
      const port = async (_schema: unknown, value: unknown) => {
        seen = structuredClone(value);
      };
      let expected: unknown, actual: unknown;
      try {
        walk(schema, document, true);
        expected = { input: validationJson(schema, document) };
      } catch (error) {
        expected = { error: (error as Error).message };
      }
      try {
        const id = typeof document._id === "string" ? document._id : "doc";
        await normalizeDocument(schema, document, id, true, port);
        actual = { input: seen };
        checked++;
      } catch (error) {
        actual = { error: (error as Error).message };
      }
      expect(shape(actual)).toBe(shape(expected));
      expect(actual).toStrictEqual(expected);
    }
  }
  expect(checked).toBeGreaterThan(400);
});

test("copyDocument copies as structuredClone does, and falls back to it for other values", () => {
  class Point {
    x = 1;
  }
  const cycle: any = { a: 1 };
  cycle.self = cycle;
  let deep: any = { leaf: true };
  for (let i = 0; i < 500; i++) deep = { deep };
  const values: unknown[] = [
    { a: 1, b: "s", c: null, d: [1, [2, { e: new Date(5) }]], f: undefined },
    Object.assign(Object.create(null), { a: { b: 1 } }),
    JSON.parse('{"__proto__": {"polluted": true}, "x": [1]}'),
    [1, , 3],
    { map: new Map([[1, { a: 1 }]]), set: new Set([1]) },
    { point: new Point() },
    { when: new Date(NaN) },
    { big: 10n, nested: { list: ["a", null, false, 0, -0] } },
    cycle,
    deep,
    "text",
    null,
  ];
  for (const value of values) {
    const copied = copyDocument(value);
    expect(copied).toStrictEqual(structuredClone(value));
    if (value && typeof value === "object") expect(copied).not.toBe(value);
  }
  // The copy is independent of the original.
  const original = { list: [{ a: 1 }], when: new Date(1) };
  const copied = copyDocument(original);
  copied.list[0]!.a = 2;
  copied.when.setTime(2);
  expect(original).toEqual({ list: [{ a: 1 }], when: new Date(1) });
  expect(({} as any).polluted).toBeUndefined();
  // Values structuredClone refuses are refused the same way.
  expect(() => copyDocument({ f: () => 1 })).toThrow(
    expect.objectContaining({ name: "DataCloneError" }),
  );
});

test('"x-undeclared": "drop" drops undeclared keys below it instead of refusing the document', async () => {
  const schema = resolveSchema({
    type: "object",
    "x-undeclared": "drop",
    properties: {
      amount: {
        type: "object",
        properties: { value: { type: "number" } },
      },
      items: {
        type: "array",
        items: { type: "object", properties: { sku: { type: "string" } } },
      },
      strict: {
        type: ["object", "null"],
        "x-undeclared": "reject",
        properties: { a: { type: "string" } },
      },
    },
  });
  const document = {
    amount: { value: 1, 0: "x" },
    items: [{ sku: "a", note: "n" }],
    extra: true,
  };
  expect(await normalizeDocument(schema, document, "d")).toEqual({
    _id: "d",
    amount: { value: 1 },
    items: [{ sku: "a" }],
  });
  await expect(
    normalizeDocument(schema, { strict: { a: "x", b: "y" } }, "d"),
  ).rejects.toThrow("Undeclared Readmodel path .strict.b");
  await expect(
    normalizeDocument(
      resolveSchema({ type: "object", properties: { a: { type: "string" } } }),
      { b: 1 },
      "d",
    ),
  ).rejects.toThrow("Undeclared Readmodel path .b");
  expect(() =>
    resolveSchema({ type: "object", "x-undeclared": "keep", properties: {} }),
  ).toThrow('x-undeclared must be "drop" or "reject"');
});

test("an absent field stays absent and an explicit null stays null; both match a null filter", async () => {
  const schema = resolveSchema({
    type: "object",
    properties: {
      zip: { type: "string" },
      note: { type: ["string", "null"] },
      address: {
        type: "object",
        properties: { type: { type: "string" }, zip: { type: "string" } },
      },
    },
  });
  const document = await normalizeDocument(
    schema,
    { note: null, address: { zip: "1000", type: undefined } },
    "d",
  );
  // A key held as `undefined` is `null`, as the Mongo driver writes it.
  expect(document).toStrictEqual({
    _id: "d",
    note: null,
    address: { zip: "1000", type: null },
  });
  expect(matches(document, { zip: null, note: null })).toBe(true);
  expect(matches(document, { "address.type": { in: [null] } })).toBe(true);
  expect(matches(document, { zip: { ne: null } })).toBe(false);
});
