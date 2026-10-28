import { isDeepStrictEqual } from "node:util";
import { createHash } from "node:crypto";
import { canonical } from "../validation.js";
import { copiesInput } from "../internal.js";
import { copyDocument } from "./query.js";
import { Ajv } from "ajv";
import addFormats from "ajv-formats";
import type { JsonObject, Schema, Validate } from "../types.js";
import { assertPlainSchema } from "../validation.js";
import type { ReadmodelDefinitions, RegisterOptions } from "./types.js";
export type StructuralSchema = Record<string, any>;
const validatorAjv = new Ajv({ strict: false, allErrors: true });
(addFormats as unknown as (a: Ajv) => void)(validatorAjv);
validatorAjv.addKeyword("x-store");
validatorAjv.addKeyword("x-undeclared");
const validators = new Map<string, ReturnType<Ajv["compile"]>>();
const forbidden = [
  "oneOf",
  "anyOf",
  "allOf",
  "if",
  "then",
  "else",
  "not",
  "patternProperties",
];
export const schemaType = (schema: StructuralSchema): string =>
  Array.isArray(schema.type)
    ? schema.type.find((t: string) => t !== "null")
    : schema.type;
const isNull = (schema: unknown): boolean =>
  !!schema &&
  typeof schema === "object" &&
  ((schema as StructuralSchema).type === "null" ||
    (schema as StructuralSchema).const === null);
const valueType = (values: unknown[]): string | undefined => {
  const types = new Set(
    values.map((value) =>
      Number.isInteger(value) ? "integer" : value === null ? "null" : typeof value,
    ),
  );
  types.delete("null");
  if (types.size === 2 && types.has("integer") && types.has("number"))
    types.delete("integer");
  const [type] = types;
  return types.size === 1 && ["string", "number", "integer", "boolean"].includes(type!)
    ? type
    : undefined;
};
/**
 * Rewrites the shapes schema builders emit into the one-type form stores map to columns:
 * `anyOf: [X, { type: "null" }]` becomes X with `null` added to its type, an `anyOf` of
 * `const`s becomes an `enum`, and an `enum` without `type` gets its values' type.
 */
const normalize = (
  schema: StructuralSchema,
  schemas: Record<string, Schema>,
  seen: string[],
): StructuralSchema => {
  let s = schema;
  if (Array.isArray(s.anyOf) && !("type" in s)) {
    const { anyOf, ...rest } = s;
    const options = anyOf.filter((option: unknown) => !isNull(option)),
      withNull = options.length < anyOf.length;
    if (
      options.length &&
      options.every(
        (option: StructuralSchema) =>
          option &&
          "const" in option &&
          Object.keys(option).every((key) => key === "const" || key === "type"),
      )
    ) {
      const values = options.map((option: StructuralSchema) => option.const),
        type = valueType(values);
      if (type)
        s = {
          ...rest,
          type: withNull ? [type, "null"] : type,
          enum: withNull ? [...values, null] : values,
        };
    } else if (options.length === 1 && withNull) {
      const target = resolveSchema(options[0], schemas, seen),
        type = schemaType(target);
      if (typeof type === "string" && type !== "null")
        s = {
          ...target,
          ...rest,
          type: [type, "null"],
          ...(Array.isArray(target.enum) && !target.enum.includes(null)
            ? { enum: [...target.enum, null] }
            : {}),
        };
      // `const` excludes null, so it becomes a two-value enum to keep accepting null.
      if (typeof type === "string" && type !== "null" && "const" in target) {
        const { const: value, ...without } = s;
        s = { ...without, enum: [value, null] };
      }
    }
  }
  if (Array.isArray(s.enum) && s.type === undefined) {
    const type = valueType(s.enum);
    if (type) s = { ...s, type: s.enum.includes(null) ? [type, "null"] : type };
  }
  return s;
};
export function resolveSchema(
  schema: Schema,
  schemas: Record<string, Schema> = {},
  seen: string[] = [],
): StructuralSchema {
  if (typeof schema === "string")
    return resolveSchema({ $ref: schema }, schemas, seen);
  if (!schema || typeof schema !== "object")
    throw new Error("Readmodel requires a structural object schema");
  if (schema.$ref) {
    const ref = schema.$ref as string;
    if (seen.includes(ref)) throw new Error(`Cyclic Readmodel schema ${ref}`);
    const target = schemas[ref];
    if (!target) throw new Error(`Unknown Readmodel schema ${ref}`);
    return resolveSchema(target, schemas, [...seen, ref]);
  }
  // TypeBox's String reads back as a string, but stores turn date-times into Dates.
  if ((schema as StructuralSchema)["~kind"] === "String" && schema.format === "date-time")
    throw new Error(
      "Readmodel date-time fields read back as Date: declare them with DateTime() from event-fabric",
    );
  if (schema["x-store"] === "json") {
    const s = { ...schema };
    delete s.$id;
    return s;
  }
  const s = normalize({ ...schema }, schemas, seen);
  delete s.$id;
  for (const key of forbidden)
    if (key in s)
      throw new Error(`Readmodel schema ${key} requires x-store: json`);
  if (
    Array.isArray(s.type) &&
    s.type.filter((t: string) => t !== "null").length !== 1
  )
    throw new Error("Readmodel schema needs one non-null type");
  if (!["drop", "reject", undefined].includes(s["x-undeclared"]))
    throw new Error('Readmodel x-undeclared must be "drop" or "reject"');
  if (schemaType(s) === "object") {
    if (typeof s.additionalProperties === "object")
      throw new Error("Dynamic Readmodel properties require x-store: json");
    s.additionalProperties = false;
    s.properties = Object.fromEntries(
      Object.entries(s.properties ?? {}).map(([k, v]) => {
        if (
          k === "_id" ||
          ["__proto__", "constructor", "prototype"].includes(k)
        )
          throw new Error(`Reserved Readmodel property ${k}`);
        return [k, resolveSchema(v as Schema, schemas, seen)];
      }),
    );
  } else if (schemaType(s) === "array") {
    s.items = resolveSchema(s.items, schemas, seen);
    if (schemaType(s.items) === "array" && s.items["x-store"] !== "json")
      throw new Error("Readmodel arrays of arrays require x-store: json");
  } else if (
    !["string", "integer", "number", "boolean", "null"].includes(schemaType(s))
  )
    throw new Error("Readmodel schema needs a supported type");
  return s;
}
export const nullable = (schema: StructuralSchema): boolean =>
  Array.isArray(schema.type)
    ? schema.type.includes("null")
    : schema.type === "null";
/** The schema of a scalar outside arrays and json fields, which every store can order and compare by natively. */
export function scalarSchema(
  schema: StructuralSchema,
  path: string,
): StructuralSchema | undefined {
  if (path === "_id") return { type: "string" };
  let s: StructuralSchema | undefined = schema;
  for (const segment of path.split(".")) {
    if (schemaType(s) !== "object" || s["x-store"] === "json") return;
    s = s.properties?.[segment];
    if (!s) return;
  }
  return s["x-store"] !== "json" &&
    !["object", "array"].includes(schemaType(s))
    ? s
    : undefined;
}
/** Whether `path` names an `x-store: json` field through plain objects only. */
export function jsonField(schema: StructuralSchema, path: string): boolean {
  let s: StructuralSchema | undefined = schema;
  for (const segment of path.split(".")) {
    if (schemaType(s) !== "object" || s["x-store"] === "json") return false;
    s = s.properties?.[segment];
    if (!s) return false;
  }
  return s["x-store"] === "json";
}
export const scalarPath = (schema: StructuralSchema, path: string): boolean =>
  path !== "_id" && !!scalarSchema(schema, path);
/** Returns the array-of-objects prefix (relational child table) holding an index path. */
function indexTable(schema: StructuralSchema, path: string): string {
  if (path === "_id") return "";
  const segments = path.split(".");
  let s: StructuralSchema | undefined = schema,
    table = "";
  for (const [i, segment] of segments.entries()) {
    if (
      s["x-store"] === "json" ||
      (schemaType(s) === "array" && s.items["x-store"] === "json")
    )
      throw new Error(
        `Readmodel index ${path} points inside an x-store: json field`,
      );
    if (schemaType(s) === "array") {
      table = segments.slice(0, i).join(".");
      s = s.items;
    }
    s = schemaType(s!) === "object" ? s!.properties?.[segment] : undefined;
    if (!s) throw new Error(`Unknown Readmodel index path ${path}`);
  }
  if (
    s["x-store"] !== "json" &&
    (schemaType(s) === "object" ||
      (schemaType(s) === "array" && schemaType(s.items) === "object"))
  )
    throw new Error(
      `Readmodel index ${path} must name a scalar, an array of scalars or an x-store: json field`,
    );
  return table;
}
export function prepareDefinitions(
  definitions: ReadmodelDefinitions,
  options: RegisterOptions = {},
): { definitions: ReadmodelDefinitions; hash: string } {
  const prepared = Object.fromEntries(
    Object.entries(definitions).map(([name, definition]) => {
      assertPlainSchema(definition.schema, `Readmodel ${name}`);
      const schema = resolveSchema(definition.schema, options.schemas);
      if (schemaType(schema) !== "object" || schema["x-store"] === "json")
        throw new Error("Readmodel root schema must be an object");
      for (const index of definition.indexes ?? []) {
        const fields = Array.isArray(index.fields)
          ? index.fields
          : Object.keys(index.fields);
        const tables = new Set(fields.map((f) => indexTable(schema, f)));
        if (!index.text && tables.size > 1)
          throw new Error(
            `Readmodel index ${fields.join(",")} spans different arrays of objects`,
          );
      }
      return [name, { ...definition, schema }];
    }),
  );
  return {
    definitions: prepared,
    hash: createHash("sha256").update(canonical(prepared)).digest("hex"),
  };
}
/** What normalizing needs of one schema node, worked out once per node rather than per document. */
type SchemaNode = {
  type: string | undefined;
  json: boolean;
  nullable: boolean;
  entries: [string, StructuralSchema][];
  required: Set<string>;
  /** Properties whose `null` validation sees: required or declared nullable. */
  keepsNull: Set<string>;
};
const schemaNodes = new WeakMap<StructuralSchema, SchemaNode>();
/**
 * `"x-undeclared": "drop"` on an object schema drops keys it does not declare, in it and below, instead of
 * refusing the document; `"reject"` restores refusal below it. Each dropped path is reported once.
 */
const drops = (s: StructuralSchema, inherited: boolean) =>
  s["x-undeclared"] === undefined ? inherited : s["x-undeclared"] === "drop";
const reported = new Set<string>();
const dropped = (path: string) => {
  const at = path.replace(/\.\d+(?=\.|$)/g, "[]");
  if (reported.has(at)) return;
  reported.add(at);
  process.emitWarning(
    `Undeclared Readmodel path ${at} dropped`,
    "EventFabricWarning",
  );
};
// An own `__proto__` key, as `Object.fromEntries` makes, rather than a new prototype.
const assign = (target: Record<string, any>, key: string, value: unknown) => {
  if (key === "__proto__")
    Object.defineProperty(target, key, {
      value,
      enumerable: true,
      writable: true,
      configurable: true,
    });
  else target[key] = value;
};
const schemaNode = (s: StructuralSchema): SchemaNode => {
  let node = schemaNodes.get(s);
  if (!node) {
    const entries = Object.entries(s.properties ?? {}) as [
      string,
      StructuralSchema,
    ][];
    const required = new Set<string>(s.required ?? []);
    node = {
      type: schemaType(s),
      json: s["x-store"] === "json",
      nullable: nullable(s),
      entries,
      required,
      keepsNull: new Set(
        entries
          .filter(
            ([key, child]) =>
              required.has(key) || (child as any)?.type?.includes?.("null"),
          )
          .map(([key]) => key),
      ),
    };
    schemaNodes.set(s, node);
  }
  return node;
};
/*
 * A normalizer generated once per schema. It builds each object in declared order from
 * the keys a document has; a key it lacks stays absent, apart from `null`.
 * It gives up on anything unusual, and the walker
 * in `normalizeDocument` then runs instead, so results and error messages stay the walker's.
 */
type Normalize = (value: any, checkRequired: boolean) => any;
const giveUp = Symbol("normalize with the walker");
// Keys the walker reads through the prototype when a document lacks them; such schemas keep it.
const inherited = (key: string) => key in Object.prototype;
const compileNode = (
  s: StructuralSchema,
  // The root skips `_id`, which `normalizeDocument` sets itself.
  root = false,
  drop = false,
  path = "",
): Normalize | undefined => {
  const node = schemaNode(s);
  drop = drops(s, drop);
  if (node.json)
    return (v) => (v === undefined ? null : copyDocument(v));
  if (node.type === "object") {
    if (!s.properties || node.entries.some(([key]) => inherited(key))) return;
    const children = new Map<string, Normalize>(),
      required: string[] = [];
    for (const [key, child] of node.entries) {
      const normalize = compileNode(child, false, drop, `${path}.${key}`);
      if (!normalize) return;
      children.set(key, normalize);
      const childNode = schemaNode(child);
      if (node.required.has(key) && !childNode.nullable && !childNode.json)
        required.push(key);
    }
    return (v, checkRequired) => {
      if (v === undefined || v === null) return null;
      if (typeof v !== "object" || Array.isArray(v) || v instanceof Date)
        throw giveUp;
      // The walker reads declared keys through any other prototype, getters included.
      const prototype = Object.getPrototypeOf(v);
      if (prototype !== Object.prototype && prototype !== null) throw giveUp;
      for (const key of Object.keys(v)) {
        if ((root && key === "_id") || children.has(key)) continue;
        if (!drop) throw giveUp;
        dropped(`${path}.${key}`);
      }
      const result: Record<string, any> = {};
      for (const [key, normalize] of children)
        if (key in v) result[key] = normalize(v[key], checkRequired);
      if (checkRequired)
        for (const key of required) if (result[key] == null) throw giveUp;
      return result;
    };
  }
  if (node.type === "array") {
    const items = compileNode(s.items, false, drop, `${path}.0`),
      itemNode = schemaNode(s.items);
    if (!items) return;
    const required = !itemNode.nullable && !itemNode.json;
    return (v, checkRequired) => {
      if (v === undefined || v === null) return null;
      if (!Array.isArray(v)) throw giveUp;
      return v.map((x) => {
        if (checkRequired && required && x == null) throw giveUp;
        // An array has no absent elements: a hole is stored as `null`, as JSON writes it.
        return x === undefined ? null : items(x, checkRequired);
      });
    };
  }
  if (node.type === "integer")
    return (v) => {
      if (v === undefined || v === null) return null;
      if (!Number.isSafeInteger(v)) throw giveUp;
      return v;
    };
  if (s.format === "date-time" && node.type === "string")
    return (v) => {
      if (v === undefined || v === null) return null;
      const date = new Date(v);
      if (Number.isNaN(date.valueOf())) throw giveUp;
      return date;
    };
  return (v) => (v === undefined ? null : v);
};
/*
 * The JSON form validation sees, generated once per schema: the value of
 * `JSON.parse(JSON.stringify(validationInput(schema, document)))` in one pass, without the copy
 * and the round trip. It gives up on values other than JSON ones, Dates, plain objects and
 * arrays, and `normalizeDocument` then makes the copy and the round trip.
 */
type JsonForm = (value: any, key: string | number) => any;
// What `JSON.stringify` leaves out of an object and turns into `null` in an array.
const absent = Symbol("not in JSON");
const jsonScalar = (v: any): any => {
  if (typeof v === "string" || typeof v === "boolean") return v;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  if (v === undefined || typeof v === "function" || typeof v === "symbol")
    return absent;
  if (v === null) return null;
  if (Object.getPrototypeOf(v) === Date.prototype)
    return Number.isFinite(v.valueOf()) ? v.toISOString() : null;
  throw giveUp;
};
// Plain JSON objects and arrays in their JSON form; it gives up on `toJSON` and other objects.
const jsonTree = (v: any, depth: number): any => {
  if (
    v === null ||
    typeof v !== "object" ||
    Object.getPrototypeOf(v) === Date.prototype
  )
    return jsonScalar(v);
  const prototype = Object.getPrototypeOf(v);
  if (depth > 200 || typeof v.toJSON === "function") throw giveUp;
  if (prototype === Array.prototype) {
    const result = [];
    for (let i = 0; i < v.length; i++) {
      const json = jsonTree(v[i], depth + 1);
      result.push(json === absent ? null : json);
    }
    return result;
  }
  if (prototype !== Object.prototype && prototype !== null) throw giveUp;
  const result: Record<string, any> = {};
  for (const key of Object.keys(v)) {
    const json = jsonTree(v[key], depth + 1);
    if (json !== absent) assign(result, key, json);
  }
  return result;
};
// A value the validation input keeps as it is: JSON converts it, `toJSON` seeing its own key.
const jsonLeaf: JsonForm = (v, key) => {
  try {
    return jsonTree(v, 0);
  } catch (error) {
    if (error !== giveUp) throw error;
  }
  const parsed = JSON.parse(JSON.stringify({ [key]: v }));
  return Object.hasOwn(parsed, key) ? parsed[key] : absent;
};
const compileJsonForm = (
  s: StructuralSchema,
  root = false,
): JsonForm | undefined => {
  const node = schemaNode(s);
  if (node.json || (node.type !== "object" && node.type !== "array"))
    return jsonLeaf;
  if (node.type === "object") {
    if (!s.properties || node.entries.some(([key]) => inherited(key))) return;
    const children = new Map<string, JsonForm>();
    for (const [key, child] of node.entries) {
      const form = compileJsonForm(child);
      if (!form) return;
      children.set(key, form);
    }
    const { keepsNull } = node;
    return (v) => {
      if (v === null) return null;
      if (v === undefined) return absent;
      if (typeof v !== "object" || Array.isArray(v)) throw giveUp;
      const prototype = Object.getPrototypeOf(v);
      if (prototype !== Object.prototype && prototype !== null) throw giveUp;
      const result: Record<string, any> = {};
      for (const key of Object.keys(v)) {
        if (root && key === "_id") continue;
        const value = v[key];
        if (value === null && !keepsNull.has(key)) continue;
        const form = children.get(key);
        if (!form) throw giveUp;
        const json = form(value, key);
        if (json !== absent) assign(result, key, json);
      }
      return result;
    };
  }
  const items = s.items && compileJsonForm(s.items);
  if (!items) return;
  return (v) => {
    if (v === null) return null;
    if (v === undefined) return absent;
    if (!Array.isArray(v) || Object.getPrototypeOf(v) !== Array.prototype)
      throw giveUp;
    const result = [];
    // A hole reads as `undefined`, which JSON also turns into `null`.
    for (let i = 0; i < v.length; i++) {
      const json = items(v[i], i);
      result.push(json === absent ? null : json);
    }
    return result;
  };
};
// Each batch reads its generation again, so a compiled root is also kept by the schema's text.
const compiledRoots = <T>(
  compile: (schema: StructuralSchema) => T | undefined,
) => {
  const bySchema = new WeakMap<StructuralSchema, T | null>(),
    byText = new Map<string, T | null>();
  return (schema: StructuralSchema): T | null => {
    let root = bySchema.get(schema);
    if (root !== undefined) return root;
    let text: string | undefined;
    try {
      text = JSON.stringify(schema);
    } catch {}
    root = text === undefined ? undefined : byText.get(text);
    if (root === undefined) {
      root =
        schemaNode(schema).type === "object" &&
        !Object.hasOwn(schema.properties ?? {}, "_id")
          ? (compile(schema) ?? null)
          : null;
      if (text !== undefined) byText.set(text, root);
    }
    bySchema.set(schema, root);
    return root;
  };
};
const compiledRoot = compiledRoots((schema) => compileNode(schema, true)),
  compiledJsonForm = compiledRoots((schema) => compileJsonForm(schema, true));
export async function normalizeDocument(
  schema: StructuralSchema,
  document: JsonObject,
  id: string,
  validate = true,
  validationPort?: Validate,
  // Reads skip it so documents stored before the check still load.
  checkRequired = true,
): Promise<JsonObject> {
  if (!document || typeof document !== "object" || Array.isArray(document))
    throw new Error("Readmodel document must be an object");
  if (document._id !== undefined && document._id !== id)
    throw new Error("Readmodel document id differs from Identity");
  function normalize(
    s: StructuralSchema,
    v: any,
    path: string,
    required = false,
    drop = false,
  ): any {
    const node = schemaNode(s);
    drop = drops(s, drop);
    // Structural even without value validation: Postgres keeps these columns NOT NULL.
    if (checkRequired && v == null && required && !node.nullable && !node.json)
      throw new Error(
        `Invalid Readmodel document: ${path.slice(1)} is required`,
      );
    if (v === undefined) return null;
    if (node.json) return copyDocument(v);
    if (v === null) return null;
    if (node.type === "object") {
      if (typeof v !== "object" || Array.isArray(v) || v instanceof Date)
        throw new Error(`Readmodel ${path} must be an object`);
      for (const key of Object.keys(v))
        if (!Object.hasOwn(s.properties, key))
          if (drop) dropped(`${path}.${key}`);
          else throw new Error(`Undeclared Readmodel path ${path}.${key}`);
      const result: Record<string, any> = {};
      for (const [key, child] of node.entries) {
        const value = normalize(
          child,
          v[key],
          `${path}.${key}`,
          node.required.has(key),
          drop,
        );
        // A key the document lacks stays absent; one it holds as `undefined` is `null`, as BSON writes it.
        if (key in v) assign(result, key, value);
      }
      return result;
    }
    if (node.type === "array") {
      if (!Array.isArray(v))
        throw new Error(`Readmodel ${path} must be an array`);
      return v.map((x, i) =>
        normalize(s.items, x, `${path}.${i}`, true, drop),
      );
    }
    if (node.type === "integer" && !Number.isSafeInteger(v))
      throw new Error(`Readmodel ${path} requires a safe integer`);
    if (s.format === "date-time" && node.type === "string") {
      const date = v instanceof Date ? new Date(v) : new Date(v);
      if (Number.isNaN(date.valueOf()))
        throw new Error(`Invalid date at ${path}`);
      return date;
    }
    return v;
  }
  // The document without `_id`, the walker's input.
  const input = () => {
    const copy = { ...document };
    delete copy._id;
    return copy;
  };
  let normalized: Record<string, any> | undefined;
  const root = compiledRoot(schema);
  // On any failure the walker runs, so the error reported is the walker's.
  if (root)
    try {
      normalized = root(document, checkRequired);
    } catch {}
  normalized ??= normalize(schema, input(), "");
  if (validate) {
    function validationInput(s: StructuralSchema, value: any): any {
      const node = schemaNode(s);
      if (value == null || node.json) return value;
      if (node.type === "array")
        return value.map((v: any) => validationInput(s.items, v));
      if (node.type !== "object") return value;
      const result: Record<string, any> = {};
      for (const key of Object.keys(value))
        // A key normalization accepted without declaring it was dropped.
        if (!Object.hasOwn(s.properties ?? {}, key)) continue;
        else if (value[key] !== null || node.keepsNull.has(key))
          assign(result, key, validationInput(s.properties[key], value[key]));
      return result;
    }
    let json: any;
    const form = compiledJsonForm(schema);
    if (form)
      try {
        json = form(document, "");
      } catch {}
    json ??= JSON.parse(
      JSON.stringify(
        validationInput(
          schema,
          Object.hasOwn(document, "_id") ? withoutId(document) : document,
        ),
      ),
    );
    try {
      // `json` is this call's own copy, so a copying validator need not copy it again.
      if (validationPort && copiesInput.has(validationPort))
        await copiesInput.get(validationPort)!(schema, json);
      else if (validationPort) {
        const before = structuredClone(json);
        await validationPort(schema, json);
        if (!isDeepStrictEqual(before, json))
          throw new Error("Readmodel validation must not mutate data");
      } else {
        const key = canonical(schema);
        let validator = validators.get(key);
        if (!validator) {
          validator = validatorAjv.compile(schema);
          validators.set(key, validator);
        }
        if (!(await validator(json)))
          throw new Error(validatorAjv.errorsText(validator.errors));
      }
    } catch (error) {
      throw new Error(
        `Invalid Readmodel document: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
  return { _id: id, ...normalized };
}
// A document's fields without `_id`, built without `delete`, which leaves an object in dictionary mode.
const withoutId = (document: JsonObject) => {
  const { _id, ...fields } = document;
  return fields;
};
