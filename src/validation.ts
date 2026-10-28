import { Ajv } from "ajv";
import type { AnySchema, AnySchemaObject, Format, Options } from "ajv";
import addFormatsModule from "ajv-formats";
import addKeywordsModule from "ajv-keywords";
import { readdir, readFile } from "node:fs/promises";
import { join, relative, sep } from "node:path";
import { pathToFileURL } from "node:url";
import type { Metadata, Schema, Validate } from "./types.js";
import { copiesInput } from "./internal.js";

export type ValidationIssue = {
  path: string;
  keyword: string;
  params: object;
  message?: string;
};
export class ValidationError extends Error {
  override name = "ValidationError";
  constructor(public details: ValidationIssue[]) {
    super(
      details
        .map(
          (issue) => `${issue.path || "/"} ${issue.message ?? issue.keyword}`,
        )
        .join("; "),
    );
  }
}
export type ValidatorOptions = {
  schemas?: Record<string, Schema>;
  formats?: Record<string, Format>;
  loadSchema?: (name: string) => Promise<Record<string, any>>;
  mutate?: boolean;
  ajv?: Options;
  schemasDir?: string;
  formatsDir?: string;
};
/** Commands may receive defaults/trim; store callers use mutate:false. */
export const createValidator = (options: ValidatorOptions = {}): Validate => {
  const normalize = (name: string) =>
    name.startsWith("/") || /^[a-z]+:/i.test(name) ? name : `/${name}`;
  const ajv = new Ajv({
    strict: false,
    allErrors: true,
    multipleOfPrecision: 4,
    useDefaults: options.mutate !== false,
    loadSchema: options.loadSchema,
    ...options.ajv,
  });
  // With strict off Ajv only warns about an unknown format and then skips the check;
  // a misspelt or unregistered format must fail the schema instead.
  const logger = ajv.logger;
  (ajv as { logger: typeof logger }).logger = {
    ...logger,
    warn(message: unknown, ...rest: unknown[]) {
      if (String(message).startsWith("unknown format"))
        throw Error(String(message));
      logger.warn(message, ...rest);
    },
  };
  const addFormats = addFormatsModule as unknown as (ajv: Ajv) => void;
  const addKeywords = addKeywordsModule as unknown as (
    ajv: Ajv,
    keywords?: string[],
  ) => void;
  addFormats(ajv);
  addKeywords(ajv, [
    "uniqueItemProperties",
    "allRequired",
    "anyRequired",
    "oneRequired",
    "patternRequired",
    "prohibited",
    "deepProperties",
    "deepRequired",
  ]);
  if (options.mutate !== false)
    addKeywords(ajv, ["transform", "dynamicDefaults"]);
  else {
    ajv.addKeyword({ keyword: "transform", schemaType: "array", valid: true });
    ajv.addKeyword({
      keyword: "dynamicDefaults",
      schemaType: "object",
      valid: true,
    });
  }
  for (const [name, format] of Object.entries(options.formats ?? {}))
    ajv.addFormat(name, format);
  for (const [name, schema] of Object.entries(options.schemas ?? {}))
    ajv.addSchema(schema as AnySchema, normalize(name));
  const ready = (async () => {
    if (options.formatsDir)
      for (const [name, format] of Object.entries(
        await loadNamedFormats(options.formatsDir),
      ))
        ajv.addFormat(name, format);
    if (options.schemasDir)
      for (const [name, schema] of Object.entries(
        await loadNamedSchemas(options.schemasDir),
      ))
        ajv.addSchema(schema as AnySchema, name);
  })();
  const compiled = new Map<string, ReturnType<Ajv["compile"]>>(),
    byObject = new WeakMap<object, string>();
  const check = async (
    schema: Parameters<Validate>[0],
    value: unknown,
    copy: boolean,
  ) => {
    await ready;
    if (typeof schema === "string") schema = normalize(schema);
    let validator;
    try {
      if (typeof schema === "string") {
        validator = ajv.getSchema(schema);
        if (!validator) {
          if (!options.loadSchema) throw Error(`Unknown schema ${schema}`);
          await ajv.compileAsync({ $async: true, $ref: schema });
          validator = ajv.getSchema(schema);
        }
      } else {
        // Ajv caches by schema object, so a schema re-read per call (a stored Readmodel
        // generation) would compile again and stay cached forever; cache by content.
        const object =
          schema && typeof schema === "object" ? schema : undefined;
        const key = (object && byObject.get(object)) || canonical(schema);
        if (object) byObject.set(object, key);
        validator = compiled.get(key);
        if (!validator) {
          try {
            validator = ajv.compile(schema as AnySchema);
          } catch (error) {
            if (
              !options.loadSchema ||
              (error as any).name !== "MissingRefError"
            )
              throw error;
            validator = await ajv.compileAsync(schema as AnySchemaObject);
          }
          compiled.set(key, validator);
        }
      }
      if (!validator) throw Error(`Unknown schema ${schema}`);
      const result = await validator(copy ? structuredClone(value) : value);
      if (result === false) throw { errors: validator.errors };
    } catch (error) {
      if (!(error as any)?.errors) throw error;
      throw new ValidationError(
        (error as any).errors.map((issue: any) => ({
          path: issuePath(issue),
          keyword: issue.keyword,
          params: issue.params,
          message: issue.message,
        })),
      );
    }
  };
  const validate: Validate = (schema, value) =>
    check(schema, value, options.mutate === false);
  if (options.mutate === false)
    copiesInput.set(validate, (schema, value) => check(schema, value, false));
  return validate;
};
/** Names follow /folder/file-before-first-dot; duplicates are refused. */
export const loadNamedSchemas = async (
  folder: string,
): Promise<Record<string, Schema>> => {
  const schemas: Record<string, Schema> = {};
  const visit = async (directory: string): Promise<void> => {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) await visit(path);
      else if (entry.isFile() && /\.(json5|ya?ml)$/.test(entry.name))
        throw Error(
          `Schema ${relative(folder, path)}: JSON5 and YAML are not loaded; convert it to JSON or JavaScript`,
        );
      else if (entry.isFile() && /\.(json|js|mjs|cjs)$/.test(entry.name)) {
        const parts = relative(folder, path).split(sep);
        parts[parts.length - 1] = entry.name.split(".")[0]!;
        const name = `/${parts.join("/")}`;
        if (Object.hasOwn(schemas, name))
          throw Error(`Duplicate schema ${name}`);
        schemas[name] = entry.name.endsWith(".json")
          ? JSON.parse(await readFile(path, "utf8"))
          : (await import(pathToFileURL(path).href)).default;
      }
    }
  };
  await visit(folder);
  return schemas;
};
/** Deep copy with JSON semantics: undefined properties drop, undefined array items become null. */
/** JSON with sorted keys and no undefined values: equal values give equal strings. */
export function canonical(value: any): string {
  const order = (v: any): any =>
    v && typeof v === "object"
      ? Array.isArray(v)
        ? v.map(order)
        : Object.fromEntries(
            Object.keys(v)
              .sort()
              .filter((k) => v[k] !== undefined)
              .map((k) => [k, order(v[k])]),
          )
      : v;
  return JSON.stringify(order(value));
}
export const toJson = <T>(value: T, label = "Value"): T => {
  let encoded: string | undefined;
  try {
    encoded = JSON.stringify(value, (_key, item) => {
      if (
        typeof item === "function" ||
        typeof item === "symbol" ||
        typeof item === "bigint" ||
        (typeof item === "number" && !Number.isFinite(item))
      )
        throw Error();
      return item;
    });
  } catch {
    throw Error(`${label} must contain only JSON values`);
  }
  if (encoded === undefined)
    throw Error(`${label} must contain only JSON values`);
  return JSON.parse(encoded);
};
export const assertMetadata = async (
  metadata: Metadata,
  options: { validate?: Validate; schema?: Schema; maxBytes?: number } = {},
): Promise<void> => {
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata))
    throw Error("Metadata must be a JSON object");
  const json = toJson(metadata, "Metadata");
  if (
    Buffer.byteLength(JSON.stringify(json), "utf8") > (options.maxBytes ?? 8192)
  )
    throw Error("Metadata exceeds size limit");
  if (options.schema !== undefined)
    await (options.validate ?? (metadataValidator ??= createValidator({ mutate: false })))(
      options.schema,
      json,
    );
};
// One for every caller without its own validator: each new one compiled the schema again.
let metadataValidator: Validate | undefined;

const issuePath = (issue: any): string => {
  const parts = (issue.instancePath ?? "")
    .split("/")
    .slice(1)
    .map((part: string) => part.replace(/~1/g, "/").replace(/~0/g, "~"));
  const property =
    issue.keyword === "required"
      ? issue.params.missingProperty
      : issue.keyword === "additionalProperties"
        ? issue.params.additionalProperty
        : undefined;
  if (property !== undefined) parts.push(property);
  return parts.join(".");
};
const loadNamedFormats = async (
  folder: string,
): Promise<Record<string, Format>> => {
  const formats: Record<string, Format> = {};
  for (const entry of await readdir(folder, { withFileTypes: true })) {
    if (!entry.isFile() || !/\.(js|mjs|cjs)$/.test(entry.name)) continue;
    const value = (await import(pathToFileURL(join(folder, entry.name)).href))
      .default;
    // A `{ name, options }` definition registers under its own name, as file-named ones do.
    const name =
      typeof value?.name === "string" && value.options !== undefined
        ? value.name
        : entry.name.split(".")[0]!;
    if (Object.hasOwn(formats, name)) throw Error(`Duplicate format ${name}`);
    formats[name] = value.options ?? value;
  }
  return formats;
};
/**
 * Rejects TypeBox `Refine` and `Codec` anywhere in a schema: they live in hidden properties
 * that Ajv never runs, so their checks would silently disappear. Express such a rule in
 * JSON Schema, or in a Step or Rule.
 */
export const assertPlainSchema = (schema: unknown, where: string): void => {
  const seen = new Set<object>();
  const visit = (node: unknown, path: string) => {
    if (!node || typeof node !== "object" || seen.has(node)) return;
    seen.add(node);
    for (const hidden of ["~refine", "~codec"])
      if (Object.hasOwn(node, hidden))
        throw new Error(
          `${where}${path}: TypeBox ${hidden === "~refine" ? "Refine" : "Codec"} is not JSON Schema, and validation would skip it; express the rule in JSON Schema or in a Step`,
        );
    for (const [key, child] of Object.entries(node))
      visit(child, `${path}/${key}`);
  };
  visit(schema, "");
};
