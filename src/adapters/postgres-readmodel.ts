import { setTimeout } from "node:timers/promises";
import { Pool } from "pg";
import type { PoolClient, PoolConfig, QueryResult } from "pg";
import type { JsonObject, PoolStats } from "../types.js";
import {
  definitionsByHash,
  durableReadmodelStore,
  generationTag,
  readableName,
  shortHash,
} from "../readmodel-store/durable.js";
import type {
  ReadmodelTransaction,
  StoredGeneration,
} from "../readmodel-store/durable.js";
import { nullable, schemaType } from "../readmodel-store/schema.js";
import type { StructuralSchema } from "../readmodel-store/schema.js";
import type {
  Filter,
  QueryOptions,
  ReadmodelStore,
} from "../readmodel-store/types.js";
export interface PostgresReadmodelNative {
  schema: string;
  table: string;
  tableFor(model: string): string;
  query(text: string, values?: any[]): Promise<QueryResult>;
  pool: Pool;
}
export interface PostgresReadmodelStore extends ReadmodelStore<PostgresReadmodelNative> {
  native(group: string, model: string): Promise<PostgresReadmodelNative>;
  poolStats(): PoolStats;
}
export type PostgresReadmodelOptions = {
  pool?: Pool;
  connectionString?: string;
  schema?: string;
  poolOptions?: PoolConfig;
};
const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
/** Postgres truncates identifiers past 63 bytes; longer derived names end in a hash instead. */
const identifier = (name: string) =>
  name.length > 63 ? `${name.slice(0, 54)}_${shortHash(name)}` : name;
const unsafe = /[^a-z0-9_]/g;
/** Strings compare in binary order, as the portable evaluator does; indexes then serve range scans and sorts. */
const binary = (schema: StructuralSchema) => {
  const scalar = schemaType(schema) === "array" ? schema.items : schema;
  return schemaType(scalar) === "string" &&
    scalar.format !== "date-time" &&
    schema["x-store"] !== "json" &&
    scalar["x-store"] !== "json"
    ? ' COLLATE "C"'
    : "";
};
type Field = {
  name: string;
  path: string[];
  schema: StructuralSchema;
  presence?: boolean;
  notNull?: boolean;
};
type Child = { path: string[]; shape: Shape };
type Shape = {
  table: string;
  schema: StructuralSchema;
  fields: Field[];
  children: Child[];
};
/** Names an array-of-objects field's child table. */
type ChildName = (table: string, path: string[]) => string;
const hashedChild: ChildName = (table, path) =>
  `rm_${shortHash(`${table}.${path.join(".")}`, 32)}`;
const readableChild = (): ChildName => {
  const used = new Set<string>();
  return (table, path) =>
    readableName(
      `${table}__${path.join("_")}`.toLowerCase(),
      63,
      `${table}.${path.join(".")}`,
      used,
      unsafe,
    );
};
function shapeFor(
  schema: StructuralSchema,
  table: string,
  rowNotNull = true,
  childName: ChildName = hashedChild,
): Shape {
  const shape: Shape = { table, schema, fields: [], children: [] };
  // notNull: every step from the row is required and non-nullable, so the value can never be NULL.
  function walk(s: StructuralSchema, path: string[], notNull: boolean): void {
    const name = path.join("_");
    if (s["x-store"] === "json") {
      shape.fields.push({ name, path, schema: s });
      return;
    }
    if (schemaType(s) === "object") {
      if (path.length)
        shape.fields.push({
          name: `${name}__present`,
          path,
          schema: s,
          presence: true,
        });
      for (const [key, value] of Object.entries(s.properties ?? {}))
        walk(
          value as StructuralSchema,
          [...path, key],
          notNull &&
            !!s.required?.includes(key) &&
            !nullable(value as StructuralSchema),
        );
    } else if (
      schemaType(s) === "array" &&
      schemaType(s.items) === "object" &&
      s.items["x-store"] !== "json"
    ) {
      shape.children.push({
        path,
        shape: shapeFor(
          s.items,
          childName(table, path),
          !nullable(s.items),
          childName,
        ),
      });
      shape.fields.push({
        name: `${name}__present`,
        path,
        schema: s,
        presence: true,
      });
    } else shape.fields.push({ name, path, schema: s, notNull });
  }
  walk(schema, [], rowNotNull);
  const names = new Set(["_id", "_key", "_parent", "_ordinal", "_item_null"]);
  for (const f of shape.fields) {
    if (names.has(f.name))
      throw new Error(`Readmodel relational column collision ${f.name}`);
    names.add(f.name);
  }
  return shape;
}
const tableShape = (gen: StoredGeneration, model: string): Shape => {
  const definition = gen.definitions[model];
  if (!definition) throw new Error(`Unknown Readmodel ${model}`);
  const named = gen.names?.[model];
  return named
    ? shapeFor(
        definition.schema as StructuralSchema,
        named,
        true,
        readableChild(),
      )
    : shapeFor(
        definition.schema as StructuralSchema,
        `rm_${shortHash(`${gen.group}.${gen.id}.${model}`, 32)}`,
      );
};
/** `rm_<group>_<model>_<generation tag>` for each model of a new generation. */
const tableNames = (gen: StoredGeneration) => {
  const used = new Set<string>();
  return Object.fromEntries(
    Object.keys(gen.definitions).map((model) => [
      model,
      `rm_${readableName(`${gen.group}_${model}`.toLowerCase(), 51, `${gen.group}.${gen.id}.${model}`, used, unsafe)}_${generationTag(gen)}`,
    ]),
  );
};
function locate(
  shape: Shape,
  path: string[],
): { shape: Shape; column: string } {
  for (const child of shape.children)
    if (child.path.every((v, i) => path[i] === v))
      return locate(child.shape, path.slice(child.path.length));
  return { shape, column: path.join("_") };
}
const at = (object: any, path: string[]) =>
  path.reduce((o, k) => o?.[k], object);
function set(object: any, path: string[], value: any): void {
  let target = object;
  for (const key of path.slice(0, -1)) {
    if (target[key] == null) target[key] = {};
    target = target[key];
  }
  target[path.at(-1)!] = value;
}
function sqlType(schema: StructuralSchema): string {
  if (schema["x-store"] === "json") return "jsonb";
  if (schemaType(schema) === "array") return `${sqlType(schema.items)}[]`;
  return schema.format === "date-time"
    ? "timestamptz"
    : (
        {
          string: "text",
          integer: "bigint",
          number: "double precision",
          boolean: "boolean",
          null: "text",
        } as Record<string, string>
      )[schemaType(schema)]!;
}
function fromSql(schema: StructuralSchema, value: any): any {
  if (value == null) return null;
  if (schema["x-store"] === "json") return value;
  if (schemaType(schema) === "array")
    return value.map((v: any) => fromSql(schema.items, v));
  if (schemaType(schema) === "integer") {
    const n = Number(value);
    if (!Number.isSafeInteger(n))
      throw new Error("Readmodel integer exceeds safe integer range");
    return n;
  }
  return value;
}
export function createPostgresReadmodelStore(
  options: PostgresReadmodelOptions = {},
): PostgresReadmodelStore {
  const pool =
    options.pool ??
    new Pool({
      ...options.poolOptions,
      connectionString: options.connectionString,
    });
  const schema = options.schema ?? "event_fabric_readmodels",
    ns = quote(schema),
    groups = `${ns}."ef_rm_groups"`,
    generations = `${ns}."ef_rm_generations"`;
  const withDefinitions = definitionsByHash();
  let initialized: Promise<void> | undefined;
  const initialize = () =>
    (initialized ??= (async () => {
      const client = await pool.connect();
      const key = `event-fabric-readmodel-upgrade:${schema}`;
      let locked = false;
      try {
        // Poll without holding a snapshot that would stall another replica's concurrent index build.
        while (!locked) {
          locked = (
            await client.query(
              "SELECT pg_try_advisory_lock(hashtextextended($1,0)) AS locked",
              [key],
            )
          ).rows[0].locked;
          if (!locked) await setTimeout(100);
        }
        await client.query(`CREATE SCHEMA IF NOT EXISTS ${ns}`);
        await client.query(
          `CREATE TABLE IF NOT EXISTS ${groups}(name text PRIMARY KEY,data jsonb NOT NULL)`,
        );
        await client.query(
          `CREATE TABLE IF NOT EXISTS ${generations}(group_name text NOT NULL,id text NOT NULL,data jsonb NOT NULL,PRIMARY KEY(group_name,id))`,
        );
        // Older tables compare `_id` in the database's collation; queries use "C".
        const ids = await client.query(
          `SELECT c.relname, i.indisvalid
         FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
         JOIN pg_attribute a ON a.attrelid=c.oid AND a.attname='_id' AND NOT a.attisdropped
         LEFT JOIN pg_class idx ON idx.relnamespace=n.oid AND idx.relname=c.relname||'__id_c'
         LEFT JOIN pg_index i ON i.indexrelid=idx.oid
         WHERE n.nspname=$1 AND c.relkind='r' AND c.relname ~ '^rm_[0-9a-f]{32}$'
         AND a.attcollation <> 'pg_catalog."C"'::regcollation`,
          [schema],
        );
        for (const row of ids.rows) {
          if (row.indisvalid) continue;
          const index = quote(`${row.relname}__id_c`);
          if (row.indisvalid === false)
            await client.query(`DROP INDEX CONCURRENTLY ${ns}.${index}`);
          await client.query(
            `CREATE INDEX CONCURRENTLY IF NOT EXISTS ${index} ON ${ns}.${quote(row.relname)}("_id" COLLATE "C")`,
          );
        }
        const children = await client.query(
          `SELECT c.relname,
         EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attname='_item_null' AND NOT a.attisdropped) AS has_item_null,
         (SELECT json_object_agg(idx.relname, i.indisvalid) FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid WHERE i.indrelid=c.oid) AS indexes
         FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
         WHERE n.nspname=$1 AND c.relkind='r' AND c.relname LIKE 'rm\\_%'
         AND EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attname='_parent' AND NOT a.attisdropped)`,
          [schema],
        );
        for (const row of children.rows) {
          if (!row.has_item_null)
            await client.query(
              `ALTER TABLE ${ns}.${quote(row.relname)} ADD COLUMN IF NOT EXISTS "_item_null" boolean NOT NULL DEFAULT false`,
            );
          const name = identifier(`${row.relname}_parent_ordinal`),
            valid = row.indexes?.[name];
          if (valid) continue;
          const index = quote(name);
          if (valid === false)
            await client.query(`DROP INDEX CONCURRENTLY ${ns}.${index}`);
          await client.query(
            `CREATE INDEX CONCURRENTLY IF NOT EXISTS ${index} ON ${ns}.${quote(row.relname)}("_parent","_ordinal")`,
          );
        }
      } finally {
        if (locked)
          await client
            .query("SELECT pg_advisory_unlock(hashtextextended($1,0))", [key])
            .catch(() => {});
        client.release();
      }
    })());
  const table = (shape: Shape) => `${ns}.${quote(shape.table)}`;
  async function create(
    client: PoolClient,
    shape: Shape,
    parent?: Shape,
  ): Promise<void> {
    const columns = shape.fields.map(
      (f) =>
        `${quote(f.name)} ${f.presence ? "boolean" : `${sqlType(f.schema)}${binary(f.schema)}`}${f.notNull ? " NOT NULL" : ""}`,
    );
    // No IF NOT EXISTS: a new generation's table name that is already taken must fail, not share.
    await client.query(
      `CREATE TABLE ${table(shape)}("_key" text PRIMARY KEY,${parent ? `"_parent" text NOT NULL REFERENCES ${table(parent)}("_key") ON DELETE CASCADE,"_ordinal" integer NOT NULL,"_item_null" boolean NOT NULL DEFAULT false` : '"_id" text COLLATE "C" NOT NULL UNIQUE'}${columns.length ? "," : ""}${columns.join(",")})`,
    );
    if (parent)
      await client.query(
        `CREATE INDEX ${quote(identifier(`${shape.table}_parent_ordinal`))} ON ${table(shape)}("_parent","_ordinal")`,
      );
    for (const f of shape.fields)
      if (!f.presence && schemaType(f.schema) === "array")
        await client.query(
          `CREATE INDEX ${quote(identifier(`${shape.table}_${shortHash(f.name)}`))} ON ${table(shape)} USING gin (${quote(f.name)})`,
        );
    for (const child of shape.children)
      await create(client, child.shape, shape);
  }
  // Replaces a batch's documents of one model: one delete (children cascade), then one multi-row insert per table, parents first.
  async function write(
    client: PoolClient,
    shape: Shape,
    documents: [id: string, doc: JsonObject | null][],
  ): Promise<void> {
    await client.query(`DELETE FROM ${table(shape)} WHERE "_id"=ANY($1)`, [
      documents.map(([id]) => id),
    ]);
    const rows = new Map<Shape, any[][]>();
    const collect = (
      shape: Shape,
      id: string,
      doc: JsonObject | null,
      parent?: string,
      ordinal?: number,
    ) => {
      const values: any[] = [
        id,
        ...(parent === undefined ? [id] : [parent, ordinal, doc === null]),
        ...shape.fields.map((f) => {
          const v = at(doc, f.path);
          return f.presence
            ? v != null
            : f.schema["x-store"] === "json"
              ? JSON.stringify(v)
              : schemaType(f.schema) === "array" &&
                  f.schema.items["x-store"] === "json"
                ? v?.map((item: any) => JSON.stringify(item))
                : v;
        }),
      ];
      if (!rows.has(shape)) rows.set(shape, []);
      rows.get(shape)!.push(values);
      for (const child of shape.children) {
        const array = at(doc, child.path);
        if (array)
          for (let i = 0; i < array.length; i++)
            collect(
              child.shape,
              `${id}/${child.path.join(".")}/${i}`,
              array[i],
              id,
              i,
            );
      }
    };
    for (const [id, doc] of documents) if (doc) collect(shape, id, doc);
    for (const [target, values] of rows) {
      const columns = [
        "_key",
        ...(target === shape ? ["_id"] : ["_parent", "_ordinal", "_item_null"]),
        ...target.fields.map((f) => f.name),
      ];
      // PostgreSQL accepts at most 65535 parameters per statement.
      const size = Math.floor(65535 / columns.length);
      for (let start = 0; start < values.length; start += size) {
        const chunk = values.slice(start, start + size);
        await client.query(
          `INSERT INTO ${table(target)}(${columns.map(quote).join(",")}) VALUES${chunk.map((_, r) => `(${columns.map((_, c) => `$${r * columns.length + c + 1}`).join(",")})`).join(",")}`,
          chunk.flat(),
        );
      }
    }
  }
  function selectPredicate(
    shape: Shape,
    filter: Filter,
    options: QueryOptions,
  ): {
    sql: string;
    values: any[];
    orderBy?: string;
    param: (value: any) => string;
  } {
    const values: any[] = [];
    let serial = 0;
    const param = (value: any) => {
      values.push(value);
      return `$${values.length}`;
    };
    function condition(
      expression: string,
      s: StructuralSchema,
      value: any,
    ): string {
      const bind = (v: any) => param(v instanceof Date ? v.toISOString() : v);
      const scalar = schemaType(s) === "array" ? s.items : s;
      // The portable evaluator compares strings by code unit, not by database collation.
      const collate =
        schemaType(scalar) === "string" && scalar.format !== "date-time"
          ? ' COLLATE "C"'
          : "";
      const eq = (v: any) =>
        v == null
          ? schemaType(s) === "array"
            ? `(${expression} IS NULL OR array_position(${expression}, NULL) IS NOT NULL)`
            : `${expression} IS NULL`
          : schemaType(s) === "array" && !Array.isArray(v)
            ? `${bind(v)} = ANY(${expression})`
            : `${expression} = ${bind(v)}`;
      if (
        value === null ||
        typeof value !== "object" ||
        Array.isArray(value) ||
        value instanceof Date
      )
        return eq(value);
      const parts = Object.entries(value).map(([key, v]) => {
        switch (key.replace(/^\$/, "")) {
          case "eq":
            return eq(v);
          case "ne":
            return `NOT coalesce((${eq(v)}),false)`;
          case "oneOf":
            return `(${(v as any[]).map((part) => condition(expression, s, part)).join(" OR ") || "false"})`;
          case "in":
            return `(${(v as any[]).map(eq).join(" OR ") || "false"})`;
          case "nin":
            return `NOT coalesce((${(v as any[]).map(eq).join(" OR ") || "false"}),false)`;
          case "gt":
          case "gte":
          case "lt":
          case "lte":
            return schemaType(s) === "array"
              ? `${bind(v)}${collate ? `::text${collate}` : ""} ${({ gt: "<", gte: "<=", lt: ">", lte: ">=" } as Record<string, string>)[key.replace(/^\$/, "")]} ANY(${expression})`
              : `${expression}${collate} ${({ gt: ">", gte: ">=", lt: "<", lte: "<=" } as Record<string, string>)[key.replace(/^\$/, "")]} ${bind(v)}`;
          case "exists":
            return `${expression} IS ${v ? "NOT " : ""}NULL`;
          default:
            return "TRUE";
        }
      });
      return parts.map((p) => `(${p})`).join(" AND ") || "TRUE";
    }
    function field(
      shape: Shape,
      path: string[],
      value: any,
      alias: string,
    ): string {
      if (path.length === 1 && path[0] === "_id")
        return condition(`${alias}."_id"`, { type: "string" }, value);
      for (const child of shape.children)
        if (child.path.every((v, i) => path[i] === v)) {
          const ca = `c${serial++}`,
            rest = path.slice(child.path.length);
          let predicate = "TRUE";
          if (
            rest.length &&
            value &&
            typeof value === "object" &&
            ["ne", "nin", "$ne", "$nin"].some((k) => k in value)
          )
            return "TRUE";
          if (
            rest.length &&
            value &&
            typeof value === "object" &&
            (value.exists === false || value.$exists === false)
          )
            return "TRUE";
          if (rest.length) predicate = field(child.shape, rest, value, ca);
          else if (
            value &&
            typeof value === "object" &&
            ("elemMatch" in value || "$elemMatch" in value)
          )
            predicate = compile(
              child.shape,
              value.elemMatch ?? value.$elemMatch,
              ca,
            );
          else if (value && typeof value === "object" && "exists" in value)
            return value.exists
              ? `${alias}.${quote(`${child.path.join("_")}__present`)}=true`
              : `${alias}.${quote(`${child.path.join("_")}__present`)}=false`;
          else return "TRUE";
          return `EXISTS(SELECT 1 FROM ${table(child.shape)} ${ca} WHERE ${ca}."_parent"=${alias}."_key" AND (${predicate}))`;
        }
      const column = shape.fields.find(
        (f) => !f.presence && f.path.join(".") === path.join("."),
      );
      // json fields are verified by the portable evaluator; SQL jsonb equality differs from its contains semantics.
      if (
        column &&
        column.schema["x-store"] !== "json" &&
        !(
          schemaType(column.schema) === "array" &&
          column.schema.items["x-store"] === "json"
        )
      )
        return condition(
          `${alias}.${quote(column.name)}`,
          column.schema,
          value,
        );
      // Dynamic JSON fields and whole object comparisons are verified by the portable evaluator.
      return "TRUE";
    }
    function compile(shape: Shape, filter: Filter, alias: string): string {
      return (
        Object.entries(filter)
          .map(([path, value]) =>
            path === "oneOf" || path === "$or"
              ? `(${(value as Filter[]).map((f) => compile(shape, f, alias)).join(" OR ") || "FALSE"})`
              : path === "and" || path === "$and"
                ? `(${(value as Filter[]).map((f) => compile(shape, f, alias)).join(" AND ") || "TRUE"})`
                : field(shape, path.split("."), value, alias),
          )
          .map((p) => `(${p})`)
          .join(" AND ") || "TRUE"
      );
    }
    // Token search cannot safely narrow portable substring matches.
    const sql = compile(shape, filter, "r");
    const orderBy =
      options.q && !options.sort
        ? `ts_rank(r."_search", websearch_to_tsquery('simple',${param(options.q)})) DESC, r."_id"`
        : undefined;
    return { sql, values, orderBy, param };
  }
  async function read(
    client: PoolClient,
    shape: Shape,
    selection?: {
      sql: string;
      values: any[];
      orderBy?: string;
      limit?: number;
    },
  ): Promise<JsonObject[]> {
    const rows = (
      await client.query(
        `SELECT * FROM ${table(shape)} r${selection ? ` WHERE ${selection.sql}${selection.orderBy ? ` ORDER BY ${selection.orderBy}` : ""}${selection.limit ? ` LIMIT ${selection.limit}` : ""}` : ""}`,
        selection?.values ?? [],
      )
    ).rows;
    return hydrate(client, shape, rows, false);
  }
  // Loads each child table once for all of these rows, instead of once per row.
  async function hydrate(
    client: PoolClient,
    shape: Shape,
    rows: any[],
    items: boolean,
  ): Promise<any[]> {
    const parents = shape.children.map(() => new Map<string, JsonObject>());
    const docs: any[] = [];
    for (const row of rows) {
      if (items && row._item_null) {
        docs.push(null);
        continue;
      }
      const doc: JsonObject = items ? {} : { _id: row._id };
      const absent: string[][] = [];
      for (const f of shape.fields) {
        if (absent.some((p) => p.every((v, i) => f.path[i] === v))) continue;
        if (f.presence) {
          if (!row[f.name]) {
            set(doc, f.path, null);
            absent.push(f.path);
          } else if (schemaType(f.schema) === "object") set(doc, f.path, {});
        } else if (!absent.some((p) => p.every((v, i) => f.path[i] === v)))
          set(doc, f.path, fromSql(f.schema, row[f.name]));
      }
      shape.children.forEach((child, c) => {
        if (
          !absent.some((p) => p.every((v, i) => child.path[i] === v)) &&
          at(doc, child.path) !== null
        ) {
          set(doc, child.path, []);
          parents[c]!.set(row._key, doc);
        }
      });
      docs.push(doc);
    }
    for (const [c, child] of shape.children.entries()) {
      if (!parents[c]!.size) continue;
      const childRows = (
        await client.query(
          `SELECT * FROM ${table(child.shape)} WHERE "_parent"=ANY($1) ORDER BY "_parent","_ordinal"`,
          [[...parents[c]!.keys()]],
        )
      ).rows;
      const values = await hydrate(client, child.shape, childRows, true);
      childRows.forEach((row, i) =>
        at(parents[c]!.get(row._parent), child.path).push(values[i]),
      );
    }
    return docs;
  }
  const store = durableReadmodelStore<PostgresReadmodelNative>({
    partitionLocks: true,
    label: `postgres schema ${schema}`,
    async transaction(group, fn, options) {
      const {
        readOnly = false,
        schemaChange = false,
        partition,
      } = options ?? {};
      await initialize();
      const client = await pool.connect();
      const lock = readOnly ? "pg_advisory_lock_shared" : "pg_advisory_lock";
      const unlock = readOnly
        ? "pg_advisory_unlock_shared"
        : "pg_advisory_unlock";
      const schemaKey = `event-fabric-readmodel-layout:${schema}:${group}`;
      let locked = false;
      try {
        // Take the layout lock before the snapshot; DROP/migration must not invalidate a reader's tables.
        if (readOnly || schemaChange) {
          await client.query(`SELECT ${lock}(hashtextextended($1,3))`, [
            schemaKey,
          ]);
          locked = true;
        }
        // Reads use a snapshot instead of the group's write lock, so they never wait for batch commits.
        if (readOnly)
          await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
        else {
          await client.query("BEGIN");
          const key = `event-fabric-readmodels:${schema}:${group}`;
          // A Partition shares the group's lock with the other Partitions and holds its own.
          if (partition === undefined)
            await client.query(
              `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
              [key],
            );
          else {
            await client.query(
              `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`,
              [key],
            );
            await client.query(
              `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`,
              [`${key}#${partition}`],
            );
          }
        }
        const tx: ReadmodelTransaction = {
          ownsDocuments: true,
          async getGroup(name) {
            const row = async (data: string) =>
              (
                await client.query(
                  `SELECT ${data} AS data FROM ${groups} WHERE name=$1`,
                  [name],
                )
              ).rows[0]?.data ?? null;
            return withDefinitions(await row(`data-'desired'`), "desired", () =>
              row("data"),
            );
          },
          async listPartitions(group) {
            return (
              await client.query(
                `SELECT data FROM ${groups} WHERE starts_with(name,$1)`,
                [`${group}#`],
              )
            ).rows.map((row) => row.data);
          },
          async putGroup(g) {
            await client.query(
              `INSERT INTO ${groups}(name,data)VALUES($1,$2) ON CONFLICT(name)DO UPDATE SET data=EXCLUDED.data`,
              [g.name, g],
            );
          },
          async getGeneration(g, id) {
            const row = async (data: string) =>
              (
                await client.query(
                  `SELECT ${data} AS data FROM ${generations} WHERE group_name=$1 AND id=$2`,
                  [g, id],
                )
              ).rows[0]?.data ?? null;
            return withDefinitions(
              await row(`data-'definitions'`),
              "definitions",
              () => row("data"),
            );
          },
          async putGeneration(gen) {
            await client.query(
              `INSERT INTO ${generations}(group_name,id,data)VALUES($1,$2,$3)ON CONFLICT(group_name,id)DO UPDATE SET data=EXCLUDED.data`,
              [gen.group, gen.id, gen],
            );
          },
          async create(gen) {
            gen.names = tableNames(gen);
            for (const model of Object.keys(gen.definitions)) {
              const shape = tableShape(gen, model);
              await create(client, shape);
              for (const index of gen.definitions[model]!.indexes ?? []) {
                const fields = Array.isArray(index.fields)
                  ? index.fields
                  : Object.keys(index.fields);
                const located = fields.map((path) =>
                  locate(shape, path.split(".")),
                );
                if (index.text) {
                  // Only rank parent-row text; portable q still matches every declared text path.
                  const expressions =
                    located
                      .filter((l) => l.shape === shape)
                      .map((l) => `coalesce(${quote(l.column)}::text,'')`)
                      .join(` || ' ' || `) || `''`;
                  await client.query(
                    `ALTER TABLE ${table(shape)} ADD COLUMN IF NOT EXISTS "_search" tsvector GENERATED ALWAYS AS (to_tsvector('simple',${expressions})) STORED`,
                  );
                  await client.query(
                    `CREATE INDEX IF NOT EXISTS ${quote(identifier(`${shape.table}_search`))} ON ${table(shape)} USING gin("_search")`,
                  );
                } else {
                  const target = located[0]!.shape,
                    columns = located.map((l) => quote(l.column)),
                    directions = Array.isArray(index.fields)
                      ? fields.map(() => 1)
                      : Object.values(index.fields);
                  // The order `page` sorts in, then `_id`, so a sorted page is an index range scan.
                  const keys = [
                    ...columns.map((c, i) =>
                      directions[i] === -1
                        ? `${c} DESC NULLS LAST`
                        : `${c} ASC NULLS FIRST`,
                    ),
                    ...(target === shape ? ['"_id"'] : []),
                  ];
                  await client.query(
                    `CREATE INDEX IF NOT EXISTS ${quote(identifier(`${target.table}_${shortHash(fields.join())}`))} ON ${table(target)}(${keys.join(",")})${index.sparse ? ` WHERE ${columns.map((c) => `${c} IS NOT NULL`).join(" AND ")}` : ""}`,
                  );
                }
              }
            }
          },
          async drop(gen) {
            async function drop(shape: Shape): Promise<void> {
              for (const child of shape.children) await drop(child.shape);
              await client.query(
                `DROP TABLE IF EXISTS ${table(shape)} CASCADE`,
              );
            }
            for (const model of Object.keys(gen.definitions))
              await drop(tableShape(gen, model));
            await client.query(
              `DELETE FROM ${generations} WHERE group_name=$1 AND id=$2`,
              [gen.group, gen.id],
            );
          },
          async read(gen, model, filter = {}, options = {}) {
            const shape = tableShape(gen, model);
            return read(client, shape, selectPredicate(shape, filter, options));
          },
          async page(gen, model, filter, order, after, limit) {
            const shape = tableShape(gen, model),
              selection = selectPredicate(shape, filter, {});
            const keys = order.map(([path, direction]) => {
              const f =
                path === "_id"
                  ? {
                      name: "_id",
                      schema: { type: "string" } as StructuralSchema,
                    }
                  : shape.fields.find(
                      (f) => !f.presence && f.path.join(".") === path,
                    )!;
              const collate =
                schemaType(f.schema) === "string" &&
                f.schema.format !== "date-time"
                  ? ' COLLATE "C"'
                  : "";
              return {
                column: `r.${quote(f.name)}${collate}`,
                direction,
                notNull: path === "_id" || !!(f as Field).notNull,
              };
            });
            // Ascending sorts put NULL first and descending sorts put it last, as compareValues does.
            const later = after?.map((value, i) => {
              const { column, direction } = keys[i]!,
                v =
                  value === null
                    ? null
                    : selection.param(
                        value instanceof Date ? value.toISOString() : value,
                      );
              const strictly =
                direction === 1
                  ? v === null
                    ? `${column} IS NOT NULL`
                    : `${column} > ${v}`
                  : v === null
                    ? "FALSE"
                    : `(${column} < ${v} OR ${column} IS NULL)`;
              return [
                ...keys
                  .slice(0, i)
                  .map(
                    (k, j) =>
                      `${k.column} IS NOT DISTINCT FROM ${after[j] === null ? "NULL" : selection.param(after[j] instanceof Date ? after[j].toISOString() : after[j])}`,
                  ),
                strictly,
              ].join(" AND ");
            });
            return read(client, shape, {
              sql: `(${selection.sql})${later ? ` AND (${later.map((l) => `(${l})`).join(" OR ")})` : ""}`,
              values: selection.values,
              // NULLS clauses only where NULL can occur: an index on a NOT NULL column sorts NULLS LAST.
              orderBy: keys
                .map(
                  ({ column, direction, notNull }) =>
                    `${column} ${direction === 1 ? "ASC" : "DESC"}${notNull ? "" : direction === 1 ? " NULLS FIRST" : " NULLS LAST"}`,
                )
                .join(","),
              limit,
            });
          },
          async count(gen, model, filter) {
            const shape = tableShape(gen, model),
              selection = selectPredicate(shape, filter, {});
            return Number(
              (
                await client.query(
                  `SELECT count(*) AS n FROM ${table(shape)} r WHERE ${selection.sql}`,
                  selection.values,
                )
              ).rows[0].n,
            );
          },
          async write(gen, model, documents) {
            await write(client, tableShape(gen, model), documents);
          },
          native(gen, model, external) {
            return {
              schema,
              table: model ? table(tableShape(gen, model)) : undefined,
              tableFor: (name: string) => table(tableShape(gen, name)),
              query: (text: string, values?: any[]) =>
                (external ? pool : client).query(text, values),
              pool,
            };
          },
        };
        const result = await fn(tx);
        await client.query("COMMIT");
        return result;
      } catch (e) {
        await client.query("ROLLBACK");
        throw e;
      } finally {
        let unlockError: Error | undefined;
        if (locked)
          await client
            .query(`SELECT ${unlock}(hashtextextended($1,3))`, [schemaKey])
            .catch((error: Error) => {
              unlockError = error;
            });
        client.release(unlockError);
      }
    },
    async close() {
      if (!options.pool) await pool.end();
    },
  });
  return Object.assign(store, {
    poolStats: () => ({
      total: pool.totalCount,
      idle: pool.idleCount,
      waiting: pool.waitingCount,
    }),
  });
}
