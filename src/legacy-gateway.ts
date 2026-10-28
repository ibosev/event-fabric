import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { Readable } from "node:stream";
import express from "express";
import type {
  ErrorRequestHandler,
  NextFunction,
  Request,
  RequestHandler,
  Response,
} from "express";
import { parse } from "qs";
import {
  ForbiddenError,
  ReadmodelRebuildingError,
  Rejection,
} from "./errors.js";
import { handle, keyedId } from "./gateway/index.js";
import { uuidv7 } from "./id.js";
import type { RegisteredLegacyApp } from "./registry.js";
import { minPosition } from "./gateway/min-position.js";
import { encodePosition } from "./position.js";
import { matchPattern } from "./runtime/index.js";
import {
  legacyListQuery,
  legacyQuery,
  legacyStream,
  type LegacyQuery,
} from "./compat.js";
import type {
  AccessMap,
  AwaitCorrelated,
  GatewayDefinition,
  ScopedHandle,
} from "./runtime/types.js";
import type {
  ApplicationNotification,
  Metadata,
  Outcome,
  RecordedEvent,
} from "./types.js";
import { legacySocket, type LegacySocketOptions } from "./legacy-socket.js";
import { lazyCursor } from "./internal.js";

/**
 * Old Gateway folders on an EventFabric Gateway: `setup.js` (or `index.js`) with its `access` map, and the
 * `routes/**` files with `{ before, routes, after }`, served with the old route semantics, toolkit
 * (`legacyRest`), request Application (`req.getApp()`), HTTP defaults and error bodies.
 */

const require = createRequire(import.meta.url);
const operation = Symbol("eventfabric.legacyOperation");
const methods = ["get", "post", "put", "delete", "options", "head", "patch"];
type Operation = { [operation]: (route: boolean, api: BuildApi) => any };
type Readmodel = { type: string; model: string; schema?: unknown };
type Transform = (value: any, app: any) => unknown;
type BuildApi = {
  domain: any;
  readmodels: any;
  services: any;
  options: LegacyGatewayOptions;
};

export type LegacyGatewayOptions = {
  /** The old Gateway folder. */
  directory: string;
  port: number;
  host?: string;
  /** Body limit for JSON and URL-encoded bodies; the old default was 10 MB. */
  bodyLimit?: string | number;
  /** The metadata the Application's auth middleware put on the request: Access rules and Commands get it. */
  metadata: (req: Request) => Metadata | undefined;
  /** Fields set on every request before any route runs, such as the name old auth code records. */
  requestFields?: Record<string, unknown>;
  /** The old error classes by name; a rejected Command's error is rebuilt as its class. */
  errors?: Record<string, any>;
  /** Builds a response serializer from an old route's response schema; default `JSON.stringify`. */
  serializer?: (schema: unknown) => (value: unknown) => string;
  /** The old Readmodels' response schemas by `"group.model"`. */
  readmodelSchemas?: Record<string, unknown>;
  /** The old Commands' payload schemas by `"context.aggregate.name"`, for route descriptions (`onRoute`). */
  commandSchemas?: Record<string, unknown>;
  /**
   * Old Commands that are no longer Commands, by `"context.aggregate.name"`. A `legacyRest.command` route naming
   * one sends nothing and answers what this returns, as it would the waited Event's `{ aggregate, payload }`; a
   * throw answers as a rejection does.
   */
  replacedCommands?: Record<
    string,
    (
      req: Request,
      command: { id: string | undefined; payload: unknown },
    ) => unknown
  >;
  /**
   * The scope an `Idempotency-Key` is unique in, from the request metadata (a tenant or user id). Without it the
   * header is ignored, as the old Gateway ignored it; with it, a repeated key answers the first Command's Outcome.
   */
  idempotencyScope?: (metadata: any) => string | undefined | Promise<string | undefined>;
  /** More members of `req.getApp()`, such as `getCrud`. */
  requestApp?: (app: LegacyRequestBase, req: Request) => RegisteredLegacyApp;
  /**
   * Called for each route method as it is built, as the old `onRoute` hook was: its full path, its method, and
   * its operation (`handler` and the route file's `schema` merged into the operation's own).
   */
  onRoute?: (route: LegacyRoute) => void;
  /** Adds routes after the route tree, before the 404 answer; awaited before the first request is answered. */
  afterRoutes?: (app: express.Express, api: { services: any }) => unknown;
  /** The Command lane of this Gateway's callers (`GatewayDefinition.lane`): `bulk` for an import API or webhooks. */
  lane?: GatewayDefinition<Request>["lane"];
  waitTimeout?: number;
  /** Options of the old socket protocol, served when the folder's setup has `ws`. */
  socket?: Pick<LegacySocketOptions, "coalesce" | "commandWaitTimeout">;
  /**
   * Which live Events and Application Notifications a caller's socket may receive, by its metadata. The old
   * socket sent every Event to everyone; without a rule a kind is denied.
   */
  live?: {
    events?: (metadata: Metadata, event: RecordedEvent) => boolean;
    notifications?: (
      metadata: Metadata,
      notification: ApplicationNotification,
    ) => boolean;
  };
};
export type LegacyRoute = {
  uri: string;
  method: string;
  operation: { handler: unknown; schema: Record<string, any> } & Record<
    string,
    any
  >;
};
/** `req.getApp()`: these members and the Application's `Register["legacyApp"]`, which `requestApp` adds. */
export type LegacyRequestApp = LegacyRequestBase & RegisteredLegacyApp;
type LegacyRequestBase = {
  getServices(): any;
  getReadmodels(): any;
  getDomain(): any;
  /** Waits for a downstream Event, as `migrateSource` rewrites a chain-wide `.await`; Commands carry the request metadata. */
  awaitCorrelated: AwaitCorrelated;
  getLogger(): any;
  errors: Record<string, any>;
};

const appOf = (req: Request): LegacyRequestApp => (req as any).getApp();

/**
 * A collection usable synchronously, as the old `base()` was: calls wait for `collection`, and a `find` or
 * `aggregate` cursor records its chain until a terminal method, iteration or `stream()` runs it.
 */
const deferredCollection = (collection: Promise<any>): any => {
  // The first call that uses it rethrows.
  collection.catch(() => {});
  return new Proxy(
    {},
    {
      get: (_target, key) => {
        if (typeof key !== "string" || key === "then") return undefined;
        if (key === "find" || key === "aggregate")
          return (...args: unknown[]) =>
            lazyCursor(async () => (await collection)[key](...args));
        return async (...args: unknown[]) => (await collection)[key](...args);
      },
    },
  );
};
/**
 * The old `group.model()` query builder over an Access-scoped repository: `find`/`filter`/`sort`/`from`/
 * `size`/`q` chain, awaiting gives the array and `stream()` a Node stream; `raw({ find, options })` and
 * `base()` use the store's native collection, scoped to the Access rule.
 */
const legacyModel = (repository: any) => {
  let query: LegacyQuery = {},
    raw: { find?: object; options?: object } | undefined;
  const native = () => deferredCollection(repository.native());
  const builder: any = {
    options: () => builder,
    base: native,
    raw: (value: typeof raw) => ((raw = value), builder),
    find: (part: LegacyQuery = {}) => (
      (query = mergeSchemas(query, part)),
      builder
    ),
    filter: (filter: LegacyQuery["filter"]) => builder.find({ filter }),
    sort: (sort: LegacyQuery["sort"] = []) => builder.find({ sort }),
    from: (from = 0) => builder.find({ from }),
    size: (size = 100) => builder.find({ size }),
    q: (q: string) => builder.find({ q }),
    exec: () =>
      raw
        ? native()
            .find(raw.find ?? {}, raw.options ?? {})
            .toArray()
        : repository.find(...legacyQuery(query)),
    then: (resolve: any, reject: any) => builder.exec().then(resolve, reject),
    catch: (reject: any) => builder.exec().catch(reject),
    stream: () =>
      raw
        ? native()
            .find(raw.find ?? {}, raw.options ?? {})
            .stream()
        : legacyStream(repository.stream(...legacyQuery(query))),
  };
  return builder;
};
/** The request's Readmodel tree: EventFabric's, except that `group.model()` without an id is the old builder. */
export const legacyReadmodels = (readmodels: any) =>
  new Proxy(readmodels, {
    get: (target, group) => {
      const models = Reflect.get(target, group);
      if (!models || typeof models !== "object") return models;
      return new Proxy(models, {
        get: (inner, model) => {
          const repository = Reflect.get(inner, model);
          if (typeof repository !== "function") return repository;
          return new Proxy(repository, {
            apply: (read, self, args) =>
              args.length
                ? Reflect.apply(read, self, args)
                : legacyModel(repository),
          });
        },
      });
    },
  });
const isOperation = (value: any): value is Operation =>
  !!value && typeof value[operation] === "function";
const build = (value: any, api: BuildApi) =>
  isOperation(value) ? value[operation](false, api) : value;
const flat = (value: any, map: (item: any) => any): any[] =>
  [value].flat(Infinity).map(map).flat(Infinity);

// Old `routes/**` loading: a folder is a prefix; `index`/`root` files mount at their folder; `a.b.js` at `/a`.
type Tree = { file?: string; children: Record<string, Tree> };
const loadTree = (directory: string): Tree => {
  const tree: Tree = { children: {} };
  if (!fs.existsSync(directory)) return tree;
  for (const name of fs.readdirSync(directory)) {
    const file = path.join(directory, name);
    const stat = fs.statSync(file);
    if (stat.isDirectory()) tree.children[name] = loadTree(file);
    else if (stat.isFile() && path.extname(file) === ".js") {
      const base = name.split(".", 1)[0]!;
      require(file);
      if (base === "index" || base === "root") tree.file = file;
      else tree.children[base] = { file, children: {} };
    }
  }
  return tree;
};
/** An Express 4 route path in Express 5 syntax: `/a/:b?` is `/a{/:b}`; a trailing slash is dropped. */
const legacyFormat = (route: string) =>
  `/${route.replace(/\/+$/, "").replace(/^\/+/, "")}`;
export const legacyPath = (route: string) => {
  const formatted = legacyFormat(route);
  if (/[*()+]/.test(formatted.replace(/\/:(\w+)\?/g, "")))
    throw new Error(`Unsupported old route path ${route}`);
  return formatted.replace(/\/:(\w+)\?/g, "{/:$1}");
};
const routeDefinition = (value: any) => {
  if (Array.isArray(value) || typeof value === "function" || "handler" in value)
    return { get: value };
  const unsupported = Object.keys(value).filter(
    (key) => !methods.includes(key),
  );
  if (unsupported.length)
    throw new Error(
      `Methods [${unsupported.join(", ")}] are not supported. Only [${methods.join(", ")}] methods are supported.`,
    );
  return value;
};
const urlJoin = (...parts: string[]) =>
  `/${parts
    .map((part) => part.replace(/^\/+/, ""))
    .filter(Boolean)
    .join("/")}`;
const buildRouter = (tree: Tree, api: BuildApi, uri = "/") => {
  const module = tree.file ? require(tree.file) : {};
  const before = [module.before ?? []].flat(),
    after = [module.after ?? []].flat();
  const router = express.Router();
  if (before.length) router.use(before.map((item) => build(item, api)));
  for (const [key, value] of Object.entries<any>(module.routes ?? {})) {
    const route = router.route(legacyPath(key));
    const at = urlJoin(uri, legacyFormat(key));
    let operations = routeDefinition(value);
    if (isOperation(operations))
      operations = routeDefinition(operations[operation](true, api));
    for (const [method, value] of Object.entries<any>(operations)) {
      const built = Array.isArray(value)
        ? flat(value, (item) => build(item, api))
        : build(value, api);
      if (
        !Array.isArray(built) &&
        typeof built !== "function" &&
        !("handler" in built)
      )
        throw Error("Operation has not handler.");
      // The old normalizer: a bare handler takes the file's schema; an operation merges it under its own.
      const described =
        Array.isArray(built) || typeof built === "function"
          ? { handler: built, schema: module.schema ?? {} }
          : Object.assign(built, {
              schema: mergeSchemas(true, module.schema, built.schema),
            });
      // The hook gets its own schemas: the old swagger plugin rewrote them in place, which a second build of the
      // routes (after a failed one, or an Application restarted in the same process) would then serve.
      api.options.onRoute?.({
        uri: at,
        method,
        operation: {
          ...described,
          schema: copied(described.schema),
          handler: Array.isArray(described.handler)
            ? described.handler.map((item: any) =>
                item && typeof item === "object" && "schema" in item
                  ? { ...item, schema: copied(item.schema) }
                  : item,
              )
            : described.handler,
        },
      });
      const handlers = flat(
        Array.isArray(built) || typeof built === "function"
          ? built
          : built.handler,
        (item) => item.handler ?? item,
      );
      (route as any)[method](handlers);
    }
  }
  for (const [name, child] of Object.entries(tree.children))
    router.use(legacyPath(name), buildRouter(child, api, urlJoin(uri, name)));
  if (after.length) router.use(after.map((item) => build(item, api)));
  return router;
};

// lodash.merge over route schemas: objects merge key by key, anything else replaces.
const isPlain = (value: unknown): value is Record<string, any> =>
  !!value && typeof value === "object" && !Array.isArray(value);
/** Plain data copied (objects and arrays); anything else, a function or a RegExp, kept as it is. */
const copied = (value: unknown): any =>
  Array.isArray(value)
    ? value.map(copied)
    : value && [Object.prototype, null].includes(Object.getPrototypeOf(value))
      ? Object.fromEntries(Object.entries(value).map(([key, item]) => [key, copied(item)]))
      : value;
// With `union`, arrays in both join without duplicates, as the old route normalizer merged them.
function mergeSchemas(...sources: unknown[]): Record<string, any>;
function mergeSchemas(union: true, ...sources: unknown[]): Record<string, any>;
function mergeSchemas(...input: unknown[]): Record<string, any> {
  const union = input[0] === true;
  const sources = union ? input.slice(1) : input;
  const into = (target: Record<string, any>, source: Record<string, any>) => {
    for (const [key, value] of Object.entries(source))
      if (value !== undefined)
        target[key] =
          isPlain(value) && isPlain(target[key])
            ? into({ ...target[key] }, value)
            : union && Array.isArray(value) && Array.isArray(target[key])
              ? [...new Set([...target[key], ...value])]
              : value;
    return target;
  };
  return sources.reduce<Record<string, any>>(
    (target, source) => (isPlain(source) ? into(target, source) : target),
    {},
  );
}
/** The old success schema: `responses[200].schema`, or `responses[200]` itself. */
const serializerFor = (endpoint: Record<string, any>, api: BuildApi) => {
  const success = endpoint.responses?.[200];
  const schema = success?.schema ?? success;
  return schema && api.options.serializer
    ? api.options.serializer(schema)
    : JSON.stringify;
};
const checkTransforms = (transforms: unknown[]) => {
  for (const transform of transforms)
    if (typeof transform !== "function")
      throw new Error("Route transformers must be of type funciton");
};
/** Transforms in order; null or undefined ends the chain and answers an empty body, as the old stream did. */
const answer = async (
  res: Response,
  value: unknown,
  transforms: Transform[],
  app: unknown,
  stringify: (value: unknown) => string,
) => {
  for (const transform of transforms) {
    if (value == null) break;
    value = await transform(value, app);
  }
  res.writeHead(200, { "content-type": "application/json" });
  res.end(value == null ? undefined : stringify(value));
};
const repositoryOf = (req: Request, { type, model }: Readmodel) => {
  const repository = handle(req).readmodels[type]?.[model];
  if (!repository) throw new Error(`Unknown Readmodel ${type}.${model}`);
  return repository;
};
/** Waits for the request's read-your-writes Position and sets the Position headers. */
const served = async (
  req: Request,
  res: Response,
  group: string,
  repository: any,
  waitTimeout?: number,
) => {
  const status = await repository.status({
    minPosition: minPosition(req),
    timeout: waitTimeout ?? handle(req).waitTimeout,
  });
  const position = status.positions[group];
  if (position) res.set("EventFabric-Position", encodePosition(position));
  if (status.behind)
    res.set("EventFabric-Behind", position ? encodePosition(position) : "none");
};
/**
 * The old list read: the request's query string (old bracket syntax) overridden key by key by `query`,
 * Access-scoped; `total` counts every match before paging.
 */
export const legacyListPage = async (
  req: Request,
  res: Response,
  group: string,
  model: string,
  query: Record<string, unknown> = {},
  waitTimeout?: number,
) => {
  const repository = handle(req).readmodels[group]?.[model];
  if (!repository) throw new ForbiddenError();
  const raw = req.originalUrl.split("?")[1] ?? "";
  // `minPosition` is EventFabric's read-your-writes token, never a field.
  const { minPosition: _token, ...requested } = parse(raw);
  const [filter, options] = legacyListQuery({ ...requested, ...query });
  await served(req, res, group, repository, waitTimeout);
  const total: number = await repository.count(
    filter,
    options.q ? { q: options.q } : {},
  );
  return {
    total,
    documents: (await repository.find(filter, options)) as any[],
  };
};

const readmodelOf = (value: any): Readmodel => {
  if (!value || !value.model || !value.type)
    throw new Error("Readmodel is required.");
  return value;
};
const one = (
  readmodel: Readmodel,
  {
    id = (req: Request, model: string) => req.params[`${model}_id`] as string,
    query = null,
    schema,
    transforms = [],
  }: {
    id?: (req: Request, model: string) => string | undefined;
    query?: Record<string, unknown> | null;
    schema?: unknown;
    transforms?: Transform[];
  },
  api: BuildApi,
) => {
  const endpoint = mergeSchemas(
    readmodel.schema && { responses: { 200: { schema: readmodel.schema } } },
    schema,
  );
  const stringify = serializerFor(endpoint, api);
  checkTransforms(transforms);
  return {
    schema: endpoint,
    handler: async (req: Request, res: Response) => {
      const repository = repositoryOf(req, readmodel);
      const modelId = id(req, readmodel.model);
      await served(
        req,
        res,
        readmodel.type,
        repository,
        api.options.waitTimeout,
      );
      const [filter] = legacyQuery({ filter: { ...query, id: modelId } });
      const document =
        modelId === undefined ? null : await repository.findOne(filter);
      await answer(res, document, transforms, appOf(req), stringify);
    },
  };
};
const list = (
  readmodel: Readmodel,
  {
    schema = {},
    query = null,
    transforms = [],
  }: {
    schema?: Record<string, any>;
    query?: Record<string, unknown> | null;
    transforms?: Transform[];
  },
  api: BuildApi,
) => {
  const item = schema?.responses?.[200] ?? readmodel.schema;
  const listed = item && {
      responses: {
        200: {
          schema: {
            properties: {
              total: { type: "integer", example: 1 },
              data: {
                type: "array",
                items: typeof item === "string" ? { $ref: item } : item,
              },
            },
          },
        },
      },
    },
    endpoint = mergeSchemas(listed, schema);
  // The old list stream serialized each document with the endpoint's success schema; its route
  // description put the list's own response schema over the route's.
  const stringify = serializerFor(endpoint, api);
  checkTransforms(transforms);
  return {
    schema: mergeSchemas(schema, listed),
    handler: async (req: Request, res: Response) => {
      const app = appOf(req);
      const { total, documents } = await legacyListPage(
        req,
        res,
        readmodel.type,
        readmodel.model,
        query ?? {},
        api.options.waitTimeout,
      );
      const data: string[] = [];
      for (let document of documents) {
        for (const transform of transforms) {
          document = await transform(document, app);
          if (document == null) break;
        }
        if (document != null) data.push(stringify(document));
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(`${JSON.stringify({ total }).slice(0, -1)},"data":[${data}]}`);
    },
  };
};

/** `context.aggregate.command`, with or without the old `domain.` prefix. */
const commandOf = (value: any) => {
  if (typeof value !== "string") return value;
  const parts = value.split(".");
  if (parts.length === 4) parts.shift();
  const [context, aggregate, name] = parts;
  return { context, aggregate, name };
};
type Wait =
  | { kind: "any" }
  | { kind: "event"; pattern: string }
  | { kind: "readmodel"; group: string; model: string; change: string };
const waitOf = (event: any): Wait => {
  const name = typeof event === "string" ? event : event?.fullname;
  if (!name) return { kind: "any" };
  const [type, group, model, change = "*"] = name.split(".");
  if (type === "readmodel") return { kind: "readmodel", group, model, change };
  return { kind: "event", pattern: name.replace(/^domain\./, "") };
};
const commandBody = (event: RecordedEvent) => ({
  aggregate: {
    id: event.stream.id,
    name: event.stream.aggregate,
    revision: event.revision,
  },
  payload: event.payload,
});
/** A Readmodel document a Command's Events wrote or deleted, from the Notifications naming its correlation. */
const changedDocument = async (
  req: Request,
  wait: Extract<Wait, { kind: "readmodel" }>,
  outcome: Extract<Outcome, { status: "accepted" }>,
  notifications: { pending: any[]; next?: () => void },
  timeout: number,
) => {
  const correlation = outcome.events[0]?.correlationId;
  const repository = repositoryOf(req, { type: wait.group, model: wait.model });
  const deadline = Date.now() + timeout;
  for (;;) {
    for (const notification of notifications.pending.splice(0)) {
      // A batch can hold other Commands' changes, so its Position alone does not identify this one's.
      if (!correlation || !notification.correlationIds.includes(correlation))
        continue;
      for (const id of notification.ids) {
        const { document } = await repository(id, {
          minPosition: notification.position,
        });
        // Notifications do not say whether a document was created or updated; both answer the document.
        const written = document !== null;
        if (
          wait.change === "*" ||
          (wait.change === "delete" ? !written : written)
        )
          return { found: true, document };
      }
    }
    const left = deadline - Date.now();
    if (left <= 0) return { found: false, document: null };
    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, left);
      notifications.next = () => {
        clearTimeout(timer);
        resolve();
      };
    });
  }
};
const command = (
  description: { command: any; event?: any },
  {
    id = (req: Request) => req.params.id as string | undefined,
    payload = (req: Request) => req.body,
    schema = {},
    transforms = [],
    ...rest
  }: {
    id?: (req: Request) => string | undefined;
    payload?: (req: Request) => unknown;
    schema?: Record<string, any>;
    transforms?: Transform[];
    [option: string]: unknown;
  },
  api: BuildApi,
) => {
  const target = commandOf(description.command);
  if (!target) throw new Error("Invalid command");
  const wait = waitOf(description.event);
  const responseSchema =
    wait.kind === "readmodel" &&
    api.options.readmodelSchemas?.[`${wait.group}.${wait.model}`];
  const response = responseSchema && {
      responses: { 200: { schema: responseSchema } },
    },
    body =
      api.options.commandSchemas?.[
        `${target.context}.${target.aggregate}.${target.name}`
      ],
    endpoint = mergeSchemas(response, schema);
  const stringify = serializerFor(endpoint, api);
  checkTransforms(transforms);
  const replaced =
    api.options.replacedCommands?.[
      `${target.context}.${target.aggregate}.${target.name}`
    ];
  if (replaced)
    return {
      ...rest,
      schema: mergeSchemas(body && { body }, response, schema),
      handler: async (req: Request, res: Response) => {
        const value = await replaced(req, {
          id: id(req),
          payload: payload(req),
        });
        await answer(res, value, transforms, appOf(req), stringify);
      },
    };
  return {
    ...rest,
    // The route description adds the Command's payload schema as the request body.
    schema: mergeSchemas(body && { body }, response, schema),
    handler: async (req: Request, res: Response) => {
      const app = appOf(req),
        request = handle(req);
      const notifications: { pending: any[]; next?: () => void } = {
        pending: [],
      };
      // Subscribed before sending, so no Notification for the Command's Events is missed.
      const subscription =
        wait.kind === "readmodel"
          ? await request.readmodels.subscribe(
              wait.group,
              wait.model,
              (notification: any) => {
                notifications.pending.push(notification);
                notifications.next?.();
              },
              { initialSize: 0 },
            )
          : undefined;
      try {
        // As the new Gateway's `command()`: an Idempotency-Key names the Command, and its id answers in a header.
        // The scope comes from request metadata, which route middleware sets, so it is read here.
        const metadata = asJson(api.options.metadata(req)) ?? {};
        const key = req.get("Idempotency-Key");
        const scope = key && (await api.options.idempotencyScope?.(metadata));
        const commandId = scope ? keyedId(scope, key) : uuidv7();
        res.set("EventFabric-Command-Id", commandId);
        // What `req.getApp().getDomain()` sends: the request metadata, no metadata of the route's own.
        const outcome: Outcome = await request.sendCommand({
          id: commandId,
          stream: {
            context: target.context,
            aggregate: target.aggregate,
            id: id(req) ?? "",
          },
          name: target.name,
          payload: payload(req),
          metadata,
        });
        if (outcome.status !== "accepted")
          throw legacyError(outcome, api.options.errors);
        if (outcome.duplicate) res.set("EventFabric-Duplicate", "true");
        const position = outcome.events.at(-1)?.position;
        if (position) res.set("EventFabric-Position", encodePosition(position));
        let value: unknown;
        // A Duplicate's documents were written by the first request, so no Notification will name them.
        if (wait.kind === "readmodel" && outcome.duplicate)
          return void res.status(202).json(outcome);
        if (wait.kind === "readmodel") {
          const { found, document } = await changedDocument(
            req,
            wait,
            outcome,
            notifications,
            api.options.waitTimeout ?? request.waitTimeout,
          );
          // The Command is recorded; without the document the client gets the Outcome, as `command()` answers.
          if (!found) return void res.status(202).json(outcome);
          value = document;
        } else {
          const event = outcome.events.find(
            (event) =>
              wait.kind === "any" ||
              matchPattern(
                `${event.stream.context}.${event.stream.aggregate}.${event.name}`,
                wait.pattern,
              ),
          );
          if (!event) return void res.status(202).json(outcome);
          value = commandBody(event);
        }
        await answer(res, value, transforms, app, stringify);
      } finally {
        await subscription?.stop();
      }
    },
  };
};

/** A request as old route code reads it: `getApp()` and any field the Application's own middleware set. */
export type LegacyRequest = Request & {
  getApp(): LegacyRequestApp;
} & Record<string, any>;

/** The old Gateway toolkit, for route files whose import of it is replaced by this. */
export const legacyRest = {
  /** A promise-returning middleware whose rejection goes to `next`. */
  async:
    (
      fn: (
        req: LegacyRequest,
        res: Response,
        next: NextFunction,
      ) => Promise<unknown>,
    ) =>
    (req: Request, res: Response, next: NextFunction = () => {}) => {
      fn(req as LegacyRequest, res, next).catch(next);
    },
  /** `fn(services)` once, when the routes are built; its result is the middleware. */
  services: (fn: (services: any) => unknown): Operation => ({
    [operation]: (_route, api) => fn(api.services),
  }),
  /**
   * Sends the Command (default `post`), waits for `event` — an Event name or `readmodel.<group>.<model>.<change>`
   * — and answers `{ aggregate, payload }` for an Event or the Readmodel document.
   */
  command: (
    describe: (domain: any) => { command: any; event?: any },
    options: Parameters<typeof command>[1] = {},
  ): Operation => ({
    [operation]: (route, api) => {
      const middleware = command(describe(api.domain), options, api);
      return route ? { post: middleware } : middleware;
    },
  }),
  readmodels: {
    /** The document whose id is `req.params.<model>_id` (or `id(req, model)`); an empty body when absent. */
    one: (
      describe: (readmodels: any) => Readmodel,
      options: Parameters<typeof one>[1] = {},
    ): Operation => ({
      [operation]: (route, api) => {
        const middleware = one(
          readmodelOf(describe(api.readmodels)),
          options,
          api,
        );
        return route ? { get: middleware } : middleware;
      },
    }),
    /** `{ total, data }` for the query string over the route's `query`. */
    list: (
      describe: (readmodels: any) => Readmodel,
      options: Parameters<typeof list>[1] = {},
    ): Operation => ({
      [operation]: (route, api) => {
        const middleware = list(
          readmodelOf(describe(api.readmodels)),
          options,
          api,
        );
        return route ? { get: middleware } : middleware;
      },
    }),
  },
};

const statusOf = (kind?: string) =>
  kind === "validation" ? 400 : kind === "existence" ? 404 : 500;
/**
 * The error the old runtime raised for a rejected Command: the thrown error rebuilt as its old class when
 * `errors` has it, otherwise its fields on an Error; a schema rejection is the old ValidationError.
 */
export const legacyError = (
  rejected:
    Rejection | Extract<Outcome, { status: "rejected" | "failed" | "unknown" }>,
  errors: Record<string, any> = {},
): Error => {
  if (!(rejected instanceof Rejection) && rejected.status !== "rejected")
    return Object.assign(new Error("Internal Server Error"), {
      statusCode: 500,
      errorCode: 500,
    });
  const error = rejected instanceof Rejection ? rejected : rejected.error;
  const thrown = error.thrown;
  if (thrown) {
    const Class = errors[String(thrown.name)];
    // Only the Application's own error classes answer with all their fields; any other error (a driver's,
    // an HTTP client's) may hold connection details, so it keeps its name, message and codes.
    if (Class)
      return Object.assign(new Class(), { message: error.message }, thrown);
    const { name, statusCode, errorCode, code } = thrown as any;
    return Object.assign(new Error(error.message), {
      name,
      ...(statusCode !== undefined && { statusCode }),
      ...(errorCode !== undefined && { errorCode }),
      ...(code !== undefined && { code }),
    });
  }
  if (
    error.kind === "validation" &&
    Array.isArray(error.details) &&
    errors.ValidationError?.formAjvError
  )
    return errors.ValidationError.formAjvError(
      error.details.map((detail: any) => ({
        ...detail,
        // The old validator's property path: `.items[0].name` for EventFabric's `items.0.name`.
        dataPath:
          detail.dataPath ??
          (detail.path === undefined
            ? (detail.instancePath ?? "").replace(/\//g, ".")
            : String(detail.path)
                .split(".")
                .filter(Boolean)
                .map((part) => (/^\d+$/.test(part) ? `[${part}]` : `.${part}`))
                .join("")),
      })),
    );
  return Object.assign(new Error(error.message), {
    statusCode: statusOf(error.kind),
    errorCode: error.code,
  });
};
/** An error as the old stack raised it: a Rejection as its old class, an Access denial as `UnauthorizedError`. */
export const asLegacyError = (error: any, errors: Record<string, any> = {}) => {
  if (error instanceof Rejection) error = legacyError(error, errors);
  else if (error instanceof ForbiddenError)
    error = errors.UnauthorizedError
      ? new errors.UnauthorizedError()
      : Object.assign(new Error("Unauthorized"), {
          statusCode: 401,
          errorCode: 401,
        });
  else if (error instanceof ReadmodelRebuildingError)
    error = Object.assign(error, { statusCode: 503 });
  // An old Command rejection carried the thrown error as its payload.
  if (error?.name === "CommandRejectedError" && error.payload)
    error = error.payload;
  return error;
};
const legacyErrors =
  (errors: Record<string, any> = {}): ErrorRequestHandler =>
  (error, _req, res, _next) => {
    res.set("Access-Control-Allow-Origin", "*");
    if (error instanceof ReadmodelRebuildingError) res.set("Retry-After", "5");
    error = asLegacyError(error, errors);
    if (res.headersSent) {
      res.socket?.destroy();
      return;
    }
    const data =
      error?.data ??
      (typeof error?.toJson === "function" ? error.toJson() : undefined);
    // The old body also carried the stack; it stays out of responses.
    res.status(error?.statusCode || 500).json({
      error: { code: error?.errorCode, message: error?.message, data },
    });
  };
const cors: RequestHandler = (req, res, next) => {
  res.set("Access-Control-Allow-Origin", "*");
  if (req.method !== "OPTIONS") return next();
  res.set("Access-Control-Allow-Methods", "GET,POST,PATCH,PUT,DELETE");
  const headers = req.get("Access-Control-Request-Headers");
  if (headers) {
    res.set("Access-Control-Allow-Headers", headers);
    res.vary("Access-Control-Request-Headers");
  }
  res.set("Content-Length", "0").status(200).end();
};
// Proxies naming any `context.aggregate.command` and `group.model`, as the old toolkit's callbacks read them.
const names = <T>(
  make: (a: string, b: string, c: string) => T,
  depth: number,
) => {
  const level = (path: string[]): any =>
    new Proxy(
      {},
      {
        get: (_, key) =>
          typeof key !== "string"
            ? undefined
            : path.length + 1 === depth
              ? (make as any)(...path, key)
              : level([...path, key]),
      },
    );
  return level([]);
};
/** JSON, as the old bus carried request metadata: an ObjectId becomes its hex text. */
const asJson = (value: unknown) =>
  value === undefined ? undefined : JSON.parse(JSON.stringify(value));

/**
 * An EventFabric Gateway serving an old Gateway folder. The Access map comes from its `setup.js` (`index.js`
 * wins), each rule receiving the request metadata; Commands sent from routes carry that metadata over their
 * own. `req.getApp()` gives `getServices()`, Access-scoped `getReadmodels()`, `getDomain()`, `getLogger()`
 * and `errors`. Routes build on the first request, once services are ready.
 */
export function legacyGateway(
  options: LegacyGatewayOptions,
): GatewayDefinition<Request> {
  const directory = path.resolve(options.directory);
  const setupFile = ["index.js", "setup.js"]
    .map((name) => path.join(directory, name))
    .find((file) => fs.existsSync(file));
  const setup = setupFile ? require(setupFile) : {};
  const tree = loadTree(path.join(directory, "routes"));
  const metadata = (req: Request) => asJson(options.metadata(req)) ?? {};
  const rules = (section: Record<string, any> = {}, withCommand = false) =>
    Object.fromEntries(
      Object.entries(section).map(([key, rule]) => [
        key,
        typeof rule === "function"
          ? withCommand
            ? (req: Request, command: unknown) => rule(metadata(req), command)
            : (req: Request) => rule(metadata(req))
          : rule,
      ]),
    );
  const access: AccessMap<Request> = {
    readmodels: rules(setup.access?.readmodels),
    // Old Command rules were never enforced.
    domain: setup.access?.domain
      ? rules(setup.access.domain, true)
      : { "*": true },
  };
  const limit = options.bodyLimit ?? "10mb";
  const keepRaw = (req: any, _res: unknown, body: Buffer) => {
    req.rawBody = body;
  };
  let router: Promise<express.Express> | undefined;
  const app = express();
  app.set("query parser", "extended");
  app.use((req, _res, next) => {
    Object.assign(req, options.requestFields);
    // Express 4 always set a body.
    req.body ??= {};
    // Express 4 parsed the query once into a plain property that middleware could change (an account
    // filter added to it, for one); Express 5 parses it again on every read, losing such changes.
    Object.defineProperty(req, "query", {
      value: req.query,
      writable: true,
      enumerable: true,
      configurable: true,
    });
    (req as any).getApp = () => requestApp(req);
    next();
  });
  const requestApp = (
    req: Request,
    request: ScopedHandle = handle(req),
  ): LegacyRequestApp => {
    const base: LegacyRequestBase = {
      getServices: () => request.services,
      getReadmodels: () => legacyReadmodels(request.readmodels),
      getDomain: () =>
        legacyDomain(request.getDomain(), metadata(req), options.errors),
      awaitCorrelated: (input, pattern, waiting) =>
        request.awaitCorrelated(
          { ...input, metadata: { ...input.metadata, ...metadata(req) } },
          pattern,
          waiting,
        ),
      getLogger: () => request.logger,
      errors: options.errors ?? {},
    };
    return Object.assign(base, options.requestApp?.(base, req)) as LegacyRequestApp;
  };
  const routes: RequestHandler = async (req, res, next) => {
    try {
      router ??= (async () => {
        const api: BuildApi = {
          domain: names(
            (context, aggregate, name) => ({ context, aggregate, name }),
            3,
          ),
          readmodels: names(
            (type, model) => ({
              type,
              model,
              schema: options.readmodelSchemas?.[`${type}.${model}`],
            }),
            2,
          ),
          services: handle(req).services,
          options,
        };
        const routes = express();
        routes.set("query parser", "extended");
        routes.use(buildRouter(tree, api));
        await options.afterRoutes?.(routes, { services: api.services });
        routes.use((_req, res) => void res.status(404).send("Not Found."));
        routes.use(legacyErrors(options.errors));
        return routes;
      })().catch((error) => {
        router = undefined;
        throw error;
      });
      (await router)(req, res, next);
    } catch (error) {
      next(error);
    }
  };
  app.use(routes);
  return {
    port: options.port,
    host: options.host,
    bodyLimit: limit,
    rawBody: true,
    middleware: [
      // JSON body errors from the Gateway's own parser answer in the old error body too.
      legacyErrors(options.errors),
      cors,
      express.urlencoded({ limit, extended: true }),
      express.json({
        limit,
        type: "application/vnd.api+json",
        verify: keepRaw,
      }),
    ],
    context: (req) => req as Request,
    commandMetadata: (req) => metadata(req as Request),
    // Command routes apply `options.idempotencyScope` once route middleware set the metadata; this only
    // keeps the router from warning that the header is ignored.
    ...(options.idempotencyScope && { idempotencyScope: () => undefined }),
    access,
    lane: options.lane,
    waitTimeout: options.waitTimeout,
    routes: [{ method: "all", path: "/{*path}", handler: app }],
    live: {
      events:
        options.live?.events &&
        ((req, event) => options.live!.events!(metadata(req), event)),
      notifications:
        options.live?.notifications &&
        ((req, notification) =>
          options.live!.notifications!(metadata(req), notification)),
    },
    upgrade:
      setup.ws &&
      legacySocket({
        ...options.socket,
        setup: setup.ws,
        fields: options.requestFields,
        metadata: (ctx) => metadata(ctx),
        appOf: (ctx, request) => requestApp(ctx, request),
        asError: (error) => asLegacyError(error, options.errors),
        rejectionOf: (outcome) => legacyError(outcome, options.errors),
      }),
  };
}

/** Commands carry the request metadata over the caller's, and `.await` raises the old error on a rejection. */
const legacyDomain = (
  domain: any,
  metadata: Metadata,
  errors?: Record<string, any>,
) =>
  new Proxy(domain, {
    get: (contexts, context) =>
      contexts[context] &&
      new Proxy(contexts[context], {
        get: (aggregates, aggregate) =>
          typeof aggregates[aggregate] === "function" &&
          ((id?: string) => {
            const commands = aggregates[aggregate](id);
            return new Proxy(commands, {
              get: (target, name) =>
                typeof target[name] !== "function" || name === "load"
                  ? target[name]
                  : (payload?: unknown, own?: Metadata) => {
                      const sent = target[name](payload, {
                        ...own,
                        ...metadata,
                      });
                      const awaited = sent.await;
                      return Object.assign(sent, {
                        await: (pattern: string | string[]) => {
                          const waiting = awaited(pattern).catch(
                            (error: unknown) => {
                              throw error instanceof Rejection
                                ? legacyError(error, errors)
                                : error;
                            },
                          );
                          // The old runner waited only once `exec` or `then` consumed it: a wait route code
                          // never consumes raises nothing, instead of ending the process unhandled.
                          waiting.catch(() => {});
                          return waiting;
                        },
                      });
                    },
            });
          }),
      }),
  });

/**
 * An old services folder: each `.js` file is a service named by its first file-name part, under its folder's
 * name (`team/auth.js` is `team:auth`). Services load in folder order; one exporting `init` is built then as
 * `init(store, app)`, where `store.get(name)` returns a service built before it and `store.services` holds
 * those built so far by name. `instances` are added as they are. Returns the old accessor tree
 * (`services.team.auth()`) and `close()`, which closes each service exporting `close`. Like the old store,
 * an instance with `init` is built through it too.
 */
export const legacyServices = (
  directory: string,
  app: unknown,
  instances: Record<string, unknown> = {},
) => {
  const files: Record<string, string> = {};
  const load = (folder: string, namespace = "") => {
    for (const name of fs.readdirSync(folder)) {
      const file = path.join(folder, name);
      if (fs.statSync(file).isDirectory()) load(file, name);
      else if (fs.statSync(file).isFile() && path.extname(file) === ".js")
        files[`${namespace ? `${namespace}:` : ""}${name.split(".", 1)[0]}`] =
          file;
    }
  };
  load(directory);
  const built: Record<string, unknown> = {},
    closers: (() => unknown)[] = [];
  const get = (name: string) => {
    if (!(name in built)) throw new Error(`No service ${name} initialized.`);
    return built[name];
  };
  const store = { services: built, get, service: get };
  const definitions = {
    ...Object.fromEntries(
      Object.entries(files).map(([name, file]) => [name, require(file)]),
    ),
    ...instances,
  };
  for (const [name, service] of Object.entries<any>(definitions)) {
    if (name in built) throw new Error(`Service ${name} already exists.`);
    built[name] =
      typeof service?.init === "function" ? service.init(store, app) : service;
    if (typeof service?.close === "function")
      closers.push(() => service.close());
  }
  // `team:auth` is `services.team.auth()`; a name splits at its first colon only.
  const services: Record<string, any> = {};
  const place = (
    tree: Record<string, any>,
    name: string,
    value: () => unknown,
  ): void => {
    const [namespace, rest] = name.split(/:(.+)/);
    if (!rest) tree[namespace!] = value;
    else place((tree[namespace!] ??= {}), rest, value);
  };
  for (const name of Object.keys(definitions))
    place(services, name, () => get(name));
  return {
    services,
    close: async () => {
      for (const close of closers) await close();
    },
  };
};
