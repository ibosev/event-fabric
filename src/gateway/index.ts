import { createHash } from "node:crypto";
import { createServer } from "node:http";
import type { IncomingMessage, ServerResponse } from "node:http";
import type { Duplex } from "node:stream";
import express from "express";
import type {
  Request,
  Response,
  RequestHandler,
  ErrorRequestHandler,
} from "express";
import { createScopedHandle } from "./access.js";
import {
  ForbiddenError,
  ReadmodelRebuildingError,
  Rejection,
} from "../errors.js";
import { createCommand, uuidv7 } from "../id.js";
import type { CommandInput } from "../id.js";
import { comparePositions, encodePosition } from "../position.js";
import { minPosition } from "./min-position.js";
import type { Application } from "../runtime/index.js";
import { awaitEvent, patternMatcher } from "../runtime/index.js";
import type { GatewayDefinition } from "../runtime/types.js";
import type {
  ReadmodelDocuments,
  ReadmodelGroupName,
  ReadmodelModelName,
  ReadmodelName,
} from "../registry.js";
import type { Filter } from "../readmodel-store/types.js";
import type { Logger, Outcome } from "../types.js";
export { createScopedHandle } from "./access.js";
export type { ReadmodelLiveChange } from "./access.js";
const handles = new WeakMap<object, ReturnType<typeof createScopedHandle>>();
export const handle = <T = any>(
  request: object,
): ReturnType<typeof createScopedHandle<T>> => {
  const value = handles.get(request);
  if (!value) throw new Error("Request is outside a Gateway");
  return value as ReturnType<typeof createScopedHandle<T>>;
};
const commands = new WeakMap<object, { id: string; correlationId: string }>();
// Command ids derived from a request's Idempotency-Key and the Gateway's scope.
const keyedIds = new WeakMap<object, string>();
/** A UUID-shaped id (version 8) derived from the scope and the client's key; the legacy Gateway derives it once route middleware ran. */
export const keyedId = (scope: string, key: string) => {
  const hex = createHash("sha256")
    .update(JSON.stringify([scope, key]))
    .digest("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-8${hex.slice(13, 16)}-${((parseInt(hex[16]!, 16) & 3) | 8).toString(16)}${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
};
const activeSse = new WeakMap<object, Set<() => void>>();
const requestServers = new WeakMap<object, object>();
export function sse(
  request: IncomingMessage,
  response: ServerResponse,
  {
    heartbeat = 15000,
    retry = 3000,
    maxBuffer = 256 * 1024,
  }: { heartbeat?: number; retry?: number; maxBuffer?: number } = {},
) {
  response.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
  });
  response.write(`retry: ${retry}\n\n`);
  let closed = false;
  const close = () => {
    if (closed) return;
    closed = true;
    clearInterval(timer);
    const server = requestServers.get(request);
    if (server) activeSse.get(server)?.delete(close);
    response.end();
  };
  const timer = setInterval(() => {
    if (response.writableLength > maxBuffer) close();
    else response.write(": heartbeat\n\n");
  }, heartbeat);
  request.on("close", close);
  response.on("close", close);
  const server = requestServers.get(request);
  if (server) {
    if (!activeSse.has(server)) activeSse.set(server, new Set());
    activeSse.get(server)!.add(close);
  }
  return {
    send(event: string, data: unknown, id?: string) {
      if (closed) return false;
      if (/\r|\n/.test(event) || (id && /\r|\n/.test(id)))
        throw new Error("Invalid SSE event or id");
      const frame = `${id ? `id: ${id}\n` : ""}event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
      if (response.writableLength + Buffer.byteLength(frame) > maxBuffer) {
        close();
        return false;
      }
      return response.write(frame);
    },
    close,
  };
}
export async function startGateway(
  name: string,
  definition: GatewayDefinition,
  {
    application,
    invoke,
    logger,
    timeout,
  }: {
    application: Application<any>;
    invoke: (...args: any[]) => Promise<void>;
    logger: Logger;
    timeout: number;
  },
) {
  const router = express();
  router.disable("x-powered-by");
  if (definition.serverTiming)
    router.use((_req, res, next) => {
      const started = performance.now(),
        writeHead = res.writeHead;
      res.writeHead = function (this: Response, ...args: any[]) {
        res.setHeader(
          "Server-Timing",
          `total;dur=${(performance.now() - started).toFixed(2)}`,
        );
        return (writeHead as any).apply(this, args);
      } as typeof res.writeHead;
      next();
    });
  router.use(
    express.json({
      limit: definition.bodyLimit ?? "1mb",
      ...(definition.rawBody && {
        verify: (req: any, _res, body) => {
          req.rawBody = body;
        },
      }),
    }),
  );
  for (const middleware of definition.middleware ?? []) router.use(middleware);
  let unscopedKeyWarned = false;
  router.use(async (req, _res, next) => {
    const ctx = definition.context ? await definition.context(req) : {};
    const key = req.get("Idempotency-Key");
    if (key) {
      const scope = await definition.idempotencyScope?.(ctx);
      if (scope) keyedIds.set(req, keyedId(scope, key));
      else if (!definition.idempotencyScope && !unscopedKeyWarned) {
        unscopedKeyWarned = true;
        logger.warn(
          { gateway: name },
          "Idempotency-Key ignored: the Gateway has no idempotencyScope",
        );
      }
    }
    handles.set(
      req,
      createScopedHandle(
        ctx,
        definition,
        application,
        logger.child({ gateway: name }),
      ),
    );
    next();
  });
  const routes = definition.routes ?? [];
  for (const route of routes) {
    const method = route.method.toLowerCase();
    if (
      ![
        "get",
        "post",
        "put",
        "patch",
        "delete",
        "options",
        "head",
        "all",
      ].includes(method)
    )
      throw new Error(`Unsupported HTTP method ${route.method}`);
    (router as any)[method](route.path, route.handler);
  }
  await invoke({
    name,
    router,
    routes: Object.freeze(
      routes.map(({ handler, ...route }) => Object.freeze(route)),
    ),
  });
  const errorMiddleware: ErrorRequestHandler = (error, req, res, _next) => {
    if (res.headersSent) {
      res.end();
      return;
    }
    if (error instanceof AcceptedCommandError) {
      logger.warn(
        {
          gateway: name,
          path: req.path,
          commandId: commands.get(req)?.id,
          correlationId: commands.get(req)?.correlationId,
          error: error.message,
        },
        "Gateway answered 202: the Command is recorded but building its response failed",
      );
      res.status(202).json(error.outcome);
      return;
    }
    if (error instanceof ForbiddenError) {
      res.status(403).json({ code: "forbidden", message: "Forbidden" });
      return;
    }
    if (error instanceof ReadmodelRebuildingError) {
      res
        .status(503)
        .set("Retry-After", "5")
        .json({ code: error.code, message: error.message });
      return;
    }
    if (error instanceof Rejection) {
      res
        .status(
          error.kind === "validation"
            ? 400
            : error.kind === "existence"
              ? 404
              : 422,
        )
        .json({
          code: error.code,
          message: error.message,
          details: error.details,
        });
      return;
    }
    if (error?.status === 400 || error?.status === 413) {
      res
        .status(error.status)
        .json({
          code: "invalid-request",
          message:
            error.status === 413
              ? "Request body too large"
              : "Invalid request body",
        });
      return;
    }
    logger.error(
      {
        gateway: name,
        path: req.path,
        commandId: commands.get(req)?.id,
        correlationId: commands.get(req)?.correlationId,
        error: error instanceof Error ? error.message : String(error),
      },
      "Gateway request failed",
    );
    res.status(500).json({ code: "internal-error", message: "Request failed" });
  };
  router.use(errorMiddleware);
  const server = createServer((req, res) => {
    requestServers.set(req, server);
    router(req, res);
  });
  let stopped = false;
  const upgraded = new Set<Duplex>();
  server.on("upgrade", (req, socket, head) => {
    if (!definition.upgrade || stopped) return void socket.destroy();
    upgraded.add(socket);
    socket.once("close", () => upgraded.delete(socket));
    try {
      definition.upgrade(req, socket, head, (ctx) =>
        createScopedHandle(
          ctx,
          definition,
          application,
          logger.child({ gateway: name }),
        ),
      );
    } catch (error) {
      logger.error({ gateway: name, error }, "Gateway upgrade failed");
      socket.destroy();
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(definition.port, definition.host ?? "127.0.0.1", () => {
      server.removeListener("error", reject);
      resolve();
    });
  });
  logger.info(
    { gateway: name, address: server.address() },
    "Gateway listening",
  );
  return async () => {
    if (stopped) return;
    stopped = true;
    for (const close of activeSse.get(server) ?? []) close();
    for (const socket of upgraded) socket.destroy();
    await new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        server.closeAllConnections();
        resolve();
      }, timeout);
      server.close(() => {
        clearTimeout(timer);
        resolve();
      });
      server.closeIdleConnections();
    });
  };
}
/** A step after an accepted Command failed; the client still gets the Outcome, never an error status. */
class AcceptedCommandError extends Error {
  constructor(
    readonly outcome: Outcome,
    cause: unknown,
  ) {
    super(cause instanceof Error ? cause.message : String(cause), { cause });
  }
}
const outcomeResponse = (
  outcome: Outcome,
  res: Response,
  create: boolean,
  codes: Record<string, number>,
) => {
  if (outcome.status === "accepted")
    return res.status(create ? 201 : 200).json(outcome);
  if (outcome.status === "unknown") return res.status(504).json(outcome);
  if (outcome.status === "failed")
    return res
      .status(500)
      .json({
        status: "failed",
        error: { code: "command-failed", message: "Command failed" },
      });
  const kind = outcome.error.kind ?? outcome.error.code;
  return res
    .status(
      codes[outcome.error.code] ??
        (
          {
            validation: 400,
            existence: 404,
            precondition: 422,
            rule: 422,
            conflict: 409,
          } as Record<string, number>
        )[kind] ??
        422,
    )
    .json(outcome);
};
export function command(
  input: CommandInput | ((req: Request) => CommandInput),
  options: {
    creates?: boolean;
    /** The Readmodel `"group.model"` whose document answers an accepted Command. */
    respondWith?: ReadmodelName;
    waitTimeout?: number;
    deadline?: number;
    await?: string;
    codes?: Record<string, number>;
  } = {},
): RequestHandler {
  const [group, model] = options.respondWith?.split(".") ?? [];
  if (options.respondWith && (!group || !model))
    throw new Error("respondWith requires group.readmodel");
  return async (req, res) => {
    const request = handle(req),
      responseId = uuidv7();
    commands.set(req, { id: responseId, correlationId: responseId });
    res.set("EventFabric-Command-Id", responseId);
    const value =
      typeof input === "function"
        ? input(req)
        : {
            ...input,
            payload: input.payload === undefined ? req.body : input.payload,
          };
    const cmd = createCommand({
      ...value,
      id: value.id ?? keyedIds.get(req) ?? responseId,
    });
    commands.set(req, cmd);
    res.set("EventFabric-Command-Id", cmd.id);
    const minimum = minPosition(req);
    const outcome = await request.sendCommand(cmd, {
      deadline: options.deadline,
    });
    if (outcome.status !== "accepted") {
      outcomeResponse(outcome, res, !!options.creates, options.codes ?? {});
      return;
    }
    if (outcome.duplicate) res.set("EventFabric-Duplicate", "true");
    const position = outcome.events.at(-1)?.position;
    const readPosition =
      minimum && (!position || comparePositions(minimum, position) > 0)
        ? minimum
        : position;
    if (position) res.set("EventFabric-Position", encodePosition(position));
    // From here on the Command is recorded: an error status would invite a retry that repeats it.
    try {
      if (options.await) awaitEvent(outcome, options.await);
    } catch (error) {
      throw new AcceptedCommandError(outcome, error);
    }
    if (group && model) {
      const id = outcome.events.at(-1)?.stream.id ?? cmd.stream.id;
      let result;
      try {
        const repository = request.readmodels[group]?.[model];
        if (!repository) throw new ForbiddenError();
        result = await repository(id, {
          minPosition: readPosition,
          timeout: options.waitTimeout ?? request.waitTimeout,
        });
      } catch (error) {
        // Only the document waits for the rebuild; that is expected, so it is not logged.
        if (error instanceof ReadmodelRebuildingError) {
          res.status(202).json(outcome);
          return;
        }
        throw new AcceptedCommandError(outcome, error);
      }
      if (result.behind)
        res.set(
          "EventFabric-Behind",
          result.position ? encodePosition(result.position) : "none",
        );
      if (!result.document) {
        res.status(202).json(outcome);
        return;
      }
      res.status(options.creates ? 201 : 200).json(result.document);
      return;
    }
    outcomeResponse(outcome, res, !!options.creates, options.codes ?? {});
  };
}
export function readmodel<
  G extends ReadmodelGroupName,
  M extends ReadmodelModelName<G>,
>(
  group: G,
  model: M,
  // NoInfer: only the names pick the model, so a filter is checked against its document
  // and not against every model of the group.
  options: NoInfer<{
    id?: (req: Request) => string;
    filter?: (req: Request) => Filter<ReadmodelDocuments[`${G}.${M}`]>;
    waitTimeout?: number;
  }> = {},
): RequestHandler {
  return async (req, res) => {
    const request = handle(req),
      repository = request.readmodels[group]?.[model];
    if (!repository) throw new ForbiddenError();
    const position = minPosition(req);
    if (options.id) {
      const result = await repository(options.id(req), {
        minPosition: position,
        timeout: options.waitTimeout ?? request.waitTimeout,
      });
      if (result.position)
        res.set("EventFabric-Position", encodePosition(result.position));
      if (result.behind)
        res.set(
          "EventFabric-Behind",
          result.position ? encodePosition(result.position) : "none",
        );
      res.status(result.document ? 200 : 404).json(result.document);
      return;
    }
    const status = await repository.status({
      minPosition: position,
      timeout: options.waitTimeout ?? request.waitTimeout,
    });
    const served = status.positions[group];
    if (served) res.set("EventFabric-Position", encodePosition(served));
    if (status.behind)
      res.set("EventFabric-Behind", served ? encodePosition(served) : "none");
    res.json(await repository.find(options.filter?.(req) ?? {}));
  };
}
/**
 * The caller's live feed over SSE: `?events=<pattern>,…` (Events by name, under the Gateway's `live.events`
 * rule), `?notifications=<name>,…` (`*` for every name, under `live.notifications`) and
 * `?readmodels=<group>.<model>,…` (Readmodel Notifications under the Access map). Items arrive as SSE events
 * named `event`, `notification` and `readmodel`. A denied subscription answers 403 before the stream opens.
 */
export function live(options: Parameters<typeof sse>[2] = {}): RequestHandler {
  return async (req, res) => {
    const request = handle(req);
    const list = (key: string) =>
      typeof req.query[key] === "string"
        ? req.query[key].split(",").filter(Boolean)
        : [];
    const events = list("events").map(patternMatcher),
      notifications = new Set(list("notifications")),
      readmodels = list("readmodels").map((name) => name.split("."));
    if (readmodels.some((name) => name.length !== 2))
      return void res
        .status(400)
        .json({ code: "bad-request", message: "readmodels are <group>.<model>" });
    // shortcut: no Last-Event-ID resume; a reconnecting client reads again what it shows.
    let stream: ReturnType<typeof sse> | undefined;
    const pending: [string, unknown][] = [];
    const send = (name: string, data: unknown) =>
      stream ? stream.send(name, data) : pending.push([name, data]);
    const stops: (() => Promise<void>)[] = [];
    const stopAll = () => Promise.all(stops.map((stop) => stop()));
    try {
      if (events.length)
        stops.push(
          await request.subscribeEvents((event) => {
            const name = `${event.stream.context}.${event.stream.aggregate}.${event.name}`;
            if (events.some((matches) => matches(name))) send("event", event);
          }),
        );
      if (notifications.size)
        stops.push(
          await request.subscribeNotifications((notification) => {
            if (notifications.has("*") || notifications.has(notification.name))
              send("notification", notification);
          }),
        );
      for (const [group, model] of readmodels)
        stops.push(
          (
            await request.readmodels.subscribe(
              group!,
              model!,
              (notification: any) =>
                send("readmodel", {
                  ...notification,
                  position: encodePosition(notification.position),
                }),
              { initialSize: 0, onError: () => stream?.close() },
            )
          ).stop,
        );
    } catch (error) {
      await stopAll();
      throw error;
    }
    res.on("close", () => void stopAll());
    stream = sse(req, res, options);
    for (const [name, data] of pending.splice(0)) stream.send(name, data);
  };
}
