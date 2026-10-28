import { randomUUID } from "node:crypto";
import type { WebSocket, WebSocketServer } from "ws";
import { ForbiddenError } from "./errors.js";
import { createCommand } from "./id.js";
import { legacyQuery } from "./compat.js";
import type { ReadmodelLiveChange } from "./gateway/access.js";
import type { GatewayDefinition, ScopedHandle } from "./runtime/types.js";
import type {
  ApplicationNotification,
  Command,
  Outcome,
  RecordedEvent,
} from "./types.js";

/**
 * The old SPA socket protocol on an EventFabric Gateway (`legacyGateway` serves it when the folder's setup has
 * `ws`): `subscribeRead`, `subscribeEvents`, `sendCommand` and `sendToken` with their old answers, over the
 * caller's Access-scoped handle and the Gateway's live feeds.
 */

/** The `ws` section of an old Gateway setup: middleware lists run at the old points. */
export type LegacySocketSetup = {
  connection?: ((req: any) => unknown)[];
  incoming?: ((message: any) => unknown)[];
  outgoing?: ((message: any, connection: unknown) => unknown)[];
  disconnect?: ((api: any, connection: unknown) => unknown)[];
  subscribtion?: unknown[];
};
export type LegacySocketOptions = {
  setup: LegacySocketSetup;
  /** Fields of every connection's request, such as the Gateway name old auth code records. */
  fields?: Record<string, unknown>;
  /** The metadata the connection middleware put on a connection or message: Commands carry it over their own. */
  metadata: (ctx: any) => Record<string, unknown> | undefined;
  /** The old request Application (`req.getApp()`) for a connection or message. */
  appOf: (ctx: any, request: ScopedHandle) => unknown;
  /** The old error for a thrown error, and for a Command that did not succeed. */
  asError: (error: unknown) => any;
  rejectionOf: (outcome: Exclude<Outcome, { status: "accepted" }>) => any;
  /** Bytes a slow client may leave unsent before it is disconnected. */
  maxBuffer?: number;
  /** Ping interval; a client that misses a pong is disconnected. */
  heartbeat?: number;
  /**
   * Milliseconds between two Readmodel changes a subscription that is not a Command wait delivers; within them
   * only the last change is kept. Off by default.
   */
  coalesce?: number;
  /** How long a Command wait stays open after its Command is accepted; then it receives `commandRejected`. */
  commandWaitTimeout?: number;
};

/** The old subset filter: every field of `subset` equals the same field of `superset`, recursively. */
export const partOf = (subset: any, superset: any): boolean => {
  if (subset === true) return true;
  if (
    typeof subset !== "object" ||
    typeof superset !== "object" ||
    (Boolean(subset) && !superset)
  )
    return false;
  return Object.keys(subset).every((key) => {
    const value = subset[key],
      other = superset[key];
    if (typeof other === "object" && other !== null && value !== null)
      return partOf(value, other);
    return value === other;
  });
};
const routed = <T extends { fullname: string }>(message: T) => ({
  ...message,
  routingKey: `event.${message.fullname}`,
});
/** A recorded Event as the old notification bus carried it. */
export const legacyEvent = (event: RecordedEvent) =>
  routed({
    type: "domain",
    id: event.id,
    name: event.name,
    payload: event.payload,
    metadata: {
      ...event.metadata,
      correlationId: event.correlationId,
      causationId: event.causationId,
    },
    context: event.stream.context,
    aggregate: {
      name: event.stream.aggregate,
      id: event.stream.id,
      revision: event.revision,
    },
    fullname: `domain.${event.stream.context}.${event.stream.aggregate}.${event.name}`,
  });
/** A Readmodel change as the old notification: the whole document, `name` its operation. */
export const legacyChange = (
  change: ReadmodelLiveChange,
  correlationId = change.correlationIds[0],
) =>
  routed({
    type: "readmodel",
    id: randomUUID(),
    name: change.kind,
    payload: change.document,
    metadata: { correlationId, timestamp: Date.now() },
    readmodel: { collection: change.model, type: change.group, id: change.id },
    fullname: `readmodel.${change.group}.${change.model}.${change.kind}`,
  });
/** An Application Notification as the old `system.<name>` notification. */
export const legacyNotification = (notification: ApplicationNotification) =>
  routed({
    type: "system",
    id: randomUUID(),
    name: notification.name,
    payload: notification.payload,
    metadata: {},
    fullname: `system.${notification.name}`,
  });
// The old domain service announced each Command it received and each it rejected.
const commandNotice = (command: Command, name: string, payload: unknown) =>
  routed({
    type: "domain",
    id: randomUUID(),
    name,
    payload,
    metadata: {
      ...command.metadata,
      correlationId: command.correlationId,
      causationId: command.id,
    },
    command: { name: command.name, id: command.id },
    context: command.stream.context,
    aggregate: { name: command.stream.aggregate, id: command.stream.id },
    fullname: `domain.${command.stream.context}.${command.stream.aggregate}.${name}`,
  });
// Without `outgoing` middleware nothing changes a notification, so every socket shares it and its JSON.
const built = new WeakMap<object, object>(),
  serialized = new WeakMap<object, string>();
const reply = (
  type: string,
  correlationId: unknown,
  payload: unknown = {},
  statusCode = 200,
) => ({ type, version: "v1", statusCode, correlationId, payload });
const asJson = (value: unknown) => JSON.parse(JSON.stringify(value ?? {}));
const ignoreDenied = (error: unknown) => {
  if (!(error instanceof ForbiddenError)) throw error;
};
// The old client subscribes `{ metadata: { correlationId } }` before sending a Command to await its results.
const commandWait = (filter: any) =>
  filter?.metadata !== null &&
  typeof filter?.metadata === "object" &&
  Object.keys(filter).length === 1 &&
  Object.keys(filter.metadata).every((key) => key === "correlationId");

export function legacySocket(
  options: LegacySocketOptions,
): NonNullable<GatewayDefinition["upgrade"]> {
  if (options.setup.subscribtion?.length)
    throw new Error("Old socket subscribtion middleware is not supported");
  let server: Promise<WebSocketServer> | undefined;
  return (req, socket, head, scoped) => {
    server ??= import("ws").then(
      ({ WebSocketServer }) => new WebSocketServer({ noServer: true }),
    );
    void (async () => {
      // Only the query counts: the old client asks for `//?token=…`, which a URL parser reads as a host.
      const params = Object.fromEntries(
        new URLSearchParams((req.url ?? "").split("?")[1] ?? ""),
      );
      const data: any = {
        ...options.fields,
        params,
        socket: { uniqueId: params.uniqueId || randomUUID() },
      };
      data.getApp = () => options.appOf(data, scoped(data));
      try {
        for (const middleware of options.setup.connection ?? [])
          await middleware(data);
      } catch {
        socket.end(
          "HTTP/1.1 401 Unauthorized\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
        );
        return;
      }
      (await server!).handleUpgrade(req, socket, head, (ws) =>
        connect(ws, data, scoped, options),
      );
    })().catch(() => socket.destroy());
  };
}

type Wait = {
  correlation?: string;
  deliver: (message: any) => void;
  event: (event: RecordedEvent) => void;
  change: (change: ReadmodelLiveChange) => void;
  timer?: ReturnType<typeof setTimeout>;
};
// At most one delivery per `ms`, the last one kept.
const throttle = (deliver: (old: object) => void, ms: number) => {
  let timer: ReturnType<typeof setTimeout> | undefined,
    held: object | undefined;
  const wait = () => {
    timer = setTimeout(() => {
      timer = undefined;
      if (!held) return;
      const old = held;
      held = undefined;
      deliver(old);
      wait();
    }, ms);
    timer.unref();
  };
  return {
    push: (old: object) => {
      if (timer) return void (held = old);
      deliver(old);
      wait();
    },
    stop: () => clearTimeout(timer),
  };
};
function connect(
  ws: WebSocket,
  data: any,
  scoped: (ctx: any) => ScopedHandle,
  options: LegacySocketOptions,
) {
  const maxBuffer = options.maxBuffer ?? 4 * 1024 * 1024;
  const subscriptions = new Map<unknown, () => unknown>();
  // Command waits that have not seen their Command yet, in subscription order.
  const waits: Wait[] = [];
  const connection = { data, socket: ws };
  const plain = !options.setup.outgoing?.length;
  const send = async (message: any, frame?: () => string) => {
    for (const middleware of options.setup.outgoing ?? [])
      await middleware(message, connection);
    if (ws.readyState !== ws.OPEN) return;
    if (ws.bufferedAmount > maxBuffer) return void ws.terminate();
    ws.send(frame?.() ?? JSON.stringify(message));
  };
  const legacyOf = <T extends object>(item: T, build: (item: T) => object) => {
    if (!plain) return build(item);
    let old = built.get(item);
    if (!old) built.set(item, (old = build(item)));
    return old;
  };
  // The bytes `JSON.stringify(reply("event", correlationId, old))` gives, with `old` serialized once.
  const sendEvent = (correlationId: unknown, old: object) =>
    send(
      reply("event", correlationId, old),
      plain
        ? () => {
            let json = serialized.get(old);
            if (json === undefined)
              serialized.set(old, (json = JSON.stringify(old)));
            const head = JSON.stringify({
              type: "event",
              version: "v1",
              statusCode: 200,
              correlationId,
            });
            return `${head.slice(0, -1)},"payload":${json}}`;
          }
        : undefined,
    );
  /*
   * A socket's Command waits share one change and one Event subscription per Access scope, opened by the first
   * wait and closed after the last; a change's correlation is checked before its Access filter.
   */
  const feeds = new Map<
    string,
    { waits: Set<Wait>; stop: Promise<() => Promise<unknown>> }
  >();
  const follow = async (wait: Wait, key: string, request: ScopedHandle) => {
    let feed = feeds.get(key);
    if (!feed) {
      const waits = new Set<Wait>();
      const waitsFor = (ids: string[]) =>
        [...waits].filter(
          (wait) => wait.correlation && ids.includes(wait.correlation),
        );
      const stop = (async () => {
        const stops = [
          await request.subscribeChanges(
            (change) => {
              for (const wait of waitsFor(change.correlationIds))
                wait.change(change);
            },
            { correlated: (ids) => waitsFor(ids).length > 0 },
          ),
        ];
        await request
          .subscribeEvents((event) => {
            for (const wait of waits) wait.event(event);
          })
          .then((stop) => void stops.push(stop), ignoreDenied);
        return () => Promise.all(stops.map((stop) => stop()));
      })();
      feed = { waits, stop };
      feeds.set(key, feed);
    }
    const current = feed;
    current.waits.add(wait);
    try {
      await current.stop;
    } catch (error) {
      current.waits.delete(wait);
      if (feeds.get(key) === current) feeds.delete(key);
      throw error;
    }
    return async () => {
      current.waits.delete(wait);
      if (current.waits.size || feeds.get(key) !== current) return;
      feeds.delete(key);
      await (
        await current.stop
      )();
    };
  };
  const fail = (correlationId: unknown, error: unknown) => {
    const old = options.asError(error) ?? {};
    const data =
      typeof old.toJson === "function"
        ? old.toJson()
        : old.data || old.payload || {};
    return send(
      reply(
        "error",
        correlationId,
        {
          message: old.message ?? "Internal Server Error",
          code: old.errorCode ?? 500,
          data,
        },
        old.statusCode ?? 500,
      ),
    );
  };
  const remove = (id: unknown) => {
    const stop = subscriptions.get(id);
    subscriptions.delete(id);
    void Promise.resolve(stop?.()).catch(() => {});
  };
  const add = (id: unknown, stop: () => unknown) => {
    remove(id);
    subscriptions.set(id, stop);
  };
  const read = async (message: any, request: ScopedHandle) => {
    const { correlationId } = message;
    const { readmodel = {}, single, query = {} } = message.payload ?? {};
    const repository = (request.readmodels as any)[readmodel.type]?.[
      readmodel.collection
    ];
    if (!repository)
      throw new Error(
        `Readmodel ${readmodel.type}.${readmodel.collection} not found.`,
      );
    let stopped = false;
    if (single) {
      const [filter] = legacyQuery({ filter: query });
      const document = await repository.findOne(filter);
      await send({ type: "subscribedRead", correlationId });
      if (document)
        await send({ type: "item", payload: document, correlationId });
    } else {
      const [filter, readOptions] = legacyQuery(query);
      const total = await repository.count(filter);
      add(correlationId, () => (stopped = true));
      await send({ type: "subscribedRead", correlationId });
      await send({ type: "stats", payload: { total }, correlationId });
      for await (const document of repository.stream(filter, readOptions)) {
        if (stopped || ws.readyState !== ws.OPEN) return;
        await send({ type: "item", payload: document, correlationId });
      }
      subscriptions.delete(correlationId);
    }
    await send({ type: "finish", statusCode: 200, correlationId });
  };
  const subscribe = async (message: any, request: ScopedHandle) => {
    const { correlationId } = message;
    const filter = message.payload?.filter ?? {};
    const stops: (() => unknown)[] = [];
    const stop = () => Promise.all(stops.map((stop) => stop()));
    add(correlationId, stop);
    const emit = (old: object) => {
      if (subscriptions.get(correlationId) === stop)
        void sendEvent(correlationId, old);
    };
    const deliver = (old: object) => {
      if (partOf(filter, old)) emit(old);
    };
    if (commandWait(filter)) {
      // The old client sends Commands without an id, so its filter has no correlation: the Command this
      // socket sends next is the one it waits for.
      // The Command's own Events come from its Outcome and may come again from the live feed.
      const seen = new Set<string>();
      const wait: Wait = {
        correlation: filter.metadata.correlationId,
        deliver,
        event: (event) => {
          if (event.correlationId !== wait.correlation || seen.has(event.id))
            return;
          seen.add(event.id);
          deliver(legacyEvent(event));
        },
        change: (change) => deliver(legacyChange(change, wait.correlation)),
      };
      if (!wait.correlation) waits.push(wait);
      stops.push(() => {
        clearTimeout(wait.timer);
        const at = waits.indexOf(wait);
        if (at >= 0) waits.splice(at, 1);
      });
      stops.push(
        await follow(
          wait,
          JSON.stringify(options.metadata(message) ?? null),
          request,
        ),
      );
    } else {
      const type = filter.type;
      if (type === undefined || type === "domain")
        await request
          .subscribeEvents((event) => deliver(legacyOf(event, legacyEvent)))
          .then((stop) => void stops.push(stop), ignoreDenied);
      if (type === undefined || type === "system")
        await request
          .subscribeNotifications((notification) =>
            deliver(legacyOf(notification, legacyNotification)),
          )
          .then((stop) => void stops.push(stop), ignoreDenied);
      if (type === undefined || type === "readmodel") {
        const { type: group, collection: model } = filter.readmodel ?? {};
        // An auto-refresh only needs to know that something changed.
        const coalesced = options.coalesce
          ? throttle(emit, options.coalesce)
          : undefined;
        if (coalesced) stops.push(coalesced.stop);
        stops.push(
          await request.subscribeChanges(
            (change) => {
              const old = legacyOf(change, legacyChange);
              if (!partOf(filter, old)) return;
              if (coalesced) coalesced.push(old);
              else emit(old);
            },
            { group, model },
          ),
        );
      }
    }
    // Unsubscribed or disconnected while subscribing.
    if (subscriptions.get(correlationId) !== stop) await stop();
    else await send(reply("subscribedEvents", correlationId));
  };
  const sendCommand = (message: any, request: ScopedHandle) => {
    const { correlationId } = message;
    const { context, aggregate, name, payload, metadata } =
      message.payload ?? {};
    const command = createCommand({
      stream: {
        context,
        aggregate: aggregate?.name,
        ...(aggregate?.id != null && { id: aggregate.id }),
      },
      name,
      payload,
      // The connection's metadata over the client's, as the old socket merged them.
      metadata: { ...metadata, ...options.metadata(message) },
    });
    const wait = waits.shift();
    if (wait) wait.correlation = command.correlationId;
    const rejected = (error: unknown) => {
      const old = options.asError(error) ?? {};
      wait?.deliver(
        commandNotice(command, "commandRejected", {
          ...old,
          name: old.name,
          message: old.message,
          code: old.code,
        }),
      );
    };
    void send(reply("sentCommand", correlationId));
    void (async () => {
      wait?.deliver(commandNotice(command, "commandReceived", {}));
      const outcome = await request.sendCommand(command);
      if (outcome.status !== "accepted") throw options.rejectionOf(outcome);
      for (const event of outcome.events) wait?.event(event);
      // The old client waits without a timeout: one awaiting a Readmodel change that never comes would hang.
      if (!wait) return;
      wait.timer = setTimeout(
        () =>
          rejected(
            Object.assign(
              new Error("Command accepted; its results did not arrive in time"),
              { name: "CommandTimeoutError", statusCode: 504, errorCode: 504 },
            ),
          ),
        options.commandWaitTimeout ?? 10000,
      );
      wait.timer.unref();
    })().catch(rejected);
  };
  const handlers: Record<
    string,
    (message: any, request: ScopedHandle) => unknown
  > = {
    sendToken: (message) => {
      (connection as any).token = message.payload;
      return send(reply("sentToken", message.correlationId));
    },
    // A read streams detached from the message queue.
    subscribeRead: (message, request) =>
      void read(message, request).catch((error) => {
        remove(message.correlationId);
        if (error instanceof ForbiddenError)
          return fail(message.correlationId, error);
        return send({
          type: "error",
          statusCode: 500,
          correlationId: message.correlationId,
        });
      }),
    unsubscribeRead: (message) => {
      remove(message.correlationId);
      return send(reply("unsubscribedRead", message.correlationId));
    },
    subscribeEvents: (message, request) =>
      subscribe(message, request).catch((error) => {
        remove(message.correlationId);
        return fail(message.correlationId, error);
      }),
    unsubscribeEvents: (message) => {
      remove(message.correlationId);
      return send(reply("unsubscribedEvents", message.correlationId));
    },
    sendCommand,
  };
  const { getApp, ...fields } = data;
  // Messages are taken in order, so a Command wait always precedes the Command it waits for.
  let queue = Promise.resolve();
  ws.on("message", (raw) => {
    queue = queue.then(async () => {
      let message: any;
      try {
        // Only the protocol's fields come from the client; everything else is the connection's.
        const { type, version, correlationId, payload } = JSON.parse(
          String(raw),
        );
        message = { type, version, correlationId, payload, ...asJson(fields) };
        for (const middleware of options.setup.incoming ?? [])
          await middleware(message);
        if (!message.correlationId)
          throw Object.assign(new Error("corellationId missing."), {
            statusCode: 400,
            errorCode: 400,
          });
        const handler = handlers[message.type];
        if (!handler) return;
        // Each message carries a copy of the connection's fields, which incoming middleware may change.
        await handler(message, scoped({ ...message, getApp }));
      } catch (error) {
        await fail(message?.correlationId, error);
      }
    });
  });
  let alive = true;
  ws.on("pong", () => (alive = true));
  const heartbeat = setInterval(() => {
    if (!alive) return void ws.terminate();
    alive = false;
    ws.ping();
  }, options.heartbeat ?? 30000);
  heartbeat.unref();
  ws.on("close", () => {
    clearInterval(heartbeat);
    for (const id of [...subscriptions.keys()]) remove(id);
    waits.length = 0;
    void (async () => {
      const api = {
        ...options.fields,
        getApp: data.getApp,
        socket: { uniqueId: data.socket.uniqueId },
      };
      for (const middleware of options.setup.disconnect ?? [])
        await Promise.resolve(middleware(api, connection)).catch(() => {});
    })();
  });
}
