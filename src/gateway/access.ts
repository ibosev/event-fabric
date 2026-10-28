import { ForbiddenError } from "../errors.js";
import { workflowName } from "../internal.js";
import type {
  GatewayDefinition,
  Readmodels,
  SagaMessageOptions,
  SendSagaMessage,
} from "../runtime/types.js";
import type { EventPattern } from "../registry.js";
import type { SignalInput, WorkflowCalls } from "../workflows/index.js";
import type { Application } from "../runtime/index.js";
import { awaitEvent } from "../runtime/index.js";
import { buildDomain } from "../kernel/index.js";
import { createCommand, type CommandInput } from "../id.js";
import type {
  ApplicationNotification,
  JsonObject,
  Logger,
  Position,
  RecordedEvent,
} from "../types.js";
import type { Filter } from "../readmodel-store/types.js";
import { matches } from "../readmodel-store/query.js";
import type { ReadOptions } from "../runtime/readmodels.js";
/** One document change as `handle(req).readmodels.changes` delivers it. */
export type ReadmodelLiveChange = {
  group: string;
  model: string;
  id: string;
  position: Position;
  kind: "create" | "update" | "delete";
  document: JsonObject;
  correlationIds: string[];
};
/** Which changes a `subscribeChanges` subscriber wants, checked before the Access map's filters. */
export type ChangeSelection = {
  group?: string;
  model?: string;
  /** Takes a change only when this returns true for its correlation ids. */
  correlated?: (correlationIds: string[]) => boolean;
};
// One live change per raw change and kind, shared by every subscriber, so per-change work can be cached on it.
const liveChanges = new WeakMap<object, Map<string, ReadmodelLiveChange>>();
const ruleFor = (rules: Record<string, any> = {}, keys: string[]) => {
  const key = keys.find((key) => Object.hasOwn(rules, key));
  return key ? rules[key] : false;
};
export function createScopedHandle<T>(
  ctx: T,
  gateway: GatewayDefinition<T>,
  application: Application<any>,
  logger: Logger | undefined = application.config.logger,
) {
  const equality = async (group: string, model: string): Promise<Filter> => {
    const rule = ruleFor(gateway.access?.readmodels, [
      `${group}.${model}`,
      `${group}.*`,
      "*",
    ]);
    const value = typeof rule === "function" ? await rule(ctx) : rule;
    if (!value) throw new ForbiddenError();
    if (value === true) return {};
    if (typeof value !== "object" || Array.isArray(value))
      throw new Error("Access map must return a boolean or equality filter");
    // A missing context value would match unowned documents, so it denies instead.
    if (Object.values(value).some((entry) => entry === undefined))
      throw new ForbiddenError();
    return Object.fromEntries(
      Object.entries(value).map(([path, value]) => [path, { eq: value }]),
    );
  };
  /** Checks the Access map's `workflows` rule for `<workflow>.<call>` and returns the Workflow's name. */
  const allowWorkflow = async (workflow: string | { name: string }, call: string, id: string, payload?: unknown) => {
    const name = workflowName(workflow);
    const rule = ruleFor(gateway.access?.workflows, [`${name}.${call}`, `${name}.*`, "*"]);
    const allowed = typeof rule === "function" ? await rule(ctx, { id, payload }) : rule;
    if (allowed !== true) throw new ForbiddenError();
    return name;
  };
  const repository = (group: string, model: string) => {
    const base = () => application.readmodels.repository(group, model);
    const readOptions = (options?: ReadOptions) => ({
      ...options,
      timeout: options?.timeout ?? gateway.waitTimeout ?? 3000,
    });
    const scopedFilter = async (filter: Filter = {}) => ({
      and: [await equality(group, model), filter],
    });
    const wait = async (options?: ReadOptions) => {
      if (options?.minPosition)
        return application.readmodels.waitFor(
          options.minPosition,
          [group],
          readOptions(options),
        );
      const current = await application.readmodels.status(group);
      return {
        behind: current.behind,
        positions: { [group]: current.position },
      };
    };
    const read = async (id: string, options?: ReadOptions) => {
      const filter = await scopedFilter({ _id: id });
      const status = await wait(options);
      return {
        document: await base().findOne(filter, {
          ...readOptions(options),
          minPosition: undefined,
        }),
        behind: status.behind,
        position: status.positions[group] ?? null,
      };
    };
    return Object.assign(read, {
      status: async (options?: ReadOptions) => {
        await equality(group, model);
        return wait(options);
      },
      find: async (filter: Filter = {}, options?: ReadOptions) => {
        const scoped = await scopedFilter(filter);
        return base().find(scoped, readOptions(options));
      },
      findOne: async (filter: Filter = {}, options?: ReadOptions) => {
        const scoped = await scopedFilter(filter);
        return base().findOne(scoped, readOptions(options));
      },
      count: async (filter: Filter = {}, options?: ReadOptions) => {
        const scoped = await scopedFilter(filter);
        return base().count(scoped, readOptions(options));
      },
      stream: async function* (filter: Filter = {}, options?: ReadOptions) {
        const scoped = await scopedFilter(filter);
        yield* base().stream(scoped, readOptions(options));
      },
      /** The store's own handle: whole for a rule granting the whole model, else read-only within the rule's filter. */
      native: async () => {
        const scope = await equality(group, model);
        // A joined model's documents are limited by the caller's rule for it.
        const joins = { joinScope: equality };
        return Object.keys(scope).length
          ? base().native(scope, joins)
          : base().native(undefined, joins);
      },
    });
  };
  const authorizeGroup = async (group: string) => {
    for (const model of Object.keys(
      application.config.readmodels?.[group]?.models ?? {},
    )) {
      try {
        await equality(group, model);
        return;
      } catch (error) {
        if (!(error instanceof ForbiddenError)) throw error;
      }
    }
    throw new ForbiddenError();
  };
  const readmodels: any = {
    repository,
    waitFor: async (position: any, names: string[], options?: any) => {
      for (const group of names) await authorizeGroup(group);
      return application.readmodels.waitFor(position, names, {
        ...options,
        timeout: options?.timeout ?? gateway.waitTimeout ?? 3000,
      });
    },
    subscribe: async (
      group: string,
      model: string,
      handler: any,
      options: any = {},
    ) =>
      application.readmodels.subscribe(
        group,
        model,
        (notification) =>
          handler({
            kind: notification.kind,
            group: notification.group,
            model: notification.model,
            ids: notification.ids,
            position: notification.position,
            correlationIds: [
              ...new Set(
                notification.changes.flatMap(
                  (change) => change.correlationIds ?? [],
                ),
              ),
            ],
          }),
        {
          ...options,
          filter: { and: [await equality(group, model), options.filter ?? {}] },
        },
      ),
  };
  for (const [group, definition] of Object.entries(
    application.config.readmodels ?? {},
  ))
    readmodels[group] = Object.fromEntries(
      Object.keys(definition.models).map((model) => [
        model,
        repository(group, model),
      ]),
    );
  const authorize = async (input: CommandInput) => {
    const command = createCommand(input);
    const { context, aggregate } = command.stream;
    const rule = ruleFor(gateway.access?.domain, [
      `${context}.${aggregate}.${command.name}`,
      `${context}.${aggregate}`,
      `${context}.*`,
      "*",
    ]);
    const allowed =
      typeof rule === "function" ? await rule(ctx, command) : rule;
    if (allowed !== true) throw new ForbiddenError();
    return {
      ...command,
      metadata: {
        ...(await gateway.commandMetadata?.(ctx, command)),
        ...command.metadata,
      },
    };
  };
  const lane = async () =>
    typeof gateway.lane === "function" ? gateway.lane(ctx) : gateway.lane;
  const sendCommand = (
    input: CommandInput,
    options?: { deadline?: number },
  ) => {
    const promise = (async () => {
      const command = await authorize(input);
      const outcome = await application.sendCommand(command, {
        lane: await lane(),
        ...options,
      });
      if (outcome.status === "failed" || outcome.status === "unknown")
        logger?.error(
          {
            commandId: command.id,
            correlationId: command.correlationId,
            causationId: command.causationId,
            outcome: outcome.status,
            ...(outcome.status === "failed"
              ? { error: outcome.error.message }
              : {}),
          },
          "Gateway Command did not complete successfully",
        );
      return outcome;
    })();
    return Object.assign(promise, {
      await: (pattern: EventPattern | readonly EventPattern[]) =>
        promise.then((outcome) => awaitEvent(outcome, pattern)),
    });
  };
  return {
    ctx,
    logger,
    /** The Application services (`setupServices`). */
    get services(): any {
      return application.getServices();
    },
    /** `getDomain().context.aggregate(id).command(payload, metadata)`, sent like `sendCommand`. */
    getDomain: (): any =>
      buildDomain(
        application.config.aggregates ?? [],
        ({ notBefore: _, replaceKey: __, ...input }) => sendCommand(input),
      ),
    /** Readmodels with the Access map's filters applied to every read. */
    readmodels: readmodels as Readmodels,
    waitTimeout: gateway.waitTimeout ?? 3000,
    sendCommand,
    /**
     * Sends a Saga Message the Access map's `sagaMessages` rule allows; denied without one. It names
     * its instance with `to`, so the rule sees the target, and cannot choose the message id.
     */
    sendSagaMessage: (async (
      saga: string,
      name: string,
      payload?: unknown,
      options?: SagaMessageOptions,
    ) => {
      if (typeof options?.to !== "string" || !options.to)
        throw new Error("A Gateway Saga Message names its instance with `to`");
      if (options.id !== undefined) throw new ForbiddenError();
      const rule = ruleFor(gateway.access?.sagaMessages, [
        `${saga}.${name}`,
        `${saga}.*`,
        "*",
      ]);
      const allowed =
        typeof rule === "function"
          ? await rule(ctx, { payload, to: options.to })
          : rule;
      if (allowed !== true) throw new ForbiddenError();
      await application.sendSagaMessage(saga, name, payload, {
        to: options.to,
        at: options.at,
        metadata: options.metadata,
      });
    }) as SendSagaMessage<Promise<void>>,
    /** Starts a Workflow instance the Access map's `workflows` rules allow: `<workflow>.start`, and its Signal's. */
    startWorkflow: (async (
      workflow: string | { name: string },
      id: string,
      input?: unknown,
      options: { reuse?: boolean; signal?: { name: string; payload?: unknown } } = {},
    ) => {
      const name = await allowWorkflow(workflow, "start", id, input);
      if (options.signal)
        await allowWorkflow(name, `signals.${options.signal.name}`, id, options.signal.payload);
      await application.startWorkflow(name as never, id, input as never, options as never);
    }) as WorkflowCalls["startWorkflow"],
    /** Sends a Signal the Access map's `<workflow>.signals.<name>` rule allows. */
    sendSignal: async (id: string, signal: SignalInput) => {
      await allowWorkflow(signal.workflow, `signals.${signal.name}`, id, signal.payload);
      await application.sendSignal(id, signal);
    },
    /** Queries an instance the Access map's `<workflow>.query` rule allows. */
    queryWorkflow: (async (workflow: string | { name: string }, id: string) => {
      const name = await allowWorkflow(workflow, "query", id);
      return application.queryWorkflow(name as never, id);
    }) as WorkflowCalls["queryWorkflow"],
    /** Live Events the Gateway's `live.events` rule lets this caller see; denied without a rule. */
    async subscribeEvents(handler: (event: RecordedEvent) => unknown) {
      const allowed = gateway.live?.events;
      if (!allowed) throw new ForbiddenError();
      return application.subscribeEvents((event) => {
        if (allowed(ctx, event)) return handler(event);
      });
    },
    /** Application Notifications the Gateway's `live.notifications` rule lets this caller see; denied without a rule. */
    async subscribeNotifications(
      handler: (notification: ApplicationNotification) => unknown,
    ) {
      const allowed = gateway.live?.notifications;
      if (!allowed) throw new ForbiddenError();
      return application.subscribeNotifications((notification) => {
        if (allowed(ctx, notification)) return handler(notification);
      });
    },
    /**
     * Every live change of a document this caller may read, across groups and models, with the document
     * (`document` is its last readable state for a delete, including a move out of the caller's scope). Access
     * rules are evaluated once per model and subscription. `select` drops other changes before any Access check.
     * Subscribers receive the same change objects and must not change them.
     */
    async subscribeChanges(
      handler: (change: ReadmodelLiveChange) => unknown,
      select: ChangeSelection = {},
    ) {
      const scopes = new Map<string, Promise<Filter | null>>();
      const scope = (group: string, model: string) => {
        const key = `${group}.${model}`;
        if (!scopes.has(key))
          scopes.set(
            key,
            equality(group, model).catch((error) => {
              if (error instanceof ForbiddenError) return null;
              throw error;
            }),
          );
        return scopes.get(key)!;
      };
      // Batches are handled one after another, so a subscriber sees changes in commit order.
      let queue = Promise.resolve();
      return application.readmodels.changes((batch) => {
        if (select.group !== undefined && select.group !== batch.group) return;
        const selected = batch.changes.filter(
          (change) =>
            (select.model === undefined || select.model === change.model) &&
            (!select.correlated ||
              select.correlated(change.correlationIds ?? [])),
        );
        if (!selected.length) return;
        queue = queue
          .then(async () => {
            for (const change of selected) {
              const filter = await scope(batch.group, change.model);
              if (!filter) continue;
              // A document moved out of the caller's scope is deleted for them, with the state they could read.
              const before = change.before && matches(change.before, filter),
                after = change.after && matches(change.after, filter);
              if (!before && !after) continue;
              const kind = !after ? "delete" : before ? "update" : "create";
              let kinds = liveChanges.get(change);
              if (!kinds) liveChanges.set(change, (kinds = new Map()));
              let live = kinds.get(kind);
              if (!live)
                kinds.set(
                  kind,
                  (live = {
                    group: batch.group,
                    model: change.model,
                    id: change.id,
                    position: batch.position,
                    kind,
                    document: (after ? change.after : change.before)!,
                    correlationIds: change.correlationIds ?? [],
                  }),
                );
              await handler(live);
            }
          })
          .catch((error) =>
            logger?.warn({ error }, "Readmodel change delivery failed"),
          );
      });
    },
    async awaitCorrelated(
      input: CommandInput,
      pattern: EventPattern,
      options?: Parameters<Application["awaitCorrelated"]>[2],
    ) {
      return application.awaitCorrelated(await authorize(input), pattern, {
        lane: await lane(),
        ...options,
      });
    },
  };
}
