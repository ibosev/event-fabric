import { comparePositions } from "../position.js";
import { measure, count } from "../observability.js";
import type { Timings } from "../observability.js";
import type { JsonObject, Position, Transport, Stop } from "../types.js";
import type {
  BatchNotification,
  ReadmodelGroup,
  Notification,
} from "./types.js";
import type {
  Filter,
  NativeOptions,
  QueryOptions,
} from "../readmodel-store/types.js";
import { matches } from "../readmodel-store/query.js";
import { ReadmodelRebuildingError } from "../errors.js";
export type WaitResult = {
  behind: boolean;
  positions: Record<string, Position | null>;
};
export type ReadOptions<D = any> = QueryOptions<D> & {
  minPosition?: Position;
  timeout?: number;
};
export function createReadmodels(
  groups: Record<string, ReadmodelGroup>,
  transport: Pick<Transport, "subscribe">,
  halted: (group: string) => boolean | Promise<boolean> = () => false,
  rebuilding: (group: string) => boolean = () => false,
  timings?: Timings,
) {
  // The active generation predates the group's definitions, so it cannot answer for them.
  const available = (group: string) => {
    if (rebuilding(group)) throw new ReadmodelRebuildingError(group);
  };
  /** A group's Checkpoint: for a partitioned group, the lowest of its Partitions'. */
  const checkpointOf = (name: string) => {
    const group = groups[name]!;
    return group.store.checkpoint(
      name,
      (group.partitions ?? 1) > 1 ? { partitions: group.partitions } : undefined,
    );
  };
  // Concurrent readers of one group share a status read started in the last 10 ms: each
  // read-your-writes wait polls every 20 ms, so per-request reads multiplied the store load.
  // A shared read is at most 10 ms old; Checkpoints only rise, so it can only report behind.
  const shared = new Map<
    string,
    { at: number; read: Promise<[Position | null, boolean]> }
  >();
  const groupStatus = (name: string) => {
    const cached = shared.get(name);
    if (cached && Date.now() - cached.at < 10) return cached.read;
    const read = Promise.all([checkpointOf(name), halted(name)]);
    shared.set(name, { at: Date.now(), read });
    read.catch(() => {
      if (shared.get(name)?.read === read) shared.delete(name);
    });
    return read;
  };
  const status = async (group: string) => {
    if (!groups[group]) throw new Error(`Unknown Readmodel group ${group}`);
    available(group);
    const [position, behind] = await groupStatus(group);
    return { behind, position };
  };
  const waitFor = async (
    position: Position,
    names: string[],
    { timeout = 3000 }: { timeout?: number } = {},
  ): Promise<WaitResult> => {
    const start = Date.now();
    for (const name of names) {
      if (!groups[name]) throw new Error(`Unknown Readmodel group ${name}`);
      available(name);
    }
    const positions: Record<string, Position | null> = {};
    for (;;) {
      const statuses = await Promise.all(names.map(groupStatus));
      names.forEach((name, index) => (positions[name] = statuses[index]![0]));
      const haltedGroups = statuses.map(([, halted]) => halted);
      const behind = names.some(
        (name, index) =>
          !positions[name] ||
          comparePositions(positions[name]!, position) < 0 ||
          haltedGroups[index],
      );
      if (!behind || Date.now() - start >= timeout) {
        if (behind) count("eventfabric.readmodel.wait_timeouts");
        measure("eventfabric.readmodel.wait_duration", Date.now() - start);
        return { behind, positions };
      }
      await new Promise((resolve) =>
        setTimeout(
          resolve,
          Math.min(20, Math.max(1, timeout - (Date.now() - start))),
        ),
      );
    }
  };
  const repository = (group: string, model: string) => {
    const definition = groups[group];
    if (!definition?.models[model])
      throw new Error(`Unknown Readmodel ${group}.${model}`);
    const { store } = definition;
    const time = <T>(stage: string, work: () => Promise<T>) =>
      timings ? timings.time(group, stage, work) : work();
    /** Waits for `minPosition`; without one a query reports no status, so it reads none. */
    const wait = async (options?: ReadOptions) => {
      available(group);
      if (options?.minPosition)
        await time("status", () =>
          waitFor(options.minPosition!, [group], options),
        );
    };
    const status = (options?: ReadOptions) =>
      time("status", async () => {
        available(group);
        return options?.minPosition
          ? waitFor(options.minPosition, [group], options)
          : await groupStatus(group).then(([position, behind]) => ({
              behind,
              positions: { [group]: position },
            }));
      });
    const read = async (id: string, options?: ReadOptions) => {
      const current = await status(options);
      return {
        document: await time("find", () =>
          store.findOne(group, model, { _id: id }, options),
        ),
        position: current.positions[group] ?? null,
        behind: current.behind,
      };
    };
    return Object.assign(read, {
      find: async (filter: Filter = {}, options?: ReadOptions) => {
        await wait(options);
        return time("find", () => store.find(group, model, filter, options));
      },
      findOne: async (filter: Filter = {}, options?: ReadOptions) => {
        await wait(options);
        return time("find", () => store.findOne(group, model, filter, options));
      },
      count: async (filter: Filter = {}, options?: ReadOptions) => {
        await wait(options);
        return time("find", () => store.count(group, model, filter, options));
      },
      stream: async function* (filter: Filter = {}, options?: ReadOptions) {
        await wait(options);
        yield* store.stream(group, model, filter, options);
      },
      native: async (scope?: Filter, options?: NativeOptions) => {
        available(group);
        return store.native(group, model, scope, options);
      },
    });
  };
  const subscribe = async (
    group: string,
    model: string,
    handler: (notification: Notification) => unknown | Promise<unknown>,
    options: {
      filter?: Filter;
      /** Maximum initial documents, in _id order; zero subscribes to changes only. */
      initialSize?: number;
      bufferSize?: number;
      onError?: (error: Error) => void;
    } = {},
  ) => {
    if (options.initialSize !== undefined &&
        (!Number.isSafeInteger(options.initialSize) || options.initialSize < 0))
      throw new Error("initialSize must be a non-negative safe integer");
    repository(group, model);
    available(group);
    // The initial read holds every change at or before `position`; after it, each document's
    // changes come from one owner in Position order, so the cursor is per document: Partitions
    // commit different documents at equal or decreasing Positions.
    let position: Position | null = null,
      ready = false,
      stopped = false,
      draining = false;
    const buffer: Notification[] = [];
    // shortcut: keeps the 10 000 most recently changed documents; an older one falls back to `position`.
    const delivered = new Map<string, Position>();
    const fresh = (id: string, at: Position) => {
      const last = delivered.get(id) ?? position;
      if (last && comparePositions(at, last) <= 0) return false;
      delivered.delete(id);
      delivered.set(id, at);
      if (delivered.size > 10000)
        delivered.delete(delivered.keys().next().value!);
      return true;
    };
    let stop: Stop = async () => {};
    const drain = async () => {
      if (draining || !ready || stopped) return;
      draining = true;
      try {
        while (buffer.length && !stopped) {
          const notification = buffer.shift()!;
          const changes = notification.changes.filter(
            (change) =>
              fresh(change.id, notification.position) &&
              (!options.filter ||
                (change.before && matches(change.before, options.filter)) ||
                (change.after && matches(change.after, options.filter))),
          );
          if (changes.length)
            await handler({
              ...notification,
              ids: changes.map((c) => c.id),
              changes,
            });
        }
      } catch (error) {
        stopped = true;
        await stop();
        options.onError?.(
          error instanceof Error ? error : new Error(String(error)),
        );
      } finally {
        draining = false;
      }
    };
    stop = await transport.subscribe(
      "readmodels",
      (batch: BatchNotification) => {
        if (stopped || batch.group !== group) return;
        const changes = batch.changes
          .filter((change) => change.model === model)
          .map(({ model: _, ...change }) => change);
        if (!changes.length) return;
        const notification: Notification = {
          kind: batch.kind,
          group,
          model,
          ids: changes.map((change) => change.id),
          position: batch.position,
          changes,
        };
        if (buffer.length >= (options.bufferSize ?? 256)) {
          stopped = true;
          void stop();
          options.onError?.(
            new Error("Notification buffer overflow; subscribe and read again"),
          );
          return;
        }
        buffer.push(notification);
        void drain();
      },
    );
    try {
      // Read the lower bound first: a concurrent commit may cause a duplicate, never a missed Notification.
      position = await checkpointOf(group);
      const store = groups[group]!.store;
      let initial: JsonObject[];
      if (options.initialSize === undefined)
        initial = await store.find(group, model, options.filter ?? {});
      else {
        initial = [];
        for await (const document of store.stream(group, model, options.filter ?? {}, { size: options.initialSize }))
          initial.push(document);
      }
      ready = true;
      void drain();
      return {
        initial,
        position,
        stop: async () => {
          stopped = true;
          await stop();
        },
      };
    } catch (error) {
      await stop();
      throw error;
    }
  };
  /** Every committed live change of every group, unfiltered: Gateways scope it with `readmodels.changes`. */
  const changes = (handler: (batch: BatchNotification) => unknown) =>
    transport.subscribe("readmodels", (batch: BatchNotification) => {
      handler(batch);
    });
  const api: any = { waitFor, repository, subscribe, status, changes };
  for (const [group, { models }] of Object.entries(groups))
    api[group] = Object.fromEntries(
      Object.keys(models).map((model) => [model, repository(group, model)]),
    );
  return api as {
    waitFor: typeof waitFor;
    repository: typeof repository;
    subscribe: typeof subscribe;
    status: typeof status;
    changes: typeof changes;
    [group: string]: any;
  };
}
