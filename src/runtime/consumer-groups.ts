import { LeaseLostError, StaleEpochError } from "../errors.js";
import { retryNow } from "../event-store/rules.js";
import { measure, recovery } from "../observability.js";
import type { Timings } from "../observability.js";
import { rendezvous } from "../transport/partition.js";
import type {
  ConsumerStatus,
  Lease,
  Logger,
  Persistence,
  Position,
  RecordedEvent,
  Stop,
} from "../types.js";
import { parseProcessGroup, parseTime } from "../internal.js";

/** What differs between Consumer group kinds: where the Checkpoint lives and how writes are fenced. */
export type ConsumerGroupKind = {
  checkpoint(): Promise<Position | null>;
  /** Refuses writes made under an older Lease epoch. */
  fence?(epoch: number): Promise<void>;
  /** A skip that must fence local writes before the Event store records it, then finish them. */
  skip?: {
    prepare(epoch: number, position: Position): Promise<void>;
    finish(epoch: number, position: Position): Promise<void>;
  };
  /** The Readmodel store's id; instances that disagree on it never share the group. */
  store?: () => Promise<string | undefined>;
};

export type ConsumerGroupsOptions = {
  store: Persistence;
  owner: string;
  leaseDuration: number;
  logger: Logger;
  timings?: Timings;
  pause: (ms?: number) => Promise<void>;
  /** Counts delivered wakes; a wake during a tick ends the pause after it before it starts. */
  wakes?: () => number;
  running: () => boolean;
  now?: () => number;
};

/** A Consumer group Partition's id: the group's name for Partition 0, `group#p` for the others. */
export const consumerId = (group: string, partition = 0) =>
  partition ? `${group}#${partition}` : group;
const splitId = (id: string) => {
  const at = id.lastIndexOf("#");
  return at < 0
    ? { group: id, partition: 0 }
    : { group: id.slice(0, at), partition: Number(id.slice(at + 1)) };
};
const idOf = (value: { group: string; partition: number }) =>
  consumerId(value.group, value.partition);
/** The live instances on each store that `instances` advertise for `group`. */
const storesOf = (instances: ConsumerStatus[], group: string) => {
  const owners = new Map<string, string[]>();
  for (const instance of instances) {
    const id = instance.stores?.[group];
    if (id) owners.set(id, [...(owners.get(id) ?? []), instance.lease!.owner]);
  }
  return owners;
};
const describeStores = (owners: Map<string, string[]>) =>
  [...owners].map(([id, names]) => `${id}: ${names.join(", ")}`).join("; ");

const errorMessage = (error: unknown) =>
  error instanceof Error ? error.message : String(error);
export const backoff = (failures: number) =>
  Math.min(300000, 100 * 2 ** Math.min(failures - 1, 20));

/** Owns Lease acquire, fence, renewal and loss, the halt policy, resume and skip for every Consumer group. */
export function createConsumerGroups(options: ConsumerGroupsOptions) {
  const { store, owner, leaseDuration, logger, pause, running } = options;
  const now = options.now ?? Date.now;
  const kinds = new Map<string, ConsumerGroupKind>(),
    leases = new Map<string, Lease>(),
    statuses = new Map<string, ConsumerStatus>(),
    stateSince = new Map<string, number>(),
    expected = new Set<string>(),
    requirements = new Map<string, () => boolean>(),
    loops: Promise<void>[] = [];
  // Lease balancing: each group's preferred owner among the live instances that run it.
  const instanceGroup = `instance:${owner}`,
    preferred = new Map<string, string>(),
    freeSince = new Map<string, number>();
  let advertised: string | undefined,
    advertisedStores = 0;
  /** This instance's store per Readmodel group, and the groups it must leave to instances on another store. */
  const stores = new Map<string, string>(),
    conflicts = new Map<string, string>();
  let storesChecked = false,
    balancing: Promise<void> | undefined;
  const live = (status: ConsumerStatus) =>
    !!status.lease && parseTime(status.lease.expiresAt) > now();
  /** Registers this instance and the groups it runs, then picks every group's preferred owner. */
  const balance = async () => {
    let own = leases.get(instanceGroup);
    if (!own) {
      own =
        (await store.acquireLease(instanceGroup, owner, leaseDuration)) ??
        undefined;
      if (!own) return;
      leases.set(instanceGroup, own);
      advertised = undefined;
    }
    for (const id of expected) {
      const group = splitId(id).group;
      if (stores.has(group)) continue;
      const found = await kinds.get(id)?.store?.();
      if (found) stores.set(group, found);
    }
    // Groups can start after startup, once their rebuild swaps in. A group this instance
    // cannot run now is not advertised, so balancing prefers another instance for it.
    const groups = [...expected].filter((id) => requirements.get(id)?.() ?? true);
    if (advertised !== groups.join() || advertisedStores !== stores.size) {
      await store.setConsumerStatus(
        {
          group: instanceGroup,
          partition: 0,
          checkpoint: null,
          state: "running",
          groups,
          ...(stores.size && { stores: Object.fromEntries(stores) }),
        },
        own,
      );
      advertised = groups.join();
      advertisedStores = stores.size;
    }
    const all = await store.consumers();
    const instances = all.filter(
      (s) => s.group.startsWith("instance:") && live(s),
    );
    // A group split across stores stays with the store most instances use; a tie stops every side.
    for (const [group, mine] of stores) {
      const owners = storesOf(instances, group);
      const own = owners.get(mine)?.length ?? 1;
      const outvoted = [...owners].some(
        ([id, names]) => id !== mine && names.length >= own,
      );
      const message = outvoted ? describeStores(owners) : undefined;
      if (message && conflicts.get(group) !== message)
        logger.error(
          { consumerGroup: group, store: mine, stores: message },
          "Instances run this Readmodel group on different stores; this instance takes none of its Leases until they agree",
        );
      if (message) conflicts.set(group, message);
      else conflicts.delete(group);
    }
    storesChecked = true;
    // Rendezvous with a cap: no instance prefers more than its share of all groups. Every
    // instance computes the same assignment from the same rows, in the same group order.
    const allGroups = [
      ...new Set(instances.flatMap((i) => i.groups ?? [])),
    ].sort();
    const capacity = Math.ceil(
      allGroups.length / Math.max(1, instances.length),
    );
    const load = new Map<string, number>();
    const assigned = new Map<string, string>();
    for (const group of allGroups) {
      const ranked = instances
        .filter((instance) => instance.groups?.includes(group))
        .map((instance) => instance.lease!.owner)
        .sort((a, b) => rendezvous(b, group) - rendezvous(a, group));
      const chosen =
        ranked.find((candidate) => (load.get(candidate) ?? 0) < capacity) ??
        ranked[0];
      if (!chosen) continue;
      assigned.set(group, chosen);
      load.set(chosen, (load.get(chosen) ?? 0) + 1);
    }
    for (const group of expected) {
      const best = assigned.get(group);
      if (best) preferred.set(group, best);
      else preferred.delete(group);
      const held = all.find((s) => idOf(s) === group);
      if (held && live(held)) freeSince.delete(group);
      else if (!freeSince.has(group)) freeSince.set(group, now());
    }
  };
  const balanceOnce = () =>
    (balancing ??= balance().finally(() => {
      balancing = undefined;
    }));
  /** Whether this instance should take a free Lease: it is preferred, or nobody took it for a Lease duration. */
  const mayAcquire = (group: string) =>
    (preferred.get(group) ?? owner) === owner ||
    now() - (freeSince.get(group) ?? now()) > leaseDuration;
  const contexts = new Map<
    string,
    { eventId: string; correlationId: string; causationId?: string }
  >();
  const kind = (id: string): ConsumerGroupKind =>
    kinds.get(id) ?? {
      checkpoint: () =>
        store.checkpoint(splitId(id).group, splitId(id).partition),
    };
  const persisted = async (id: string) =>
    (await store.consumers()).find((status) => idOf(status) === id);
  const status = async (lease: Lease, update: Partial<ConsumerStatus>) => {
    const id = idOf(lease);
    const current = statuses.get(id);
    const value: ConsumerStatus = {
      group: lease.group,
      partition: lease.partition,
      state: "running",
      ...current,
      ...update,
      checkpoint: await kind(id).checkpoint(),
      lease,
    };
    if (current?.state !== value.state) stateSince.set(id, now());
    statuses.set(id, value);
    // A healthy group's row only changes on a state change; skipping the rest keeps the Lease row's lock free.
    const unchanged =
      value.state === "running" &&
      current?.state === "running" &&
      !value.attempts &&
      !current.attempts &&
      !value.error &&
      !current.error &&
      current.lease?.epoch === lease.epoch;
    if (!unchanged) await store.setConsumerStatus(value, lease);
  };
  /** `ready` gates acquiring the Lease, for a Partition that must wait for its group's layout. */
  const run = (
    group: string,
    tick: (lease: Lease) => Promise<boolean>,
    { ready, idle, requires }: {
      ready?: () => Promise<boolean>;
      /** Waits after a tick with no work; the default is the poll pause. */
      idle?: () => Promise<void>;
      /** Whether this instance can do the group's work now, e.g. reach the broker; if not, it holds no Lease. */
      requires?: () => boolean;
    } = {},
  ) => {
    expected.add(group);
    if (requires) requirements.set(group, requires);
    const loop = async () => {
      let failures = 0;
      while (running()) {
        let lease = leases.get(group);
        try {
          if (requires && !requires()) {
            if (lease) {
              leases.delete(group);
              await store.releaseLease(lease);
              logger.warn(
                { consumerGroup: group, epoch: lease.epoch },
                "Lease released: this instance cannot run the group now",
              );
            }
            await pause();
            continue;
          }
          if (kind(group).store) {
            if (!storesChecked) await balanceOnce();
            if (!storesChecked || conflicts.has(splitId(group).group)) {
              if (lease) {
                leases.delete(group);
                await store.releaseLease(lease);
              }
              await pause();
              continue;
            }
          }
          if (lease && (preferred.get(group) ?? owner) !== owner) {
            // Hand over between ticks, so the preferred instance never overlaps this one.
            leases.delete(group);
            await store.releaseLease(lease);
            logger.debug(
              { consumerGroup: group, epoch: lease.epoch, to: preferred.get(group) },
              "Lease handed over",
            );
            continue;
          }
          if (!lease) {
            lease =
              mayAcquire(group) && (!ready || (await ready()))
                ? ((await store.acquireLease(
                    splitId(group).group,
                    owner,
                    leaseDuration,
                    splitId(group).partition,
                  )) ?? undefined)
                : undefined;
            if (!lease) {
              await pause();
              continue;
            }
            leases.set(group, lease);
            logger.debug({ consumerGroup: group, epoch: lease.epoch }, "Lease acquired");
            // A restart or handover must keep a poison Event halted until it succeeds or is skipped.
            const previous = await persisted(group);
            if (previous) statuses.set(group, previous);
            failures = previous?.attempts ?? 0;
            await kind(group).fence?.(lease.epoch);
          }
          if (parseTime(lease.expiresAt) <= now())
            throw new LeaseLostError("Lease expired");
          const woken = options.wakes?.();
          const didWork = await tick(lease);
          failures = 0;
          if (statuses.get(group)?.state !== "waiting")
            await status(lease, {
              state: "running",
              attempts: 0,
              error: undefined,
              eventId: undefined,
              position: undefined,
              waitingOn: undefined,
            });
          // New Events, or a group it reads committing, arrived while it looked: it looks again.
          if (!didWork && woken === options.wakes?.()) {
            const wait = () => idle?.() ?? pause();
            if (statuses.get(group)?.state === "waiting" && options.timings)
              await options.timings.time(group, "waitReads", wait);
            else await wait();
          }
        } catch (error) {
          if (
            error instanceof LeaseLostError ||
            error instanceof StaleEpochError
          ) {
            leases.delete(group);
            logger.debug(
              { consumerGroup: group, epoch: lease?.epoch, error: errorMessage(error) },
              "Lease lost",
            );
            continue;
          }
          if (!running()) break;
          failures++;
          const until = now() + backoff(failures);
          if (lease)
            await status(lease, {
              state: failures >= 5 ? "halted" : "running",
              attempts: failures,
              error: errorMessage(error),
              retryAt: new Date(until).toISOString(),
            }).catch(() => {});
          const halted = failures >= 5;
          logger
            .child({ consumerGroup: group, ...contexts.get(group) })
            .error(
              {
                error: errorMessage(error),
                attempts: failures,
                ...(halted && {
                  state: "halted",
                  recovery: recovery.halted(group, contexts.get(group)?.eventId),
                }),
              },
              halted
                ? "Consumer group halted; retrying the same Event"
                : "Consumer group will retry the same Event",
            );
          while (running() && now() < until) {
            await pause(Math.min(1000, until - now()));
            const current = (await store.consumers()).find(
              (value) => idOf(value) === group,
            );
            if (
              current?.retryAt === retryNow ||
              (current?.lease && current.lease.epoch !== lease?.epoch) ||
              (requires && !requires())
            )
              break;
          }
        }
      }
    };
    loops.push(loop());
  };
  const startRenewals = (): Stop => {
    let renewing: Promise<void> | undefined;
    const timer = setInterval(
      () => {
        const held = [...leases];
        if (renewing) return;
        renewing = (
          held.length
            ? store
                .renewLeases(
                  held.map(([, lease]) => lease),
                  leaseDuration,
                )
                .then((renewed) =>
                  held.forEach(([group, previous], index) => {
                    const current = leases.get(group);
                    if (
                      current?.epoch !== previous.epoch ||
                      current.owner !== previous.owner
                    )
                      return;
                    const lease = renewed[index];
                    if (lease) leases.set(group, lease);
                    else {
                      leases.delete(group);
                      logger.debug(
                        { consumerGroup: group, epoch: previous.epoch },
                        "Lease lost",
                      );
                    }
                  }),
                )
            : Promise.resolve()
        )
          .then(() => (expected.size ? balanceOnce() : undefined))
          .catch((error) =>
            logger.warn({ error: errorMessage(error) }, "Lease renewal failed"),
          )
          .finally(() => {
            renewing = undefined;
          });
      },
      Math.max(10, Math.min(10000, leaseDuration / 3)),
    );
    return async () => {
      clearInterval(timer);
      await renewing;
    };
  };
  /**
   * Holds a group's Lease for a bounded piece of work. Reuses this instance's Lease, or acquires
   * one, renews it while `work` runs and releases it after. `alive` throws once the Lease is lost.
   * `busy` runs instead when another instance holds the Lease.
   */
  const hold = async <T>(
    group: string,
    work: (lease: Lease, alive: () => void) => Promise<T>,
    busy: () => Promise<T>,
  ): Promise<T> => {
    const own = leases.get(group);
    if (own)
      return work(own, () => {
        if (leases.get(group)?.epoch !== own.epoch)
          throw new LeaseLostError("Lease lost");
      });
    let lease = await store.acquireLease(group, owner, leaseDuration);
    if (!lease) return busy();
    logger.debug({ consumerGroup: group, epoch: lease.epoch }, "Lease acquired");
    let lost = false,
      renewing = Promise.resolve();
    const timer = setInterval(
      () => {
        renewing = renewing
          .then(async () => {
            if (lost) return;
            const [renewed] = await store.renewLeases([lease!], leaseDuration);
            if (renewed) lease = renewed;
            else {
              lost = true;
              logger.debug(
                { consumerGroup: group, epoch: lease!.epoch },
                "Lease lost",
              );
            }
          })
          .catch((error) =>
            logger.warn({ error: errorMessage(error) }, "Lease renewal failed"),
          );
      },
      Math.max(10, Math.min(10000, leaseDuration / 3)),
    );
    try {
      return await work(lease, () => {
        if (lost || parseTime(lease!.expiresAt) <= now())
          throw new LeaseLostError("Lease lost");
      });
    } finally {
      clearInterval(timer);
      await renewing;
      await store.releaseLease(lease);
    }
  };
  const consumers = async () => {
    // shortcut: a crashed instance's `instance:` row stays in the store, hidden here once expired; delete rows if they pile up.
    const result = (await store.consumers()).filter(
      (status) => !status.group.startsWith("instance:") || live(status),
    );
    // Instance rows list what each live replica runs; nothing is marked until every one has listed its groups.
    const instances = result.filter((s) => s.group.startsWith("instance:"));
    const declared = new Set(instances.flatMap((i) => i.groups ?? []));
    const known = instances.length > 0 && instances.every((i) => i.groups);
    const [head] = await store.readAll({ reverse: true, limit: 1 });
    for (const status of result) {
      const split = storesOf(instances, status.group);
      if (split.size > 1)
        status.error = `Instances use different Readmodel stores: ${describeStores(split)}`;
      if (
        known &&
        // Rebuild Leases are taken on demand, never listed by an instance.
        !/^(instance|layout|rebuild):/.test(status.group) &&
        !declared.has(idOf(status))
      )
        status.state = "undeclared";
      if (kinds.has(idOf(status)))
        status.checkpoint = await kinds.get(idOf(status))!.checkpoint();
      // The Outbox and the bookkeeping rows have no Checkpoint of their own, so no lag.
      if (head && !/^(outbox$|instance:|layout:|rebuild:)/.test(status.group)) {
        const [processed] = await store.readAll({
          ...(status.checkpoint
            ? { toPosition: status.checkpoint, reverse: true }
            : {}),
          limit: 1,
        });
        status.lagMs = processed
          ? Math.max(
              0,
              parseTime(head.timestamp) - parseTime(processed.timestamp),
            )
          : 0;
        // An undeclared group never catches up; its lag would trip alerts forever.
        if (status.state !== "undeclared")
          measure("eventfabric.consumer.lag", status.lagMs, {
            group: status.group,
            partition: status.partition,
          });
      }
    }
    return result;
  };
  /** Deletes a Saga or Workflow group no live instance declares, with its Checkpoints and Saga state or Workflow instances. */
  const retire = async (group: string) => {
    // A Readmodel group's epochs live in its Readmodel store, so a fresh row would be fenced out.
    if (!parseProcessGroup(group) || group.includes("#"))
      throw new Error(`Only a Saga or Workflow group can be retired, not ${group}`);
    const rows = await store.consumers();
    if (!rows.some((s) => s.group === group))
      throw new Error(`Unknown Consumer group ${group}`);
    const declaring = rows
      .filter(
        (s) =>
          s.group.startsWith("instance:") &&
          live(s) &&
          (!s.groups || s.groups.some((id) => splitId(id).group === group)),
      )
      .map((s) => s.lease!.owner);
    if (declaring.length)
      throw new Error(`${group} is still declared by ${declaring.join(", ")}`);
    await store.retireConsumer(group);
  };
  const skip = async (id: string, eventId: string, who: string) => {
    const { group, partition } = splitId(id);
    const local = kinds.get(id)?.skip;
    if (!local) return store.skipConsumer(group, eventId, who, { partition });
    const current = await persisted(id);
    if (
      current?.state !== "halted" ||
      current.eventId !== eventId ||
      !current.position ||
      !current.lease
    )
      throw new Error("Consumer is not halted on exactly that Event");
    const epoch = current.lease.epoch;
    // Fence local writes first; the durable audit lets a new owner finish after a crash.
    await local.prepare(epoch, current.position);
    await store.skipConsumer(group, eventId, who, {
      expectedEpoch: epoch,
      partition,
    });
    try {
      await local.finish(epoch + 1, current.position);
    } catch (error) {
      if (!(error instanceof StaleEpochError)) throw error;
    }
  };
  return {
    statuses,
    loops,
    define: (group: string, definition: ConsumerGroupKind) => {
      kinds.set(group, definition);
    },
    lease: (group: string) => leases.get(group),
    status,
    /** Names the Event a group is on, for its error logs. */
    observe: (group: string, event: RecordedEvent) => {
      contexts.set(group, {
        eventId: event.id,
        correlationId: event.correlationId,
        causationId: event.causationId,
      });
    },
    run,
    hold,
    startRenewals,
    consumers,
    retire,
    skip,
    stateDuration: (status: ConsumerStatus) =>
      status.state === "running"
        ? 0
        : now() - (stateSince.get(idOf(status)) ?? now()),
    /** Whether every group this instance runs has a live Lease in the store. */
    held: (active: ConsumerStatus[]) =>
      [...expected].every((group) =>
        active.some(
          (status) =>
            idOf(status) === group &&
            status.lease &&
            parseTime(status.lease.expiresAt) > now(),
        ),
      ),
    release: async () => {
      const results = await Promise.allSettled(
        [...leases.values()].map((lease) => store.releaseLease(lease)),
      );
      leases.clear();
      return results;
    },
    clear: () => {
      leases.clear();
      expected.clear();
      requirements.clear();
      preferred.clear();
      freeSince.clear();
      stores.clear();
      conflicts.clear();
      storesChecked = false;
      advertisedStores = 0;
    },
  };
}
export type ConsumerGroups = ReturnType<typeof createConsumerGroups>;
