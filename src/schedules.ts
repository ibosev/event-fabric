import { ConcurrencyError } from "./errors.js";
import { canonical } from "./validation.js";
import type {
  Command,
  OutboxEntry,
  RecordedSchedule,
  ScheduleCancellation,
  ScheduleChange,
  ScheduleGuard,
  StreamId,
} from "./types.js";

export const scheduleKey = (stream: StreamId, name: string) =>
  JSON.stringify([stream.context, stream.aggregate, stream.id, name]);

export function assertScheduleName(name: string) {
  if (typeof name !== "string" || !/^[A-Za-z0-9_.-]{1,128}$/.test(name))
    throw new Error("Invalid Schedule name");
}

export function assertScheduleAttempt(
  schedule: NonNullable<Command["schedule"]>,
) {
  assertScheduleName(schedule.name);
  if (
    typeof schedule.dueAt !== "string" ||
    new Date(schedule.dueAt).toISOString() !== schedule.dueAt
  )
    throw new Error("Invalid Schedule due time");
}

export function assertScheduleChanges(
  stream: StreamId,
  changes: ScheduleChange[],
  outbox: OutboxEntry[],
) {
  const names = new Set<string>();
  for (const { name, entry } of changes) {
    assertScheduleName(name);
    if (names.has(name))
      throw new Error("A Command can change a Schedule only once");
    names.add(name);
    if (!entry) continue;
    const command = entry.message as Command;
    if (
      entry.kind !== "command" ||
      !command.schedule ||
      command.schedule.name !== name ||
      scheduleKey(command.stream, name) !== scheduleKey(stream, name) ||
      entry.notBefore !== command.schedule.dueAt ||
      entry.replaceKey !== undefined ||
      !outbox.some(
        (row) => row.id === entry.id && canonical(row) === canonical(entry),
      )
    )
      throw new Error("Invalid Schedule change");
  }
}

export function assertScheduleGuard(
  current: RecordedSchedule | undefined | null,
  guard: ScheduleGuard | undefined,
  hasEffects: boolean,
) {
  if (!guard) return;
  const matches =
    current?.commandId === guard.commandId &&
    current?.fingerprint === guard.fingerprint;
  if (matches === guard.obsolete)
    throw new ConcurrencyError("Schedule attempt changed");
  if (guard.obsolete && hasEffects)
    throw new Error("An obsolete Schedule cannot record effects");
}

export const isScheduledAttempt = (
  entry: OutboxEntry | undefined,
): entry is OutboxEntry =>
  entry?.kind === "command" && !!(entry.message as Command).schedule;

/** What cancelling a Schedule found: none, an attempt still pending, or one already dispatched. */
export const cancellationOf = (
  previous: { dispatchedAt?: string | null } | undefined | null,
): ScheduleCancellation =>
  !previous ? "inactive" : previous.dispatchedAt ? "dispatched" : "cancelled";
