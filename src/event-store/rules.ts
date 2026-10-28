// Rules every Event store adapter shares; adapters only load and persist what these return.
import { createHash, randomUUID } from "node:crypto";
import { canonical } from "../validation.js";
import { LeaseLostError } from "../errors.js";
import { comparePositions } from "../position.js";
import type {
  Command,
  ConsumerStatus,
  DeadLetter,
  OutboxEntry,
  Position,
  SagaMessage,
} from "../types.js";

/** Request identity includes the Schedule marker when present; ordinary fingerprints stay stable. */
export const commandFingerprint = (
  command: Pick<Command, "name" | "stream" | "payload" | "schedule">,
) =>
  createHash("sha256")
    .update(canonical([command.name, command.stream, command.payload, ...(command.schedule ? [command.schedule] : [])]))
    .digest("hex");

// retryAt value that tells a backing-off Consumer group to retry at once.
export const retryNow = new Date(0).toISOString();

/** An operator skip moves past exactly the halted Event, once, under the Lease epoch the caller saw. */
export function skippedEventLetter(skip: {
  group: string;
  eventId: string;
  who: string;
  status: ConsumerStatus | undefined;
  checkpoint: Position | null;
  epoch: number | undefined;
  expectedEpoch?: number;
  partition?: number;
}): DeadLetter & { position: Position } {
  const { group, eventId, status, checkpoint, expectedEpoch } = skip;
  if (group.startsWith("readmodels:") && expectedEpoch === undefined)
    throw new Error("Readmodel skips require Application.skipConsumer");
  if (
    status?.state !== "halted" ||
    status.eventId !== eventId ||
    !status.position
  )
    throw new Error("Consumer is not halted on exactly that Event");
  if (checkpoint && comparePositions(checkpoint, status.position) >= 0)
    throw new Error("Consumer already processed that Event");
  if (expectedEpoch !== undefined && skip.epoch !== expectedEpoch)
    throw new LeaseLostError("Consumer Lease changed during skip");
  return {
    id: randomUUID(),
    kind: "skipped-event",
    group,
    ...(skip.partition ? { partition: skip.partition } : {}),
    eventId,
    position: structuredClone(status.position),
    error: status.error ?? "Operator skip",
    attempts: status.attempts ?? 0,
    who: skip.who,
    createdAt: new Date().toISOString(),
  };
}

/** The dead letter for an Outbox row whose delivery failed for the last time. */
export const outboxLetter = (
  entry: OutboxEntry,
  transactionId: string,
  error: string,
): DeadLetter => ({
  id: entry.id,
  kind: "outbox",
  message: structuredClone(entry),
  transactionId,
  correlationId: entry.message.correlationId,
  error,
  // `attempt` counts earlier failures; this dead-lettering delivery failed too.
  attempts: (entry.attempt ?? 0) + 1,
  createdAt: new Date().toISOString(),
});

/** A replay gets a fresh delivery id and retry budget; a Schedule keeps its original due time. */
export function replayedEntry(entry: OutboxEntry): OutboxEntry {
  const { notBefore: _, ...rest } = entry;
  const schedule = entry.kind === "command" ? (entry.message as Command).schedule : undefined;
  return {
    ...rest,
    id: randomUUID(),
    attempt: 0,
    ...(schedule ? { notBefore: schedule.dueAt, replaceKey: undefined } : {}),
    message:
      entry.kind === "message"
        ? { ...entry.message, attempt: 1 }
        : entry.message,
  };
}

/** The dead letter of a Saga Message whose handler failed its last attempt. */
export const sagaMessageLetter = (message: SagaMessage, error: string): DeadLetter => ({
  id: message.id,
  kind: "saga-message",
  sagaMessage: structuredClone(message),
  group: message.group,
  correlationId: message.correlationId,
  error,
  attempts: (message.attempt ?? 0) + 1,
  createdAt: new Date().toISOString(),
});

export function replayable(letter: DeadLetter | undefined): OutboxEntry {
  if (!letter) throw new Error("Dead letter not found");
  if (!letter.message)
    throw new Error("Skipped Events are replayed through a rebuild");
  return letter.message;
}

/** An existing Outbox row as admission sees it; `replaceKey` is only set while the row still holds its key (not yet claimed). */
export type AdmissionRow = {
  id: string;
  replaceKey?: string | null;
  blocked: boolean;
};

/**
 * Admits new Outbox entries against the rows sharing their ids or keys: refuses a duplicate id
 * and a key held by a dead-lettered row, and returns the undelivered rows the entries replace.
 * A later entry in the same batch replaces an earlier one with the same key.
 */
export function admitOutbox(
  entries: OutboxEntry[],
  existing: AdmissionRow[],
): { replaced: string[]; admitted: [ordinal: number, entry: OutboxEntry][] } {
  const replaced = new Set<string>(),
    seen = new Set<string>(),
    byKey = new Map<string, number>();
  for (const [ordinal, entry] of entries.entries()) {
    const key = entry.replaceKey;
    const held = existing.filter((row) => key && row.replaceKey === key);
    if (held.some((row) => row.blocked))
      throw new Error("Cannot replace a dead-lettered Outbox row");
    if (
      seen.has(entry.id) ||
      existing.some((row) => row.id === entry.id && !held.includes(row))
    )
      throw new Error("Duplicate Outbox id");
    seen.add(entry.id);
    for (const row of held) replaced.add(row.id);
    if (key) byKey.set(key, ordinal);
  }
  return {
    replaced: [...replaced],
    admitted: [...entries.entries()].filter(
      ([ordinal, entry]) =>
        !entry.replaceKey || byKey.get(entry.replaceKey) === ordinal,
    ),
  };
}
