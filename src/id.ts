import { randomBytes } from "node:crypto";
import type { Command, Metadata, StreamId } from "./types.js";
import { Rejection } from "./errors.js";
import type { CommandName, CommandPayload } from "./registry.js";
import { parseTime } from "./internal.js";

/** RFC 9562 UUIDv7: milliseconds first, cryptographic random remainder. */
export const uuidv7 = (now: number = Date.now()): string => {
  if (!Number.isSafeInteger(now) || now < 0 || now > 0xffffffffffff)
    throw new Error("Invalid UUIDv7 timestamp");
  const bytes = randomBytes(16);
  bytes.writeUIntBE(now, 0, 6);
  bytes[6] = (bytes[6]! & 0x0f) | 0x70;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
};
export type CommandInput = {
  stream: Omit<StreamId, "id"> & { id?: string };
  name: string;
  payload?: any;
  metadata?: Metadata;
  id?: string;
  schemaVersion?: number;
  correlationId?: string;
  causationId?: string;
  timestamp?: string;
  traceparent?: string;
};
/**
 * A Command to the Stream `id` by qualified name, e.g. `commandTo("billing.invoice.issue", id, payload)`.
 * Name and payload are checked against the generated `Register`; omit `id` to let the Aggregate generate one.
 */
export const commandTo = <N extends CommandName>(
  name: N,
  id: string | undefined,
  payload: CommandPayload<N>,
  options: Omit<CommandInput, "stream" | "name" | "payload"> = {},
): CommandInput => {
  const parts = String(name).split(".");
  if (parts.length !== 3 || parts.some((part) => !part))
    throw new Error(
      `Command name must be context.aggregate.command, got ${String(name)}`,
    );
  const [context, aggregate, command] = parts as [string, string, string];
  return { ...options, stream: { context, aggregate, id }, name: command, payload };
};
export const createCommand = (input: CommandInput): Command => {
  // Schedule attempts are recorded only by `app.schedule`; the kernel validates the field it receives.
  if (input && "schedule" in input)
    throw new Rejection("validation", "Schedule attempts are created by EventFabric");
  if (
    !input ||
    typeof input !== "object" ||
    !input.stream ||
    !["context", "aggregate"].every(
      (key) =>
        typeof (input.stream as any)[key] === "string" &&
        /^[A-Za-z0-9_-]+$/.test((input.stream as any)[key]),
    ) ||
    (input.stream.id !== undefined && typeof input.stream.id !== "string") ||
    typeof input.name !== "string" ||
    !/^[A-Za-z0-9_-]+$/.test(input.name)
  )
    throw new Rejection("validation", "Invalid Command address");
  for (const key of [
    "id",
    "causationId",
    "correlationId",
    "traceparent",
  ] as const)
    if (
      input[key] !== undefined &&
      (typeof input[key] !== "string" || !input[key])
    )
      throw new Rejection("validation", `Invalid Command ${key}`);
  if (
    input.schemaVersion !== undefined &&
    (!Number.isSafeInteger(input.schemaVersion) || input.schemaVersion < 1)
  )
    throw new Rejection("validation", "Invalid Command Schema version");
  if (
    input.timestamp !== undefined &&
    !Number.isFinite(parseTime(input.timestamp))
  )
    throw new Rejection("validation", "Invalid Command timestamp");
  if (
    input.metadata !== undefined &&
    (!input.metadata ||
      typeof input.metadata !== "object" ||
      Array.isArray(input.metadata))
  )
    throw new Rejection("validation", "Command metadata must be an object");
  const id = input.id ?? uuidv7();
  return {
    ...input,
    id,
    stream: { ...input.stream, id: input.stream.id ?? "" },
    payload: input.payload === undefined ? {} : input.payload,
    metadata: input.metadata ?? {},
    schemaVersion: input.schemaVersion ?? 1,
    correlationId: input.correlationId ?? id,
    timestamp: input.timestamp ?? new Date().toISOString(),
  };
};
