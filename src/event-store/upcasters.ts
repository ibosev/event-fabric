import { createValidator, toJson } from "../validation.js";
import { copiesInput, parseTime } from "../internal.js";
import { isDeepStrictEqual } from "node:util";
import { assertScheduleAttempt, isScheduledAttempt } from "../schedules.js";
import type {
  Command,
  OutboxEntry,
  PendingEvent,
  Position,
  RecordedEvent,
  Schema,
  StreamId,
  Validate,
} from "../types.js";
export type UpcasterDefinition = {
  version: number;
  upcasters: Record<number, (payload: any) => any | Promise<any>>;
  schema?: Schema;
};
export type EventStoreOptions = {
  upcasters?: Record<string, UpcasterDefinition>;
  validate?: Validate;
  metadataSchema?: Schema;
  /** Default 8192, matching the kernel's `metadataMaxBytes` default. */
  metadataMaxBytes?: number;
};
export function assertMetadata(metadata: any, maxBytes = 8192) {
  assertJson(metadata);
  if (!metadata || Array.isArray(metadata) || typeof metadata !== "object")
    throw new Error("Metadata must be a JSON object");
  if (Buffer.byteLength(JSON.stringify(metadata), "utf8") > maxBytes)
    throw new Error(`Metadata exceeds ${maxBytes} bytes`);
}
export function assertJson(value: any, ancestors = new Set<object>()): void {
  if (typeof value === "string" && value.includes("\0"))
    throw new Error("JSON strings cannot contain NUL");
  if (value === null || typeof value === "string" || typeof value === "boolean")
    return;
  // JSON semantics: nested undefined is dropped (property) or null (array item)
  if (value === undefined && ancestors.size) return;
  if (typeof value === "number" && Number.isFinite(value)) return;
  if (
    typeof value !== "object" ||
    ancestors.has(value) ||
    (!Array.isArray(value) &&
      Object.getPrototypeOf(value) !== Object.prototype &&
      Object.getPrototypeOf(value) !== null)
  )
    throw new Error("Value must be JSON");
  ancestors.add(value);
  for (const child of Object.values(value)) assertJson(child, ancestors);
  ancestors.delete(value);
}
/** `newStream`: the id may be empty, as in a creation Command whose handler assigns it. */
export function assertStream(
  stream: StreamId,
  revision?: number,
  newStream = false,
) {
  for (const field of ["context", "aggregate", "id"] as const)
    if (
      typeof stream?.[field] !== "string" ||
      (!stream[field] && !(newStream && field === "id")) ||
      stream[field].includes("\0")
    )
      throw new Error(`Invalid Stream ${field}`);
  if (
    revision !== undefined &&
    (!Number.isSafeInteger(revision) || revision < 0)
  )
    throw new Error("Invalid expected Revision");
}
export function assertOutbox(entry: OutboxEntry, metadataMaxBytes?: number) {
  if (
    typeof entry.id !== "string" ||
    !entry.id ||
    !["command", "message"].includes(entry.kind)
  )
    throw new Error("Invalid Outbox entry");
  const message = entry.message;
  for (const field of ["id", "name", "correlationId"] as const)
    if (typeof message?.[field] !== "string" || !message[field])
      throw new Error(`Invalid Outbox message ${field}`);
  if (
    !Number.isInteger(message.schemaVersion) ||
    message.schemaVersion < 1 ||
    !Number.isFinite(parseTime(message.timestamp))
  )
    throw new Error("Invalid Outbox message envelope");
  if (entry.kind === "command") {
    const command = message as Command;
    assertStream(command.stream, undefined, true);
    if (command.schedule) assertScheduleAttempt(command.schedule);
  }
  assertJson(message.payload);
  assertMetadata(message.metadata, metadataMaxBytes);
  if (
    entry.notBefore !== undefined &&
    !Number.isFinite(parseTime(entry.notBefore))
  )
    throw new Error("Invalid Outbox notBefore");
}
export function assertLeaseTTL(ttlMs: number) {
  if (!Number.isFinite(ttlMs) || ttlMs <= 0)
    throw new Error("Lease TTL must be positive");
}
// Historical Saga state is imported at log head so no Reaction replays its effects.
export function assertImportAtHead(
  position: Position | null,
  head: Position | null,
) {
  if (
    position?.transactionId !== head?.transactionId ||
    position?.sequence !== head?.sequence
  )
    throw new Error(
      `Sagas can only be imported at the current log head (${head ? `${head.transactionId}/${head.sequence}` : "empty log: null"})`,
    );
}
// A bare Event name would match the same name across every Aggregate.
export function assertEventNames(names?: string[]) {
  for (const name of names ?? [])
    if (typeof name !== "string" || name.split(".").length < 3)
      throw new Error(
        `Event name filter must be fully qualified (context.aggregate.name): ${name}`,
      );
}
const assertUpcasters = (definitions: Record<string, UpcasterDefinition>) => {
  for (const [name, definition] of Object.entries(definitions)) {
    if (!Number.isInteger(definition.version) || definition.version < 1)
      throw new Error(`Invalid Schema version for ${name}`);
    for (let version = 1; version < definition.version; version++) {
      if (typeof definition.upcasters[version] !== "function")
        throw new Error(
          `Missing Upcaster for ${name} Schema version ${version}`,
        );
    }
  }
};
export function eventCodec(options: EventStoreOptions = {}) {
  assertUpcasters(options.upcasters ?? {});
  const upcasters = { ...options.upcasters };
  // Schemas declared by an Aggregate are checked even when the store was given no validator.
  let fallback: Validate | undefined;
  const validate: Validate = (schema, value) =>
    (options.validate ?? (fallback ??= createValidator()))(schema, value);
  const validateMetadata = async (metadata: unknown) => {
    assertMetadata(metadata, options.metadataMaxBytes);
    if (options.metadataSchema !== undefined) {
      await validate(options.metadataSchema, toJson(metadata));
    }
  };
  return {
    async validateOutbox(entry: OutboxEntry, replay = false) {
      assertOutbox(entry, options.metadataMaxBytes);
      // The kernel decides whether an old Schedule still needs current business validation.
      if (replay && isScheduledAttempt(entry)) return;
      await validateMetadata(entry.message.metadata);
    },
    register(definitions: Record<string, UpcasterDefinition>) {
      assertUpcasters(definitions);
      for (const [name, definition] of Object.entries(definitions))
        if (upcasters[name] && !isDeepStrictEqual(upcasters[name], definition))
          throw new Error(`Conflicting Upcasters for ${name}`);
      Object.assign(upcasters, definitions);
    },
    async validate(event: PendingEvent) {
      for (const field of ["id", "name", "commandId", "correlationId"] as const)
        if (
          typeof event[field] !== "string" ||
          !event[field] ||
          event[field].includes("\0")
        )
          throw new Error(`Invalid Event ${field}`);
      for (const field of ["causationId", "traceparent"] as const)
        if (event[field] !== undefined && typeof event[field] !== "string")
          throw new Error(`Invalid Event ${field}`);
      if (
        typeof event.timestamp !== "string" ||
        !/^\d{4}-\d\d-\d\dT/.test(event.timestamp) ||
        !Number.isFinite(parseTime(event.timestamp))
      )
        throw new Error("Invalid Event timestamp");
      assertJson(event.payload);
      await validateMetadata(event.metadata);
      if (!Number.isInteger(event.schemaVersion) || event.schemaVersion < 1)
        throw new Error("Invalid Schema version");
    },
    // Takes ownership of `event`: upcasting changes it in place, so callers pass a copy of anything they keep.
    async read(event: RecordedEvent): Promise<RecordedEvent> {
      const definition =
        upcasters[
          `${event.stream.context}.${event.stream.aggregate}.${event.name}`
        ] ?? upcasters[event.name];
      if (!definition) return event;
      if (event.schemaVersion > definition.version)
        throw new Error(`Unsupported Schema version for ${event.name}`);
      while (event.schemaVersion < definition.version) {
        event.payload = await definition.upcasters[event.schemaVersion]!(
          event.payload,
        );
        assertJson(event.payload);
        event.schemaVersion++;
      }
      if (definition.schema !== undefined) {
        await validate(
          definition.schema,
          options.validate && copiesInput.has(options.validate)
            ? event.payload
            : structuredClone(event.payload),
        );
      }
      return event;
    },
  };
}
