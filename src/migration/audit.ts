import { isDeepStrictEqual } from "node:util";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { createKernel, loadAggregate } from "../kernel/index.js";
import type { AggregateDefinition, KernelOptions } from "../kernel/index.js";
import { createCommand } from "../id.js";
import { createInMemoryEventStore } from "../event-store/memory.js";
import { readmodelOrder } from "../runtime/dependencies.js";
import { createApplication } from "../runtime/index.js";
import type { ApplicationConfig } from "../runtime/types.js";
import type { Command, RecordedEvent, StreamId } from "../types.js";
const canonical = (value: any): any => JSON.parse(JSON.stringify(value));
export type ReplayDifference = {
  id: string;
  kind: "state" | "command" | "readmodel";
  expected: any;
  actual: any;
  explanation?: { reason: string; decision: string };
};
export type ReplayAudit = {
  checked: { streams: number; commands: number; readmodels: number };
  skipped: { id: string; reason: string }[];
  differences: ReplayDifference[];
  passed: boolean;
};
export type ReplayAuditInput = {
  definitions: AggregateDefinition[];
  streams?: { stream: StreamId; events: RecordedEvent[]; expectedState: any }[];
  commands?: {
    command: Command;
    history: RecordedEvent[];
    expectedEvents: any[];
    skip?: string;
  }[];
  kernel?: Omit<KernelOptions, "definitions" | "store">;
  readmodels?: {
    config: ApplicationConfig;
    history: RecordedEvent[];
    expected: { group: string; model: string; documents: any[] }[];
  };
  explanations?: Record<string, { reason: string; decision: string }>;
};
export const auditReplay = async (
  input: ReplayAuditInput,
): Promise<ReplayAudit> => {
  const result: ReplayAudit = {
    checked: { streams: 0, commands: 0, readmodels: 0 },
    skipped: [],
    differences: [],
    passed: false,
  };
  const compare = (
    id: string,
    kind: ReplayDifference["kind"],
    expected: any,
    actual: any,
  ) => {
    if (!isDeepStrictEqual(canonical(expected), canonical(actual))) {
      const explanation = input.explanations?.[id];
      if (explanation && (!explanation.reason || !explanation.decision))
        throw Error(
          `Difference ${id} requires both a reason and a decision link`,
        );
      result.differences.push({
        id,
        kind,
        expected: canonical(expected),
        actual: canonical(actual),
        explanation,
      });
    }
  };
  for (const fixture of input.streams ?? []) {
    const definition = input.definitions.find(
      (definition) =>
        definition.context === fixture.stream.context &&
        definition.name === fixture.stream.aggregate,
    );
    if (!definition)
      throw Error(
        `Missing Aggregate definition for ${JSON.stringify(fixture.stream)}`,
      );
    compare(
      `state:${JSON.stringify(fixture.stream)}`,
      "state",
      fixture.expectedState,
      loadAggregate(definition, fixture.stream.id, fixture.events).get(),
    );
    result.checked.streams++;
  }
  for (const fixture of input.commands ?? []) {
    if (fixture.skip) {
      result.skipped.push({ id: fixture.command.id, reason: fixture.skip });
      continue;
    }
    const store = createInMemoryEventStore();
    await store.import(fixture.history);
    const clock = () =>
      new Date(
        fixture.expectedEvents[0]?.timestamp ?? fixture.command.timestamp,
      );
    const outcome = await createKernel({
      ...input.kernel,
      definitions: input.definitions,
      store,
      clock: input.kernel?.clock ?? clock,
    }).handle(fixture.command);
    const fields = (event: any) => ({
      name: event.name,
      payload: event.payload,
      metadata: event.metadata,
    });
    compare(
      `command:${fixture.command.id}`,
      "command",
      fixture.expectedEvents.map(fields),
      outcome.status === "accepted" ? outcome.events.map(fields) : outcome,
    );
    result.checked.commands++;
  }
  if (input.readmodels) {
    const { config, history, expected } = input.readmodels;
    await config.store.import(history);
    const application = createApplication(config);
    try {
      for (const group of readmodelOrder(application.config.readmodels ?? {}))
        await application.rebuild(group);
      for (const fixture of expected) {
        const group = application.config.readmodels?.[fixture.group];
        if (!group) throw Error(`Unknown Readmodel group ${fixture.group}`);
        const sort = (documents: any[]) =>
          documents
            .map(canonical)
            .sort((a, b) => String(a._id).localeCompare(String(b._id)));
        compare(
          `readmodel:${fixture.group}.${fixture.model}`,
          "readmodel",
          sort(fixture.documents),
          sort(await group.store.find(fixture.group, fixture.model)),
        );
        result.checked.readmodels++;
      }
      const halted = (await config.store.consumers()).filter(
        (status) => status.state === "halted",
      );
      if (halted.length)
        throw Error(
          `Replay halted: ${halted.map((status) => status.group).join(", ")}`,
        );
    } finally {
      await application.close();
    }
  }
  result.passed =
    Object.values(result.checked).some((count) => count > 0) &&
    result.differences.every(
      (difference) => difference.explanation !== undefined,
    );
  return result;
};

// The old Mongo stores kept BSON values; imported Events and Saga state are JSON.
const plainJson = (
  value: any,
  path: string,
  what: string,
  dates?: "iso" | "epoch",
): any => {
  if (value instanceof Date) {
    if (!dates)
      throw Error(
        `${what} ${path} is a Date; pass dates: "iso" or "epoch" and update code that calls Date methods on it`,
      );
    return dates === "iso" ? value.toISOString() : value.getTime();
  }
  if (Array.isArray(value))
    return value.map((item, index) =>
      plainJson(item, `${path}[${index}]`, what, dates),
    );
  if (value && typeof value === "object") {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null)
      throw Error(
        `${what} ${path} is a ${value.constructor?.name ?? "non-plain object"}, not JSON; convert it by hand`,
      );
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [
        key,
        plainJson(item, path ? `${path}.${key}` : key, what, dates),
      ]),
    );
  }
  if (
    ["function", "symbol", "bigint"].includes(typeof value) ||
    (typeof value === "number" && !Number.isFinite(value))
  )
    throw Error(`${what} ${path} is not JSON; convert it by hand`);
  return value;
};

/**
 * Source revisions must be declared; importing never guesses or rewrites the source log.
 * Pass the domain Event's own id, not the storage wrapper's; a Date timestamp becomes ISO.
 */
export const migrateHistoricalEvent = (
  source: any,
  options: { revisionBase: 0 | 1; sequence?: number; dates?: "iso" | "epoch" },
): { event: RecordedEvent; reviews: string[] } => {
  // System fields leave metadata first: the old timestamp there may be a Date.
  const metadata = plainJson(
      mapMetadata(source.metadata),
      "metadata",
      "Event",
      options.dates,
    ),
    reviews: string[] = [];
  if (metadata.email !== undefined || metadata.owner?.email !== undefined)
    reviews.push(
      "Decide whether email belongs in erasable payload before importing this Event",
    );
  const stream = source.stream;
  const prefix = `${stream.context}.${stream.aggregate}.`;
  const name = String(source.name).replace(/^domain\./, "");
  const event: RecordedEvent = {
    id: source.id ?? source.eventId,
    stream: structuredClone(stream),
    name: name.startsWith(prefix) ? name.slice(prefix.length) : name,
    payload: plainJson(source.payload ?? {}, "payload", "Event", options.dates),
    metadata,
    revision: source.revision + (options.revisionBase === 0 ? 1 : 0),
    schemaVersion: 1,
    timestamp:
      source.timestamp instanceof Date
        ? source.timestamp.toISOString()
        : source.timestamp,
    commandId: source.commandId,
    causationId: undefined,
    correlationId:
      source.causationId ?? source.correlationId ?? source.commandId,
    position: { transactionId: "0", sequence: String(options.sequence ?? 0) },
  };
  if (
    !event.id ||
    !event.timestamp ||
    !event.commandId ||
    !Number.isSafeInteger(event.revision) ||
    event.revision < 1
  )
    throw Error("Invalid historical Event envelope");
  return { event, reviews };
};
export const migrateSagaState = (
  state: Record<string, any>,
  options: { dates?: "iso" | "epoch" } = {},
): Record<string, any> => {
  const {
    _commitStamp,
    _hash,
    _commands,
    _timeoutAt,
    _timeoutCommands,
    ...rest
  } = state;
  if (_commands && Object.keys(_commands).length)
    throw Error("Pending Saga Commands require reconciliation before import");
  if (
    _timeoutAt != null ||
    (_timeoutCommands && Object.keys(_timeoutCommands).length)
  )
    throw Error(
      "A pending Saga timeout would never fire: the runtime has no Saga timeouts. Reconcile it as a delayed Command (notBefore) or retire it before import",
    );
  return plainJson(rest, "", "Saga state", options.dates);
};
export const auditFixtureFiles = async (
  folder: string,
  options: {
    definitions: AggregateDefinition[];
    revisionBase: 0 | 1;
    kernel?: ReplayAuditInput["kernel"];
    explanations?: ReplayAuditInput["explanations"];
    skipCommand?: (command: any) => string | undefined;
    stateOmit?: string[];
  },
): Promise<ReplayAudit> => {
  const lines = async (name: string): Promise<any[]> =>
    (await readFile(join(folder, name), "utf8"))
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  const [rawEvents, states, commands] = await Promise.all([
    lines("events.jsonl"),
    lines("states.jsonl"),
    lines("commands.jsonl"),
  ]);
  const events = rawEvents.map(
    (event, index) =>
      migrateHistoricalEvent(event, {
        revisionBase: options.revisionBase,
        sequence: index,
      }).event,
  );
  const streamKey = (stream: StreamId) =>
    JSON.stringify([stream.context, stream.aggregate, stream.id]);
  const history = (stream: StreamId) =>
    events
      .filter((event) => streamKey(event.stream) === streamKey(stream))
      .sort((a, b) => a.revision - b.revision);
  return auditReplay({
    ...options,
    streams: states.map((fixture) => {
      const expectedState = structuredClone(fixture.state);
      for (const key of options.stateOmit ?? []) delete expectedState[key];
      return {
        stream: fixture.stream,
        events: history(fixture.stream),
        expectedState,
      };
    }),
    commands: commands.map((fixture) => {
      const expectedEvents = events.filter((event) =>
        fixture.eventIds.includes(event.id),
      );
      const start = Math.min(...expectedEvents.map((event) => event.revision));
      return {
        command: createCommand({
          id: fixture.commandId,
          stream: fixture.stream,
          name: String(fixture.name).split(".").at(-1)!,
          payload: fixture.payload,
          metadata: mapMetadata(fixture.metadata),
          correlationId: expectedEvents[0]?.correlationId,
          timestamp: expectedEvents[0]?.timestamp,
        }),
        history: history(fixture.stream).filter(
          (event) => event.revision < start,
        ),
        expectedEvents,
        skip: options.skipCommand?.(fixture),
      };
    }),
  });
};

const mapMetadata = (input: any): Record<string, any> => {
  const metadata = structuredClone(input ?? {});
  for (const key of ["timestamp", "correlationId", "causationId", "position"])
    delete metadata[key];
  if (metadata.auth && typeof metadata.auth === "object")
    for (const key of ["socket", "platform", "forAccount"])
      delete metadata.auth[key];
  return metadata;
};
