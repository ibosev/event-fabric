import { toJson } from "../validation.js";
import {
  ConcurrencyError,
  DuplicateCommandError,
  LeaseLostError,
  StaleSagaMessageError,
} from "../errors.js";
import { fnv1a } from "../transport/partition.js";
import { comparePositions } from "../position.js";
import { pageLimit } from "./pages.js";
import {
  assertScheduleChanges,
  assertScheduleGuard,
  cancellationOf,
  scheduleKey,
} from "../schedules.js";
import {
  admitOutbox,
  outboxLetter,
  replayable,
  replayedEntry,
  retryNow,
  sagaMessageLetter,
  skippedEventLetter,
  commandFingerprint,
} from "./rules.js";
import type {
  Command,
  ConsumerStatus,
  DeadLetter,
  Lease,
  OutboxEntry,
  OutboxRow,
  Persistence,
  Position,
  RecordedEvent,
  RecordedSchedule,
  SagaMessage,
  SagaMessageChange,
  Schedule,
  ScheduleCancellations,
  WorkflowRecord,
} from "../types.js";
import {
  assertJson,
  assertLeaseTTL,
  assertEventNames,
  assertImportAtHead,
  assertStream,
  eventCodec,
  type EventStoreOptions,
} from "./upcasters.js";
import { parseTime } from "../internal.js";
const key = (value: any) =>
  JSON.stringify(
    value && !Array.isArray(value) && "context" in value
      ? [value.context, value.aggregate, value.id]
      : value,
  );
export function createInMemoryEventStore(
  options: EventStoreOptions & {
    /** The clock Saga Messages fall due on; pass the Application's `clock` to move time in a test. */
    clock?: () => Date;
  } = {},
): Persistence {
  const clock = options.clock ?? (() => new Date());
  const codec = eventCodec(options),
    // The codec takes ownership of what it reads; the log keeps its own Events.
    readCopy = (event: RecordedEvent) => codec.read(structuredClone(event)),
    events: RecordedEvent[] = [],
    leases = new Map<string, Lease>();
  const checkpoints = new Map<string, Position>(),
    sagas = new Map<string, any>(),
    workflows = new Map<string, WorkflowRecord>(),
    outbox: OutboxRow[] = [],
    letters = new Map<string, DeadLetter>(),
    statuses = new Map<string, ConsumerStatus>();
  const admittedOutbox = new Set<string>();
  // Pending Saga Messages by [group, instance, slot]: a Deadline's slot is its name; `seq` is the send order.
  const sagaMessages = new Map<string, { message: SagaMessage; dead: boolean; seq: number }>();
  let sagaMessageSeq = 0;
  const slot = (message: SagaMessage) =>
    key([message.group, message.instance, message.deadline === undefined ? `m:${message.id}` : `d:${message.deadline}`]);
  const findSagaMessage = (id: string) =>
    [...sagaMessages].find(([, row]) => row.message.id === id);
  const applySagaMessages = (changes: SagaMessageChange[] = []) => {
    for (const change of changes)
      if ("send" in change) {
        const message = toJson(change.send);
        // A Deadline replaces the instance's pending one of its name; another Saga Message keeps a pending id.
        if (message.deadline !== undefined || !findSagaMessage(message.id))
          sagaMessages.set(slot(message), { message, dead: false, seq: ++sagaMessageSeq });
      } else {
        const { group, instance, deadline } = change.cancel;
        for (const [id, row] of sagaMessages)
          if (
            row.message.group === group &&
            row.message.instance === instance &&
            row.message.deadline !== undefined &&
            (deadline === undefined || row.message.deadline === deadline)
          )
            sagaMessages.delete(id);
      }
  };
  const schedules = new Map<string, RecordedSchedule>();
  const acceptedCommands = new Map<
    string,
    { fingerprint: string; eventIds: string[]; cancellations?: ScheduleCancellations }
  >();
  let transactionId = 0n;
  let serial = Promise.resolve();
  const atomic = <T>(fn: () => Promise<T> | T): Promise<T> => {
    const result = serial.then(fn);
    serial = result.then(
      () => {},
      () => {},
    );
    return result;
  };
  const partitionKey = (group: string, partition = 0) =>
    key([group, partition]);
  const fence = (lease: Lease) => {
    const current = leases.get(partitionKey(lease.group, lease.partition));
    if (
      !current ||
      current.owner !== lease.owner ||
      current.epoch !== lease.epoch ||
      parseTime(current.expiresAt) <= Date.now()
    )
      throw new LeaseLostError("Lease expired or replaced");
  };
  const saveCheckpoint = (lease: Lease, position: Position) => {
    const id = partitionKey(lease.group, lease.partition),
      current = checkpoints.get(id);
    if (current && comparePositions(position, current) < 0)
      throw new LeaseLostError("Lease lost or Checkpoint would move backwards");
    checkpoints.set(id, structuredClone(position));
  };
  // Runs inside `atomic`: validates every entry before touching the Outbox.
  const addOutbox = async (
    entries: OutboxEntry[],
    tx = String(++transactionId),
    replay = false,
  ) => {
    for (const entry of entries) await codec.validateOutbox(entry, replay);
    entries = entries.map((entry) => toJson(entry));
    const { replaced, admitted } = admitOutbox(
      entries,
      outbox
        .filter(
          (row) =>
            entries.some(
              (entry) =>
                entry.id === row.id ||
                (entry.replaceKey && entry.replaceKey === row.replaceKey),
            ),
        )
        .map((row) => ({
          id: row.id,
          // A claimed row has released its key, as Postgres clears replace_key on claim.
          replaceKey: admittedOutbox.has(row.id) ? null : row.replaceKey,
          blocked: !!row.blocked,
        })),
    );
    for (const id of replaced)
      outbox.splice(
        outbox.findIndex((row) => row.id === id),
        1,
      );
    for (const [ordinal, entry] of admitted)
      outbox.push({
        ...structuredClone(entry),
        transactionId: tx,
        ordinal,
        createdAt: new Date().toISOString(),
      });
  };
  /** A Saga or Workflow commit: `write` stores its instances beside the Outbox, Saga Messages and Checkpoint. */
  const commitProcess = (
    lease: Lease,
    position: Position | null,
    entries: OutboxEntry[],
    messages: { changes?: SagaMessageChange[]; consumed?: string },
    check: () => void,
    write: () => void,
  ) =>
    atomic(async () => {
      fence(lease);
      check();
      const current = checkpoints.get(
        partitionKey(lease.group, lease.partition),
      );
      if (position && current && comparePositions(position, current) < 0)
        throw new LeaseLostError(
          "Lease lost or Checkpoint would move backwards",
        );
      const consumed =
        messages.consumed === undefined ? undefined : findSagaMessage(messages.consumed);
      if (messages.consumed !== undefined && (!consumed || consumed[1].dead))
        throw new StaleSagaMessageError();
      await addOutbox(entries);
      if (consumed) sagaMessages.delete(consumed[0]);
      applySagaMessages(messages.changes);
      write();
      if (position) saveCheckpoint(lease, position);
    });
  /** The Stream a Command row targets; rows for one Stream are delivered in Outbox order. */
  const target = (row: (typeof outbox)[number]) =>
    row.kind === "command" && (row.message as Command).stream?.id
      ? key((row.message as Command).stream)
      : undefined;
  /** Whether an earlier row for the same Stream is still due or retrying. */
  const waitsBehind = (row: (typeof outbox)[number]) => {
    const stream = target(row);
    if (!stream) return false;
    const at = outbox.indexOf(row);
    return outbox.some(
      (prior, index) =>
        index < at &&
        !prior.blocked &&
        target(prior) === stream &&
        (!prior.notBefore ||
          parseTime(prior.notBefore) <= Date.now() ||
          prior.attempt !== undefined),
    );
  };
  const store: Persistence = {
    async readStream(stream, readOptions) {
      return Promise.all(
        events
          .filter(
            (event) =>
              key(event.stream) === key(stream) &&
              event.revision <= (readOptions?.atRevision ?? Infinity),
          )
          .map(readCopy),
      );
    },
    registerUpcasters: codec.register,
    async eventNames() {
      return [
        ...new Set(
          events.map(
            (event) =>
              `${event.stream.context}.${event.stream.aggregate}.${event.name}`,
          ),
        ),
      ];
    },
    async getSchedule(stream, name) {
      return structuredClone(schedules.get(scheduleKey(stream, name)) ?? null);
    },
    async acceptedCommand(id) {
      const accepted = acceptedCommands.get(id);
      if (!accepted) return null;
      return {
        fingerprint: accepted.fingerprint,
        ...(accepted.cancellations ? { cancellations: structuredClone(accepted.cancellations) } : {}),
        events: await Promise.all(
          events
            .filter((event) => accepted.eventIds.includes(event.id))
            .map(readCopy),
        ),
      };
    },
    async readAll(readOptions = {}) {
      assertEventNames(readOptions.eventNames);
      // Reverse reads page backwards, so fromPosition is an exclusive upper bound.
      const after = readOptions.reverse ? -1 : 1;
      const selected = events.filter(
        (event) =>
          (!readOptions.fromPosition ||
            comparePositions(event.position, readOptions.fromPosition) ===
              after) &&
          (!readOptions.toPosition ||
            comparePositions(event.position, readOptions.toPosition) <= 0) &&
          (!readOptions.eventNames ||
            readOptions.eventNames.includes(
              `${event.stream.context}.${event.stream.aggregate}.${event.name}`,
            )) &&
          (!readOptions.correlationId ||
            event.correlationId === readOptions.correlationId),
      );
      if (readOptions.reverse) selected.reverse();
      return Promise.all(
        selected.slice(0, readOptions.limit ?? 1000).map(readCopy),
      );
    },
    async readPage(options = {}) {
      const limit = pageLimit(options);
      const through = events.findLast(
        (event) =>
          !options.toPosition ||
          comparePositions(event.position, options.toPosition) <= 0,
      )?.position;
      if (
        !through ||
        (options.fromPosition &&
          comparePositions(through, options.fromPosition) <= 0)
      )
        return {
          events: [],
          through: structuredClone(options.fromPosition ?? null),
          complete: true,
        };
      const selected = events
        .filter(
          (event) =>
            (!options.fromPosition ||
              comparePositions(event.position, options.fromPosition) > 0) &&
            comparePositions(event.position, through) <= 0 &&
            (!options.eventNames ||
              options.eventNames.includes(
                `${event.stream.context}.${event.stream.aggregate}.${event.name}`,
              )),
        )
        .slice(0, limit + 1);
      return {
        events: await Promise.all(selected.slice(0, limit).map(readCopy)),
        through: structuredClone(
          selected.length > limit ? selected[limit - 1]!.position : through,
        ),
        complete: selected.length <= limit,
      };
    },
    appendToStream(stream, expectedRevision, pending, command, appendOptions) {
      return atomic(async () => {
        appendOptions?.signal?.throwIfAborted();
        assertStream(stream, expectedRevision);
        const revision =
          events.filter((event) => key(event.stream) === key(stream)).at(-1)
            ?.revision ?? 0;
        if (revision !== expectedRevision)
          throw new ConcurrencyError("Stream Revision conflict");
        const changes = appendOptions?.schedules ?? [];
        assertScheduleChanges(stream, changes, appendOptions?.outbox ?? []);
        const guard = appendOptions?.scheduleGuard;
        assertScheduleGuard(
          guard ? schedules.get(scheduleKey(stream, guard.name)) : undefined,
          guard, !!(pending.length || changes.length || appendOptions?.outbox?.length),
        );
        for (const event of pending) {
          await codec.validate(event);
          if (
            events.some((existing) => existing.id === event.id) ||
            pending.filter((other) => other.id === event.id).length > 1
          )
            throw new Error("Duplicate Event id");
        }
        appendOptions?.signal?.throwIfAborted();
        const accepting = command && appendOptions?.fingerprint !== undefined;
        if (accepting && acceptedCommands.has(command.id))
          throw new DuplicateCommandError();
        const tx = String(++transactionId),
          saved = pending.map((event, i) => ({
            ...toJson(event),
            stream: structuredClone(stream),
            revision: revision + i + 1,
            position: { transactionId: tx, sequence: String(i + 1) },
          }));
        await addOutbox(appendOptions?.outbox ?? [], tx);
        const cancellations: ScheduleCancellations = Object.create(null);
        for (const change of changes) {
          const id = scheduleKey(stream, change.name);
          const previous = schedules.get(id);
          if (previous) {
            for (let i = outbox.length - 1; i >= 0; i--)
              if (outbox[i]!.message.id === previous.commandId) {
                admittedOutbox.delete(outbox[i]!.id);
                outbox.splice(i, 1);
              }
          }
          if (!change.entry) {
            cancellations[change.name] = cancellationOf(previous);
            schedules.delete(id);
            continue;
          }
          const scheduled = change.entry.message as Command;
          schedules.set(id, {
            stream: structuredClone(stream),
            name: change.name,
            commandId: scheduled.id,
            dueAt: scheduled.schedule!.dueAt,
            fingerprint: commandFingerprint(scheduled),
          });
        }
        events.push(...saved);
        if (guard && !guard.obsolete &&
          schedules.get(scheduleKey(stream, guard.name))?.commandId === guard.commandId)
          schedules.delete(scheduleKey(stream, guard.name));
        if (accepting)
          acceptedCommands.set(command.id, {
            fingerprint: appendOptions.fingerprint!,
            eventIds: saved.map((event) => event.id),
            ...(Object.keys(cancellations).length ? { cancellations } : {}),
          });
        return structuredClone(saved);
      });
    },
    import(historical, importOptions) {
      return atomic(async () => {
        const revisions = new Map<string, number>(),
          ids = new Set(events.map((event) => event.id));
        for (const event of historical) {
          importOptions?.signal?.throwIfAborted();
          assertStream(event.stream);
          await codec.validate(event);
          const streamKey = key(event.stream),
            revision =
              revisions.get(streamKey) ??
              events
                .filter((existing) => key(existing.stream) === streamKey)
                .at(-1)?.revision ??
              0;
          if (event.revision !== revision + 1)
            throw new ConcurrencyError(
              "Imported Stream Revision must be contiguous",
            );
          if (ids.has(event.id)) throw new Error("Duplicate Event id");
          ids.add(event.id);
          revisions.set(streamKey, event.revision);
        }
        importOptions?.signal?.throwIfAborted();
        const tx = String(++transactionId),
          saved = historical.map((event, i) => ({
            ...toJson(event),
            position: { transactionId: tx, sequence: String(i + 1) },
          }));
        events.push(...saved);
        return structuredClone(saved);
      });
    },
    async head() {
      return structuredClone(events.at(-1)?.position ?? null);
    },
    acquireLease(group, owner, ttlMs = 30000, partition = 0) {
      return atomic(() => {
        assertLeaseTTL(ttlMs);
        const id = partitionKey(group, partition),
          current = leases.get(id);
        if (current && parseTime(current.expiresAt) > Date.now()) return null;
        const lease = {
          group,
          partition,
          owner,
          epoch: (current?.epoch ?? 0) + 1,
          expiresAt: new Date(Date.now() + ttlMs).toISOString(),
        };
        leases.set(id, lease);
        return structuredClone(lease);
      });
    },
    renewLeases(batch, ttlMs = 30000) {
      return atomic(() => {
        assertLeaseTTL(ttlMs);
        return batch.map((lease) => {
          try {
            fence(lease);
          } catch {
            return null;
          }
          const renewed = {
            ...lease,
            expiresAt: new Date(Date.now() + ttlMs).toISOString(),
          };
          leases.set(partitionKey(lease.group, lease.partition), renewed);
          return structuredClone(renewed);
        });
      });
    },
    releaseLease(lease) {
      return atomic(() => {
        const current = leases.get(partitionKey(lease.group, lease.partition));
        if (current?.owner === lease.owner && current.epoch === lease.epoch)
          current.expiresAt = new Date(0).toISOString();
      });
    },
    async checkpoint(group, partition = 0) {
      return structuredClone(
        checkpoints.get(partitionKey(group, partition)) ?? null,
      );
    },
    commitCheckpoint(lease, position) {
      return atomic(() => {
        fence(lease);
        saveCheckpoint(lease, position);
      });
    },
    async positionAt(at) {
      const first = events.findIndex((event) => parseTime(event.timestamp) >= at.getTime());
      const before = first < 0 ? events.at(-1) : events[first - 1];
      return structuredClone(before?.position ?? null);
    },
    startConsumer(group, from, position, held) {
      return atomic(() => {
        try {
          held.forEach(fence);
        } catch {
          return false;
        }
        const id = partitionKey(`layout:${group}`, 0);
        if (
          statuses.has(id) ||
          [...checkpoints.keys()].some((key) => JSON.parse(key)[0] === group)
        )
          return false;
        if (position)
          for (const lease of held) saveCheckpoint(lease, position);
        statuses.set(id, {
          group: `layout:${group}`,
          partition: 0,
          checkpoint: structuredClone(position),
          state: "running",
          partitions: held.length,
          from: structuredClone(from),
        });
        return true;
      });
    },
    async loadSaga(group, id) {
      return structuredClone(sagas.get(key([group, id])) ?? null);
    },
    commitSaga(lease, position, changes, entries, messages = {}) {
      const check = () => changes.forEach((change) => assertJson(change.state));
      return commitProcess(lease, position, entries, messages, check, () => {
        for (const change of changes) {
          const id = key([lease.group, change.id]);
          if (change.state === null) sagas.delete(id);
          else sagas.set(id, toJson(change.state));
        }
      });
    },
    async loadWorkflow(group, id) {
      return structuredClone(workflows.get(key([group, id])) ?? null);
    },
    async listWorkflows(status) {
      return [...workflows]
        .map(([key, record]) => ({ group: JSON.parse(key)[0] as string, record }))
        .filter(({ record }) => !status || record.status === status)
        .sort((a, b) => a.group.localeCompare(b.group) || a.record.id.localeCompare(b.record.id))
        .map(({ group, record }) => ({
          group,
          id: record.id,
          version: record.version,
          status: record.status,
          ...(record.error === undefined ? {} : { error: record.error }),
          startedAt: record.startedAt,
          ...(record.finishedAt === undefined ? {} : { finishedAt: record.finishedAt }),
        }));
    },
    purgeWorkflows(finishedBefore) {
      return atomic(() => {
        let purged = 0;
        for (const [id, record] of workflows)
          if (record.finishedAt !== undefined && parseTime(record.finishedAt) < parseTime(finishedBefore))
            purged += +workflows.delete(id);
        return purged;
      });
    },
    commitWorkflow(lease, position, changes, entries, messages = {}) {
      const check = () => changes.forEach((change) => assertJson(change));
      return commitProcess(lease, position, entries, messages, check, () => {
        for (const change of changes)
          workflows.set(key([lease.group, change.id]), toJson(change));
      });
    },
    recordSagaMessages(messages) {
      return atomic(() => {
        for (const message of messages)
          if (!findSagaMessage(message.id) && !sagaMessages.has(slot(message)))
            sagaMessages.set(slot(message), { message: toJson(message), dead: false, seq: ++sagaMessageSeq });
      });
    },
    async dueSagaMessages(group, slice, limit) {
      const now = clock().getTime();
      const rows = [...sagaMessages.values()]
        .filter(
          ({ message, dead }) =>
            !dead &&
            message.group === group &&
            (!slice || fnv1a(message.instance) % slice.count === slice.partition),
        )
        .sort(
          (a, b) =>
            parseTime(a.message.dueAt) - parseTime(b.message.dueAt) || a.seq - b.seq,
        );
      const due = rows.filter(({ message }) => parseTime(message.dueAt) <= now);
      const next = rows.find(({ message }) => parseTime(message.dueAt) > now);
      return {
        messages: structuredClone(due.slice(0, limit).map((row) => row.message)),
        nextDelay: next ? parseTime(next.message.dueAt) - now : null,
      };
    },
    retrySagaMessage(lease, id, attempt, dueAt) {
      return atomic(() => {
        fence(lease);
        const row = findSagaMessage(id)?.[1];
        if (row && !row.dead) row.message = { ...row.message, attempt, dueAt };
      });
    },
    deadLetterSagaMessage(lease, id, error) {
      return atomic(() => {
        fence(lease);
        const row = findSagaMessage(id)?.[1];
        if (!row || row.dead) return;
        row.dead = true;
        letters.set(id, sagaMessageLetter(row.message, error));
      });
    },
    importSagas(group, states, position) {
      return atomic(() => {
        if (
          leases.has(partitionKey(group)) &&
          parseTime(leases.get(partitionKey(group))!.expiresAt) > Date.now()
        )
          throw new Error("Cannot import Sagas with an active Lease");
        assertImportAtHead(position, events.at(-1)?.position ?? null);
        for (const state of states) assertJson(state.state);
        for (const state of states)
          sagas.set(key([group, state.id]), toJson(state.state));
        if (position)
          checkpoints.set(partitionKey(group), structuredClone(position));
        else checkpoints.delete(partitionKey(group));
      });
    },
    recordOutbox(entries) {
      return atomic(() => addOutbox(entries));
    },
    async pendingOutbox(limit = 100, excludeIds = []) {
      return structuredClone(
        outbox
          .filter(
            (row) =>
              !row.blocked &&
              !excludeIds.includes(row.id) &&
              (!row.notBefore || parseTime(row.notBefore) <= Date.now()) &&
              !waitsBehind(row),
          )
          .slice(0, limit),
      );
    },
    async nextOutboxDelay(excludeIds = []) {
      const due = outbox.filter((row) =>
        !row.blocked && row.notBefore && !excludeIds.includes(row.id) && !waitsBehind(row))
        .map((row) => parseTime(row.notBefore!));
      const first = due.reduce((first, at) => Math.min(first, at), Infinity);
      return due.length ? Math.max(0, first - Date.now()) : null;
    },
    admitOutbox(entries, lease) {
      return atomic(() => {
        fence(lease);
        return entries.flatMap(({ id }) => {
          const row = outbox.find((row) => row.id === id);
          if (
            !row ||
            row.blocked ||
            (row.notBefore && parseTime(row.notBefore) > Date.now()) ||
            waitsBehind(row)
          )
            return [];
          admittedOutbox.add(id);
          if (row.kind === "command") {
            const command = row.message as Command;
            if (command.schedule) {
              const schedule = schedules.get(scheduleKey(command.stream, command.schedule.name));
              if (schedule?.commandId === command.id) schedule.dispatchedAt ??= new Date().toISOString();
            }
          }
          return [id];
        });
      });
    },
    async claimOutbox(id, lease) {
      return (await store.admitOutbox([{ id } as OutboxEntry], lease)).length === 1;
    },
    async outboxStats() {
      const pending = outbox.filter((row) => !row.blocked);
      return {
        pending: pending.length,
        oldestAgeMs: pending.length
          ? Math.max(
              0,
              Date.now() -
                pending.reduce(
                  (oldest, row) => Math.min(oldest, parseTime(row.createdAt)),
                  Infinity,
                ),
            )
          : 0,
      };
    },
    completeOutbox(ids) {
      return atomic(() => {
        for (const id of [ids].flat()) {
          admittedOutbox.delete(id);
          const index = outbox.findIndex((row) => row.id === id);
          if (index >= 0) outbox.splice(index, 1);
        }
      });
    },
    retryOutbox(id, attempt, notBefore) {
      return atomic(() => {
        const row = outbox.find((row) => row.id === id);
        if (row) Object.assign(row, { attempt, notBefore });
      });
    },
    deadLetterOutbox(id, error) {
      return atomic(() => {
        const row = outbox.find((row) => row.id === id);
        if (!row) return;
        const { transactionId, ordinal: _, createdAt: __, ...entry } = row;
        letters.set(id, outboxLetter(entry, transactionId, error));
        row.blocked = true;
      });
    },
    recordDeadLetter(letter) {
      return atomic(() => {
        letters.set(letter.id, structuredClone(letter));
      });
    },
    async deadLetters() {
      return structuredClone([...letters.values()]);
    },
    replayDeadLetter(id) {
      return atomic(async () => {
        if (letters.get(id)?.kind === "saga-message") {
          // Replaced or cancelled since: the replay is a no-op.
          const row = findSagaMessage(id)?.[1];
          if (row?.dead) {
            row.dead = false;
            row.message = { ...row.message, attempt: 0, dueAt: clock().toISOString() };
          }
          letters.delete(id);
          return;
        }
        const entry = replayable(letters.get(id));
        const index = outbox.findIndex((row) => row.id === id);
        if (index >= 0) {
          const row = outbox[index]!,
            replayed = replayedEntry(row);
          // A claimed row keeps its released key under its new id, as in Postgres.
          if (admittedOutbox.delete(id)) admittedOutbox.add(replayed.id);
          outbox[index] = {
            ...replayed,
            transactionId: row.transactionId,
            ordinal: row.ordinal,
            createdAt: row.createdAt,
            blocked: false,
          };
        } else await addOutbox([replayedEntry(entry)], undefined, true);
        letters.delete(id);
      });
    },
    discardDeadLetter(id) {
      return atomic(() => {
        const entry = letters.get(id)?.message;
        const command = entry?.kind === "command" ? entry.message as Command : undefined;
        if (command?.schedule) {
          const key = scheduleKey(command.stream, command.schedule.name);
          if (schedules.get(key)?.commandId === command.id) schedules.delete(key);
        }
        if (letters.get(id)?.kind === "saga-message") {
          const found = findSagaMessage(id);
          if (found?.[1].dead) sagaMessages.delete(found[0]);
        }
        letters.delete(id);
        admittedOutbox.delete(id);
        const index = outbox.findIndex((row) => row.id === id && row.blocked);
        if (index >= 0) outbox.splice(index, 1);
      });
    },
    setConsumerStatus(status, lease) {
      return atomic(() => {
        if (lease) {
          fence(lease);
          if (
            status.group !== lease.group ||
            status.partition !== lease.partition
          )
            throw new LeaseLostError("Consumer status does not match Lease");
        }
        statuses.set(
          partitionKey(status.group, status.partition),
          structuredClone(status),
        );
      });
    },
    publishLayout(group, from, to, position, held) {
      return atomic(() => {
        try {
          held.forEach(fence);
        } catch {
          return false;
        }
        const id = partitionKey(`layout:${group}`, 0),
          current = statuses.get(id);
        if ((current?.partitions ?? 1) !== from) return false;
        statuses.set(id, {
          group: `layout:${group}`,
          partition: 0,
          checkpoint: structuredClone(position),
          state: "running",
          partitions: to,
          ...(current?.from && { from: current.from }),
        });
        return true;
      });
    },
    async consumers() {
      const keys = new Set([
        ...leases.keys(),
        ...statuses.keys(),
        ...checkpoints.keys(),
      ]);
      return structuredClone(
        [...keys].map((id) => {
          const [group, partition] = JSON.parse(id);
          return {
            group,
            partition,
            state: "running" as const,
            ...statuses.get(id),
            checkpoint: checkpoints.get(id) ?? null,
            lease: leases.get(id),
          };
        }),
      );
    },
    retryConsumer(group) {
      return atomic(() => {
        for (const status of statuses.values())
          if (status.group === group) status.retryAt = retryNow;
      });
    },
    retireConsumer(group) {
      return atomic(() => {
        const ids = [...new Set([...leases.keys(), ...statuses.keys(), ...checkpoints.keys()])]
          .filter((id) => JSON.parse(id)[0] === group);
        if (ids.some((id) => parseTime(leases.get(id)?.expiresAt ?? "") > Date.now()))
          throw new Error(`Consumer group ${group} holds a live Lease`);
        for (const id of [...ids, partitionKey(`layout:${group}`)]) {
          leases.delete(id);
          statuses.delete(id);
          checkpoints.delete(id);
        }
        for (const id of sagas.keys())
          if (JSON.parse(id)[0] === group) sagas.delete(id);
        for (const id of workflows.keys())
          if (JSON.parse(id)[0] === group) workflows.delete(id);
        for (const id of sagaMessages.keys())
          if (JSON.parse(id)[0] === group) sagaMessages.delete(id);
      });
    },
    skipConsumer(group, eventId, who, options) {
      const partition = options?.partition ?? 0;
      return atomic(() => {
        const letter = skippedEventLetter({
          group,
          eventId,
          who,
          status: statuses.get(partitionKey(group, partition)),
          checkpoint: checkpoints.get(partitionKey(group, partition)) ?? null,
          epoch: leases.get(partitionKey(group, partition))?.epoch,
          expectedEpoch: options?.expectedEpoch,
          partition,
        });
        letters.set(letter.id, letter);
        const lease = leases.get(partitionKey(group, partition));
        if (lease) {
          lease.epoch++;
          lease.expiresAt = new Date(0).toISOString();
        }
        checkpoints.set(
          partitionKey(group, partition),
          structuredClone(letter.position),
        );
        statuses.set(partitionKey(group, partition), {
          group,
          partition,
          checkpoint: structuredClone(letter.position),
          state: "running",
        });
      });
    },
    async ping() {},
    async close() {},
  };
  return store;
}
