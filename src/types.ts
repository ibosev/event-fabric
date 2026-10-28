import type { UpcasterDefinition } from "./event-store/upcasters.js";
import type { WorkflowInstance } from "./workflows/engine.js";
export type JsonObject = Record<string, any>;
export type Metadata = JsonObject;
export type Schema = Record<string, any> | string | boolean;
export type StreamId = { context: string; aggregate: string; id: string };
export type Schedule = {
  stream: StreamId;
  name: string;
  commandId: string;
  dueAt: string;
  dispatchedAt?: string;
};
export type ScheduleChange = { name: string; entry?: OutboxEntry };
export type ScheduleCancellation = "cancelled" | "dispatched" | "inactive";
/** What each cancelled Schedule name was doing when the Command cancelled it. */
export type ScheduleCancellations = Record<string, ScheduleCancellation>;
/** A current Schedule with its attempt's request fingerprint, for kernel fencing. */
export type RecordedSchedule = Schedule & { fingerprint: string };
/** The Schedule attempt a scheduled Command must still be (or, when obsolete, must no longer be) at append time. */
export type ScheduleGuard = { name: string; commandId: string; fingerprint: string; obsolete: boolean };
export type Position = { transactionId: string; sequence: string };
/**
 * Where a new Saga or Workflow group starts reading the Event log: before every Event, after every
 * Event recorded so far, at the Events recorded at or after a Date, or after a Position token as
 * `EventFabric-Position` returns it. It applies only while no Partition of the group has a Checkpoint.
 */
export type ConsumerStart = "beginning" | "now" | Date | { position: string };
/** A `ConsumerStart` as a `layout:<group>` row records it. */
export type RecordedStart = Exclude<ConsumerStart, Date> | { date: string };
export type Command = {
  id: string;
  stream: StreamId;
  name: string;
  payload: any;
  metadata: Metadata;
  schemaVersion: number;
  correlationId: string;
  causationId?: string;
  timestamp: string;
  traceparent?: string;
  /** Framework-owned attempt identity; only a named Schedule creates this. */
  schedule?: { name: string; dueAt: string };
};
export type PendingEvent = {
  id: string;
  name: string;
  payload: any;
  metadata: Metadata;
  schemaVersion: number;
  timestamp: string;
  commandId: string;
  causationId?: string;
  correlationId: string;
  traceparent?: string;
};
export type RecordedEvent = PendingEvent & {
  stream: StreamId;
  revision: number;
  position: Position;
};
export type Message = {
  id: string;
  name: string;
  payload: any;
  metadata: Metadata;
  schemaVersion: number;
  timestamp: string;
  causationId?: string;
  correlationId: string;
  partitionKey?: string;
  idempotencyKey?: string;
  attempt?: number;
  traceparent?: string;
  /** Set by `app.queue(..., { replyTo })`: the Worker replies here once it succeeds or dead-letters. */
  replyTo?: ReplyTo;
};
/** Where a Worker's reply goes: the Saga Message `name` to one Saga instance, or the `ask` step of a Workflow instance. */
export type ReplyTo =
  | { saga: string; instance: string; name: string }
  /** `run` names the Workflow run that asked; a reply to an earlier run is dropped. */
  | { workflow: string; instance: string; step: string; run?: string };
/** The payload of a Worker's reply: what `execute` returned, or why its last attempt failed. */
export type WorkerReply<V = unknown> =
  | { outcome: "ok"; value: V }
  | { outcome: "failed"; error: string; attempts: number };
/** The reply of a Worker, its `value` typed from what `execute` returns: `ReplyOf<typeof receiptSender>`. */
export type ReplyOf<W extends { execute(...args: any[]): unknown }> = WorkerReply<
  Awaited<ReturnType<W["execute"]>>
>;
/**
 * A Saga Message: a System Event addressed to one Saga instance, due now or later, never in the
 * Event log. A Deadline is one an instance sends itself under a name (ADR-0020).
 */
export type SagaMessage = {
  id: string;
  /** The Saga's Consumer group, `saga:<name>`. */
  group: string;
  instance: string;
  name: string;
  /** Set on a Deadline: at most one per (group, instance, deadline) is pending. */
  deadline?: string;
  payload: any;
  metadata: Metadata;
  correlationId: string;
  causationId?: string;
  timestamp: string;
  traceparent?: string;
  dueAt: string;
  /** Failed handling attempts so far. */
  attempt?: number;
};
/** A Workflow instance as the store keeps it, under its Consumer group `workflow:<name>` and pinned version. */
export type WorkflowRecord = WorkflowInstance & {
  id: string;
  version: number;
  startedAt: string;
  finishedAt?: string;
};
/** A Workflow instance as `workflows list` shows it: no input, journal, inbox, state or result. */
export type WorkflowSummary = Pick<WorkflowRecord, "id" | "version" | "status" | "error" | "startedAt" | "finishedAt"> & {
  group: string;
};
/** A Saga commit's Saga Message writes, applied in order. */
export type SagaMessageChange =
  | { send: SagaMessage }
  /** Cancels one Deadline of the instance, or all of them without `deadline`. */
  | { cancel: { group: string; instance: string; deadline?: string } };
export type Outcome =
  | {
      status: "accepted";
      events: RecordedEvent[];
      result?: any;
      /** A Duplicate Command: the first accepted outcome, replayed without running it. */
      duplicate?: true;
      cancellations?: ScheduleCancellations;
    }
  | {
      status: "rejected";
      error: {
        code: string;
        kind?: string;
        message: string;
        details?: any;
        /** The Application error a Step threw, as JSON: its `name` and own fields, without the stack. */
        thrown?: Record<string, unknown>;
      };
    }
  | { status: "failed"; error: { code?: string; message: string } }
  | { status: "unknown"; commandId: string };
export type OutboxEntry = {
  id: string;
  kind: "command" | "message";
  message: Command | Message;
  queue?: string;
  notBefore?: string;
  replaceKey?: string;
  attempt?: number;
};
export type OutboxRow = OutboxEntry & {
  transactionId: string;
  ordinal: number;
  createdAt: string;
  blocked?: boolean;
};
export type Lease = {
  group: string;
  partition: number;
  owner: string;
  epoch: number;
  expiresAt: string;
};
export type ConsumerStatus = {
  group: string;
  partition: number;
  checkpoint: Position | null;
  lease?: Lease;
  /** `undeclared`: reported by status only, when no live instance runs the group. */
  state: "running" | "halted" | "waiting" | "undeclared";
  eventId?: string;
  position?: Position;
  attempts?: number;
  error?: string;
  waitingOn?: string[];
  retryAt?: string;
  lagMs?: number;
  /** An `instance:<owner>` row: the Consumer groups that instance runs, for Lease balancing. */
  groups?: string[];
  /** An `instance:<owner>` row: the store id of each Readmodel group it runs. */
  stores?: Record<string, string>;
  /** A `layout:<group>` row: the Partition count the group's Checkpoints are laid out for. */
  partitions?: number;
  /** A Saga's or Workflow's `layout:<group>` row: the `from` it started with. */
  from?: RecordedStart;
};
export type DeadLetter = {
  id: string;
  kind: "outbox" | "outbox-rejected" | "worker" | "skipped-event" | "saga-message";
  message?: OutboxEntry;
  /** `saga-message`: replay makes it due again unless it was replaced or cancelled since. */
  sagaMessage?: SagaMessage;
  /** `outbox-rejected`: why the target Aggregate refused the Command. */
  rejection?: { code: string; kind?: string; message: string; details?: any };
  group?: string;
  /** `skipped-event` of a partitioned group: the Partition that skipped it. */
  partition?: number;
  position?: Position;
  eventId?: string;
  error: string;
  attempts: number;
  correlationId?: string;
  createdAt: string;
  who?: string;
  transactionId?: string;
};
export type ReadAllOptions = {
  toPosition?: Position;
  fromPosition?: Position | null;
  eventNames?: string[];
  correlationId?: string;
  limit?: number;
  reverse?: boolean;
};
export type ReadPageOptions = Pick<
  ReadAllOptions,
  "fromPosition" | "toPosition" | "eventNames" | "limit"
>;
export type EventPage = {
  events: RecordedEvent[];
  /** Safe global progress, including excluded Events; commit only after folding the entire page. */
  through: Position | null;
  /**
   * True when `through` is the end of the visible log (or `toPosition`): no selected Event remains
   * unread. A page without it counts as complete when it holds fewer Events than its limit.
   */
  complete?: boolean;
};
export type Validate = (schema: Schema, value: any) => Promise<void>;
export interface EventStore {
  listen?(wake: () => void): Promise<Stop>;
  /** Fresh Events on every call: the kernel freezes their data in place while it loads the Aggregate. */
  readStream(
    stream: StreamId,
    options?: { atRevision?: number },
  ): Promise<RecordedEvent[]>;
  readAll(options?: ReadAllOptions): Promise<RecordedEvent[]>;
  /** Forward filtered consumption. Without it, the runtime captures `head()`, then reads a filtered `readAll` up to it. */
  readPage?(options?: ReadPageOptions): Promise<EventPage>;
  /** Throws while migrations or indexes the runtime relies on are pending; Application startup calls it. */
  assertMigrated?(): Promise<void>;
  appendToStream(
    stream: StreamId,
    expectedRevision: number,
    events: PendingEvent[],
    command?: Command,
    options?: {
      outbox?: OutboxEntry[];
      signal?: AbortSignal;
      /** Records `command.id` as accepted with this fingerprint; a recorded id throws `DuplicateCommandError`. */
      fingerprint?: string;
      /** Schedules this Command creates, replaces or cancels, atomically with its Events. */
      schedules?: ScheduleChange[];
      /** Fences a scheduled Command against replacement or cancellation of its attempt. */
      scheduleGuard?: ScheduleGuard;
    },
  ): Promise<RecordedEvent[]>;
  /** Every qualified Event name (`context.aggregate.name`) in the log. */
  eventNames(): Promise<string[]>;
  /** The fingerprint and Events of an accepted Command id, or null. */
  acceptedCommand(
    id: string,
  ): Promise<{ fingerprint: string; events: RecordedEvent[]; cancellations?: ScheduleCancellations } | null>;
  /** The current Schedule, including its request fingerprint for kernel fencing. */
  getSchedule(stream: StreamId, name: string): Promise<RecordedSchedule | null>;
  import(
    events: RecordedEvent[],
    options?: { signal?: AbortSignal },
  ): Promise<RecordedEvent[]>;
  /** Never ahead of `readAll`: every Event at or before it must already be visible there. */
  head(): Promise<Position | null>;
  /** Merges qualified-Event-name Upcasters; conflicting redefinitions throw. */
  registerUpcasters(definitions: Record<string, UpcasterDefinition>): void;
}
export interface Persistence extends EventStore {
  acquireLease(
    group: string,
    owner: string,
    ttlMs?: number,
    partition?: number,
  ): Promise<Lease | null>;
  /** One write for all Leases; result order matches input, null for lost Leases. */
  renewLeases(leases: Lease[], ttlMs?: number): Promise<(Lease | null)[]>;
  releaseLease(lease: Lease): Promise<void>;
  checkpoint(group: string, partition?: number): Promise<Position | null>;
  commitCheckpoint(lease: Lease, position: Position): Promise<void>;
  /**
   * Where a group starting at `at` resumes: the Position before the first visible Event recorded
   * at or after `at`, null when that is the first Event, and `head()` when there is none.
   */
  positionAt(at: Date): Promise<Position | null>;
  /**
   * Starts a new group, in one transaction: if `group` has no `layout:<group>` row and none of its
   * Partitions has a Checkpoint, sets each Lease's Partition's Checkpoint to `position` (none when
   * null) and records `from` and the Partition count in the layout row. Fails like `publishLayout`
   * when a Lease no longer fences its Partition; returns whether it started the group.
   */
  startConsumer(
    group: string,
    from: RecordedStart,
    position: Position | null,
    leases: Lease[],
  ): Promise<boolean>;
  loadSaga(group: string, id: string): Promise<JsonObject | null>;
  /**
   * `position` null keeps the Checkpoint (a Saga Message commit). `messages.consumed` is the Saga
   * Message handled; when it is no longer pending the commit throws `StaleSagaMessageError`.
   */
  commitSaga(
    lease: Lease,
    position: Position | null,
    changes: { id: string; state: JsonObject | null }[],
    outbox: OutboxEntry[],
    messages?: { changes?: SagaMessageChange[]; consumed?: string },
  ): Promise<void>;
  /** Records Saga Messages outside a Saga commit; an id or a Deadline already pending is kept. */
  recordSagaMessages?(messages: SagaMessage[]): Promise<void>;
  loadWorkflow?(group: string, id: string): Promise<WorkflowRecord | null>;
  /** Every Workflow instance, or those with `status`, by group and id. */
  listWorkflows?(status?: WorkflowInstance["status"]): Promise<WorkflowSummary[]>;
  /** Deletes the instances that finished before `finishedBefore`; returns how many. */
  purgeWorkflows?(finishedBefore: string): Promise<number>;
  /** As `commitSaga`, for Workflow instances: their rows, Outbox, Saga Messages and Checkpoint in one transaction. */
  commitWorkflow?(
    lease: Lease,
    position: Position | null,
    changes: WorkflowRecord[],
    outbox: OutboxEntry[],
    messages?: { changes?: SagaMessageChange[]; consumed?: string },
  ): Promise<void>;
  /**
   * Due Saga Messages of a Saga Partition (`fnv1a(instance) % count`), oldest due first, on the
   * store's clock, and the delay until the next one falls due.
   */
  dueSagaMessages?(
    group: string,
    slice: { partition: number; count: number } | undefined,
    limit: number,
  ): Promise<{ messages: SagaMessage[]; nextDelay: number | null }>;
  retrySagaMessage?(lease: Lease, id: string, attempt: number, dueAt: string): Promise<void>;
  deadLetterSagaMessage?(lease: Lease, id: string, error: string): Promise<void>;
  importSagas(
    group: string,
    states: { id: string; state: JsonObject }[],
    position: Position | null,
  ): Promise<void>;
  recordOutbox(entries: OutboxEntry[]): Promise<void>;
  pendingOutbox(limit?: number, excludeIds?: string[]): Promise<OutboxRow[]>;
  /** Delay until the next delayed row can be due, measured on the store's clock. */
  nextOutboxDelay(excludeIds?: string[]): Promise<number | null>;
  /**
   * Admits the entries that may be delivered now, under `lease`, in one transaction, and returns
   * their ids. The entries are rows as last read; their messages never change under their ids.
   */
  admitOutbox(entries: OutboxEntry[], lease: Lease): Promise<string[]>;
  /** One entry's Admission; `entry` is the row as last read and spares the store a read. */
  claimOutbox(id: string, lease: Lease, entry?: OutboxEntry): Promise<boolean>;
  outboxStats(): Promise<{ pending: number; oldestAgeMs: number }>;
  completeOutbox(id: string | string[]): Promise<void>;
  retryOutbox(id: string, attempt: number, notBefore: string): Promise<void>;
  deadLetterOutbox(id: string, error: string): Promise<void>;
  recordDeadLetter(letter: DeadLetter): Promise<void>;
  deadLetters(): Promise<DeadLetter[]>;
  replayDeadLetter(id: string): Promise<void>;
  discardDeadLetter(id: string): Promise<void>;
  setConsumerStatus(status: ConsumerStatus, lease?: Lease): Promise<void>;
  /**
   * Records `to` Partitions for `group` in its `layout:<group>` row, in one transaction, if the
   * row still records `from` (1 when absent) and every Lease in `leases` is still held. Returns
   * whether it did.
   */
  publishLayout(
    group: string,
    from: number,
    to: number,
    position: Position | null,
    leases: Lease[],
  ): Promise<boolean>;
  consumers(): Promise<ConsumerStatus[]>;
  retryConsumer(group: string): Promise<void>;
  /**
   * Deletes every row of `group`, its `layout:` row and its Saga state, in one transaction;
   * throws if any of its Partitions holds an unexpired Lease.
   */
  retireConsumer(group: string): Promise<void>;
  skipConsumer(
    group: string,
    eventId: string,
    who: string,
    options?: { expectedEpoch?: number; partition?: number },
  ): Promise<void>;
  ping(): Promise<void>;
  close(): Promise<void>;
}
export interface Logger {
  /** Optional pino-compatible level check; omitting it keeps diagnostics enabled. */
  isLevelEnabled?(level: "debug"): boolean;
  debug(bindings: object, message?: string): void;
  info(bindings: object, message?: string): void;
  warn(bindings: object, message?: string): void;
  error(bindings: object, message?: string): void;
  child(bindings: object): Logger;
}
/** A connection pool's occupancy, for adapters that keep one (`poolStats()`). */
export type PoolStats = { total: number; idle: number; waiting: number };
export type Stop = () => Promise<void>;
/** A live message an Application publishes with `notify`, such as a lock update: not an Event, never stored. */
export type ApplicationNotification = { name: string; payload: unknown };
/**
 * Which of its Partition's two queues a Command waits in, chosen by its sender: Gateway callers are `interactive`
 * unless the Gateway says `bulk`; Sagas, Schedules, Workers and Tasks send `bulk`. Each lane keeps its own order.
 */
export type CommandLane = "interactive" | "bulk";
export interface Transport {
  send(
    command: Command,
    options?: { deadline?: number; lane?: CommandLane },
  ): Promise<Outcome>;
  /** Handles one Context's Commands on the given Partitions, both lanes at once. */
  handle(
    context: string,
    partitions: number[],
    handler: (command: Command) => Promise<Outcome>,
  ): Promise<Stop>;
  publish(topic: string, message: any): Promise<void>;
  subscribe(
    topic: string,
    handler: (message: any) => void | Promise<void>,
  ): Promise<Stop>;
  enqueue(queue: string, message: Message): Promise<void>;
  consume(
    queue: string,
    handler: (message: Message) => Promise<void>,
    options?: { concurrency?: number },
  ): Promise<Stop>;
  /** Opens the connection now; a Transport that connects lazily otherwise opens it on first use. */
  connect?(): Promise<void>;
  connected(): boolean;
  close(): Promise<void>;
}
