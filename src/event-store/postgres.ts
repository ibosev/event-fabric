import { toJson } from "../validation.js";
import { comparePositions } from "../position.js";
import { pageLimit } from "./pages.js";
import {
  assertScheduleChanges,
  assertScheduleGuard,
  cancellationOf,
  isScheduledAttempt,
} from "../schedules.js";
import { createHash } from "node:crypto";
import { setTimeout } from "node:timers/promises";
import { Client, Pool, type PoolClient, type PoolConfig } from "pg";
import {
  ConcurrencyError,
  DuplicateCommandError,
  LeaseLostError,
  StaleSagaMessageError,
} from "../errors.js";
import { fnv1a } from "../transport/partition.js";
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
  DeadLetter,
  PoolStats,
  ConsumerStatus,
  Lease,
  OutboxEntry,
  Persistence,
  Position,
  RecordedEvent,
  StreamId,
  RecordedSchedule,
  SagaMessage,
  SagaMessageChange,
  Schedule,
  ScheduleCancellations,
  Command,
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
export type PostgresEventStoreOptions = EventStoreOptions & {
  pool?: Pool;
  connectionString?: string;
  /** For the pool the store creates when no `pool` is given, e.g. `{ max: 20 }`; pg's default `max` is 10. */
  poolOptions?: PoolConfig;
  schema?: string;
  metadataIndexes?: string[];
};
export const quoteIdentifier = (value: string) => {
  if (!value || value.includes("\0") || Buffer.byteLength(value) > 63)
    throw new Error("Invalid SQL identifier");
  return `"${value.replaceAll('"', '""')}"`;
};
const literal = (value: string) => `'${value.replaceAll("'", "''")}'`;
export function createPostgresEventStore(
  options: PostgresEventStoreOptions = {},
): Persistence & {
  migrate(options?: { dryRun?: boolean }): Promise<string>;
  migrationSQL(): string;
  notificationChannel: string;
  readerLag(): Promise<number>;
  poolStats(): PoolStats;
} {
  const pool =
    options.pool ??
    new Pool({
      ...options.poolOptions,
      connectionString: options.connectionString,
    });
  const schema = options.schema ?? "event_fabric",
    s = quoteIdentifier(schema),
    codec = eventCodec(options);
  const table = (name: string) => `${s}.${quoteIdentifier(name)}`;
  const channel = `ef_${createHash("sha256").update(schema).digest("hex").slice(0, 40)}`;
  const listeners = new Set<Client>();
  const bootstrapSQL = `CREATE SCHEMA IF NOT EXISTS ${s};
CREATE TABLE IF NOT EXISTS ${table("migrations")} (version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT clock_timestamp());`;
  // Ordered by version; append new steps, never edit an applied one.
  const migrations = [
    {
      version: 1,
      sql: `CREATE SEQUENCE IF NOT EXISTS ${table("event_sequence")};
CREATE TABLE IF NOT EXISTS ${table("events")} (
 id text PRIMARY KEY, context text NOT NULL, aggregate text NOT NULL, stream_id text NOT NULL, revision bigint NOT NULL CHECK(revision > 0),
 transaction_id xid8 NOT NULL, sequence bigint NOT NULL DEFAULT nextval(${literal(`${s}."event_sequence"`)}),
 name text NOT NULL, schema_version integer NOT NULL CHECK(schema_version > 0), command_id text NOT NULL, causation_id text, correlation_id text NOT NULL,
 timestamp timestamptz NOT NULL, metadata jsonb NOT NULL, traceparent text,
 UNIQUE(context,aggregate,stream_id,revision), UNIQUE(transaction_id,sequence));
CREATE INDEX IF NOT EXISTS events_position ON ${table("events")}(transaction_id,sequence);
CREATE INDEX IF NOT EXISTS events_correlation ON ${table("events")}(correlation_id,transaction_id,sequence);
CREATE TABLE IF NOT EXISTS ${table("event_payloads")} (event_id text PRIMARY KEY REFERENCES ${table("events")}(id), payload jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS ${table("consumers")} (
 group_name text NOT NULL, partition integer NOT NULL DEFAULT 0, owner text, epoch bigint NOT NULL DEFAULT 0, expires_at timestamptz,
 checkpoint_tx xid8, checkpoint_seq bigint, status jsonb NOT NULL DEFAULT '{"state":"running"}', PRIMARY KEY(group_name,partition));
CREATE TABLE IF NOT EXISTS ${table("sagas")} (group_name text NOT NULL, id text NOT NULL, state jsonb NOT NULL, PRIMARY KEY(group_name,id));
CREATE TABLE IF NOT EXISTS ${table("outbox")} (
 id text PRIMARY KEY, transaction_id xid8 NOT NULL, ordinal integer NOT NULL, entry jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), not_before timestamptz, replace_key text UNIQUE, blocked boolean NOT NULL DEFAULT false);
CREATE INDEX IF NOT EXISTS outbox_order ON ${table("outbox")}(transaction_id,ordinal);
CREATE TABLE IF NOT EXISTS ${table("dead_letters")} (id text PRIMARY KEY, letter jsonb NOT NULL);`,
    },
    {
      version: 2,
      // Accepted Command ids, kept forever (ADR-0014).
      sql: `CREATE TABLE IF NOT EXISTS ${table("commands")} (
 id text PRIMARY KEY, fingerprint text NOT NULL, event_ids text[] NOT NULL, accepted_at timestamptz NOT NULL DEFAULT clock_timestamp());`,
    },
    {
      version: 3,
      // The Stream a Command row targets: rows for one Stream are delivered in Outbox order.
      sql: `ALTER TABLE ${table("outbox")} ADD COLUMN IF NOT EXISTS target text GENERATED ALWAYS AS (
 CASE WHEN entry->>'kind'='command' AND COALESCE(entry#>>'{message,stream,id}','')<>''
 THEN (entry#>>'{message,stream,context}')||'.'||(entry#>>'{message,stream,aggregate}')||'.'||(entry#>>'{message,stream,id}') END) STORED;
CREATE INDEX IF NOT EXISTS outbox_target ON ${table("outbox")}(target,transaction_id,ordinal) WHERE target IS NOT NULL;`,
    },
    {
      version: 4,
      // The Outbox is a queue next to long-lived delayed rows: vacuum after a fixed number of
      // dead rows, not a share of the table, so polls stop walking delivered rows.
      sql: `ALTER TABLE ${table("outbox")} SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000, autovacuum_vacuum_cost_delay = 0);`,
    },
    {
      version: 5,
      sql: `CREATE TABLE IF NOT EXISTS ${table("schedules")} (
 context text NOT NULL, aggregate text NOT NULL, stream_id text NOT NULL, name text NOT NULL,
 command_id text NOT NULL UNIQUE, due_at timestamptz NOT NULL, dispatched_at timestamptz,
 fingerprint text NOT NULL, PRIMARY KEY(context,aggregate,stream_id,name));
ALTER TABLE ${table("commands")} ADD COLUMN IF NOT EXISTS cancellations jsonb;
CREATE INDEX IF NOT EXISTS outbox_schedule_command ON ${table("outbox")} ((entry#>>'{message,id}')) WHERE entry#>'{message,schedule}' IS NOT NULL;`,
    },
    {
      version: 6,
      // Pending Saga Messages (ADR-0020); a Deadline's slot is `d:<name>`, any other's `m:<id>`.
      sql: `CREATE TABLE IF NOT EXISTS ${table("saga_messages")} (
 group_name text NOT NULL, instance text NOT NULL, slot text NOT NULL, id text NOT NULL UNIQUE, hash bigint NOT NULL,
 due_at timestamptz NOT NULL, attempt integer NOT NULL DEFAULT 0, dead boolean NOT NULL DEFAULT false, message jsonb NOT NULL,
 PRIMARY KEY(group_name,instance,slot));
CREATE INDEX IF NOT EXISTS saga_messages_due ON ${table("saga_messages")}(group_name,due_at) WHERE NOT dead;`,
    },
    {
      version: 7,
      // Saga Messages due at the same instant are handled in the order they were sent.
      sql: `ALTER TABLE ${table("saga_messages")} ADD COLUMN IF NOT EXISTS seq bigserial;`,
    },
    {
      version: 8,
      // Workflow instances (ADR-0021); the journal and inbox are emptied when an instance finishes.
      sql: `CREATE TABLE IF NOT EXISTS ${table("workflows")} (
 group_name text NOT NULL, id text NOT NULL, version integer NOT NULL, status text NOT NULL,
 input jsonb, journal jsonb NOT NULL, inbox jsonb NOT NULL, state jsonb, result jsonb, error text,
 cancel_requested boolean NOT NULL DEFAULT false, started_at timestamptz NOT NULL, finished_at timestamptz,
 PRIMARY KEY(group_name,id));`,
    },
    {
      version: 9,
      // For `workflows list --status` and `workflows purge --finished-before`.
      sql: `CREATE INDEX IF NOT EXISTS workflows_status ON ${table("workflows")}(status,group_name,id);
CREATE INDEX IF NOT EXISTS workflows_finished ON ${table("workflows")}(finished_at) WHERE finished_at IS NOT NULL;`,
    },
    {
      version: 10,
      /*
       * A Saga or Workflow group without a `layout:` row is new and starts where `from`
       * says. Every group that already has rows gets the row it would have had (one Partition, no
       * recorded `from`, so no warning), so one halted before its first Checkpoint is not skipped.
       */
      sql: `INSERT INTO ${table("consumers")}(group_name,partition,status)
SELECT 'layout:'||g,0,jsonb_build_object('group','layout:'||g,'partition',0,'checkpoint',NULL,'state','running','partitions',1)
FROM (SELECT DISTINCT group_name AS g FROM ${table("consumers")} WHERE group_name LIKE 'saga:%' OR group_name LIKE 'workflow:%') existing
ON CONFLICT(group_name,partition) DO NOTHING;`,
    },
    {
      version: 11,
      // The run a Worker's reply must name; older instances have none.
      sql: `ALTER TABLE ${table("workflows")} ADD COLUMN IF NOT EXISTS run text;`,
    },
  ].map(({ version, sql }) => ({
    version,
    sql: `${sql}\nINSERT INTO ${table("migrations")}(version) VALUES(${version}) ON CONFLICT DO NOTHING;`,
  }));
  // Built CONCURRENTLY outside any transaction so appends keep running.
  const concurrentIndexes = () => [
    {
      name: "events_type_position",
      sql: `CREATE INDEX CONCURRENTLY IF NOT EXISTS "events_type_position" ON ${table("events")} ((context||'.'||aggregate||'.'||name),transaction_id,sequence);`,
    },
    {
      name: "outbox_due",
      sql: `CREATE INDEX CONCURRENTLY IF NOT EXISTS "outbox_due" ON ${table("outbox")}(not_before) WHERE NOT blocked;`,
    },
    {
      name: "outbox_immediate",
      sql: `CREATE INDEX CONCURRENTLY IF NOT EXISTS "outbox_immediate" ON ${table("outbox")}(transaction_id,ordinal) WHERE NOT blocked AND not_before IS NULL;`,
    },
    ...(options.metadataIndexes ?? []).map((path) => {
      const parts = path.split(".");
      if (parts.some((part) => !part || part.includes("\0")))
        throw new Error("Invalid metadata index path");
      const name = `metadata_${createHash("sha256").update(path).digest("hex").slice(0, 24)}`;
      return {
        name,
        sql: `CREATE INDEX CONCURRENTLY IF NOT EXISTS ${quoteIdentifier(name)} ON ${table("events")} ((metadata #>> ARRAY[${parts.map(literal).join(",")}]));`,
      };
    }),
  ];
  // Unapplied versions, then concurrent indexes that are missing or invalid.
  const pendingMigrations = async (client: Pick<Pool, "query">) => {
    const exists = (
      await client.query("SELECT to_regclass($1) IS NOT NULL AS exists", [
        table("migrations"),
      ])
    ).rows[0].exists;
    const applied = new Set<number>(
      exists
        ? (
            await client.query(`SELECT version FROM ${table("migrations")}`)
          ).rows.map((row) => row.version)
        : [],
    );
    const steps = [
      ...(exists ? [] : [bootstrapSQL]),
      ...migrations
        .filter((step) => !applied.has(step.version))
        .map((step) => step.sql),
    ];
    const indexes: string[] = [];
    for (const index of concurrentIndexes()) {
      const valid = (
        await client.query(
          "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2",
          [schema, index.name],
        )
      ).rows[0]?.indisvalid;
      if (valid) continue;
      // An interrupted concurrent build leaves an invalid index that IF NOT EXISTS would keep.
      indexes.push(
        ...(valid === false
          ? [
              `DROP INDEX CONCURRENTLY IF EXISTS ${table(index.name)};`,
              index.sql,
            ]
          : [index.sql]),
      );
    }
    return { steps, indexes };
  };
  const migrationSQL = () =>
    [
      bootstrapSQL,
      ...migrations.map((step) => step.sql),
      ...concurrentIndexes().map((index) => index.sql),
    ].join("\n");
  // A NOTIFY inside a transaction holds one database-wide lock from its commit through the WAL
  // flush, so every appending commit would queue behind the last. One notification after
  // commit per burst holds it only briefly; a lost one costs listeners a poll interval.
  const notifying = new WeakSet<PoolClient>();
  let sending: Promise<void> | undefined,
    again = false;
  const notifyListeners = () => {
    if (sending) {
      again = true;
      return;
    }
    sending = (async () => {
      do {
        again = false;
        await pool.query("SELECT pg_notify($1,'')", [channel]).catch(() => {});
      } while (again);
      sending = undefined;
    })();
  };
  const transaction = async <T>(
    work: (client: PoolClient) => Promise<T>,
    signal?: AbortSignal,
  ): Promise<T> => {
    signal?.throwIfAborted();
    const client = await pool.connect();
    try {
      // One round trip: a parameterless query may hold several statements.
      await client.query(
        "BEGIN; SET LOCAL statement_timeout = '30s'; SET LOCAL idle_in_transaction_session_timeout = '30s'",
      );
      const result = await work(client);
      signal?.throwIfAborted();
      await client.query("COMMIT");
      if (notifying.has(client)) notifyListeners();
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      notifying.delete(client);
      client.release();
    }
  };
  /** A concurrent writer took the Revision first: the unique index, not a pre-read, fences appends. */
  const revisionConflict = (error: unknown) =>
    (error as { code?: string })?.code === "23505" &&
    /_revision_key$/.test((error as { constraint?: string }).constraint ?? "");
  const streamLock = async (client: PoolClient, stream: StreamId) => {
    await client.query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))", [
      JSON.stringify([stream.context, stream.aggregate, stream.id]),
    ]);
  };
  const schedule = async (
    client: Pick<Pool, "query">,
    stream: StreamId,
    name: string,
  ): Promise<RecordedSchedule | null> => {
    const row = (
      await client.query(
        `SELECT command_id,due_at,dispatched_at,fingerprint FROM ${table("schedules")} WHERE context=$1 AND aggregate=$2 AND stream_id=$3 AND name=$4`,
        [stream.context, stream.aggregate, stream.id, name],
      )
    ).rows[0];
    return row
      ? {
          stream: { ...stream },
          name,
          commandId: row.command_id,
          dueAt: row.due_at.toISOString(),
          ...(row.dispatched_at
            ? { dispatchedAt: row.dispatched_at.toISOString() }
            : {}),
          fingerprint: row.fingerprint,
        }
      : null;
  };
  const revision = async (client: PoolClient, stream: StreamId) =>
    Number(
      (
        await client.query(
          `SELECT COALESCE(MAX(revision),0)::text AS revision FROM ${table("events")} WHERE context=$1 AND aggregate=$2 AND stream_id=$3`,
          [stream.context, stream.aggregate, stream.id],
        )
      ).rows[0].revision,
    );
  const addOutbox = async (client: PoolClient, entries: OutboxEntry[], replay = false) => {
    for (const entry of entries) await codec.validateOutbox(entry, replay);
    if (!entries.length) return;
    const keys = [
      ...new Set(entries.flatMap((entry) => entry.replaceKey ?? [])),
    ].sort();
    // Sorted so two writers never take the same keys in opposite orders.
    for (const key of keys)
      await client.query(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,1))",
        [key],
      );
    const existing = (
      await client.query(
        `SELECT id,replace_key AS "replaceKey",blocked FROM ${table("outbox")} WHERE id=ANY($1) OR replace_key=ANY($2)`,
        [entries.map((entry) => entry.id), keys],
      )
    ).rows;
    const { replaced, admitted } = admitOutbox(entries, existing);
    // Re-checked under the row lock: a row claimed since the SELECT keeps its delivery.
    if (replaced.length)
      await client.query(
        `DELETE FROM ${table("outbox")} WHERE id=ANY($1) AND replace_key IS NOT NULL AND NOT blocked`,
        [replaced],
      );
    // The primary key and UNIQUE replace_key still refuse a concurrent writer the rule did not see.
    if (admitted.length)
      await client.query(
        `INSERT INTO ${table("outbox")}(id,transaction_id,ordinal,entry,not_before,replace_key)
         SELECT id,pg_current_xact_id(),ordinal,entry::jsonb,not_before,replace_key
         FROM unnest($1::text[],$2::integer[],$3::text[],$4::timestamptz[],$5::text[]) AS u(id,ordinal,entry,not_before,replace_key)`,
        [
          admitted.map(([, entry]) => entry.id),
          admitted.map(([ordinal]) => ordinal),
          admitted.map(([, entry]) => JSON.stringify(entry)),
          admitted.map(([, entry]) => entry.notBefore ?? null),
          admitted.map(([, entry]) => entry.replaceKey ?? null),
        ],
      );
    // Wakes Outbox dispatchers on other instances once committed.
    notifying.add(client);
  };
  const slotOf = (message: SagaMessage) =>
    message.deadline === undefined ? `m:${message.id}` : `d:${message.deadline}`;
  const insertSagaMessage = (
    client: Pick<Pool, "query">,
    message: SagaMessage,
    // A Deadline replaces the instance's pending one of its name; an import keeps what is there.
    conflict: "replace" | "keep",
  ) => {
    const { attempt = 0, ...stored } = toJson(message) as SagaMessage;
    return client.query(
      `INSERT INTO ${table("saga_messages")}(group_name,instance,slot,id,hash,due_at,attempt,message) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT ${
        conflict === "keep"
          ? "DO NOTHING"
          : "(group_name,instance,slot) DO UPDATE SET id=EXCLUDED.id,due_at=EXCLUDED.due_at,attempt=EXCLUDED.attempt,dead=false,message=EXCLUDED.message,seq=DEFAULT"
      }`,
      [
        message.group,
        message.instance,
        slotOf(message),
        message.id,
        fnv1a(message.instance),
        message.dueAt,
        attempt,
        JSON.stringify(stored),
      ],
    );
  };
  const applySagaMessages = async (
    client: PoolClient,
    changes: SagaMessageChange[] = [],
  ) => {
    for (const change of changes)
      if ("send" in change)
        await insertSagaMessage(
          client,
          change.send,
          change.send.deadline === undefined ? "keep" : "replace",
        );
      else
        await client.query(
          `DELETE FROM ${table("saga_messages")} WHERE group_name=$1 AND instance=$2 AND slot LIKE 'd:%' AND ($3::text IS NULL OR slot='d:'||$3)`,
          [change.cancel.group, change.cancel.instance, change.cancel.deadline ?? null],
        );
    if (changes.some((change) => "send" in change)) notifying.add(client);
  };
  const insertEvent = async (
    client: PoolClient,
    event: Omit<RecordedEvent, "position">,
  ): Promise<RecordedEvent> => {
    await codec.validate(event);
    const row = (
      await client.query(
        `INSERT INTO ${table("events")}(id,context,aggregate,stream_id,revision,transaction_id,name,schema_version,command_id,causation_id,correlation_id,timestamp,metadata,traceparent)
      VALUES($1,$2,$3,$4,$5,pg_current_xact_id(),$6,$7,$8,$9,$10,$11,$12,$13) RETURNING transaction_id::text,sequence::text`,
        [
          event.id,
          event.stream.context,
          event.stream.aggregate,
          event.stream.id,
          event.revision,
          event.name,
          event.schemaVersion,
          event.commandId,
          event.causationId ?? null,
          event.correlationId,
          event.timestamp,
          JSON.stringify(event.metadata),
          event.traceparent ?? null,
        ],
      )
    ).rows[0];
    await client.query(
      `INSERT INTO ${table("event_payloads")}(event_id,payload) VALUES($1,$2)`,
      [event.id, JSON.stringify(event.payload)],
    );
    return {
      ...toJson(event),
      position: { transactionId: row.transaction_id, sequence: row.sequence },
    };
  };
  /*
   * One statement checks the Revision, records the Command id and inserts the Events with their
   * payloads. It must be the transaction's first write: its transaction id is then
   * assigned after its snapshot saw the Stream's previous Revision commit, so Positions follow
   * Revisions without a Stream lock. The unique Revision index refuses a concurrent writer that
   * the snapshot missed (see `revisionConflict`); `ON CONFLICT` waits for a concurrent append of
   * the same Command id and refuses it once that one commits.
   */
  const appendEvents = async (
    client: PoolClient,
    stream: StreamId,
    expectedRevision: number,
    events: Omit<RecordedEvent, "position">[],
    command: { id: string; fingerprint: string } | undefined,
  ): Promise<RecordedEvent[]> => {
    for (const event of events) await codec.validate(event);
    const column = <T>(pick: (event: Omit<RecordedEvent, "position">) => T) =>
      events.map(pick);
    const result = await client.query(
      `WITH current AS (
         SELECT COALESCE(MAX(revision),0) AS revision FROM ${table("events")}
         WHERE context=$1 AND aggregate=$2 AND stream_id=$3
       ), expected AS (SELECT 1 FROM current WHERE revision=$4::bigint),
       command AS (
         INSERT INTO ${table("commands")}(id,fingerprint,event_ids)
         SELECT $5,$6,$7::text[] FROM expected WHERE $5::text IS NOT NULL
         ON CONFLICT DO NOTHING RETURNING 1
       ), admitted AS (
         SELECT 1 FROM expected WHERE $5::text IS NULL OR EXISTS (SELECT 1 FROM command)
       ), inserted AS (
         INSERT INTO ${table("events")}(id,context,aggregate,stream_id,revision,transaction_id,name,schema_version,command_id,causation_id,correlation_id,timestamp,metadata,traceparent)
         SELECT u.id,$1,$2,$3,$4::bigint+u.o,pg_current_xact_id(),u.name,u.schema_version,u.command_id,u.causation_id,u.correlation_id,u.timestamp,u.metadata::jsonb,u.traceparent
         FROM unnest($7::text[],$8::text[],$9::integer[],$10::text[],$11::text[],$12::text[],$13::timestamptz[],$14::text[],$15::text[])
           WITH ORDINALITY AS u(id,name,schema_version,command_id,causation_id,correlation_id,timestamp,metadata,traceparent,o)
         CROSS JOIN admitted
         -- ORDER BY o hands rows to the INSERT in Revision order, so sequences follow it.
         ORDER BY u.o RETURNING id,transaction_id::text,sequence::text
       ), payloads AS (
         INSERT INTO ${table("event_payloads")}(event_id,payload)
         SELECT inserted.id,u.payload::jsonb FROM inserted
         JOIN unnest($7::text[],$16::text[]) AS u(id,payload) ON u.id=inserted.id
         RETURNING 1
       )
       SELECT (SELECT count(*) FROM expected)::int AS expected,
         (SELECT count(*) FROM admitted)::int AS admitted,
         (SELECT count(*) FROM payloads)::int AS payloads,
         inserted.id,inserted.transaction_id,inserted.sequence
       FROM (SELECT 1) singleton LEFT JOIN inserted ON true`,
      [
        stream.context,
        stream.aggregate,
        stream.id,
        expectedRevision,
        command?.id ?? null,
        command?.fingerprint ?? null,
        column((e) => e.id),
        column((e) => e.name),
        column((e) => e.schemaVersion),
        column((e) => e.commandId),
        column((e) => e.causationId ?? null),
        column((e) => e.correlationId),
        column((e) => e.timestamp),
        column((e) => JSON.stringify(e.metadata)),
        column((e) => e.traceparent ?? null),
        column((e) => JSON.stringify(e.payload)),
      ],
    );
    const first = result.rows[0]!;
    if (!first.expected) throw new ConcurrencyError("Stream Revision conflict");
    if (!first.admitted) throw new DuplicateCommandError();
    const positions = new Map<string, Position>(
      result.rows
        .filter((row) => row.id !== null)
        .map((row) => [
          row.id,
          { transactionId: row.transaction_id, sequence: row.sequence },
        ]),
    );
    if (positions.size !== events.length || first.payloads !== events.length)
      throw new Error("Append recorded an incomplete set of Events");
    return events.map((event) => ({
      ...toJson(event),
      position: positions.get(event.id)!,
    }));
  };
  const decode = (row: any): RecordedEvent => ({
    id: row.id,
    stream: {
      context: row.context,
      aggregate: row.aggregate,
      id: row.stream_id,
    },
    revision: Number(row.revision),
    position: { transactionId: row.transaction_id, sequence: row.sequence },
    name: row.name,
    schemaVersion: row.schema_version,
    commandId: row.command_id,
    ...(row.causation_id ? { causationId: row.causation_id } : {}),
    correlationId: row.correlation_id,
    timestamp: row.timestamp.toISOString(),
    metadata: row.metadata,
    payload: row.payload,
    ...(row.traceparent ? { traceparent: row.traceparent } : {}),
  });
  const headSQL = `SELECT e.transaction_id::text,e.sequence::text FROM ${table("events")} e WHERE e.transaction_id<pg_snapshot_xmin(pg_current_snapshot()) ORDER BY e.transaction_id DESC,e.sequence DESC LIMIT 1`;
  const select = `SELECT e.*,e.transaction_id::text,e.sequence::text,p.payload FROM ${table("events")} e JOIN ${table("event_payloads")} p ON p.event_id=e.id`;
  /** Holds the Lease row until commit, so no new owner commits in between. `SHARE` when the caller never updates the row. */
  const fence = async (
    client: PoolClient,
    lease: Lease,
    lock: "UPDATE" | "SHARE" = "UPDATE",
  ) => {
    const result = await client.query(
      `SELECT 1 FROM ${table("consumers")} WHERE group_name=$1 AND partition=$2 AND owner=$3 AND epoch=$4 AND expires_at>clock_timestamp() FOR ${lock}`,
      [lease.group, lease.partition, lease.owner, lease.epoch],
    );
    if (!result.rowCount) throw new LeaseLostError("Lease expired or replaced");
  };
  const checkpoint = async (
    client: PoolClient,
    lease: Lease,
    position: Position,
  ) => {
    const result = await client.query(
      `UPDATE ${table("consumers")} SET checkpoint_tx=$5::xid8,checkpoint_seq=$6::bigint WHERE group_name=$1 AND partition=$2 AND owner=$3 AND epoch=$4 AND expires_at>clock_timestamp() AND (checkpoint_tx IS NULL OR (checkpoint_tx,checkpoint_seq)<=($5::xid8,$6::bigint))`,
      [
        lease.group,
        lease.partition,
        lease.owner,
        lease.epoch,
        position.transactionId,
        position.sequence,
      ],
    );
    if (!result.rowCount)
      throw new LeaseLostError("Lease lost or Checkpoint would move backwards");
  };
  /** A Saga or Workflow commit: `write` stores its instances beside the Outbox, Saga Messages and Checkpoint. */
  const commitProcess = (
    lease: Lease,
    position: Position | null,
    entries: OutboxEntry[],
    messages: { changes?: SagaMessageChange[]; consumed?: string },
    write: (client: PoolClient) => Promise<void>,
  ) =>
    transaction(async (client) => {
      await fence(client, lease);
      if (
        messages.consumed !== undefined &&
        !(
          await client.query(
            `DELETE FROM ${table("saga_messages")} WHERE id=$1 AND NOT dead`,
            [messages.consumed],
          )
        ).rowCount
      )
        throw new StaleSagaMessageError();
      await applySagaMessages(client, messages.changes);
      await write(client);
      await addOutbox(client, entries);
      if (position) await checkpoint(client, lease, position);
    });
  const leaseFrom = (row: any): Lease => ({
    group: row.group_name,
    partition: row.partition,
    owner: row.owner,
    epoch: Number(row.epoch),
    expiresAt: row.expires_at.toISOString(),
  });
  const rowPosition = (row: any): Position | null =>
    row?.checkpoint_tx
      ? { transactionId: row.checkpoint_tx, sequence: row.checkpoint_seq }
      : null;
  /** An Outbox row `o` waits for no earlier row of its target: the ordering rule of ADR-0011, for every Outbox query. */
  const unblocked = `NOT EXISTS(SELECT 1 FROM ${table("outbox")} p WHERE p.target=o.target AND NOT p.blocked AND (p.transaction_id,p.ordinal)<(o.transaction_id,o.ordinal) AND (p.not_before IS NULL OR p.not_before<=statement_timestamp() OR p.entry ? 'attempt'))`;
  const store: Persistence & {
    migrate(options?: { dryRun?: boolean }): Promise<string>;
    migrationSQL(): string;
    notificationChannel: string;
    readerLag(): Promise<number>;
    poolStats(): PoolStats;
  } = {
    notificationChannel: channel,
    migrationSQL,
    async listen(wake) {
      const client = new Client(pool.options);
      listeners.add(client);
      client.on("error", () => {});
      client.on("notification", (notification) => {
        if (notification.channel === channel) wake();
      });
      try {
        await client.connect();
        await client.query(`LISTEN ${quoteIdentifier(channel)}`);
      } catch (error) {
        listeners.delete(client);
        await client.end().catch(() => {});
        throw error;
      }
      return async () => {
        listeners.delete(client);
        await client.end();
      };
    },
    poolStats: () => ({
      total: pool.totalCount,
      idle: pool.idleCount,
      waiting: pool.waitingCount,
    }),
    async readerLag() {
      const result = await pool.query(
        `SELECT COALESCE(EXTRACT(EPOCH FROM (clock_timestamp()-MIN(timestamp)))*1000,0) AS lag FROM ${table("events")} WHERE transaction_id>=pg_snapshot_xmin(pg_current_snapshot())`,
      );
      return Math.max(0, Number(result.rows[0].lag));
    },
    async assertMigrated() {
      const { steps, indexes } = await pendingMigrations(pool);
      if (steps.length || indexes.length)
        throw new Error(
          "Event store migrations are pending; run `event-fabric migrate` before starting",
        );
    },
    async migrate(migrateOptions = {}) {
      const dryRun = migrateOptions.dryRun ?? false;
      let client: PoolClient;
      try {
        client = await pool.connect();
      } catch (error) {
        // A dry run without a reachable database plans every step, as for an empty one.
        if (dryRun) return migrationSQL();
        throw error;
      }
      let locked = false;
      try {
        // Polled session lock: a blocked lock wait would stall a concurrent CREATE INDEX CONCURRENTLY.
        while (!dryRun && !locked) {
          locked = (
            await client.query(
              "SELECT pg_try_advisory_lock(hashtextextended($1,2)) AS locked",
              [schema],
            )
          ).rows[0].locked;
          if (!locked) await setTimeout(100);
        }
        const { steps, indexes } = await pendingMigrations(client);
        for (const sql of dryRun ? [] : steps) {
          await client.query("BEGIN");
          try {
            await client.query(sql);
            await client.query("COMMIT");
          } catch (error) {
            await client.query("ROLLBACK").catch(() => {});
            throw error;
          }
        }
        for (const sql of dryRun ? [] : indexes) await client.query(sql);
        return [...steps, ...indexes].join("\n");
      } finally {
        if (locked)
          await client
            .query("SELECT pg_advisory_unlock(hashtextextended($1,2))", [
              schema,
            ])
            .catch(() => {});
        client.release();
      }
    },
    async readStream(stream, readOptions) {
      const result = await pool.query(
        `${select} WHERE context=$1 AND aggregate=$2 AND stream_id=$3 AND revision <= $4 ORDER BY revision`,
        [
          stream.context,
          stream.aggregate,
          stream.id,
          readOptions?.atRevision ?? "9223372036854775807",
        ],
      );
      return Promise.all(result.rows.map((row) => codec.read(decode(row))));
    },
    registerUpcasters: codec.register,
    async eventNames() {
      // Skips from name to name through events_type_position: milliseconds instead of minutes on a log of millions of Events.
      const name = "context||'.'||aggregate||'.'||name";
      return (
        await pool.query(
          `WITH RECURSIVE names AS ((SELECT ${name} AS name FROM ${table("events")} ORDER BY 1 LIMIT 1) UNION ALL SELECT (SELECT ${name} FROM ${table("events")} WHERE ${name} > names.name ORDER BY 1 LIMIT 1) FROM names WHERE names.name IS NOT NULL) SELECT name FROM names WHERE name IS NOT NULL`,
        )
      ).rows.map((row) => row.name);
    },
    getSchedule(stream, name) {
      return schedule(pool, stream, name);
    },
    async acceptedCommand(id) {
      const row = (
        await pool.query(
          `SELECT fingerprint,event_ids,cancellations FROM ${table("commands")} WHERE id=$1`,
          [id],
        )
      ).rows[0];
      if (!row) return null;
      const result = await pool.query(
        `${select} WHERE e.id=ANY($1) ORDER BY e.transaction_id,e.sequence`,
        [row.event_ids],
      );
      return {
        fingerprint: row.fingerprint,
        ...(row.cancellations ? { cancellations: row.cancellations } : {}),
        events: await Promise.all(
          result.rows.map((event) => codec.read(decode(event))),
        ),
      };
    },
    async readAll(readOptions = {}) {
      assertEventNames(readOptions.eventNames);
      const direction = readOptions.reverse ? "DESC" : "ASC";
      // Reverse reads page backwards, so fromPosition is an exclusive upper bound.
      const after = readOptions.reverse ? "<" : ">";
      const result = await pool.query(
        `${select} WHERE e.transaction_id<pg_snapshot_xmin(pg_current_snapshot()) AND ($1::xid8 IS NULL OR (e.transaction_id,e.sequence)${after}($1::xid8,$2::bigint)) AND ($6::xid8 IS NULL OR (e.transaction_id,e.sequence)<=($6::xid8,$7::bigint)) AND ($3::text[] IS NULL OR (e.context||'.'||e.aggregate||'.'||e.name)=ANY($3)) AND ($4::text IS NULL OR e.correlation_id=$4) ORDER BY e.transaction_id ${direction},e.sequence ${direction} LIMIT $5`,
        [
          readOptions.fromPosition?.transactionId ?? null,
          readOptions.fromPosition?.sequence ?? null,
          readOptions.eventNames ?? null,
          readOptions.correlationId ?? null,
          readOptions.limit ?? 1000,
          readOptions.toPosition?.transactionId ?? null,
          readOptions.toPosition?.sequence ?? null,
        ],
      );
      return Promise.all(result.rows.map((row) => codec.read(decode(row))));
    },
    async readPage(readOptions = {}) {
      const limit = pageLimit(readOptions);
      const type = `(e.context||'.'||e.aggregate||'.'||e.name)`;
      const ceiling = `ceiling AS MATERIALIZED (
          SELECT transaction_id,sequence FROM ${table("events")}
          WHERE transaction_id<pg_snapshot_xmin(pg_current_snapshot())
            AND ($4::xid8 IS NULL OR (transaction_id,sequence)<=($4::xid8,$5::bigint))
          ORDER BY transaction_id DESC,sequence DESC LIMIT 1
        )`;
      const progress = (through: Position | null) =>
        readOptions.fromPosition &&
        (!through || comparePositions(through, readOptions.fromPosition) < 0)
          ? readOptions.fromPosition
          : through;
      /*
       * Selected Events that are most of the log read fastest in Position order: scan one page and
       * keep the selected ones (several times faster than seeking each type). A sparse
       * selection falls back to seeking each type, which skips unrelated history.
       */
      if (readOptions.eventNames !== undefined) {
        const scan = await pool.query(
          `WITH ${ceiling}, scanned AS MATERIALIZED (
            SELECT e.*,${type}=ANY($3::text[]) AS selected FROM ceiling h CROSS JOIN LATERAL (
              SELECT * FROM ${table("events")} e
              WHERE ($1::xid8 IS NULL OR (e.transaction_id,e.sequence)>($1::xid8,$2::bigint))
                AND (e.transaction_id,e.sequence)<=(h.transaction_id,h.sequence)
              ORDER BY e.transaction_id,e.sequence LIMIT $6) e
          ), last AS (SELECT transaction_id,sequence FROM scanned ORDER BY transaction_id DESC,sequence DESC LIMIT 1),
          -- Whether a selected Event remains between the scan and the ceiling: one index probe per type.
          more AS (SELECT EXISTS (SELECT 1 FROM unnest($3::text[]) AS names(name), last l, ceiling h
            CROSS JOIN LATERAL (SELECT 1 FROM ${table("events")} e
              WHERE (${type},e.transaction_id,e.sequence)>(names.name,l.transaction_id,l.sequence)
                AND (${type},e.transaction_id,e.sequence)<=(names.name,h.transaction_id,h.sequence)
              -- Without the index order the planner may seq-scan the log for this LIMIT 1 (tens of seconds on a log of millions of Events).
              ORDER BY ${type},e.transaction_id,e.sequence LIMIT 1) found) AS found)
          SELECT e.*,e.transaction_id::text,e.sequence::text,p.payload,
            (SELECT transaction_id::text FROM ceiling) AS through_tx,
            (SELECT sequence::text FROM ceiling) AS through_seq,
            (SELECT transaction_id::text FROM last) AS last_tx,
            (SELECT sequence::text FROM last) AS last_seq,
            (SELECT count(*) FROM scanned) AS scanned,
            (SELECT count(*) FROM scanned WHERE selected) AS kept,
            (SELECT found FROM more) AS more
          FROM (SELECT 1) singleton
          LEFT JOIN (SELECT * FROM scanned WHERE selected) e ON true
          LEFT JOIN ${table("event_payloads")} p ON p.event_id=e.id
          ORDER BY e.transaction_id,e.sequence`,
          [
            readOptions.fromPosition?.transactionId ?? null,
            readOptions.fromPosition?.sequence ?? null,
            [...new Set(readOptions.eventNames)],
            readOptions.toPosition?.transactionId ?? null,
            readOptions.toPosition?.sequence ?? null,
            limit,
          ],
        );
        const first = scan.rows[0]!,
          complete = Number(first.scanned) < limit || !first.more;
        if (complete || Number(first.kept) * 2 >= limit)
          return {
            events: await Promise.all(
              scan.rows
                .filter((row) => row.id !== null)
                .map((row) => codec.read(decode(row))),
            ),
            through: progress(
              complete
                ? first.through_tx
                  ? { transactionId: first.through_tx, sequence: first.through_seq }
                  : null
                : { transactionId: first.last_tx, sequence: first.last_seq },
            ),
            complete,
          };
      }
      // Bounds on (type, Position) select exactly one type and seek past its consumed history.
      const range = (name?: string) => {
        const key = `${name ? `${type},` : ""}e.transaction_id,e.sequence`;
        const after = name
          ? `(${key})>(${name},COALESCE($1::xid8,'0'::xid8),COALESCE($2::bigint,0))`
          : `($1::xid8 IS NULL OR (${key})>($1::xid8,$2::bigint))`;
        return `SELECT e.transaction_id,e.sequence FROM ${table("events")} e
          WHERE ${after} AND (${key})<=(${name ? `${name},` : ""}h.transaction_id,h.sequence)
          ORDER BY ${key} LIMIT $6`;
      };
      // One snapshot proves the ceiling complete; each type seeks Positions, then only the page's rows join.
      const selected = `SELECT candidate.* FROM ceiling h
        ${readOptions.eventNames === undefined ? "" : "CROSS JOIN names"}
        CROSS JOIN LATERAL (${range(readOptions.eventNames === undefined ? undefined : "names.name")}) candidate
        ORDER BY candidate.transaction_id,candidate.sequence LIMIT $6`;
      const result = await pool.query(
        `WITH names AS MATERIALIZED (SELECT unnest($3::text[]) AS name),
        ${ceiling}, selected AS MATERIALIZED (${selected})
        SELECT e.*,e.transaction_id::text,e.sequence::text,p.payload,
          (SELECT transaction_id::text FROM ceiling) AS through_tx,
          (SELECT sequence::text FROM ceiling) AS through_seq,
          (SELECT count(*)<=$7 FROM selected) AS complete
        FROM (SELECT 1) singleton
        LEFT JOIN (SELECT * FROM selected ORDER BY transaction_id,sequence LIMIT $7) s ON true
        LEFT JOIN ${table("events")} e ON (e.transaction_id,e.sequence)=(s.transaction_id,s.sequence)
        LEFT JOIN ${table("event_payloads")} p ON p.event_id=e.id
        ORDER BY e.transaction_id,e.sequence`,
        [
          readOptions.fromPosition?.transactionId ?? null,
          readOptions.fromPosition?.sequence ?? null,
          readOptions.eventNames === undefined
            ? null
            : [...new Set(readOptions.eventNames)],
          readOptions.toPosition?.transactionId ?? null,
          readOptions.toPosition?.sequence ?? null,
          limit + 1,
          limit,
        ],
      );
      const first = result.rows[0]!;
      const events = await Promise.all(
        result.rows
          .filter((row) => row.id !== null)
          .map((row) => codec.read(decode(row))),
      );
      return {
        events,
        through: progress(
          first.complete
            ? first.through_tx
              ? { transactionId: first.through_tx, sequence: first.through_seq }
              : null
            : events.at(-1)!.position,
        ),
        complete: first.complete,
      };
    },
    appendToStream(stream, expectedRevision, pending, command, appendOptions) {
      return transaction(async (client) => {
        assertStream(stream, expectedRevision);
        const outbox = appendOptions?.outbox ?? [];
        // Admission of a scheduled Command takes the Stream lock: Schedule changes keep it.
        if (
          !appendOptions?.schedules?.length &&
          !appendOptions?.scheduleGuard &&
          !outbox.some(isScheduledAttempt)
        ) {
          const saved = await appendEvents(
            client,
            stream,
            expectedRevision,
            pending.map((event, index) => ({
              ...event,
              stream,
              revision: expectedRevision + index + 1,
            })),
            command && appendOptions?.fingerprint !== undefined
              ? { id: command.id, fingerprint: appendOptions.fingerprint }
              : undefined,
          );
          await addOutbox(client, outbox);
          notifying.add(client);
          return saved;
        }
        await streamLock(client, stream);
        const current = await revision(client, stream);
        if (current !== expectedRevision)
          throw new ConcurrencyError("Stream Revision conflict");
        const changes = appendOptions?.schedules ?? [];
        assertScheduleChanges(stream, changes, appendOptions?.outbox ?? []);
        const guard = appendOptions?.scheduleGuard;
        assertScheduleGuard(
          guard ? await schedule(client, stream, guard.name) : null,
          guard,
          !!(pending.length || changes.length || appendOptions?.outbox?.length),
        );
        // Waits for a concurrent append of the same id, then refuses it once that one commits.
        if (command && appendOptions?.fingerprint !== undefined) {
          const recorded = await client.query(
            `INSERT INTO ${table("commands")}(id,fingerprint,event_ids) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`,
            [
              command.id,
              appendOptions.fingerprint,
              pending.map((event) => event.id),
            ],
          );
          if (!recorded.rowCount) throw new DuplicateCommandError();
        }
        const saved: RecordedEvent[] = [];
        for (const [index, event] of pending.entries())
          saved.push(
            await insertEvent(client, {
              ...event,
              stream,
              revision: current + index + 1,
            }),
          );
        await addOutbox(client, appendOptions?.outbox ?? []);
        const cancellations: ScheduleCancellations = Object.create(null);
        for (const change of changes) {
          const previous = await schedule(client, stream, change.name);
          if (previous)
            await client.query(
              `DELETE FROM ${table("outbox")} WHERE entry#>'{message,schedule}' IS NOT NULL AND entry#>>'{message,id}'=$1`,
              [previous.commandId],
            );
          if (!change.entry) {
            cancellations[change.name] = cancellationOf(previous);
            await client.query(
              `DELETE FROM ${table("schedules")} WHERE context=$1 AND aggregate=$2 AND stream_id=$3 AND name=$4`,
              [stream.context, stream.aggregate, stream.id, change.name],
            );
          } else {
            const scheduled = change.entry.message as Command;
            await client.query(
              `INSERT INTO ${table("schedules")}(context,aggregate,stream_id,name,command_id,due_at,fingerprint) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(context,aggregate,stream_id,name) DO UPDATE SET command_id=EXCLUDED.command_id,due_at=EXCLUDED.due_at,fingerprint=EXCLUDED.fingerprint,dispatched_at=NULL`,
              [
                stream.context,
                stream.aggregate,
                stream.id,
                change.name,
                scheduled.id,
                scheduled.schedule!.dueAt,
                commandFingerprint(scheduled),
              ],
            );
          }
        }
        if (guard && !guard.obsolete)
          await client.query(
            `DELETE FROM ${table("schedules")} WHERE command_id=$1`,
            [guard.commandId],
          );
        if (command && Object.keys(cancellations).length)
          await client.query(
            `UPDATE ${table("commands")} SET cancellations=$2 WHERE id=$1`,
            [command.id, cancellations],
          );
        notifying.add(client);
        return saved;
      }, appendOptions?.signal).catch((error) => {
        throw revisionConflict(error)
          ? new ConcurrencyError("Stream Revision conflict")
          : error;
      });
    },
    // Set-based, so a cutover import costs a few statements per batch rather than several per Event.
    import(historical, importOptions) {
      return transaction(async (client) => {
        for (const event of historical) assertStream(event.stream);
        for (const event of historical) await codec.validate(event);
        const key = (stream: StreamId) =>
          JSON.stringify([stream.context, stream.aggregate, stream.id]);
        const streams = new Map(
          historical.map((event) => [key(event.stream), event.stream]),
        );
        const sorted = [...streams.keys()].sort((a, b) => a.localeCompare(b));
        await client.query(
          "SELECT pg_advisory_xact_lock(hashtextextended(k,0)) FROM unnest($1::text[]) WITH ORDINALITY AS u(k,o) ORDER BY o",
          [sorted],
        );
        const revisions = new Map<string, number>(
          (
            await client.query(
              `SELECT u.context,u.aggregate,u.stream_id,COALESCE(MAX(e.revision),0)::text AS revision
               FROM unnest($1::text[],$2::text[],$3::text[]) AS u(context,aggregate,stream_id)
               LEFT JOIN ${table("events")} e ON e.context=u.context AND e.aggregate=u.aggregate AND e.stream_id=u.stream_id
               GROUP BY u.context,u.aggregate,u.stream_id`,
              [
                sorted.map((k) => streams.get(k)!.context),
                sorted.map((k) => streams.get(k)!.aggregate),
                sorted.map((k) => streams.get(k)!.id),
              ],
            )
          ).rows.map((row) => [
            key({
              context: row.context,
              aggregate: row.aggregate,
              id: row.stream_id,
            }),
            Number(row.revision),
          ]),
        );
        for (const event of historical) {
          const id = key(event.stream);
          if (event.revision !== revisions.get(id)! + 1)
            throw new ConcurrencyError(
              "Imported Stream Revision must be contiguous",
            );
          revisions.set(id, event.revision);
        }
        if (!historical.length) return [];
        const column = <T>(pick: (event: RecordedEvent) => T) =>
          historical.map(pick);
        // ORDER BY o hands rows to the INSERT in input order, so sequences follow it.
        const positions = new Map(
          (
            await client.query(
              `INSERT INTO ${table("events")}(id,context,aggregate,stream_id,revision,transaction_id,name,schema_version,command_id,causation_id,correlation_id,timestamp,metadata,traceparent)
               SELECT id,context,aggregate,stream_id,revision,pg_current_xact_id(),name,schema_version,command_id,causation_id,correlation_id,timestamp,metadata::jsonb,traceparent
               FROM unnest($1::text[],$2::text[],$3::text[],$4::text[],$5::bigint[],$6::text[],$7::integer[],$8::text[],$9::text[],$10::text[],$11::timestamptz[],$12::text[],$13::text[])
                 WITH ORDINALITY AS u(id,context,aggregate,stream_id,revision,name,schema_version,command_id,causation_id,correlation_id,timestamp,metadata,traceparent,o)
               ORDER BY o RETURNING id,transaction_id::text,sequence::text`,
              [
                column((e) => e.id),
                column((e) => e.stream.context),
                column((e) => e.stream.aggregate),
                column((e) => e.stream.id),
                column((e) => e.revision),
                column((e) => e.name),
                column((e) => e.schemaVersion),
                column((e) => e.commandId),
                column((e) => e.causationId ?? null),
                column((e) => e.correlationId),
                column((e) => e.timestamp),
                column((e) => JSON.stringify(e.metadata)),
                column((e) => e.traceparent ?? null),
              ],
            )
          ).rows.map((row) => [
            row.id as string,
            { transactionId: row.transaction_id, sequence: row.sequence },
          ]),
        );
        await client.query(
          `INSERT INTO ${table("event_payloads")}(event_id,payload) SELECT id,payload::jsonb FROM unnest($1::text[],$2::text[]) AS u(id,payload)`,
          [column((e) => e.id), column((e) => JSON.stringify(e.payload))],
        );
        notifying.add(client);
        return historical.map((event) => ({
          ...toJson(event),
          position: positions.get(event.id)!,
        }));
      }, importOptions?.signal);
    },
    async head() {
      const row = (await pool.query(headSQL)).rows[0];
      return row
        ? { transactionId: row.transaction_id, sequence: row.sequence }
        : null;
    },
    async acquireLease(group, owner, ttlMs = 30000, partition = 0) {
      assertLeaseTTL(ttlMs);
      const row = (
        await pool.query(
          `INSERT INTO ${table("consumers")} AS c(group_name,partition,owner,epoch,expires_at) VALUES($1,$2,$3,1,clock_timestamp()+$4*interval '1 millisecond') ON CONFLICT(group_name,partition) DO UPDATE SET owner=$3,epoch=c.epoch+1,expires_at=clock_timestamp()+$4*interval '1 millisecond' WHERE c.expires_at IS NULL OR c.expires_at<=clock_timestamp() RETURNING *`,
          [group, partition, owner, ttlMs],
        )
      ).rows[0];
      return row ? leaseFrom(row) : null;
    },
    async renewLeases(leases, ttlMs = 30000) {
      assertLeaseTTL(ttlMs);
      if (!leases.length) return [];
      const id = (lease: Lease) =>
        JSON.stringify([
          lease.group,
          lease.partition,
          lease.owner,
          lease.epoch,
        ]);
      const renewed = new Map(
        (
          await pool.query(
            `UPDATE ${table("consumers")} c SET expires_at=clock_timestamp()+$5*interval '1 millisecond' FROM unnest($1::text[],$2::integer[],$3::text[],$4::bigint[]) AS l(group_name,partition,owner,epoch) WHERE c.group_name=l.group_name AND c.partition=l.partition AND c.owner=l.owner AND c.epoch=l.epoch AND c.expires_at>clock_timestamp() RETURNING c.*`,
            [
              leases.map((lease) => lease.group),
              leases.map((lease) => lease.partition),
              leases.map((lease) => lease.owner),
              leases.map((lease) => lease.epoch),
              ttlMs,
            ],
          )
        ).rows.map((row) => {
          const lease = leaseFrom(row);
          return [id(lease), lease];
        }),
      );
      return leases.map((lease) =>
        structuredClone(renewed.get(id(lease)) ?? null),
      );
    },
    async releaseLease(lease) {
      await pool.query(
        `UPDATE ${table("consumers")} SET expires_at=clock_timestamp() WHERE group_name=$1 AND partition=$2 AND owner=$3 AND epoch=$4`,
        [lease.group, lease.partition, lease.owner, lease.epoch],
      );
    },
    async checkpoint(group, partition = 0) {
      return rowPosition(
        (
          await pool.query(
            `SELECT checkpoint_tx::text,checkpoint_seq::text FROM ${table("consumers")} WHERE group_name=$1 AND partition=$2`,
            [group, partition],
          )
        ).rows[0],
      );
    },
    commitCheckpoint(lease, position) {
      return transaction(async (client) => {
        await fence(client, lease);
        await checkpoint(client, lease, position);
      });
    },
    async positionAt(at) {
      // shortcut: no index on timestamp, so this scans the log in Position order once per new group; add one if starts get slow.
      const row = (
        await pool.query(
          `WITH visible AS (SELECT e.transaction_id,e.sequence,e.timestamp FROM ${table("events")} e WHERE e.transaction_id<pg_snapshot_xmin(pg_current_snapshot())),
first AS (SELECT transaction_id,sequence FROM visible WHERE timestamp>=$1 ORDER BY transaction_id,sequence LIMIT 1)
SELECT v.transaction_id::text,v.sequence::text FROM visible v WHERE NOT EXISTS(SELECT 1 FROM first) OR (v.transaction_id,v.sequence)<(SELECT transaction_id,sequence FROM first) ORDER BY v.transaction_id DESC,v.sequence DESC LIMIT 1`,
          [at],
        )
      ).rows[0];
      return row
        ? { transactionId: row.transaction_id, sequence: row.sequence }
        : null;
    },
    startConsumer(group, from, position, leases) {
      return transaction(async (client) => {
        for (const lease of leases) await fence(client, lease);
        const started = await client.query(
          `SELECT 1 FROM ${table("consumers")} WHERE (group_name=$1 AND checkpoint_tx IS NOT NULL) OR group_name=$2 LIMIT 1`,
          [group, `layout:${group}`],
        );
        if (started.rowCount) return false;
        if (position)
          for (const lease of leases) await checkpoint(client, lease, position);
        const status: ConsumerStatus = {
          group: `layout:${group}`,
          partition: 0,
          checkpoint: position,
          state: "running",
          partitions: leases.length,
          from,
        };
        await client.query(
          `INSERT INTO ${table("consumers")}(group_name,partition,status) VALUES($1,0,$2)`,
          [status.group, JSON.stringify(status)],
        );
        return true;
      }).catch((error) => {
        if (error instanceof LeaseLostError) return false;
        throw error;
      });
    },
    async loadSaga(group, id) {
      return (
        (
          await pool.query(
            `SELECT state FROM ${table("sagas")} WHERE group_name=$1 AND id=$2`,
            [group, id],
          )
        ).rows[0]?.state ?? null
      );
    },
    commitSaga(lease, position, changes, entries, messages = {}) {
      return commitProcess(lease, position, entries, messages, async (client) => {
        for (const change of changes) {
          assertJson(change.state);
          if (change.state === null)
            await client.query(
              `DELETE FROM ${table("sagas")} WHERE group_name=$1 AND id=$2`,
              [lease.group, change.id],
            );
          else
            await client.query(
              `INSERT INTO ${table("sagas")}(group_name,id,state) VALUES($1,$2,$3) ON CONFLICT(group_name,id) DO UPDATE SET state=EXCLUDED.state`,
              [lease.group, change.id, JSON.stringify(change.state)],
            );
        }
      });
    },
    async loadWorkflow(group, id) {
      const row = (
        await pool.query(
          // JSONB null is a value the body set; SQL NULL is none.
          `SELECT *, state IS NULL AS no_state, result IS NULL AS no_result FROM ${table("workflows")} WHERE group_name=$1 AND id=$2`,
          [group, id],
        )
      ).rows[0];
      if (!row) return null;
      const optional = (key: string, value: unknown) =>
        value === null ? {} : { [key]: value };
      return {
        id: row.id,
        version: row.version,
        status: row.status,
        input: row.input,
        journal: row.journal,
        inbox: row.inbox,
        ...(row.no_state ? {} : { state: row.state }),
        ...(row.no_result ? {} : { result: row.result }),
        ...optional("error", row.error),
        ...optional("run", row.run),
        ...(row.cancel_requested ? { cancelRequested: true } : {}),
        startedAt: row.started_at.toISOString(),
        ...optional("finishedAt", row.finished_at?.toISOString() ?? null),
      };
    },
    async listWorkflows(status) {
      const { rows } = await pool.query(
        `SELECT group_name,id,version,status,error,started_at,finished_at FROM ${table("workflows")}
        ${status ? "WHERE status=$1" : ""} ORDER BY group_name,id`,
        status ? [status] : [],
      );
      return rows.map((row) => ({
        group: row.group_name,
        id: row.id,
        version: row.version,
        status: row.status,
        ...(row.error === null ? {} : { error: row.error }),
        startedAt: row.started_at.toISOString(),
        ...(row.finished_at === null ? {} : { finishedAt: row.finished_at.toISOString() }),
      }));
    },
    async purgeWorkflows(finishedBefore) {
      return (
        await pool.query(`DELETE FROM ${table("workflows")} WHERE finished_at < $1`, [finishedBefore])
      ).rowCount!;
    },
    commitWorkflow(lease, position, changes, entries, messages = {}) {
      return commitProcess(lease, position, entries, messages, async (client) => {
        for (const change of changes) {
          assertJson(change);
          await client.query(
            `INSERT INTO ${table("workflows")}(group_name,id,version,status,input,journal,inbox,state,result,error,cancel_requested,started_at,finished_at,run)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT(group_name,id) DO UPDATE SET
            version=EXCLUDED.version,status=EXCLUDED.status,input=EXCLUDED.input,journal=EXCLUDED.journal,inbox=EXCLUDED.inbox,
            state=EXCLUDED.state,result=EXCLUDED.result,error=EXCLUDED.error,cancel_requested=EXCLUDED.cancel_requested,
            started_at=EXCLUDED.started_at,finished_at=EXCLUDED.finished_at,run=EXCLUDED.run`,
            [
              lease.group,
              change.id,
              change.version,
              change.status,
              JSON.stringify(change.input ?? null),
              JSON.stringify(change.journal),
              JSON.stringify(change.inbox),
              JSON.stringify(change.state),
              JSON.stringify(change.result),
              change.error ?? null,
              !!change.cancelRequested,
              change.startedAt,
              change.finishedAt ?? null,
              change.run ?? null,
            ],
          );
        }
      });
    },
    recordSagaMessages(messages) {
      return transaction(async (client) => {
        for (const message of messages) await insertSagaMessage(client, message, "keep");
        if (messages.length) notifying.add(client);
      });
    },
    async dueSagaMessages(group, slice, limit) {
      const live = `group_name=$1 AND NOT dead AND ($2::integer IS NULL OR hash%$2=$3)`;
      const rows = (
        await pool.query(
          `(SELECT message,attempt,due_at,true AS due,0::float8 AS delay FROM ${table("saga_messages")} WHERE ${live} AND due_at<=statement_timestamp() ORDER BY due_at,seq LIMIT $4)
          UNION ALL
          (SELECT NULL,NULL,due_at,false,EXTRACT(EPOCH FROM (due_at-statement_timestamp()))*1000 FROM ${table("saga_messages")} WHERE ${live} AND due_at>statement_timestamp() ORDER BY due_at LIMIT 1)`,
          [group, slice?.count ?? null, slice?.partition ?? 0, limit],
        )
      ).rows;
      const next = rows.find((row) => !row.due);
      return {
        messages: rows
          .filter((row) => row.due)
          .map((row) => ({ ...row.message, attempt: row.attempt, dueAt: row.due_at.toISOString() })),
        // Measured on the store's clock, as `nextOutboxDelay` is.
        nextDelay: next ? Math.max(0, Math.ceil(Number(next.delay))) : null,
      };
    },
    retrySagaMessage(lease, id, attempt, dueAt) {
      return transaction(async (client) => {
        await fence(client, lease);
        await client.query(
          `UPDATE ${table("saga_messages")} SET attempt=$2,due_at=$3 WHERE id=$1 AND NOT dead`,
          [id, attempt, dueAt],
        );
      });
    },
    deadLetterSagaMessage(lease, id, error) {
      return transaction(async (client) => {
        await fence(client, lease);
        const row = (
          await client.query(
            `UPDATE ${table("saga_messages")} SET dead=true WHERE id=$1 AND NOT dead RETURNING message,attempt,due_at`,
            [id],
          )
        ).rows[0];
        if (!row) return;
        const letter = sagaMessageLetter(
          { ...row.message, attempt: row.attempt, dueAt: row.due_at.toISOString() },
          error,
        );
        await client.query(
          `INSERT INTO ${table("dead_letters")}(id,letter) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET letter=EXCLUDED.letter`,
          [id, JSON.stringify(letter)],
        );
      });
    },
    importSagas(group, states, position) {
      return transaction(async (client) => {
        const head = (await client.query(headSQL)).rows[0];
        assertImportAtHead(
          position,
          head
            ? { transactionId: head.transaction_id, sequence: head.sequence }
            : null,
        );
        await client.query(
          `INSERT INTO ${table("consumers")}(group_name,partition) VALUES($1,0) ON CONFLICT DO NOTHING`,
          [group],
        );
        const current = (
          await client.query(
            `SELECT * FROM ${table("consumers")} WHERE group_name=$1 AND partition=0 FOR UPDATE`,
            [group],
          )
        ).rows[0];
        if (current.expires_at && current.expires_at.getTime() > Date.now())
          throw new Error("Cannot import Sagas with an active Lease");
        for (const state of states) assertJson(state.state);
        for (const state of states)
          await client.query(
            `INSERT INTO ${table("sagas")}(group_name,id,state) VALUES($1,$2,$3) ON CONFLICT(group_name,id) DO UPDATE SET state=EXCLUDED.state`,
            [group, state.id, JSON.stringify(state.state)],
          );
        await client.query(
          `UPDATE ${table("consumers")} SET checkpoint_tx=$2::xid8,checkpoint_seq=$3::bigint WHERE group_name=$1 AND partition=0`,
          [group, position?.transactionId ?? null, position?.sequence ?? null],
        );
      });
    },
    recordOutbox(entries) {
      return transaction((client) => addOutbox(client, entries));
    },
    async pendingOutbox(limit = 100, excludeIds = []) {
      // Index immediate work separately so retained future Schedules cannot slow its Position-order scan.
      const eligible = `o.transaction_id<pg_snapshot_xmin(pg_current_snapshot()) AND NOT o.blocked AND NOT(o.id=ANY($2::text[]))`;
      const rows = (
        await pool.query(
          `SELECT batch.*,batch.transaction_id::text FROM (
            (SELECT o.* FROM ${table("outbox")} o WHERE ${eligible} AND o.not_before IS NULL AND ${unblocked} ORDER BY o.transaction_id,o.ordinal LIMIT $1)
            UNION ALL
            (SELECT o.* FROM ${table("outbox")} o WHERE ${eligible} AND o.not_before<=statement_timestamp() AND ${unblocked} ORDER BY o.transaction_id,o.ordinal LIMIT $1)
          ) batch ORDER BY batch.transaction_id,batch.ordinal LIMIT $1`,
          [limit, excludeIds],
        )
      ).rows;
      return rows.map((row) => ({
        ...row.entry,
        transactionId: row.transaction_id,
        ordinal: row.ordinal,
        createdAt: row.created_at.toISOString(),
        blocked: row.blocked,
      }));
    },
    async nextOutboxDelay(excludeIds = []) {
      const row = (
        await pool.query(
          `SELECT GREATEST(0,EXTRACT(EPOCH FROM (not_before-statement_timestamp()))*1000) AS delay FROM ${table("outbox")} o WHERE o.transaction_id<pg_snapshot_xmin(pg_current_snapshot()) AND NOT blocked AND not_before IS NOT NULL AND NOT(o.id=ANY($1::text[])) AND ${unblocked} ORDER BY not_before LIMIT 1`,
          [excludeIds],
        )
      ).rows[0];
      return row ? Math.ceil(Number(row.delay)) : null;
    },
    admitOutbox(entries, lease) {
      if (!entries.length) return Promise.resolve([]);
      return transaction(async (client) => {
        const scheduled = entries.filter(
          (entry) => entry.kind === "command" && (entry.message as Command).schedule,
        );
        // Admission of a scheduled Command takes its Stream's lock; one order keeps two Admissions from deadlocking.
        const streams = new Map(
          scheduled.map((entry) => {
            const { stream } = entry.message as Command;
            return [JSON.stringify([stream.context, stream.aggregate, stream.id]), stream];
          }),
        );
        for (const key of [...streams.keys()].sort())
          await streamLock(client, streams.get(key)!);
        // Shared: parallel deliveries of one Lease must not queue behind each other.
        await fence(client, lease, "SHARE");
        // Admission releases the replacement key; admitted deliveries may still repeat after a crash.
        const admitted: string[] = (
          await client.query(
            `UPDATE ${table("outbox")} o SET replace_key=NULL WHERE id=ANY($1::text[]) AND o.transaction_id<pg_snapshot_xmin(pg_current_snapshot()) AND NOT blocked AND (not_before IS NULL OR not_before<=statement_timestamp()) AND ${unblocked} RETURNING id`,
            [entries.map((entry) => entry.id)],
          )
        ).rows.map((row) => row.id);
        const dispatched = scheduled
          .filter((entry) => admitted.includes(entry.id))
          .map((entry) => (entry.message as Command).id);
        if (dispatched.length)
          await client.query(
            `UPDATE ${table("schedules")} SET dispatched_at=COALESCE(dispatched_at,clock_timestamp()) WHERE command_id=ANY($1::text[])`,
            [dispatched],
          );
        return admitted;
      });
    },
    async claimOutbox(id, lease, known) {
      // A row's message never changes under its id, so the caller's copy can stand in.
      const entry =
        known ??
        ((
          await pool.query(`SELECT entry FROM ${table("outbox")} WHERE id=$1`, [id])
        ).rows[0]?.entry as OutboxEntry | undefined);
      return (
        await store.admitOutbox([{ ...(entry ?? ({ kind: "message" } as OutboxEntry)), id }], lease)
      ).length === 1;
    },
    async outboxStats() {
      const result = await pool.query(
        `SELECT count(*)::integer AS pending, COALESCE(EXTRACT(EPOCH FROM (clock_timestamp()-MIN(created_at)))*1000,0) AS age FROM ${table("outbox")} WHERE NOT blocked`,
      );
      return {
        pending: result.rows[0].pending,
        oldestAgeMs: Math.max(0, Number(result.rows[0].age)),
      };
    },
    async completeOutbox(ids) {
      await pool.query(`DELETE FROM ${table("outbox")} WHERE id=ANY($1::text[])`, [[ids].flat()]);
    },
    async retryOutbox(id, attempt, notBefore) {
      await pool.query(
        `WITH retried AS (UPDATE ${table("outbox")} SET entry=entry||jsonb_build_object('attempt',$2::integer,'notBefore',$3::text),not_before=$3::timestamptz WHERE id=$1 RETURNING 1) SELECT pg_notify($4,'') FROM retried`,
        [id, attempt, notBefore, channel],
      );
    },
    deadLetterOutbox(id, error) {
      return transaction(async (client) => {
        const entry = (
          await client.query(
            `SELECT entry FROM ${table("outbox")} WHERE id=$1`,
            [id],
          )
        ).rows[0]?.entry as OutboxEntry | undefined;
        if (isScheduledAttempt(entry))
          await streamLock(client, (entry.message as Command).stream);
        const row = (
          await client.query(
            `UPDATE ${table("outbox")} SET blocked=true WHERE id=$1 RETURNING *,transaction_id::text`,
            [id],
          )
        ).rows[0];
        if (!row) return;
        const letter = outboxLetter(row.entry, row.transaction_id, error);
        await client.query(
          `INSERT INTO ${table("dead_letters")}(id,letter) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET letter=EXCLUDED.letter`,
          [id, JSON.stringify(letter)],
        );
      });
    },
    async recordDeadLetter(letter) {
      await pool.query(
        `INSERT INTO ${table("dead_letters")}(id,letter) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET letter=EXCLUDED.letter`,
        [letter.id, JSON.stringify(letter)],
      );
    },
    async deadLetters() {
      return (
        await pool.query(
          `SELECT letter FROM ${table("dead_letters")} ORDER BY id`,
        )
      ).rows.map((row) => row.letter);
    },
    replayDeadLetter(id) {
      return transaction(async (client) => {
        const letter = (
          await client.query(
            `SELECT letter FROM ${table("dead_letters")} WHERE id=$1`,
            [id],
          )
        ).rows[0]?.letter as DeadLetter | undefined;
        if (letter?.kind === "saga-message") {
          // Replaced or cancelled since: the row is gone or under another id, and the replay is a no-op.
          await client.query(
            `UPDATE ${table("saga_messages")} SET dead=false,attempt=0,due_at=clock_timestamp() WHERE id=$1 AND dead`,
            [id],
          );
          await client.query(`DELETE FROM ${table("dead_letters")} WHERE id=$1`, [id]);
          notifying.add(client);
          return;
        }
        if (
          letter?.message?.kind === "command" &&
          (letter.message.message as Command).schedule
        )
          await streamLock(client, (letter.message.message as Command).stream);
        const entry = replayable(
          (
            await client.query(
              `SELECT letter FROM ${table("dead_letters")} WHERE id=$1 FOR UPDATE`,
              [id],
            )
          ).rows[0]?.letter,
        );
        const row = (
          await client.query(
            `SELECT entry FROM ${table("outbox")} WHERE id=$1 FOR UPDATE`,
            [id],
          )
        ).rows[0];
        if (row) {
          const replayed = replayedEntry(row.entry);
          await client.query(
            `UPDATE ${table("outbox")} SET id=$2,blocked=false,not_before=$4,entry=$3 WHERE id=$1`,
            [
              id,
              replayed.id,
              JSON.stringify(replayed),
              replayed.notBefore ?? null,
            ],
          );
        } else await addOutbox(client, [replayedEntry(entry)], true);
        await client.query(`DELETE FROM ${table("dead_letters")} WHERE id=$1`, [
          id,
        ]);
      });
    },
    discardDeadLetter(id) {
      return transaction(async (client) => {
        const letter = (
          await client.query(
            `SELECT letter FROM ${table("dead_letters")} WHERE id=$1`,
            [id],
          )
        ).rows[0]?.letter as DeadLetter | undefined;
        if (
          letter?.message?.kind === "command" &&
          (letter.message.message as Command).schedule
        )
          await streamLock(client, (letter.message.message as Command).stream);
        const removed = await client.query(
          `DELETE FROM ${table("dead_letters")} WHERE id=$1 RETURNING letter`,
          [id],
        );
        const entry = removed.rows[0]?.letter.message as
          | OutboxEntry
          | undefined;
        if (isScheduledAttempt(entry))
          await client.query(
            `DELETE FROM ${table("schedules")} WHERE command_id=$1`,
            [entry.message.id],
          );
        await client.query(
          `DELETE FROM ${table("outbox")} WHERE id=$1 AND blocked`,
          [id],
        );
        if (removed.rows[0]?.letter.kind === "saga-message")
          await client.query(
            `DELETE FROM ${table("saga_messages")} WHERE id=$1 AND dead`,
            [id],
          );
      });
    },
    setConsumerStatus(status, lease) {
      return transaction(async (client) => {
        if (lease) {
          await fence(client, lease);
          if (
            status.group !== lease.group ||
            status.partition !== lease.partition
          )
            throw new LeaseLostError("Consumer status does not match Lease");
        }
        await client.query(
          `INSERT INTO ${table("consumers")}(group_name,partition,status) VALUES($1,$2,$3) ON CONFLICT(group_name,partition) DO UPDATE SET status=EXCLUDED.status`,
          [status.group, status.partition, JSON.stringify(status)],
        );
      });
    },
    async publishLayout(group, from, to, position, leases) {
      return transaction(async (client) => {
        for (const lease of leases) await fence(client, lease);
        const row = (
          await client.query(
            `SELECT status FROM ${table("consumers")} WHERE group_name=$1 AND partition=0 FOR UPDATE`,
            [`layout:${group}`],
          )
        ).rows[0];
        if ((row?.status?.partitions ?? 1) !== from) return false;
        const status: ConsumerStatus = {
          group: `layout:${group}`,
          partition: 0,
          checkpoint: position,
          state: "running",
          partitions: to,
          ...(row?.status?.from && { from: row.status.from }),
        };
        await client.query(
          `INSERT INTO ${table("consumers")}(group_name,partition,status) VALUES($1,0,$2) ON CONFLICT(group_name,partition) DO UPDATE SET status=EXCLUDED.status`,
          [status.group, JSON.stringify(status)],
        );
        return true;
      }).catch((error) => {
        if (error instanceof LeaseLostError) return false;
        throw error;
      });
    },
    async consumers() {
      return (
        await pool.query(
          `SELECT *,checkpoint_tx::text,checkpoint_seq::text FROM ${table("consumers")} ORDER BY group_name,partition`,
        )
      ).rows.map((row) => ({
        ...row.status,
        group: row.group_name,
        partition: row.partition,
        checkpoint: rowPosition(row),
        ...(row.owner ? { lease: leaseFrom(row) } : {}),
      }));
    },
    async retryConsumer(group) {
      await pool.query(
        `WITH retried AS (UPDATE ${table("consumers")} SET status=status||jsonb_build_object('retryAt',$2::text) WHERE group_name=$1 RETURNING 1) SELECT pg_notify($3,'') FROM retried LIMIT 1`,
        [group, retryNow, channel],
      );
    },
    retireConsumer(group) {
      return transaction(async (client) => {
        const rows = (
          await client.query(
            `SELECT expires_at>clock_timestamp() AS live FROM ${table("consumers")} WHERE group_name=$1 FOR UPDATE`,
            [group],
          )
        ).rows;
        if (rows.some((row) => row.live))
          throw new Error(`Consumer group ${group} holds a live Lease`);
        await client.query(
          `DELETE FROM ${table("consumers")} WHERE group_name=ANY($1)`,
          [[group, `layout:${group}`]],
        );
        await client.query(
          `DELETE FROM ${table("sagas")} WHERE group_name=$1`,
          [group],
        );
        await client.query(
          `DELETE FROM ${table("workflows")} WHERE group_name=$1`,
          [group],
        );
        await client.query(
          `DELETE FROM ${table("saga_messages")} WHERE group_name=$1`,
          [group],
        );
      });
    },
    skipConsumer(group, eventId, who, options) {
      return transaction(async (client) => {
        const row = (
          await client.query(
            `SELECT * FROM ${table("consumers")} WHERE group_name=$1 AND partition=$2 FOR UPDATE`,
            [group, options?.partition ?? 0],
          )
        ).rows[0];
        const letter = skippedEventLetter({
          group,
          eventId,
          who,
          status: row?.status,
          checkpoint: row ? rowPosition(row) : null,
          epoch: row ? Number(row.epoch) : undefined,
          expectedEpoch: options?.expectedEpoch,
          partition: options?.partition,
        });
        await client.query(
          `INSERT INTO ${table("dead_letters")}(id,letter) VALUES($1,$2)`,
          [letter.id, JSON.stringify(letter)],
        );
        await client.query(
          `UPDATE ${table("consumers")} SET checkpoint_tx=$2::xid8,checkpoint_seq=$3::bigint,status='{"state":"running"}',epoch=epoch+1,expires_at=clock_timestamp() WHERE group_name=$1 AND partition=$4`,
          [
            group,
            letter.position.transactionId,
            letter.position.sequence,
            options?.partition ?? 0,
          ],
        );
      });
    },
    async ping() {
      await pool.query("SELECT 1");
    },
    async close() {
      await Promise.all([...listeners].map((client) => client.end()));
      listeners.clear();
      if (!options.pool) await pool.end();
    },
  };
  return store;
}
