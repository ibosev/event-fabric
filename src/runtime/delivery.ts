import { uuidv7 } from "../id.js";
import type { CommandInput } from "../id.js";
import type { MessageRouting } from "../effects.js";
import { outboxLetter } from "../event-store/rules.js";
import {
  count,
  measure,
  qualifiedName,
  recovery,
  streamName,
  withSpan,
} from "../observability.js";
import type { Timings } from "../observability.js";
import type {
  Command,
  CommandLane,
  Lease,
  Logger,
  Message,
  Outcome,
  OutboxRow,
  Persistence,
  Transport,
  WorkerReply,
} from "../types.js";
import type { Readmodels, SagaMessageOptions, WorkerDefinition } from "./types.js";
import { parseTime, workflowWake } from "../internal.js";
import type { WorkflowCalls } from "../workflows/index.js";

export class PermanentError extends Error {
  constructor(public original: unknown) {
    super(original instanceof Error ? original.message : String(original));
  }
}
export const permanent = (error: unknown): PermanentError =>
  new PermanentError(error);
const errorMessage = (error: unknown) =>
  error instanceof Error ? error.message : String(error);

/** Exponential backoff for the nth failed attempt, shared by the Outbox relay and Workers. */
export const retryDelay = (
  attempt: number,
  { baseDelay = 1000, maxDelay = 600000 } = {},
) => Math.min(maxDelay, baseDelay * 2 ** (attempt - 1));
const outboxAttempts = 5;
// An `unknown` outcome is re-sent without limit (a Duplicate Command is harmless); warn once it lasts this long.
const unknownWarnAfter = 10 * 60_000;

/**
 * Delivers claimed Outbox rows under the outbox Lease: rows for one Stream in
 * order, everything else independently, with retry and dead-lettering.
 */
export function createOutboxRelay(options: {
  store: Persistence;
  transport: Transport;
  routing: MessageRouting;
  logger: Logger;
  /** Whether this instance still holds the Lease for that group. */
  held: (group: string) => boolean;
  /** False once the Application stops; a delivery failing after that is expected. */
  running: () => boolean;
  wake: () => void;
  timings?: Timings;
}) {
  const { store, transport, routing, logger, held, wake } = options;
  const time = <T>(stage: string, work: () => Promise<T>) =>
    options.timings ? options.timings.time("outbox", stage, work) : work();
  const dispatching = new Map<string, Promise<void>>();
  const warnedUnknown = new Set<string>();
  let nextStatsAt = 0;
  // Completions that arrive while one is written share the next write.
  let completing: { id: string; done: (error?: unknown) => void }[] = [],
    writing = false;
  const complete = (id: string) =>
    new Promise<void>((resolve, reject) => {
      completing.push({ id, done: (error) => (error ? reject(error) : resolve()) });
      void (async () => {
        if (writing) return;
        writing = true;
        while (completing.length) {
          const batch = completing;
          completing = [];
          const error = await time("complete", () =>
            store.completeOutbox(batch.map((entry) => entry.id)),
          ).then(
            () => undefined,
            (error) => error ?? new Error("Outbox completion failed"),
          );
          for (const entry of batch) entry.done(error);
        }
        writing = false;
      })();
    });
  /** Delivers rows already admitted under `lease`. */
  const deliver = async (lease: Lease, entries: OutboxRow[]) => {
    try {
      for (const row of entries.sort((a, b) => a.ordinal - b.ordinal)) {
        count("eventfabric.outbox.attempts");
        let outcomeReceived = false;
        try {
          const outcome = await time("send", () =>
            withSpan(
              "eventfabric.outbox.dispatch",
              row.message,
              async () => {
                if (row.kind === "command")
                  return transport.send(row.message as Command, {
                    lane: "bulk",
                  });
                await transport.enqueue(
                  routing.queue(row.message.name),
                  row.message as Message,
                );
                return { status: "accepted", events: [] } as Outcome;
              },
              true,
            ),
          );
          outcomeReceived = true;
          options.timings?.add("outbox", outcome.status);
          if (outcome.status === "accepted" || outcome.status === "rejected") {
            if (outcome.status === "rejected") {
              // Not retried (the same Command is refused again) and not blocking its Stream;
              // dead-lettered so an operator sees it and can replay it.
              // The letter is an upsert by entry id, written before the entry completes.
              const { transactionId, ordinal: _, createdAt: __, blocked: ___, ...entry } = row;
              const { stream, name } = row.message as Command;
              await store.recordDeadLetter({
                ...outboxLetter(entry, transactionId, outcome.error.message),
                kind: "outbox-rejected",
                rejection: outcome.error,
              });
              count("eventfabric.outbox.rejected", {
                command: qualifiedName({ stream, name }),
                code: outcome.error.code,
              });
              logger.warn(
                {
                  correlationId: row.message.correlationId,
                  causationId: row.message.causationId,
                  commandId: row.message.id,
                  command: name,
                  stream: streamName(stream),
                  code: outcome.error.code,
                  deadLetterId: row.id,
                  recovery: recovery.deadLetter(row.id),
                },
                "Outbox Command rejected; dead-lettered",
              );
            }
            await complete(row.id);
            warnedUnknown.delete(row.id);
            continue;
          }
          if (outcome.status === "unknown") {
            count("eventfabric.outbox.unknown");
            if (
              Date.now() - parseTime(row.createdAt) >= unknownWarnAfter &&
              !warnedUnknown.has(row.id)
            ) {
              warnedUnknown.add(row.id);
              logger.warn(
                {
                  consumerGroup: lease.group,
                  commandId: row.message.id,
                  correlationId: row.message.correlationId,
                  since: row.createdAt,
                },
                "Outbox Command outcome still unknown after 10 minutes; re-sending",
              );
            }
            await store.retryOutbox(
              row.id,
              row.attempt ?? 0,
              new Date(Date.now() + 1000).toISOString(),
            );
            break;
          }
          throw new Error(outcome.error.message);
        } catch (error) {
          if (!outcomeReceived) options.timings?.add("outbox", "sendErrors");
          // The broker is gone, not the Message at fault: the entry stays pending without an attempt.
          if (!outcomeReceived && !transport.connected()) throw error;
          const attempt = (row.attempt ?? 0) + 1;
          if (attempt >= outboxAttempts) {
            await store.deadLetterOutbox(row.id, errorMessage(error));
            count("eventfabric.outbox.dead_letters");
            logger.error(
              {
                consumerGroup: lease.group,
                messageId: row.message.id,
                causationId: row.message.causationId,
                correlationId: row.message.correlationId,
                error: errorMessage(error),
                deadLetterId: row.id,
                recovery: recovery.deadLetter(row.id),
              },
              "Outbox entry dead-lettered",
            );
          } else
            await store.retryOutbox(
              row.id,
              attempt,
              new Date(Date.now() + retryDelay(attempt)).toISOString(),
            );
          break;
        }
      }
      return true;
    } catch (error) {
      // The row stays pending; the dispatcher picks it up again on its next pass. Stopping
      // or losing the broker interrupts deliveries as expected, so those log as warnings.
      const expected = !options.running() || !transport.connected();
      logger[expected ? "warn" : "error"](
        { consumerGroup: lease.group, error: errorMessage(error) },
        "Outbox delivery failed",
      );
      return false;
    }
  };
  const dispatch = async (lease: Lease) => {
    const capacity = 100 - dispatching.size;
    const rows = capacity > 0 ? await time("poll", () => store.pendingOutbox(capacity, [...dispatching.keys()])) : [];
    options.timings?.add("outbox", "polledRows", rows.length);
    // Exact counts include delayed rows; sample them instead of scanning on every wake-up.
    if (Date.now() >= nextStatsAt) {
      const stats = await store.outboxStats();
      nextStatsAt = Date.now() + 5000;
      measure("eventfabric.outbox.pending", stats.pending);
      measure("eventfabric.outbox.oldest_age", stats.oldestAgeMs);
    }
    // Each pending row is the next one for its Stream (or a queued Message), so rows run
    // independently: a slow one never delays another's. They are admitted together.
    const fresh = rows.filter((row) => !dispatching.has(row.id));
    let admitted: Set<string>;
    try {
      admitted = new Set(
        fresh.length && held(lease.group)
          ? await time("claim", () => store.admitOutbox(fresh, lease))
          : [],
      );
    } catch (error) {
      const expected = !options.running() || !transport.connected();
      logger[expected ? "warn" : "error"](
        { consumerGroup: lease.group, error: errorMessage(error) },
        "Outbox delivery failed",
      );
      return false;
    }
    for (const row of fresh) {
      if (!admitted.has(row.id)) continue;
      const delivery = deliver(lease, [row]).then((delivered) => {
        dispatching.delete(row.id);
        // A failed store call waits for the next poll instead of spinning.
        if (delivered) wake();
      });
      dispatching.set(row.id, delivery);
    }
    // Only a full batch suggests more due rows; otherwise wait for a wake, so steady load
    // does not poll back-to-back and starve other Consumers of database time.
    return capacity > 0 && rows.length === capacity;
  };
  return {
    dispatch,
    nextDelay: () => dispatching.size >= 100 ? Promise.resolve(null) : store.nextOutboxDelay([...dispatching.keys()]),
    /** Deliveries still in flight, for shutdown to wait on. */
    inFlight: () => [...dispatching.values()],
  };
}

/**
 * The Saga Message a Worker replies with to the instance that queued `message`, or none. Its id
 * comes from the Idempotency key and the outcome, so a re-run after a crash repeats the same reply.
 */
export const workerReply = (message: Message, payload: WorkerReply) => {
  const to = message.replyTo;
  if (!to) return undefined;
  const options = { to: to.instance, id: `${message.idempotencyKey}:${payload.outcome}` };
  // A Workflow's reply is its Saga Message `reply:<step>`, which wakes the `ask` step.
  return "workflow" in to
    ? { workflow: to.workflow, name: workflowWake.reply(workflowWake.step(to.step, to.run)), payload, options }
    : { saga: to.saga, name: to.name, payload, options };
};

/**
 * Runs one Worker attempt: timeout and abort, the Idempotency key, and the
 * retry schedule or dead letter when it fails.
 */
export function createWorkerRunner(options: {
  store: Persistence;
  logger: Logger;
  /** The Application's services, read at each attempt. */
  services: () => Record<string, any>;
  readmodels: Readmodels;
  sendCommand: (
    input: CommandInput,
    options?: { lane?: CommandLane },
  ) => Promise<Outcome>;
  /** Sends a Saga Message caused by `cause`. */
  sendSagaMessage: (
    saga: string,
    name: string,
    payload: unknown,
    options: SagaMessageOptions | undefined,
    cause: Message,
  ) => Promise<void>;
  /** Sends a Workflow instance its Saga Message, caused by `cause`. */
  sendWorkflowMessage: (
    workflow: string,
    name: string,
    payload: unknown,
    options: { to: string; id: string },
    cause: Message,
  ) => Promise<void>;
  /** A Worker's `startWorkflow`, `sendSignal` and `queryWorkflow`, caused by its Message. */
  workflowCalls: (cause: Message) => WorkflowCalls;
  workerQueue: (worker: string) => string;
  wake: () => void;
  random?: () => number;
}) {
  const { store, logger, services, sendCommand, sendSagaMessage, sendWorkflowMessage, workerQueue, wake } =
    options;
  const random = options.random ?? Math.random;
  const reply = async (message: Message, payload: WorkerReply) => {
    const sent = workerReply(message, payload);
    if (sent?.workflow !== undefined)
      await sendWorkflowMessage(sent.workflow, sent.name, sent.payload, sent.options, message);
    else if (sent) await sendSagaMessage(sent.saga, sent.name, sent.payload, sent.options, message);
  };
  return async (
    name: string,
    worker: WorkerDefinition<any, any>,
    message: Message,
  ) => {
    const attempt = message.attempt ?? 1,
      startedAt = Date.now();
    const child = logger.child({
      worker: name,
      messageId: message.id,
      causationId: message.causationId,
      correlationId: message.correlationId,
    });
    const controller = new AbortController();
    const expiresAt = performance.now() + (worker.timeout ?? 60000);
    let timer: ReturnType<typeof setTimeout> | undefined;
    count("eventfabric.worker.attempts", { worker: name });
    try {
      if (!message.idempotencyKey)
        throw permanent(new Error("Queued Message has no Idempotency key"));
      const value = await withSpan(
        "eventfabric.worker.attempt",
        message,
        () =>
          Promise.race([
            Promise.resolve().then(() =>
              worker.execute(message, {
                idempotencyKey: message.idempotencyKey!,
                attempt,
                signal: controller.signal,
                services: services(),
                readmodels: options.readmodels,
                logger: child,
                send: (input) =>
                  sendCommand(
                    {
                      ...input,
                      metadata: { ...message.metadata, ...input.metadata },
                      causationId: message.id,
                      correlationId: message.correlationId,
                      traceparent: message.traceparent,
                    },
                    { lane: "bulk" },
                  ),
                sendSagaMessage: (saga, name, payload, sent) =>
                  sendSagaMessage(saga, name, payload, sent, message),
                ...options.workflowCalls(message),
              }),
            ),
            new Promise<never>((_, reject) => {
              timer = setTimeout(() => {
                controller.abort();
                reject(new Error("Worker attempt timed out"));
              }, worker.timeout ?? 60000);
            }),
          ]),
        true,
      );
      if (performance.now() >= expiresAt) {
        controller.abort();
        throw new Error("Worker attempt timed out");
      }
      // A reply that cannot be sent fails the attempt; the retry repeats `execute`, as after a crash.
      await reply(message, { outcome: "ok", value: value ?? null });
    } catch (error) {
      count("eventfabric.worker.failures", { worker: name });
      if (
        error instanceof PermanentError ||
        attempt >= (worker.retry?.attempts ?? 5)
      ) {
        const deadLetterId = uuidv7();
        await store.recordDeadLetter({
          id: deadLetterId,
          kind: "worker",
          message: {
            id: message.id,
            kind: "message",
            message,
            queue: workerQueue(name),
          },
          attempts: attempt,
          error: errorMessage(error),
          correlationId: message.correlationId,
          createdAt: new Date().toISOString(),
        });
        count("eventfabric.worker.dead_letters", { worker: name });
        child.error(
          {
            error: errorMessage(error),
            attempt,
            deadLetterId,
            recovery: recovery.deadLetter(deadLetterId),
          },
          "Worker Message dead-lettered",
        );
        // Replaying the dead letter runs the Worker again, and that run replies.
        await reply(message, { outcome: "failed", error: errorMessage(error), attempts: attempt }).catch(
          (error) =>
            child.error(
              { error: errorMessage(error), deadLetterId, recovery: recovery.deadLetter(deadLetterId) },
              "Worker reply not sent",
            ),
        );
      } else {
        const delay = retryDelay(attempt, worker.retry);
        await store.recordOutbox([
          {
            id: uuidv7(),
            kind: "message",
            message: { ...message, attempt: attempt + 1 },
            queue: workerQueue(name),
            notBefore: new Date(
              Date.now() + delay * (0.5 + random() * 0.5),
            ).toISOString(),
          },
        ]);
        child.warn(
          { attempt, error: errorMessage(error) },
          "Worker Message retry scheduled",
        );
        wake();
      }
    } finally {
      if (timer) clearTimeout(timer);
      measure("eventfabric.worker.duration", Date.now() - startedAt, {
        worker: name,
      });
    }
  };
}
