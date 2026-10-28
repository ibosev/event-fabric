import { connect } from "amqplib";
import type {
  Channel,
  ChannelModel,
  ConfirmChannel,
  ConsumeMessage,
} from "amqplib";
import { randomUUID } from "node:crypto";
import type {
  Command,
  CommandLane,
  Outcome,
  Stop,
  Transport,
} from "./types.js";
import { commandPartition, commandPriority } from "./transport/partition.js";

type RabbitMQOptions = {
  url: string;
  application: string;
  /** Names this instance in Command Partition priorities; a fresh id per process by default. */
  instance?: string;
  /** Messages a broadcast subscription's queue holds before the oldest are dropped. */
  broadcastMaxLength?: number;
};

/**
 * Connects on `connect()` (the Application's start) or on first use, so loading a config
 * for `migrate`, `rebuild` or a rebuild worker opens no broker connection.
 */
export async function createRabbitMQTransport(
  options: RabbitMQOptions,
): Promise<Transport> {
  const { application, broadcastMaxLength = 10000 } = options;
  if (!application || application.length > 100)
    throw new Error("Invalid Application name");
  if (!Number.isSafeInteger(broadcastMaxLength) || broadcastMaxLength < 1)
    throw new Error("Invalid broadcastMaxLength");
  let opening: Promise<Transport> | undefined,
    opened: Transport | undefined,
    closed = false;
  const open = () => {
    // A closed transport that never connected fails like a disconnected one.
    if (closed && !opening)
      return Promise.reject(new Error("RabbitMQ disconnected"));
    return (opening ??= connectTransport(options).then((t) => (opened = t)));
  };
  return {
    connect: async () => void (await open()),
    // A broker that cannot be reached leaves the Command undelivered, so its outcome is unknown.
    send: (command, options) =>
      open().then(
        (t) => t.send(command, options),
        () => ({ status: "unknown", commandId: command.id }),
      ),
    handle: async (...args) => (await open()).handle(...args),
    publish: async (...args) => (await open()).publish(...args),
    subscribe: async (...args) => (await open()).subscribe(...args),
    enqueue: async (...args) => (await open()).enqueue(...args),
    consume: async (...args) => (await open()).consume(...args),
    connected: () => opened?.connected() ?? false,
    async close() {
      closed = true;
      await opening?.then(
        (t) => t.close(),
        () => {},
      );
    },
  };
}

async function connectTransport({
  url,
  application,
  instance = randomUUID(),
  broadcastMaxLength = 10000,
}: RabbitMQOptions): Promise<Transport> {
  const connection = await connect(url);
  let connected = true;
  const pending = new Map<
    string,
    {
      id: string;
      resolve: (outcome: Outcome) => void;
      timer: ReturnType<typeof setTimeout>;
    }
  >();
  const stops = new Set<Stop>();
  const disconnect = () => {
    connected = false;
    for (const p of pending.values()) {
      clearTimeout(p.timer);
      p.resolve({ status: "unknown", commandId: p.id });
    }
    pending.clear();
  };
  connection.on("error", disconnect);
  connection.on("close", disconnect);
  const publisher = await connection.createConfirmChannel();
  const rpc = await connection.createConfirmChannel();
  for (const ch of [publisher, rpc]) {
    ch.on("error", disconnect);
    ch.on("close", disconnect);
  }
  const broadcastExchange = `${application}.broadcast`;
  await publisher.assertExchange(broadcastExchange, "topic", { durable: true });
  await rpc.consume(
    "amq.rabbitmq.reply-to",
    (message) => {
      if (!message) return;
      const id = message.properties.correlationId,
        p = pending.get(id);
      if (p) {
        clearTimeout(p.timer);
        pending.delete(id);
        try {
          p.resolve(JSON.parse(message.content.toString()));
        } catch {
          p.resolve({ status: "unknown", commandId: p.id });
        }
      }
    },
    { noAck: true },
  );
  const confirmed = (
    ch: ConfirmChannel,
    exchange: string,
    key: string,
    value: any,
    options: object = {},
  ) =>
    new Promise<void>((resolve, reject) => {
      if (!connected) {
        reject(new Error("RabbitMQ disconnected"));
        return;
      }
      ch.publish(
        exchange,
        key,
        Buffer.from(JSON.stringify(value)),
        { contentType: "application/json", ...options },
        (error) => (error ? reject(error) : resolve()),
      );
    });
  // The interactive lane keeps the original queue name; the bulk lane is a second queue beside it.
  const commandQueue = (context: string, p: number, lane?: CommandLane) =>
    `${application}.commands.${context}.${p}${lane === "bulk" ? ".bulk" : ""}`;
  const assertCommandQueue = (
    ch: Channel,
    context: string,
    p: number,
    lane?: CommandLane,
  ) =>
    ch.assertQueue(commandQueue(context, p, lane), {
      durable: true,
      // Quorum queues honour consumer priority under single active consumer (RabbitMQ 4.0+).
      arguments: {
        "x-queue-type": "quorum",
        "x-single-active-consumer": true,
      },
    });
  const consumer = async (
    configure: (
      ch: Channel,
      launch: (
        m: ConsumeMessage | null,
        fn: (m: ConsumeMessage) => Promise<void>,
      ) => void,
    ) => Promise<string[]>,
    conn: ChannelModel = connection,
  ): Promise<Stop> => {
    const own = conn !== connection;
    const ch = await conn.createChannel();
    // A failure while an own connection is set up is the caller's to retry; once it consumes, it is broker loss.
    let ready = !own,
      stopped = false;
    const lost = () => {
      if (ready && !stopped) disconnect();
    };
    ch.on("error", lost);
    if (own) conn.on("close", lost);
    const tasks = new Set<Promise<void>>();
    const launch = (
      m: ConsumeMessage | null,
      fn: (m: ConsumeMessage) => Promise<void>,
    ) => {
      // A message arriving while stopping stays unacked; closing the channel requeues it in place.
      if (!m || stopped) return;
      const task = fn(m)
        .catch(() => {
          if (connected) ch.nack(m, false, true);
        })
        .finally(() => tasks.delete(task));
      tasks.add(task);
    };
    let tags: string[];
    try {
      tags = await configure(ch, launch);
    } catch (error) {
      await ch.close().catch(() => {});
      if (own) await conn.close().catch(() => {});
      throw error;
    }
    ready = true;
    const stop = async () => {
      if (stopped) return;
      stopped = true;
      stops.delete(stop);
      // Finish in-flight work before cancelling: a cancel hands a single-active-consumer queue to the next consumer at once.
      await Promise.all(tasks);
      if (connected) await Promise.all(tags.map((tag) => ch.cancel(tag)));
      await ch.close().catch(() => {});
      if (own) await conn.close().catch(() => {});
    };
    stops.add(stop);
    return stop;
  };
  const handlers = (
    context: string,
    partitions: number[],
    handler: Parameters<Transport["handle"]>[2],
    conn: ChannelModel,
  ) =>
    consumer(async (ch, launch) => {
      await ch.prefetch(1);
      const tags: string[] = [];
      for (const p of partitions)
        if (!Number.isInteger(p) || p < 0 || p >= 128)
          throw new Error("Invalid Command Partition");
      for (const p of partitions)
        for (const lane of ["interactive", "bulk"] as const) {
          await assertCommandQueue(ch, context, p, lane);
          const { consumerTag } = await ch.consume(
            commandQueue(context, p, lane),
            (m) =>
              launch(m, async (delivery) => {
                let outcome: Outcome;
                try {
                  outcome = await handler(
                    JSON.parse(delivery.content.toString()) as Command,
                  );
                } catch {
                  outcome = {
                    status: "failed",
                    error: { message: "Command handling failed" },
                  };
                }
                if (delivery.properties.replyTo)
                  await confirmed(
                    publisher,
                    "",
                    delivery.properties.replyTo,
                    outcome,
                    { correlationId: delivery.properties.correlationId },
                  );
                ch.ack(delivery);
              }),
            // Both lanes of a Partition share its priority, so one replica normally holds both.
            { priority: commandPriority(instance, context, p) },
          );
          tags.push(consumerTag);
        }
      return tags;
    }, conn);
  return {
    async send(command, options) {
      const { deadline = 10000, lane } = options ?? {};
      if (!connected) return { status: "unknown", commandId: command.id };
      const correlationId = randomUUID();
      return new Promise<Outcome>((resolve) => {
        const timer = setTimeout(() => {
          pending.delete(correlationId);
          resolve({ status: "unknown", commandId: command.id });
        }, deadline);
        pending.set(correlationId, { id: command.id, resolve, timer });
        void (async () => {
          const { context } = command.stream;
          const p = commandPartition(command);
          await assertCommandQueue(rpc, context, p, lane);
          await confirmed(rpc, "", commandQueue(context, p, lane), command, {
            persistent: true,
            correlationId,
            replyTo: "amq.rabbitmq.reply-to",
          });
        })().catch(() => {
          clearTimeout(timer);
          pending.delete(correlationId);
          resolve({ status: "unknown", commandId: command.id });
        });
      });
    },
    async handle(context, partitions, handler) {
      // RabbitMQ closes the whole connection when a consume fails, so each Context's handlers get their own.
      for (let attempt = 0; ; attempt++) {
        if (!connected) throw new Error("RabbitMQ disconnected");
        const own = await connect(url);
        let reason: Error | undefined;
        own.on("error", (error) => (reason = error));
        try {
          return await handlers(context, partitions, handler, own);
        } catch (error) {
          // A quorum queue another instance is declaring exists before its Ra cluster runs: consuming it then
          // fails with `noproc`.
          const starting = [reason, error].some((e) =>
            /noproc/.test(String((e as Error | undefined)?.message)),
          );
          if (!starting || attempt >= 4) throw error;
          await new Promise((resolve) =>
            setTimeout(resolve, 100 * 2 ** attempt),
          );
        }
      }
    },
    async publish(topic, message) {
      await confirmed(publisher, broadcastExchange, topic, message);
    },
    async subscribe(topic, handler) {
      return consumer(async (ch) => {
        /*
         * A process that falls behind drops its oldest Notifications at the broker instead of buffering them.
         * Each is acknowledged as it is read, still at most once: the prefetch keeps the backlog in the queue,
         * where its length is capped, and not in the broker's or this process's buffers. A small window also keeps
         * large Notifications from delaying Command replies on the same connection.
         */
        await ch.prefetch(10);
        const { queue } = await ch.assertQueue("", {
          exclusive: true,
          autoDelete: true,
          arguments: {
            "x-max-length": broadcastMaxLength,
            "x-overflow": "drop-head",
          },
        });
        await ch.bindQueue(queue, broadcastExchange, topic);
        const { consumerTag } = await ch.consume(queue, (m) => {
          if (!m) return;
          ch.ack(m);
          void Promise.resolve()
            .then(() => handler(JSON.parse(m.content.toString())))
            .catch(() => {});
        });
        return [consumerTag];
      });
    },
    async enqueue(queue, message) {
      // A closed channel would otherwise fail the assert with a bare "Channel closed".
      if (!connected) throw new Error("RabbitMQ disconnected");
      await publisher.assertQueue(queue, { durable: true });
      await confirmed(publisher, "", queue, message, { persistent: true });
    },
    async consume(queue, handler, { concurrency = 1 } = {}) {
      if (!Number.isInteger(concurrency) || concurrency < 1)
        throw new Error("Invalid concurrency");
      return consumer(async (ch, launch) => {
        await ch.assertQueue(queue, { durable: true });
        await ch.prefetch(concurrency);
        const { consumerTag } = await ch.consume(queue, (m) =>
          launch(m, async (delivery) => {
            await handler(JSON.parse(delivery.content.toString()));
            ch.ack(delivery);
          }),
        );
        return [consumerTag];
      });
    },
    connected: () => connected,
    async close() {
      await Promise.all([...stops].map((stop) => stop()));
      disconnect();
      await connection.close().catch(() => {});
    },
  };
}
