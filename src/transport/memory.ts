import type { Command, Message, Outcome, Stop, Transport } from "../types.js";
import { randomUUID } from "node:crypto";
import { commandPartition, commandPriority } from "./partition.js";

type Delivery = { message: any; reply?: (outcome: Outcome) => void };
type Consumer = {
  run: (delivery: Delivery) => Promise<void>;
  active: number;
  concurrency: number;
  stopped: boolean;
  single: boolean;
  priority: number;
  tasks: Set<Promise<void>>;
};
export const createInMemoryBroker = () => ({
  queues: new Map<string, Delivery[]>(),
  consumers: new Map<string, Consumer[]>(),
  subscribers: new Map<string, Set<(message: any) => void | Promise<void>>>(),
});
export type InMemoryBroker = ReturnType<typeof createInMemoryBroker>;
export function createInMemoryTransport({
  broker = createInMemoryBroker(),
  instance = randomUUID(),
}: { broker?: InMemoryBroker; instance?: string } = {}): Transport {
  let closed = false;
  const stops = new Set<Stop>();
  const pending = new Set<() => void>();
  const assertOpen = () => {
    if (closed) throw new Error("Transport closed");
  };
  const drain = (queue: string) => {
    const messages = broker.queues.get(queue) ?? [];
    const consumers = broker.consumers.get(queue) ?? [];
    // Like a RabbitMQ quorum queue's single active consumer: a consumer with work in
    // flight stays active; otherwise the highest priority wins, the oldest on a tie.
    const active =
      consumers.find((c) => c.active > 0) ??
      consumers
        .filter((c) => !c.stopped)
        .reduce<Consumer | undefined>(
          (best, c) => (!best || c.priority > best.priority ? c : best),
          undefined,
        );
    for (const consumer of consumers[0]?.single
      ? active
        ? [active]
        : []
      : consumers) {
      while (
        !consumer.stopped &&
        consumer.active < consumer.concurrency &&
        messages.length
      ) {
        const delivery = messages.shift()!;
        consumer.active++;
        const task = Promise.resolve()
          .then(() => consumer.run(delivery))
          .catch(async () => {
            messages.unshift(delivery);
            await new Promise((resolve) => setTimeout(resolve, 10));
          })
          .finally(() => {
            consumer.active--;
            consumer.tasks.delete(task);
            drain(queue);
          });
        consumer.tasks.add(task);
      }
    }
  };
  const push = (queue: string, delivery: Delivery) => {
    assertOpen();
    if (!broker.queues.has(queue)) broker.queues.set(queue, []);
    broker.queues.get(queue)!.push(delivery);
    drain(queue);
  };
  const consume = async (
    queue: string,
    run: Consumer["run"],
    concurrency = 1,
    single = false,
    priority = 0,
  ): Promise<Stop> => {
    assertOpen();
    if (!Number.isInteger(concurrency) || concurrency < 1)
      throw new Error("Invalid concurrency");
    const consumer: Consumer = {
      run,
      concurrency,
      active: 0,
      stopped: false,
      single,
      priority,
      tasks: new Set(),
    };
    if (!broker.consumers.has(queue)) broker.consumers.set(queue, []);
    broker.consumers.get(queue)!.push(consumer);
    const stop = async () => {
      consumer.stopped = true;
      // Stay registered until in-flight work ends, so the next single active consumer never overlaps it.
      await Promise.all(consumer.tasks);
      broker.consumers.set(
        queue,
        broker.consumers.get(queue)!.filter((c) => c !== consumer),
      );
      stops.delete(stop);
      drain(queue);
    };
    stops.add(stop);
    drain(queue);
    return stop;
  };
  return {
    async send(command, options) {
      const { deadline = 10000, lane } = options ?? {};
      if (closed) return { status: "unknown", commandId: command.id };
      return new Promise<Outcome>((resolve) => {
        const finish = (outcome: Outcome) => {
          clearTimeout(timer);
          pending.delete(cancel);
          resolve(structuredClone(outcome));
        };
        const cancel = () =>
          finish({ status: "unknown", commandId: command.id });
        const timer = setTimeout(cancel, deadline);
        pending.add(cancel);
        push(
          `commands.${command.stream.context}.${commandPartition(command)}${lane === "bulk" ? ".bulk" : ""}`,
          {
            message: structuredClone(command),
            reply: finish,
          },
        );
      });
    },
    async handle(context, partitions, handler) {
      for (const p of partitions)
        if (!Number.isInteger(p) || p < 0 || p >= 128)
          throw new Error("Invalid Command Partition");
      const handlers = await Promise.all(
        partitions
          .flatMap((p) => ["", ".bulk"].map((lane) => [p, lane] as const))
          .map(([p, lane]) => {
            return consume(
              `commands.${context}.${p}${lane}`,
              async (d) => {
                let outcome: Outcome;
                try {
                  outcome = await handler(d.message);
                } catch {
                  outcome = {
                    status: "failed",
                    error: { message: "Command handling failed" },
                  };
                }
                d.reply?.(outcome);
              },
              1,
              true,
              commandPriority(instance, context, p),
            );
          }),
      );
      return async () => {
        await Promise.all(handlers.map((stop) => stop()));
      };
    },
    async publish(topic, message) {
      assertOpen();
      await Promise.allSettled(
        [...(broker.subscribers.get(topic) ?? [])].map((handler) =>
          Promise.resolve().then(() => handler(structuredClone(message))),
        ),
      );
    },
    async subscribe(topic, handler) {
      assertOpen();
      if (!broker.subscribers.has(topic))
        broker.subscribers.set(topic, new Set());
      broker.subscribers.get(topic)!.add(handler);
      const stop = async () => {
        broker.subscribers.get(topic)?.delete(handler);
        stops.delete(stop);
      };
      stops.add(stop);
      return stop;
    },
    async enqueue(queue, message) {
      push(queue, { message: structuredClone(message) });
    },
    async consume(queue, handler, { concurrency = 1 } = {}) {
      return consume(queue, (d) => handler(d.message as Message), concurrency);
    },
    connected: () => !closed,
    async close() {
      if (closed) return;
      closed = true;
      for (const cancel of pending) cancel();
      await Promise.all([...stops].map((stop) => stop()));
    },
  };
}
