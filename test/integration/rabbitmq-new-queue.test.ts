import { EventEmitter } from "node:events";
import { expect, test, vi } from "vitest";
import { createRabbitMQTransport } from "../../src/rabbitmq.js";
import { createCommand } from "../../src/id.js";

// The broker cannot be made to lose this race on demand, so the first consume of a Context's queues fails the
// way RabbitMQ 4.4 failed it: the broker closes the whole connection with `internal_error … noproc`.
const failures = { left: 0 };
vi.mock("amqplib", async (original) => {
  const amqp = await original<typeof import("amqplib")>();
  return {
    ...amqp,
    connect: async (...args: Parameters<typeof amqp.connect>) => {
      const connection = await amqp.connect(...args);
      const createChannel = connection.createChannel.bind(connection);
      connection.createChannel = async () => {
        const channel = await createChannel();
        const consume = channel.consume.bind(channel);
        channel.consume = (async (queue: string, ...rest: any[]) => {
          if (!queue.includes(".commands.") || failures.left <= 0)
            return consume(queue, ...(rest as [any]));
          failures.left--;
          const error = new Error(
            `Connection closed: 541 (INTERNAL-ERROR) with message "INTERNAL_ERROR - failed consuming from quorum queue '${queue}' in vhost '/': noproc"`,
          );
          (connection as unknown as EventEmitter).emit("error", error);
          await connection.close();
          throw error;
        }) as typeof channel.consume;
        return channel;
      };
      return connection;
    },
  };
});

const url = "amqp://127.0.0.1:55679";
const command = () =>
  createCommand({
    stream: { context: "sales", aggregate: "order", id: "o1" },
    name: "create",
    payload: {},
  });

test("a Context whose new quorum queue is not running yet retries on a new connection instead of losing the broker", async () => {
  const transport = await createRabbitMQTransport({
    application: `test-${crypto.randomUUID()}`,
    url,
  });
  try {
    failures.left = 2;
    const all = Array.from({ length: 128 }, (_, p) => p);
    const stop = await transport.handle("sales", all, async () => ({
      status: "accepted",
      events: [],
    }));
    expect(failures.left).toBe(0);
    expect(transport.connected()).toBe(true);
    expect(await transport.send(command())).toEqual({
      status: "accepted",
      events: [],
    });
    // Stopping closes the Context's own connection, which is not broker loss.
    await stop();
    expect(transport.connected()).toBe(true);
  } finally {
    await transport.close();
  }
});

test("a queue that stays unavailable fails `handle` after five attempts", async () => {
  const transport = await createRabbitMQTransport({
    application: `test-${crypto.randomUUID()}`,
    url,
  });
  try {
    failures.left = 5;
    await expect(
      transport.handle("sales", [0], async () => ({
        status: "accepted",
        events: [],
      })),
    ).rejects.toThrow(/noproc/);
    expect(failures.left).toBe(0);
  } finally {
    failures.left = 0;
    await transport.close();
  }
});
