import { expect, test } from "vitest";
import { createApplication } from "../src/runtime/index.js";
import { compile } from "../src/authoring/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import { partitionFor } from "../src/transport/partition.js";
import { createScopedHandle } from "../src/gateway/access.js";
import type { Transport } from "../src/types.js";

const stream = (id: string) => ({ context: "sales", aggregate: "order", id });
// Order ids that share one Command Partition.
const samePartition = (count: number) => {
  const ids: string[] = [];
  for (let i = 0; ids.length < count; i++)
    if (partitionFor(stream(`o${i}`)) === partitionFor(stream("o0")))
      ids.push(`o${i}`);
  return ids;
};
const fixture = (transport: Transport = createInMemoryTransport()) => {
  let open!: () => void;
  const gate = new Promise<void>((resolve) => (open = resolve));
  const handled: string[] = [];
  const application = createApplication({
    name: "lanes",
    store: createInMemoryEventStore(),
    transport,
    pollInterval: 5,
    aggregates: [
      compile("sales", "order", {
        commands: {
          slow: async (command: any, state: any) => {
            await gate;
            handled.push(`slow ${command.aggregate.id}`);
            state.apply.touched({});
          },
          touch: (command: any, state: any) => {
            handled.push(`touch ${command.aggregate.id}`);
            state.apply.touched({});
          },
        },
        events: { touched: () => {} },
      }),
    ],
  });
  return { application, handled, open };
};
const pending = async (promise: Promise<unknown>) =>
  (await Promise.race([
    promise.then(() => false),
    new Promise((resolve) => setTimeout(resolve, 50, true)),
  ])) as boolean;

test("an interactive Command overtakes a bulk backlog in its Partition; one Stream's Commands take turns", async () => {
  const { application, handled, open } = fixture();
  const [a, b, c, d] = samePartition(4) as [string, string, string, string];
  await application.start();
  try {
    const bulk = [a, b, c].map((id) =>
      application.sendCommand(
        { stream: stream(id), name: "slow" },
        { lane: "bulk" },
      ),
    );
    // The bulk lane is held by `a` and has `b` and `c` waiting; the interactive lane is free.
    expect(
      (await application.sendCommand({ stream: stream(d), name: "touch" }))
        .status,
    ).toBe("accepted");
    expect(handled).toEqual([`touch ${d}`]);
    // `a`'s own Stream waits for its bulk Command in progress.
    const same = application.sendCommand({ stream: stream(a), name: "touch" });
    expect(await pending(same)).toBe(true);
    open();
    expect((await same).status).toBe("accepted");
    expect((await Promise.all(bulk)).map((outcome) => outcome.status)).toEqual(
      ["accepted", "accepted", "accepted"],
    );
    expect(handled.slice(0, 3)).toEqual([`touch ${d}`, `slow ${a}`, `touch ${a}`]);
    expect(handled.slice(3)).toEqual([`slow ${b}`, `slow ${c}`]);
  } finally {
    await application.stop();
  }
});

test("Gateway callers send interactive unless the Gateway says bulk; Sagas and Workers send bulk", async () => {
  const memory = createInMemoryTransport(),
    lanes: string[] = [];
  const transport: Transport = {
    ...memory,
    send: (command, options) => {
      lanes.push(`${command.name} ${options?.lane ?? "interactive"}`);
      return memory.send(command, options);
    },
  };
  const application = createApplication({
    name: "lanes",
    store: createInMemoryEventStore(),
    transport,
    pollInterval: 5,
    aggregates: [
      compile("sales", "order", {
        commands: {
          place: (_: any, state: any) => state.apply.placed({}),
          note: (_: any, state: any) => state.apply.noted({}),
          mail: (_: any, state: any) => state.apply.mailed({}),
        },
        events: { placed: () => {}, noted: () => {}, mailed: () => {} },
      }),
    ],
    sagas: {
      notice: {
        reactions: {
          "sales.order.placed": (event: any, _saga: any, app: any) => {
            app.getDomain().sales.order(event.aggregate.id).note({});
            app.queue("mail", { id: event.aggregate.id });
          },
        },
      },
    },
    workers: {
      mail: {
        handles: ["mail"],
        execute: async (message: any, ctx) => {
          await ctx.send({
            stream: stream(message.payload.id),
            name: "mail",
          });
        },
      },
    },
  });
  await application.start();
  try {
    const user = createScopedHandle({}, { port: 0, access: { domain: { "*": true } } }, application);
    const importer = createScopedHandle(
      {},
      { port: 0, access: { domain: { "*": true } }, lane: () => "bulk" },
      application,
    );
    expect((await user.sendCommand({ stream: stream("1"), name: "place" })).status).toBe("accepted");
    await expect.poll(() => lanes.length).toBe(3);
    expect((await importer.sendCommand({ stream: stream("2"), name: "note" })).status).toBe("accepted");
    expect(lanes.toSorted()).toEqual(
      ["place interactive", "note bulk", "mail bulk", "note bulk"].toSorted(),
    );
  } finally {
    await application.stop();
  }
});
