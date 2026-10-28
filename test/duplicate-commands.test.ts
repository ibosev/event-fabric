import { expect, test } from "vitest";
import { compile } from "../src/authoring/index.js";
import { createKernel } from "../src/kernel/index.js";
import { createCommand } from "../src/id.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createApplication } from "../src/runtime/index.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import { Rejection } from "../src/errors.js";

// Duplicate Commands (ADR-0014): a Command id is accepted at most once.
const setup = () => {
  const runs: string[] = [];
  const store = createInMemoryEventStore();
  const definition = compile("sales", "account", {
    commands: {
      add: (command: any, account: any) => {
        runs.push(command.id);
        if (command.payload.refuse)
          throw new Rejection("precondition", "Refused");
        account.apply.added(command.payload);
      },
    },
    events: { added: () => {} },
  });
  const kernel = createKernel({ definitions: [definition], store });
  return { runs, store, kernel };
};
const add = (payload: object, id = "cmd-1", streamId: string | undefined = "a") =>
  createCommand({
    id,
    stream: { context: "sales", aggregate: "account", id: streamId },
    name: "add",
    payload,
  });

test("an accepted Command sent twice runs once and replays its Events, marked duplicate", async () => {
  const { runs, kernel } = setup();
  const first = await kernel.handle(add({ amount: 1 }));
  const second = await kernel.handle(add({ amount: 1 }));
  expect(runs).toHaveLength(1);
  expect(first).toMatchObject({ status: "accepted" });
  expect(first).not.toHaveProperty("duplicate");
  expect(second).toMatchObject({ status: "accepted", duplicate: true });
  expect(second.status === "accepted" && second.events).toEqual(
    first.status === "accepted" && first.events,
  );
});

test("the same id with a different Command is rejected as a conflict", async () => {
  const { runs, kernel } = setup();
  await kernel.handle(add({ amount: 1 }));
  expect(await kernel.handle(add({ amount: 2 }))).toMatchObject({
    status: "rejected",
    error: { kind: "conflict" },
  });
  expect(runs).toHaveLength(1);
});

test("a rejected Command leaves no record, so a retry runs again", async () => {
  const { runs, kernel } = setup();
  expect(await kernel.handle(add({ refuse: true }))).toMatchObject({
    status: "rejected",
  });
  expect(await kernel.handle(add({ refuse: true }))).toMatchObject({
    status: "rejected",
  });
  expect(runs).toHaveLength(2);
});

test("concurrent sends of one id record the Events once and both return accepted", async () => {
  const { store, kernel } = setup();
  const outcomes = await Promise.all([
    kernel.handle(add({ amount: 1 })),
    kernel.handle(add({ amount: 1 })),
  ]);
  expect(outcomes.map((outcome) => outcome.status)).toEqual([
    "accepted",
    "accepted",
  ]);
  expect(
    await store.readStream({ context: "sales", aggregate: "account", id: "a" }),
  ).toHaveLength(1);
});

test("a retried creating Command replays the Aggregate it created", async () => {
  const { runs, kernel } = setup();
  const first = await kernel.handle(add({ amount: 1 }, "create-1", undefined));
  const second = await kernel.handle(add({ amount: 1 }, "create-1", undefined));
  expect(runs).toHaveLength(1);
  expect(second).toMatchObject({ status: "accepted", duplicate: true });
  const streamOf = (outcome: typeof first) =>
    outcome.status === "accepted" ? outcome.events[0]?.stream.id : undefined;
  expect(streamOf(second)).toBe(streamOf(first));
});

test("an Outbox re-send after an unknown outcome does not run the Command twice", async () => {
  const runs: string[] = [];
  const transport = createInMemoryTransport();
  const send = transport.send.bind(transport);
  let lost = true;
  // The first delivery runs, but its reply is lost: the relay sees `unknown` and re-sends.
  transport.send = async (command, options) => {
    const outcome = await send(command, options);
    if (command.name === "count" && lost) {
      lost = false;
      return { status: "unknown", commandId: command.id };
    }
    return outcome;
  };
  const store = createInMemoryEventStore();
  const application = createApplication({
    name: "resend",
    store,
    transport,
    pollInterval: 5,
    aggregates: [
      compile("sales", "account", {
        commands: {
          open: async (_c: any, account: any, app: any) => {
            account.apply.opened({});
            await app.tell().sales.account("b").count({});
          },
          count: (command: any, account: any) => {
            runs.push(command.id);
            account.apply.counted({});
          },
        },
        events: { opened: () => {}, counted: () => {} },
      }),
    ],
  });
  await application.start();
  try {
    await application.sendCommand({
      stream: { context: "sales", aggregate: "account", id: "a" },
      name: "open",
    });
    await expect.poll(() => lost, { timeout: 2000 }).toBe(false);
    // The relay retries an unknown outcome after a second; outboxStats counts delayed rows too.
    await expect
      .poll(async () => (await store.outboxStats()).pending, {
        timeout: 4000,
      })
      .toBe(0);
    expect(runs).toHaveLength(1);
  } finally {
    await application.close();
  }
});

test("the Stream read overlaps the Duplicate check, and a duplicate still runs no Step", async () => {
  const { runs, store, kernel: first } = setup();
  await first.handle(add({ amount: 1 }));
  const order: string[] = [];
  let release!: () => void;
  const checked = new Promise<void>((resolve) => (release = resolve));
  const watched = new Proxy(store, {
    get(target, key, receiver) {
      const value = Reflect.get(target, key, receiver);
      if (key === "acceptedCommand")
        return async (id: string) => {
          order.push("acceptedCommand");
          await checked;
          return value.call(target, id);
        };
      if (key === "readStream")
        return (...args: unknown[]) => {
          order.push("readStream");
          return value.apply(target, args);
        };
      return value;
    },
  });
  const definition = compile("sales", "account", {
    commands: {
      add: (command: any, account: any) => {
        runs.push(command.id);
        account.apply.added(command.payload);
      },
    },
    events: { added: () => {} },
  });
  const kernel = createKernel({ definitions: [definition], store: watched });
  const fresh = kernel.handle(add({ amount: 2 }, "cmd-2"));
  const repeated = kernel.handle(add({ amount: 1 }, "cmd-1"));
  await new Promise((resolve) => setTimeout(resolve, 10));
  // Both reads started before either Duplicate check answered.
  expect(order.filter((call) => call === "readStream")).toHaveLength(2);
  release();
  expect(await fresh).toMatchObject({ status: "accepted" });
  expect(await repeated).toMatchObject({ status: "accepted", duplicate: true });
  expect(runs).toEqual(["cmd-1", "cmd-2"]);
});
