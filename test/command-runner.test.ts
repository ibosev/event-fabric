import { expect, test } from "vitest";
import { compile, only } from "../src/authoring/index.js";
import { createCommand } from "../src/id.js";
import { createApplication } from "../src/runtime/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import { Rejection } from "../src/errors.js";
test("domain Command runners await only their own Events and historical loads fold stored Events", async () => {
  const order = compile("sales", "order", {
    initialState: {},
    commands: {
      create: [
        only.ifNotExists(),
        (_command, state) => state.apply.created({ title: "first" }),
      ],
      inspect: () => {},
    },
    events: {
      created: (event, state) => state.set("title", event.payload.title),
    },
  });
  const application = createApplication({
    name: "runner",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [order],
    pollInterval: 5,
  });
  await application.start();
  try {
    const event = await application
      .getDomain()
      .sales.order("one")
      .create()
      .await("sales.order.created");
    expect(event.revision).toBe(1);
    // A list waits for the first Event matching any of its patterns.
    const either = await application
      .getDomain()
      .sales.order("two")
      .create()
      .await(["sales.order.renamed", "sales.order.created"]);
    expect(either.name).toBe("created");
    const loaded = await application
      .getDomain()
      .sales.order("one")
      .load({ atRevision: 1 });
    expect(loaded.get("title")).toBe("first");
    await expect(
      application
        .getDomain()
        .sales.order("one")
        .inspect()
        .await("sales.order.created"),
    ).rejects.toThrow("accepted without awaited Event");
    // A rejected Command throws its Rejection, so a Gateway answers it as one.
    const rejection = await application
      .getDomain()
      .sales.order("one")
      .create()
      .await("sales.order.created")
      .catch((error) => error);
    expect(rejection).toBeInstanceOf(Rejection);
    expect(rejection.message).toBe("Aggregate already exists");
  } finally {
    await application.close();
  }
});
test("Command envelopes preserve an explicit null payload and reject malformed addresses", () => {
  expect(
    createCommand({
      stream: { context: "sales", aggregate: "order", id: "" },
      name: "create",
      payload: null,
    }).payload,
  ).toBeNull();
  expect(() =>
    createCommand({
      stream: { context: "sales.other", aggregate: "order", id: "one" },
      name: "create",
    }),
  ).toThrow("address");
});

test("sendCommand generates an Aggregate id when the address omits it", async () => {
  const store = createInMemoryEventStore();
  const app = createApplication({
    name: "generated-id",
    store,
    transport: createInMemoryTransport(),
    aggregates: [
      compile("c", "a", {
        idGenerator: () => "generated",
        commands: { add: (_: any, state: any) => state.apply.added({}) },
        events: { added: () => {} },
      }),
    ],
  });
  await app.start({ only: ["commands:*"] });
  try {
    const outcome = await app.sendCommand({
      stream: { context: "c", aggregate: "a" },
      name: "add",
    });
    expect(outcome).toMatchObject({
      status: "accepted",
      events: [{ stream: { id: "generated" } }],
    });
    expect(
      await store.readStream({ context: "c", aggregate: "a", id: "generated" }),
    ).toHaveLength(1);
  } finally {
    await app.close();
  }
});
