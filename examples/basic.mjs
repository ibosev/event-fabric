import assert from "node:assert/strict";
import { compile, only, createApplication } from "../dist/index.js";
import {
  createInMemoryEventStore,
  createInMemoryTransport,
  createMemoryReadmodelStore,
} from "../dist/testing.js";

const order = compile("sales", "order", {
  initialState: {},
  commands: {
    create: [
      only.ifNotExists(),
      (command, aggregate) => aggregate.apply.created(command.payload),
    ],
  },
  events: {
    created: (event, aggregate) => aggregate.set("title", event.payload.title),
  },
});
const application = createApplication({
  name: "example",
  aggregates: [order],
  store: createInMemoryEventStore(),
  transport: createInMemoryTransport(),
  pollInterval: 10,
  readmodels: {
    detail: {
      store: createMemoryReadmodelStore(),
      models: {
        order: {
          schema: {
            type: "object",
            properties: { title: { type: "string" } },
            required: ["title"],
          },
          reactions: {
            "sales.order.created": (event, vm) =>
              vm.set("title", event.payload.title),
          },
        },
      },
    },
  },
});
await application.start();
try {
  const outcome = await application.sendCommand({
    stream: { context: "sales", aggregate: "order", id: "one" },
    name: "create",
    payload: { title: "First order" },
  });
  assert.equal(outcome.status, "accepted");
  const read = await application.readmodels.detail.order("one", {
    minPosition: outcome.events.at(-1).position,
  });
  assert.equal(read.behind, false);
  assert.equal(read.document.title, "First order");
  process.stdout.write(`${JSON.stringify(read.document)}\n`);
} finally {
  await application.close();
}
