import { expect, test } from "vitest";
import pino from "pino";
import { consumerStartContract } from "./contracts/consumer-start.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createApplication } from "../src/runtime/index.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import { workflow } from "../src/workflows/index.js";

consumerStartContract("memory", () => createInMemoryEventStore());

test("from is checked when the Application is created", () => {
  const make = (from: unknown, kind: "sagas" | "workflows" = "sagas") =>
    createApplication({
      name: "start",
      store: createInMemoryEventStore(),
      transport: createInMemoryTransport(),
      logger: pino({ level: "silent" }),
      [kind]: {
        s: kind === "sagas" ? { reactions: {}, from } : { version: 1, run: async () => {}, from },
      },
    } as never);
  expect(() => make("later")).toThrow('Saga s: from must be "beginning", "now", a Date or { position }');
  expect(() => make(new Date(Number.NaN))).toThrow("Saga s: from must be a valid Date");
  expect(() => make({ position: "not a token" })).toThrow("Saga s: from.position must be a Position token");
  expect(() => make(null, "workflows")).toThrow("Workflow s: from must be");
  expect(() => make({ position: "MTI6MQ" })).not.toThrow();
});

test("workflow(name, { from }) keeps from on the definition", () => {
  const since = new Date("2026-10-01T00:00:00Z");
  expect(workflow("w", { version: 1, from: since, run: async () => {} }).from).toBe(since);
});
