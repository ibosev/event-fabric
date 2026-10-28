import { expect, test } from "vitest";
import pino from "pino";
import { workflowContract } from "./contracts/workflows.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createApplication } from "../src/runtime/index.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import { workflowsApi } from "../src/internal.js";
import type { ApplicationConfig } from "../src/runtime/types.js";

workflowContract("memory", () => createInMemoryEventStore());

test("retire deletes a removed Workflow group's instances with its Checkpoints, as a Saga group's state", async () => {
  const store = createInMemoryEventStore();
  const config = (workflows: ApplicationConfig["workflows"]) =>
    createApplication({
      name: "retiring",
      store,
      aggregates: [],
      transport: createInMemoryTransport(),
      workflows,
      leaseDuration: 300,
      pollInterval: 20,
      logger: pino({ level: "silent" }),
    });
  const before = config({
    gone: { version: 1, signals: { go: {} }, run: (ctx) => ctx.waitFor({ kind: "signal", name: "go" }) },
  });
  const after = config({});
  await before.start();
  await after.start();
  try {
    await before[workflowsApi].start("gone", "g1");
    await expect.poll(() => store.loadWorkflow!("workflow:gone", "g1")).toMatchObject({ status: "running" });
    await before.stop();
    const state = async () => (await after.consumerStatus()).find((s) => s.group === "workflow:gone")?.state;
    await expect.poll(state, { timeout: 5000 }).toBe("undeclared");
    await after.retireConsumer("workflow:gone");
    expect(await state()).toBeUndefined();
    expect(await store.loadWorkflow!("workflow:gone", "g1")).toBeNull();
  } finally {
    await before.stop();
    await after.stop();
  }
});
