import { expect, test, vi } from "vitest";
import { createApplication } from "../src/runtime/index.js";
import { compile } from "../src/authoring/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import {
  commandPartition,
  commandPriority,
  partitionFor,
  fnv1a,
} from "../src/transport/partition.js";
import type { Command } from "../src/types.js";
const command = (id: string): Command => ({
  id,
  stream: { context: "sales", aggregate: "order", id: "1" },
  name: "create",
  payload: {},
  metadata: {},
  schemaVersion: 1,
  correlationId: id,
  timestamp: "2026-01-01T00:00:00Z",
});
test("FNV-1a matches published vectors over UTF-8", () => {
  expect(["", "a", "foobar"].map(fnv1a)).toEqual([
    2166136261, 3826002220, 3214735720,
  ]);
  expect(partitionFor({ context: "sales", aggregate: "order", id: "1" })).toBe(
    fnv1a("sales.order.1") % 128,
  );
});
test("FNV-1a hashes multi-byte UTF-8 keys byte by byte", () => {
  // Expected values computed independently from the UTF-8 bytes (Python).
  expect(fnv1a("Zürich")).toBe(3607133984);
  expect(
    partitionFor({ context: "sales", aggregate: "order", id: "Zürich" }),
  ).toBe(100);
  expect(fnv1a("sales.order.🚀")).toBe(248113894);
  expect(partitionFor({ context: "sales", aggregate: "order", id: "🚀" })).toBe(
    102,
  );
});
test("the sender's Command deadline defaults to 10 s", async () => {
  vi.useFakeTimers();
  try {
    const transport = createInMemoryTransport();
    let outcome: unknown;
    void transport.send(command("slow")).then((o) => (outcome = o));
    await vi.advanceTimersByTimeAsync(9999);
    expect(outcome).toBeUndefined();
    await vi.advanceTimersByTimeAsync(1);
    expect(outcome).toEqual({ status: "unknown", commandId: "slow" });
    await transport.close();
  } finally {
    vi.useRealTimers();
  }
});
test("the Command handler limit defaults to 30 s", async () => {
  const app = createApplication({
    name: "handler-limit",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    aggregates: [
      compile("c", "a", {
        commands: { hang: () => new Promise<void>(() => {}) },
        events: {},
      }),
    ],
  });
  await app.start({ only: ["commands:*"] });
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  try {
    let outcome: any;
    void app
      .sendCommand(
        { stream: { context: "c", aggregate: "a", id: "x" }, name: "hang" },
        { deadline: 60000 },
      )
      .then((o) => (outcome = o));
    await vi.advanceTimersByTimeAsync(29999);
    expect(outcome).toBeUndefined();
    await vi.advanceTimersByTimeAsync(1);
    expect(outcome).toMatchObject({
      status: "failed",
      error: { code: "timeout" },
    });
  } finally {
    vi.useRealTimers();
    await app.close();
  }
});
test("Command priorities spread Partitions fairly and move only a leaver's share", () => {
  const winner = (instances: string[], p: number) =>
    instances.reduce((best, i) =>
      commandPriority(i, "sales", p) > commandPriority(best, "sales", p)
        ? i
        : best,
    );
  const three = ["replica-a", "replica-b", "replica-c"];
  const owners = Array.from({ length: 128 }, (_, p) => winner(three, p));
  for (const instance of three) {
    const share = owners.filter((o) => o === instance).length;
    expect(share).toBeGreaterThan(25);
    expect(share).toBeLessThan(60);
  }
  // When replica-c leaves, only its Partitions change owner.
  const two = ["replica-a", "replica-b"];
  owners.forEach((owner, p) => {
    if (owner !== "replica-c") expect(winner(two, p)).toBe(owner);
  });
});

test("creation Commands without a Stream id spread by Command id, and a resend keeps its Partition", () => {
  const creation = (id: string): Command => ({
    ...command(id),
    stream: { context: "sales", aggregate: "order", id: "" },
  });
  const partitions = new Set(
    Array.from({ length: 100 }, (_, i) => commandPartition(creation(`c${i}`))),
  );
  expect(partitions.size).toBeGreaterThan(50);
  expect(commandPartition(creation("same"))).toBe(
    commandPartition(creation("same")),
  );
  expect(commandPartition(command("x"))).toBe(
    partitionFor(command("x").stream),
  );
});
