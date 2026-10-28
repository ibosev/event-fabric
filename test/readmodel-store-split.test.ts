import pino from "pino";
import { expect, test } from "vitest";
import { createApplication } from "../src/runtime/index.js";
import { compile } from "../src/authoring/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import {
  createInMemoryBroker,
  createInMemoryTransport,
} from "../src/transport/memory.js";
import type { ReadmodelStore } from "../src/readmodel-store/types.js";

test("an instance on another Readmodel store than the other instances takes none of the group's Leases", async () => {
  const store = createInMemoryEventStore(),
    broker = createInMemoryBroker(),
    shared = createMemoryReadmodelStore(),
    other = createMemoryReadmodelStore();
  const errors: string[] = [];
  const make = (models: ReadmodelStore, lines?: string[]) =>
    createApplication({
      name: "split",
      store,
      transport: createInMemoryTransport({ broker }),
      pollInterval: 5,
      leaseDuration: 600,
      logger: lines
        ? pino(
            { level: "error" },
            { write: (line: string) => lines.push(line) },
          )
        : pino({ level: "silent" }),
      aggregates: [
        compile("c", "a", { commands: {}, events: { added: () => {} } }),
      ],
      readmodels: {
        g: {
          store: models,
          models: {
            item: {
              schema: {
                type: "object",
                properties: { count: { type: "integer" } },
              },
              reactions: {
                "c.a.added": (_e: any, vm: any) =>
                  vm.set("count", (vm.get("count") ?? 0) + 1),
              },
            },
          },
        },
      },
    });
  const apps = [make(shared), make(other, errors)];
  const append = async (revision: number) =>
    (
      await store.appendToStream(
        { context: "c", aggregate: "a", id: "one" },
        revision,
        [
          {
            id: crypto.randomUUID(),
            name: "added",
            payload: {},
            metadata: {},
            schemaVersion: 1,
            timestamp: new Date().toISOString(),
            commandId: crypto.randomUUID(),
            correlationId: crypto.randomUUID(),
          },
        ],
      )
    )[0]!;
  const split = async () =>
    (await apps[0]!.consumerStatus()).find(
      (row) => row.group === "readmodels:g",
    )?.error;
  try {
    // One instance per store is a tie: neither takes the group.
    await Promise.all(apps.map((app) => app.start()));
    await expect
      .poll(split, { timeout: 10000 })
      .toMatch(
        /^Instances use different Readmodel stores: memory \(\w{8}\): \S+; memory \(\w{8}\): \S+$/,
      );
    // An instance that took the Lease before the other one advertised lets go at its next renewal.
    await new Promise((resolve) => setTimeout(resolve, 600));
    for (let revision = 0; revision < 10; revision++) await append(revision);
    await new Promise((resolve) => setTimeout(resolve, 1500));
    expect(await shared.checkpoint("g")).toBeNull();
    expect(await other.checkpoint("g")).toBeNull();
    expect(errors.join("")).toContain(
      "Instances run this Readmodel group on different stores",
    );
    // A second instance on the shared store outvotes the other one, which still takes nothing.
    apps.push(make(shared));
    await apps[2]!.start();
    let head;
    for (let revision = 10; revision < 20; revision++)
      head = await append(revision);
    await expect
      .poll(
        async () => (await shared.findOne("g", "item", { _id: "one" }))?.count,
        { timeout: 10000 },
      )
      .toBe(20);
    expect(await shared.checkpoint("g")).toEqual(head!.position);
    expect(await other.checkpoint("g")).toBeNull();
    expect(await split()).toMatch(/: \S+, \S+; /);
  } finally {
    await Promise.all(apps.map((app) => app.stop()));
  }
});
