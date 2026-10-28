import { expect, test, vi } from "vitest";
import { compile } from "../src/authoring/index.js";
import { createApplication } from "../src/runtime/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import { createInMemoryTransport } from "../src/transport/memory.js";

// Turns the staged-change index off, so query Identities check every staged change one by one.
const toggle = vi.hoisted(() => ({ indexed: true }));
vi.mock("../src/runtime/filter-index.js", async (original) => {
  const actual =
    await original<typeof import("../src/runtime/filter-index.js")>();
  return {
    ...actual,
    memoryIndexes: (document: Parameters<typeof actual.memoryIndexes>[0]) => {
      const indexes = actual.memoryIndexes(document);
      return {
        touch: indexes.touch,
        candidates: (model: string, filter: any) =>
          toggle.indexed ? indexes.candidates(model, filter) : undefined,
      };
    },
  };
});

const aggregates = [
  compile("sales", "order", {
    commands: {},
    events: { opened: () => {} },
  }),
];
const stream = { context: "sales", aggregate: "order", id: "order-one" };

test("query Identities select the same staged documents, in the same order, through the staged-change index", async () => {
  let seed = 11;
  const random = () => (seed = (seed * 1103515245 + 12345) % 2 ** 31) / 2 ** 31;
  const tags = ["a", "b", "c", "d"];
  const pick = <T>(items: T[]) => items[Math.floor(random() * items.length)]!;
  const identity = () => {
    const kind = random();
    if (kind < 0.45) return `d${Math.floor(random() * 120)}`;
    if (kind < 0.7) return { tag: pick(tags) };
    if (kind < 0.85) return { tag: { in: [pick(tags), pick(tags)] } };
    if (kind < 0.93) return { tag: pick(tags), n: { gte: 3 } };
    return { n: { gte: 4 } };
  };
  const store = createInMemoryEventStore();
  await store.appendToStream(
    stream,
    0,
    Array.from({ length: 8000 }, (_, i) => ({
      id: `query-${i}`,
      name: "opened",
      payload: {
        identity: identity(),
        tag: pick(tags),
        destroy: random() < 0.05,
      },
      metadata: {},
      schemaVersion: 1,
      timestamp: "2026-10-21T00:00:00.000Z",
      commandId: `query-${i}`,
      correlationId: `query-${i}`,
    })),
  );
  const rebuild = async (indexed: boolean) => {
    toggle.indexed = indexed;
    let counter = 0;
    const models = createMemoryReadmodelStore();
    const batch = models.batch.bind(models);
    // Slow commits, so folds also see the previous batch's carried changes.
    models.batch = async (...args) => {
      await new Promise((resolve) => setTimeout(resolve, 10));
      return batch(...args);
    };
    const app = createApplication({
      name: "queries",
      store,
      transport: createInMemoryTransport(),
      aggregates,
      readmodels: {
        detail: {
          store: models,
          models: {
            entity: {
              schema: {
                type: "object",
                properties: {
                  tag: { type: ["string", "null"] },
                  n: { type: "integer" },
                  last: { type: "integer" },
                },
              },
              initialState: { tag: null, n: 0, last: 0 },
              identity: {
                "sales.order.opened": (event) => event.payload.identity,
              },
              reactions: {
                "sales.order.opened": (event, vm) => {
                  if (event.payload.destroy) return vm.destroy();
                  // `last` records the order in which Reactions ran across documents.
                  vm.set("last", ++counter);
                  vm.set("tag", event.payload.tag);
                  vm.set("n", (vm.get("n") + 1) % 6);
                },
              },
            },
          },
        },
      },
    });
    try {
      await app.rebuild("detail");
      return await models.find("detail", "entity", {}, { sort: { _id: 1 } });
    } finally {
      await app.close();
    }
  };
  const scanned = await rebuild(false),
    indexed = await rebuild(true);
  expect(scanned.length).toBeGreaterThan(50);
  expect(indexed).toEqual(scanned);
}, 120000);
