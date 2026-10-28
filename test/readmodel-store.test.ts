import { expect, test } from "vitest";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import { StaleEpochError } from "../src/errors.js";
import { definitionsByHash } from "../src/readmodel-store/durable.js";
const position = (n: number) => ({ transactionId: String(n), sequence: "1" });
// Store contracts shared by every adapter run in test/integration/readmodel-stores.test.ts.
test("in-memory writes serialize: a fence issued during a batch's validation applies after it", async () => {
  const store = createMemoryReadmodelStore();
  await store.register("g", {
    m: { schema: { type: "object", properties: { v: { type: "integer" } } } },
  });
  await store.fence("g", 1);
  const commit = store.batch("g", 1, position(1), [
    { model: "m", id: "x", document: { v: 1 } },
  ]);
  await store.fence("g", 2);
  await commit;
  expect(await store.checkpoint("g")).toEqual(position(1));
  await expect(
    store.batch("g", 1, position(2), [
      { model: "m", id: "x", document: { v: 2 } },
    ]),
  ).rejects.toBeInstanceOf(StaleEpochError);
  expect((await store.findOne("g", "m", { _id: "x" }))?.v).toBe(1);
});

test("stores decode each hash's definitions once and share them frozen", async () => {
  const withDefinitions = definitionsByHash();
  const definitions = { m: { schema: { type: "object" } } };
  let loads = 0;
  const load = async () => (
    loads++,
    { group: "g", id: "a", hash: "h", checkpoint: null, definitions }
  );
  const record = { group: "g", id: "a", hash: "h", checkpoint: null };
  const first = await withDefinitions(record, "definitions", load);
  const second = await withDefinitions(record, "definitions", load);
  expect(loads).toBe(1);
  expect(second).toEqual(first);
  expect(Object.isFrozen(second!.definitions.m!.schema)).toBe(true);
  // Partition records have no hash and no definitions.
  const partition = { name: "g#1", epoch: 0 } as any;
  expect(await withDefinitions(partition, "desired", load)).toBe(partition);
});
