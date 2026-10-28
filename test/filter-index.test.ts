import { expect, test } from "vitest";
import { exactFilter, matches } from "../src/readmodel-store/query.js";
import {
  candidates,
  clauseValues,
  indexDocument,
  indexableClauses,
  pathIndex,
} from "../src/runtime/filter-index.js";

// A small deterministic generator, so a failure reproduces.
let seed = 7;
const random = () => (seed = (seed * 1103515245 + 12345) % 2 ** 31) / 2 ** 31;
const pick = <T>(items: T[]): T => items[Math.floor(random() * items.length)]!;
const epoch = new Date(0),
  day = new Date("2026-10-21T00:00:00.000Z");
const scalars = [
  "a",
  "b",
  "",
  "2026-10-21T00:00:00.000Z",
  0,
  1,
  86400000,
  true,
  false,
  null,
  epoch,
  day,
];
const value = (depth = 0): unknown => {
  const kind = random();
  if (kind < 0.5 || depth > 1) return pick(scalars);
  if (kind < 0.7)
    return Array.from({ length: Math.floor(random() * 3) }, () =>
      value(depth + 1),
    );
  if (kind < 0.85) return { x: value(depth + 1) };
  return [{ x: value(depth + 1) }, { y: 1 }];
};
const document = (): Record<string, unknown> => {
  const result: Record<string, unknown> = {};
  if (random() < 0.85) result.a = value();
  if (random() < 0.85) result.o = random() < 0.2 ? null : value(1);
  return result;
};
const conditions = (): unknown[] => [
  ...scalars,
  undefined,
  ...scalars.map((v) => ({ eq: v })),
  { $eq: "a" },
  { in: ["a", 1, null] },
  { $in: [epoch, "b"] },
  { in: [] },
  { in: [day, undefined] },
];

test("an index's candidates include every document the matcher accepts, through re-keying and removal", () => {
  for (const path of ["a", "o.x", "o"]) {
    const index = pathIndex(path);
    const documents = new Map<string, Record<string, unknown>>();
    const check = () => {
      for (const condition of conditions()) {
        const values = clauseValues(condition);
        if (!values) continue;
        const found = candidates(index, values);
        if (!found) continue;
        for (const [id, doc] of documents)
          if (matches(doc as any, { [path]: condition } as any))
            expect(found.has(id), `${id} ${path} ${String(condition)}`).toBe(
              true,
            );
      }
    };
    for (let i = 0; i < 400; i++) {
      const id = `d${i}`,
        doc = document();
      documents.set(id, doc);
      indexDocument(index, id, doc);
    }
    check();
    // Re-key half of them with new values, and remove a quarter.
    for (let i = 0; i < 400; i += 2) {
      const id = `d${i}`;
      if (i % 4) {
        const doc = document();
        documents.set(id, doc);
        indexDocument(index, id, doc);
      } else {
        documents.delete(id);
        indexDocument(index, id, null);
      }
    }
    check();
    // Nothing of a removed document is left behind.
    for (const ids of [...index.values.values(), ...index.dates.values()])
      for (const id of ids) expect(documents.has(id)).toBe(true);
    expect([...index.keysOf.keys()].every((id) => documents.has(id))).toBe(
      true,
    );
  }
});

test("clauses that an index cannot narrow exactly are not indexed", () => {
  expect(clauseValues({ ne: "a" })).toBeUndefined();
  expect(clauseValues({ $nin: ["a"] })).toBeUndefined();
  expect(clauseValues({ eq: "a", ne: "b" })).toBeUndefined();
  expect(clauseValues({ x: 1 })).toBeUndefined();
  expect(clauseValues(["a"])).toBeUndefined();
  expect(clauseValues({ eq: ["a"] })).toBeUndefined();
  expect(clauseValues({ in: [{ x: 1 }] })).toBeUndefined();
  expect(clauseValues({ gt: 1 })).toBeUndefined();
  expect(clauseValues({ eq: 1, gt: 0 })).toEqual([1]);
  expect(
    indexableClauses({
      a: "x",
      oneOf: [{ b: 1 }],
      $and: [{ c: 1 }],
      d: { ne: 1 },
      e: { in: [1, 2] },
    } as any),
  ).toEqual([
    { path: "a", values: ["x"] },
    { path: "e", values: [1, 2] },
  ]);
  // A Date condition cannot use an index that holds strings or numbers a Date could equal.
  const index = pathIndex("a");
  indexDocument(index, "s", { a: "2026-10-21T00:00:00.000Z" });
  expect(candidates(index, [day])).toBeUndefined();
  expect([...candidates(index, ["2026-10-21T00:00:00.000Z"])!]).toEqual(["s"]);
});

test("an index keys array elements and narrows to the documents that hold a value", () => {
  const index = pathIndex("o.x");
  indexDocument(index, "array", { o: [{ x: ["p", "q"] }, { x: "r" }] });
  indexDocument(index, "nested", { o: { x: [["p"]] } });
  indexDocument(index, "other", { o: { x: "s" } });
  indexDocument(index, "absent", { o: null });
  expect([...candidates(index, ["q"])!]).toEqual(["nested", "array"]);
  expect([...index.always]).toEqual(["nested"]);
  expect([...candidates(index, [undefined])!]).toEqual(["nested", "absent"]);
  expect([...candidates(index, ["r", "s"])!].sort()).toEqual([
    "array",
    "nested",
    "other",
  ]);
});
test("exactFilter accepts json-field equality only for a store that matches arrays natively, and never an empty condition", () => {
  const schema = {
    type: "object",
    properties: { n: { type: "integer" }, owner: { type: "object", properties: { account: { "x-store": "json" } } } },
  };
  expect(exactFilter({ "owner.account": { eq: "x" } }, schema)).toBe(false);
  expect(exactFilter({ "owner.account": { eq: "x" } }, schema, { jsonEquality: true })).toBe(true);
  expect(exactFilter({ "owner.account": { in: ["x", "y"] } }, schema, { jsonEquality: true })).toBe(true);
  for (const condition of [{ ne: "x" }, { eq: 1 }, { eq: "2026-01-02" }, { gt: "x" }])
    expect(exactFilter({ "owner.account": condition }, schema, { jsonEquality: true })).toBe(false);
  expect(exactFilter({ n: {} }, schema)).toBe(false);
  expect(exactFilter({ n: { eq: 1 } }, schema)).toBe(true);
});
