import { expect, test, vi } from "vitest";
import {
  legacyListQuery,
  legacyMetadata,
  legacyOne,
  legacyQuery,
  legacyStream,
} from "../src/compat.js";
import { queryDocuments } from "../src/readmodel-store/query.js";

test("legacyMetadata deep-merges Command metadata into the Event's, as lodash.merge did", () => {
  const event = {
    tenant: { id: "e", region: "eu" },
    tags: ["a", "b"],
    keep: 1,
  };
  const command = {
    tenant: { id: "c" },
    tags: ["z"],
    skipped: undefined,
    owner: { id: "o" },
  };
  const merged = legacyMetadata(event, command);
  expect(merged).toEqual({
    tenant: { id: "c", region: "eu" },
    tags: ["z", "b"],
    keep: 1,
    owner: { id: "o" },
  });
  // The old merge changed the Event's object in place; the Command's values are copied, not shared.
  expect(merged).toBe(event);
  expect((merged as any).owner).not.toBe(command.owner);
  expect(legacyMetadata(undefined, { a: 1 })).toEqual({ a: 1 });
  const hostile = JSON.parse('{"__proto__":{"polluted":true},"constructor":{"x":1}}');
  expect(legacyMetadata({}, hostile)).toEqual({});
  expect(({} as any).polluted).toBeUndefined();
});

test("legacyMetadata copies only the objects it takes from the Command", () => {
  const clones = vi.spyOn(globalThis, "structuredClone");
  try {
    const merged = legacyMetadata({ keep: 1 }, { userId: "u", tenant: "t", at: 3, owner: { id: "o" } });
    expect(merged).toEqual({ keep: 1, userId: "u", tenant: "t", at: 3, owner: { id: "o" } });
    // The nested object is copied; strings and numbers are values.
    expect(clones).toHaveBeenCalledTimes(1);
  } finally {
    clones.mockRestore();
  }
});

const documents = [
  { _id: "1", status: "open", total: 5, created: "2026-01-03" },
  { _id: "2", status: "paid", total: 7, created: "2026-01-01" },
  { _id: "3", status: "void", total: 9, created: "2026-01-02" },
];
const run = (query: Parameters<typeof legacyQuery>[0]) =>
  queryDocuments(documents, ...legacyQuery(query)).map((d) => d._id);

test("legacyQuery keeps the old filter meanings on the portable evaluator", () => {
  expect(run({ filter: { status: ["open", "paid"] } })).toEqual(["1", "2"]);
  expect(run({ filter: { status: ["paid"] } })).toEqual(["2"]);
  expect(run({ filter: { status: { ne: ["open", "paid"] } } })).toEqual(["3"]);
  expect(run({ filter: { status: { ne: ["open"] } } })).toEqual(["2", "3"]);
  expect(
    run({ filter: { $oneOf: [{ status: "void" }, { total: { lt: 6 } }] } }),
  ).toEqual(["1", "3"]);
  expect(run({ filter: { id: "2" } })).toEqual(["2"]);
  // An absent value, as in `.filter({ id: req.query.ids })`, sets no condition.
  expect(run({ filter: { id: undefined } })).toHaveLength(3);
  expect(legacyQuery({ filter: { id: undefined } })[0]).toEqual({});
});

test("legacyQuery translates sort records, the default size of 100 and size 0 as unlimited", () => {
  expect(run({ sort: [{ created: "desc" }] })).toEqual(["1", "3", "2"]);
  expect(run({ sort: { total: "asc" }, from: 1, size: 1 })).toEqual(["2"]);
  expect(legacyQuery({})[1]).toEqual({ size: 100 });
  expect(legacyQuery({ size: 0 })[1]).toEqual({});
  expect(legacyQuery({ q: "x" })[1]).toEqual({ size: 100, q: "x" });
  expect(() => legacyQuery({ sort: { total: "up" } })).toThrow(
    'Unknown old sort direction "up"',
  );
});

test("legacyOne resolves to the old array and legacyStream to a Node object stream", async () => {
  expect(await legacyOne(Promise.resolve({ document: { _id: "1" } }))).toEqual([
    { _id: "1" },
  ]);
  expect(await legacyOne(Promise.resolve({ document: null }))).toEqual([]);
  async function* rows() {
    yield { _id: "1" };
    yield { _id: "2" };
  }
  const seen: unknown[] = [];
  await new Promise((resolve, reject) =>
    legacyStream(rows())
      .on("data", (row) => seen.push(row))
      .on("end", resolve)
      .on("error", reject),
  );
  expect(seen).toEqual([{ _id: "1" }, { _id: "2" }]);
});

test("legacyListQuery keeps the old list query-string meanings", () => {
  expect(legacyListQuery({})).toEqual([{}, { size: 100 }]);
  // Reserved keys, comma membership with empty tokens kept, a `filter` key is a field.
  expect(
    legacyListQuery({
      from: "1.9",
      size: "12rows",
      sort: "-rank, name,+rank",
      q: "acme",
      aggs: "x",
      tag: "a, b, ,c",
      filter: "f",
      id: "7",
    }),
  ).toEqual([
    { tag: { in: ["a", "b", "", "c"] }, filter: "f", _id: "7" },
    { sort: { rank: 1, name: 1 }, from: 1, size: 12, q: "acme" },
  ]);
  // size 0 is unlimited, never an empty page; arrays take the first number.
  expect(legacyListQuery({ size: "0", from: ["2", "6"] })).toEqual([
    {},
    { from: 2 },
  ]);
  // Range suffixes on the last segment; values stay strings; `.ne` is not a suffix.
  expect(
    legacyListQuery({
      "profile.age.gte": "18",
      "profile.age.lt": "65",
      "a.ne": "x",
    }),
  ).toEqual([
    { "profile.age": { gte: "18", lt: "65" }, "a.ne": "x" },
    { size: 100 },
  ]);
  // Key order decides between membership and a range on one field, as in the old parser.
  expect(legacyListQuery({ amount: "a,b", "amount.gt": "5" })[0]).toEqual({
    amount: { gt: "5" },
  });
  expect(legacyListQuery({ "amount.gt": "5", amount: "a,b" })[0]).toEqual({
    amount: { in: ["a", "b"] },
  });
  // Bracketed forms: ne arrays, a range drops ne, empty membership drops the field, $oneOf, null, objects.
  expect(
    legacyListQuery({
      tag: { ne: ["a", "b"] },
      kind: { ne: ["k"] },
      level: { ne: "a", gt: 5 },
      none: [],
      $oneOf: [{ status: "open" }, { status: "late" }],
      gone: null,
      odd: { $exists: true },
    })[0],
  ).toEqual({
    tag: { nin: ["a", "b"] },
    kind: { ne: "k" },
    level: { gt: 5 },
    oneOf: [{ status: "open" }, { status: "late" }],
    gone: null,
    odd: { eq: { $exists: true } },
  });
  for (const input of [
    { size: "" },
    { from: null },
    { size: [] },
    { from: "x1" },
    { size: "-1" },
    { sort: [{ a: 1 }] },
    { q: ["a"] },
  ])
    expect(() => legacyListQuery(input), JSON.stringify(input)).toThrow(
      "Bad Request",
    );
  // A sort record was ignored by the old parser.
  expect(legacyListQuery({ sort: { a: 1 } })[1]).toEqual({ size: 100 });
});
test("an Application that replaces Date.parse, as the old datejs package did, leaves Commands and Leases working", async () => {
  const { createCommand } = await import("../src/id.js");
  const { createInMemoryEventStore } =
    await import("../src/event-store/memory.js");
  const parse = Date.parse;
  // datejs's parse returns a Date, or null for text it does not know.
  Date.parse = ((text: string) =>
    /^\d{4}-/.test(text) ? null : new Date(text)) as any;
  try {
    const command = createCommand({
      stream: { context: "sales", aggregate: "order", id: "1" },
      name: "place",
      timestamp: "2026-01-02T03:04:05.678Z",
    });
    expect(command.timestamp).toBe("2026-01-02T03:04:05.678Z");
    const store = createInMemoryEventStore();
    expect(await store.acquireLease("saga:notice", "me", 60000)).toBeTruthy();
    expect(await store.acquireLease("saga:notice", "other", 60000)).toBeNull();
  } finally {
    Date.parse = parse;
  }
});
