import { expect, test } from "vitest";
import type {
  PendingEvent,
  Persistence,
  RecordedEvent,
} from "../../src/types.js";

export function eventPageContract(
  adapter: string,
  open: () => Promise<Persistence>,
) {
  test(`${adapter}: head and filtered progress order multi-digit Positions numerically`, async () => {
    const store = await open();
    const events = await store.appendToStream(
      { context: "pages", aggregate: "record", id: "many" },
      0,
      Array.from({ length: 12 }, (_, index) => ({
        id: `many-${index}`,
        name: "changed",
        payload: {},
        metadata: {},
        schemaVersion: 1,
        timestamp: "2026-10-17T00:00:00.000Z",
        commandId: "many",
        correlationId: "many",
      })),
    );
    expect(await store.head()).toEqual(events[11]!.position);
    const page = await store.readPage!({
      eventNames: ["pages.record.changed"],
      fromPosition: events[8]!.position,
    });
    expect(page.events.map((event) => event.id)).toEqual([
      "many-9",
      "many-10",
      "many-11",
    ]);
    expect(page.through).toEqual(events[11]!.position);
    const positions = structuredClone(events.map((event) => event.position));
    for (const limit of [1, 100]) {
      const returned = await store.readPage!({ limit });
      returned.through!.sequence = "999999";
      returned.events[0]!.position.transactionId = "999999";
    }
    expect((await store.readAll()).map((event) => event.position)).toEqual(
      positions,
    );
  });
  test(`${adapter}: empty pages and invalid bounds never invent progress`, async () => {
    const store = await open();
    expect(await store.readPage!()).toEqual({
      events: [],
      through: null,
      complete: true,
    });
    for (const limit of [0, -1, 1.5, Infinity, NaN])
      await expect(store.readPage!({ limit })).rejects.toThrow("limit");
    await expect(
      store.readPage!({ eventNames: ["unqualified"] }),
    ).rejects.toThrow("qualified");
    await expect(
      store.readPage!({ fromPosition: { transactionId: "-1", sequence: "0" } }),
    ).rejects.toThrow("Position");
    await expect(
      store.readPage!({
        fromPosition: { transactionId: "3", sequence: "0" },
        toPosition: { transactionId: "2", sequence: "10" },
      }),
    ).rejects.toThrow("fromPosition");
  });
  test(`${adapter}: a matching append during Upcasting remains after the page's complete Position`, async () => {
    const store = await open();
    const stream = { context: "race", aggregate: "record", id: "one" };
    const pending: PendingEvent = {
      id: "before",
      name: "changed",
      payload: { append: true },
      metadata: {},
      schemaVersion: 1,
      timestamp: "2026-10-17T00:00:00.000Z",
      commandId: "before",
      correlationId: "before",
    };
    const [first] = await store.appendToStream(stream, 0, [pending]);
    let late: RecordedEvent | undefined;
    store.registerUpcasters({
      "race.record.changed": {
        version: 2,
        upcasters: {
          1: async (payload) => {
            if (payload.append)
              [late] = await store.appendToStream(stream, 1, [
                { ...pending, id: "after", payload: {} },
              ]);
            return {};
          },
        },
      },
    });
    const page = await store.readPage!({ eventNames: ["race.record.changed"] });
    expect(page.events.map((event) => event.id)).toEqual(["before"]);
    expect(page.through).toEqual(first!.position);
    expect(await store.head()).toEqual(late!.position);
    const next = await store.readPage!({ fromPosition: page.through });
    expect(next.events.map((event) => event.id)).toEqual(["after"]);
    expect(next.through).toEqual(late!.position);
  });
  test(`${adapter}: a page upcasts only returned Events, not excluded Events or the lookahead`, async () => {
    const store = await open();
    const stream = { context: "pages", aggregate: "record", id: "one" };
    const event = (id: string, name: string, fail = false): PendingEvent => ({
      id,
      name,
      payload: { fail },
      metadata: {},
      schemaVersion: 1,
      timestamp: "2026-10-17T00:00:00.000Z",
      commandId: id,
      correlationId: id,
    });
    store.registerUpcasters({
      "pages.record.changed": {
        version: 2,
        upcasters: {
          1: (payload) => {
            if (payload.fail) throw new Error("poison match");
            return { total: 10 };
          },
        },
      },
      "pages.record.ignored": {
        version: 2,
        upcasters: {
          1: () => {
            throw new Error("unrelated payload");
          },
        },
      },
    });
    await store.appendToStream(stream, 0, [
      event("ignored", "ignored"),
      event("good", "changed"),
      event("bad", "changed", true),
    ]);
    const page = await store.readPage!({
      eventNames: ["pages.record.changed"],
      limit: 1,
    });
    expect(page.events).toMatchObject([
      { id: "good", schemaVersion: 2, payload: { total: 10 } },
    ]);
    expect(page.through).toEqual(page.events[0]!.position);
    await expect(
      store.readPage!({
        eventNames: ["pages.record.changed"],
        fromPosition: page.through,
      }),
    ).rejects.toThrow("poison match");
  });
  test(`${adapter}: paging dense and sparse selections returns exactly the selected Events in order, ending at the head`, async () => {
    const store = await open();
    // Mostly "common" Events, with a "rare" one every seventh: dense and sparse selections of one log.
    const events = await store.appendToStream(
      { context: "pages", aggregate: "mix", id: "one" },
      0,
      Array.from({ length: 40 }, (_, index) => ({
        id: `mix-${index}`,
        name: index % 7 === 3 ? "rare" : "common",
        payload: {},
        metadata: {},
        schemaVersion: 1,
        timestamp: "2026-10-17T00:00:00.000Z",
        commandId: `mix-${index}`,
        correlationId: "mix",
      })),
    );
    for (const names of [["common"], ["rare"], ["common", "rare"], ["none"]])
      for (const limit of [1, 3, 8]) {
        const eventNames = names.map((name) => `pages.mix.${name}`);
        const read: string[] = [];
        let through = null as RecordedEvent["position"] | null,
          more = false;
        for (;;) {
          const page = await store.readPage!({
            eventNames,
            limit,
            ...(through ? { fromPosition: through } : {}),
          });
          expect(page.events.length).toBeLessThanOrEqual(limit);
          // An incomplete page promises a selected Event after it; a complete one ends at the head.
          if (more) expect(page.events.length).toBeGreaterThan(0);
          if (page.complete)
            expect(page.through).toEqual(events.at(-1)!.position);
          more = page.complete === false;
          read.push(...page.events.map((event) => event.id));
          if (!page.events.length) {
            expect(page.through).toEqual(events.at(-1)!.position);
            expect(page.complete).toBe(true);
            break;
          }
          through = page.through;
        }
        expect(read).toEqual(
          events
            .filter((event) => names.includes(event.name))
            .map((event) => event.id),
        );
      }
  });
  test(`${adapter}: filtered Event pages preserve cross-Context order and progress through excluded Events`, async () => {
    const store = await open();
    const pending = (id: string, name: string): PendingEvent => ({
      id,
      name,
      payload: {},
      metadata: {},
      schemaVersion: 1,
      timestamp: "2026-10-17T00:00:00.000Z",
      commandId: id,
      correlationId: id,
    });
    const [first] = await store.appendToStream(
      { context: "sales", aggregate: "order", id: "one" },
      0,
      [pending("first", "opened")],
    );
    const [second] = await store.appendToStream(
      { context: "payments", aggregate: "receipt", id: "two" },
      0,
      [pending("second", "recorded")],
    );
    const [tail] = await store.appendToStream(
      { context: "audit", aggregate: "entry", id: "three" },
      0,
      [pending("tail", "observed")],
    );
    const eventNames = [
      "payments.receipt.recorded",
      "sales.order.opened",
      "sales.order.opened",
    ];
    const page = await store.readPage!({ eventNames, limit: 1 });
    expect(page.events.map((event) => event.id)).toEqual(["first"]);
    expect(page.through).toEqual(first!.position);
    expect(page.complete).toBe(false);
    const next = await store.readPage!({
      eventNames,
      limit: 1,
      fromPosition: page.through,
    });
    expect(next.events.map((event) => event.id)).toEqual(["second"]);
    expect(next.through).toEqual(tail!.position);
    // The last selected Event fills the page, yet nothing selected follows it: the page is complete.
    expect(next.complete).toBe(true);
    expect(
      await store.readPage!({ eventNames, fromPosition: second!.position }),
    ).toEqual({ events: [], through: tail!.position, complete: true });
    expect(
      await store.readPage!({ eventNames: [], fromPosition: first!.position }),
    ).toEqual({ events: [], through: tail!.position, complete: true });
    const bounded = await store.readPage!({
      eventNames,
      toPosition: first!.position,
    });
    expect(bounded.events.map((event) => event.id)).toEqual(["first"]);
    expect(bounded.through).toEqual(first!.position);
    expect(bounded.complete).toBe(true);
    const unfiltered = await store.readPage!({ limit: 2 });
    expect(unfiltered.events.map((event) => event.id)).toEqual([
      "first",
      "second",
    ]);
    expect(unfiltered.through).toEqual(second!.position);
    expect(
      (await store.readPage!({ fromPosition: unfiltered.through })).events.map(
        (event) => event.id,
      ),
    ).toEqual(["tail"]);
  });
}
