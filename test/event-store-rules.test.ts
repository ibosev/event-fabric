import { expect, test } from "vitest";
import { LeaseLostError } from "../src/errors.js";
import {
  replayable,
  replayedEntry,
  skippedEventLetter,
} from "../src/event-store/rules.js";
import type { ConsumerStatus } from "../src/types.js";

const position = (n: number) => ({ transactionId: String(n), sequence: "1" });
const halted: ConsumerStatus = {
  group: "saga:mail",
  partition: 0,
  checkpoint: position(1),
  state: "halted",
  eventId: "poison",
  position: position(2),
  attempts: 5,
  error: "boom",
};
const skip = (overrides: Partial<Parameters<typeof skippedEventLetter>[0]>) =>
  skippedEventLetter({
    group: "saga:mail",
    eventId: "poison",
    who: "ops",
    status: halted,
    checkpoint: position(1),
    epoch: 3,
    ...overrides,
  });

test("an operator skip records the halted Event's failure and Position", () => {
  expect(skip({})).toMatchObject({
    kind: "skipped-event",
    group: "saga:mail",
    eventId: "poison",
    position: position(2),
    error: "boom",
    attempts: 5,
    who: "ops",
  });
});

test("an operator skip refuses anything but exactly the halted, unprocessed Event under the expected epoch", () => {
  expect(() => skip({ eventId: "other" })).toThrow(/halted on exactly/);
  expect(() => skip({ status: { ...halted, state: "running" } })).toThrow(
    /halted on exactly/,
  );
  expect(() => skip({ checkpoint: position(2) })).toThrow(/already processed/);
  expect(() => skip({ expectedEpoch: 2 })).toThrow(LeaseLostError);
  expect(() => skip({ group: "readmodels:list" })).toThrow(
    /Application.skipConsumer/,
  );
  expect(skip({ group: "readmodels:list", expectedEpoch: 3 }).group).toBe(
    "readmodels:list",
  );
});

test("a replayed entry gets a new id, no attempts and no delay", () => {
  const replayed = replayedEntry({
    id: "old",
    kind: "message",
    queue: "mail",
    attempt: 4,
    notBefore: "2099-01-01T00:00:00.000Z",
    message: {
      id: "m",
      name: "mail",
      payload: {},
      metadata: {},
      schemaVersion: 1,
      timestamp: "2026-10-15T00:00:00.000Z",
      correlationId: "c",
      attempt: 5,
    },
  });
  expect(replayed.id).not.toBe("old");
  expect(replayed).toMatchObject({ attempt: 0, message: { attempt: 1 } });
  expect(replayed).not.toHaveProperty("notBefore");
  expect(() => replayable(undefined)).toThrow(/not found/);
  expect(() =>
    replayable({
      id: "s",
      kind: "skipped-event",
      error: "x",
      attempts: 1,
      createdAt: "2026-10-15T00:00:00.000Z",
    }),
  ).toThrow(/rebuild/);
});
