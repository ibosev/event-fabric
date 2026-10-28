import { afterEach, expect, test } from "vitest";
import { Ajv } from "ajv";
import { createApplication } from "../src/runtime/index.js";
import {
  aggregate,
  DateTime,
  defineDomain,
  message,
  worker,
} from "../src/authoring/aggregate.js";
import { only } from "../src/authoring/index.js";
import { createInMemoryEventStore } from "../src/event-store/memory.js";
import { createMemoryReadmodelStore } from "../src/adapters/memory-readmodel.js";
import { createInMemoryTransport } from "../src/transport/memory.js";
import { resolveSchema } from "../src/readmodel-store/schema.js";
import { Type } from "typebox";

const cleanups: (() => Promise<unknown>)[] = [];
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});
const Amount = Type.Object(
  { amount: Type.Number({ exclusiveMinimum: 0 }) },
  { additionalProperties: false },
);
const bankAccount = () =>
  aggregate("bank", "account", {
    state: Type.Object({ balance: Type.Number() }),
    initialState: { balance: 0 },
    events: {
      deposited: {
        schema: Amount,
        handler: ({ payload }, state) =>
          state.set("balance", state.get("balance") + payload.amount),
      },
    },
    commands: {
      deposit: {
        schema: Amount,
        steps: [(command, account) => account.apply.deposited(command.payload)],
        rules: [
          (current) => {
            if (current.get("balance") > 100) throw new Error("over the limit");
          },
        ],
      },
      tip: {
        // Ajv fills the default before the schema check, so the Step always receives it.
        schema: Type.Object({ amount: Type.Optional(Type.Number({ default: 1 })) }),
        steps: [
          (command, account) =>
            account.apply.deposited({ amount: command.payload.amount ?? 0 }),
        ],
      },
    },
  });

test("a domain runs end to end: Command builders, Reaction keys, Rules, queued Message, Worker", async () => {
  const account = bankAccount();
  const domain = defineDomain(account);
  const statement = message(
    "send-statement",
    Type.Object({ account: Type.String() }, { additionalProperties: false }),
  );
  const sent: unknown[] = [];
  const application = createApplication({
    name: "handles",
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    readmodelStore: createMemoryReadmodelStore(),
    aggregates: [account],
    pollInterval: 5,
    readmodels: {
      bank: {
        models: {
          balances: {
            schema: Type.Object({
              balance: Type.Number(),
              status: Type.Union([Type.Literal("low"), Type.Literal("ok"), Type.Null()]),
              at: DateTime(),
            }),
            reactions: {
              [domain.bank.account.deposited]: [
                only.ifNotExists(),
                (event: any, view: any) =>
                  view.set({
                    balance: event.payload.amount,
                    status: null,
                    at: event.timestamp,
                  }),
              ],
            },
          },
        },
      },
    },
    sagas: {
      statements: {
        reactions: {
          [domain.bank.account.deposited]: (event: any, _: any, app: any) =>
            app.queue(statement, { account: event.aggregate.id }),
        },
      },
    },
    workers: {
      statements: worker(statement, {
        execute: ({ payload }) => void sent.push(payload),
      }),
    },
  });
  await application.start();
  cleanups.push(() => application.stop());

  expect(domain.bank.account.deposited).toBe("bank.account.deposited");
  const send = application.sendCommand;
  expect((await send(domain.bank.account.deposit("a-1", { amount: 5 }))).status).toBe(
    "accepted",
  );
  expect((await send(domain.bank.account.deposit("a-1", { amount: -1 }))).status).toBe(
    "rejected",
  );
  // The per-Command Rule rejects after the Steps.
  const over = await send(domain.bank.account.deposit("a-1", { amount: 500 }));
  expect(over.status).toBe("rejected");
  const tipped = await send(domain.bank.account.tip("a-1", {}));
  expect(tipped.status === "accepted" && tipped.events[0]!.payload).toEqual({
    amount: 1,
  });
  await expect.poll(() => sent.length).toBe(2);
  await expect
    .poll(async () => (await application.readmodels.bank.balances("a-1")).document)
    .toMatchObject({ balance: 5, status: null });
  const { document } = await application.readmodels.bank.balances("a-1");
  expect(document?.at).toBeInstanceOf(Date);
});

test("a domain rejects a Command and Event of the same name, and duplicate Aggregates", () => {
  const clash = aggregate("bank", "clash", {
    state: Type.Object({}),
    initialState: {},
    events: { open: { schema: Type.Object({}), handler: () => {} } },
    commands: { open: { schema: Type.Object({}), steps: [] } },
  });
  expect(() => defineDomain(clash)).toThrow("Command and Event are both named open");
  expect(() => defineDomain(bankAccount(), bankAccount())).toThrow(
    "Duplicate Aggregate bank.account",
  );
});

test("TypeBox Refine and Codec are rejected wherever a schema enters, even nested", async () => {
  const positive = Type.Refine(Type.Number(), (value) => value > 0);
  expect(() =>
    aggregate("bank", "refined", {
      state: Type.Object({}),
      initialState: {},
      events: {},
      commands: {
        pay: { schema: Type.Object({ amount: positive }), steps: [] },
      },
    }),
  ).toThrow("bank.refined.pay/properties/amount: TypeBox Refine is not JSON Schema");
  expect(() => message("m", Type.Object({ n: positive }))).toThrow("Refine");
  await expect(
    createMemoryReadmodelStore().register("g", {
      m: { schema: Type.Object({ n: positive }), reactions: {} } as any,
    }),
  ).rejects.toThrow("Readmodel m/properties/n: TypeBox Refine");
});

test("a plain TypeBox date-time String in a Readmodel asks for DateTime()", () => {
  expect(() =>
    resolveSchema(Type.Object({ at: Type.String({ format: "date-time" }) })),
  ).toThrow("declare them with DateTime()");
});

test("nullable normalization accepts and rejects exactly what the original schema does", () => {
  const ajv = new Ajv({ strict: false });
  const cases: [object, unknown[]][] = [
    [
      { anyOf: [{ type: "string", enum: ["open", "settled"] }, { type: "null" }] },
      ["open", "settled", null, "closed", 1],
    ],
    [
      { anyOf: [{ type: "string", const: "x", description: "d" }, { type: "null" }] },
      ["x", null, "y", 1],
    ],
    [
      { anyOf: [{ const: "a" }, { const: "b" }, { type: "null" }] },
      ["a", "b", null, "c"],
    ],
    [{ anyOf: [{ type: "integer", minimum: 2 }, { type: "null" }] }, [2, 1, null, 2.5, "2"]],
    [{ enum: [1, 2.5] }, [1, 2.5, 3, "1", null]],
  ];
  for (const [original, values] of cases) {
    const normalized = resolveSchema(original as any);
    for (const value of values)
      expect(ajv.validate(normalized, value), JSON.stringify([original, value])).toBe(
        ajv.validate(original, value),
      );
  }
});
