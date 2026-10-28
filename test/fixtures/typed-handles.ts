// Type-checked by test/typed-authoring.test.ts with NO generated Register: the domain
// carries every type itself. Every @ts-expect-error must stay an error.
import {
  aggregate,
  DateTime,
  defineDomain,
  message,
  only,
  Type,
  worker,
  type Document,
  type EventsOf,
  type Identities,
  type ReactionApp,
  type ReactionView,
  type Reactions,
  type ReadmodelRepository,
} from "../../src/index.js";

const Amount = Type.Object({ amount: Type.Number({ exclusiveMinimum: 0 }) });
const Opened = Type.Object({ owner: Type.String({ minLength: 1 }) });

export const account = aggregate("bank", "account", {
  state: Type.Object({
    owner: Type.String(),
    balance: Type.Number(),
    closedBy: Type.Union([Type.String(), Type.Null()]),
  }),
  initialState: { owner: "", balance: 0, closedBy: null },
  events: {
    opened: {
      schema: Opened,
      handler: ({ payload }, state) => state.set("owner", payload.owner),
    },
    deposited: {
      schema: Amount,
      handler: ({ payload }, state) =>
        state.set("balance", state.get("balance") + payload.amount),
    },
    closed: {
      schema: Type.Object({ by: Type.String() }),
      // @ts-expect-error State value of the wrong type
      handler: ({ payload }, state) => state.set("balance", payload.by),
    },
  },
  commands: {
    open: {
      schema: Opened,
      steps: [
        only.ifNotExists(),
        (command, account) => account.apply.opened(command.payload),
      ],
    },
    deposit: {
      schema: Amount,
      steps: [
        only.ifExists(),
        // A Precondition is a Step that throws.
        (command, account) => {
          if (account.get("closedBy") !== null) throw new Error("closed");
          // @ts-expect-error unknown State key inside a Precondition
          account.get("closd");
        },
        // @ts-expect-error untyped only.ifState callbacks are not accepted; write a Step
        only.ifState(() => {}),
        (command, account) => {
          account.apply.deposited({ amount: command.payload.amount });
          if (account.get("balance") > 100) account.apply.closed({ by: "limit" });
          // @ts-expect-error unknown Event
          account.apply.withdrawn({ amount: 1 });
          // @ts-expect-error wrong Event payload
          account.apply.deposited({ amount: "1" });
          // @ts-expect-error unknown State key
          account.get("missing");
          // @ts-expect-error unknown Command payload field
          command.payload.owner;
        },
      ],
      rules: [
        (current, previous, _events, command) => {
          if (current.get("balance") < previous.get("balance") + command.payload.amount)
            throw new Error("lost a deposit");
          // @ts-expect-error unknown State key inside a Rule
          current.get("balanse");
          // @ts-expect-error unknown Command payload field inside a Rule
          command.payload.owner;
        },
      ],
    },
  },
  rules: [
    // @ts-expect-error unknown State key in an Aggregate-level Rule
    (current) => current.get("missing"),
  ],
});

aggregate("bank", "ledger", {
  state: Type.Object({ total: Type.Number() }),
  // @ts-expect-error initialState must match the State schema
  initialState: { total: "0" },
  events: {},
  commands: {},
});

export const domain = defineDomain(account);

// The domain carries qualified names and typed Commands to other files.
const opened: "bank.account.opened" = domain.bank.account.opened;
domain.bank.account.deposit("a-1", { amount: 5 });
// @ts-expect-error wrong Command payload
domain.bank.account.deposit("a-1", { amount: "5" });
// @ts-expect-error unknown Command
domain.bank.account.withdraw("a-1", { amount: 5 });
// @ts-expect-error unknown Event
domain.bank.account.openedd;
// @ts-expect-error unknown Aggregate
domain.bank.acount;

// Several Aggregates, two in one context: each keeps its own members.
const card = aggregate("bank", "card", {
  state: Type.Object({}),
  initialState: {},
  events: { blocked: { schema: Type.Object({}), handler: () => {} } },
  commands: {
    block: { schema: Type.Object({}), steps: [(_, c) => c.apply.blocked({})] },
  },
});
const audit = aggregate("risk", "audit", {
  state: Type.Object({}),
  initialState: {},
  events: { flagged: { schema: Type.Object({}), handler: () => {} } },
  commands: {},
});
const wide = defineDomain(account, card, audit);
wide.bank.account.deposit("a-1", { amount: 5 });
wide.bank.card.block("c-1", {});
const flagged: "risk.audit.flagged" = wide.risk.audit.flagged;
// @ts-expect-error a member of another Aggregate in the same context
wide.bank.card.deposit;
const wideEvents: Reactions<EventsOf<typeof wide>> = {
  [wide.bank.account.deposited]: ({ payload }) => payload.amount,
  [wide.bank.card.blocked]: () => {},
  [wide.risk.audit.flagged]: () => {},
};

const Statement = Type.Object({
  owner: Type.String(),
  balance: Type.Number(),
  note: Type.Optional(Type.String()),
  sentAt: DateTime(),
});
type Statement = Document<typeof Statement>;
// Stores read an absent optional field as null and a date-time as a Date.
export const stored: Statement = {
  _id: "a-1",
  owner: "Ana",
  balance: 1,
  note: null,
  sentAt: new Date(),
};
// @ts-expect-error a stored date-time is a Date, not a string
export const wrongDate: Statement = { ...stored, sentAt: "2026-01-01T00:00:00Z" };

// Readmodel queries are typed by the document: fields, value types and operators.
const Ledger = Type.Object({
  owner: Type.String(),
  balance: Type.Number(),
  status: Type.Union([Type.Literal("open"), Type.Literal("closed")]),
  tags: Type.Array(Type.String()),
  entries: Type.Array(Type.Object({ amount: Type.Number(), memo: Type.String() })),
  holder: Type.Optional(Type.Object({ name: Type.String() })),
  sentAt: DateTime(),
});
declare const ledgers: ReadmodelRepository<Document<typeof Ledger>>;
export async function queries() {
  await ledgers.find({
    status: "open",
    balance: { gte: 10, lt: 100 },
    tags: "vip",
    "entries.amount": { gt: 0 },
    entries: { elemMatch: { memo: "fee", amount: { lte: 5 } } },
    "holder.name": { in: ["Ana", "Ben"] },
    sentAt: { gt: "2026-01-01T00:00:00Z" },
    oneOf: [{ owner: "Ana" }, { holder: { exists: false } }],
  });
  await ledgers.count({ _id: { nin: ["a"] }, "holder.name": null });
  for await (const ledger of ledgers.stream({}, { sort: { sentAt: -1 }, size: 10 }))
    ledger.balance;
  // @ts-expect-error unknown field
  await ledgers.find({ ownr: "Ana" });
  // @ts-expect-error unknown nested path
  await ledgers.findOne({ "holder.age": 1 });
  // @ts-expect-error wrong value type
  await ledgers.find({ balance: "10" });
  // @ts-expect-error value outside a literal union
  await ledgers.count({ status: "pending" });
  // @ts-expect-error range on a field that is not orderable: an array of objects
  await ledgers.find({ entries: { gt: 1 } });
  // @ts-expect-error elemMatch on a field that is not an array
  await ledgers.find({ owner: { elemMatch: { eq: "Ana" } } });
  // @ts-expect-error unknown sort field
  await ledgers.find({}, { sort: { ownr: 1 } });
  // @ts-expect-error Mongo-style operators are not part of the typed vocabulary
  await ledgers.find({ balance: { $gt: 1 } });
}
// An untyped Readmodel still accepts any filter.
declare const loose: ReadmodelRepository;
export const anything = loose.find({ whatever: { $gt: 1 }, "a.b.c": [1] }, { sort: { x: 1 } });

export const statement = message(
  "send-statement",
  Type.Object({ account: Type.String() }),
);

export const reactions: Reactions<
  EventsOf<typeof domain>,
  ReactionView<Statement>,
  ReactionApp
> = {
  [domain.bank.account.opened]: ({ payload }, view, app) => {
    view.set({ owner: payload.owner, balance: 0, note: null, sentAt: new Date() });
    app.queue(statement, { account: view.id });
    // @ts-expect-error wrong Message payload
    app.queue(statement, { account: 1 });
  },
  [domain.bank.account.deposited]: [
    only.ifExists(),
    ({ payload }, view) =>
      view.set("balance", view.get("balance") + payload.amount),
  ],
  // @ts-expect-error payload of another Event
  [domain.bank.account.closed]: ({ payload }) => payload.amount,
};

export const identities: Identities<EventsOf<typeof domain>> = {
  [domain.bank.account.opened]: (event) => event.payload.owner,
  // @ts-expect-error unknown payload field in an Identity
  [domain.bank.account.closed]: (event) => event.payload.owner,
};

export const statements = worker(statement, {
  concurrency: 2,
  async execute({ payload }, ctx) {
    const outcome = await ctx.send(
      domain.bank.account.deposit(payload.account, { amount: 1 }),
    );
    if (outcome.status === "rejected") outcome.error.code;
    // @ts-expect-error unknown Message payload field
    payload.owner;
  },
});
export { opened };
