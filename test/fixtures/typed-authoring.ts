// Type-checked by test/typed-authoring.test.ts against a Register generated from its model.
// Every @ts-expect-error must stay an error.
import {
  commandTo,
  compile,
  createApplication,
  only,
  readmodel,
  type AccessMap,
  type ReadmodelReactions,
  type SagaReactions,
  type WorkerDefinition,
} from "../../dist/index.js";

type State = { owner: string; balance: number; closed: boolean };

export const account = compile("bank", "account", {
  initialState: { owner: "", balance: 0, closed: false } as State,
  commands: {
    open: [
      only.ifNotExists(),
      only.ifValidatedBy({}),
      (command, account) => account.apply.opened({ owner: command.payload.owner }),
    ],
    deposit: (command, account) => {
      const balance: number = account.get("balance") + command.payload.amount;
      account.apply.deposited({ amount: command.payload.amount });
      if (balance > 100) account.apply.closed();
      // @ts-expect-error unknown Event
      account.apply.withdrawn({ amount: 1 });
      // @ts-expect-error wrong Event payload
      account.apply.deposited({ amount: "1" });
      // @ts-expect-error unknown State key
      account.get("missing");
      // @ts-expect-error unknown payload field
      command.payload.owner;
    },
    // Declared here but not generated yet: loose until `event-fabric types` runs.
    freeze: (command, account) => account.apply.frozen(command.payload.anything),
  },
  events: {
    opened: (event, state) => state.set("owner", event.payload.owner),
    deposited: (event, state) =>
      state.set("balance", state.get("balance") + event.payload.amount),
    // @ts-expect-error wrong State value
    closed: (_, state) => state.set("closed", "yes"),
    frozen: () => {},
  },
});

compile("bank", "account", {
  // @ts-expect-error every registered Event needs a handler
  events: { opened: () => {}, deposited: () => {} },
});

// An Aggregate that is not registered stays as permissive as untyped JavaScript.
compile("other", "thing", {
  initialState: {},
  commands: { any: (command, thing) => thing.apply.whatever(command.payload.x) },
  events: { whatever: (event, state) => state.set("nested.path", event.payload) },
});

export const documents: ReadmodelReactions<"bank.accounts"> = {
  "bank.account.opened": (event, view) =>
    view.set({ owner: event.payload.owner, balance: 0 }),
  "bank.account.deposited": [
    only.ifExists(),
    (event, view) => view.set("balance", view.get("balance") + event.payload.amount),
  ],
  // @ts-expect-error unknown qualified Event
  "bank.account.typo": () => {},
};

export const statements: SagaReactions<{ sent: number }> = {
  "bank.account.closed": (event, saga, app) => {
    saga.set("sent", 1);
    app.queue("send-statement", { account: event.aggregate.id });
    // @ts-expect-error unknown Message
    app.queue("send-invoice", { account: event.aggregate.id });
    // @ts-expect-error wrong Message payload
    app.queue("send-statement", { account: 1 });
    app.getDomain().bank.account(event.aggregate.id).deposit({ amount: 1 });
    // @ts-expect-error wrong Command payload through getDomain
    app.getDomain().bank.account("x").deposit({ amount: "1" });
  },
};

export const worker: WorkerDefinition<"send-statement"> = {
  handles: ["send-statement"],
  async execute(message, ctx) {
    const account: string = message.payload.account;
    const outcome = await ctx.send(
      commandTo("bank.account.deposit", account, { amount: 1 }),
    );
    if (outcome.status === "rejected") outcome.error.code;
    // @ts-expect-error unknown payload field
    message.payload.owner;
    // @ts-expect-error unknown Command
    commandTo("bank.account.refund", account, {});
    // @ts-expect-error wrong Command payload
    commandTo("bank.account.deposit", account, { amount: "1" });
  },
};

export const access: AccessMap = {
  domain: { "bank.account.open": true, "bank.*": true, "bank.account": true },
  readmodels: { "bank.accounts": true, "bank.*": true },
};
export const typoAccess: AccessMap = {
  // @ts-expect-error unknown Command in the Access map
  domain: { "bank.account.opne": true },
};

readmodel("bank", "accounts");
// A narrowed query value stays a literal although the group has another model.
readmodel("bank", "accounts", {
  filter: (req) => {
    const status = req.query.status;
    return status === "open" || status === "closed" ? { status } : {};
  },
});
// @ts-expect-error unknown Readmodel
readmodel("bank", "acounts");

export const check = async (app: ReturnType<typeof createApplication>) => {
  const read = await app.getReadmodels().bank.accounts("one");
  const owner: string | undefined = read.document?.owner;
  // @ts-expect-error unknown document field
  read.document?.missing;
  await app.getDomain().bank.account("one").open({ owner: "Ana" }).await("bank.account.opened");
  // @ts-expect-error unknown Event pattern
  await app.sendCommand(commandTo("bank.account.open", "one", { owner: "Ana" })).await("bank.account.typo");
  return owner;
};
