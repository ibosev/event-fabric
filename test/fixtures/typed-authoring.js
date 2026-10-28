// @ts-check
// JavaScript under checkJs gets the same name checks from the generated Register.
import { commandTo, compile } from "../../dist/index.js";

export const account = compile("bank", "account", {
  initialState: { owner: "", balance: 0, closed: false },
  commands: {
    open: (command, account) => account.apply.opened({ owner: command.payload.owner }),
    // @ts-expect-error unknown Event
    deposit: (command, account) => account.apply.setled({ amount: 1 }),
  },
  events: {
    opened: (event, state) => state.set("owner", event.payload.owner),
    deposited: () => {},
    closed: () => {},
  },
});

/** @satisfies {import("../../dist/index.js").SagaReactions} */
export const reactions = {
  "bank.account.opened": () => {},
  // @ts-expect-error unknown qualified Event
  "bank.account.typo": () => {},
};

// @ts-expect-error unknown Command
commandTo("bank.account.refund", "one", {});
