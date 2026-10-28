// Compiled by test/decorators.test.ts with tsc (standard decorators), then imported.
import {
  aggregate,
  only,
  Rejection,
  Type,
  type ReactionApp,
  type ReactionView,
  type Reactions,
  type SagaMessageView,
} from "../../dist/index.js";
import {
  Aggregate,
  CommandHandler,
  EventHandler,
  EventSourcingHandler,
  Readmodel,
  Saga,
  SagaDeadlineHandler,
  SagaEventHandler,
  SagaMessageHandler,
  command,
  event,
  eventOf,
  sagaMessage,
  type AggregateOf,
  type DocumentOf,
  type EventTypes,
  type SagaOf,
  type StateOf,
} from "../../dist/decorators.js";

const Opened = Type.Object(
  { owner: Type.String({ minLength: 1 }) },
  { additionalProperties: false },
);
const Amount = Type.Object(
  { amount: Type.Number({ exclusiveMinimum: 0 }) },
  { additionalProperties: false },
);
const Deposit = Type.Object(
  { amount: Type.Number({ exclusiveMinimum: 0 }), note: Type.String() },
  { additionalProperties: false },
);
export class OpenAccount extends command("bank.account.open", Opened) {}
export class MakeDeposit extends command("bank.account.deposit", Amount) {}
export class AccountOpened extends event("bank.account.opened", Opened) {}
// Version 2 added `note`; version 1 payloads are upcast when read.
export class Deposited extends event("bank.account.deposited", Deposit, {
  schemaVersion: 2,
  upcasters: { 1: (payload) => ({ ...payload, note: "" }) },
}) {}

const AccountState = Type.Object({
  owner: Type.String(),
  balance: Type.Number(),
});
const limit = (current: { get(path: "balance"): number }) => {
  if (current.get("balance") > 100)
    throw new Rejection("rule", "over the limit");
};

@Aggregate({
  state: AccountState,
  initialState: { owner: "", balance: 0 },
  rules: [limit],
})
export class Account {
  @CommandHandler(OpenAccount, { before: [only.ifNotExists()] })
  open(command: OpenAccount, account: AggregateOf<typeof AccountState>) {
    account.apply(AccountOpened, command.payload);
  }

  @CommandHandler(MakeDeposit, {
    before: [
      only.ifExists(),
      (_command, account: AggregateOf<typeof AccountState>) => {
        if (!account.get("owner"))
          throw new Rejection("precondition", "no owner");
      },
    ],
  })
  deposit(command: MakeDeposit, account: AggregateOf<typeof AccountState>) {
    account.apply(Deposited, {
      amount: command.payload.amount,
      note: `by ${account.get("owner")}`,
    });
  }

  @EventSourcingHandler(AccountOpened)
  opened(event: AccountOpened, state: StateOf<typeof AccountState>) {
    state.set("owner", event.payload.owner);
  }

  @EventSourcingHandler(Deposited)
  deposited(event: Deposited, state: StateOf<typeof AccountState>) {
    state.set("balance", state.get("balance") + event.payload.amount);
  }
}

/** The same Aggregate written with `aggregate()`, for parity. */
export const accountObject = aggregate("bank", "account", {
  state: AccountState,
  initialState: { owner: "", balance: 0 },
  rules: [limit],
  commands: {
    open: {
      schema: Opened,
      steps: [only.ifNotExists(), (c, a) => a.apply.opened(c.payload)],
    },
    deposit: {
      schema: Amount,
      steps: [
        only.ifExists(),
        (_, a) => {
          if (!a.get("owner")) throw new Rejection("precondition", "no owner");
        },
        (c, a) =>
          a.apply.deposited({
            amount: c.payload.amount,
            note: `by ${a.get("owner")}`,
          }),
      ],
    },
  },
  events: {
    opened: {
      schema: Opened,
      handler: (e, s) => s.set("owner", e.payload.owner),
    },
    deposited: {
      schema: Deposit,
      schemaVersion: 2,
      upcasters: { 1: (payload) => ({ ...payload, note: "" }) },
      handler: (e, s) => s.set("balance", s.get("balance") + e.payload.amount),
    },
  },
});

/** An existing `aggregate()` Aggregate that decorated components react to and send to. */
export const audit = aggregate("bank", "audit", {
  state: Type.Object({ notes: Type.Number() }),
  initialState: { notes: 0 },
  commands: {
    note: { schema: Amount, steps: [(c, a) => a.apply.noted(c.payload)] },
  },
  events: {
    noted: {
      schema: Amount,
      handler: (_e, s) => s.set("notes", s.get("notes") + 1),
    },
  },
});
export class Noted extends eventOf(audit, "noted") {}

const Balance = Type.Object({ owner: Type.String(), balance: Type.Number() });
@Readmodel({ schema: Balance, indexes: [{ fields: ["owner"] }] })
export class Balances {
  @EventHandler(AccountOpened, { before: [only.ifNotExists()] })
  opened(event: AccountOpened, view: DocumentOf<typeof Balance>) {
    view.set({ owner: event.payload.owner, balance: 0 });
  }

  @EventHandler(Deposited)
  deposited(event: Deposited, view: DocumentOf<typeof Balance>) {
    view.set("balance", view.get("balance") + event.payload.amount);
  }
}

/** Notes per audit Stream, from an Event of an existing `aggregate()` Aggregate. */
const Notes = Type.Object({ count: Type.Number(), total: Type.Number() });
@Readmodel({ schema: Notes, initialState: { count: 0, total: 0 } })
export class AuditNotes {
  @EventHandler(Noted)
  noted(event: Noted, view: DocumentOf<typeof Notes>) {
    view.set({
      count: view.get("count") + 1,
      total: view.get("total") + event.payload.amount,
    });
  }
}

/** Big deposits are noted on the owner's audit Stream: decorated Saga → existing Aggregate. */
const Seen = Type.Object({ deposits: Type.Number() });
@Saga()
export class BigDeposits {
  @SagaEventHandler(Deposited)
  deposited(event: Deposited, saga: SagaOf<typeof Seen>, app: ReactionApp) {
    saga.set("deposits", (saga.get("deposits") ?? 0) + 1);
    if (event.payload.amount >= 50)
      app.tell({
        stream: {
          context: "bank",
          aggregate: "audit",
          id: `audit-${event.aggregate.id}`,
        },
        name: "note",
        payload: { amount: event.payload.amount },
      });
  }
}

/** A Saga keyed by a payload function, reacting to an existing Aggregate's Event and sending to a decorated one. */
@Saga()
export class Reopen {
  @SagaEventHandler(Noted, {
    associationProperty: (event) =>
      event.payload.amount > 1000 ? null : event.aggregate.id,
  })
  noted(event: Noted, saga: SagaOf<typeof Seen>, app: ReactionApp) {
    saga.set("deposits", 1);
    app.tell(OpenAccount.to(`from-${event.aggregate.id}`, { owner: "audit" }));
  }
}

/** Saga Messages and Deadlines, decorated: a nudge sets a Deadline whose handler deposits. */
const Nudge = Type.Object({ by: Type.String() }, { additionalProperties: false });
export class Nudged extends sagaMessage("nudge", Nudge) {}
const Reminder = Type.Object({ by: Type.String(), reminded: Type.Number() });
@Saga({ from: "beginning" })
export class Reminders {
  @SagaEventHandler(AccountOpened)
  opened(_event: AccountOpened, saga: SagaOf<typeof Reminder>) {
    saga.set("reminded", 0);
  }

  @SagaMessageHandler(Nudged)
  nudged(message: Nudged, saga: SagaOf<typeof Reminder>) {
    saga.set("by", message.payload.by);
    saga.deadline("remind", new Date(Date.now() + 20));
  }

  @SagaDeadlineHandler("remind")
  remind(_message: SagaMessageView, saga: SagaOf<typeof Reminder>, app: ReactionApp) {
    saga.set("reminded", (saga.get("reminded") ?? 0) + 1);
    app.tell(MakeDeposit.to(saga.id, { amount: 1 }));
  }
}

/** An object Readmodel keyed by decorated Event types. */
export const counts: Reactions<
  EventTypes<typeof AccountOpened | typeof Deposited>,
  ReactionView<{ events: number }>
> = {
  [AccountOpened.type]: (_e, view) =>
    view.set("events", (view.get("events") ?? 0) + 1),
  [Deposited.type]: (e, view) =>
    view.set(
      "events",
      (view.get("events") ?? 0) + (e.payload.amount > 0 ? 1 : 0),
    ),
};

/** Classes whose declaration must fail; each runs its decorators when called. */
export const invalid: Record<string, () => unknown> = {
  "no handlers": () => {
    @Aggregate({ state: AccountState, initialState: { owner: "", balance: 0 } })
    class Empty {}
    return Empty;
  },
  "two Aggregates": () => {
    @Aggregate({ state: AccountState, initialState: { owner: "", balance: 0 } })
    class Mixed {
      @CommandHandler(OpenAccount)
      open(_c: OpenAccount, _a: AggregateOf<typeof AccountState>) {}
      @EventSourcingHandler(Noted)
      noted(_e: Noted, _s: StateOf<typeof AccountState>) {}
    }
    return Mixed;
  },
  "duplicate handler": () => {
    @Aggregate({ state: AccountState, initialState: { owner: "", balance: 0 } })
    class Twice {
      @EventSourcingHandler(AccountOpened)
      one(_e: AccountOpened, _s: StateOf<typeof AccountState>) {}
      @EventSourcingHandler(AccountOpened)
      two(_e: AccountOpened, _s: StateOf<typeof AccountState>) {}
    }
    return Twice;
  },
  "undecorated method": () => {
    @Readmodel({ schema: Balance })
    class Helper {
      @EventHandler(AccountOpened)
      opened(_e: AccountOpened, _v: DocumentOf<typeof Balance>) {}
      format() {}
    }
    return Helper;
  },
  "wrong role": () => {
    @Readmodel({ schema: Balance })
    class Wrong {
      @SagaEventHandler(AccountOpened)
      opened(_e: AccountOpened, _v: DocumentOf<typeof Balance>) {}
    }
    return Wrong;
  },
  inheritance: () => {
    class Base {}
    @Readmodel({ schema: Balance })
    class Child extends Base {
      @EventHandler(AccountOpened)
      opened(_e: AccountOpened, _v: DocumentOf<typeof Balance>) {}
    }
    return Child;
  },
  "static member": () => {
    @Readmodel({ schema: Balance })
    class Static {
      static count = 0;
      @EventHandler(AccountOpened)
      opened(_e: AccountOpened, _v: DocumentOf<typeof Balance>) {}
    }
    return Static;
  },
  "static handler": () => {
    @Readmodel({ schema: Balance })
    class StaticHandler {
      @EventHandler(AccountOpened)
      static opened(_e: AccountOpened, _v: DocumentOf<typeof Balance>) {}
    }
    return StaticHandler;
  },
  "Command as Event": () => {
    @Readmodel({ schema: Balance })
    class Confused {
      @EventHandler(OpenAccount as any)
      opened(_e: AccountOpened, _v: DocumentOf<typeof Balance>) {}
    }
    return Confused;
  },
  "unqualified name": () => command("open" as any, Opened),
};

/** Declares fine, but applying another Aggregate's Event fails when the Command runs. */
@Aggregate({ state: AccountState, initialState: { owner: "", balance: 0 } })
export class ForeignApply {
  @CommandHandler(OpenAccount)
  open(_c: OpenAccount, account: AggregateOf<typeof AccountState>) {
    account.apply(Noted, { amount: 1 });
  }
  @EventSourcingHandler(AccountOpened)
  opened(_e: AccountOpened, _s: StateOf<typeof AccountState>) {}
}

/** Declares fine, but a handler reading `this` fails on its first call: handlers run unbound. */
@Readmodel({ schema: Balance })
export class Stateful {
  @EventHandler(AccountOpened)
  opened(_e: AccountOpened, view: DocumentOf<typeof Balance>) {
    view.set("owner", (this as any).owner);
  }
}
export const notDecorated = class Plain {};
