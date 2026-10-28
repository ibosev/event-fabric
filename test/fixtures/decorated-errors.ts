// Compile-only (test/decorators.test.ts): every @ts-expect-error line must be an error.
import {
  Type,
  type ReactionApp,
  type ReactionView,
  type Reactions,
} from "../../dist/index.js";
import {
  Aggregate,
  CommandHandler,
  EventHandler,
  EventSourcingHandler,
  Readmodel,
  SagaEventHandler,
  command,
  event,
  type AggregateOf,
  type DocumentOf,
  type EventTypes,
  type SagaOf,
  type StateOf,
} from "../../dist/decorators.js";

const Opened = Type.Object({ owner: Type.String() });
const Amount = Type.Object({ amount: Type.Number() });
class OpenAccount extends command("bank.account.open", Opened) {}
class MakeDeposit extends command("bank.account.deposit", Amount) {}
class AccountOpened extends event("bank.account.opened", Opened) {}
class Deposited extends event("bank.account.deposited", Amount) {}
class Withdrawn extends event("bank.account.withdrawn", Amount) {}
const State = Type.Object({ owner: Type.String(), balance: Type.Number() });
const Balance = Type.Object({ balance: Type.Number() });

// @ts-expect-error a message name has three parts
command("bank.open", Opened);
// @ts-expect-error wrong payload when building a Command
OpenAccount.to("a-1", { amount: 1 });
// @ts-expect-error initialState must match the State schema
@Aggregate({ state: State, initialState: { owner: 1, balance: 0 } })
class WrongInitial {}

@Aggregate({ state: State, initialState: { owner: "", balance: 0 } })
export class Account {
  // @ts-expect-error the method takes another Command
  @CommandHandler(MakeDeposit)
  open(command: OpenAccount, account: AggregateOf<typeof State>) {
    // @ts-expect-error wrong Event payload
    account.apply(Deposited, command.payload);
    // @ts-expect-error a Command type is not an Event type
    account.apply(OpenAccount, command.payload);
    // @ts-expect-error not a State field
    account.get("missing");
  }

  // @ts-expect-error an Event type is not a Command type
  @CommandHandler(AccountOpened)
  wrongKind(_command: OpenAccount, _account: AggregateOf<typeof State>) {}

  // @ts-expect-error same payload shape, different Event: told apart by name
  @EventSourcingHandler(Deposited)
  withdrawn(_event: Withdrawn, state: StateOf<typeof State>) {
    // @ts-expect-error State field type
    state.set("balance", "ten");
  }

  @EventSourcingHandler(AccountOpened)
  // @ts-expect-error decorators do not type parameters: annotate them
  opened(event, state) {
    state.set("owner", event.payload.owner);
  }
}

@Readmodel({ schema: Balance })
export class Balances {
  // @ts-expect-error associationProperty must be a payload key
  @EventHandler(Deposited, { associationProperty: "owner" })
  deposited(event: Deposited, view: DocumentOf<typeof Balance>) {
    // @ts-expect-error not a document field
    view.set("owner", event.payload.amount);
  }

  @EventHandler(AccountOpened, {
    // @ts-expect-error an Identity function returns ids, not numbers
    associationProperty: () => 1,
  })
  opened(_event: AccountOpened, _view: DocumentOf<typeof Balance>) {}
}

export class Saga {
  @SagaEventHandler(Deposited)
  deposited(event: Deposited, saga: SagaOf<typeof Balance>, app: ReactionApp) {
    // @ts-expect-error Saga state field type
    saga.set("balance", "ten");
    app.tell(MakeDeposit.to(saga.id, event.payload));
  }
}

export const reactions: Reactions<
  EventTypes<typeof AccountOpened | typeof Deposited>,
  ReactionView
> = {
  // @ts-expect-error payload of another Event
  [Deposited.type]: (event) => event.payload.owner,
  // @ts-expect-error not among the listed Event types
  [Withdrawn.type]: () => {},
};
void WrongInitial;
