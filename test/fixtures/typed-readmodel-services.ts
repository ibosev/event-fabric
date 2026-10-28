import {
  Type,
  createApplication,
  type ApplicationConfig,
  type ReadmodelApp,
  type ReadmodelReactions,
  type ReactionApp,
} from "../../dist/index.js";
import { event, EventHandler } from "../../dist/decorators.js";
import { createInMemoryEventStore, createInMemoryTransport } from "../../dist/testing.js";

type Services = { directory: { owner(): string } };
type Helpers = { owner(name: string): string };
declare module "../../dist/index.js" {
  interface Register {
    services: Services;
    readmodelServices: Helpers;
  }
}
export const ports = {
  name: "readmodel-services",
  store: createInMemoryEventStore(),
  transport: createInMemoryTransport(),
  services: { directory: { owner: () => "mutable" } },
};
export const helpers: Helpers = { owner: (name) => name.toUpperCase() };
export const config = { ...ports, readmodelServices: helpers } satisfies ApplicationConfig;
createApplication(config);
// @ts-expect-error required Readmodel helpers cannot be omitted
createApplication(ports);
createApplication({
  ...ports,
  // @ts-expect-error config cannot infer a weaker helper contract
  readmodelServices: {},
});
createApplication({
  ...ports,
  // @ts-expect-error wrong helper implementation
  readmodelServices: { owner: (_name: number) => "wrong" },
});
// Explicit local contracts work without changing the registered default.
createApplication<Services, { label(value: number): string }>({
  ...ports,
  readmodelServices: { label: String },
});
createApplication<Services, {}>({ ...ports, readmodelServices: {} });
createApplication<Services, {}>({
  ...ports,
  // @ts-expect-error Readmodel helpers are an object, not an array
  readmodelServices: [],
});
// @ts-expect-error an explicit empty helper contract must opt out of legacy services
createApplication<Services, {}>(ports);
// @ts-expect-error local helper contract must also be supplied
createApplication<Services, { label(value: number): string }>(ports);

export const reactions: ReadmodelReactions<"bank.accounts"> = {
  "bank.account.opened": (event, view, app) => {
    view.set("owner", app.getServices().owner(event.payload.owner));
    // @ts-expect-error Application services are absent
    app.getServices().directory.owner();
    // @ts-expect-error helper arguments are checked
    app.getServices().owner(1);
  },
};
export const saga = (app: ReactionApp) => app.getServices().directory.owner();
class Opened extends event("bank.account.opened", Type.Object({ owner: Type.String() })) {}
export class Accounts {
  @EventHandler(Opened)
  opened(event: Opened, _view: unknown, app: ReadmodelApp) {
    app.getServices().owner(event.payload.owner);
    // @ts-expect-error decorated Readmodels get only their helper contract
    app.getServices().directory.owner();
  }
}
