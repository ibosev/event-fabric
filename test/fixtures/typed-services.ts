// Type-checked by test/typed-authoring.test.ts with typed-services.js. Every @ts-expect-error must stay an error.
import {
  Type,
  compile,
  createApplication,
  message,
  worker,
  type CommandApp,
  type Plugin,
  type ReactionApp,
  type ReadmodelApp,
  type Reactions,
  type ReactionView,
  type Task,
  type WorkerDefinition,
} from "../../dist/index.js";
import { createInMemoryEventStore } from "../../dist/testing.js";
import { createInMemoryTransport } from "../../dist/transport/memory.js";
import { command, CommandHandler } from "../../dist/decorators.js";

export interface Services {
  registry: { verify(name: string): Promise<void> };
  // A callable factory stays a value: `mailer()` is the Application's own call.
  mailer: () => { send(address: string): void };
}
declare module "../../dist/index.js" {
  interface Register {
    services: Services;
  }
}
export const services = {
  registry: { async verify(_name: string) {} },
  mailer: () => ({ send(_address: string) {} }),
} satisfies Services;
const ports = {
  store: createInMemoryEventStore(),
  transport: createInMemoryTransport(),
};

// Declared apart from createApplication: the context reads Register["services"].
export const account = compile("bank", "account", {
  commands: {
    open: async (command, account, app) => {
      await app.getServices().registry.verify(String(command.payload));
      app.getServices().mailer().send("person@example.test");
      // @ts-expect-error missing service
      app.getServices().mailre();
      // @ts-expect-error wrong argument through a callable factory
      app.getServices().mailer().send(123);
      app.now.toISOString();
      account.apply.opened();
    },
  },
  events: { opened: () => {} },
  idGenerator: (_command, app) => {
    // @ts-expect-error id generators get the same Command context
    app.getServices().unknown();
    return "id";
  },
});
// A reusable Step names only what it needs.
export const verifyOnly = (app: CommandApp<Pick<Services, "registry">>) =>
  app.getServices().registry.verify("name");

type Opened = { "bank.account.opened": { owner: string } };
export const readmodel: Reactions<Opened, ReactionView, ReadmodelApp> = {
  "bank.account.opened": (event, view, app) => {
    app.getServices().mailer();
    view.set("owner", event.payload.owner);
    // @ts-expect-error Readmodel Reactions cannot tell
    app.tell;
    // @ts-expect-error Readmodel Reactions cannot queue
    app.queue("mail", {});
  },
};
export const saga: Reactions<Opened, ReactionView, ReactionApp> = {
  "bank.account.opened": (event, _state, app) => {
    app.queue("mail", { to: event.payload.owner });
    // @ts-expect-error missing service
    app.getServices().nope;
  },
};

const Mail = message("mail", Type.Object({ to: Type.String() }));
export const mailer = worker(Mail, {
  execute(message, ctx) {
    ctx.services.mailer().send(message.payload.to);
    // @ts-expect-error missing service
    ctx.services.registryMissing();
  },
});
export const plugin: Plugin = (ctx) => {
  ctx.services.mailer().send("person@example.test");
  // @ts-expect-error typed plugin context
  ctx.services.unknown();
};
export const task: Task = async (ctx, [name]) => {
  await ctx.services.registry.verify(name ?? "");
  // @ts-expect-error typed Task context
  await ctx.services.registry.verify(3);
};

class Open extends command("bank.account.open", Type.String()) {}
export class Decorated {
  @CommandHandler(Open)
  open(command: Open, _state: unknown, app: CommandApp) {
    app.getServices().registry.verify(command.payload);
    // @ts-expect-error an annotated decorated method is checked
    app.getServices().missing();
  }
}

createApplication({ name: "good", ...ports, services, plugins: [plugin] });
createApplication({
  name: "managed",
  ...ports,
  async setupServices({ resources }) {
    resources.defer(() => {});
    return services;
  },
});
// @ts-expect-error required services cannot be left out
createApplication({ name: "missing", ...ports });
createApplication({
  name: "undeclared helpers",
  ...ports,
  services,
  // @ts-expect-error Readmodels still expect the services until Register.readmodelServices is declared
  readmodelServices: {},
});
createApplication({
  name: "incomplete",
  ...ports,
  // @ts-expect-error an incomplete map is not the contract
  services: { mailer: services.mailer },
});
createApplication({
  name: "bad override",
  ...ports,
  services: {
    ...services,
    // @ts-expect-error a test replacement must implement the contract
    registry: { verify: async (_name: number) => {} },
  },
});
createApplication({
  name: "good override",
  ...ports,
  services: { ...services, registry: { verify: async () => {} } },
});
createApplication({
  name: "setup missing",
  ...ports,
  // @ts-expect-error setup must return every service
  setupServices: () => ({ mailer: services.mailer }),
});
// @ts-expect-error one form or the other
createApplication({
  name: "both",
  ...ports,
  services,
  setupServices: () => services,
});
const typed = createApplication({ name: "typed", ...ports, services });
typed.getServices().registry.verify("name");

// Another Application's contract is given explicitly, never inferred.
type OtherServices = { notifier: { send(text: string): void } };
const reusable: WorkerDefinition<"notice", OtherServices> = {
  handles: ["notice"],
  execute: (_message, ctx) => ctx.services.notifier.send("ready"),
};
createApplication<OtherServices>({
  name: "other",
  ...ports,
  services: { notifier: { send() {} } },
  workers: { notice: reusable },
});
createApplication<OtherServices>({
  name: "other missing",
  ...ports,
  // @ts-expect-error an explicit contract cannot be satisfied by an empty map
  services: {},
});
