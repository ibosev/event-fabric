// @ts-check
// Checked JavaScript sees the services contract typed-services.ts registers.
import { compile, createApplication } from "../../dist/index.js";
import { createInMemoryEventStore } from "../../dist/testing.js";
import { createInMemoryTransport } from "../../dist/transport/memory.js";
import { services } from "./typed-services.js";

const ports = {
  store: createInMemoryEventStore(),
  transport: createInMemoryTransport(),
};
/** @satisfies {import('../../dist/index.js').ApplicationConfig} */
const config = { name: "javascript", ...ports, services };
createApplication(config);
// @ts-expect-error required services cannot be left out
createApplication({ name: "missing", ...ports });

export const account = compile("bank", "account", {
  commands: {
    open: (_command, account, app) => {
      app.getServices().mailer().send("person@example.test");
      // @ts-expect-error missing service
      app.getServices().mailre();
      account.apply.opened();
    },
  },
  events: { opened: () => {} },
});
/** @type {import('../../dist/index.js').Task} */
export const run = async (ctx) => {
  await ctx.services.registry.verify("name");
  // @ts-expect-error wrong argument
  await ctx.services.registry.verify(1);
};
