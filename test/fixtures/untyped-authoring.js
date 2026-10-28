// @ts-check
// Without a generated Register, authoring is as permissive as before ADR-0012.
import { commandTo, compile, only } from "../../dist/index.js";

export const anything = compile("any", "thing", {
  initialState: {},
  commands: {
    run: [
      only.ifExists(),
      async (command, thing, app) => {
        // No services contract registered: any service, including a callable factory.
        await app.getServices().crud("thing").find({ id: command.payload.id });
        thing.apply.happened(command.payload.x);
      },
    ],
  },
  events: { happened: (event, state) => state.set("nested.path", event.payload) },
});

/** @satisfies {import("../../dist/index.js").SagaReactions} */
export const reactions = {
  "any.thing.happened": (event, saga, app) => {
    saga.set("seen", event.payload.whatever);
    app.queue("anything", { at: 1 });
    app.getDomain().any.thing("x").run({ y: 1 });
    app.getServices().mailer.send(app.getServices().anything);
  },
};

commandTo("any.thing.run", undefined, { free: true });

/** @type {import("../../dist/index.js").Task} */
export const run = async (ctx) => ctx.services.whatever(1, 2);

/** @type {import("../../dist/index.js").ReadmodelApp} */
const readmodelApp = /** @type {any} */ ({});
// Without a separate Readmodel services contract, legacy services stay available.
readmodelApp.getServices().mailer();
