// @ts-check
import { createApplication } from "../../dist/index.js";
import { ports, helpers } from "./typed-readmodel-services.js";

/** @satisfies {import('../../dist/index.js').ApplicationConfig} */
const config = { ...ports, readmodelServices: helpers };
createApplication(config);
// @ts-expect-error checked JavaScript must supply registered Readmodel helpers
createApplication(ports);

/** @satisfies {import('../../dist/index.js').ReadmodelReactions<'bank.accounts'>} */
export const reactions = {
  "bank.account.opened": (event, view, app) => {
    view.set("owner", app.getServices().owner(event.payload.owner));
    // @ts-expect-error Application services are absent in checked JavaScript too
    app.getServices().directory.owner();
  },
};
