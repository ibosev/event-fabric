import {
  createApplication,
  type CommandApp,
  type ReactionApp,
  type ReadmodelApp,
} from "../../dist/index.js";
import type { LegacyRequestApp } from "../../dist/compat.js";
import { createInMemoryEventStore, createInMemoryTransport } from "../../dist/testing.js";

type Crud = { manage: { user(): string } };
declare module "../../dist/index.js" {
  interface Register {
    services: { crud: Crud };
    legacyApp: { getCrud(): Crud };
  }
}
const ports = {
  name: "legacy-app",
  store: createInMemoryEventStore(),
  transport: createInMemoryTransport(),
  services: { crud: { manage: { user: () => "ada" } } },
};
createApplication({ ...ports, legacyApp: (app) => ({ getCrud: () => app.getServices().crud }) });
// @ts-expect-error legacyApp supplies every registered member
createApplication({ ...ports, legacyApp: () => ({}) });
export const step = (app: CommandApp): string => app.getCrud().manage.user();
export const saga = (app: ReactionApp): string => app.getCrud().manage.user();
export const readmodel = (app: ReadmodelApp): string => app.getCrud().manage.user();
export const route = (app: LegacyRequestApp): string => app.getCrud().manage.user();
// @ts-expect-error only registered members
export const typo = (app: CommandApp) => app.getCrudd();
