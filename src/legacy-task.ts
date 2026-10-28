import { buildDomain } from "./kernel/index.js";
import { legacyReadmodels } from "./legacy-gateway.js";
import { legacyEvent } from "./legacy-socket.js";
import { awaitEvent } from "./runtime/index.js";
import type { CommandInput } from "./id.js";
import type { TaskContext } from "./runtime/types.js";
import type { Outcome } from "./types.js";

// Anything but accepted throws as `.await` does: the Rejection, or `Command <status>`.
const settled = (outcome: Outcome) => {
  if (outcome.status !== "accepted") awaitEvent(outcome, "*");
  return outcome;
};

/**
 * The old `getDomain()` on a Task context: a Command is sent on the next microtask, unless `.await(pattern)` is
 * chained first; that waits chain-wide for the pattern (with or without the old `domain.` prefix), as the old
 * runner did, and resolves to the Event in its old shape. Both throw on a Rejection.
 */
const legacyTaskDomain = (ctx: TaskContext<any>) =>
  buildDomain(ctx.config.aggregates ?? [], (input: CommandInput) => {
    let sent: Promise<Outcome> | undefined;
    const send = () =>
      (sent ??= Promise.resolve(ctx.sendCommand(input)).then(settled));
    queueMicrotask(() => sent ?? send());
    const command = {
      then: (resolve: any, reject: any) => send().then(resolve, reject),
      catch: (reject: any) => send().catch(reject),
      await: (pattern: string) => {
        const name = pattern.replace(/^domain\./, "");
        sent ??= ctx.awaitCorrelated(input, name).then((waited) => {
          if (waited.status === "accepted")
            return legacyEvent(waited.event) as any;
          settled(waited.outcome);
          throw new Error(`Command accepted without awaited Event ${name}`);
        });
        return sent;
      },
    };
    return command;
  });

/**
 * The Application an old Task module (`module.exports = async (task, app) => …`) receives from `event-fabric task`:
 * `getDomain()`, `getReadmodels()` (where `group.model()` is the old query builder), `getServices()` and
 * `getLogger()` over the Task context, then whatever the config module's `legacyTaskApp(ctx)` export adds.
 */
export const legacyTaskApp = (
  ctx: TaskContext<any>,
  own: Record<string, unknown> = {},
) => ({
  getDomain: () => legacyTaskDomain(ctx),
  getReadmodels: () => legacyReadmodels(ctx.readmodels),
  getServices: () => ctx.services,
  getLogger: () => ctx.logger,
  ...own,
});
