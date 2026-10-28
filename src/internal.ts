import { Readable } from "node:stream";
import type { Validate } from "./types.js";

/* Shared between EventFabric's own modules; not part of the public interface. */

/**
 * Validators that validate a copy of their input, so callers need not copy it or check it afterwards;
 * each maps to the same validator without the copy, for a caller whose input is already its own copy.
 */
export const copiesInput = new WeakMap<Validate, Validate>();
/** The fold of one worker of a sliced rebuild, called by `runtime/rebuild-worker.ts`. */
export const foldSlice = Symbol("foldSlice");
/** Workflow start, Signal and query by name, untyped, for the CLI and tests; Applications use the contexts. */
export const workflowsApi = Symbol("workflows");
/** The name of a Workflow the contexts take as its Handle or its name. */
export const workflowName = (workflow: string | { name: string }) =>
  typeof workflow === "string" ? workflow : workflow.name;
/** The one Message a Worker that `ctx.ask` takes handles. */
export const askedMessage = (worker: { handles: string[] }) => {
  if (worker.handles.length !== 1)
    throw new Error("ctx.ask takes a Worker of one Message, as worker(message, ...) builds");
  return worker.handles[0]!;
};
/** A Workflow's Consumer group, `workflow:<name>`. */
export const workflowGroup = (name: string) => `workflow:${name}`;
/** The kind and name of a Saga's or Workflow's Consumer group, `saga:<name>` or `workflow:<name>`; else undefined. */
export const parseProcessGroup = (group: string): { kind: "saga" | "workflow"; name: string } | undefined => {
  const colon = group.indexOf(":"),
    kind = group.slice(0, colon);
  return kind === "saga" || kind === "workflow" ? { kind, name: group.slice(colon + 1) } : undefined;
};
/**
 * The Saga Messages a Workflow instance receives besides `start`, `cancel`, `resume` and its Deadlines
 * (named after their step): a Signal `signal:<name>` and a Worker's reply `reply:<step>` (ADR-0021).
 */
export const workflowWake = {
  signal: (name: string) => `signal:${name}`,
  reply: (step: string) => `reply:${step}`,
  /** A step of one run, `<step>@<run>`: its Outbox suffix and the step its reply names. */
  step: (step: string, run?: string) => (run === undefined ? step : `${step}@${run}`),
  parse: (
    name: string,
  ): { kind: "signal"; name: string } | { kind: "reply"; step: string; run?: string } | undefined => {
    if (name.startsWith("signal:")) return { kind: "signal", name: name.slice("signal:".length) };
    if (!name.startsWith("reply:")) return undefined;
    const [step, run] = name.slice("reply:".length).split("@") as [string, string?];
    return run === undefined ? { kind: "reply", step } : { kind: "reply", step, run };
  },
};
/** Enrichers marked by `legacyEnricher`: they run inside each `apply`, before the fold, as old enrichers did. */
export const legacyEnrichers = new WeakSet<Function>();
/**
 * Milliseconds since the epoch of an ISO time, or NaN. Not `Date.parse`, which Applications may replace globally (the
 * old `datejs` package returns a Date or null); the Date constructor keeps the standard parsing.
 */
export const parseTime = (text: string): number => new Date(text).getTime();
// Cursor methods that run the query; every other cursor method chains.
const terminal = new Set([
  "toArray",
  "next",
  "hasNext",
  "tryNext",
  "forEach",
  "close",
  "count",
  "explain",
]);
/** A driver cursor opened later: it records its chain until a terminal method, iteration or `stream()` runs it. */
export const lazyCursor = (open: () => Promise<any>): any => {
  const steps: [string, unknown[]][] = [];
  const run = async () =>
    steps.reduce((at, [method, args]) => at[method](...args), await open());
  const proxy: any = new Proxy(
    {},
    {
      get: (_target, key) => {
        if (key === Symbol.asyncIterator)
          return async function* () {
            yield* await run();
          };
        if (typeof key !== "string" || key === "then") return undefined;
        if (key === "stream")
          return () =>
            Readable.from(
              (async function* () {
                yield* await run();
              })(),
            );
        if (terminal.has(key))
          return async (...args: unknown[]) => (await run())[key](...args);
        return (...args: unknown[]) => (steps.push([key, args]), proxy);
      },
    },
  );
  return proxy;
};
