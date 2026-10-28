import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Linter } from "eslint";
import { expect, test } from "vitest";
import { plugin, configs } from "../src/eslint-plugin.js";
const lint = (code: string, filename = "account.aggregate.js") =>
  new Linter().verify(
    code,
    [
      {
        files: ["**/*.js"],
        plugins: { eventfabric: plugin },
        rules: {
          "eventfabric/determinism": "error",
          "eventfabric/command-clock": "warn",
        },
      },
    ],
    filename,
  );
test("deterministic authoring rejects clock/randomness and CRUD reads, Commands warn, Sagas permit clocks", () => {
  const code = `const definition = { events: { added: e => Date.now() }, commands: { add: () => new Date() }, eventEnricher: () => Math.random() };`;
  expect(lint(code).map((message) => message.severity)).toEqual([2, 1, 2]);
  expect(
    lint(
      "const model = { reactions: { added: () => app.getServices().crud.account() }, identity: { added: () => crypto.randomUUID() } };",
      "account.readmodel.js",
    ),
  ).toHaveLength(2);
  expect(
    lint(
      "const saga = { reactions: { added: () => Date.now() }, identity: { added: () => Date.now() } };",
      "account.saga.js",
    ),
  ).toHaveLength(1);
  expect(
    lint(
      "const definition = { events: { added: e => new Date(e.timestamp) } };",
    ),
  ).toEqual([]);
});
test("a Workflow body takes time and randomness from ctx; its id function and other run methods are free", () => {
  const messages = lint(
    `import { workflow as define } from "event-fabric/workflows";
    export const dunning = define("dunning", {
      id: () => String(Date.now()),
      async run(ctx) { const at = Date.now(); await ctx.sleep("s", 1); return [at, Math.random(), await ctx.now()]; },
    });
    export const job = { run: () => Date.now() };`,
    "dunning.workflow.js",
  );
  expect(messages.map((message) => [message.line, message.messageId])).toEqual([
    [4, "workflow"],
    [4, "workflow"],
  ]);
});
test("named authoring exports, assigned CommonJS shapes and UUID aliases are checked", () => {
  expect(
    lint(
      "export const events={added:()=>Date.now()}; export const commands={add:()=>new Date()};",
    ).map((message) => message.severity),
  ).toEqual([2, 1]);
  expect(lint("exports.identity={added:()=>Math.random()};")).toHaveLength(1);
  expect(
    lint(
      'import { v4 as makeId } from "uuid"; export const reactions={added:()=>makeId()};',
      "account.readmodel.js",
    ),
  ).toHaveLength(1);
});
test("namespace UUID calls and imported crypto UUID aliases are rejected in handlers", () => {
  expect(
    lint(
      'import * as ids from "uuid";import {randomUUID as makeId} from "node:crypto";export const events={added:()=>[ids.v4(),makeId()]};',
    ),
  ).toHaveLength(2);
});

test("CommonJS crypto destructuring, module aliases and named authoring functions enforce determinism", () => {
  for (const code of [
    'const {randomUUID:makeId}=require("node:crypto");export const events={added:()=>makeId()};',
    'const crypto=require("node:crypto");const other=crypto;const {randomUUID:makeId}=other;export const events={added:()=>makeId()};',
    'const crypto=require("crypto");const makeId=crypto.randomUUID;export function eventEnricher(){return makeId()}',
    "export function identity(){return Date.now()}",
  ])
    expect(lint(code), code).toHaveLength(1);
});

test("the recommended package rules reject default exports", () => {
  const messages = new Linter().verify(
    "export default {};",
    [{ ...configs.recommended, files: ["**/*.js"] }],
    "index.js",
  );
  expect(messages.map((message) => message.ruleId)).toEqual([
    "eventfabric/no-default-export",
  ]);
});

test("inline Date calls read the wall clock even when passed an Event timestamp", () => {
  expect(
    lint("export const events={added:e=>[Date(),Date(e.timestamp)]};"),
  ).toHaveLength(2);
  expect(
    lint("export const reactions={added:()=>Date()};", "clock.saga.js"),
  ).toEqual([]);
});

test("Aggregate-level Rules get the Command clock warning", () => {
  expect(
    lint(
      "export const rules=[(state,command)=>{if(Date.now()>state.due)throw Error('late')}];",
    ).map((message) => message.severity),
  ).toEqual([1]);
  expect(
    lint("const definition={rules:[()=>new Date()]};").map(
      (message) => message.ruleId,
    ),
  ).toEqual(["eventfabric/command-clock"]);
});

test("Sagas are detected structurally, by comment or by option; Readmodel Reactions and Identities stay checked", () => {
  const saga =
    "export const billing={reactions:{added:()=>Date.now()},identity:{added:()=>Date.now()}};";
  expect(lint(saga, "billing.js")).toHaveLength(2);
  expect(lint(`// @eventfabric-saga\n${saga}`, "billing.js")).toHaveLength(1);
  expect(
    lint("const billingSaga={reactions:{added:()=>Date.now()}};", "billing.js"),
  ).toEqual([]);
  expect(
    lint(
      "export const config={sagas:{billing:{reactions:{added:()=>Date.now()}}},readmodels:{sagaReport:{models:{row:{reactions:{added:()=>Date.now()}}}}}};",
      "event-fabric.config.js",
    ),
  ).toHaveLength(1);
  const withOption = (code: string, filename: string) =>
    new Linter().verify(
      code,
      [
        {
          files: ["**/*.js"],
          plugins: { eventfabric: plugin },
          rules: {
            "eventfabric/determinism": [
              "error",
              { sagaFiles: "/process-managers/" },
            ],
          },
        },
      ],
      filename,
    );
  expect(withOption(saga, "src/process-managers/billing.js")).toHaveLength(1);
  expect(withOption(saga, "src/account.saga.js")).toHaveLength(2);
  expect(
    lint(
      "export const model={schema:{},reactions:{added:()=>Date.now()}};",
      "saga-report.readmodel.js",
    ),
  ).toHaveLength(1);
});

// Decorated TypeScript (`event-fabric/decorators`) is linted as tsc 7 emits it, like this
// repository's own `eslint dist`: no TypeScript parser is needed. Markers are block comments,
// which tsc keeps on the emitted line.
const decorated = `
import { randomUUID } from "node:crypto";
import { Aggregate as Entity, EventSourcingHandler as Fold } from "./decorators.mjs";
declare const Aggregate: any, CommandHandler: any, EventSourcingHandler: any, Readmodel: any,
  EventHandler: any, Saga: any, SagaEventHandler: any, only: any, app: any, x: any, Base: any;
@Aggregate({ state: {}, initialState: {} })
export class Account {
  balance = 0; /* expect: decorated-class */
  constructor() { console.log("built"); } /* expect: decorated-class */
  static count = 0; /* expect: decorated-class */

  @CommandHandler(x, { before: [only.ifNotExists(), () => Date.now() /* expect: command-clock */] })
  open(command: any, account: any) {
    account.apply(x, { ...command.payload, at: new Date() /* expect: command-clock */ });
    this.balance = 1; /* expect: decorated-class */
  }

  @EventSourcingHandler(x)
  opened(event: any, state: any) {
    state.set("id", randomUUID()); /* expect: determinism */
    state.set("at", Date.now()); /* expect: determinism */
    state.set("from", new Date(event.timestamp));
  }
}

@Readmodel({ schema: {} })
export class Balances {
  @EventHandler(x, { associationProperty: () => randomUUID() /* expect: determinism */ })
  opened(event: any, view: any) {
    view.set({ owner: event.payload.owner, seen: Math.random() /* expect: determinism */ });
    app.getServices().crud.accounts(); /* expect: determinism */
  }
}

@Saga()
export class Reminders extends Base {
  @SagaEventHandler(x, { associationProperty: () => Date.now() /* expect: determinism */ })
  opened(_event: any, saga: any) {
    saga.set("at", Date.now()); // a Saga may read the clock
  }
}

/* No class decorator, no handler decorators: not checked. */
export class Plain {
  count = 0;
  constructor() {}
  now() { return Date.now(); }
}

/* Renamed imports keep their roles. */
@Entity({ state: {}, initialState: {} })
export class Aliased {
  count = 0; /* expect: decorated-class */
  @Fold(x)
  opened(_event: any, state: any) { state.set("at", Date.now()); /* expect: determinism */ }
}

/* A handler class whose empty constructor tsc fills with its own initializer call. */
export class Quiet {
  @EventHandler(x)
  opened(_event: any, view: any) { view.set("ok", true); }
}
`;
test("decorated classes compiled by tsc 7: roles come from the decorators, and handler classes hold no state", async () => {
  const directory = await mkdtemp(join(tmpdir(), "eventfabric-lint-"));
  try {
    // .mts: Applications are ESM (ADR-0010), so imports stay imports.
    await writeFile(join(directory, "billing.mts"), decorated);
    await writeFile(
      join(directory, "decorators.mts"),
      "export const Aggregate: any = () => () => {}, EventSourcingHandler: any = () => () => {};",
    );
    execFileSync(join(process.cwd(), "node_modules/.bin/tsc"), [
      "--ignoreConfig",
      "--target",
      "ES2023",
      "--module",
      "NodeNext",
      "--types",
      "node",
      "--outDir",
      join(directory, "out"),
      join(directory, "billing.mts"),
      join(directory, "decorators.mts"),
    ]);
    const emitted = await readFile(join(directory, "out/billing.mjs"), "utf8");
    const messages = new Linter().verify(
      emitted,
      [{ ...configs.recommended, files: ["**/*.mjs"] }],
      "billing.mjs",
    );
    const lines = emitted.split("\n");
    const got = messages.map(
      (m) => `${m.line}:${m.ruleId?.replace("eventfabric/", "")}`,
    );
    const want = lines.flatMap((line, i) =>
      [...line.matchAll(/expect: ([\w-]+)/g)].map(
        (match) => `${i + 1}:${match[1]}`,
      ),
    );
    // The superclass is reported where tsc puts it: `var Reminders = class extends _classSuper`.
    want.push(
      `${lines.findIndex((line) => / class extends /.test(line)) + 1}:decorated-class`,
    );
    expect(got.sort()).toEqual(want.sort());
    // The CRUD read is reported as such, not as nondeterminism.
    expect(messages.find((m) => m.messageId === "crud")?.line).toBe(
      lines.findIndex((line) => line.includes("crud.accounts")) + 1,
    );
    expect(
      messages.find((m) => m.message.includes("superclass"))?.message,
    ).toBe(
      "A decorated Saga class holds no state: remove the superclass. Handlers receive the State, document or Saga state as an argument.",
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
