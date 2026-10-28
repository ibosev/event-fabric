import { expect, test } from "vitest";
import { compile, only } from "../src/authoring/index.js";
import { generateTypes } from "../src/cli/types.js";
test("types expose qualified Event/Command names and schema payloads/documents without runtime constants", () => {
  const aggregate = compile("sales", "account", {
    commands: {
      add: [
        only.ifValidatedBy({
          type: "object",
          required: ["amount"],
          properties: {
            amount: { type: "number" },
            note: { type: ["string", "null"] },
          },
        }),
        () => {},
      ],
    },
    events: {
      added: {
        handler: () => {},
        schema: {
          type: "object",
          required: ["amount"],
          properties: { amount: { type: "number" } },
        },
      },
      closed: () => {},
    },
  });
  const output = generateTypes({
    aggregates: [aggregate],
    readmodels: {
      api: {
        models: {
          account: {
            schema: {
              type: "object",
              properties: { at: { type: "string", format: "date-time" } },
            },
          },
        },
      },
    },
  });
  expect(output).toContain('declare module "event-fabric"');
  expect(output).toContain('"added": { "amount": number }');
  // An Event without a schema keeps a checked name and a loose payload.
  expect(output).toContain('"closed": any');
  expect(output).toContain(
    '"add": { "amount": number; "note"?: string | null }',
  );
  expect(output).toContain('"account": { _id: string } & { "at"?: Date | null }');
  expect(output).not.toMatch(/export (const|let|var)/);
});
test("named schemas resolve their own local references and recursive Command payloads remain typed", () => {
  const aggregate = compile("sales", "tree", {
    commands: { add: [only.ifValidatedBy("/Tree"), () => {}] },
    events: {},
  });
  const source = generateTypes(
    { aggregates: [aggregate] },
    {
      "/Tree": {
        type: "object",
        properties: {
          value: { $ref: "#/definitions/Value" },
          children: { type: "array", items: { $ref: "#" } },
        },
        definitions: { Value: { type: "string" } },
      },
    },
  );
  expect(source).toContain("export type ReferencedSchema2 = string;");
  expect(source).toContain(
    '(import("event-fabric").CommandPayloads["sales.tree.add"])[]',
  );
});

test("the generated Register resolves each Readmodel group's own schema overrides for the TypeScript compiler", async () => {
  const { mkdtemp, writeFile, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const { execFileSync } = await import("node:child_process");
  const directory = await mkdtemp(join(tmpdir(), "eventfabric-types-"));
  try {
    const library = join(process.cwd(), "dist/index.js");
    const source = generateTypes(
      {
        readmodels: {
          numeric: {
            schemas: { "/Age": { type: "integer" } },
            models: {
              person: {
                schema: {
                  type: "object",
                  required: ["age"],
                  properties: { age: { $ref: "/Age" } },
                },
              },
            },
          },
          textual: {
            schemas: { "/Age": { type: "string" } },
            models: {
              person: {
                schema: {
                  type: "object",
                  required: ["age"],
                  properties: { age: { $ref: "/Age" } },
                },
              },
            },
          },
        },
      },
      {},
      { module: library },
    );
    await writeFile(join(directory, "registry.d.ts"), source);
    await writeFile(
      join(directory, "model.ts"),
      `import type { ReadmodelDocument } from ${JSON.stringify(library)};
const numeric: ReadmodelDocument<"numeric.person"> = {_id:"one",age:42};
const textual: ReadmodelDocument<"textual.person"> = {_id:"one",age:"forty-two"};
// @ts-expect-error Each group resolves its own schema overrides.
const wrongNumeric: ReadmodelDocument<"numeric.person"> = {_id:"one",age:"forty-two"};
// @ts-expect-error Each group resolves its own schema overrides.
const wrongTextual: ReadmodelDocument<"textual.person"> = {_id:"one",age:42};
`,
    );
    expect(() =>
      execFileSync(
        join(process.cwd(), "node_modules/.bin/tsc"),
        [
          "--ignoreConfig",
          "--noEmit",
          "--strict",
          "--skipLibCheck",
          "--target",
          "ES2023",
          "--module",
          "NodeNext",
          join(directory, "model.ts"),
          join(directory, "registry.d.ts"),
        ],
        { encoding: "utf8" },
      ),
    ).not.toThrow();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
test("patternProperties (TypeBox Record) render as an index signature", () => {
  const output = generateTypes({
    readmodels: {
      report: {
        models: {
          day: {
            schema: {
              type: "object",
              required: ["byMethod"],
              properties: {
                byMethod: {
                  type: "object",
                  patternProperties: { "^.*$": { type: "number" } },
                },
              },
            },
          },
        },
      },
    },
  });
  expect(output).toContain('"byMethod": { [key: string]: number }');
});
test("generated Saga Message types check Saga names, message names and payloads at every sender", async () => {
  const { mkdtemp, writeFile, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const { execFileSync } = await import("node:child_process");
  const directory = await mkdtemp(join(tmpdir(), "eventfabric-saga-types-"));
  try {
    const library = join(process.cwd(), "dist/index.js");
    const source = generateTypes(
      {
        sagas: {
          "crm.connector": {
            messages: {
              done: {
                handle() {},
                schema: { type: "object", required: ["by"], properties: { by: { type: "string" } } },
              },
              ping: () => {},
            },
          },
          quiet: {},
        },
      },
      {},
      { module: library },
    );
    expect(source).toContain('"crm.connector": {\n        "done": { "by": string };\n        "ping": any;');
    expect(source).not.toContain('"quiet"');
    await writeFile(join(directory, "registry.d.ts"), source);
    await writeFile(
      join(directory, "send.ts"),
      `import type { AccessMap, SendSagaMessage } from ${JSON.stringify(library)};
declare const send: SendSagaMessage<Promise<void>>;
void send("crm.connector", "done", { by: "route" }, { to: "f1" });
void send("crm.connector", "ping", 1);
// @ts-expect-error The payload follows the handler's schema.
void send("crm.connector", "done", { by: 1 });
// @ts-expect-error Only declared Saga Messages.
void send("crm.connector", "other", {});
// @ts-expect-error Only Sagas that handle Saga Messages.
void send("quiet", "done", {});
const access: AccessMap = { sagaMessages: { "crm.connector.done": true, "crm.connector.*": true, "*": false } };
// @ts-expect-error Access keys name declared Saga Messages.
const wrong: AccessMap = { sagaMessages: { "crm.connector.other": true } };
export { access, wrong };
`,
    );
    expect(() =>
      execFileSync(
        join(process.cwd(), "node_modules/.bin/tsc"),
        [
          "--ignoreConfig",
          "--noEmit",
          "--strict",
          "--skipLibCheck",
          "--target",
          "ES2023",
          "--module",
          "NodeNext",
          join(directory, "send.ts"),
          join(directory, "registry.d.ts"),
        ],
        { encoding: "utf8" },
      ),
    ).not.toThrow();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
test("generated Workflow types check Workflow names, Signal names and payloads, and Access keys", async () => {
  const { mkdtemp, writeFile, rm } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const { execFileSync } = await import("node:child_process");
  const directory = await mkdtemp(join(tmpdir(), "eventfabric-workflow-types-"));
  try {
    const library = join(process.cwd(), "dist/index.js");
    const run = async () => {};
    const source = generateTypes(
      {
        workflows: {
          dunning: {
            versions: [
              // `legacy` is v1's only: instances pinned to v1 still take it.
              { version: 1, run, signals: { poke: {}, legacy: { schema: { type: "string" } } } },
              {
                version: 2,
                run,
                signals: {
                  promised: { schema: { type: "object", required: ["until"], properties: { until: { type: "string" } } } },
                  poke: {},
                },
              },
            ],
          },
          quiet: { version: 1, run },
        },
      },
      {},
      { module: library },
    );
    expect(source).toContain(
      '"dunning": {\n        signals: {\n          "legacy": string;\n          "poke": any;\n          "promised": { "until": string };',
    );
    await writeFile(join(directory, "registry.d.ts"), source);
    await writeFile(
      join(directory, "send.ts"),
      `import type { AccessMap, PluginContext } from ${JSON.stringify(library)};
declare const ctx: PluginContext;
void ctx.sendSignal("i1", { workflow: "dunning", name: "promised", payload: { until: "2026-12-01" } });
void ctx.sendSignal("i1", { workflow: "dunning", name: "legacy", payload: "old" });
void ctx.startWorkflow("quiet", "q1", { anything: true });
void ctx.startWorkflow("dunning", "i1", undefined, { signal: { workflow: "dunning", name: "poke", payload: null } });
void ctx.queryWorkflow("dunning", "i1");
// @ts-expect-error The payload follows the Signal's schema.
void ctx.sendSignal("i1", { workflow: "dunning", name: "promised", payload: { until: 1 } });
// @ts-expect-error Only declared Signals.
void ctx.sendSignal("i1", { workflow: "dunning", name: "other", payload: null });
// @ts-expect-error Only the named Workflow's Signals.
void ctx.sendSignal("i1", { workflow: "quiet", name: "poke", payload: null });
// @ts-expect-error A start's Signal belongs to the Workflow it starts.
void ctx.startWorkflow("quiet", "q1", {}, { signal: { workflow: "dunning", name: "poke", payload: null } });
// @ts-expect-error Only registered Workflows.
void ctx.queryWorkflow("missing", "i1");
const access: AccessMap = { workflows: { "dunning.start": true, "dunning.signals.promised": true, "quiet.*": true, "*": false } };
// @ts-expect-error Access keys name declared Signals.
const wrong: AccessMap = { workflows: { "dunning.signals.other": true } };
export { access, wrong };
`,
    );
    expect(() =>
      execFileSync(
        join(process.cwd(), "node_modules/.bin/tsc"),
        [
          "--ignoreConfig",
          "--noEmit",
          "--strict",
          "--skipLibCheck",
          "--target",
          "ES2023",
          "--module",
          "NodeNext",
          join(directory, "send.ts"),
          join(directory, "registry.d.ts"),
        ],
        { encoding: "utf8" },
      ),
    ).not.toThrow();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
