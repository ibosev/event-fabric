import { execFileSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, expect, test } from "vitest";
import { compile, only } from "../src/authoring/index.js";
import { generateTypes } from "../src/cli/types.js";

const object = (properties: Record<string, unknown>) => ({
  type: "object",
  properties,
  required: Object.keys(properties),
  additionalProperties: false,
});
// The fixtures' model; the test generates its Register exactly as `event-fabric types` would.
const model = {
  aggregates: [
    compile("bank", "account", {
      commands: {
        open: [only.ifValidatedBy(object({ owner: { type: "string" } })), () => {}],
        deposit: [only.ifValidatedBy(object({ amount: { type: "number" } })), () => {}],
      },
      events: {
        opened: { schema: object({ owner: { type: "string" } }), handler: () => {} },
        deposited: { schema: object({ amount: { type: "number" } }), handler: () => {} },
        closed: () => {},
      },
    }),
  ],
  readmodels: {
    bank: {
      models: {
        accounts: {
          schema: object({
            owner: { type: "string" },
            balance: { type: "number" },
            status: { enum: ["open", "closed"] },
          }),
        },
        // A second model, so filters must be checked against the named one.
        cards: { schema: object({ number: { type: "string" } }) },
      },
    },
  },
  workers: {
    statements: {
      handles: ["send-statement"],
      schemas: { "send-statement": object({ account: { type: "string" } }) },
    },
  },
};
const directory = await mkdtemp(join(tmpdir(), "eventfabric-registry-"));
afterAll(() => rm(directory, { recursive: true, force: true }));
const tsc = (files: string[], ...flags: string[]) =>
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
      "--types",
      "node",
      ...flags,
      ...files,
    ],
    { encoding: "utf8" },
  );
const registry = async () => {
  const file = join(directory, "event-fabric.generated.d.ts");
  await writeFile(
    file,
    generateTypes(model, {}, { module: join(process.cwd(), "dist/index.js") }),
  );
  return file;
};

test("a generated Register types TypeScript authoring and rejects every marked mistake", async () => {
  const generated = await registry();
  expect(() =>
    tsc([join(process.cwd(), "test/fixtures/typed-authoring.ts"), generated]),
  ).not.toThrow();
});

test("JavaScript under checkJs gets the same name checks from the Register", async () => {
  const generated = await registry();
  expect(() =>
    tsc(
      [join(process.cwd(), "test/fixtures/typed-authoring.js"), generated],
      "--allowJs",
      "--checkJs",
    ),
  ).not.toThrow();
});

test("without a Register, untyped JavaScript authoring stays permissive", () => {
  expect(() =>
    tsc(
      [join(process.cwd(), "test/fixtures/untyped-authoring.js")],
      "--allowJs",
      "--checkJs",
    ),
  ).not.toThrow();
});

test("a registered services contract types every context and requires the config to supply it", () => {
  expect(() =>
    tsc(
      [
        join(process.cwd(), "test/fixtures/typed-services.ts"),
        join(process.cwd(), "test/fixtures/typed-services-checked.js"),
      ],
      "--allowJs",
      "--checkJs",
    ),
  ).not.toThrow();
});

test("typed handles carry their own types: no Register, every marked mistake rejected", () => {
  expect(() =>
    tsc([join(process.cwd(), "test/fixtures/typed-handles.ts")]),
  ).not.toThrow();
});

test("Workflow Handles type waitFor, ask, Signals, starts and queries: no Register, every marked mistake rejected", () => {
  expect(() =>
    tsc([join(process.cwd(), "test/fixtures/typed-workflows.ts")]),
  ).not.toThrow();
});

test("Readmodel helpers have a separate required contract in TypeScript, decorators and checked JavaScript", () => {
  expect(() =>
    tsc(
      [
        join(process.cwd(), "test/fixtures/typed-readmodel-services.ts"),
        join(process.cwd(), "test/fixtures/typed-readmodel-services-checked.js"),
      ],
      "--allowJs",
      "--checkJs",
    ),
  ).not.toThrow();
});

test("a registered legacyApp types the old accessors on Command, Reaction and request contexts", () => {
  expect(() =>
    tsc([join(process.cwd(), "test/fixtures/typed-legacy-app.ts")]),
  ).not.toThrow();
});

test("typed handles stay cheap to check against the built declarations", async () => {
  const { readFile } = await import("node:fs/promises");
  const fixture = join(directory, "typed-handles-dist.ts");
  const source = await readFile(
    join(process.cwd(), "test/fixtures/typed-handles.ts"),
    "utf8",
  );
  await writeFile(
    fixture,
    source.replace(
      "../../src/index.js",
      join(process.cwd(), "dist/index.js"),
    ),
  );
  const output = tsc([fixture], "--extendedDiagnostics");
  const instantiations = Number(/Instantiations:\s+(\d+)/.exec(output)?.[1]);
  // About 20K today; a naive mapped signature over TypeBox's Static costs over 1M.
  expect(instantiations).toBeGreaterThan(0);
  expect(instantiations).toBeLessThan(100_000);
});
