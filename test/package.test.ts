import { createRequire } from "node:module";
import { readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { expect, test } from "vitest";
test("every built entry can load with CommonJS require(esm)", () => {
  const require = createRequire(import.meta.url);
  const manifest = JSON.parse(
    readFileSync(new URL("../package.json", import.meta.url), "utf8"),
  );
  for (const [entry, target] of Object.entries(manifest.exports) as [
    string,
    { default: string },
  ][]) {
    const namespace = require(
      new URL(`../${target.default}`, import.meta.url).pathname,
    );
    expect(namespace, entry).toBeTypeOf("object");
    expect(namespace, entry).not.toHaveProperty("default");
  }
});
test("event-fabric/workflows exports workflow and signal", async () => {
  const manifest = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
  const entry = manifest.exports["./workflows"];
  expect(entry).toEqual({ types: "./dist/workflows/index.d.ts", default: "./dist/workflows/index.js" });
  const workflows = await import(new URL(`../${entry.default}`, import.meta.url).href);
  expect(Object.keys(workflows).sort()).toEqual(["signal", "workflow"]);
});
test("CommonJS loads every entry and runs managed services without top-level await", () => {
  expect(
    execFileSync(process.execPath, ["test/fixtures/require.cjs"], {
      encoding: "utf8",
    }),
  ).toContain("manage services");
});
test("the runnable example reads its own committed Command", () => {
  expect(
    execFileSync(process.execPath, ["examples/basic.mjs"], {
      encoding: "utf8",
    }),
  ).toContain("First order");
});
// registerHooks sees every specifier resolved by import and require alike
const driverLoads = (entry: string, how: "import" | "require"): string[] =>
  JSON.parse(
    execFileSync(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        `import { registerHooks, createRequire } from "node:module";
         const seen = [];
         registerHooks({ resolve(specifier, context, next) { seen.push(specifier); return next(specifier, context); } });
         const target = new URL(${JSON.stringify(`../${entry}`)}, ${JSON.stringify(import.meta.url)});
         if (${JSON.stringify(how)} === "import") await import(target.href);
         else createRequire(import.meta.url)(target.pathname);
         console.log(JSON.stringify(seen.filter((s) => /^(mongodb|amqplib)(\\/|$)/.test(s))));`,
      ],
      { encoding: "utf8" },
    ),
  );
test("importing the root entry loads neither mongodb nor amqplib (ESM and CommonJS)", () => {
  expect(driverLoads("dist/index.js", "import")).toEqual([]);
  expect(driverLoads("dist/index.js", "require")).toEqual([]);
  // proves the hook observes driver loads
  expect(driverLoads("dist/rabbitmq.js", "import")).toContain("amqplib");
  expect(driverLoads("dist/rabbitmq.js", "require")).toContain("amqplib");
});
