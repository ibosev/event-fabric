#!/usr/bin/env node
import { parseArgs } from "node:util";
import { writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { auditFixtureFiles } from "./audit.js";
export const runAuditCLI = async (args: string[]): Promise<number> => {
  try {
    const { values } = parseArgs({
      args,
      options: {
        config: { type: "string" },
        fixtures: { type: "string" },
        "revision-base": { type: "string" },
        output: { type: "string" },
        help: { type: "boolean" },
      },
    });
    if (values.help) {
      process.stdout.write(
        "node dist/migration/cli.js --config <audit.mjs> --fixtures <folder> --revision-base 0|1 [--output <report.json>]\n",
      );
      return 0;
    }
    if (
      !values.config ||
      !values.fixtures ||
      !["0", "1"].includes(values["revision-base"] ?? "")
    )
      throw Error(
        "Specify --config, --fixtures and the source --revision-base 0|1",
      );
    const module = await import(pathToFileURL(resolve(values.config)).href);
    if (!module.audit?.definitions)
      throw Error(
        "Audit config must export named audit with definitions and optional kernel, explanations, skipCommand and stateOmit",
      );
    const result = await auditFixtureFiles(resolve(values.fixtures), {
      ...module.audit,
      revisionBase: Number(values["revision-base"]) as 0 | 1,
    });
    if (values.output)
      await writeFile(
        resolve(values.output),
        `${JSON.stringify(result, null, 2)}\n`,
      );
    process.stdout.write(
      JSON.stringify({
        passed: result.passed,
        checked: result.checked,
        skipped: result.skipped.length,
        differences: result.differences.length,
      }) + "\n",
    );
    return result.passed ? 0 : 1;
  } catch (error) {
    process.stderr.write(
      `${error instanceof Error ? error.message : String(error)}\n`,
    );
    return 1;
  }
};
if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
)
  void runAuditCLI(process.argv.slice(2)).then((code) => {
    process.exitCode = code;
  });
