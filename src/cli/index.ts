import { parseArgs } from "node:util";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { resolve, dirname } from "node:path";
import { pathToFileURL } from "node:url";
import { createInterface } from "node:readline/promises";
import { spawn } from "node:child_process";
import { userInfo } from "node:os";
import {
  assertDeclaredEvents,
  createApplication,
} from "../runtime/index.js";
import type {
  ResolvedApplicationConfig,
  TaskContext,
} from "../runtime/types.js";
import type { Application } from "../runtime/index.js";
import { resolveComponents } from "../authoring/decorators.js";
import { createLogger } from "../observability.js";
import { loadNamedSchemas } from "../validation.js";
import { generateTypes } from "./types.js";
import { migrateApplication } from "../migration/application.js";
import { parseTime, workflowGroup, workflowsApi } from "../internal.js";
export type CLIIO = {
  cwd?: string;
  out?: (value: string) => void;
  err?: (value: string) => void;
  signal?: AbortSignal;
  confirm?: (question: string) => Promise<boolean>;
};
const usage: Record<string, string> = {
  run: "run [--only <kind>:<name>] [--config <file>] [--env <name>]",
  dev: "dev [--inspect] [--only <kind>:<name>] [--config <file>] [--env <name>]",
  migrate: "migrate [--dry-run] [--config <file>]",
  rebuild:
    "rebuild <group> [<group> ...] | --all [--swap auto|manual] [--concurrency <n>] | rebuild --swap <group> (any group name, including auto or manual)",
  task: "task <module> [-- args]",
  types: "types [--check]",
  "migrate-app": "migrate-app <folder> [--patches <folder>] [--packages coexistence|cutover] [--dependency <spec>] [--check] [--report <file>]",
  "dead-letters": "dead-letters list | replay <id|--all> | discard <id>",
  workflows:
    "workflows list [--status <status>] | inspect <name> <id> | resume <name> <id> | cancel <name> <id> | purge --finished-before <date>",
  consumers:
    "consumers status [--json] | retry <group> | skip <group> --event <id> [--yes] [--who <operator>] | retire <saga or workflow group> [--yes]",
};
export async function runCLI(argv: string[], io: CLIIO = {}): Promise<number> {
  const out =
      io.out ??
      ((text: string) => {
        process.stdout.write(`${text}\n`);
      }),
    err =
      io.err ??
      ((text: string) => {
        process.stderr.write(`${text}\n`);
      }),
    cwd = io.cwd ?? process.cwd();
  let application: Application<any> | undefined;
  let config: (ResolvedApplicationConfig & { schemasDir?: string }) | undefined;
  try {
    const separator = argv.indexOf("--"),
      taskArgs = separator < 0 ? [] : argv.slice(separator + 1);
    const { values, positionals } = parseArgs({
      args: separator < 0 ? argv : argv.slice(0, separator),
      allowPositionals: true,
      options: {
        config: { type: "string" },
        env: { type: "string" },
        help: { type: "boolean", short: "h" },
        only: { type: "string", multiple: true },
        "dry-run": { type: "boolean" },
        swap: { type: "string" },
        check: { type: "boolean" },
        json: { type: "boolean" },
        event: { type: "string" },
        yes: { type: "boolean" },
        who: { type: "string" },
        inspect: { type: "boolean" },
        all: { type: "boolean" },
        concurrency: { type: "string" },
        report: { type: "string" },
        dependency: { type: "string" },
        patches: { type: "string" },
        packages: { type: "string" },
        status: { type: "string" },
        "finished-before": { type: "string" },
      },
    });
    const [command, action, argument] = positionals;
    if (!command || values.help) {
      out(
        command && usage[command]
          ? `event-fabric ${usage[command]}`
          : `event-fabric <${Object.keys(usage).join("|")}>\nUse --help after a command.`,
      );
      return 0;
    }
    if (!usage[command]) throw Error(`Unknown command ${command}`);
    if (command === "migrate-app") {
      if (!action || ![undefined, "coexistence", "cutover"].includes(values.packages))
        throw Error(`Usage: event-fabric ${usage[command]}`);
      const result = await migrateApplication(resolve(cwd, action), {
        check: values.check ?? false,
        ...(values.patches && { patches: resolve(cwd, values.patches) }),
        ...(values.dependency && { dependency: values.dependency }),
        ...(values.packages && { packages: values.packages as "coexistence" | "cutover" }),
      });
      if (values.report)
        await writeFile(resolve(cwd, values.report), `${JSON.stringify(result, null, 2)}\n`);
      const changed = result.files.filter((file) => file.changed).length;
      out(JSON.stringify({
        files: result.files.length,
        changed,
        reviews: result.files.reduce((sum, file) => sum + file.reviews.length, 0),
        events: result.events,
      }));
      return values.check && changed ? 1 : 0;
    }
    if (command === "dev") {
      const args = argv
        .map((value, index) => (index === argv.indexOf("dev") ? "run" : value))
        .filter((value) => value !== "--inspect");
      return await new Promise<number>((resolve, reject) => {
        const child = spawn(
          process.execPath,
          [
            "--watch",
            ...(values.inspect ? ["--inspect"] : []),
            new URL("./main.js", import.meta.url).pathname,
            ...args,
          ],
          { cwd, stdio: "inherit", env: process.env },
        );
        child.once("error", reject);
        child.once("exit", (code) => resolve(code ?? 1));
        io.signal?.addEventListener("abort", () => child.kill("SIGTERM"), {
          once: true,
        });
      });
    }
    const configPath = resolve(cwd, values.config ?? "event-fabric.config.js");
    const env = values.env ?? process.env.EVENT_FABRIC_ENV;
    if (env !== undefined) process.env.EVENT_FABRIC_ENV = env;
    const module = await import(pathToFileURL(configPath).href);
    if (!Object.hasOwn(module, "config"))
      throw Error("Config module must export named config");
    const loaded =
      typeof module.config === "function"
        ? await module.config(env)
        : module.config;
    if (!loaded || typeof loaded !== "object")
      throw Error("Config must be an object");
    // Decorated classes become definitions here, so Register generation and startup checks see them too.
    config = resolveComponents(loaded);
    if (command === "types") {
      if (!config.typesPath) throw Error("Set typesPath in config");
      const schemas = {
        ...(config.schemasDir
          ? await loadNamedSchemas(
              resolve(dirname(configPath), config.schemasDir),
            )
          : {}),
        ...config.schemas,
      };
      const source = generateTypes(config, schemas, { mode: config.typesMode, module: config.typesModule }),
        output = resolve(cwd, config.typesPath);
      if (values.check) {
        let actual: string | undefined;
        try {
          actual = await readFile(output, "utf8");
        } catch (error) {
          if ((error as any).code !== "ENOENT") throw error;
        }
        if (actual !== source)
          throw Error(
            `Generated types are stale: ${output}; run event-fabric types`,
          );
        out(`Types are current: ${output}`);
      } else {
        await mkdir(dirname(output), { recursive: true });
        await writeFile(output, source);
        out(`Wrote ${output}`);
      }
      return 0;
    }
    if (command === "migrate") {
      const store = config.store as ResolvedApplicationConfig["store"] & {
        migrate?: (options: { dryRun: boolean }) => Promise<string>;
      };
      if (!store.migrate)
        throw Error("Configured Event store has no migrations");
      const sql = await store.migrate({ dryRun: values["dry-run"] ?? false });
      if (!values["dry-run"])
        await assertDeclaredEvents(store, config.aggregates ?? []);
      out(values["dry-run"] ? sql : "Migrations applied");
      return 0;
    }
    application = createApplication({
      ...config,
      configModule: pathToFileURL(configPath).href,
    });
    if (command === "run") {
      await application.start({ only: values.only });
      out(`Started ${config.name}`);
      const code = await application.run({ signal: io.signal });
      if (code)
        err("Broker connection lost; exiting so the supervisor restarts this instance");
      return code;
    }
    if (command === "task") {
      if (!action) throw Error("Task module path is required");
      const task = await import(pathToFileURL(resolve(cwd, action)).href);
      // An old Task module exports only `(task, app)`, its CommonJS export arriving as the default.
      const legacy = typeof task.run !== "function" && typeof task.default === "function";
      if (typeof task.run !== "function" && !legacy)
        throw Error("Task module must export run(ctx, args)");
      // Sets up services without starting plugins, consumers or Gateways; close() cleans them up.
      await application.initialize();
      const app = application;
      const ctx: TaskContext<any> = {
        config,
        // A Task's Commands go in the bulk lane, so users' Commands never wait behind them.
        sendCommand: (input, options) =>
          app.sendCommand(input, { lane: "bulk", ...options }),
        sendSagaMessage: application.sendSagaMessage,
        startWorkflow: application.startWorkflow,
        sendSignal: application.sendSignal,
        queryWorkflow: application.queryWorkflow,
        awaitCorrelated: (input, pattern, options) =>
          app.awaitCorrelated(input, pattern, {
            lane: "bulk",
            ...options,
          }),
        readmodels: application.readmodels,
        services: application.getServices(),
        readStream: config.store.readStream.bind(config.store),
        readAll: config.store.readAll.bind(config.store),
        import: config.store.import.bind(config.store),
        logger: config.logger ?? createLogger(),
      };
      if (legacy) {
        const { legacyTaskApp } = await import("../legacy-task.js");
        await task.default({ args: taskArgs }, legacyTaskApp(ctx, module.legacyTaskApp?.(ctx)));
      } else await task.run(ctx, taskArgs);
      return 0;
    }
    if (command === "rebuild") {
      // Without a positional group, --swap names the group to swap, even one called auto or manual.
      if (values.swap && !action) {
        out(JSON.stringify(await application.swapRebuild(values.swap)));
        return 0;
      }
      const names = positionals.slice(1);
      if (!names.length === !values.all)
        throw Error("Name the Readmodel groups to rebuild, or pass --all");
      if (values.swap && !["auto", "manual"].includes(values.swap))
        throw Error(
          "Use rebuild <group> --swap auto|manual, or rebuild --swap <group> to swap a completed manual rebuild",
        );
      const concurrency =
        values.concurrency === undefined
          ? undefined
          : Number(values.concurrency);
      if (
        concurrency !== undefined &&
        !(Number.isInteger(concurrency) && concurrency > 0)
      )
        throw Error("--concurrency takes a positive integer");
      // Several groups rebuild at once, each in its own worker thread.
      if (values.all || names.length > 1) {
        out(
          JSON.stringify(
            await application.rebuildGroups(values.all ? undefined : names, {
              swap: values.swap as "auto" | "manual" | undefined,
              concurrency,
            }),
          ),
        );
        return 0;
      }
      out(
        JSON.stringify(
          await application.rebuild(names[0]!, {
            swap: values.swap as "auto" | "manual" | undefined,
          }),
        ),
      );
      return 0;
    }
    if (command === "dead-letters") {
      const store = config.store;
      if (action === "list") {
        out(JSON.stringify(await store.deadLetters(), null, 2));
        return 0;
      }
      if (action === "replay" && values.all) {
        if (argument) throw Error("Choose an id or --all");
        for (const letter of await store.deadLetters())
          await store.replayDeadLetter(letter.id);
        out("Replayed all dead letters");
        return 0;
      }
      if (!argument) throw Error("Dead-letter id is required");
      if (action === "replay") {
        await store.replayDeadLetter(argument);
        out(`Replayed ${argument}`);
        return 0;
      }
      if (action === "discard") {
        await store.discardDeadLetter(argument);
        (config.logger ?? createLogger()).info(
          { component: "store", deadLetterId: argument },
          "Discarded dead letter",
        );
        out(`Discarded ${argument}`);
        return 0;
      }
      throw Error(`Unknown dead-letters action ${action}`);
    }
    if (command === "workflows") {
      const api = application[workflowsApi],
        store = config.store,
        id = positionals[3];
      if (!store.listWorkflows || !store.loadWorkflow || !store.purgeWorkflows)
        throw Error("This Event store does not keep Workflows");
      if (action === "list") {
        const statuses = ["running", "completed", "failed", "cancelled", "drifted"] as const;
        if (values.status !== undefined && !statuses.includes(values.status as never))
          throw Error("--status takes running, completed, failed, cancelled or drifted");
        out(JSON.stringify(await store.listWorkflows(values.status as (typeof statuses)[number]), null, 2));
        return 0;
      }
      if (action === "purge") {
        const before = values["finished-before"];
        if (!before || Number.isNaN(parseTime(before)))
          throw Error("purge takes --finished-before <date>");
        out(`Purged ${await store.purgeWorkflows(new Date(before).toISOString())} Workflow instances`);
        return 0;
      }
      if (!argument || !id) throw Error(`Usage: event-fabric ${usage[command]}`);
      if (action === "inspect") {
        const record = await store.loadWorkflow(workflowGroup(argument), id);
        if (!record) throw Error(`No Workflow ${argument} ${id}`);
        out(JSON.stringify(record, null, 2));
        return 0;
      }
      if (action === "cancel" || action === "resume") {
        await api[action](argument, id);
        out(`${action === "cancel" ? "Cancel" : "Resume"} requested for ${argument} ${id}`);
        return 0;
      }
      throw Error(`Unknown workflows action ${action}`);
    }
    if (command === "consumers") {
      if (action === "status") {
        const entries = await application.consumerStatus();
        out(
          values.json
            ? JSON.stringify(entries, null, 2)
            : entries
                .map(
                  (entry: any) =>
                    `${entry.group}\t${entry.state}\tcheckpoint=${JSON.stringify(entry.checkpoint)}\tlagMs=${entry.lagMs ?? "unknown"}\towner=${entry.lease?.owner ?? "-"}${entry.eventId ? `\tevent=${entry.eventId} attempts=${entry.attempts ?? 0} error=${entry.error ?? ""}` : ""}${entry.waitingOn?.length ? `\twaiting=${entry.waitingOn.join(",")}` : ""}`,
                )
                .join("\n"),
        );
        return 0;
      }
      if (!argument) throw Error("Consumer group is required");
      if (action === "retry") {
        await config.store.retryConsumer(argument);
        out(`Retry requested for ${argument}`);
        return 0;
      }
      const confirm =
        io.confirm ??
        (async (question: string) => {
          const prompt = createInterface({
            input: process.stdin,
            output: process.stdout,
          });
          try {
            return /^y(?:es)?$/i.test((await prompt.question(question)).trim());
          } finally {
            prompt.close();
          }
        });
      if (action === "retire") {
        const question = `Retire ${argument}? This deletes its Checkpoints and its Saga state or Workflow instances; Commands it already recorded stay in the Outbox. [y/N] `;
        if (!values.yes && !(await confirm(question))) {
          out("Retire canceled");
          return 1;
        }
        await application.retireConsumer(argument);
        out(`Retired ${argument}`);
        return 0;
      }
      if (action === "skip") {
        if (!values.event)
          throw Error("Specify the exact halted Event with --event <id>");
        const question = `Skip Event ${values.event} for ${argument}? This advances its Checkpoint and loses that Reaction. [y/N] `;
        if (!values.yes && !(await confirm(question))) {
          out("Skip canceled");
          return 1;
        }
        await application.skipConsumer(
          argument,
          values.event,
          values.who ?? userInfo().username,
        );
        out(`Skipped ${values.event} for ${argument}`);
        return 0;
      }
      throw Error(`Unknown consumers action ${action}`);
    }
    throw Error(`Unsupported command ${command}`);
  } catch (error) {
    err(error instanceof Error ? error.message : String(error));
    return 1;
  } finally {
    if (application) await application.close();
    else if (config) {
      const results = await Promise.allSettled(
        [
          ...new Set([
            config.store,
            config.transport,
            config.readmodelStore,
            ...Object.values(config.readmodels ?? {}).map(
              (group) => group.store,
            ),
          ]),
        ]
          .filter((port) => port !== undefined)
          .map((port) => port.close()),
      );
      const failed = results.find((result) => result.status === "rejected");
      if (failed?.status === "rejected") {
        err(
          failed.reason instanceof Error
            ? failed.reason.message
            : String(failed.reason),
        );
        return 1;
      }
    }
  }
}
