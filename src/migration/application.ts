import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { compositionRoot } from "./composition.js";
import { migrateSource, parseSource } from "./index.js";

export type ApplicationMigrationOptions = {
  /** `coexistence` (default) imports `event-fabric-next`; `cutover` imports `event-fabric`. */
  packages?: "coexistence" | "cutover";
  /** Report what would change and write nothing. */
  check?: boolean;
  /**
   * A folder of unified diffs (`*.patch`, applied in name order after the rules): the Application's own decisions that
   * no convention supplies. A hunk whose lines are found neither before nor after the change stops the run.
   */
  patches?: string;
  /** The package.json dependency on EventFabric; default a `file:` link to the running package. */
  dependency?: string;
};
export type ApplicationMigrationResult = {
  files: { file: string; kind: string; changed: boolean; reviews: string[]; patched?: boolean; deleted?: boolean }[];
  /** The old framework's package, as the sources import it. */
  framework?: string;
  events: number;
};

/** The source folders of an old-framework Application; Tasks (`app/tasks/`) run unchanged on `event-fabric task`. */
const sourceFolders = ["app", "lib", "plugins"];
const files = async (root: string, folder: string): Promise<string[]> => {
  const entries = await readdir(join(root, folder), { withFileTypes: true }).catch(() => []);
  const found = await Promise.all(
    entries.map((entry) => {
      const path = `${folder}/${entry.name}`;
      return entry.isDirectory() ? files(root, path) : entry.isFile() && path.endsWith(".js") ? [path] : [];
    }),
  );
  return found.flat();
};
/** Each file's kind, from its folder: `app/domain/<context>/*.js` Aggregates, `app/sagas/<group>/*.js` Sagas, and so on. */
const kindOf = (file: string) => {
  const parts = file.split("/"), [top, folder] = parts, grouped = parts.length === 4;
  if (top !== "app") return top!;
  if (folder === "domain" && grouped) return "aggregate";
  if (folder === "sagas" && grouped) return "saga";
  if (folder === "readmodels" && grouped) return "readmodel";
  return folder === "gateways" ? "gateway" : folder === "tasks" ? "task" : folder!;
};

const nameOf = (node: any): string | undefined =>
  node?.type === "Identifier" ? node.name : typeof node?.value === "string" ? node.value : undefined;
/** The Event names an Aggregate file declares: the keys of its exported `events` map, read without running it. */
const declaredEvents = (program: any, file: string): string[] => {
  const declared = (name: string) =>
    program.body
      .flatMap((statement: any) => (statement.type === "VariableDeclaration" ? statement.declarations : []))
      .find((declarator: any) => declarator.id.name === name)?.init;
  const object = (node: any): any => (node?.type === "Identifier" ? object(declared(node.name)) : node);
  const exported = program.body.find(
    (statement: any) =>
      statement.expression?.type === "AssignmentExpression" &&
      statement.expression.left.object?.name === "module" &&
      nameOf(statement.expression.left.property) === "exports",
  )?.expression.right;
  const events = object(
    object(exported)?.properties?.find((property: any) => nameOf(property.key) === "events")?.value,
  );
  if (events?.type !== "ObjectExpression") throw Error(`${file}: no exported events map`);
  const keys = (map: any): string[] =>
    map.properties.flatMap((property: any) => {
      if (property.type === "SpreadElement") return keys(object(property.argument));
      const key = !property.computed && nameOf(property.key);
      if (!key) throw Error(`${file}: an Event key that is not a name`);
      return [key];
    });
  return keys(events);
};
/**
 * Applies a unified diff to the given sources, exactly; a hunk already applied is skipped. A `--- /dev/null` section
 * creates its file, a `+++ /dev/null` section empties it for deletion (a file already gone is skipped).
 */
const applyPatch = (patch: string, name: string, read: (file: string) => string, write: (file: string, source: string) => void, exists: (file: string) => boolean) => {
  for (const section of patch.split(/^(?=--- )/m).filter((part) => part.startsWith("--- "))) {
    const target = /^\+\+\+ (?:b\/)?(.+)$/m.exec(section)?.[1]?.trim();
    if (!target) throw Error(`${name}: a file section without a +++ line`);
    const file = target === "/dev/null" ? /^--- (?:a\/)?(.+)$/m.exec(section)![1]!.trim() : target;
    if (!exists(file)) continue;
    const lines = read(file).split("\n");
    let offset = 0;
    for (const hunk of section.split(/^(?=@@ )/m).slice(1)) {
      // `-N,0` names the line the new lines follow (`diff -U0`), any other range its first line.
      const [, line, count] = /^@@ -(\d+)(?:,(\d+))?/.exec(hunk)!;
      const start = (count === "0" ? Number(line) : Math.max(Number(line) - 1, 0)) + offset, before: string[] = [], after: string[] = [];
      for (const line of hunk.split("\n").slice(1)) {
        if (line.startsWith(" ") || line.startsWith("-")) before.push(line.slice(1));
        if (line.startsWith(" ") || line.startsWith("+")) after.push(line.slice(1));
      }
      const at = (expected: string[]) => expected.every((line, index) => lines[start + index] === line);
      // A hunk without old lines (a created file) is applied where its new lines are not there yet.
      if (before.length ? at(before) : !at(after)) lines.splice(start, before.length, ...after);
      else if (!at(after)) throw Error(`${name}: ${file} no longer matches the hunk at line ${start + 1 - offset}`);
      offset += after.length - before.length;
    }
    write(file, lines.join("\n"));
  }
};
/** The package a `require(…)` call names, when it is one. */
const required = (node: any): string | undefined =>
  node?.type === "CallExpression" && node.callee.name === "require" && typeof node.arguments[0]?.value === "string"
    ? node.arguments[0].value
    : undefined;
const walk = (node: any, visit: (node: any) => void) => {
  if (!node || typeof node.type !== "string") return;
  visit(node);
  for (const [key, value] of Object.entries(node))
    if (key !== "parent")
      for (const child of Array.isArray(value) ? value : [value])
        if (child && typeof child === "object") walk(child, visit);
};

/**
 * Migrates an old-framework Application in place, by convention: it finds the sources, the Aggregates' Event names,
 * the Sagas and the Gateways from the folder layout, and the old framework's package from the imports, then runs
 * `migrateSource` on every file. A second run changes nothing.
 */
export const migrateApplication = async (
  root: string,
  options: ApplicationMigrationOptions = {},
): Promise<ApplicationMigrationResult> => {
  const paths = (await Promise.all(sourceFolders.map((folder) => files(root, folder)))).flat().sort();
  const sources = await Promise.all(
    paths
      .filter((file) => kindOf(file) !== "task")
      .map(async (file) => {
        const source = await readFile(join(root, file), "utf8");
        return { file, kind: kindOf(file), source, program: (await parseSource(source)).program };
      }),
  );
  // Event-name constants: domain.<context>.<aggregate>.<event>, also under the generated module's `events`.
  const symbols: Record<string, string> = {};
  for (const { file, kind, program } of sources) {
    if (kind !== "aggregate") continue;
    const [, , context, base] = file.split("/");
    const aggregate = base!.split(".")[0];
    for (const event of [...declaredEvents(program, file), "commandRejected"]) {
      const name = `${context}.${aggregate}.${event}`;
      symbols[`domain.${name}`] = symbols[`events.domain.${name}`] = name;
    }
  }
  // The old framework is the package whose toolkit the sources import: require('<package>').tools or '<package>/lib/tools…'.
  const counts = new Map<string, number>();
  for (const { program } of sources)
    walk(program, (node) => {
      const name = required(node);
      const toolkit = name?.match(/^((?:@[^/]+\/)?[^/]+)\/lib\/tools(\/|$)/)?.[1] ??
        (name && node.parent?.type === "MemberExpression" && nameOf(node.parent.property) === "tools" ? name : undefined);
      if (toolkit) counts.set(toolkit, (counts.get(toolkit) ?? 0) + 1);
    });
  const framework = [...counts].sort((a, b) => b[1] - a[1])[0]?.[0];
  const next = options.packages === "cutover" ? "event-fabric" : "event-fabric-next";
  const replacements: Record<string, string> = {};
  const own = new Map<string, Record<string, string>>();
  if (framework) {
    // Gateway routes keep the old REST toolkit, served by legacyGateway.
    const legacyRest = `require('${next}/compat').legacyRest`;
    replacements[`require('${framework}').tools.rest`] = legacyRest;
    replacements[`require('${framework}/lib/tools/rest')`] = legacyRest;
    // A one-line import of the old `only` guards takes EventFabric's.
    for (const { source, program } of sources)
      walk(program, (node) => {
        let init = node.type === "VariableDeclarator" ? node.init : undefined;
        while (init?.type === "MemberExpression") init = init.object;
        const text = source.slice(...(node.range as [number, number]));
        if (required(init)?.split("/lib/")[0] === framework && /^\{.*\bonly\b/.test(text) && !text.includes("\n"))
          replacements[text] = `{ only } = require('${next}')`;
      });
    // `const { tools } = require('<old>'); const { only } = tools.domain.command;`, where `tools` serves only that.
    for (const { file, source, program } of sources) {
      const statements = program.body.filter((statement: any) => statement.type === "VariableDeclaration" && statement.declarations.length === 1);
      const toolkit = statements.find((statement: any) => {
        const [{ id, init }] = statement.declarations;
        return required(init) === framework && id.type === "ObjectPattern" && id.properties.length === 1 && nameOf(id.properties[0].key) === "tools" && id.properties[0].value?.name === "tools";
      });
      const guards = statements.find((statement: any) => {
        const [{ id, init }] = statement.declarations;
        return id.type === "ObjectPattern" && id.properties.length === 1 && nameOf(id.properties[0].key) === "only" && source.slice(...(init.range as [number, number])) === "tools.domain.command";
      });
      if (toolkit && guards && source.match(/\btools\b/g)!.length === 2)
        own.set(file, {
          [source.slice(...(toolkit.range as [number, number]))]: "",
          [source.slice(...(guards.range as [number, number]))]: `const { only } = require('${next}');`,
        });
    }
  }
  const results: (ApplicationMigrationResult["files"][number] & { source: string })[] = [];
  for (const { file, kind, source } of sources) {
    const fileOptions = {
      symbols,
      replacements: { ...replacements, ...own.get(file) },
      saga: kind === "saga",
      aggregate: kind === "aggregate",
      packages: options.packages ?? "coexistence",
      // The composition root supplies getCrud() through legacyApp, requestApp and legacyTaskApp.
      keepGetCrud: true,
      legacyGateways: kind === "gateway" || kind === "lib",
    };
    const result = await migrateSource(source, fileOptions);
    if ((await migrateSource(result.source, fileOptions)).source !== result.source)
      throw Error(`${file}: a second migration changes the file again; migration stopped before writing`);
    results.push({ file, kind, changed: result.source !== source, reviews: result.reviews, source: result.source });
  }
  // The composition root: rendered where a file is missing; package.json gains the dependency. At cutover, an
  // Application whose sources no longer import the old framework renames EventFabric's package in the root it has.
  const originals = new Map(sources.map(({ file, source }) => [file, source]));
  const cutover = options.packages === "cutover";
  if (framework || cutover) {
    const rendered = await compositionRoot(root, { framework: framework ?? "", next, ...(options.dependency && { dependency: options.dependency }) });
    // `migration/` holds the root's modules, the Application's own ones included.
    const modules = cutover ? (await readdir(join(root, "migration"), { recursive: true }).catch(() => [])).filter((file) => /\.(c?js|mjs|ts|json)$/.test(file)) : [];
    const composition = [...new Set([...Object.keys(rendered), ...modules.map((file) => `migration/${file}`)])];
    for (const file of composition) {
      const current = await readFile(join(root, file), "utf8").catch(() => undefined);
      let source = rendered[file];
      if (current !== undefined && file !== "package.json") {
        if (!cutover) continue;
        source = current.replace(/(['"])event-fabric-next(?=['"/])/g, "$1event-fabric");
      }
      if (source === undefined || (current === undefined && !framework)) continue;
      if (current !== undefined) originals.set(file, current);
      results.push({ file, kind: "composition", changed: source !== current, reviews: [], source });
    }
  }
  if (options.patches) {
    const byFile = new Map(results.map((result) => [result.file, result]));
    for (const name of (await readdir(options.patches)).filter((name) => name.endsWith(".patch")).sort()) {
      const patch = await readFile(join(options.patches, name), "utf8"), touched = new Set<string>();
      const created = new Set([...patch.matchAll(/^--- \/dev\/null\n\+\+\+ (?:b\/)?(.+)$/gm)].map((match) => match[1]!.trim()));
      const deleted = new Set([...patch.matchAll(/^--- (?:a\/)?(.+)\n\+\+\+ \/dev\/null$/gm)].map((match) => match[1]!.trim()));
      for (const match of patch.matchAll(/^--- (?:a\/)?(.+)\n\+\+\+ (?:b\/)?(.+)$/gm)) {
        const file = match[2]!.trim() === "/dev/null" ? match[1]!.trim() : match[2]!.trim();
        // A file the patch deletes and that is gone already was deleted by an earlier run.
        if (deleted.has(file) && !byFile.has(file) && (await readFile(join(root, file), "utf8").catch(() => undefined)) === undefined) continue;
        if (!byFile.has(file)) {
          // A file the patch creates (`--- /dev/null`) starts empty.
          const source = await readFile(join(root, file), "utf8").catch(() => undefined);
          if (source === undefined && !created.has(file)) throw Error(`${name}: ${file} does not exist`);
          if (source !== undefined) originals.set(file, source);
          byFile.set(file, { file, kind: kindOf(file), changed: false, reviews: [], source: source ?? "" });
          results.push(byFile.get(file)!);
        }
        touched.add(file);
      }
      applyPatch(patch, name, (file) => byFile.get(file)!.source, (file, source) => {
        byFile.get(file)!.source = source;
      }, (file) => touched.has(file));
      for (const file of touched) {
        const result = byFile.get(file)!;
        if (deleted.has(file) && result.source !== "") throw Error(`${name}: ${file} has lines the deletion does not list`);
        Object.assign(result, { patched: true, changed: deleted.has(file) || result.source !== originals.get(file), ...(deleted.has(file) && { deleted: true }) });
      }
    }
  }
  if (!options.check)
    for (const result of results)
      if (result.deleted) await rm(join(root, result.file), { force: true });
      else if (result.changed) {
        await mkdir(dirname(join(root, result.file)), { recursive: true });
        await writeFile(join(root, result.file), result.source);
      }
  return {
    files: results.map(({ source: _, ...result }) => result),
    ...(framework && { framework }),
    events: Object.keys(symbols).length / 2,
  };
};
