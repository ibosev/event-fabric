import { readdir, readFile, stat } from "node:fs/promises";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

/** The composition root's files, rendered from `templates/migrate-app/` beside the package's `dist/`. */
const templates = fileURLToPath(new URL("../../templates/migrate-app/", import.meta.url));
const packageRoot = fileURLToPath(new URL("../../", import.meta.url));

export type CompositionInput = {
  /** The old framework's package, as the sources import it. */
  framework: string;
  /** EventFabric's package name in the Application (`event-fabric-next` beside the old one). */
  next: string;
  /** The package.json dependency on EventFabric; default a `file:` link to this package. */
  dependency?: string;
};

const listing = async (folder: string, at = ""): Promise<string[]> =>
  (await Promise.all((await readdir(join(folder, at), { withFileTypes: true })).map((entry) =>
    entry.isDirectory() ? listing(folder, `${at}${entry.name}/`) : [`${at}${entry.name}`]))).flat();
const folders = async (path: string) =>
  (await readdir(path, { withFileTypes: true }).catch(() => [])).filter((entry) => entry.isDirectory()).map((entry) => entry.name).sort();
const quote = (value: string) => (/^[A-Za-z_$][\w$]*$/.test(value) ? value : `'${value}'`);

/**
 * The composition root an Application in the old layout needs on EventFabric, by path: the config, the thin
 * `migration/` modules over EventFabric's compat loaders (with empty decision tables), the type and check configs,
 * and `package.json` with the EventFabric dependency. The Application's decisions come as patches over these.
 */
export const compositionRoot = async (root: string, input: CompositionInput): Promise<Record<string, string>> => {
  const pkg = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
  const aliases: [string, string][] = Object.entries<string>(pkg._moduleAliases ?? {}).map(([alias, target]) => [alias, target.replace(/^\.\//, "")]);
  const directories = await Promise.all(aliases.map(async ([, target]) => (await stat(join(root, target)).catch(() => undefined))?.isDirectory() ?? false));
  const jsconfig = await readFile(join(root, "jsconfig.json"), "utf8").then((text) => JSON.parse(text), () => ({}));
  const gateways = (await Promise.all((await folders(join(root, "app/gateways"))).map(async (kind) =>
    (await folders(join(root, "app/gateways", kind))).map((name) => `['${kind}', '${name}']`)))).flat();
  const values: Record<string, string> = {
    next: input.next,
    framework: input.framework,
    name: pkg.name,
    // Database and schema names: the package name in identifier characters.
    database: String(pkg.name).replace(/\W/g, "_"),
    aliases: `{ ${aliases.map(([alias, target]) => `${quote(alias)}: path.join(local.root, '${target}')`).join(", ")} }`,
    gateways: `[${gateways.join(", ")}]`,
    paths: `{\n${aliases.map(([alias, target], index) => directories[index] ? `\t\t\t"${alias}/*": ["./${target}/*"]` : `\t\t\t"${alias}": ["./${target}"]`).join(",\n")}\n\t\t}`,
    exclude: JSON.stringify(jsconfig.exclude ?? ["node_modules"]).replaceAll(",", ", "),
  };
  const files: Record<string, string> = {};
  for (const file of await listing(templates))
    files[file] = (await readFile(join(templates, file), "utf8")).replace(/%%(\w+)%%/g, (_, key: string) => {
      if (!(key in values)) throw Error(`templates/migrate-app/${file}: unknown value ${key}`);
      return values[key]!;
    });
  // The dependency sits beside the old framework until cutover, where it replaces it.
  const section = input.next === "event-fabric" ? "dependencies" : "optionalDependencies";
  pkg[section] = { ...pkg[section], [input.next]: input.dependency ?? `file:${relative(root, packageRoot) || "."}` };
  if (section === "dependencies") {
    delete pkg.optionalDependencies?.["event-fabric-next"];
    if (pkg.optionalDependencies && !Object.keys(pkg.optionalDependencies).length) delete pkg.optionalDependencies;
  }
  pkg.engines = { ...pkg.engines, node: ">=24.15" };
  files["package.json"] = `${JSON.stringify(pkg, null, "\t")}\n`;
  return files;
};
