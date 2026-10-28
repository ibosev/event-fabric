import { readFile } from "node:fs/promises";
import type { Schema } from "../types.js";
export type SourceMigrationOptions = {
  symbols?: Record<string, string>;
  constantsModule?: string;
  replacements?: Record<string, string>;
  task?: boolean;
  /** The file holds Saga declarations: Events without an Identity keep the old per-Event instances, and the ignored `initialState` is removed. */
  saga?: boolean;
  /** `coexistence` (default) keeps `event-fabric-next` beside the vendored `event-fabric`; `cutover` renames it to `event-fabric`. */
  packages?: "coexistence" | "cutover";
  /**
   * Gateway route files are served by `legacyGateway`, with the old toolkit's import replaced by `legacyRest`:
   * command descriptors, Readmodel route helpers and `req.getApp()` stay as they are.
   */
  legacyGateways?: boolean;
  /** `getCrud()` stays as it is, for an Application whose `legacyApp`, `requestApp` and `legacyTaskApp` supply it. */
  keepGetCrud?: boolean;
  /** The file declares an Aggregate, so every `<app>.getLogger()` in it is a Command context's: it becomes `<app>.logger`. */
  aggregate?: boolean;
};
/** Parses a module or, failing that, a CommonJS file, with each node's `range`; throws on syntax errors. */
export const parseSource = async (source: string): Promise<{ program: any; scopeManager: any }> => {
  const { Linter } = await import("eslint");
  let program: any, scopeManager: any;
  const parse = (sourceType: "module" | "commonjs") => new Linter().verify(source, [
    {
      languageOptions: { sourceType },
      plugins: {
        migration: {
          rules: {
            capture: {
              create: (context: any) => ({
                Program: (node: any) => {
                  program = node;
                  scopeManager = context.sourceCode.scopeManager;
                },
              }),
            },
          },
        },
      },
      rules: { "migration/capture": "error" },
    },
  ]);
  let diagnostics = parse("module");
  if (diagnostics.some((diagnostic) => diagnostic.fatal))
    diagnostics = parse("commonjs");
  if (!program || diagnostics.some((diagnostic) => diagnostic.fatal))
    throw Error("Source has syntax errors; migration stopped");
  return { program, scopeManager };
};
/** Runs only on source supplied by the caller; inspect the returned review list before writing a copy. An empty list means no known pattern was found, not that the copy works: run its tests. */
export const migrateSource = async (
  source: string,
  options: SourceMigrationOptions = {},
): Promise<{ source: string; reviews: string[] }> => {
  const { program, scopeManager } = await parseSource(source);
  const exported = Object.keys(
    JSON.parse(
      await readFile(new URL("../../package.json", import.meta.url), "utf8"),
    ).exports,
  );
  const edits: { start: number; end: number; text: string }[] = [],
    reviews = new Set<string>();
  let taskApp: string | undefined, taskParameter: string | undefined;
  // The identifiers that reference the Task's parameters; a nested scope may reuse their names.
  const taskApps = new Set<any>(),
    taskParameters = new Set<any>();
  const isTaskApp = (node: any) => taskApps.has(node);
  const rootOf = (node: any): any =>
    node.type === "MemberExpression" ? rootOf(node.object) : node;
  const textOf = (node: any) => source.slice(node.range[0], node.range[1]);
  const replace = (node: any, text: string) =>
    edits.push({ start: node.range[0], end: node.range[1], text });
  const skip = new Set<any>();
  // Grouped identity and query entries moved into Reaction sets.
  const moved = new Set<any>();
  const stringOf = (node: any): string | undefined =>
    node?.type === "Literal" && typeof node.value === "string"
      ? node.value
      : node?.type === "TemplateLiteral" && !node.expressions.length
        ? node.quasis[0].value.cooked
        : undefined;
  // A computed string key, `['a.b.c']` (as the Event-name replacement writes them), is the same key.
  const keyOf = (property: any): string | undefined =>
    property.type !== "Property"
      ? undefined
      : property.computed
        ? stringOf(property.key)
        : (property.key.name ?? property.key.value);
  // Generated names contain only name characters, so the source quote is reused unescaped.
  const quote = (node: any, value: string) =>
    `${textOf(node)[0]}${value}${textOf(node)[0]}`;
  // Migrated code keeps old behavior through `event-fabric/compat` helpers; one import is added for all of them.
  const compat = new Set<string>();
  const useCompat = (name: string) => {
    if (!/^event-fabric(?:-next)?\/compat$/.test(bindings.get(name) ?? ""))
      compat.add(name);
    return name;
  };
  // Where each top-level binding was imported from: `only` from the new package is the new marker.
  const bindings = new Map<string, string>();
  for (const statement of program.body) {
    if (statement.type === "ImportDeclaration")
      for (const specifier of statement.specifiers)
        bindings.set(specifier.local.name, statement.source.value);
    for (const declarator of statement.type === "VariableDeclaration"
      ? statement.declarations
      : []) {
      const init =
        declarator.init?.type === "MemberExpression"
          ? declarator.init.object
          : declarator.init;
      if (init?.type !== "CallExpression" || init.callee.name !== "require")
        continue;
      for (const name of declarator.id.type === "ObjectPattern"
        ? declarator.id.properties.map((property: any) => property.value?.name)
        : [declarator.id.name])
        if (name) bindings.set(name, String(init.arguments[0]?.value));
    }
  }
  const fromNewPackage = (name: string) =>
    new RegExp(
      `^event-fabric${options.packages === "cutover" ? "(-next)?" : "-next"}(/|$)`,
    ).test(bindings.get(name) ?? "");
  const oldIfNotExists = (node: any) =>
    node?.type === "CallExpression" &&
    !node.arguments.length &&
    node.callee.type === "MemberExpression" &&
    !node.callee.computed &&
    node.callee.property.name === "ifNotExists" &&
    node.callee.object.type === "Identifier" &&
    !fromNewPackage(node.callee.object.name);
  // The nearest enclosing property or variable named one of `keys`.
  const within = (node: any, keys: string[]) => {
    for (let parent = node.parent; parent; parent = parent.parent) {
      const name =
        parent.type === "Property"
          ? keyOf(parent)
          : parent.type === "VariableDeclarator"
            ? parent.id.name
            : undefined;
      if (keys.includes(name ?? "")) return name;
    }
  };
  // An Event parameter: the first parameter of a Reaction, Event handler, Identity or enricher.
  const isEventParameter = (node: any) => {
    for (let fn = functionOf(node); fn; fn = functionOf(fn))
      if (fn.params.some((param: any) => param.name === node.name))
        return (
          fn.params[0]?.name === node.name &&
          !!within(fn, ["reactions", "events", "identity", "eventEnricher"])
        );
    return false;
  };
  const functionOf = (node: any): any => {
    for (let parent = node.parent; parent; parent = parent.parent)
      if (/Function/.test(parent.type)) return parent;
  };
  // A Rule's State parameter: one of the first two parameters of a function in `rules` or passed to `after`.
  const isRuleState = (node: any) => {
    for (let fn = functionOf(node); fn; fn = functionOf(fn)) {
      const rule = within(fn, ["rules"]) ||
        (fn.parent?.type === "CallExpression" && fn.parent.callee.property?.name === "after");
      if (rule && fn.params.slice(0, 2).some((param: any) => param.name === node.name) &&
        scopeManager.getDeclaredVariables(fn).some((variable: any) =>
          variable.references.some((reference: any) => reference.identifier === node),
        ))
        return true;
    }
    return false;
  };
  // `getDomain().<context>.<aggregate>(id)` outside a Task, whose read gets `.load()`.
  const isDomainHandle = (node: any) =>
    node?.type === "CallExpression" &&
    node.callee.type === "MemberExpression" &&
    node.callee.object.type === "MemberExpression" &&
    node.callee.object.object.type === "CallExpression" &&
    node.callee.object.object.callee.property?.name === "getDomain" &&
    !node.callee.object.object.arguments.length &&
    !isTaskApp(node.callee.object.object.callee.object);
  const spread = (nodes: any[]) =>
    nodes.some((node) => node.type === "SpreadElement");
  // Removes a property or element together with its separating comma.
  const removeItem = (node: any) => {
    let start = node.range[0],
      end = node.range[1];
    const trailing = /^\s*,\s*/.exec(source.slice(end));
    // Line comments directly above the item go with it.
    const comments = /(\/\/[^\n]*\n[ \t]*)+$/.exec(source.slice(0, start));
    const lineStart = source.lastIndexOf("\n", start - 1) + 1,
      ownLine = /^[ \t]*,[ \t]*\n/.exec(source.slice(end));
    if (
      trailing &&
      !comments &&
      ownLine &&
      /^[ \t]*$/.test(source.slice(lineStart, start))
    ) {
      // An item on its own line goes with its line.
      start = lineStart;
      end += ownLine[0].length;
    } else if (trailing) {
      end += trailing[0].length;
      if (comments) start = comments.index;
    } else {
      const preceding = /,\s*$/.exec(source.slice(0, start));
      if (preceding) start = preceding.index;
    }
    edits.push({ start, end, text: "" });
  };
  const indentOf = (node: any) =>
    source
      .slice(source.lastIndexOf("\n", node.range[0] - 1) + 1, node.range[0])
      .match(/^[\t ]*/u)![0];
  const children = (node: any): any[] =>
    Object.entries(node).flatMap(([key, value]) =>
      ["parent", "tokens", "comments"].includes(key)
        ? []
        : (Array.isArray(value) ? value : [value]).filter(
            (child) => child && typeof child.type === "string",
          ),
    );
  const temporaryFields = new Set<string>();
  const collectTemporaryFields = (node: any) => {
    const target = node.type === "AssignmentExpression" ? node.left : undefined;
    if (
      target?.type === "MemberExpression" && !target.computed &&
      target.object.type === "MemberExpression" &&
      target.object.property.name === "_aggregateModel"
    ) temporaryFields.add(target.property.name);
    children(node).forEach(collectTemporaryFields);
  };
  collectTemporaryFields(program);
  // The `<x>.getReadmodels()` call `node` is, or holds as a `const` (`const rms = req.getApp().getReadmodels()`).
  const readmodelsCall = (node: any): any => {
    if (node?.type === "Identifier") {
      const definition = variableOf(node)?.defs[0];
      return definition?.type === "Variable" && definition.parent.kind === "const" && definition.node.id === definition.name
        ? readmodelsCall(definition.node.init)
        : undefined;
    }
    return node?.type === "CallExpression" && node.callee.type === "MemberExpression" && node.callee.property.name === "getReadmodels"
      ? node
      : undefined;
  };
  // `<x>.getReadmodels().<group>.<model>` when `node` calls it, as the old QueryBuilder did.
  const repositoryOf = (node: any): string | undefined => {
    const model = node?.type === "CallExpression" ? node.callee : undefined,
      readmodels = model?.object?.object,
      call = readmodelsCall(readmodels);
    return model?.type === "MemberExpression" && model.object.type === "MemberExpression" && call
      ? `${call === readmodels && isTaskApp(call.callee.object) ? "ctx.readmodels" : textOf(readmodels)}${source.slice(readmodels.range[1], model.range[1])}`
      : undefined;
  };
  const taskConfig = (text: string, now: string) =>
    `${text}: now ${now}; ctx.config is the EventFabric config, so the Application's own settings (such as a database URI) read undefined from it; give them to an Application-owned service in setupServices, or read them from the environment, by hand`;
  const queryContract =
    "await <model>(id) now resolves to { document, position, behind } (document null when absent, never an array); queries are <model>.find(filter, options) resolving to an array (no toArray), .findOne, .count and .stream (an AsyncIterable, not a Node stream), with portable filter operators. Translate the chain and every use of its result by hand";
  const find = (node: any, match: (node: any) => boolean): any =>
    match(node)
      ? node
      : children(node).reduce(
          (found, child) => found ?? find(child, match),
          undefined,
        );
  const all = (node: any, match: (node: any) => boolean): any[] => [
    ...(match(node) ? [node] : []),
    ...children(node).flatMap((child) => all(child, match)),
  ];
  // The map a function sits in directly (in an Reaction array or a call there too): `events`, `reactions` or `commands`.
  const handlerMap = (fn: any): string | undefined => {
    for (let parent = fn.parent; parent; parent = parent.parent) {
      if (/Function/.test(parent.type)) return undefined;
      const name = parent.type === "Property" ? keyOf(parent) : parent.type === "VariableDeclarator" ? parent.id.name : undefined;
      if (["events", "reactions", "commands"].includes(name ?? "")) return name;
    }
  };
  // Removes a statement on its own line; between two blank lines, one of them goes too.
  const removeLine = (node: any) => {
    const start = source.lastIndexOf("\n", node.range[0] - 1) + 1;
    let end = source.indexOf("\n", node.range[1]) + 1 || source.length;
    const blank = /^[ \t]*\n/.exec(source.slice(end));
    if (blank && /\n[ \t]*\n$/.test(source.slice(0, start))) end += blank[0].length;
    edits.push({ start, end, text: "" });
  };
  // The variable an identifier refers to, through the enclosing scopes.
  const variableOf = (identifier: any) => {
    let node = identifier, scope: any;
    while (node && !(scope = scopeManager.acquire(node, true))) node = node.parent;
    for (; scope; scope = scope.upper) {
      const variable = scope.set.get(identifier.name);
      if (variable) return variable;
    }
  };
  // The old framework's process entry forked for a Task (`fork('<root>/distributed.js', ['task', file, …args])`): EventFabric's
  // CLI runs the Task, with `--` before its arguments, and the child gets this process's environment, whose settings
  // EventFabric reads, instead of only the old environment selector.
  const migrateTaskFork = (node: any) => {
    const [entry, args, settings] = node.arguments;
    const variable = entry?.type === "Identifier" ? variableOf(entry) : undefined,
      declarator = variable?.defs[0]?.node;
    const path = declarator?.type === "VariableDeclarator" ? declarator.init : undefined;
    const file = stringOf(path) ?? (path?.type === "TemplateLiteral" ? path.quasis.at(-1).value.cooked : undefined);
    if (!file?.endsWith("/distributed.js") || declarator.parent.declarations.length !== 1 || args?.type !== "ArrayExpression") return;
    const q = textOf(args.elements[0])[0],
      name = /\bcli\b/.test(source) ? entry.name : "cli",
      next = `event-fabric${options.packages === "cutover" ? "" : "-next"}`;
    replace(declarator.parent, `// The Task runs on EventFabric's Task runner, with this process's environment.\n${indentOf(declarator.parent)}${declarator.parent.kind} ${name} = require(${q}node:path${q}).join(require(${q}node:path${q}).dirname(require.resolve(${q}${next}${q})), ${q}cli/main.js${q});`);
    for (const reference of variable.references)
      if (reference.identifier !== declarator.id) replace(reference.identifier, name);
    if (args.elements.length > 2) edits.push({ start: args.elements[1].range[1], end: args.elements[1].range[1], text: `, ${q}--${q}` });
    const env = settings?.type === "ObjectExpression" ? settings.properties.find((property: any) => keyOf(property) === "env")?.value : undefined;
    const selected = env?.type === "Identifier" ? variableOf(env)?.defs[0]?.node.init : env;
    if (selected?.type === "ObjectExpression" && selected.properties.every((property: any) =>
      property.type === "Property" && !property.computed && property.value.type === "MemberExpression" &&
      textOf(property.value) === `process.env.${keyOf(property)}`)) replace(selected, "process.env");
  };
  const timestampHandlers = new Set<any>();
  // The Event's commit time left metadata: an Aggregate Event handler reads `timestamp` from the Event.
  const migrateEventTime = (fn: any, map: string) => {
    const [first] = fn.params;
    const metadata = map === "events" && first?.type === "ObjectPattern"
      ? first.properties.find((property: any) => keyOf(property) === "metadata" && property.value?.type === "Identifier")
      : undefined;
    const reads: any[] = [], unpacks: any[] = [];
    let used = false;
    for (const reference of (metadata && scopeManager.getDeclaredVariables(fn).find((variable: any) => variable.name === metadata.value.name)?.references) ?? []) {
      const at = reference.identifier.parent;
      if (at.type === "MemberExpression" && at.object === reference.identifier && !at.computed && at.property.name === "timestamp" &&
        !(at.parent.type === "AssignmentExpression" && at.parent.left === at)) reads.push(at);
      else if (at.type === "VariableDeclarator" && at.init === reference.identifier && at.id.type === "ObjectPattern" &&
        at.id.properties.length === 1 && keyOf(at.id.properties[0]) === "timestamp" && at.id.properties[0].value?.name === "timestamp" &&
        at.parent.declarations.length === 1) unpacks.push(at.parent);
      else used = true;
    }
    if (!reads.length && !unpacks.length) return;
    const declares = (name: string, kept: (definition: any) => boolean) => {
      const search = (scope: any): boolean =>
        scope.variables.some((variable: any) => variable.name === name && !variable.defs.every(kept)) || scope.childScopes.some(search);
      return search(scopeManager.acquire(fn));
    };
    if (first?.type === "ObjectPattern") {
      const own = first.properties.find((property: any) => keyOf(property) === "timestamp" && property.value?.name === "timestamp");
      const last = first.properties.at(-1);
      if (declares("timestamp", (definition) => unpacks.includes(definition.parent) || definition.name.parent === own) || last?.type === "RestElement") return;
      for (const read of reads) replace(read, "timestamp");
      for (const unpack of unpacks) removeLine(unpack);
      if (!own)
        if (metadata && !used && (reads.length || unpacks.length)) replace(metadata, "timestamp");
        else edits.push({ start: (metadata ?? last).range[1], end: (metadata ?? last).range[1], text: ", timestamp" });
    } else return;
    timestampHandlers.add(fn);
    for (const node of reads) skip.add(node);
  };
  const unwrapFilters = (node: any): void => {
    if (
      node.type === "ObjectExpression" &&
      node.properties.some((property: any) => keyOf(property) === "filter")
    ) {
      const [wrapper] = node.properties;
      if (
        node.properties.length === 1 &&
        !wrapper.method &&
        wrapper.value.type === "ObjectExpression"
      ) {
        edits.push(
          { start: node.range[0], end: wrapper.value.range[0], text: "" },
          { start: wrapper.value.range[1], end: node.range[1], text: "" },
        );
      } else
        reviews.add(
          `access readmodels filter: ${textOf(node)} ${node.properties.length > 1 ? "mixes the filter wrapper with other keys; merge them into one plain { path: value } filter by hand" : "is not an object literal, so it cannot be told apart from a document field named filter; unwrap by hand"}`,
        );
      return;
    }
    children(node).forEach(unwrapFilters);
  };
  const migrateAccess = (node: any) => {
    if (node.properties.some((property: any) => !keyOf(property))) {
      reviews.add(
        "access: the Access map has spread or computed keys; add domain: { '*': true } by hand, since Command rules were not enforced before migration",
      );
      return;
    }
    const section = (name: string) =>
      node.properties.find((property: any) => keyOf(property) === name);
    if (section("*"))
      reviews.add(
        "access '*': a top-level default grants nothing now, since only access.readmodels and access.domain are read; move the Readmodel default into readmodels by hand. Reads and Notifications are now filtered, so rows the old unfiltered queries showed may disappear; fix the caller rather than widening the rule",
      );
    const readmodels = section("readmodels")?.value;
    for (const rule of readmodels?.type === "ObjectExpression"
      ? readmodels.properties
      : []) {
      if (keyOf(rule) === "*" && rule.value?.value === true)
        reviews.add(
          "access readmodels '*': true opens every Readmodel; tighten after cutover",
        );
      if (rule.value) unwrapFilters(rule.value);
    }
    const domain = section("domain"),
      last = node.properties.at(-1);
    if (!domain) {
      if (!last) return replace(node, "{ domain: { '*': true } }");
      const line = source.slice(
        source.lastIndexOf("\n", last.range[0] - 1) + 1,
        last.range[0],
      );
      edits.push({
        start: last.range[1],
        end: last.range[1],
        text: `${/^\s*$/.test(line) ? `,\n${line}` : ", "}domain: { '*': true }`,
      });
      return;
    }
    const rules = domain.value;
    if (rules.type !== "ObjectExpression")
      return void reviews.add(
        "access domain: the section is not an object literal; set domain: { '*': true } by hand, since Command rules were not enforced before migration",
      );
    if (
      rules.properties.length === 1 &&
      keyOf(rules.properties[0]) === "*" &&
      rules.properties[0].value.value === true
    )
      return;
    skip.add(rules);
    if (!rules.properties.length) return replace(rules, "{ '*': true }");
    const indent = source
        .slice(
          source.lastIndexOf("\n", domain.range[0] - 1) + 1,
          domain.range[0],
        )
        .match(/^[\t ]*/u)![0],
      inner = `${indent}${indent.includes("\t") ? "\t" : "  "}`;
    replace(
      rules,
      `{\n${inner}// Not enforced before migration; review before enabling:\n${source
        .slice(rules.range[0] + 1, rules.range[1] - 1)
        .split("\n")
        .map((line) => line.trim())
        .filter(Boolean)
        .map((line) => `${inner}// ${line}\n`)
        .join("")}${inner}'*': true,\n${indent}}`,
    );
    reviews.add(
      `access domain: Command rules were never enforced before migration; they are kept as comments under '*': true. Tighten after cutover: ${rules.properties.map((property: any) => textOf(property.key ?? property)).join(", ")}`,
    );
  };
  // `<app>.getDomain().<context>.<aggregate>(id).<command>(payload, metadata)`, as the old Domain built it.
  const commandOf = (send: any) => {
    const target = send?.callee,
      stream = target?.object,
      aggregate = stream?.callee,
      domain = aggregate?.object?.object;
    const plain = (member: any) =>
      member?.type === "MemberExpression" && !member.computed;
    if (
      send?.type !== "CallExpression" ||
      !plain(target) ||
      stream.type !== "CallExpression" ||
      !plain(aggregate) ||
      !plain(aggregate.object) ||
      domain?.type !== "CallExpression" ||
      !plain(domain.callee) ||
      domain.callee.property.name !== "getDomain" ||
      domain.arguments.length ||
      send.arguments.length > 2 ||
      stream.arguments.length > 1 ||
      [...send.arguments, ...stream.arguments].some(
        (arg: any) => arg.type === "SpreadElement",
      )
    )
      return undefined;
    const owner = domain.callee.object;
    return {
      send,
      context: aggregate.object.property.name,
      aggregate: aggregate.property.name,
      // A Task's ctx has no getDomain, so its Commands always become ctx calls.
      task: isTaskApp(owner),
      owner,
      domain,
    };
  };
  // Visits the children of `node`, then renders any part of it with their edits applied; the caller replaces `node`.
  const renderInner = (node: any) => {
    const before = edits.length;
    children(node).forEach((child) => visit(child, node));
    const inner = edits.splice(before);
    return Object.assign(
      (part: any): string =>
        inner
          .filter(
            (edit) => edit.start >= part.range[0] && edit.end <= part.range[1],
          )
          .sort((a, b) => b.start - a.start)
          .reduce(
            (text, edit) =>
              text.slice(0, edit.start - part.range[0]) +
              edit.text +
              text.slice(edit.end - part.range[0]),
            textOf(part),
          ),
      { inner },
    );
  };
  // The old QueryBuilder methods; a chain of them on getReadmodels().<group>.<model>() becomes one find or stream.
  const builderMethods = [
    "find",
    "filter",
    "sort",
    "from",
    "size",
    "q",
    "exec",
    "stream",
  ];
  const builderCall = (node: any) =>
    node?.type === "CallExpression" &&
    node.callee.type === "MemberExpression" &&
    !node.callee.computed &&
    builderMethods.includes(node.callee.property.name);
  const migrateQuery = (node: any): boolean => {
    const chain: any[] = [];
    let root = node;
    while (builderCall(root) && !repositoryOf(root)) {
      chain.unshift(root);
      root = root.callee.object;
    }
    const repository = repositoryOf(root),
      stream = chain.at(-1)?.callee.property.name === "stream",
      next =
        node.parent?.type === "MemberExpression" &&
        node.parent.object === node &&
        node.parent.property.name;
    // Only promise methods may follow the old thenable; anything after a stream uses the Node stream.
    if (
      !repository ||
      spread(root.arguments) ||
      (next && !stream && !["then", "catch", "finally"].includes(next))
    )
      return false;
    if (root.arguments.length === 1 && !chain.length) {
      if (
        node.parent?.type === "CallExpression" &&
        node.parent.callee.name === "legacyOne" &&
        fromNewPackage("legacyOne")
      ) return false;
      const render = renderInner(node);
      replace(
        node,
        `${useCompat("legacyOne")}(${repository}(${render(root.arguments[0])}))`,
      );
      return true;
    }
    const arity = (call: any) => {
      const method = call.callee.property.name;
      return method === "exec" || method === "stream"
        ? [0]
        : method === "find"
          ? [0, 1]
          : [1];
    };
    // A bare builder that is passed on or kept, not awaited, may still be used as a builder.
    if (
      !chain.length &&
      !next &&
      node.parent?.type !== "AwaitExpression"
    )
      return false;
    if (
      root.arguments.length ||
      chain.some(
        (call, index) =>
          spread(call.arguments) ||
          !arity(call).includes(call.arguments.length) ||
          (call.callee.property.name === "stream" &&
            index !== chain.length - 1),
      )
    )
      return false;
    skip.add(root);
    const render = renderInner(node);
    const parts: string[] = [];
    let text = false;
    for (const call of chain) {
      const method = call.callee.property.name,
        [argument] = call.arguments;
      if (!argument) continue;
      if (method === "find" && argument.type === "ObjectExpression") {
        text ||= argument.properties.some(
          (property: any) => keyOf(property) === "q",
        );
        parts.push(render(argument).slice(1, -1).trim());
      } else if (method === "find") parts.push(`...${render(argument)}`);
      else {
        text ||= method === "q";
        parts.push(`${method}: ${render(argument)}`);
      }
    }
    const [only] = chain.filter((call) => call.arguments.length);
    const query =
      parts.length === 1 &&
      only.callee.property.name === "find" &&
      only.arguments[0].type === "ObjectExpression"
        ? render(only.arguments[0])
        : parts.filter(Boolean).length
          ? `{ ${parts.filter(Boolean).join(", ")} }`
          : "";
    const call = `${repository}.${stream ? "stream" : "find"}(...${useCompat("legacyQuery")}(${query}))`;
    replace(node, stream ? `${useCompat("legacyStream")}(${call})` : call);
    if (text)
      reviews.add(
        `${repository}: text search (q) now needs a declared text index and matches case-insensitive substrings and phrases, not Mongo $text; compare the results`,
      );
    return true;
  };
  // Old Aggregate Commands: `(command, aggregate) => aggregate.apply.<event>(payload, metadata)` or `aggregate.apply('<event>', …)`.
  const migrateApply = (node: any): boolean => {
    const callee = node.callee,
      callable =
        callee.type === "MemberExpression" &&
        !callee.computed &&
        callee.property.name === "apply",
      apply = callable
        ? callee
        : callee.type === "MemberExpression" &&
            !callee.computed &&
            callee.object.type === "MemberExpression" &&
            !callee.object.computed &&
            callee.object.property.name === "apply"
          ? callee.object
          : undefined,
      aggregate = apply?.object;
    if (!apply || aggregate.type !== "Identifier" || spread(node.arguments))
      return false;
    const handler = functionOf(node),
      ownHandler =
        handler?.params[1]?.type === "Identifier" &&
        handler.params[1].name === aggregate.name,
      command = ownHandler ? handler.params[0] : undefined,
      literal = callable ? stringOf(node.arguments[0]) : undefined,
      computed =
        callable && literal === undefined && isString(node.arguments[0]);
    // A string Event name tells the old callable apply from Function.prototype.apply.
    if (callable && !computed && !/^[A-Za-z_$][\w$]*$/.test(literal ?? "")) {
      if (ownHandler)
        reviews.add(
          `${textOf(node)}: the old callable apply; call ${aggregate.name}.apply.<event>(payload, metadata) with a declared Event name by hand`,
        );
      return false;
    }
    const name = callable ? literal : callee.property.name,
      args = callable ? node.arguments.slice(1) : node.arguments,
      [, metadata] = args,
      merged =
        metadata?.type === "CallExpression" &&
        metadata.callee.name === "legacyMetadata";
    const commandMetadata =
      metadata && !merged ? metadataOf(handler, command) : undefined;
    if (metadata && !merged && !commandMetadata)
      reviews.add(
        `${textOf(node)}: Event metadata now overrides the Command's in a shallow merge; pass legacyMetadata(metadata, command.metadata) with the Command in scope to keep the old deep merge with Command values winning`,
      );
    if (!callable && !commandMetadata) return false;
    const render = renderInner(node);
    const rendered = args.map(render);
    if (commandMetadata)
      rendered[1] = `${useCompat("legacyMetadata")}(${rendered[1]}, ${commandMetadata})`;
    replace(
      node,
      `${aggregate.name}.apply${computed ? `[${render(node.arguments[0])}]` : `.${name}`}(${rendered.join(", ")})`,
    );
    return true;
  };
  // The Command's metadata in a Command function: `command.metadata`, the destructured `metadata`
  // binding, or one added to the pattern. Undefined when the pattern cannot name it safely.
  const addedMetadata = new Map<any, string>();
  const metadataOf = (fn: any, command: any): string | undefined => {
    if (command?.type === "Identifier") return `${command.name}.metadata`;
    if (command?.type !== "ObjectPattern" || !command.properties.length) return;
    if (command.properties.some((p: any) => p.type === "RestElement")) return;
    const own = command.properties.find((p: any) => keyOf(p) === "metadata");
    if (own)
      return own.value.type === "Identifier" &&
        !scopeManager.getDeclaredVariables(fn).some((variable: any) =>
          variable.identifiers.includes(own.value) &&
          variable.references.some((reference: any) => reference.isWrite()))
        ? own.value.name
        : undefined;
    if (!addedMetadata.has(command)) {
      let name = "commandMetadata";
      for (let i = 2; new RegExp(`\\b${name}\\b`).test(textOf(fn)); i++)
        name = `commandMetadata${i}`;
      const last = command.properties.at(-1);
      edits.push({ start: last.range[1], end: last.range[1], text: `, metadata: ${name}` });
      addedMetadata.set(command, name);
    }
    return addedMetadata.get(command);
  };
  // A computed Event name that is surely a string: a template, a concatenation with one, or a
  // variable only ever assigned such values.
  const isString = (node: any, direct = false): boolean => {
    if (node?.type === "Literal") return typeof node.value === "string";
    if (node?.type === "TemplateLiteral") return true;
    if (node?.type === "BinaryExpression" && node.operator === "+")
      return isString(node.left, direct) || isString(node.right, direct);
    if (node?.type === "ConditionalExpression")
      return isString(node.consequent, direct) && isString(node.alternate, direct);
    if (node?.type !== "Identifier" || direct) return false;
    const variable = scopeManager.scopes
      .flatMap((scope: any) => scope.references)
      .find((reference: any) => reference.identifier === node)?.resolved;
    const writes = variable?.references.filter((reference: any) => reference.isWrite()) ?? [];
    return writes.length > 0 &&
      writes.every((reference: any) => isString(reference.writeExpr, true));
  };
  const groupedKey = (key: string | undefined) =>
    /^([^:\s]+\.[^:\s]+):([^:\s]+)$/.exec(key ?? "");
  // Old grouped keys with their own Identity, query entry or markers become Reaction sets.
  const separateGroups = (definition: any, reactions: any) => {
    const map = (name: string) =>
      definition.properties.find((property: any) => keyOf(property) === name);
    const identity = map("identity"),
      query = map("query");
    const entry = (owner: any, key: string) =>
      owner?.value.type === "ObjectExpression"
        ? owner.value.properties.find(
            (property: any) => keyOf(property) === key,
          )
        : undefined;
    const marked = (value: any) =>
      (value.type === "ArrayExpression" ? value.elements : [value]).some(
        (item: any) =>
          !oldIfNotExists(item) &&
          !/Function|Identifier|MemberExpression/.test(item?.type ?? ""),
      );
    const separate = reactions.value.properties.filter((property: any) => {
      const key = keyOf(property),
        base = groupedKey(key)?.[1];
      if (!base) return false;
      const own = entry(identity, key!) ?? entry(query, key!);
      // A group selecting by the Event's query entry is ported by hand.
      if (!own && !entry(identity, base) && entry(query, base)) return false;
      return own || marked(property.value);
    });
    if (!separate.length) return;
    const rendered = (property: any) => {
      const render = renderInner(property);
      skip.add(property);
      return render(property.value);
    };
    // A set without its own Identity selects what its Event's Identity selects, as when groups merge.
    const inherited = new Map<string, string>();
    const inherit = (base: string) => {
      const property = entry(identity, base);
      if (property && !inherited.has(base)) {
        const render = renderInner(property);
        edits.push(...render.inner);
        skip.add(property);
        inherited.set(base, render(property.value));
      }
      return inherited.get(base);
    };
    const wholeQuery =
      query?.value.type === "ObjectExpression" &&
      query.value.properties.every((property: any) =>
        separate.some((moved: any) => keyOf(moved) === keyOf(property)),
      );
    const groups = new Map<
      string,
      { identity: string[]; reactions: string[] }
    >();
    for (const property of separate) {
      const key = keyOf(property)!,
        [, base, name] = groupedKey(key)!,
        event = quote(property.key, base!),
        group = groups.get(name!) ?? { identity: [], reactions: [] };
      groups.set(name!, group);
      const own = entry(identity, key),
        fromQuery = entry(query, key);
      const selects = own
        ? rendered(own)
        : fromQuery
          ? `${useCompat("legacyQueryIdentity")}(${rendered(fromQuery)})`
          : inherit(base!);
      if (own) removeItem(own);
      // The query map is removed whole, here or when its other entries move into identity.
      for (const entry of [own, fromQuery]) if (entry) moved.add(entry);
      if (selects) group.identity.push(`${event}: ${selects}`);
      group.reactions.push(`${event}: ${rendered(property)}`);
      removeItem(property);
    }
    if (wholeQuery) {
      skip.add(query);
      removeItem(query);
    }
    edits.push({
      start: reactions.range[1],
      end: reactions.range[1],
      text: `, reactionSets: { ${[...groups]
        .map(
          ([name, group]) =>
            `${/^[A-Za-z_$][\w$]*$/.test(name) ? name : JSON.stringify(name)}: { ${group.identity.length ? `identity: { ${group.identity.join(", ")} }, ` : ""}reactions: { ${group.reactions.join(", ")} } }`,
        )
        .join(", ")} }`,
    });
    reviews.add(
      `${separate.map((property: any) => textOf(property.key)).join(", ")}: moved into reactionSets with their own Identities and markers; they fold the documents they select after the model's own Reactions, in declaration order`,
    );
  };
  // Old grouped Reaction keys `<event>:<group>`: same-Event groups run in order as one Reaction.
  const migrateGroups = (node: any) => {
    const groups = new Map<string, any[]>();
    for (const property of node.properties) {
      if (skip.has(property)) continue;
      const key = keyOf(property),
        base = groupedKey(key)?.[1];
      if (base) groups.set(base, [...(groups.get(base) ?? []), property]);
    }
    if (!groups.size) return false;
    for (const [base, grouped] of groups) {
      const plain = node.properties.find(
        (property: any) => keyOf(property) === base,
      );
      groups.set(
        base,
        node.properties.filter((property: any) =>
          [plain, ...grouped].includes(property),
        ),
      );
    }
    const sibling = (name: string) =>
      node.parent?.parent?.properties?.find(
        (property: any) => keyOf(property) === name,
      )?.value;
    const handlers = (value: any) =>
      (value.type === "ArrayExpression" ? value.elements : [value]).filter(
        (item: any) => !oldIfNotExists(item),
      );
    const mergeable = [...groups.values()].every((properties) =>
      properties.every((property: any) =>
        handlers(property.value).every((item: any) =>
          /Function|Identifier|MemberExpression/.test(item?.type ?? ""),
        ),
      ),
    );
    if (
      !mergeable ||
      [sibling("identity"), sibling("query")].some((map) =>
        map?.properties?.some(
          (property: any) =>
            !skip.has(property) && (keyOf(property) ?? "").includes(":"),
        ),
      )
    ) {
      reviews.add(
        `${[...groups.values()]
          .flat()
          .map((property) => textOf(property.key))
          .join(
            ", ",
          )}: grouped Reaction keys with their own markers or Identities; merge each Event's groups into one Reaction by hand, keeping each group's document and skip rules`,
      );
      return false;
    }
    const render = renderInner(node),
      replaced: any[] = [];
    for (const [base, [first, ...rest]] of groups) {
      edits.push({
        start: first.range[0],
        end: first.range[1],
        text: `${quote(first.key, base)}: [${groups
          .get(base)!
          .flatMap((property: any) => handlers(property.value).map(render))
          .join(", ")}]`,
      });
      rest.forEach(removeItem);
      replaced.push(first, ...rest);
      reviews.add(
        `${groups
          .get(base)!
          .map((property: any) => textOf(property.key))
          .join(
            ", ",
          )}: merged into one Reaction for ${base} that runs their functions in this order on the document its Identity selects, as the old groups did`,
      );
    }
    // Edits inside the properties that were not merged apply as they are.
    edits.push(
      ...render.inner.filter(
        (edit) =>
          !replaced.some(
            (property) =>
              edit.start >= property.range[0] && edit.end <= property.range[1],
          ),
      ),
    );
    return true;
  };
  // Replaces `node` with `<owner>.<method>(CommandInput<suffix>)`, keeping transforms inside the arguments.
  const replaceCommand = (
    node: any,
    command: NonNullable<ReturnType<typeof commandOf>>,
    method: string,
    suffix = "",
  ) => {
    if (command.task) skip.add(command.domain);
    const render = renderInner(node);
    // Node ranges exclude grouping parentheses, so restore them where the new position needs them.
    const value = (part: any) =>
      part.type === "SequenceExpression" ? `(${render(part)})` : render(part);
    const owner = (part: any) =>
      [
        "Identifier",
        "MemberExpression",
        "CallExpression",
        "ThisExpression",
      ].includes(part.type)
        ? render(part)
        : `(${render(part)})`;
    const { send } = command,
      [id] = send.callee.object.arguments,
      [payload, metadata] = send.arguments;
    replace(
      node,
      `${command.task ? "ctx" : owner(command.owner)}.${method}({ stream: { context: '${command.context}', aggregate: '${command.aggregate}'${id ? `, id: ${value(id)}` : ""} }, name: '${send.callee.property.name}'${payload ? `, payload: ${value(payload)}` : ""}${metadata ? `, metadata: ${value(metadata)}` : ""} }${suffix})`,
    );
  };
  const migrateAwait = (node: any): boolean => {
    const pattern = stringOf(node.arguments[0]);
    if (node.arguments.length !== 1 || pattern === undefined) {
      reviews.add(
        `await(${node.arguments.map(textOf).join(", ")}): the Event pattern is not a string literal, so it cannot be told whether the wait is chain-wide; use .await for the sent Command's own Events or awaitCorrelated for downstream Events`,
      );
      return false;
    }
    if (!pattern.startsWith("domain.")) return false;
    const name = pattern.slice("domain.".length),
      command = commandOf(node.callee.object);
    if (!command) {
      reviews.add(
        `${pattern}: the awaited Command is not a direct getDomain().<context>.<aggregate>(id).<command>(payload, metadata) call, so its target Aggregate cannot be compared with the pattern; choose .await or awaitCorrelated by hand`,
      );
      return false;
    }
    if (
      !command.task &&
      name.startsWith(`${command.context}.${command.aggregate}.`)
    ) {
      replace(node.arguments[0], quote(node.arguments[0], name));
      return false;
    }
    replaceCommand(
      node,
      command,
      "awaitCorrelated",
      `, ${quote(node.arguments[0], name)}`,
    );
    reviews.add(settledReview);
    return true;
  };
  // An object literal, or a variable whose only write is its declaration with one.
  const objectOf = (node: any): any => {
    if (node?.type === "ObjectExpression") return node;
    if (node?.type !== "Identifier") return undefined;
    const variable = scopeManager.scopes
      .flatMap((scope: any) => scope.references)
      .find((reference: any) => reference.identifier === node)?.resolved;
    const writes =
      variable?.references.filter((reference: any) => reference.isWrite()) ??
      [];
    return writes.length === 1 &&
      writes[0].init &&
      writes[0].writeExpr.type === "ObjectExpression"
      ? writes[0].writeExpr
      : undefined;
  };
  // Old Saga declarations: an Event without an Identity started a new instance, and initialState was ignored.
  // Their maps may be declared beside the Saga, as in `module.exports = { identity, reactions }`.
  const migrateSaga = (node: any, reactions: any) => {
    const property = (name: string) =>
      node.properties.find((property: any) => keyOf(property) === name);
    const initial = property("initialState");
    if (initial) {
      removeItem(initial);
      reviews.add(
        "initialState: removed; the old Saga builder ignored it, so every instance started with empty State. Add it back only to give new instances that State",
      );
    }
    // A computed `[domain.<context>.<aggregate>.<event>]` key counts as the name the symbol map renames it to.
    const eventKeyOf = (property: any): string | undefined =>
      keyOf(property) ??
      (property.type === "Property" &&
      property.computed &&
      Object.hasOwn(options.symbols ?? {}, textOf(property.key))
        ? options.symbols![textOf(property.key)]
        : undefined);
    const reactionMap = objectOf(reactions.value);
    const events = [
      ...new Set<string | undefined>(
        reactionMap.properties.map((property: any) =>
          eventKeyOf(property)?.replace(/:[^:]*$/, ""),
        ),
      ),
    ];
    const identity =
      objectOf(property("identity")?.value) ?? property("identity")?.value;
    if (
      events.includes(undefined) ||
      (identity &&
        (identity.type !== "ObjectExpression" ||
          identity.properties.some((property: any) => !eventKeyOf(property))))
    )
      return void reviews.add(
        "identity: the Saga's Reaction or Identity keys are not all literal; give every Event an Identity by hand, since a missing one now uses the Stream id where the old store started a new instance",
      );
    const declared = new Set(
      identity?.properties.map((property: any) =>
        eventKeyOf(property)!.replace(/:[^:]*$/, ""),
      ),
    );
    const missing = events.filter((event): event is string => !declared.has(event));
    if (!missing.length) return;
    // Entries follow the map's own key form: a renamed `[domain.…]` constant becomes `["…"]`, as the rename writes it.
    const like = identity?.properties[0] ?? reactionMap.properties[0];
    const keyText = (event: string) => {
      const literal = like.key.type === "Literal" ? quote(like.key, event) : JSON.stringify(event);
      return like.computed ? `[${literal}]` : like.key.type === "Literal" ? literal : `'${event}'`;
    };
    const entries = missing.map((event) => `${keyText(event)}: (event) => event.id`);
    const lineStart = (at: number) => source.lastIndexOf("\n", at - 1) + 1;
    const indentAt = (at: number) => /^[ \t]*/.exec(source.slice(lineStart(at)))![0];
    const unit = /^\t/m.test(source) ? "\t" : "  ";
    const multiline = (node: any) => source.slice(node.range[0], node.range[1]).includes("\n");
    // A map written on one line gets its entries on that line; a multi-line map gets one entry per line.
    const block = (indent: string) =>
      `{\n${entries.map((entry) => `${indent}${unit}${entry},\n`).join("")}${indent}}`;
    if (!identity) {
      const line = source.slice(lineStart(reactions.range[0]), reactions.range[0]);
      const own = /^\s*$/.test(line);
      edits.push({
        start: reactions.range[0],
        end: reactions.range[0],
        text: own && multiline(reactionMap)
          ? `identity: ${block(line)},\n${line}`
          : `identity: { ${entries.join(", ")} },${own ? `\n${line}` : " "}`,
      });
    } else if (!identity.properties.length) {
      if (!multiline(identity))
        replace(identity, multiline(reactionMap) ? block(indentAt(identity.range[0])) : `{ ${entries.join(", ")} }`);
      else {
        // Keep what the empty map holds (comments): the entries go before its closing brace.
        const close = identity.range[1] - 1, indent = indentAt(identity.range[0]);
        edits.push({ start: lineStart(close), end: lineStart(close), text: entries.map((entry) => `${indent}${unit}${entry},\n`).join("") });
      }
    } else if (!multiline(identity)) {
      const last = identity.properties.at(-1);
      edits.push({ start: last.range[1], end: last.range[1], text: `, ${entries.join(", ")}` });
    } else {
      const last = identity.properties.at(-1), indent = indentAt(last.range[0]);
      const after = /^\s*,/.exec(source.slice(last.range[1]));
      if (after) {
        // After the trailing comma and any comment ending that line.
        const comma = last.range[1] + after[0].length;
        const end = source.indexOf("\n", comma);
        const rest = source.slice(comma, end < 0 ? undefined : end);
        const at = /^\s*(\/\/.*)?$/.test(rest) ? (end < 0 ? source.length : end) : comma;
        edits.push({ start: at, end: at, text: entries.map((entry) => `\n${indent}${entry},`).join("") });
      } else
        edits.push({ start: last.range[1], end: last.range[1], text: entries.map((entry) => `,\n${indent}${entry}`).join("") });
    }
    reviews.add(
      `identity: ${missing.join(", ")} had no Identity, so the old Saga store started a new instance for each such Event; each now starts the instance named by the Event id. Key it by a payload field instead where later Events must reach that instance`,
    );
  };
  // An old Readmodel `query` map: each entry becomes an Identity returning the translated filter.
  const migrateQueryMap = (node: any, query: any): boolean => {
    const identityProperty = node.properties.find(
        (property: any) => keyOf(property) === "identity",
      ),
      identity = identityProperty?.value,
      // Entries `separateGroups` moved into Reaction sets are already gone.
      kept = (map: any) =>
        map.properties.filter((property: any) => !moved.has(property)),
      keys = (map: any) => kept(map).map((property: any) => keyOf(property));
    if (
      query.value.type !== "ObjectExpression" ||
      keys(query.value).some((key: any) => !key || key.includes(":")) ||
      (identity &&
        (identity.type !== "ObjectExpression" ||
          keys(identity).some(
            (key: any) => !key || keys(query.value).includes(key),
          )))
    )
      return false;
    const render = renderInner(query.value);
    skip.add(query);
    const entries = kept(query.value)
      .map(
        (property: any) =>
          `${textOf(property.key)}: ${useCompat("legacyQueryIdentity")}(${render(property.value)})`,
      )
      .join(", ");
    if (!identity) replace(query, `identity: { ${entries} }`);
    else {
      if (kept(identity).length)
        edits.push({
          start: kept(identity).at(-1).range[1],
          end: kept(identity).at(-1).range[1],
          text: `, ${entries}`,
        });
      else if (identity.properties.length)
        edits.push({
          start: identity.range[0] + 1,
          end: identity.range[0] + 1,
          text: ` ${entries}`,
        });
      else replace(identity, `{ ${entries} }`);
      removeItem(query);
    }
    return true;
  };
  const visit = (node: any, parent?: any): void => {
    if (skip.has(node)) return;
    const text = textOf(node);
    if (
      node.type === "ObjectExpression" &&
      ((parent?.type === "VariableDeclarator" && parent.id.name === "access") ||
        (parent?.type === "Property" &&
          parent.value === node &&
          keyOf(parent) === "access") ||
        (parent?.type === "AssignmentExpression" &&
          parent.right === node &&
          parent.left.type === "MemberExpression" &&
          (parent.left.property.name ?? parent.left.property.value) ===
            "access"))
    )
      migrateAccess(node);
    const reactions =
      node.type === "ObjectExpression" &&
      node.properties.find(
        (property: any) =>
          keyOf(property) === "reactions" &&
          (property.value.type === "ObjectExpression" ||
            (options.saga && objectOf(property.value))),
      );
    if (options.saga && reactions) migrateSaga(node, reactions);
    if (!options.saga && reactions) separateGroups(node, reactions);
    const queryMap =
      reactions &&
      node.properties.find(
        (property: any) => keyOf(property) === "query" && !skip.has(property),
      );
    if (queryMap && migrateQueryMap(node, queryMap))
      reviews.add(
        "query: each entry moved into identity through legacyQueryIdentity, which translates the old filter; the Reaction still runs on every matching document",
      );
    if (node.type === "ObjectExpression" && migrateGroups(node)) return;
    if (
      node.type === "CallExpression" &&
      !(
        parent?.type === "MemberExpression" &&
        parent.object === node &&
        builderCall(parent.parent) &&
        parent.parent.callee === parent
      ) &&
      migrateQuery(node)
    )
      return;
    if (node.type === "CallExpression" && migrateApply(node)) return;
    // The old only.ifNotExists() allowed creation and still handled existing documents and instances.
    if (
      oldIfNotExists(node) &&
      parent?.type === "ArrayExpression" &&
      within(node, ["reactions"])
    ) {
      if (parent.elements.length > 1) removeItem(node);
      reviews.add(
        parent.elements.length > 1
          ? "only.ifNotExists(): the old marker is removed, since it allowed creation and still handled existing documents and Saga instances, as an unmarked Reaction does now. The new only.ifNotExists() skips existing ones"
          : `${textOf(parent)}: a Reaction with only the old only.ifNotExists() marker; it had no function to run, so remove the Reaction by hand`,
      );
      return;
    }
    // Old REST helpers were descriptors read by a route builder; the new ones handle requests.
    const [selector] = node.arguments ?? [];
    if (
      node.type === "CallExpression" &&
      ["ArrowFunctionExpression", "FunctionExpression"].includes(selector?.type)
    ) {
      const helper = node.callee.name ?? node.callee.property?.name,
        parameter = selector.params[0]?.name;
      const descriptor =
        helper === "command" &&
        find(
          selector.body,
          (child) =>
            child.type === "ObjectExpression" &&
            child.properties.some(
              (property: any) => keyOf(property) === "command",
            ),
        );
      const value = (key: string) =>
        descriptor?.properties.find((property: any) => keyOf(property) === key)
          ?.value;
      const wait = descriptor && value("event"),
        readmodel =
          descriptor &&
          /^readmodel\.([^.]+)\.([^.]+)\./.exec(stringOf(wait) ?? "");
      if (readmodel) skip.add(wait);
      if (descriptor && !options.legacyGateways) {
        const target = textOf(value("command")).split(".");
        const [context, aggregate, name] =
          target.length === 4 && target[0] === parameter
            ? target.slice(1).map((part) => `'${part}'`)
            : ["<context>", "<aggregate>", "<name>"];
        // Rewritten only when every part is known: the old id and payload defaults are not.
        const settings = node.arguments[1],
          option = (key: string) =>
            settings?.properties?.find(
              (property: any) => keyOf(property) === key,
            )?.value,
          callback = (key: string) =>
            /Function/.test(option(key)?.type ?? "") ? option(key) : undefined,
          applied = (fn: any) =>
            fn.type === "ArrowFunctionExpression" &&
            fn.params.length === 1 &&
            fn.params[0].type === "Identifier" &&
            fn.params[0].name === "req" &&
            fn.body.type !== "BlockStatement"
              ? fn.body.type === "ObjectExpression"
                ? `(${textOf(fn.body)})`
                : textOf(fn.body)
              : `(${textOf(fn)})(req)`;
        if (
          node.arguments.length === 2 &&
          selector.body === descriptor &&
          !context!.startsWith("<") &&
          (!wait || readmodel) &&
          descriptor.properties.every((property: any) =>
            ["command", "event"].includes(keyOf(property) ?? ""),
          ) &&
          settings.type === "ObjectExpression" &&
          settings.properties.every((property: any) =>
            ["id", "payload"].includes(keyOf(property) ?? ""),
          ) &&
          callback("id") &&
          callback("payload")
        ) {
          replace(
            node,
            `${textOf(node.callee)}((req) => ({ stream: { context: ${context}, aggregate: ${aggregate}, id: ${applied(callback("id"))} }, name: ${name}, payload: ${applied(callback("payload"))} })${readmodel ? `, { respondWith: '${readmodel[1]}.${readmodel[2]}' }` : ""})`,
          );
          reviews.add(
            `${textOf(value("command"))}: rewritten to command(req => CommandInput${readmodel ? ", { respondWith }" : ""}). The response is now ${readmodel ? `the ${readmodel[1]}.${readmodel[2]} document whose id is the Stream id (202 with the Outcome while it is not written yet)` : "the Outcome"}, not the old Notification payload or { aggregate, payload }; check the clients`,
          );
          return;
        }
        reviews.add(
          `${textOf(value("command"))}: an old REST command descriptor, which the new command() would send as the Command itself; write command(req => ({ stream: { context: ${context}, aggregate: ${aggregate}, id: <from req> }, name: ${name}, payload: <from req> })${readmodel ? `, { respondWith: '${readmodel[1]}.${readmodel[2]}' }` : ""}) by hand, moving the old id and payload options into the callback and dropping transforms and schema.${readmodel ? " respondWith reads the document whose id is the Stream id; for another Identity write a handler that waits for the Position" : ""}`,
        );
      }
      const listed =
        selector.body.type === "MemberExpression" &&
        !selector.body.computed &&
        selector.body.object.type === "MemberExpression" &&
        !selector.body.object.computed &&
        selector.body.object.object.name === parameter;
      const [, settings] = node.arguments;
      if (
        !options.legacyGateways &&
        helper === "list" &&
        listed &&
        node.callee.object?.name === "readmodels" &&
        node.arguments.length <= 2 &&
        (!settings ||
          (settings.type === "ObjectExpression" &&
            settings.properties.every((property: any) =>
              ["query", "transforms"].includes(keyOf(property) ?? ""),
            )))
      ) {
        const render = renderInner(node);
        skip.add(node);
        replace(
          node,
          `${useCompat("legacyList")}('${selector.body.object.property.name}', '${selector.body.property.name}'${settings ? `, ${render(settings)}` : ""})`,
        );
        reviews.add(
          `${textOf(node)}: rewritten to legacyList, which keeps the query string, defaults, Access scope and { total, data }. Errors now answer { code, message } (400 "Bad Request" for bad paging), negative paging is refused, q is the store's text search${settings?.properties.some((property: any) => keyOf(property) === "transforms") ? ", and transforms receive (document, handle(req)) instead of the old Application" : ""}`,
        );
        return;
      }
      if (!options.legacyGateways && (helper === "one" || helper === "list") && listed)
        reviews.add(
          helper === "one"
            ? `${textOf(node)}: an old Readmodel route helper; write readmodel('${selector.body.object.property.name}', '${selector.body.property.name}', { id: (req) => req.params.<id> }) or a route handler by hand. The id is no longer read from req.params.<model>_id`
            : `${textOf(node)}: an old Readmodel list route with options legacyList does not take; write legacyList('${selector.body.object.property.name}', '${selector.body.property.name}', { query, transforms }) by hand. The old schema option serialized each document as {}; decide which fields the clients need`,
        );
    }
    if (
      !options.legacyGateways &&
      node.type === "CallExpression" &&
      node.callee.property?.name === "getApp" &&
      !node.arguments.length
    )
      reviews.add(
        `${text}: the old request Application; Gateway handlers use handle(req) for { ctx, sendCommand, awaitCorrelated, readmodels }, and its context(req) and commandMetadata options map identity and metadata`,
      );
    if (stringOf(node)?.startsWith("readmodel.")) {
      reviews.add(
        `${stringOf(node)}: a Readmodel wait name outside an old REST command descriptor; where it is a wait, use respondWith or readmodels.waitFor by hand`,
      );
      return;
    }
    if (
      node.type === "CallExpression" &&
      node.callee.type === "MemberExpression" &&
      !node.callee.computed &&
      node.callee.property.name === "await" &&
      node.callee.object.type === "CallExpression" &&
      migrateAwait(node)
    )
      return;
    if (
      taskApp &&
      !(
        parent?.type === "MemberExpression" &&
        parent.object === node &&
        parent.property.name === "await"
      )
    ) {
      const command = commandOf(node);
      if (command?.task) {
        replaceCommand(node, command, "sendCommand");
        reviews.add(
          "sendCommand: resolves to the outcome { status, … } and does not throw on a Rejection; check status where the old call threw",
        );
        return;
      }
    }
    // The old Task's `{ args }` was the task's own arguments, which the new second parameter holds.
    const argsPattern = (param: any) =>
      param?.type === "ObjectPattern" &&
      param.properties.length === 1 &&
      keyOf(param.properties[0]) === "args" &&
      param.properties[0].value.type === "Identifier" &&
      param.properties[0].value.name === "args";
    if (
      options.task &&
      node.type === "AssignmentExpression" &&
      textOf(node.left) === "module.exports" &&
      !(
        ["ArrowFunctionExpression", "FunctionExpression"].includes(
          node.right.type,
        ) &&
        node.right.params.length <= 2 &&
        (!node.right.params[0] ||
          node.right.params[0].type === "Identifier" ||
          argsPattern(node.right.params[0])) &&
        (!node.right.params[1] || node.right.params[1].type === "Identifier")
      ) &&
      !(
        node.right.type === "ObjectExpression" &&
        node.right.properties.some((property: any) => keyOf(property) === "run")
      )
    )
      reviews.add(
        "module.exports: this Task export is not a function of (task, app), ({ args }, app), (task) or (); write exports.run = async (ctx, args) => … by hand, with ctx for the Application and args for task.args",
      );
    else if (
      options.task &&
      node.type === "AssignmentExpression" &&
      textOf(node.left) === "module.exports" &&
      node.right.params
    ) {
      const [first, second] = node.right.params;
      const extractedArgs = node.right.body.body?.flatMap((statement: any) =>
        statement.type === "VariableDeclaration" && first?.type === "Identifier"
          ? statement.declarations.filter((declaration: any) =>
              declaration.init?.name === first.name &&
              declaration.id.type === "ObjectPattern" &&
              declaration.id.properties.length === 1 &&
              keyOf(declaration.id.properties[0]) === "args" &&
              declaration.id.properties[0].value.name === "args",
            ).map((declaration: any) => ({ statement, declaration }))
          : [],
      ) ?? [];
      const collisions = scopeManager.scopes.flatMap((scope: any) =>
        scope.block.range[0] >= node.right.range[0] &&
        scope.block.range[1] <= node.right.range[1]
          ? scope.variables.filter((variable: any) =>
              ["ctx", "args"].includes(variable.name) &&
              variable.defs.some((definition: any) =>
                !(definition.type === "Parameter" && definition.node === node.right) &&
                !extractedArgs.some(({ declaration }: any) => definition.node === declaration),
              ),
            )
          : [],
      );
      if (collisions.length) {
        reviews.add("Task bindings: ctx or args would shadow an existing binding; rename it before migrating this Task");
        return;
      }
      taskParameter = first?.name;
      taskApp = second?.name;
      for (const variable of scopeManager.getDeclaredVariables(node.right))
        for (const { identifier } of variable.references)
          if (variable.name === taskApp) taskApps.add(identifier);
          else if (variable.name === taskParameter)
            taskParameters.add(identifier);
      replace(node.left, "exports.run");
      if (second) {
        replace(first, "ctx");
        replace(second, "args");
      } else if (first)
        // `task =>` has no parentheses to keep.
        replace(first, /\(\s*$/.test(source.slice(node.right.range[0], first.range[0])) ? "ctx, args" : "(ctx, args)");
      else {
        const open = source.indexOf("(", node.right.range[0]) + 1;
        edits.push({ start: open, end: open, text: "ctx, args" });
      }
      for (const { statement, declaration } of extractedArgs) {
        if (statement.declarations.length === 1) {
          replace(statement, "");
          skip.add(statement);
        } else {
          removeItem(declaration);
          skip.add(declaration);
        }
      }
      visit(node.right.body, node.right);
      return;
    }
    if (
      taskApp &&
      node.type === "MemberExpression" &&
      text === `${taskApp}.wire.app.config` &&
      isTaskApp(rootOf(node))
    ) {
      replace(node, "ctx.config");
      reviews.add(taskConfig(text, "ctx.config"));
      return;
    }
    if (
      taskApp &&
      node.type === "MemberExpression" &&
      text === `${taskApp}.wire.app` &&
      isTaskApp(rootOf(node))
    ) {
      replace(node, "ctx");
      reviews.add(taskConfig(text, "ctx"));
      return;
    }
    if (
      isTaskApp(node) &&
      !(
        parent?.type === "MemberExpression" &&
        parent.property === node &&
        !parent.computed
      )
    ) {
      // A shorthand `{ app }` keeps its key.
      const shorthand = parent?.type === "Property" && parent.shorthand && parent.value === node;
      replace(node, shorthand ? `${node.name}: ctx` : "ctx");
      // The Task's context as an argument, or a property of an object argument, of a call that is not its own.
      const argument = parent?.type === "Property" && parent.value === node ? parent.parent : node,
        call = argument === node ? parent : argument.parent;
      if (
        ["CallExpression", "NewExpression"].includes(call?.type) &&
        call.arguments.includes(argument) &&
        !isTaskApp(rootOf(call.callee))
      )
        reviews.add(
          `${textOf(call.callee)}(…): passes the Task context on, whose fields (config, services, readmodels, logger, sendCommand, awaitCorrelated) replace the Application's accessors, so a helper calling getServices(), getReadmodels() or getDomain() on it fails; update the helper, or pass it what it reads, by hand`,
        );
      return;
    }
    if (taskParameters.has(node))
      reviews.add(
        "Task helper: replace the old task helper with args, logger, stdin or Application-owned clients",
      );
    if (Object.hasOwn(options.replacements ?? {}, text)) {
      replace(node, options.replacements![text]!);
      return;
    }
    if (
      node.type === "MemberExpression" &&
      Object.hasOwn(options.symbols ?? {}, text)
    ) {
      replace(node, JSON.stringify(options.symbols![text]));
      return;
    }
    const specifier =
      node.type === "Literal" &&
      ((parent?.type === "CallExpression" &&
        (parent.callee.name === "require" ||
          textOf(parent.callee) === "require.resolve")) ||
        (parent?.source === node &&
          [
            "ImportDeclaration",
            "ImportExpression",
            "ExportNamedDeclaration",
            "ExportAllDeclaration",
          ].includes(parent.type))) &&
      /^event-fabric(-next)?(\/.*)?$/.exec(stringOf(node) ?? "");
    if (specifier) {
      const [, next, path = ""] = specifier,
        cutover = options.packages === "cutover",
        target = `event-fabric${cutover ? "" : "-next"}`,
        entries = `the new package exports only ${exported.map((entry) => `${target}${entry.slice(1)}`).join(", ")}`,
        known = !path || exported.includes(`.${path}`);
      if (!next && !cutover)
        reviews.add(
          `${node.value}: during coexistence this is the vendored old package; ${known ? `import ${target}${path}` : `${entries}; import from one of them`} for the new one`,
        );
      else if (!known)
        reviews.add(
          `${node.value}: ${entries}; import from one of them by hand`,
        );
      if (next && cutover) replace(node, quote(node, `event-fabric${path}`));
      return;
    }
    if (
      node.type === "CallExpression" &&
      node.callee.type === "MemberExpression"
    ) {
      const property = node.callee.property.name,
        task = isTaskApp(node.callee.object),
        owner = task ? "ctx" : textOf(node.callee.object);
      if (property === "getReadmodels" && !node.arguments.length) {
        const group = node.parent;
        if (!(
          group?.type === "MemberExpression" &&
          group.parent?.type === "MemberExpression"
        ))
          reviews.add(
            `${text}: the Readmodels are kept or passed on; review every query built from them. ${queryContract}`,
          );
      }
      const ofTask = task && !node.arguments.length;
      // The Task context has fields where the Application has accessors.
      if (ofTask && property === "getServices")
        return void replace(node, "ctx.services");
      if (ofTask && property === "getReadmodels")
        return void replace(node, "ctx.readmodels");
      // An awaited getDomain().<context>.<aggregate>(id) was the Aggregate; now it is a handle.
      if (
        property === "getDomain" &&
        !node.arguments.length &&
        node.parent?.type === "MemberExpression" &&
        node.parent.parent?.type === "MemberExpression" &&
        node.parent.parent.parent?.type === "CallExpression" &&
        node.parent.parent.parent.callee === node.parent.parent &&
        node.parent.parent.parent.parent?.type === "AwaitExpression" &&
        !task
      ) {
        const read = node.parent.parent.parent;
        edits.push({
          start: read.range[1],
          end: read.range[1],
          text: ".load()",
        });
        reviews.add(
          `${textOf(read)}.load(): getDomain() now builds a Command handle, so the read loads the State, with get(path) and revision in place of the old model's get(), toJSON() and getRevision()`,
        );
      }
      if (ofTask && property === "getDomain")
        reviews.add(
          `${text}: the Task context has no getDomain; send each Command with ctx.sendCommand({ stream, name, payload, metadata }) or ctx.awaitCorrelated by hand`,
        );
      if (property === "getCrud" && node.arguments.length === 0 && !options.keepGetCrud) {
        replace(
          node,
          ofTask ? "ctx.services.crud" : `${owner}.getServices().crud`,
        );
        return;
      }
      if (
        property === "getRevision" &&
        node.callee.object.type === "MemberExpression" &&
        node.callee.object.property.name === "_aggregateModel"
      ) {
        replace(node, `${textOf(node.callee.object.object)}.revision`);
        return;
      }
      if (property === "getRevision" && !node.arguments.length) {
        replace(node, `${textOf(node.callee.object)}.revision`);
        reviews.add(
          `${text}: now ${textOf(node.callee.object)}.revision; check that it reads an Aggregate State, whose revision counts the Stream's Events`,
        );
        return;
      }
      if (property === "getLogger" && !node.arguments.length && options.aggregate) {
        replace(node, `${textOf(node.callee.object)}.logger`);
        return;
      }
      if (property === "getLogger" && !node.arguments.length)
        reviews.add(
          `${text}: the old logger accessor; use the logger field of the Reaction, Task or Gateway context`,
        );
      if (property === "getWorkers" && !node.arguments.length)
        reviews.add(
          `${text}: Workers are no longer called directly; record the work with queue(…) from a Saga or a Command, so the Outbox delivers it once committed`,
        );
      if (property === "base" && node.arguments.length === 0) {
        // The old base() was the Mongo collection, so the chain after it stays Mongo code.
        const repository = repositoryOf(node.callee.object);
        let top = node;
        while (
          (top.parent?.type === "MemberExpression" &&
            top.parent.object === top) ||
          (top.parent?.type === "CallExpression" && top.parent.callee === top)
        )
          top = top.parent;
        if (
          repository &&
          !node.callee.object.arguments.length &&
          top.parent?.type === "AwaitExpression"
        ) {
          replace(node, `(await ${repository}.native())`);
          reviews.add(
            `${repository}.base(): now (await ${repository}.native()), the Mongo collection; it works only with the Mongo Readmodel store. Port the query to ${repository}.find(filter, options) to use another store`,
          );
        } else {
          reviews.add(
            `${text}: the old base() returned the Mongo collection synchronously; inside an awaited chain on getReadmodels().<group>.<model>() write (await <repository>.native()), or port the query to <repository>.find(filter, options), by hand`,
          );
          // The receiver is still rewritten, so a Task's Application does not stay unbound; the base() review covers the model call.
          const receiver = repository ? node.callee.object : node.callee;
          for (const child of children(receiver)) visit(child, receiver);
        }
        return;
      }
      if (repositoryOf(node))
        reviews.add(`${text}: an old QueryBuilder call. ${queryContract}`);
      if (property === "scheduleTask")
        reviews.add(
          "scheduleTask: map the due time and replace key to a delayed tell Command; provide an exact replacement for this call",
        );
      if (property === "getEventstore")
        reviews.add(`${property}: review the Event store/repository contract`);
    }
    // Old reserved Revision reads: `_revision` as a member, through `attributes` or `get('_revision')`.
    if (
      node.type === "MemberExpression" &&
      !node.computed &&
      node.property.name === "_revision"
    ) {
      const owner =
        node.object.type === "MemberExpression" &&
        !node.object.computed &&
        node.object.property.name === "attributes"
          ? node.object.object
          : node.object;
      replace(node, `${textOf(owner)}.revision`);
      reviews.add(
        `${text}: now ${textOf(owner)}.revision, kept outside the State attributes`,
      );
      return;
    }
    if (
      node.type === "CallExpression" &&
      node.callee.type === "MemberExpression" &&
      !node.callee.computed &&
      node.callee.property.name === "get" &&
      node.arguments.length === 1 &&
      stringOf(node.arguments[0]) === "_revision"
    ) {
      replace(node, `${textOf(node.callee.object)}.revision`);
      reviews.add(
        `${text}: now ${textOf(node.callee.object)}.revision, kept outside the State attributes`,
      );
      return;
    }
    if (
      node.type === "MemberExpression" &&
      !node.computed &&
      node.property.name === "attributes" &&
      node.object.type === "MemberExpression" &&
      node.object.property.name === "_aggregateModel"
    ) {
      replace(node, `${textOf(node.object.object)}.get()`);
      reviews.add(
        `${text}: now ${textOf(node.object.object)}.get(), a read of the State; write changes with set() rather than through the object`,
      );
      return;
    }
    if (node.type === "CallExpression" && node.callee.type === "Identifier" && node.callee.name === "fork" &&
      stringOf(node.arguments[1]?.elements?.[0]) === "task") migrateTaskFork(node);
    if (/Function/.test(node.type)) {
      const map = handlerMap(node);
      if (map === "events" || map === "reactions") migrateEventTime(node, map);
      // A Command's metadata has no commit time: a step falls back to the time of its attempt.
      const [command, , app] = node.params;
      const metadata = map === "commands" && app?.type === "Identifier" && command?.type === "ObjectPattern"
        ? command.properties.find((property: any) => keyOf(property) === "metadata" && property.value?.type === "Identifier")
        : undefined;
      for (const reference of (metadata && scopeManager.getDeclaredVariables(node).find((variable: any) => variable.name === metadata.value.name)?.references) ?? []) {
        const at = reference.identifier.parent;
        if (at.type === "MemberExpression" && at.object === reference.identifier && !at.computed && at.property.name === "timestamp" &&
          !(at.parent.type === "LogicalExpression" && at.parent.operator === "??" && at.parent.left === at) &&
          !(at.parent.type === "AssignmentExpression" && at.parent.left === at)) {
          edits.push({ start: at.range[1], end: at.range[1], text: ` ?? ${app.name}.now` });
          skip.add(at);
          timestampHandlers.add(node);
        }
      }
    }
    // The old Event envelope: Context was a string and commit time sat in metadata.timestamp.
    if (/Function/.test(node.type) && node.params[0]?.type === "ObjectPattern") {
      for (const property of node.params[0].properties) {
        if (keyOf(property) === "metadata") {
          const name = property.value?.name;
          if (name && !timestampHandlers.has(node) && find(node.body, (child) =>
            child.type === "MemberExpression" && child.object.name === name &&
            (child.property.name ?? child.property.value) === "timestamp",
          )) reviews.add("destructured metadata.timestamp: on an Event, read its top-level timestamp (an ISO string); update this helper and its callers by hand");
        }
        if (keyOf(property) === "context")
          reviews.add("destructured context: an Event now carries { name }; review this helper and its callers before treating it as the old string");
      }
    }
    if (
      node.type === "MemberExpression" &&
      !node.computed &&
      node.object.type === "Identifier"
    ) {
      const field = node.property.name,
        event = isEventParameter(node.object);
      if (
        field === "metadata" &&
        parent?.type === "MemberExpression" &&
        parent.object === node &&
        !parent.computed &&
        parent.property.name === "timestamp"
      ) {
        if (event) {
          skip.add(parent);
          edits.push({
            start: node.range[0],
            end: parent.range[1],
            text: `${node.object.name}.timestamp`,
          });
        }
        reviews.add(
          event
            ? "metadata.timestamp: the Event's commit time is now the top-level timestamp, an ISO string; check code that expected a Date or a number"
            : `${textOf(parent)}: the old Event commit time; on an Event read event.timestamp, an ISO string`,
        );
        return;
      }
      if (
        field === "context" &&
        !(
          parent?.type === "MemberExpression" &&
          parent.object === node &&
          (parent.property.name ?? parent.property.value) === "name"
        )
      ) {
        if (event) {
          replace(node, `${text}.name`);
          reviews.add(
            "context: the Event's Context is now { name }, so event.context became event.context.name",
          );
          return;
        }
        if (parent?.type === "TemplateLiteral")
          reviews.add(
            `${text}: interpolated as the old string Context; on a new Event it is { name }, so write ${text}.name`,
          );
      }
    }
    if (
      node.type === "MemberExpression" &&
      node.object.type === "MemberExpression" &&
      node.object.property.name === "_aggregateModel"
    ) {
      replace(
        node,
        `${textOf(node.object.object)}.locals.${node.property.name}`,
      );
      return;
    }
    if (
      node.type === "MemberExpression" && !node.computed &&
      node.object.type === "Identifier" && temporaryFields.has(node.property.name) &&
      isRuleState(node.object)
    ) {
      replace(node, `${node.object.name}.locals.${node.property.name}`);
      return;
    }
    // The old model's attributes held the Stream id; the State's do not, and get() adds it.
    if (
      node.type === "MemberExpression" && !node.computed &&
      node.property.name === "attributes" &&
      ((node.object.type === "Identifier" && isRuleState(node.object)) ||
        (node.object.type === "AwaitExpression" &&
          (isDomainHandle(node.object.argument) ||
            (node.object.argument.type === "CallExpression" &&
              node.object.argument.callee.property?.name === "load" &&
              isDomainHandle(node.object.argument.callee.object)))))
    )
      replace(node.property, "get()");
    if (
      node.type === "Property" &&
      (node.key.name ?? node.key.value) === "hasSnapshot"
    ) {
      removeItem(node);
      return;
    }
    // The old enricher ran inside each apply, on a mutable Event and the State before it.
    if (
      node.type === "Property" &&
      keyOf(node) === "eventEnricher" &&
      !(
        node.value.type === "CallExpression" &&
        node.value.callee.name === "legacyEnricher"
      )
    ) {
      const render = renderInner(node.value);
      skip.add(node.value);
      const value = `${node.method ? `${node.value.async ? "async " : ""}function` : ""}${render(node.value)}`;
      replace(node, `${textOf(node.key)}: ${useCompat("legacyEnricher")}(${value})`);
    }
    if (
      node.type === "MemberExpression" &&
      !node.computed &&
      node.object.type === "MemberExpression" &&
      !node.object.computed &&
      node.object.property.name === "aggregate" &&
      ["revision", "id", "name"].includes(node.property.name)
    )
      reviews.add(oldShapeReview(text));
    // Old Consumer markers: the runtime refuses them at startup, so each needs a deliberate rewrite.
    if (node.type === "Property" && !node.computed) {
      const key = node.key.name ?? node.key.value;
      if (key === "useAsId")
        reviews.add(
          `useAsId: an inline Identity marker; move it into identity as (event) => event.${stringOf(node.value) ?? "<path>"}`,
        );
      if (key === "identity" && node.value.type === "ObjectExpression")
        for (const entry of node.value.properties)
          if (stringOf(entry.value) !== undefined)
            reviews.add(
              `identity ${textOf(entry.key)}: a property-path string; write (event) => event.${stringOf(entry.value)}`,
            );
      if (
        key === "query" &&
        node.value.type === "ObjectExpression" &&
        parent?.properties?.some(
          (sibling: any) =>
            (sibling.key?.name ?? sibling.key?.value) === "reactions",
        )
      )
        reviews.add(
          "query: a separate Readmodel query map; return each filter from that Event's identity function instead",
        );
    }
    if (node.type === "Identifier" && node.name === "_runtime")
      reviews.add(
        "_runtime: internal runtime access has no public equivalent to map to; rewrite it to sendCommand, logger, hooks, services or an Application-owned client",
      );
    if (node.type === "Literal" && node.value === options.constantsModule)
      reviews.add(
        "constants import: remove it after every reference has been replaced, including Command constants; rejected pseudo-Events become outcomes",
      );
    for (const child of children(node)) visit(child, node);
  };
  visit(program);
  if (compat.size) {
    const names = [...compat].sort().join(", "),
      from = `event-fabric${options.packages === "cutover" ? "" : "-next"}/compat`,
      first = program.body.find((statement: any) => !statement.directive),
      at = first?.range[0] ?? source.length;
    edits.push({
      start: at,
      end: at,
      text: program.body.some((statement: any) =>
        /^(Import|Export)/.test(statement.type),
      )
        ? `import { ${names} } from "${from}";\n`
        : `const { ${names} } = require("${from}");\n`,
    });
  }
  let output = source;
  for (const edit of edits.sort((a, b) => b.start - a.start))
    output = output.slice(0, edit.start) + edit.text + output.slice(edit.end);
  if (/\.(awaitCorrelated|await)\(/.test(output)) {
    const waits = await settleWaits(output);
    output = waits.source;
    if (waits.settled) reviews.delete(settledReview);
    for (const text of waits.rewritten) reviews.delete(oldShapeReview(text));
  }
  return { source: output, reviews: [...reviews].sort() };
};
const oldShapeReview = (text: string) =>
  `${text}: the old Event shape; Events carry stream { context, aggregate, id } and revision at the top level`;
const settledReview =
  "awaitCorrelated: resolves to { status, event, outcome } and does not throw on a Rejection; check status and read .event where the old await returned the Event";
/**
 * A wait whose result the old code kept as the Event: `const x = await <app>.awaitCorrelated(…)` throws the Outcome
 * of a Rejection, as the old await did, and keeps the Event as `x`. On the result of any wait, the old `x.aggregate.id`
 * is the Event's `x.stream.id`. `settled` tells whether every awaitCorrelated call was such a declaration.
 */
const settleWaits = async (source: string): Promise<{ source: string; settled: boolean; rewritten: string[] }> => {
  const { program, scopeManager } = await parseSource(source);
  const edits: { start: number; end: number; text: string }[] = [];
  const textOf = (node: any) => source.slice(node.range[0], node.range[1]);
  const indentOf = (node: any) => source.slice(source.lastIndexOf("\n", node.range[0] - 1) + 1, node.range[0]).match(/^[\t ]*/u)![0];
  let calls = 0, settled = 0;
  const rewritten: string[] = [];
  const visit = (node: any) => {
    const wait = node.type === "CallExpression" && node.callee.type === "MemberExpression" && !node.callee.computed
      ? node.callee.property.name : undefined;
    if (wait === "awaitCorrelated") calls++;
    const declarator = node.parent?.type === "AwaitExpression" && node.parent.parent?.type === "VariableDeclarator" &&
      node.parent.parent.init === node.parent ? node.parent.parent : undefined;
    if ((wait === "awaitCorrelated" || wait === "await") && declarator?.id.type === "Identifier") {
      const [variable] = scopeManager.getDeclaredVariables(declarator);
      const reads = variable.references.map((reference: any) => reference.identifier.parent)
        .filter((member: any) => member.type === "MemberExpression" && !member.computed);
      const statement = declarator.parent, name = declarator.id.name;
      for (const member of reads)
        if (member.property.name === "aggregate" && member.parent.type === "MemberExpression" && !member.parent.computed && member.parent.property.name === "id")
        {
          edits.push({ start: member.range[0], end: member.parent.range[1], text: `${name}.stream.id` });
          rewritten.push(textOf(member.parent));
        }
      if (wait === "awaitCorrelated" && reads.some((member: any) => member.property.name === "status")) settled++;
      else if (wait === "awaitCorrelated" && statement.declarations.length === 1 && !/\bwaited\b/.test(source)) {
        const indent = indentOf(statement), q = /'/.test(textOf(node)) ? "'" : '"';
        edits.push({ start: statement.range[0], end: declarator.init.range[0], text: `${statement.kind} waited = ` });
        edits.push({ start: statement.range[1], end: statement.range[1], text: `\n${indent}// The old await threw on a rejection.\n${indent}if (waited.status !== ${q}accepted${q})\n${indent}\tthrow waited.outcome;\n${indent}${statement.kind} ${name} = waited.event;` });
        settled++;
      }
    }
    for (const [key, value] of Object.entries(node))
      if (key !== "parent")
        for (const child of Array.isArray(value) ? value : [value])
          if (child && typeof child.type === "string") visit(child);
  };
  visit(program);
  let output = source;
  for (const edit of edits.sort((a, b) => b.start - a.start))
    output = output.slice(0, edit.start) + edit.text + output.slice(edit.end);
  return { source: output, settled: calls === settled, rewritten };
};

export const draftReadmodelSchema = (
  initialState: Record<string, any>,
  samples: Record<string, any>[] = [],
  options: { dynamicPaths?: string[] } = {},
): { schema: Record<string, any>; reviews: string[]; source: string } => {
  const reviews: string[] = [];
  const json = (path: string, reason: string): Schema => {
    reviews.push(`${path || "<root>"}: ${reason}; review JSON storage`);
    return { "x-store": "json" };
  };
  const infer = (values: any[], path: string): any => {
    if (options.dynamicPaths?.includes(path)) return json(path, "dynamic keys");
    const present = values.filter((value) => value !== undefined),
      nullable = present.some((value) => value === null),
      nonNull = present.filter((value) => value !== null);
    if (!nonNull.length) return json(path, "no non-null evidence");
    const types = new Set(
      nonNull.map((value) =>
        value instanceof Date
          ? "date"
          : Array.isArray(value)
            ? "array"
            : typeof value,
      ),
    );
    if (types.size > 1) return json(path, "mixed types");
    const type = [...types][0]!;
    let schema: any;
    if (type === "date") schema = { type: "string", format: "date-time" };
    else if (type === "array") {
      const elements = nonNull.flat();
      if (!elements.length) return json(path, "no array element evidence");
      let items = infer(elements, `${path}[]`);
      if (
        items.type === "array" ||
        (Array.isArray(items.type) && items.type.includes("array"))
      )
        items = json(`${path}[]`, "nested arrays");
      schema = { type: "array", items };
    } else if (type === "object") {
      const keys = [...new Set(nonNull.flatMap((value) => Object.keys(value)))]
        .filter((key) => path || key !== "_id")
        .sort();
      if (
        path && keys.some((key) =>
          ["_id", "__proto__", "constructor", "prototype"].includes(key),
        )
      )
        return json(path, "nested reserved properties");
      if (path && keys.length > 20) return json(path, "more than 20 keys");
      if (!path && keys.includes("id"))
        reviews.push(
          "id: the old ViewModel stored each document's id here, and Reactions no longer do; set it where the document is created (view.set('id', view.id)) or read _id instead",
        );
      schema = {
        type: "object",
        additionalProperties: false,
        properties: Object.fromEntries(
          keys.map((key) => [
            key,
            infer(
              nonNull.map((value) => value[key]),
              path ? `${path}.${key}` : key,
            ),
          ]),
        ),
      };
    } else if (type === "number")
      schema = { type: nonNull.every(Number.isInteger) ? "integer" : "number" };
    else if (type === "string" || type === "boolean") schema = { type };
    else return json(path, `unsupported ${type}`);
    if (nullable) schema.type = [schema.type, "null"];
    return schema;
  };
  const schema = infer([initialState, ...samples], "");
  return {
    schema,
    reviews,
    source: `${reviews.map((review) => `// review: ${review}`).join("\n")}\nexport const schema = ${JSON.stringify(schema, null, 2)};\n`,
  };
};
export { migrateApplication } from "./application.js";
export type { ApplicationMigrationOptions, ApplicationMigrationResult } from "./application.js";
