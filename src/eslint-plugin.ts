import type { Rule } from "eslint";
const propertyName = (node: any): string | undefined =>
  node?.key?.name ?? node?.key?.value;
const memberName = (node: any): string | undefined =>
  node?.type === "MemberExpression"
    ? (node.property.name ?? node.property.value)
    : undefined;
const HANDLERS = [
  "CommandHandler",
  "EventSourcingHandler",
  "EventHandler",
  "SagaEventHandler",
];
const COMPONENTS = ["Aggregate", "Readmodel", "Saga"];
const calleeName = (node: any): string | undefined =>
  memberName(node) ?? node?.name;
/** A decorator's exported name, so `import { Aggregate as Entity }` still counts as `Aggregate`. */
const roleName = (context: Rule.RuleContext, callee: any) => {
  if (callee?.type === "Identifier")
    for (
      let scope: any = context.sourceCode.getScope(callee);
      scope;
      scope = scope.upper
    ) {
      const definition = scope.set.get(callee.name)?.defs[0]?.node;
      if (definition)
        return definition.type === "ImportSpecifier"
          ? (definition.imported.name ?? definition.imported.value)
          : callee.name;
    }
  return calleeName(callee);
};
const decoratorNames = (context: Rule.RuleContext, list: any[]): string[] =>
  list.flatMap((item) => {
    const name = roleName(
      context,
      item?.type === "CallExpression" ? item.callee : item,
    );
    return name ? [name] : [];
  });
/**
 * The decorators of a class as tsc 7 emits them (`event-fabric/decorators` source needs a
 * build, and these rules lint its output). `@CommandHandler(X) open() {}` becomes, in a
 * static block of the class, `_open_decorators = [CommandHandler(X)]` and
 * `__esDecorate(this, null, _open_decorators, { kind: "method", name: "open" })`; a class
 * decorator becomes `let _classDecorators = [Aggregate(...)]` around the class.
 */
const compiled = new WeakMap<
  any,
  { methods: Map<string, string[]>; component?: string }
>();
const decoratorsOf = (context: Rule.RuleContext, node: any) => {
  if (compiled.has(node)) return compiled.get(node)!;
  const lists = new Map<string, string[]>(),
    methods = new Map<string, string[]>();
  let classList: string | undefined;
  for (const member of node.body.body)
    if (member.type === "StaticBlock")
      for (const { expression } of member.body) {
        if (
          expression?.type === "AssignmentExpression" &&
          expression.left.type === "Identifier" &&
          expression.right.type === "ArrayExpression"
        )
          lists.set(
            expression.left.name,
            decoratorNames(context, expression.right.elements),
          );
        if (calleeName(expression?.callee) !== "__esDecorate") continue;
        const [, , list, decoration] = expression.arguments;
        const field = (key: string) =>
          decoration?.properties?.find((p: any) => propertyName(p) === key)
            ?.value?.value;
        if (list?.type !== "Identifier") continue;
        if (field("kind") === "method")
          methods.set(field("name"), lists.get(list.name) ?? []);
        if (field("kind") === "class") classList = list.name;
      }
  let component: string | undefined;
  for (
    let scope: any = classList && context.sourceCode.getScope(node);
    scope && !component;
    scope = scope.upper
  ) {
    const init = scope.set.get(classList)?.defs[0]?.node.init;
    if (init?.type === "ArrayExpression")
      component = decoratorNames(context, init.elements).find((name) =>
        COMPONENTS.includes(name),
      );
  }
  const result = { methods, component };
  compiled.set(node, result);
  return result;
};
/** The handler roles of a method in a compiled decorated class. */
const methodRoles = (context: Rule.RuleContext, method: any): string[] =>
  method.parent?.type === "ClassBody"
    ? (
        decoratorsOf(context, method.parent.parent).methods.get(
          propertyName(method) ?? "",
        ) ?? []
      ).filter((name) => HANDLERS.includes(name))
    : [];
const scope = (context: Rule.RuleContext, node: any) => {
  const ancestors = context.sourceCode.getAncestors(node) as any[];
  const names = ancestors.flatMap((ancestor) =>
    ancestor.type === "Property"
      ? [propertyName(ancestor)]
      : ancestor.type === "VariableDeclarator"
        ? [ancestor.id.name]
        : ancestor.type === "FunctionDeclaration"
          ? [ancestor.id?.name]
          : ancestor.type === "AssignmentExpression"
            ? [memberName(ancestor.left)]
            : [],
  );
  // A Workflow body replays from its journal: `run` of `workflow(...)` from `event-fabric/workflows`.
  if (
    names.includes("run") &&
    ancestors.some(
      (ancestor) =>
        ancestor.type === "CallExpression" && roleName(context, ancestor.callee) === "workflow",
    )
  )
    return { deterministic: true, workflow: true, command: false, readmodel: false };
  const options = (context.options[0] ?? {}) as { sagaFiles?: string };
  // A Readmodel declares a schema next to its Reactions; a Saga never does.
  const readmodelShape = ancestors.some(
    (ancestor) =>
      ancestor.type === "ObjectExpression" &&
      ancestor.properties.some((p: any) => propertyName(p) === "reactions") &&
      ancestor.properties.some((p: any) => propertyName(p) === "schema"),
  );
  const saga =
    !readmodelShape &&
    !names.includes("readmodels") &&
    // `sagas: {...}` config keys and `billingSaga`-style names mark a Saga.
    (names.some((name) => /saga/i.test(name ?? "")) ||
      context.sourceCode
        .getAllComments()
        .some((comment) => /@eventfabric-saga\b/.test(comment.value)) ||
      new RegExp(options.sagaFiles ?? "(?:^|[./-])saga(?:s|[./-]|$)", "i").test(
        context.filename,
      ));
  // A decorated handler's role comes from its decorator (`event-fabric/decorators`): the
  // method it decorates, or the decorator's own arguments (`before` Steps, `associationProperty`).
  const roles = ancestors.flatMap((ancestor) =>
    ancestor.type === "MethodDefinition"
      ? methodRoles(context, ancestor)
      : ancestor.type === "CallExpression" &&
          HANDLERS.includes(roleName(context, ancestor.callee) ?? "")
        ? [roleName(context, ancestor.callee)!]
        : [],
  );
  if (roles.length)
    return {
      deterministic:
        names.includes("associationProperty") ||
        roles.includes("EventSourcingHandler") ||
        roles.includes("EventHandler"),
      command: roles.includes("CommandHandler"),
      readmodel: roles.includes("EventHandler"),
    };
  return {
    deterministic:
      names.includes("identity") ||
      names.includes("eventEnricher") ||
      names.includes("events") ||
      (names.includes("reactions") && !saga),
    command: names.includes("commands") || names.includes("rules"),
    readmodel: names.includes("reactions") && !saga,
  };
};
const clockRead = (node: any): boolean =>
  (node.type === "NewExpression" &&
    node.callee.name === "Date" &&
    node.arguments.length === 0) ||
  (node.type === "CallExpression" &&
    (node.callee.name === "Date" ||
      (memberName(node.callee) === "now" &&
        node.callee.object.name === "Date") ||
      (["moment", "dayjs"].includes(node.callee.name) &&
        node.arguments.length === 0)));
const randomRead = (node: any, uuids: Set<string>): boolean =>
  node.type === "CallExpression" &&
  ((memberName(node.callee) === "random" &&
    node.callee.object.name === "Math") ||
    memberName(node.callee) === "randomUUID" ||
    /^uuid(?:v[1-8])?$/i.test(
      node.callee.name ?? memberName(node.callee) ?? "",
    ) ||
    uuids.has(node.callee.name) ||
    (node.callee.type === "MemberExpression" &&
      uuids.has(node.callee.object.name)));
const determinism: Rule.RuleModule = {
  meta: {
    type: "problem",
    schema: [
      {
        type: "object",
        properties: { sagaFiles: { type: "string" } },
        additionalProperties: false,
      },
    ],
    messages: {
      nondeterministic:
        "Use only the Event payload and envelope here; clock and randomness prevent deterministic replay.",
      crud: "Readmodel Reactions cannot read mutable CRUD data; include the needed data on the Event.",
      workflow:
        "A Workflow body replays: take time and randomness from ctx.now(), ctx.random() and ctx.uuid().",
    },
  },
  create(context) {
    const uuids = new Set<string>();
    const cryptoModules = new Set<string>();
    const check = (node: any) => {
      const where = scope(context, node);
      if (where.deterministic && (clockRead(node) || randomRead(node, uuids)))
        context.report({
          node,
          messageId: "workflow" in where ? "workflow" : "nondeterministic",
        });
    };
    return {
      ImportDeclaration(node: any) {
        for (const specifier of node.specifiers)
          if (
            /uuid/i.test(node.source.value) ||
            specifier.imported?.name === "randomUUID"
          )
            uuids.add(specifier.local.name);
      },
      VariableDeclarator(node: any) {
        const required =
          node.init?.type === "CallExpression" &&
          node.init.callee.name === "require"
            ? node.init.arguments[0]?.value
            : undefined;
        const uuidModule =
          typeof required === "string" && /uuid/i.test(required);
        const cryptoModule =
          ["crypto", "node:crypto"].includes(required) ||
          cryptoModules.has(node.init?.name);
        if (node.id.type === "Identifier") {
          if (
            uuidModule ||
            uuids.has(node.init?.name) ||
            memberName(node.init) === "randomUUID"
          )
            uuids.add(node.id.name);
          if (cryptoModule) cryptoModules.add(node.id.name);
        } else if (uuidModule || cryptoModule) {
          for (const property of node.id.properties ?? [])
            if (
              property.value?.name &&
              (uuidModule || propertyName(property) === "randomUUID")
            )
              uuids.add(property.value.name);
        }
      },
      NewExpression: check,
      CallExpression(node: any) {
        check(node);
        if (
          scope(context, node).readmodel &&
          memberName(node.callee) === "getCrud"
        )
          context.report({ node, messageId: "crud" });
      },
      MemberExpression(node: any) {
        if (
          scope(context, node).readmodel &&
          memberName(node) === "crud" &&
          node.object.type === "CallExpression" &&
          memberName(node.object.callee) === "getServices"
        )
          context.report({ node, messageId: "crud" });
      },
    };
  },
};
const commandClock: Rule.RuleModule = {
  meta: {
    type: "suggestion",
    schema: [],
    messages: {
      clock:
        "Use the Command attempt now supplied to Steps and Rules so tests and retries control time.",
    },
  },
  create(context) {
    const check = (node: any) => {
      if (scope(context, node).command && clockRead(node))
        context.report({ node, messageId: "clock" });
    };
    return { NewExpression: check, CallExpression: check };
  },
};
const noDefaultExport: Rule.RuleModule = {
  meta: {
    type: "problem",
    schema: [],
    messages: {
      named:
        "Use named exports so CommonJS consumers can load the ESM package consistently.",
    },
  },
  create(context) {
    return {
      ExportDefaultDeclaration(node) {
        context.report({ node, messageId: "named" });
      },
      ExportNamedDeclaration(node: any) {
        for (const specifier of node.specifiers ?? [])
          if (
            (specifier.exported.name ?? specifier.exported.value) === "default"
          )
            context.report({ node: specifier, messageId: "named" });
      },
    };
  },
};
const decoratedClass: Rule.RuleModule = {
  meta: {
    type: "problem",
    schema: [],
    messages: {
      state:
        "A decorated {{kind}} class holds no state: remove the {{what}}. Handlers receive the State, document or Saga state as an argument.",
    },
  },
  create(context) {
    // tsc adds this constructor to run decorator initializers (after `super(...arguments)`
    // in a subclass); the class is never constructed.
    const generated = (member: any) =>
      member.kind === "constructor" &&
      member.value.body.body.every(
        ({ expression }: any) =>
          calleeName(expression?.callee) === "__runInitializers" ||
          expression?.callee?.type === "Super",
      );
    const check = (node: any) => {
      const { methods, component } = decoratorsOf(context, node);
      const handlers = [...methods.values()].some((names) =>
        names.some((name) => HANDLERS.includes(name)),
      );
      if (!component && !handlers) return;
      const report = (target: any, what: string) =>
        context.report({
          node: target,
          messageId: "state",
          data: { kind: component ?? "handler", what },
        });
      if (node.superClass) report(node.superClass, "superclass");
      for (const member of node.body.body) {
        if (member.type === "StaticBlock" || generated(member)) continue;
        if (member.kind === "constructor") report(member, "constructor");
        else if (member.static) report(member, "static member");
        else if (member.type !== "MethodDefinition") report(member, "field");
      }
    };
    return {
      ClassDeclaration: check,
      ClassExpression: check,
      ThisExpression(node: any) {
        const method = (
          context.sourceCode.getAncestors(node) as any[]
        ).findLast((ancestor) => ancestor.type === "MethodDefinition");
        if (method && methodRoles(context, method).length)
          context.report({
            node,
            messageId: "state",
            data: { kind: "handler", what: "use of `this`" },
          });
      },
    };
  },
};
export const rules = {
  determinism,
  "command-clock": commandClock,
  "no-default-export": noDefaultExport,
  "decorated-class": decoratedClass,
};
export const plugin = {
  meta: { name: "event-fabric", version: "0.0.0" },
  rules,
};
export const configs = {
  recommended: {
    plugins: { eventfabric: plugin },
    rules: {
      "eventfabric/no-default-export": "error" as const,
      "eventfabric/determinism": "error" as const,
      "eventfabric/command-clock": "warn" as const,
      "eventfabric/decorated-class": "error" as const,
    },
  },
};
