import type { AggregateDefinition } from "../kernel/index.js";
import type { Schema } from "../types.js";
import type { WorkflowConfig } from "../runtime/types.js";
export type TypeModel = {
  aggregates?: readonly AggregateDefinition[];
  workers?: Record<
    string,
    { handles: readonly string[]; schemas?: Record<string, Schema | undefined> }
  >;
  readmodels?: Record<
    string,
    {
      models: Record<string, { schema: Schema }>;
      schemas?: Record<string, Schema>;
    }
  >;
  sagas?: Record<string, { messages?: Record<string, unknown> }>;
  workflows?: Record<string, WorkflowConfig>;
};
export const generateTypes = (
  model: TypeModel,
  schemas: Record<string, Schema> = {},
  options: { module?: string; mode?: "full" | "names" } = {},
): string => {
  const events = (model.aggregates ?? [])
    .flatMap((aggregate) =>
      Object.keys(aggregate.events).map((name) => {
        const qualified = `${aggregate.context}.${aggregate.name}.${name}`;
        return {
          name: qualified,
          schema: aggregate.eventSchemas?.[qualified]?.schema ?? true,
        };
      }),
    )
    .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  const commands = (model.aggregates ?? [])
    .flatMap((aggregate) =>
      Object.entries(aggregate.commands).map(([name, command]) => ({
        name: `${aggregate.context}.${aggregate.name}.${name}`,
        schema: command.schema ?? true,
      })),
    )
    .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  const documents = Object.entries(model.readmodels ?? {})
    .flatMap(([group, definition]) =>
      Object.entries(definition.models).map(([name, definition]) => ({
        name: `${group}.${name}`,
        schema: definition.schema,
        schemas: model.readmodels?.[group]?.schemas,
      })),
    )
    .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  const aliases: string[] = [],
    references = new Map<string, string>();
  const resolve = (
    reference: string,
    root: Schema,
    registry: Record<string, Schema>,
  ): Schema => {
    const [name, pointer] = reference.split("#");
    let schema: any = name ? (registry[name] ?? registry[`/${name}`]) : root;
    if (schema === undefined)
      throw Error(
        `Unknown schema ${reference}; provide the named schemas to type generation`,
      );
    if (pointer)
      for (const segment of pointer.replace(/^\//, "").split("/"))
        schema = schema?.[segment.replace(/~1/g, "/").replace(/~0/g, "~")];
    if (schema === undefined)
      throw Error(`Unknown schema reference ${reference}`);
    return schema;
  };
  const render = (
    schema: Schema,
    root: Schema,
    self: string,
    document: boolean,
    registry: Record<string, Schema> = schemas,
  ): string => {
    if (typeof schema === "string")
      return render({ $ref: schema }, root, self, document, registry);
    if (schema === true) return "unknown";
    if (schema === false) return "never";
    if (schema.$ref) {
      if (schema.$ref === "#") return self;
      const key = `${document}:${self}:${schema.$ref}`;
      const existing = references.get(key);
      if (existing) return existing;
      const name = `ReferencedSchema${references.size + 1}`;
      references.set(key, name);
      const target = resolve(schema.$ref, root, registry);
      const externalName = String(schema.$ref).split("#")[0];
      const nextRoot = externalName
        ? (registry[externalName] ?? registry[`/${externalName}`] ?? root)
        : root;
      aliases.push(
        `export type ${name} = ${render(target, nextRoot, self, document, registry)};`,
      );
      return name;
    }
    if (schema.const !== undefined) return JSON.stringify(schema.const);
    if (schema.enum)
      return (
        schema.enum
          .map((value: unknown) => JSON.stringify(value))
          .join(" | ") || "never"
      );
    if (schema.oneOf || schema.anyOf)
      return (schema.oneOf ?? schema.anyOf)
        .map(
          (entry: Schema) =>
            `(${render(entry, root, self, document, registry)})`,
        )
        .join(" | ");
    if (schema.allOf)
      return schema.allOf
        .map(
          (entry: Schema) =>
            `(${render(entry, root, self, document, registry)})`,
        )
        .join(" & ");
    if (Array.isArray(schema.type))
      return schema.type
        .map((type: string) =>
          render({ ...schema, type }, root, self, document, registry),
        )
        .join(" | ");
    switch (schema.type ?? (schema.properties ? "object" : undefined)) {
      case "null":
        return "null";
      case "boolean":
        return "boolean";
      case "number":
      case "integer":
        return "number";
      case "string":
        return document && schema.format === "date-time" ? "Date" : "string";
      case "array":
        return Array.isArray(schema.items)
          ? `[${schema.items.map((entry: Schema) => render(entry, root, self, document, registry)).join(", ")}]`
          : `(${render(schema.items ?? true, root, self, document, registry)})[]`;
      case "object": {
        const fields = Object.entries(schema.properties ?? {}).map(
          ([name, entry]) => {
            const required = schema.required?.includes(name);
            const type = render(
              entry as Schema,
              root,
              self,
              document,
              registry,
            );
            return `${JSON.stringify(name)}${required ? "" : "?"}: ${type}${document && !required && !/(^| \| )null($| \| )/.test(type) ? " | null" : ""}`;
          },
        );
        // TypeScript has no regex keys, so every pattern (TypeBox `Record`) joins one string index.
        const extra = [
          ...Object.values(schema.patternProperties ?? {}),
          ...(schema.additionalProperties ? [schema.additionalProperties] : []),
        ].map((entry) =>
          entry === true
            ? "unknown"
            : render(entry as Schema, root, self, document, registry),
        );
        if (extra.length)
          fields.push(
            `[key: string]: ${extra.length === 1 ? extra[0] : extra.map((type) => `(${type})`).join(" | ")}`,
          );
        return `{ ${fields.join("; ")} }`;
      }
      default:
        return "unknown";
    }
  };
  const module = options.module ?? "event-fabric",
    loose = options.mode === "names";
  const self = (map: string, name: string) =>
    `import(${JSON.stringify(module)}).${map}[${JSON.stringify(name)}]`;
  // A part without a schema is `any`: its name is checked, its payload stays loose.
  const payload = (schema: Schema, map: string, name: string, document = false) =>
    loose || schema === true
      ? "any"
      : render(schema, schema, self(map, name), document);
  const key = JSON.stringify;
  const nested = (
    entries: { name: string; type: string }[],
    depth: number,
    indent: string,
  ): string => {
    if (depth === 0)
      return entries.length === 1 && !entries[0]!.name
        ? entries[0]!.type
        : "never";
    if (!entries.length) return "{}";
    const groups = new Map<string, { name: string; type: string }[]>();
    for (const entry of entries) {
      const [head, ...rest] = entry.name.split(".");
      if (!groups.has(head!)) groups.set(head!, []);
      groups.get(head!)!.push({ name: rest.join("."), type: entry.type });
    }
    return `{\n${[...groups]
      .map(
        ([name, children]) =>
          `${indent}  ${key(name)}: ${nested(children, depth - 1, `${indent}  `)};`,
      )
      .join("\n")}\n${indent}}`;
  };
  const aggregates = (model.aggregates ?? []).map((aggregate) => {
    const prefix = `${aggregate.context}.${aggregate.name}`;
    const section = (
      entries: { name: string; schema: Schema }[],
      map: string,
    ) =>
      nested(
        entries
          .filter((entry) => entry.name.startsWith(`${prefix}.`))
          .map((entry) => ({
            name: entry.name.slice(prefix.length + 1),
            type: payload(entry.schema, map, entry.name),
          })),
        1,
        "          ",
      );
    return {
      name: prefix,
      type: `{\n          events: ${section(events, "EventPayloads")};\n          commands: ${section(commands, "CommandPayloads")};\n        }`,
    };
  });
  const readmodels = documents.map((document) => ({
    name: document.name,
    type: loose
      ? "any"
      : `{ _id: string } & ${render(document.schema, document.schema, self("ReadmodelDocuments", document.name), true, { ...schemas, ...document.schemas })}`,
  }));
  const messages = Object.values(model.workers ?? {})
    .flatMap((worker) =>
      worker.handles.map((name) => ({
        name,
        type: payload(
          worker.schemas?.[name] ?? true,
          "MessagePayloads",
          name,
        ),
      })),
    )
    .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  // Saga names may hold dots, so Saga Messages nest by Saga, not by name segment.
  const sagaMessages = Object.entries(model.sagas ?? {})
    .map(([saga, definition]) => ({
      saga,
      messages: Object.entries(definition.messages ?? {})
        .map(([name, handler]) => {
          const schema = (handler as { schema?: Schema })?.schema ?? true;
          return { name, type: loose || schema === true ? "any" : render(schema, schema, "unknown", false) };
        })
        .sort((a, b) => (a.name < b.name ? -1 : 1)),
    }))
    .filter((saga) => saga.messages.length)
    .sort((a, b) => (a.saga < b.saga ? -1 : 1));
  // Every registered version's Signals, as the runtime accepts them while old instances drain.
  const workflows = Object.entries(model.workflows ?? {})
    .map(([workflow, entry]) => {
      const versions = "versions" in entry ? entry.versions : [entry];
      const types = new Map<string, Set<string>>();
      for (const version of versions)
        for (const [name, { schema = true }] of Object.entries(version.signals ?? {}))
          (types.get(name) ?? types.set(name, new Set()).get(name)!).add(
            loose || schema === true ? "any" : render(schema, schema, "unknown", false),
          );
      return {
        workflow,
        signals: [...types]
          .map(([name, union]) => ({ name, type: union.has("any") ? "any" : [...union].join(" | ") }))
          .sort((a, b) => (a.name < b.name ? -1 : 1)),
      };
    })
    .sort((a, b) => (a.workflow < b.workflow ? -1 : 1));
  const parts = [
    aggregates.length &&
      `    aggregates: ${nested(aggregates, 2, "    ")};`,
    readmodels.length && `    readmodels: ${nested(readmodels, 2, "    ")};`,
    messages.length &&
      `    messages: {\n${messages.map((message) => `      ${key(message.name)}: ${message.type};`).join("\n")}\n    };`,
    sagaMessages.length &&
      `    sagaMessages: {\n${sagaMessages.map(({ saga, messages }) => `      ${key(saga)}: {\n${messages.map((message) => `        ${key(message.name)}: ${message.type};`).join("\n")}\n      };`).join("\n")}\n    };`,
    workflows.length &&
      `    workflows: {\n${workflows.map(({ workflow, signals }) => `      ${key(workflow)}: {\n        signals: {${signals.map((signal) => `\n          ${key(signal.name)}: ${signal.type};`).join("")}\n        };\n      };`).join("\n")}\n    };`,
    `    mode: ${key(loose ? "names" : "full")};`,
  ].filter(Boolean);
  return `// Generated by EventFabric. Run event-fabric types to update.\nimport ${key(module)};\ndeclare module ${key(module)} {\n  interface Register {\n${parts.join("\n")}\n  }\n}\n${aliases.join("\n")}\n`;
};
