# TypeScript authoring uses typed handles and TypeBox schemas; the Register stays the string-name path

A TypeScript Application declares an Aggregate with `aggregate(context, name, { state, initialState, commands, events })` and TypeBox schemas. Every type comes from that call: each schema is both a payload's TypeScript type and the plain draft-07 JSON Schema that Ajv validates and Postgres derives columns from. `defineDomain(invoice, ...)` collects Aggregates into one namespace of Handles: `domain.billing.invoice.settled` is the qualified Event name `"billing.invoice.settled"`, typed with its payload, and `domain.billing.invoice.issue(id, payload)` builds a typed Command. Other files import the domain instead of writing names. `message(name, schema)` does the same for queued Messages. `compile` and string names typed by the generated `Register` (ADR-0012) remain the path for JavaScript and for any code that names something it cannot import.

ADR-0012 made the generated `Register` the source of every type. Probing the billing example showed what that costs a TypeScript author while editing:

- a typo in the Aggregate's name silently turns all its types into `any`;
- a new Event is untyped until `event-fabric types` runs;
- a rename makes the stale Register demand the old name, with an unreadable error;
- State inferred from `initialState` forces an `as State` cast.

ADR-0012 rejected handles because `FromSchema` cannot infer from the Application's schemas. TypeBox has no such limit.

## Considered Options

- **Keep the generated Register as the only source (ADR-0012).** Rejected for TypeScript authors, for the editing costs above. It stays for JavaScript, which cannot use inferred handles.
- **Zod, Valibot or ArkType schemas converted to JSON Schema.** Rejected as the default: conversion can drop checks silently (Zod's `.refine()`), and Zod and ArkType default to draft 2020-12. Accepting any Standard JSON Schema can be added later.
- **Explicit `compile<Events, State, Commands>` type arguments.** Rejected: the payload types duplicate the schemas.

## Consequences

- `typebox` is a dependency, and `Type` is re-exported from `event-fabric`, so EventFabric's public interface follows TypeBox's major versions.
- The framework flattens TypeBox's `Static` once (`Static` from `event-fabric`), which keeps type-checking cheap: the billing-shaped fixture costs about 16K instantiations, and a test keeps it under 100K.
- Commands in `aggregate` are `{ schema, steps, rules? }`, so TypeScript can infer the payload from the schema. `steps` takes only `only.ifExists()`/`only.ifNotExists()` and Steps: a Precondition is a Step that throws, and Rules go in `rules`, so no untyped `only.ifState`/`only.after` callback slips in. `compile`'s array form stays for JavaScript and codemods (ADR-0002).
- Event names in the domain are plain qualified-name strings, so they work wherever a name is accepted (Reaction and Identity keys, `.await`, `readAll`). Commands and Events share one namespace per Aggregate, and `defineDomain` rejects a Command and an Event with the same name.
- Types promise only what the runtime delivers. A Command's payload type is the validated payload; TypeBox does not record `default`s, so a defaulted field is declared `Type.Optional`. `Document<typeof Schema>` types an optional field as absent or `null` (the memory and Mongo stores keep absent fields absent; Postgres reads them as `null`), and Readmodel date-time fields use `DateTime()` (read back as `Date`); a plain TypeBox date-time `String` in a Readmodel is rejected. TypeBox `Refine` and `Codec` are rejected wherever a schema enters, because Ajv would skip them.
- `resolveSchema` normalizes the nullable `anyOf`, bare `enum` and `const` unions that schema builders emit, so their Readmodel schemas map to columns.
- Event stores check declared Event and metadata schemas with the framework's validator when given none.
- Handles carry the same qualified names and plain schemas, so the runtime model, the startup name check, the stored data and `event-fabric types` are unchanged. A TypeScript Application can still use the Register for names it writes as strings, such as Gateway Access maps.
