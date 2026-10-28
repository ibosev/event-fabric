# Names and payload types come from one generated Register

Narrowed by [ADR-0013](0013-typescript-authoring-through-typed-handles.md): TypeScript authoring gets its types from typed handles; the Register remains the path for JavaScript and for string names.

An Application writes Event, Command, Readmodel and Message names as plain qualified strings, such as `"billing.invoice.settled"`, and never maintains constants or payload types by hand. `event-fabric types` derives a types-only `declare module "event-fabric" { interface Register { ... } }` from the compiled model. Schemas are the single source of payload types: Command and Event payloads come from their schemas, Readmodel documents from the Readmodel schema, and queued Messages from the Worker's `schemas`. Every public signature reads its types from `Register`: `compile`, Reaction and Identity maps, `app.queue`, Worker `handles`, `commandTo`, `getDomain()`, `getReadmodels()`, Access-map keys, `respondWith`, `readmodel()` and `.await` patterns. So a misspelled name or a wrong payload fails `tsc` on the line that has it, and autocomplete lists only names that exist. A part with no schema keeps a checked name and an `any` payload. With no Register at all, every signature is as loose as before, so JavaScript and freshly migrated code compile unchanged.

The first typed authoring took `compile<Events, State, Commands>` type arguments. Applications then rebuilt the same names and payloads by hand in every Aggregate, as name maps, `Qualified` types, Command builders and copies of generated payload types. The type arguments also could not reach JavaScript Applications at all. TanStack Router, Prisma and Emmett show the alternative: types generated from, or inferred off, one declaration, with names as checked string literals.

## Considered Options

- **Explicit type arguments on `compile`.** Rejected: every Application keeps a parallel set of types and name constants, and JavaScript cannot pass type arguments.
- **Typed handles returned by `compile` (`invoice.event.settled`, `invoice.send.issue(...)`).** Rejected for now: payload inference from `as const` schemas (`FromSchema`) cannot follow the Application's file-based `$ref`s, is costly to compile, and silently widens to `any`. Handles also put imports back into the files that the migration codemod turns into plain string literals.
- **A generated runtime constants file, as the old stack uses.** Rejected again: it is a CommonJS file at the Application root, it needs a generate-by-loading cycle, and a typed literal checks the same names with nothing to import.

## Consequences

- `compile("billing", "invoice", { ... })` infers its context and name and needs no type arguments. State is inferred from `initialState`, and an author narrows it with `as State`. An Event or Command declared in the call but not generated yet stays loose until `event-fabric types` runs. Use sites are checked and definition sites stay open, so a new Event never blocks its own declaration.
- The Register is global to a compilation, so one `tsc` project holds one Application.
- Generated types can be stale. `event-fabric types --check` fails CI when they are, and the startup check still rejects unknown names at runtime.
- `typesMode: "names"` generates checked names with `any` payloads and loose State. A large JavaScript Application adopts name checking first and payload checking later.
- Reaction and Identity maps get checked keys where they are written: as `ReadmodelReactions<"group.model">`, `SagaReactions<State>` or `Identities` in TypeScript, and with `/** @satisfies {...} */` in JavaScript. A map built elsewhere and passed in loses excess-key checks.
- Runtime changes are limited to two. `commandTo(name, id, payload)` builds a `CommandInput` from a qualified name. A Worker's `schemas` are checked before a queued Message is recorded, so a schema is never type-only.
