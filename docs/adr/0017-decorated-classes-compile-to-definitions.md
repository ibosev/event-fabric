# Decorated classes compile to definitions, and lint reads the compiled output

`event-fabric/decorators` adds an optional, Axon Framework-style way to declare Aggregates, Readmodels and Sagas: Command classes and Event classes made with `command(name, schema)` and `event(name, schema)`, and classes marked `@Aggregate`, `@Readmodel` or `@Saga` whose methods carry `@CommandHandler`, `@EventSourcingHandler`, `@EventHandler` or `@SagaEventHandler`. When a class is declared, it becomes the same definition `aggregate()` or an object produces. `createApplication` and the CLI replace each class in the config by that definition, so the kernel, stores, transport and Register generation never see a class. ADR-0002's existing forms and ADR-0013's Handles are unchanged, and both styles share one Application.

TypeScript 7 ships no compiler API, so no TypeScript ESLint parser runs on it. The lint rules therefore read what `tsc` 7 emits for standard decorators, the way this repository already lints `dist`: a method's role comes from its `__esDecorate` entry, and `before` Steps and `associationProperty` functions from the decorator call around them.

## Rules that follow

- **Standard (TC39) decorators only.** `experimentalDecorators` (Nest's default) is not supported, and decorated source must be compiled: Node's type stripping rejects decorators.
- **Classes hold no state.** A class is never constructed; handlers run unbound with `this` undefined. A superclass, a static member or an undecorated method throws where the class is declared. Fields and constructors cannot be told apart in compiled output at runtime, so the `decorated-class` lint rule reports them.
- **Names stay explicit.** Context and Aggregate name come from the qualified Command and Event names; Readmodel and Saga names are their keys in the config. Renaming a class or a method changes nothing stored.
- **Parameters are annotated.** Decorators cannot type method parameters. The Command or Event class is its own parameter type; `AggregateOf`, `StateOf`, `DocumentOf` and `SagaOf` take a schema. The decorator rejects a method whose signature does not match its message.
- **A missing or duplicate handler is caught when the class is declared, not at compile time.**

## Considered Options

- **A contract object per Aggregate with bound decorators** (`@account.CommandHandler("open")`, an earlier draft). It gives the same type checks, but every decorator repeats the contract, and the maintainer asked for Axon's form.
- **typescript-eslint on a TypeScript 6 alias.** It would point lint messages at source lines, but needs a second compiler for tooling; the maintainer chose TypeScript 7 only.
- **Class instances holding State** (Axon's mutable Aggregates). It needs rehydration, rollback and per-invocation isolation in the kernel; out of scope.

## Consequences

- Lint messages for decorated classes point at `dist` lines; source maps lead back. When a TypeScript 7 parser exists, the rules can read decorators in source as well.
- A class's definition rides on the class under `Symbol.for("event-fabric.component")`, so two copies of the package resolve each other's classes.
- `ApplicationConfig` accepts classes in `aggregates`, Readmodel group `models` and `sagas`; `ReadmodelGroupConfig` names a group as the config declares it.
