# EventFabric

A TypeScript framework for event-sourced CQRS Applications: Aggregates, Readmodels, Sagas, Gateways and Workers.

One ESM package, for Node **24.15 or newer**, with named exports and CommonJS `require(esm)` support. The Event log and durable coordination live in Postgres. Readmodel groups choose an in-memory, relational Postgres or Mongo store; transport is in-memory or RabbitMQ (4.0 or newer, tested on 4.4: Command queues are quorum queues, and replicas share their Partitions by consumer priority, ADR-0015).

```sh
npm ci
npm run build
node examples/basic.mjs
```

The example records a Command and waits for its Readmodel's Checkpoint before reading the document. It needs no external services. The billing example, a complete TypeScript Application (Aggregate, Readmodel, Saga, Worker and Gateway) on Postgres and RabbitMQ that runs entirely in Docker, lives in its own repository, `event-fabric-billing`, with a CRM frontend. For an overview with flow diagrams, read [how EventFabric works](docs/architecture.md) and [how it compares with oblak](docs/comparison-with-oblak.md), the framework it succeeds. [Migrating an oblak Application](docs/migrating-from-oblak.md) explains how an existing Application moves over with `event-fabric migrate-app`. See [the glossary](GLOSSARY.md) and [architecture decisions](docs/adr/) for the contracts behind the implementation. [How Readmodel rebuilds work](docs/readmodel-rebuilds.md) follows a rebuild end to end, with measured times.

## Inspired by oblak

EventFabric is inspired by **oblak** and its packages (`cqrs-domain`, `cqrs-eventdenormalizer`, `cqrs-saga`, `eventstore` and their message buses), and it is an attempt to modernize them. oblak has served its Applications well: its way of writing Aggregates, Readmodels and Sagas as small declarative definitions is why they stayed readable as they grew, and EventFabric keeps that authoring style so existing Applications can move over with codemods ([ADR-0002](docs/adr/0002-authoring-interface-compatible-with-codemods.md)).

What needed refreshing is underneath. EventFabric is a clean-room TypeScript rewrite ([ADR-0001](docs/adr/0001-clean-room-rewrite.md)) for Node 24: generated types for Commands, Events and Readmodels; a durable Postgres log that consumers read directly instead of relying on broker delivery; a transactional Outbox for effects; Leases, epochs and Partitions so several replicas can share the work safely; schemas for Readmodels; and whole-group rebuilds into a shadow generation that is published atomically. [The comparison with oblak](docs/comparison-with-oblak.md) goes through the differences in detail.

## Author an Aggregate

```js
import { compile, only } from "event-fabric";

export const order = compile("sales", "order", {
  initialState: {},
  commands: {
    create: [
      only.ifNotExists(),
      only.ifValidatedBy({
        type: "object",
        properties: { title: { type: "string" } },
        required: ["title"],
        additionalProperties: false,
      }),
      (command, aggregate) => aggregate.apply.created(command.payload),
    ],
  },
  events: {
    created: (event, aggregate) => aggregate.set("title", event.payload.title),
  },
});
```

In TypeScript, declare the Aggregate with `aggregate` and TypeBox schemas (`Type`, re-exported from `event-fabric`). Every type comes from the call itself, with no type arguments and no generated file:

```ts
import { aggregate, only, Type } from "event-fabric";

const Created = Type.Object({ title: Type.String() }, { additionalProperties: false });
export const order = aggregate("sales", "order", {
  state: Type.Object({ title: Type.String() }),
  initialState: { title: "" },
  commands: {
    create: {
      schema: Created,
      steps: [only.ifNotExists(), (command, order) => order.apply.created(command.payload)],
    },
  },
  events: {
    created: { schema: Created, handler: ({ payload }, state) => state.set("title", payload.title) },
  },
});
```

Each schema is both the payload's type and the JSON Schema checked at runtime; `state` types `get`, `set` and `initialState`. A Precondition is a Step that throws, and per-Command Rules go in `rules`. The `steps` array takes `only.ifExists()`/`only.ifNotExists()` and Steps, but not the untyped `only.ifState`/`only.after`. A Command's payload type is the validated payload: Ajv fills `default`s, but TypeBox's types do not record them, so declare a defaulted field `Type.Optional`. TypeBox `Refine` and `Codec` are rejected, because Ajv would skip them.

Collect the Aggregates with `export const domain = defineDomain(order, ...)`. Then `domain.sales.order.created` is the qualified Event name `"sales.order.created"`, and `domain.sales.order.create(id, payload)` builds a typed Command. Key Reaction maps by those names and type them with `Reactions<EventsOf<typeof domain>, ReactionView<Document<typeof Schema>>>`; `Identities<EventsOf<typeof domain>>` does the same for Identities. `Document` types an optional field as absent or `null`; declare date-time fields with `DateTime()`, which reads back as a `Date`. `message(name, schema)` declares a queued Message: `app.queue(handle, payload)` and `worker(handle, { execute })` take it, and its schema is checked when it is queued. `compile` and string names typed by the generated `Register` (ADR-0012) remain the JavaScript path; ADR-0013 records the split. The billing example (`event-fabric-billing`) uses the domain. Editors follow and rename Events and Commands through their handles. Rename a Command freely, but never an Event that is already recorded: Event names are permanent, and Upcasters change payloads, not names. Starting an Application (and `migrate`) refuses a log that holds an Event name no Aggregate declares, and lists those names. To retire an Event, keep it declared with its schema and a no-op handler. After renaming anything, run `event-fabric types` so the generated Register matches; `types --check` fails until you do.

### Decorators (Axon style)

`event-fabric/decorators` declares the same components as classes, the way Axon Framework does. Commands and Events are classes that carry their qualified name and TypeBox schema; a class marked `@Aggregate`, `@Readmodel` or `@Saga` becomes exactly the definition `aggregate()` or an object would, so both styles share one Application, Event log and set of guarantees.

```ts
import { only, Type } from "event-fabric";
import { Aggregate, CommandHandler, EventSourcingHandler, command, event, type AggregateOf, type StateOf } from "event-fabric/decorators";

const Created = Type.Object({ title: Type.String() }, { additionalProperties: false });
export class CreateOrder extends command("sales.order.create", Created) {}
export class OrderCreated extends event("sales.order.created", Created) {}
const OrderState = Type.Object({ title: Type.String() });

@Aggregate({ state: OrderState, initialState: { title: "" } })
export class Order {
  @CommandHandler(CreateOrder, { before: [only.ifNotExists()] })
  create(command: CreateOrder, order: AggregateOf<typeof OrderState>) {
    order.apply(OrderCreated, command.payload);
  }

  @EventSourcingHandler(OrderCreated)
  created({ payload }: OrderCreated, state: StateOf<typeof OrderState>) {
    state.set("title", payload.title);
  }
}

createApplication({ aggregates: [Order, existingAggregate], readmodels: { sales: { models: { orders: Orders } } }, sagas: { fulfilment: Fulfilment } /* ... */ });
await application.sendCommand(CreateOrder.to("one", { title: "First order" }));
```

- `@Aggregate({ state, initialState, rules?, idGenerator?, eventEnricher? })` takes its Context and name from the messages it handles, which must all belong to one Aggregate. `@CommandHandler(Command, { before?, rules? })`: `before` holds `only.*` checks and earlier Steps, in order, then the method runs, then `rules`. `@EventSourcingHandler(Event)` folds. `event(name, schema, { schemaVersion, upcasters })` carries Schema versions.
- `@Readmodel({ schema, indexes?, initialState? })` with `@EventHandler(Event, { associationProperty?, before? })`, and `@Saga({ reads?, partitions?, from? })` with `@SagaEventHandler(Event, { associationProperty? })`. `associationProperty` is the Identity: a payload key, or a function returning ids, a filter or `null` to skip; the default is the Stream id. The key in `models` or `sagas` is the permanent name; renaming a class changes nothing stored.
- Annotate parameters: decorators cannot type them. The message class is its own type; `AggregateOf`, `StateOf`, `DocumentOf` and `SagaOf` take the State, document or Saga state schema. A handler whose signature does not match its message fails to compile (TS1241, with the reason below it).
- Old and new styles meet in both directions. `eventOf(invoice, "settled")` gives an `aggregate()` Event a class for decorated Readmodels and Sagas; `EventTypes<typeof A | typeof B>` keys object `Reactions` by decorated Events; `X.to(id, payload)` and `defineDomain()` Handles both build Commands anywhere.
- Classes hold no state: they are never constructed, handlers run with `this` undefined, and a class may not extend another class, have static members or undecorated methods. A missing, duplicate or misplaced handler throws where the class is declared. Standard (TC39) decorators only: `experimentalDecorators` is not supported.
- Decorated source must be compiled with `tsc`; Node's type stripping rejects decorators. The ESLint rules read the compiled output (`eslint dist`), so they need no TypeScript parser: TypeScript 7 ships none.

The billing example (`event-fabric-billing`) mixes both styles: its Subscription Aggregate, `subscriptions` and `plans` Readmodels and `renewals` Saga are decorated.

Preconditions and handlers are ordered Steps; Rules (`only.after`) run last. Only throwing rejects. Event handlers and Readmodel Reactions must be deterministic: use the Event's `timestamp`, and perform external calls in Workers. Command Steps can use `aggregate.now` and `command.now`, and log through `app.logger`. `aggregate.locals` passes temporary values to Rules.

A Command id is accepted at most once (ADR-0014). The Event store records each accepted id with a fingerprint of the name, Stream as sent, and payload, in the same transaction as its Events. A Duplicate Command, one whose id is already recorded, does not run again: it returns the first outcome with `duplicate: true`. The same id with a different Command is rejected with kind `conflict`. Rejected and failed Commands leave no record, so a retry runs again. This covers Outbox re-sends, which keep the Command id, and lets callers make retries safe by passing a stable `id`. Queued Messages are still delivered at least once and carry a stable `idempotencyKey` for the external provider.

## Run an Application

Use `createApplication(config)` with `name`, `aggregates`, `store`, and `transport`; optionally provide `readmodels`, `sagas`, `workers`, `gateways`, `services`, `plugins` and a `logger`. [examples/basic.mjs](examples/basic.mjs) is a complete configuration.

```js
await application.start();
const outcome = await application.sendCommand({
  stream: { context: "sales", aggregate: "order", id: "one" },
  name: "create",
  payload: { title: "First order" },
});
// outcome.status: accepted | rejected | failed | unknown
```

`accepted` carries the committed Events. `sendCommand(...).await('sales.order.created')` filters those Events (a list of patterns takes the first Event matching any) and fails promptly if none match; on a rejected Command it throws the `Rejection` (with `kind`, `code`, `details` and `thrown`, the name and own fields of the error a Step threw), so a Gateway answers it as a rejection. `awaitCorrelated(input, pattern, options)` subscribes before sending when a downstream Event is needed; it is also on Gateway handles (behind the domain Access map and `commandMetadata`), Task contexts and the plugin context. `subscribeEvents(handler)` receives relayed Event notifications and drops any at or below the last Position it saw, since a deposed relay may republish. The relay jumps to the head when the oldest unpublished Event is more than `relayMaxLag` (default 60 000 ms, measured between Event timestamps) behind the newest. Positions are objects internally; `encodePosition` / `decodePosition` provide opaque HTTP tokens.

**Lanes.** Each Command Partition has two queues, interactive and bulk, handled at the same time, so a backlog of bulk Commands never delays a user's Command. The sender picks the lane: `sendCommand(input, { lane: 'bulk' })` and `awaitCorrelated(…, { lane })`, interactive by default. Saga and Schedule Commands (through the Outbox), Worker sends and Task sends go bulk. A Gateway's Commands are interactive unless its `lane` (a lane, or `(ctx) => lane`) says `bulk`, for machine callers such as an import API or webhooks. Each lane keeps its send order. Across lanes, an interactive Command may overtake a bulk Command to the same Stream, but the two never run at once: a replica handles one Stream's Commands one at a time. Both lanes of a Partition share its consumer priority, so one replica normally holds both; while ownership moves, the revision check still refuses a stale write and the Command retries. A slow handler holds back only its own lane and its own Stream. On RabbitMQ the interactive queue keeps the original name, and the bulk queue adds `.bulk`.

`application.getDomain().sales.order('one')` exposes Command runners and `load({ atRevision })`. `getReadmodels()` and `getServices()` are also public. Use `stop()` to drain work while retaining clients, or `close()` to drain and close owned resources. `close()` is terminal: a second call returns the same promise, and a closed Application cannot start again.

### Services

Services are the Application's own clients and helpers: provider clients, pools, CRUD models, factories. Command Steps, Preconditions, Rules, id generators and Saga Reactions get them from `app.getServices()`, and Workers, plugins and Tasks from `ctx.services`. Readmodel Reactions share these services unless the Application supplies Readmodel helpers (below). A callable service stays a value: `getServices().mailer()` is the Application's own call.

Pass a ready object as `services` (the Application borrows it and never closes it), or build it in `setupServices` (the Application owns what it registers there):

```ts
createApplication({
  // ...
  async setupServices({ resources, logger }) {
    const mail = resources.adopt(await openMailTransport(), (client) => client.close());
    return { mailer: () => createMailer({ mail, logger }), registry: createRegistry() };
  },
});
```

`setupServices` runs once, when `initialize()` first runs (`start()`, `rebuild`, a Task), before any Reaction, startup rebuild, plugin or Task can use the services; concurrent callers share it. `resources` is a native `AsyncDisposableStack`: register each resource's cleanup right after acquiring it. If setup throws, the cleanup registered so far runs in reverse order and the Application stays failed, without retrying; when a cleanup fails too, both errors are kept in an `AggregateError`. `close()` waits for a start or setup in progress, stops (shutdown hooks can still use services), runs the cleanup last-acquired first (a failing one does not skip the rest), then closes the configured stores and Transport. Do not register those adapters in `resources`, or they close twice. Before setup finishes and after `close()`, `getServices()` throws. `createApplication` itself opens nothing, and `types` and `migrate` never run setup, so keep connections out of config module top level.

Cleanup runs after work drained within `shutdownTimeout`. Work that ignores its `signal` and outlives the deadline may still be running and find its client closed: the timeout cannot stop it.

Register the Application services contract once, as generated types do:

```ts
declare module "event-fabric" {
  interface Register {
    services: { mailer: Mailer; registry: Registry };
  }
}
```

`CommandApp`, `ReactionApp` (Sagas), `WorkerContext`, `PluginContext`, `TaskContext` and `Task` default to it, so separately declared handlers are checked, and `createApplication` then requires `services` or `setupServices` to supply every one; a test passes fakes the same way. Decorated methods and standalone functions need an annotation (`app: CommandApp`). Reusable code names only what it needs (`CommandApp<Pick<Services, "registry">>`); an Application with another contract passes it explicitly, `createApplication<OtherServices>(config)`, since the contract is never inferred from the config. Checked JavaScript uses `/** @type {import("event-fabric").Task} */`; unchecked JavaScript stays untyped. Services live as long as the Application: keep request or attempt state in the context arguments, not in services.

Readmodel helpers are opt-in and apply to live processing and every rebuild. Omit `readmodelServices` (or leave it `undefined`) to keep existing `app.getServices()` access to `services` or the ready `setupServices` result. Supply an object to give Readmodel Reactions only those helpers, or `{}` for none:

```ts
type ReadmodelServices = { normalizeOwner(name: string): string };
declare module "event-fabric" {
  interface Register { readmodelServices: ReadmodelServices }
}
createApplication({
  // ...
  readmodelServices: { normalizeOwner: (name) => name.trim() },
});
// In a Reaction annotated with ReadmodelApp:
// view.set("owner", app.getServices().normalizeOwner(event.payload.owner));
```

`ReadmodelApp` and `@EventHandler` use `Register.readmodelServices` when declared, otherwise `Register.services`. The config follows the types: once the helpers contract is declared, `readmodelServices` is required, even for an empty contract; while `Register.services` is declared without it, `readmodelServices` is refused, since Readmodel types would still promise services the runtime withholds. Local contracts use `ReadmodelApp<Helpers>` and `createApplication<Services, Helpers>` / `ApplicationConfig<Services, Helpers>`. Readmodel helpers are borrowed, have no setup or cleanup, and are read-only: assigning or deleting one of their properties throws, so a Reaction cannot keep state there. Share the functions or objects already in `services` instead of duplicating them. Keep helpers free of mutable data, clocks, randomness and external effects. A read-only repository still reads changing data, so it does not qualify: record the required values in Events instead. The runtime controls which object reaches a Reaction and guards only its top level; it cannot prove a helper pure or stop Application imports from reaching other resources, so review helpers and keep the determinism lint. Legacy service access stays available, but reading mutable data can still produce different documents on rebuild; switching to Readmodel helpers is an explicit migration step.

A dependency-injection container is optional and stays inside `setupServices`. Awilix fits object/factory graphs: create async clients first, register them as values and their cleanup once in `resources`, resolve the services the handlers use, and return them; never also give the container a disposer for the same client.

### Stores and adapters

| Import                       | Exports                                                                                    |
| ---------------------------- | ------------------------------------------------------------------------------------------ |
| `event-fabric`               | Authoring, kernel, Application runtime, Postgres Event store, validation, Gateway helpers  |
| `event-fabric/postgres`      | `createPostgresReadmodelStore`                                                             |
| `event-fabric/mongo`         | `createMongoReadmodelStore`                                                                |
| `event-fabric/rabbitmq`      | `createRabbitMQTransport`                                                                  |
| `event-fabric/testing`       | In-memory Event/Readmodel stores and transport, shared in-memory broker, Partition helpers, `processFixture` for Sagas and Workflows |
| `event-fabric/compat`        | Helpers that keep the old framework's behavior in migrated code ([migration guide](docs/migration.md#old-behavior-is-the-default)) |
| `event-fabric/workflows`     | `workflow()`, `signal()` and the Workflow, Signal and query types ([Workflows](#workflows)) |
| `event-fabric/decorators`    | Axon-style `@Aggregate`, `@Readmodel`, `@Saga`, handler decorators, `command()`/`event()`   |
| `event-fabric/eslint-plugin` | Named `rules` and configuration for determinism checks; lint the compiled output           |

`mongodb` and `amqplib` are optional peers. Importing the root never loads them. Postgres pools and Mongo clients passed by the Application remain Application-owned. The Event store accepts a `connectionString` (with `poolOptions` for the pool it creates) or an existing `pool`, a `schema`, indexed metadata paths, Upcasters, `metadataMaxBytes` (default 8192) and a `metadataSchema` checked on appends, imports and Outbox writes. `createApplication` registers every Aggregate's `eventSchemas` with the store at startup through `store.registerUpcasters` (identical re-registration is a no-op, a conflicting one throws); Event and metadata schemas are checked with the framework's validator unless the store is given its own `validate`. Give the store the same `metadataMaxBytes` and `metadataSchema` as the Application: the Application checks Commands on arrival, the store checks every write. `readAll` filters `eventNames` by fully qualified name (`context.aggregate.name`) only; a reverse read treats `fromPosition` as an exclusive upper bound. `importSagas` accepts only the current log head Position (`null` only for an empty log).

The optional Event-store `readPage({ fromPosition, toPosition, eventNames, limit })` returns `{ events, through }` in ascending Position order. `fromPosition` is exclusive and `toPosition` inclusive; `limit` defaults to 1000. Omit `eventNames` for all types, or supply `[]` for none. `through` is safe global progress, including excluded Events: commit it only after processing every returned Event. An empty page can still advance it. PostgreSQL selects a complete visible prefix and bounded ranges in the Event-type/Position index in one snapshot, then loads only the selected payloads. Run `event-fabric migrate --dry-run`, then `event-fabric migrate` with the deployment's config before using the optimization; the new `events_type_position` index builds concurrently using the existing restartable migration path. This adds index storage and work on appends. Older Event-store adapters remain supported: the runtime captures `head()` before a bounded `readAll()` and advances to it. So such an adapter's `head()` must never be ahead of `readAll`: every Event at or before it must already be readable, as the bundled stores guarantee by reading below the oldest running transaction. Applications refuse to start while Event-store migrations or indexes are pending, including an index left invalid by an interrupted build.

Run migrations explicitly: `await store.migrate()` or `event-fabric migrate`. Migrations are versioned steps; only unapplied versions run, each in its own transaction. Metadata indexes and the Outbox due-time index are built with `CREATE INDEX CONCURRENTLY` outside a transaction, so appends keep running. `migrate({ dryRun: true })` returns the SQL still pending. Startup does not migrate framework tables; it refuses to start until `migrate` has run. Postgres Readmodel stores add missing child-table indexes concurrently when opened, including existing generations, and give tables from earlier versions an index on `_id` in binary order. Use a dedicated Postgres role with `idle_in_transaction_session_timeout` configured to bound cluster-wide reader stalls; framework write transactions also apply 30-second statement and idle timeouts.

Set `readmodelStore` as the Application default; a Readmodel group may override it with `store`. Readmodel groups declare `{ store?, models, reads? }`; each model declares `{ schema, initialState?, identity?, reactions, indexes?, reactionSets? }`. Reactions use fully qualified Event names. `reactionSets` names further `{ identity?, reactions }` sets on the same documents: each set selects its own documents and has its own markers, and runs after the model's own Reactions, in declaration order (the old grouped `<event>:<group>` keys). A Reaction array may start with markers: `only.ifExists()` handles only existing documents, `only.ifNotExists()` only absent ones, and `only.if(predicate)` skips the whole Reaction, creating nothing, when `predicate(event, view, app)` returns false; the same markers apply to Saga instances. Schema fields map to typed columns and child tables in Postgres; `"x-store": "json"` explicitly opts a field into JSON storage. Documents are closed even with value validation disabled. A key a document's schema does not declare is refused, which halts the group on that Event; `"x-undeclared": "drop"` on an object schema drops such keys in it and below instead, reporting each path once as an `EventFabricWarning` (`"reject"` restores refusal below it). Use it where Event payloads that clients shape are copied into documents. Nullable fields use `[type, "null"]`. An optional field a document leaves out stays absent, distinct from an explicit `null`, in the memory and Mongo stores; Postgres columns read both as `null`. Filters treat absent as `null` on every store (`{ field: null }` matches both, as in Mongo). Required non-nullable fields are rejected when `null` or absent, even with value validation disabled, and new Postgres generations declare them `NOT NULL`. Arrays of arrays need `"x-store": "json"`. Index paths must name a scalar, an array of scalars or a json field; a path into an array of objects (`items.sku`) indexes its Postgres child table, and one compound index cannot span different arrays of objects or point inside a json field. Dates round-trip as `Date`. A schema change uses an exact hash migration or automatically rebuilds a shadow generation before the group starts. `start()` does not wait for that rebuild: Commands, Workers, the Outbox and every other group start at once, while required rebuilds run behind them in dependency order, one replica each, resuming a pending generation left by an interrupted or failed attempt (a failure is logged and retried from its Checkpoint). Until a group's new generation swaps in, its reads, `waitFor` and subscriptions throw `ReadmodelRebuildingError` (HTTP 503 with `Retry-After`; a Command whose `respondWith` names that group is still recorded and answers 202), and Readmodel groups and Sagas that declare it in `reads` start only after the swap. A migration's `up(native)` runs in one transaction with the hash update and receives the in-memory generation as a `Map` of model name to documents by id, Mongo `{ session, collection(model), database }` (both handles inject the transaction's `session`), or Postgres `{ schema, tableFor(model), query, pool }` (`query` is transaction-bound; `pool` is not). Application `schemas` and per-group `schemas` resolve named references (group declarations win). Store writes use the Application validator, overridable with a group `validator`; async formats are supported. Store validation uses a separate copy and rejects validators that fill defaults, transform values or otherwise change the supplied data.

**Readmodel storage.** A new generation's storage is named after its group, model and generation tag, the first 8 hex characters of the generation's id:

| Store    | Documents                                                                                           | Group and generation records          |
| -------- | --------------------------------------------------------------------------------------------------- | ------------------------------------- |
| Postgres | table `rm_<group>_<model>_<tag>` in the store's `schema`; child tables `<table>__<path>`              | `ef_rm_groups`, `ef_rm_generations`    |
| Mongo    | collection `<model>_<tag>` in the group's database, `<database>-<group>-readmodels` (`<name>` for groups sharing one) | `<prefix>_groups`, `<prefix>_generations` in `database` |

For example, the `billing` group's `invoices` are `rm_billing_invoices_3f9a1c2e` on Postgres and `invoices_3f9a1c2e` in `billing-billing-readmodels` on Mongo. `createMongoReadmodelStore`'s `databases` option (group → name) puts several groups in one database, `<database>-<name>-readmodels`, as an old configuration sharing a database did; a native pipeline's `$lookup`, `$graphLookup` and `$unionWith` may then name a model of any group in that database. Their collections stay apart, since each ends in its own generation's tag. A changed name applies from the group's next generation, so rebuild the group to move it. Characters outside `[a-z0-9_]` (Postgres, lowercased) or `[A-Za-z0-9_-]` (Mongo) become `_`. A name past the database's length limit (63 bytes for Postgres identifiers and Mongo database names), or one another model of the same generation already uses, keeps a prefix and ends in a short hash. Each generation records its names when it is created, so generations created before readable names keep their hashed `rm_<hash>` tables or `<prefix>_rm_<hash>` collections until the group's next rebuild. The live generation is the group record's `active`, or `store.activeGeneration(group)`. A rebuild's shadow sits next to it under its own tag until the swap. The Mongo store needs a replica set on MongoDB 5.0 or newer: writes are transactions, and reads use snapshot sessions. The Mongo store's `databases` option (`{ <group>: <name> }`) places groups in a shared `<database>-<name>-readmodels` database instead; a change applies at each group's next generation. Generation records name their database in full, so a store copied under another `database` refuses to read or write a group until its generation records are repointed at the copy's databases.

Every instance must give a group the same store. Each instance advertises the `storeId()` of every Readmodel group it runs (for example `mongo database billing (1f0c2a9e)`, an id kept in the store itself). An instance on a store that fewer live instances use takes none of the group's Leases and logs an error, and on a tie no instance takes them. `consumers status` shows the split on the group's rows. An instance that already holds the group lets go at its next Lease renewal, within a third of the Lease duration.

`application.readmodels.detail.order(id, { minPosition })` returns `{ document, position, behind }`. Repositories also provide `find`, `findOne`, `count`, `stream`, and adapter-specific `native()` for internal Application code (`native(scope)` with an equality filter is the scoped, read-only Mongo view a Gateway hands out). In a Mongo native pipeline, `$lookup`, `$graphLookup` and `$unionWith` name the joined model, which resolves in the reading group first and otherwise in the one other group of the same database that declares it; `native(scope, { joinScope })` limits each joined model to the equality filter `joinScope(group, model)` returns (a Gateway's scoped view passes the caller's Access filter, so a model without a rule is Forbidden). A join on a field no declared index of the joined model starts with warns once as an `EventFabricWarning`, since it scans that model per input document. When the document type is known (a generated Register, or `ReadmodelRepository<Document<typeof schema>>`), filters and `sort` are typed: unknown paths, wrong value types and operators on the wrong kind of field fail to compile. Untyped code keeps the loose `Filter`. Query filters support equality/array contains, `in`, `ne`, `nin`, ranges, `exists`, `elemMatch`, `oneOf`, and `and`; options provide `sort`, `from`, `size`, and `q`. Object equality ignores property order; arrays keep element order. Field-level `oneOf` accepts predicate alternatives. Array ranges match an element. `q` requires a declared text index: case-insensitive substring terms are AND-ed, quoted terms match phrases, and a leading `-` excludes a term or phrase. Postgres retains a generated `tsvector` and uses its rank when no sort is given; native text tokenization never filters out portable substring matches. Portable `q` scans documents selected by the other filters, so use adapter `native()` for application-specific indexed text search when that cost becomes material. Sorting compares values (dates by time, strings by UTF-8 binary order) and breaks ties by `_id`, identically on every store. Reads use a snapshot and never wait for batch commits. On every store, `stream` reads pages of 100 in one snapshot, ordered by the sort and `_id`. `find` and `findOne` page the same way and stop once they hold `from + size` matches. `count` counts in the database when every condition compares a scalar field outside arrays and json fields with a non-null value of the field's type, and page by page otherwise. On Postgres the snapshot lasts until iteration ends: consume or close streams promptly, since generation cleanup or schema migrations may wait for them. On Mongo the server keeps a read's snapshot for `minSnapshotHistoryWindowInSeconds` (default 300 s), and a longer stream fails with an error naming that setting. New Postgres generations store strings in binary order (`COLLATE "C"`), and a declared index lists its fields and then `_id`, so sorted pages are index range scans. Older generations get the `_id` index when the store opens, and indexed sorts after a rebuild. Text ranking without an explicit sort, and sorts on arrays, json or array-nested paths, still load all matches. Declared cross-group reads wait for the dependency's Checkpoint. Rebuilds with cross-group reads can observe later data.

Within a Readmodel Reaction, portable queries to its own group read the current generation plus changes staged by earlier Reactions in the batch. This also applies during shadow rebuilds. Use portable reads there: native access cannot include staged writes, and waits for the same group's uncommitted Checkpoint are rejected.

Live Readmodels, rebuilds and Partition layout drains derive their Event-name union from the group's Reactions, including Reactions from different Contexts. They advance Checkpoints through unrelated Events without loading their payloads or running their Upcasters. Rebuilds retain unfinished pages across the fold cutoff (50 ms live, 2 s for rebuilds); document changes and progress still commit atomically. The shared fold bulk-loads documents selected by Stream ids or Event-derived Identities. Query Identities still select against the state at their Event, including earlier staged changes. Live batches remain bounded at 100 selected Events, rebuild batches at 5,000; every batch writes each model's documents with one delete and multi-row inserts per table, on Postgres. A rebuild reads its next page and commits each batch while it folds the next one, against the uncommitted documents of the batch still committing; one commit is in flight at a time, in order. A partitioned group (`partitions: N > 1`) on the Postgres Readmodel store, rebuilt through the CLI, folds a new generation in N worker threads, one per Partition: the rebuild reads each page once and sends each Event to the workers owning its documents, and each worker commits its own documents and progress. Once every worker has reached the same Position, the single loop continues from there to the head and swaps. A stopped sliced rebuild resumes only through the CLI with the same `partitions` ([stopping and resuming](docs/readmodel-rebuilds.md#9-stopping-and-resuming)); workers start only with more than one CPU available, and a rebuild container gains up to about one CPU per Partition. [Readmodel rebuilds](docs/readmodel-rebuilds.md) describes the whole path. Rebuilding a new group whose Event types first appear late in the log skips the unrelated prefix automatically; a new group over existing types still reconstructs their earlier history.

Postgres adds null markers to existing child tables without changing existing object values. If a Readmodel stored null object-array items before this fix, rebuild that group from Events: the old layout cannot distinguish those nulls from objects whose fields are all null.

Sagas declare `{ initialState?, identity?, reactions, reads?, partitions?, from? }`. A Reaction's `app.tell` / `app.getDomain` records Commands in the same transaction as state and Checkpoint; `app.queue(name, payload, options)` records a queued Message, and `{ replyTo: "<saga message>" }` asks its Worker to reply to this instance (see below). Saga `destroy()` removes state and retains its Outbox entries. Saga state is stored as JSON after each Reaction: a Date it sets reads back as ISO text in later Events. A Command without a Stream id (`getDomain().c.a().create(...)`) gets its id when handled. Sagas can be imported at a known Position and are never rebuilt. Readmodel Reactions reject every Command path, including `tell()` chains and `getDomain()`, and cannot queue Messages. An Outbox Command whose target rejects it is not retried and does not hold back later Commands to its Stream; it is dead-lettered as `outbox-rejected` with its Rejection, counted in `eventfabric.outbox.rejected`, and `dead-letters replay` re-sends it with the same Command id once the refused data is fixed. A delivery that fails because the broker connection is gone stays pending and does not count toward its five attempts; a replica that has lost the broker releases the `outbox` and `relay` Leases and stops listing them, so another replica takes them. Nobody else is told, so a refusal the business must react to belongs in an Event the target Aggregate records (e.g. `chargeRefused`), not in a Rejection. Pending Outbox replace keys apply until atomic dispatcher admission; an admitted delivery retains its retries and may run alongside a later replacement.

**Where a new Saga starts.** A newly deployed Saga or Workflow does not replay the log: replaying it would send a Command or Message for every old Event. `from` chooses where it starts reading, once, when its group is new (no Partition has a Checkpoint):

```ts
sagas: {
  reminders: { reactions, from: "now" }, // default: only Events recorded after the first replica running it started
  audit: { reactions, from: "beginning" }, // every Event in the log
  promo: { reactions, from: new Date("2026-11-01T00:00:00Z") }, // Events recorded at or after that instant
  resumed: { reactions, from: { position: token } }, // Events after a Position, as the EventFabric-Position header returns it
}
```

`@Saga({ from })` and `workflow(name, { from })` take the same values. The first replica to start the group writes the resolved Position as every Partition's Checkpoint in one transaction, so all replicas agree; restarts, Lease handovers and `partitions` changes keep it. Changing `from` later has no effect on an existing group and logs a warning at startup; to replay history, retire the group and deploy it again (see [Operations](#operations)). A `Date` may also include an Event stamped just before it but committed after the first one at or after it, because Positions follow transaction order. Readmodel groups have no `from`: they always fold the whole log. `from` is checked when the Application is created. Run `event-fabric migrate` before starting this version: migration 10 gives every Saga and Workflow group that already has rows its `layout:` row, so it counts as started and keeps its Checkpoints, including one halted before its first Checkpoint, which then handles that Event instead of skipping it. It records no `from` for them, so they never log the changed-`from` warning. A test that records Events before starting the Application under test, and expects a Saga to see them, sets `from: "beginning"`. Custom persistence adapters implement `positionAt` and `startConsumer` (`test/contracts/consumer-start.ts`); the stored start lives in the `layout:<group>` row the store already keeps for Partition layouts.

Workers declare `{ handles, execute, concurrency?, timeout?, retry? }`. `execute(message, ctx)` receives `idempotencyKey`, `attempt`, `signal`, `send`, `sendSagaMessage`, `services`, `readmodels` (read as a Task reads them; they may lag the Message's cause), and a logger. Honor `signal` for cancellation. A monotonic deadline also detects synchronous work that blocks the timer; cancellation cannot undo an external effect already performed. `throw permanent(error)` dead-letters immediately; transient failures schedule durable retries, five attempts by default. A provider without idempotency support can repeat an effect after a crash.

A Saga Reaction or Saga Message handler that queues with `app.queue(message, payload, { replyTo: "receipt" })` gets a reply: the Worker runtime sends the Saga's own Saga Message `receipt` to the instance that queued it, `{ outcome: "ok", value }` with what `execute` returned once it succeeds, or `{ outcome: "failed", error, attempts }` once the Message is dead-lettered (the dead letter is still kept). The Message's envelope carries `replyTo: { saga, instance, name }`; its Idempotency key does not change. The Saga must declare the reply under `messages` (or `@SagaMessageHandler`), checked when the Message is recorded; only Sagas ask for replies. To check it when compiling, declare `messages` first and type the Reactions' app as `ReactionApp<MessagePayloads, RegisteredServices, keyof typeof messages>`: `replyTo` then takes only those names (billing's `receipts`). The reply's id is derived from the Idempotency key and the outcome, so a Worker re-run after a crash repeats a pending reply rather than adding one; delivery is still at least once, so a reply handler must tolerate a repeat (a destroyed instance drops it). A reply that cannot be sent fails the attempt, which retries; after a dead letter it is logged as `Worker reply not sent`, and replaying the dead letter runs the Worker again. `ReplyOf<typeof myWorker>` types the payload from the Worker's return type (`worker(handle, ...)` keeps it):

```ts
app.queue(sendReceipt, { invoiceId }, { replyTo: "receipt" });
const receipt: SagaMessageFunction<ReplyOf<typeof receiptSender>, State> = ({ payload }, saga, app) => {
  if (payload.outcome === "failed") return saga.set("failure", payload.error);
  app.tell(domain.billing.invoice.confirmReceipt(saga.id, { reference: payload.value }));
  saga.destroy();
};
```

### Named Command Schedules

A Command Step can create or replace a one-shot Schedule for its own Stream. The Schedule name is scoped by Context, Aggregate and Stream id. Schedule changes and Events commit together; a rejected or failed Command leaves both unchanged.

```ts
// Inside an Invoice Command Step:
app.schedule("overdue", "2027-01-01T00:00:00.000Z", {
  stream: { context: "billing", aggregate: "invoice", id: invoice.id },
  name: "markOverdue",
  payload: {},
});
// In a different Command, after full payment or voiding:
app.cancelSchedule("overdue");
```

Use a `Date` or an ISO timestamp with an explicit timezone; the framework stores UTC. Names contain 1–128 letters, digits, dots, underscores or hyphens. A Command can change a given Schedule once. The target must be its own full Stream, and the framework generates each attempt's Command id. These methods are available to both typed and decorated Command handlers after Stream load. Sagas request the owning Aggregate's Command; ending the requesting Saga does not cancel the Schedule. Recurrence remains outside this interface; a Saga that waits on its own state uses a Deadline (below).

An accepted cancellation returns `outcome.cancellations[name]`: `cancelled` before dispatch admission, `dispatched` after it, or `inactive` when no Schedule exists. Cancellation and admission serialize atomically. An admitted Command may already be travelling; a cancelled or superseded attempt is durably accepted with no Events or effects, even on manual dead-letter replay. Replacement also fences an old handler that is already running. Events accepted before cancellation remain in history. A Duplicate Command returns its original cancellation result and cannot cancel a newer attempt.

`await application.getSchedule(stream, name)` returns `{ stream, name, commandId, dueAt, dispatchedAt? }` for the current Schedule, or `null`. Acceptance consumes it unless that Command replaces it. Failed/rejected deliveries retain it for the existing Outbox retry/dead-letter workflow; discarding their dead letter clears only that attempt. Inspection is current state, without a separate Schedule history. Rehydration and Readmodel rebuilds create no Schedule effects.

Due time is the earliest business-check start. The dispatcher wakes at the next eligible due time and retains polling as recovery for missed notifications. The kernel also refuses early execution: an attempt that arrives up to one second before its due time on the executing Application's clock waits for it, and an earlier one returns `unknown` and is retried by the Outbox after a second. Keep database and Application clocks synchronized: no-early execution is relative to the executing Application's clock. `eventfabric.schedule.lateness` measures business-check entry minus the original due time, once per attempt when it is accepted or rejected (failed deliveries that are retried are not counted again); `eventfabric.schedule.missed` and a warning report starts more than 100 ms late, including catch-up after downtime. The 100 ms healthy-operation target is not a hard real-time guarantee: visibility barriers, busy Command Partitions, pool contention, clock skew, downtime and overload can cause misses. Validate the target under the deployment's workload before relying on it. Retry eligibility never overwrites the original due instant; unknown outcomes retain the existing indefinite retry policy.

Run `event-fabric migrate` before starting this version: migration 5 adds Schedule state and durable cancellation results. Upgrade all Command/Outbox replicas together before using Schedules; older replicas do not understand the attempt fence. Custom persistence adapters must implement `getSchedule`, atomic `schedules`/`scheduleGuard` append options, cancellation results, admission/discard semantics, `pendingOutbox` exclusions and `nextOutboxDelay` on the store clock. Both supplied adapters implement these contracts.

### Saga Messages and Deadlines

Domain Events are what Aggregates record in the Event log. A Saga Message is a System Event instead: addressed to one Saga instance, due now or later, and never in the log, so Readmodels, rebuilds and Command waits never see it (ADR-0020). A Deadline is a named Saga Message an instance sends itself.

```ts
sagas: {
  connector: {
    reactions: {
      "crm.file.opened": (event, saga) => saga.deadline("remind", event.payload.remindAt),
      "crm.file.determined": (_event, saga) => saga.cancelDeadline("remind"),
    },
    deadlines: {
      remind: (message, saga, app) => app.tell(remindCommand(saga.id)),
    },
    messages: {
      done: (message, saga) => saga.set("done", true),
      report: { start: true, schema: Report, handle: (message, saga) => saga.set("month", message.payload.month) },
    },
  },
}
await application.sendSagaMessage("connector", "done", {}, { to: "file-1" });
await application.sendSagaMessage("connector", "report", { month: "2026-10" }, { at: "2026-11-01T06:00:00Z" });
```

`saga.deadline(name, at, payload?)` sets or replaces the instance's Deadline `name`, and `saga.cancelDeadline(name)` cancels it; both commit with the handler's state, and destroying the instance cancels its Deadlines. A Deadline needs a handler in `deadlines`. A Saga Message reaches the instance in `to`, else the one its handler's `identity` (payload key or function) picks; a `start: true` handler creates a missing instance, by default under the Saga Message's id, and any other Saga Message to a missing instance is dropped. `schema` checks the payload when it is sent. Handlers get `(message, saga, app)` like a Saga Reaction; `message` carries `saga`, `instance`, `name`, `deadline` (Deadlines), `payload`, `dueAt`, Metadata, Causation and Correlation. Decorated Sagas use `@SagaMessageHandler(Type, { start?, associationProperty? })` with `class Done extends sagaMessage("done", schema) {}`, and `@SagaDeadlineHandler("remind")`.

Senders: Saga handlers (`app.sendSagaMessage`, committed with their state), Workers (`ctx.sendSagaMessage`, inheriting the Message's Metadata and Correlation), Tasks, plugins and `application.sendSagaMessage`. A Gateway sends one with `handle(req).sendSagaMessage` only where its Access map's `sagaMessages` names `<saga>.<message>` (or `<saga>.*`, `*`) as `true` or a function of `(ctx, { payload, to })`; anything else is forbidden. A Gateway send names its instance in `to`, so the rule sees the target, and cannot set `id`. `options.id` makes a send idempotent: a Saga Message or Deadline already pending under that id or name is kept, which suits importing an old scheduler's tasks.

The owner of the instance's Partition handles a Saga Message once it is due and once its Checkpoint has passed every Event recorded before it fell due, then commits the instance's state, effects, Saga Message changes and the message's consumption under its Lease; the Checkpoint does not move. A lagging Partition therefore delays its Saga Messages. They are never early, and those due at the same instant are handled in the order they were sent; after downtime they catch up, and `eventfabric.saga_message.lateness` measures start minus due time. The owner keeps a timer for its next due time and other processes nudge it through the store's notifications, with `pollInterval` as the fallback: on Postgres, Deadlines start about 16 ms after their due time and a Saga Message from another process about 12 ms after it is sent (the integration test's median on a laptop). A failing handler retries with backoff (100 ms, doubling) and is dead-lettered on its fifth attempt as `saga-message`, while the Saga's Events keep flowing; replaying it makes it due again unless it was replaced or cancelled since. Run `event-fabric migrate`: migration 6 adds the `saga_messages` table and migration 7 its send order. Custom persistence adapters implement `recordSagaMessages`, `dueSagaMessages`, `retrySagaMessage`, `deadLetterSagaMessage` and the `messages` argument of `commitSaga` (`test/contracts/saga-messages.ts`); a Saga that declares handlers refuses to start without them.

### Testing Sagas

`processFixture` from `event-fabric/testing` runs one Saga, object form or Decorated, on the in-memory store with a virtual clock. `given` and `when` take Events, Saga Messages and passing time, then let the Saga's Partitions run until they have passed the log's end and nothing is due; the expectations check what the last `when` did.

```ts
import { event, processFixture, sagaMessage, timePasses } from "event-fabric/testing";

await using saga = processFixture(connector, { name: "connector", now: "2026-10-01T08:00:00Z" });
await saga
  .given(event("crm.file.opened", "f1", { remindAt: "2026-10-01T09:00:00Z" }))
  .expectDeadline("remind", "2026-10-01T09:00:00Z")
  .when(timePasses(60 * 60_000))
  .expectCommands([remindCommand("f1")])
  .expectNoDeadline("remind")
  .when(sagaMessage("done", {}, { to: "f1" }))
  .expectState({ done: true });
```

- `event(name | EventClass, streamId, payload)` records an Event at the clock's time; `sagaMessage(name, payload, options)` sends one as `application.sendSagaMessage` does; `reply(message, { outcome: "ok", value } | { outcome: "failed", error, attempts })` answers the oldest unanswered Message of that name the Saga queued with `replyTo`, as its Worker would; `timePasses(ms)` moves the clock, stopping at every due time on the way, so Deadlines and retries run in order and a handler failing past its fifth attempt is dead-lettered.
- `expectCommands` and `expectQueued` compare, in order, the Commands told and Messages queued by the last `when`, each on the keys it names (`stream`, `name`, `payload`, `notBefore`, `replaceKey`, `idempotencyKey`, ...). `expectState(state, instance?)` compares the whole state (`null` once destroyed); `expectDeadline(name, at, instance?)` and `expectNoDeadline(name, instance?)` read the pending Deadlines. `instance` defaults to the last one the Saga changed.
- Names and payloads are typed through the Register and the domain Handles, as `commandTo` and `domain.billing.invoice.issue(...)` are.
- Options: `name` (default `process`), `now`, `services`; `workers` for the Messages the Saga queues (declared for routing and schema checks, never run: the fixture runs no Outbox, so `reply` steps stand in for them); `readmodels` and `readmodelServices` for the groups it `reads`, with the real `aggregates` (by default the fixture declares just the Events the Saga names).
- A Workflow definition (one version or `{ versions }`) runs the same way under `workflow:<name>`, by default the name `workflow(name, ...)` gave it. `start(workflow, id, input?, { reuse?, signal? })` and `signal(id, workflow.signals.<name>(payload))` start and Signal an instance as `startWorkflow` and `sendSignal` do, typed by the Workflow's Handle (or its name, through the Register; a Signal names its Workflow); `reply` answers an `ask`. `expectStatus(status, instance?)` and `expectResult(result, instance?)` read the stored instance, the result typed by the Workflow, and `expectState` its `set` view. A Workflow's Deadlines are named after their step: `workflowStep.sleep("nap")`, `workflowStep.waitFor("crm.file.closed", 1)` (the second such wait) or `workflowStep.ask(worker)`, which also names the `replyTo.step` of an `ask`'s queued Message.
- Awaiting the chain runs it; the first failed expectation rejects it, listing any handler failures logged since the last `when`. `fixture.application`, `fixture.store` and `fixture.now()` reach further.

`connector` is the Saga above. The fixture runs the process with `from: "beginning"`, whatever its definition says, so every `given` Event reaches it. The fixture moves time through the Application's `clock`, which also stamps Saga Messages and schedules their retries, and the in-memory store's `clock` option, which decides when they fall due; pass the same function to both to do this by hand. The Postgres store keeps its own clock.

### Workflows

A Workflow is one `async` function per instance that waits durably for Events, Signals, time and Worker replies (ADR-0021); a Saga is the other way to write the same process, as Reactions over hand-kept state. Pick the Workflow when the process reads best top to bottom ("wait for the payment, else remind, wait again, then give up"), the Saga when each Event is handled on its own. Both run on the same Partitions, Leases, Outbox and Saga Messages. Billing's `dunning` (`server/src/workflows/dunning.ts` in `event-fabric-billing`), shortened:

```ts
import { Type } from "event-fabric";
import { signal, workflow } from "event-fabric/workflows";

const { issued, settled } = domain.billing.invoice;

export const dunning = workflow("dunning", {
  version: 1,
  domain, // types the Events from the domain's Handles; without it they come from the Register
  start: issued, // an issued Invoice starts its instance; the Event is the input
  id: (event) => event.aggregate.id,
  correlate: { [settled]: (event) => event.aggregate.id }, // routes the payment to its instance
  signals: { promised: signal(Type.Object({ until: Type.String({ format: "date" }) })) },
  state: Type.Object({ stage: Type.String(), remindersSent: Type.Integer() }), // types `set` and the query
  reads: ["billing"], // groups `ctx.read` may read
  async run(ctx, event) {
    await ctx.set({ stage: "awaiting-payment", remindersSent: 0 });
    const paid = await ctx.waitFor(settled, { at: overdueAt(event.payload.dueDate) }); // or { timeout: ms }
    if (!paid.timedOut) return "settled";
    const invoice = await ctx.read("billing", "invoices", ctx.id); // the Readmodel document, Dates and all
    const reply = await ctx.ask(reminderSender, { invoiceId: ctx.id, debtor: event.payload.debtor, outstanding: invoice!.amount - invoice!.paid });
    await ctx.set({ stage: "reminded", remindersSent: reply.outcome === "ok" ? 1 : 0 });
    // ... wait again, then:
    await ctx.tell(domain.billing.invoice.markOverdue(ctx.id, {}));
    return "overdue";
  },
});
// workflows: { dunning } in the ApplicationConfig; its key must be the Workflow's name.
// A new Workflow starts at from: "now" by default, as a Saga does: Invoices issued before it starts no instance.
await app.startWorkflow(other, "id-1", input, { signal: other.signals.go() }); // an id-started Workflow
await app.sendSignal("inv-1", dunning.signals.promised({ until: "2026-12-01" }));
const { status, state, result } = (await app.queryWorkflow(dunning, "inv-1")) ?? {};
```

**`ctx`.** `waitFor(event | ctx.signals.<name>, { timeout } | { at })` resolves the oldest correlated Event or Signal not yet read; with a `timeout` (ms from the step's first run) or an absolute `at` (`string | Date`, as Saga Deadlines take it) it resolves `{ value }` or `{ timedOut: true }`, and a zero timeout or a past `at` takes one that already arrived without waiting. `sleep(name, ms | at)` waits a duration or until a time. `ask(worker, payload, { timeout? })` queues the Worker's Message and resolves its reply (`ReplyOf<typeof worker>`: `{ outcome: "ok", value }` or `{ outcome: "failed", error, attempts }`, or `{ outcome: "timedOut" }`). `read(group, model, id)` reads a document of a group in `reads`. `tell(command)` takes a Command Handle's or class's Command, `queue(message, payload)` a Message, `start(workflow, id, input, { reuse? })` starts another Workflow (a no-op while it runs; a finished id dead-letters the start as `workflow-finished` unless `reuse`); all three leave through the Outbox. `set(view)` sets what `queryWorkflow` returns as `state`. `now()`, `random()` and `uuid()` give time, randomness and ids. `fail(reason)`, `cancel()` and `restart(input)` end the run; `restart` begins a fresh journal under the same id, which keeps a long-lived loop's journal bounded, and the fresh run takes the highest registered version, as a new instance does. `ctx.id` is the instance id, so a Workflow started by id needs no id in its input. Without `start`, `run`'s annotated second parameter types the input; `run`'s return value types `result`.

**Rules for the body.** Each `ctx` call is a journaled step, named by its name and occurrence (`event:billing.invoice.settled#1`, `ask:send-reminder#0`, `sleep:grace#0`). After every wake (an Event, a Signal, a Deadline, a reply) and after a restart, the body runs from the top and finished steps return their recorded results without acting again. So:

- Await only `ctx` calls, and take time, randomness and ids from `ctx`. The `determinism` lint rule reports `Date.now()`, `new Date()`, `Math.random()` and `randomUUID()` inside `run`; a `ctx` call made after the run was judged suspended (because the body awaited something else) throws `LateWorkflowCallError` and is counted as `eventfabric.workflow.late_calls`.
- Give steps in concurrent branches distinct names. In `Promise.all([ctx.sleep("a", ms), ctx.sleep("b", ms)])` each step keeps its own journal entry; two `sleep("wait", …)` calls in parallel branches would be `sleep:wait#0` and `#1` by the order they happen to be reached. `waitFor` steps are named after their Event or Signal, so do not wait for the same one in two concurrent branches.
- `read` journals the document as JSON and revives its `DateTime()` fields from the Readmodel's schema, so it is the document type `getReadmodels()` returns, with `Date`s, on the first run and on every replay. Its value is the document when the step first ran; read again (another step) for a fresher one.
- Steps that send or read record their arguments. Code that, for running instances, moves, renames or removes steps, or changes what a step sends, **drifts** them: the instance is parked `drifted`, its wakes dead-lettered as `workflow-drifted: <reason>`, and every other instance runs on. Ship such a change as a new version and register both until the old instances finish: `workflows: { dunning: { versions: [dunningV1, dunning] } }`. New instances take the highest version; each instance runs the one it pinned. Correlated Events route by every registered version's `correlate`, each instance taking those its pinned version routes; a Signal is checked against the instance's pinned version (any registered version's declaration, for an instance not running yet); `reads` waits on every version's groups. Adding steps after the point every running instance has reached is safe within a version.

**Where it starts.** `from` works as a Saga's (the highest registered version's applies) and limits only the Events the group reads: Event-started instances and correlated Events. `startWorkflow`, Signals and `ask` replies are Saga Messages and arrive whatever it says.

**Contexts and Gateways.** `startWorkflow(workflow, id, input, { signal?, reuse? })`, `sendSignal(id, signal)` and `queryWorkflow(workflow, id)` are on the Application, plugins, Tasks, Workers (inheriting the Message's Correlation) and Saga handlers (starts and Signals commit with the Saga's state). A Signal names its Workflow: `dunning.signals.promised(payload)` builds one, and by string `{ workflow: "dunning", name: "promised", payload }`, typed by the Register; a start's Signal must be the started Workflow's. A start is a no-op on a live instance unless it carries a Signal, which the instance then receives; a finished id refuses unless `reuse`: with a `Rejection` coded `workflow-finished` (a Gateway's 422) where the call is checked at once, and, for a Saga's recorded start, by dead-lettering the start as `workflow-finished` (`workflow-missing` when it carries a Signal, which would otherwise be lost). `signal(schema)` checks the payload when it is sent and refuses a mismatch with a validation `Rejection` (a Gateway's 400); a Signal to a missing or finished instance dead-letters as `workflow-missing`. `queryWorkflow` reads the committed `{ status, state, result }`; nothing runs the body to answer. A Gateway's `handle(req)` has the three only where its Access map's `workflows` names `<workflow>.start`, `.query` or `.signals.<name>` (or `<workflow>.*`, `*`) as `true` or a function of `(ctx, { id, payload })`; a start with a Signal needs both entries. Where a Workflow is named by string, `event-fabric types` types its name, Signals and Access keys (list the Workflows in the types config's `workflows`).

**Testing.** `processFixture(workflow, { now, aggregates, readmodels, workers })` steps one Workflow on a virtual clock (see [Testing Sagas](#testing-sagas)): `start`, `signal`, `event`, `reply` and `timePasses` drive it, `expectStatus`, `expectState`, `expectResult`, `expectQueued`, `expectCommands` and `expectDeadline` check it. `start(workflow, id, input, options?)`, `signal(workflow, id, workflow.signals.<name>(payload))` and `expectResult` are typed by the Workflow, and `workflowStep.waitFor(event | signal, n?)`, `.sleep(name, n?)` and `.ask(worker, n?)` name a step's Deadline or Worker request. `accepted(app.sendCommand(...))` asserts a Command was accepted and returns its narrowed Outcome. Billing's `src/dunning.test.ts` tests each branch of `dunning` that way; `src/dunning-stores.test.ts` runs the whole Application on the in-memory store, and `npm run test:integration` runs the same scenario on Postgres, through a restart mid-wait and a careless deploy that drifts one instance.

### Gateways and plugins

`readmodels.subscribe(group, model, handler, options)` subscribes before reading initial documents. Set `initialSize` to a non-negative integer to cap that read in `_id` order; `0` returns no initial documents and listens for changes only. Omitting it keeps the full initial read. The filter and Access map apply to both the initial read and Notifications; the size limit applies only to initial documents. For paginated lists, use `initialSize: 0`, fetch the current page after subscribing, and refresh it on change hints and reconnection. Notifications carry a Position for `minPosition` reads, and the `correlationIds` of the Events that made the changes, so a client finds its own Command's writes in any model.

Live subscribers share one Transport subscription per topic in each process (Events, Readmodel Notifications, Application Notifications): a thousand open feeds cost one broker queue and one parse per message, not a thousand, and every replica's subscribers receive every message. Subscribers get the same message object and must not change it. `notify(name, payload)` (on the Application and the plugin context) publishes an Application Notification, such as a lock update, to every replica's live subscribers; nothing stores it. `subscribeNotifications(handler)` receives them.

A Gateway declares an Express 5 port, ordered middleware, `context(req)`, `commandMetadata(ctx, command)`, an Access map and routes. `handle(req)` returns the scoped context, Command sender and Readmodel API, plus `getDomain()` (Commands through the same Access map and `commandMetadata`), `services` and `logger`. A repository's `native()` is the store's whole collection where the model's rule grants it whole (`true`). Under an equality-filtered rule it is a read-only Mongo view limited to the filter: `find`, `findOne`, `countDocuments`, `distinct` and `aggregate` AND the filter in (`aggregate` prepends a `$match`), and pipeline stages that read or write other collections (`$lookup`, `$unionWith`, `$out`, `$merge` and the like, also inside `$facet` or added through a cursor) throw, as does replacing a cursor's filter. Other stores, and filters that are not plain equalities, deny it. `rawBody: true` keeps each JSON body's bytes as `req.rawBody`, for webhook signatures. Both Access-map sections deny by default; equality filters are AND-ed into all read paths and client Readmodel Notifications. A filter value that is `undefined` (for example a missing tenant in the Request context) denies with 403 instead of matching documents without that field. `command(...)` and `readmodel(...)` return Express handlers. With `idempotencyScope(ctx)` (for example `(ctx) => ctx.tenant`), a request's `Idempotency-Key` header becomes its Command id, derived from the scope and the key, so a client retry is a Duplicate Command. It answers with the same status and `EventFabric-Command-Id`, renders `respondWith` from the current document, and sets `EventFabric-Duplicate: true`. Once a Command is accepted, `command()` never answers with an error status: if its `await` check or `respondWith` read fails, it answers `202` with the Outcome and `EventFabric-Position` and logs a warning, so a client never retries a recorded Command. A key reused for a different request gets 409. Without a scope the header is ignored, with one warning, so keys from different tenants never share outcomes. Use `sse(req, res)` for bounded live feeds. Client-facing Event-feed authorization belongs to the Application: a Gateway's `live: { events(ctx, event), notifications(ctx, notification) }` rules say which Events and Application Notifications a caller may receive; they run for every item and subscriber, so keep them cheap and synchronous, and a kind without a rule is denied. `handle(req)` gives `subscribeEvents`, `subscribeNotifications` and `subscribeChanges` (every live change of a document the caller may read, across models, with the document; a document moved out of the caller's scope arrives as a `delete` with the state they could read). The `live()` route serves them over SSE: `?events=<pattern>,…`, `?notifications=<name>,…` (`*` for all) and `?readmodels=<group>.<model>,…`, as SSE events named `event`, `notification` and `readmodel`; a denied subscription answers 403 before the stream opens, and a reconnecting client reads again what it shows (no `Last-Event-ID` resume). `upgrade(req, socket, head, scoped)` takes HTTP upgrades such as a WebSocket protocol, with `scoped(ctx)` giving the caller's handle; upgraded sockets are destroyed when the Gateway stops.

Plugins receive `logger`, `config`, `sendCommand`, `awaitCorrelated`, `subscribeEvents`, `readmodels`, `services`, read-only `model`, and `on`. Hooks are `start`, `gateway`, and reverse-order `shutdown`. Shutdown drains requests and consumers within ten seconds by default; the drain keeps a reserve (a fifth of the timeout, at most two seconds) so every `shutdown` hook still runs and Leases are released when a request hangs. The drain includes Outbox deliveries the relay's last pass started; one cut off by the timeout stays pending for another replica and logs a warning. The replica's Command handlers stop last, so requests, Workers and Outbox deliveries waiting on a Command's reply still get it from this replica. A failing or hanging hook does not skip the others; each hook shares the remaining time with later hooks and Lease release. `stop()` rejects on hook errors, timeouts or failed Lease release. `close()` still closes every resource if shutdown fails.

## Operations

An Application can start from its own entry point instead of `event-fabric run`, as billing's `src/main.ts` does:

```ts
const application = createApplication({
  ...(await config()),
  // Rebuild worker threads load the config from here; without it a rebuild folds on one thread.
  configModule: import.meta.resolve("./event-fabric.config.js"),
});
process.exitCode = await application.run({ only });
```

`run({ only?, signal? })` starts the Application unless it is already started, waits for SIGTERM, SIGINT or `signal`, closes it and resolves the exit code (1 when the broker connection was lost). The config module stays the composition root and must not start anything when imported: the CLI's other commands and rebuild worker threads load it.

The CLI loads a named `config` export from `event-fabric.config.js`, overridable with `--config`. Use `--help` for command options. It supports `run`, `dev`, `migrate`, `rebuild`, `task`, `types`, `dead-letters`, `consumers` and `workflows`. `--only kind:name` selects deployment roles (`--only workflow:<name>` runs one Workflow's Partitions); `--only commands:<app>/<context>` runs the Command handlers of one Context, so Contexts can scale as separate processes. `types --check` checks generated declarations without running hooks or `setupServices`; `task <module>` runs the module's `run(ctx, args)` after setup (an old Task module, whose only export is `(task, app)`, runs unchanged, see docs/migration.md), starts no consumers, plugins or Gateways, and cleans up on exit, whether the Task succeeds or fails. A Task's `sendCommand` is handled by a running Application replica on the same Transport, since the Task process handles no Commands.

`application.health()` reports liveness and readiness, plus `rebuilding`: the groups whose required rebuild has not swapped in yet. The RabbitMQ transport does not reconnect: once the broker connection is lost, liveness turns false and `application.run()` (also behind `event-fabric run`) closes the Application and resolves 1, so run it under a supervisor that restarts it (billing uses `restart: unless-stopped`). `createRabbitMQTransport` connects when the Application starts, or on first use in a Task; a broker it cannot reach fails `start`. Loading a config for `migrate`, `rebuild`, `types` or a rebuild worker thread opens no broker connection. Readiness stays true while they rebuild. Readiness checks that expected Consumer groups have an active durable Lease across the cluster, including when this replica is standing by. Logging uses a pino-compatible port; Application metadata is excluded. The default logger is pino at `info`; `createLogger({ level: "debug" })` (any pino options) or your own logger at `debug` adds diagnostic records. EventFabric's records carry a `component` (`application`, `consumers`, `outbox`, `commands`, `worker`, `gateway`, `readmodels`, `store`) to filter on, and the same field names wherever they apply, so one search follows a request: `correlationId`, `causationId`, `commandId`, `command`, `stream` (`context.aggregate.id`), `consumerGroup` (as `consumers status` names it), `eventId`, `event`, `messageId`, `deadLetterId`. At `debug`, each replica logs every 10 s: `Consumer group timings` per held Partition (`read`, `checkReads`, `waitReads`, `loadState`, `react`, `commit` or `apply`, each as `<stage>Ms` and `<stage>Count`, plus `events`; `checkReads` measures dependency checks and `waitReads` the pauses while dependencies are behind); `Consumer group lag` for the Partitions it holds; `Outbox timings` (`poll`, `claim` once per poll's Admission, `send`, `complete` once per batch of completions, `polledRows`, a count per returned outcome, and `sendErrors` for transport exceptions); `Readmodel read timings` per group, with the Checkpoint and halted check (`status`) apart from the document read (`find`); and `Pool occupancy` for each Postgres pool (`waitingAvg` and `waitingMax` of requests waiting for a connection, `busyMax`, sampled every 250 ms). Lease acquire, hand-over and loss are logged as they happen. With pino, `info` skips stage timing, accumulation and pool sampling after a level check. Custom loggers can provide `isLevelEnabled("debug")`; without that optional method diagnostics remain enabled for compatibility. `eventfabric.pool.waiting` reports each pool's waiting requests as a gauge. To read these records: a Saga busy most of each window is at its limit, and a pool with waiting requests delays every group on that replica. Tracing and metrics use the OpenTelemetry API and remain no-ops until the Application installs an SDK/exporter. Configure alerts for `halted` and `waiting` Consumer-group states first, then lag, Outbox age, dead letters and `eventfabric.outbox.rejected`. Outbox pending count and oldest age include delayed rows and are sampled every five seconds while the relay runs; direct `outboxStats()` calls remain exact. Revision-conflict, Aggregate-load, Worker and read-wait instruments provide supporting signals.

**Records to alert on.** Each record that needs an operator carries `recovery`, the next step to take.

| Record (`msg`) | Level | Component | Meaning and next step |
| --- | --- | --- | --- |
| `Application started` | info | application | What the replica runs: `instance`, `application`, `version`, `node`, `roles`, Readmodel groups with `partitions` and `store` (a label, never an address), `sagas`, `workers`, `slowHandlerThreshold`. |
| `Application stopping` / `Application stopped` | info | application | Shutdown began / finished, with `durationMs`. |
| `Application shutdown failed` | error | application | The drain, a shutdown hook or a Lease release timed out or threw (`errors`). |
| `Broker connection lost; exiting` | error | application | `event-fabric run` exits 1 for its supervisor to restart it. |
| `Consumer group will retry the same Event` | error | consumers | A Reaction failed (`eventId`, `attempts`, `error`); it retries with backoff. |
| `Consumer group halted; retrying the same Event` | error | consumers | Five failures in a row: `state: "halted"`; `recovery` names `consumers retry <group>` or `consumers skip <group> --event <id>`. |
| `Outbox Command rejected; dead-lettered`, `Outbox entry dead-lettered`, `Worker Message dead-lettered`, `Saga Message dead-lettered` | warn / error | outbox, worker, consumers | Parked in the dead letters under `deadLetterId`; `recovery` names `dead-letters replay <id>` or `discard <id>`. |
| `Command failed` | error | commands | A Command handler threw (`command`, `stream`, `commandId`, `error`); the sender gets `failed`. |
| `Slow Command handler` | warn | commands | A handler took `durationMs` ≥ `slowHandlerThreshold` (an Application option, default 1000 ms); it holds its Stream and its lane's Partition that long. |
| `Workflow drifted; instance parked until an operator resumes it` | error | consumers | The instance's code no longer matches its journal (`reason`); its wakes are dead-lettered as `workflow-drifted`. Deploy matching code or its pinned version, then `workflows resume <name> <id>`. |
| `Workflow ctx call after its run was judged suspended` | warn | consumers | The body awaited something other than a `ctx` call (`step`); fix the body. Counted as `eventfabric.workflow.late_calls`. |
| `Slow Reaction` | warn | consumers | A live Readmodel or Saga Reaction took `durationMs` ≥ `slowHandlerThreshold` (`consumerGroup`, `event`, `eventId`, `stream`); rebuilds do not log it. |
| `Outbox Command outcome still unknown after 10 minutes; re-sending` | warn | outbox | No replica answered the Command; check that the Command handlers run. |
| `Worker Message retry scheduled`, `Saga Message failed; retrying` | warn | worker, consumers | A failed attempt that will run again. |
| `Gateway request failed`, `Gateway Command did not complete successfully` | error | gateway | A request threw / a Command came back `failed` or `unknown`. |

**Live fan-out under load.** Each RabbitMQ broadcast subscription (one per topic and process) reads its exclusive queue 10 messages at a time and acknowledges each as it is read, so Notifications stay at most once. A process that falls behind leaves the backlog in the queue, which keeps at most `broadcastMaxLength` messages (a `createRabbitMQTransport` option, default 10 000) and drops the oldest beyond that. Memory and latency stay bounded instead of growing in the process. The small window also keeps large Readmodel Notifications from delaying Command replies on the same connection. Each Context's Command handlers use their own broker connection: RabbitMQ closes the whole connection when a consume fails. When a quorum queue that another replica is still declaring refuses a consume with `noproc`, `handle` retries on a new connection up to five times (100 ms to 1.6 s apart) before failing. For Gateways that serve many live clients during bulk work, such as an import:

- **Topology.** Run the Gateway replicas as their own processes with `--only gateway:<name>`, and the Command handlers (`commands:<app>/<context>`), Consumer groups (`readmodels:<group>`, `saga:<name>`), Outbox and relay in others. Bulk folding and Command handling then never share a Gateway's event loop, and Gateway replicas scale with clients alone.
- **Groups and alerts.** Keep the groups that interactive screens read free of Reactions to import-only Event names, or split those Reactions into another group, so the import cannot make them lag; raise a group's `partitions` if it still lags. Alert on `eventfabric.consumer.lag` (milliseconds, attributes `group` and `partition`, recorded every 10 s) per group, with a tighter threshold for the interactive ones.
- **Old sockets.** `legacyGateway`'s `socket: { coalesce }` throttles auto-refresh subscriptions, and `commandWaitTimeout` ends a Command wait that never sees its change ([migration guide](docs/migration.md#the-old-socket-protocol)). `test/integration/live-load.test.ts` measures the Gateway's event-loop delay and an interactive Command route's latency with 300 sockets during a burst of 10 000 changes.

A failing Reaction rolls back and retries the same Event; after five attempts the group reports halted while continuing retries. Restarts and ownership changes retain the saved failure count and halt until the Event succeeds or an operator skips it. Running Consumer groups renew their Leases together on the renewal interval. `consumers retry` wakes it; `consumers skip` requires the exact halted Event and operator confirmation. Use `application.skipConsumer(group, eventId, who)` for programmatic skips. A group that no live replica's `instance:` row lists reports `undeclared` instead of its stored state, and emits no lag metric; nothing is marked while any live replica has yet to list its groups. `consumers retire <saga or workflow group>` deletes a removed Saga or Workflow group's Checkpoints, `layout:` row, pending Saga Messages and Deadlines, and its Saga state or Workflow instances (every `workflows` row of the group, running or finished) after confirmation (`--yes` skips it). It is refused while any live replica declares the group or any of its Partitions holds a Lease, so run it after every replica runs the new config. The state goes too because a Saga or Workflow added back later is a new group: it starts where its `from` says, and with `from: "beginning"` old state or instances would double-apply its Events. **Replaying history**: to run a Saga or Workflow over Events recorded before it started (or before a `from` you now regret), retire its group as above and deploy it again with `from: "beginning"` (or an earlier `Date` or `{ position }`); changing `from` on a group that already started only logs a warning (`from changed on a group that already started`). Commands and Messages the Saga already recorded stay in the Outbox and are delivered when due, as in billing's former `overdue` Saga, whose delayed `markOverdue` Commands re-check the Invoice; drop such rows only with application-specific SQL. Readmodel groups cannot be retired this way, because their Lease epochs also live in the Readmodel store. Readmodel skips fence local writes and reject an already-processed Event, then record an exact epoch-checked Postgres audit before advancing the local Checkpoint. Across stores this is a recoverable sequence: a crash after the audit is completed from the ledger by the next owner; before the audit it leaves no recorded skip and the operator may retry. Saga audit and Checkpoint updates share one Postgres transaction. Rebuilds ignore skips and retain the live generation on failure. `rebuild --swap manual` prepares a generation for later catchup and swap.

**Workflow instances.** `workflows list [--status running|completed|failed|cancelled|drifted]` prints the instances (group, id, pinned version, status, error, start and finish) as JSON; `workflows inspect <name> <id>` prints one with its input, journal, inbox, state and result. `workflows cancel <name> <id>` and `workflows resume <name> <id>` send the instance a Saga Message that its owner handles, so a replica running `workflow:<name>` must be up; they refuse an instance that is missing or finished. Cancel throws `WorkflowCancelled` at the body's pending `ctx` await, where it may clean up before the instance ends `cancelled`; a drifted instance, whose body cannot run, ends `cancelled` at once with its Deadlines. Resume is for a `drifted` instance once matching code (or its pinned version) is deployed: the instance runs again from its journal, reading the Events buffered while it was parked, and then its dead-lettered wakes are replayed; if it still drifts it parks again. `workflows purge --finished-before <date>` deletes instances that finished before the date and prints how many; a later `startWorkflow` of a purged id starts a fresh instance without `{ reuse: true }`. Finished rows stay until purged (their journals are already dropped), so purge on a schedule.

**Deploying Workflow changes.** Before a deploy, check whether the change moves, renames or removes steps, or changes what a step sends, for instances that already passed that point; if so, bump `version` and register both versions. After the deploy, `workflows list --status drifted` and `dead-letters list` show any instance that drifted anyway. To recover one, deploy code that matches its journal (usually the old version, registered beside the new one) and run `workflows resume <name> <id>`; or end it with `workflows cancel <name> <id>`. An old version can be removed once `workflows list --status running` shows none of its instances. A drifted instance never halts its Partition. `--only workflow:<name>` runs one Workflow's Partitions; `partitions: N` spreads its instances over N Leases, like a Saga's.

A Consumer group looks for new Events when the Event store notifies an append, and polls every `pollInterval` as recovery. A group or Saga that waits for a group in its `reads` resumes when that group commits, on this replica directly and on others through the Readmodel notification the commit sends, so it trails the read group by a commit, not by a poll. A wake that arrives while a group is looking makes it look again rather than pause. Group and generation records carry their model definitions, which every Readmodel transaction reads; the Mongo and Postgres stores read the records without them and take each definition set by its hash, decoded once per process.

Rebuilds publish complete Readmodel groups. A pending generation records a fingerprint of the model declarations with their resolved schemas, function source, declared Event schemas/Upcasters and dependencies; other Application schemas do not affect it. Resuming it with a different fingerprint is rejected before a commit or swap; generations prepared by an earlier framework version without that fingerprint must be discarded and rebuilt. Set a group's `rebuildVersion` and bump it when helpers, captured values or externally configured Upcasters change: function source cannot reveal those changes. This version guards pending reconstruction; it does not automatically rebuild active documents. Changes to Reactions or their Event set that need historical data require an explicit whole-group rebuild. Keep one reconstruction definition across replicas during a rebuild. To discard an incompatible pending result, obtain its id with `store.pendingGeneration(group)` and call `store.dropGeneration(group, id)`; the active generation remains available. Custom Readmodel-store adapters must persist the reconstruction value supplied to `createGeneration` and check it on `batch` and `swapGeneration`.

A rebuild keeps what it reads in a **Rebuild cache** until it ends, capped for the whole process by `rebuildCacheBytes` (default 256 MiB; `0` turns it off). Concurrent rebuilds and fold worker threads share the cap, and the least recently used entries are evicted first. It holds three kinds of read:

- **The group's own documents.** They stay exact, because the rebuild is their only writer. A rebuild that starts on an empty generation has every document in memory until its first eviction. Until then it needs no store read by `_id`, and it answers a filter with an equality or `in` clause from an index it builds on that clause's path, with the store's results. A filter without such a clause, a text search and every read after the first eviction still go to the store. The indexes count against the cap at about 250 bytes per document and indexed path. Give large groups room: a dedicated rebuild host can set `rebuildCacheBytes` to several GiB.
- **Reads of the groups in `reads`.** Each is reused only for batches that the read group's Checkpoint at the time of the read covers. That matches the rebuild's wait for those groups before each batch.
- **Values from `rebuildMemo(key, load)`.** Inside a rebuild, `load` runs once per JSON key until the rebuild ends; anywhere else it runs on every call. Use it for data a rebuild may treat as fixed, such as CRUD records a Readmodel reads, and do not change the values it returns.

A rebuild reads mutable data either way: `rebuildMemo` makes those reads consistent within one rebuild, not deterministic.

A filter without a sort returns documents in `_id` order. A rebuild folds a batch while the previous one commits, so without the cache a document that batch created is not yet in the store and comes after the stored ones. With the cache it takes its `_id` place, as in live processing. Reactions whose result depends on the order of several documents should sort.

`rebuild <group> <group> …` or `rebuild --all` (`application.rebuildGroups(names, { swap, concurrency })`) rebuilds several groups at once. Under the CLI, each group rebuilds in its own worker thread, which loads the Application from the configuration module, so groups fold on separate CPUs. Up to `--concurrency` groups (default: the available CPUs) rebuild at a time, sharing the process's `rebuildCacheBytes`. A group that reads another group of the same call starts after that one ends, and does not start when it failed. The other groups still finish, and the failures are reported together. Without a configuration module (an Application created in code without `configModule`), the groups rebuild concurrently on one thread. Startup rebuilds under `run` still go one group at a time.

A **Readmodel import** takes over documents that another store already folded, so a group need not be rebuilt from the whole log at a cutover. `application.importReadmodels(group, { model: documents }, { position, swap })` loads each model's documents (any iterable or async iterable with string `_id` values) into a new pending generation under the group's Lease, normalized and validated like a rebuild's writes. It then sets that generation's Checkpoint to `position` (the log head by default) and resumes it as a rebuild: Events after `position` are folded, and with `swap: "auto"` (the default) the generation is swapped in. `swap: "manual"` leaves it pending for `rebuild --swap`. Documents go in chunks of `chunk` (1000), with `concurrency` (4) chunks written at once, since a pending generation's documents have no order between them; a failed chunk waits for the others before the generation is dropped. A failed load drops the generation, and an import is refused while the group has a pending generation. The imported documents must be exactly what folding the log up to `position` would produce; prove that once with a full rebuild and a comparison before relying on it ([ADR-0019](docs/adr/0019-cutover-imports-readmodel-documents.md)). Readmodel stores provide it through the optional `load` method, which writes only into a pending generation without a Checkpoint and is fenced by the group's epoch.

Each Consumer group runs on one replica at a time; replicas share the groups out by a capped rendezvous score, so every replica holds about the same number. `start()` logs the replica's `instance`, the Lease owner in `consumers` rows. A replica's Outbox deliveries, Consumer groups and Command handling share its Event-store pool, and the Outbox dispatcher runs up to 100 deliveries at once, so with pg's default of 10 connections a Saga on the replica holding the Outbox falls behind with CPU to spare. Size `poolOptions.max`, keeping replicas × connections under Postgres `max_connections`. 20 kept every Partition of billing's load test bounded. A larger pool does not lift a single Partition's own limit: billing's `receipts` Saga makes a Readmodel read and a Saga-state read per Reaction, about 6 ms under load, so one Partition tops out near 150 Reactions/s. Queueing on the Outbox's replica pushed it over that limit, and it fell over 100,000 Events behind. Declare `partitions` for a Saga whose Reactions arrive faster than one Partition can run them. A Readmodel group or Saga that needs more than one replica declares `partitions: N` (ADR-0016). Every Partition advances through the whole log in Position order and applies only the documents (`model/id`) or Saga instances it owns, so each document and Saga instance still sees every Event that touches it, once, in order. Reactions of a partitioned group cannot read the group's own documents; read-your-writes waits for every Partition. Changing `partitions` drains the old Partitions to one Position and starts the new ones there; run one value across all replicas. Commands recorded for one Stream leave the Outbox in the order of their recording transactions' ids (and within one transaction, in the order recorded); Commands to different Streams and queued Messages leave independently, even from one transaction: a row waits until every transaction with a lower id has committed or rolled back, so a long-running transaction delays the Outbox as it delays Event readers. Transaction ids are assigned in start order, not commit order, and two independent writers get no order between them beyond that. So one Saga instance's Commands to a Stream leave in the order of its Events, but different Saga instances or Sagas get no order between them: effects that depend on each other's order belong in one Saga instance.

## Verify

```sh
npm run check
# Real adapter and runtime recovery tests:
docker compose -f compose.test.yml up -d --wait
docker compose -f compose.test.yml exec -T mongo mongosh --quiet --eval \
  'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'
npm run test:integration
docker compose -f compose.test.yml down -v
```

`npm run typecheck` (part of `check`) runs [tsrs](https://github.com/oxc-project/tsrs), a Rust port of the TypeScript 7 checker, pinned to an exact version because it is experimental; `npm run build` still emits with `tsc -b`, whose build mode tsrs does not fully cover. Applications and the billing example keep using `tsc`. The CI matrix tests package loading on Node 24.15 and 26. Adapter tests use disposable localhost services and synthetic data. [Migration and cutover guidance](docs/migration.md) covers mechanical source changes, schema drafts, imports and the offline replay audit. [Synthetic compatibility examples](docs/migration-compatibility.md) demonstrate import/rebuild comparisons and the coverage required beyond `auditReplay.passed`.
