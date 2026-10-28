# EventFabric

A framework for building event-sourced CQRS applications. An Application declares Aggregates, Readmodels, Sagas and Workflows; EventFabric runs them.

## Language

### Write side

**Application**:
A codebase built on EventFabric that declares its Contexts, Readmodels, Sagas and Gateways.
_Avoid_: app (in docs), service, project

**Context**:
A named group of Aggregates inside an Application, e.g. `billing` or `shop`.
_Avoid_: domain, module, bounded context (unless meaning DDD in general)

**Aggregate**:
A consistency unit identified by name and id that accepts Commands and records Events.
_Avoid_: entity, model, document

**Command**:
A request addressed to one Aggregate instance to change it. Can be rejected.
_Avoid_: action, message, request

**Domain Event**:
An immutable fact recorded by an Aggregate in the Event log, from which the Aggregate's state is rebuilt (event sourcing). Named in the past tense. Its name is permanent once recorded: a Domain Event that is no longer emitted stays declared. Plain "Event" means a Domain Event.
_Avoid_: message, notification, change

**System Event**:
An event the framework delivers to handlers outside the Event log (event driven, not event sourced). It has no Stream, Revision or Position, and Readmodels, rebuilds and Command waits never see it. Saga Messages and Application Notifications are System Events.
_Avoid_: non-domain event

**Precondition**:
A check that rejects a Command by throwing (`only.ifExists`, `only.ifState`, `only.ifValidatedBy`). Payload and existence checks run before any Step; `only.ifState` checks are Steps. A returned value never rejects.
_Avoid_: middleware, guard, validator

**Step**:
One entry in a Command's ordered list of `only.ifState` Preconditions and handlers. Steps run in the order they are written, and any Step may apply Events.
_Avoid_: middleware, pipeline stage, handler chain

**Rule**:
A check that runs after all of a Command's Steps and rejects the whole Command by throwing when the resulting state is invalid (`only.after`).
_Avoid_: business rule, post-condition, invariant check

**Rejection**:
The outcome of a Command that recorded no Events because a Precondition, Rule or handler refused it.
_Avoid_: failure, error

**Duplicate Command**:
A Command whose id was already accepted. It is not run again: it returns the outcome the first one had. A Command that was rejected or failed leaves no record, so a retry runs again.
_Avoid_: replay, retry, idempotent request

**Message**:
Queued work for a Worker, sent through the Outbox: neither a Command nor an event. It has no Aggregate and no Revision, and it is never part of the Event log.
_Avoid_: job, System Event, notification

### Ordering

**Stream**:
The ordered sequence of Events of one Aggregate instance, identified by Context, Aggregate name and id together. The id alone is not unique.
_Avoid_: event stream (when it means the global log), topic

**Revision**:
The position of an Event within its Stream, starting at 1. Used for optimistic concurrency.
_Avoid_: version, sequence

**Schema version**:
The version of an Event's payload shape, recorded with every Event so old shapes can be recognised.
_Avoid_: event version, revision

**Upcaster**:
A function that turns an Event stored in an older Schema version into the current shape when it is read. Stored Events are never rewritten.
_Avoid_: migration, converter, transformer

**Snapshot**:
A cached state of one Aggregate at a given Revision, used only to load faster and rebuildable from its Stream. Not provided by EventFabric.
_Avoid_: checkpoint, memento

**Position**:
The place of an Event in the global order across all Streams. Consumers read and resume by Position.
_Avoid_: offset, global sequence, index

**Partition key**:
The value that decides which Partition a message belongs to, e.g. an Aggregate id. All messages with the same Partition key are processed in order, by one consumer at a time.
_Avoid_: routing key, shard key, hash, message key

**Partition**:
One of a fixed number of ordered lanes that messages are split into by Partition key. Each Partition is owned by exactly one instance of a Consumer group at a time. A partitioned Readmodel group or Saga splits by owner instead: every Partition advances through the global Event history in Position order and applies only the documents or Saga instances it owns (ADR-0016).
_Avoid_: shard, queue, topic partition (unless meaning Kafka's)

**Command lane**:
Which of its Partition's two queues a Command waits in: interactive (a user's request) or bulk (imports, Sagas, Schedules, Workers, Tasks). The sender decides. Bulk work never delays interactive Commands, and each lane keeps its own order.
_Avoid_: priority, queue

**Partition layout**:
The number of Partitions a Readmodel group's or Saga's Checkpoints are laid out for, recorded in its `layout:<group>` row. Changing it fences every Partition of both layouts, drains the old Partitions to one Position, starts the new ones there and publishes the new count by compare-and-set, so no Event is skipped or handled twice; a switch that fails leaves the old layout running.

**Lease**:
Time-limited ownership of one Partition by one instance of a Consumer group. Each new Lease raises the Partition's epoch, so a Checkpoint written under an older epoch is refused.
_Avoid_: lock, claim, assignment

**Consumer group**:
A unit that processes messages and owns their Partitions: a Readmodel group, a single Saga, or the Command handlers of an Application. It keeps one Checkpoint per Partition.
_Avoid_: Saga group, subscription, worker pool

**Causation id / Correlation id**:
Causation id is the id of the message that directly caused this one; Correlation id is shared by every message in one chain.
_Avoid_: request id, trace id

**Metadata**:
The Application's own data carried alongside a Command, Event or Message (e.g. who acted, which account owns it), inherited down a chain. EventFabric does not interpret it. It is never erased, so it must hold no personal data.
_Avoid_: headers, context, envelope (the envelope is EventFabric's own fields)

**Outbox**:
The Postgres table where a transaction records the Commands and queued Messages it wants sent, optionally delayed. EventFabric delivers them after the commit, at least once. Commands to one Stream leave in the order of their recording transactions' ids, and in recorded order within one transaction; everything else leaves independently. A row leaves only once every transaction with a lower id has ended, as Event readers wait, so a long-running transaction delays the Outbox.
_Avoid_: pending commands, `_commands`, job queue

**Schedule**:
A named request for a Command to run no earlier than its due time, owned by the target Stream. The Stream and name identify it across rescheduling.
_Avoid_: job, Task, timer, obligation

**Schedule attempt**:
The Command a Schedule will send, recorded in the Outbox with an id EventFabric generates and the Schedule's name and due time. Replacing or cancelling the Schedule makes its earlier attempt obsolete: if it still arrives, it is accepted with no Events or effects.
_Avoid_: job, scheduled task, timeout

**Admission**:
The Outbox dispatcher's atomic claim of a due row before delivery. A Schedule cancelled before Admission never leaves; one cancelled after it may already be travelling and is fenced as an obsolete Schedule attempt.
_Avoid_: dispatch (which includes the delivery after it), pickup

**Idempotency key**:
A stable key EventFabric derives from the source of a queued Message (the Consumer group, or the Aggregate for a Command handler), the Saga instance id for a Saga, the id of the Event or Command that caused it, and a per-call suffix: by default the effect's position among the effects that one instance (or Command handler) records for that cause, or the `suffix` it passes. It does not depend on Partitions or batches. A Worker passes it to an external system so a repeated effect can be recognised. Not the HTTP `Idempotency-Key` header, which a Gateway turns into a Command id (see Duplicate Command).
_Avoid_: request id, dedupe id, job id

### Read side

**Readmodel**:
A queryable collection of documents kept up to date by reacting to Events.
_Avoid_: view, viewmodel, projection, denormalizer

**Readmodel store**:
The database that holds a Readmodel group's documents, reached through one interface whichever database it is (e.g. Mongo or Postgres).
_Avoid_: readmodel repository, database, collection

**Readmodel group**:
A named set of Readmodels that are deployed and Checkpointed together, e.g. `api`.
_Avoid_: denormalizer, type

**Saga**:
A long-running reaction to Domain Events and Saga Messages that keeps its own state and sends Commands. One Saga instance's Commands to a Stream leave in the order of its Events; different instances and different Sagas get no order between them, so effects that must be ordered share an instance.
_Avoid_: process manager, orchestrator

**Saga Message**:
A System Event addressed to one Saga instance, delivered now or at a due time and handled by that instance against its state. It survives restarts until handled, but is never recorded in the Event log.
_Avoid_: timeout, wake-up, scheduled event, signal

**Deadline**:
A named Saga Message an instance sends itself with a due time. At most one per name is pending for an instance; setting it again replaces it, and destroying the instance cancels it.
_Avoid_: timeout, timer, reminder

**Workflow**:
A long-running process written as one function per instance that runs from its start to its end, waiting durably for Events, Signals and time in between. The alternative to a Saga for Applications that prefer sequential code to Reactions.
_Avoid_: saga, process manager, durable function

**Signal**:
A one-way, durable input to one Workflow instance, sent by a Gateway, Saga, Worker, Task or another Workflow. The Workflow reads it where it waits for it.
_Avoid_: message, event, wake-up

**Update**:
A Signal whose sender receives a result once the Workflow has handled it.
_Avoid_: command, request

**Query**:
A read-only look at a Workflow instance's current state, answered without running the Workflow.
_Avoid_: get status, inspect

**Drift**:
A Workflow instance whose deployed code no longer matches its journal: replaying it reaches a new step before a step it already finished, or repeats a step with other arguments. A drifted instance is parked until an operator resumes it; other instances run on.
_Avoid_: nondeterminism error, corruption

**Reaction**:
The handling a Readmodel or Saga declares for one Event name. In a Decorated class, `@EventHandler` and `@SagaEventHandler` declare Reactions; they keep Axon Framework's names.
_Avoid_: handler, listener, subscriber

**Reaction marker**:
An item before a Reaction's functions that decides whether the Reaction runs: `only.ifExists()`, `only.ifNotExists()` or `only.if(predicate)`. A Reaction its markers skip runs no function and creates no document or Saga instance. The old toolkit's `autoCreate`, `shouldHandleEvent` and `shouldHandle` settings are the same markers.
_Avoid_: guard, filter, setting

**Reaction set**:
A named set of further Reactions in a Readmodel's `reactionSets`, with its own Identities and markers, on the model's documents. Its Reactions run after the model's own, in declaration order. Migration puts the old grouped `<event>:<group>` keys here.
_Avoid_: group (a Readmodel group is a set of models), branch

**Handle**:
A typed reference to a declared name. In the namespace that `defineDomain` returns, `domain.billing.invoice.settled` is the qualified Event name, typed with its payload, and `domain.billing.invoice.issue(id, payload)` builds a typed Command. `message(name, schema)` returns a Message handle. Other files import handles instead of writing names.
_Avoid_: token, ref, descriptor

**Command class**, **Event class**:
The Handle `command(name, schema)` or `event(name, schema)` declares in `event-fabric/decorators`, e.g. `class OpenAccount extends command("bank.account.open", Opened) {}`. It carries the qualified name and TypeBox schema, is the parameter type of a Decorated class's method, and `OpenAccount.to(id, payload)` builds the Command. Not a queued Message.
_Avoid_: message class, DTO

**Decorated class**:
A class marked `@Aggregate`, `@Readmodel` or `@Saga` whose decorated methods become the same definition `aggregate()` or an object declares. It holds no state and is never constructed; its key in the Application's config is its permanent name.
_Avoid_: component, entity, bean

**Identity**:
The mapping from an Event to the Readmodel document(s) or Saga instance it applies to.
_Avoid_: identifier, key, router

**Checkpoint**:
The last Position a Consumer group has fully processed in one Partition, including Events it has no Reaction to. A new Saga or Workflow group (no Partition has one) starts with every Partition's Checkpoint where its `from` says: `"now"` by default, `"beginning"`, a Date or a Position.
_Avoid_: position guard, cursor, offset

### Edge

**Notification**:
An at-most-once System Event telling Gateways that an Event was recorded or a Readmodel changed, carrying the Position it reflects.
_Avoid_: update, push, change event

**Application Notification**:
A named live System Event an Application publishes with `notify` (a lock taken or released), delivered at most once to every replica's live subscribers; never stored.
_Avoid_: signal

**Live rule**:
A Gateway's `live` rule deciding, per caller and item, which Events and Application Notifications reach that caller's live feed; a kind without a rule is denied.
_Avoid_: event filter, subscription ACL

**Gateway**:
A named HTTP entry point of an Application (REST or SPA) that sends Commands and reads Readmodels on behalf of callers.
_Avoid_: API, server, BFF (unless meaning that pattern)

**Request context**:
The Application's own data about one incoming request (who is calling, for which account). A Gateway turns it into Command metadata explicitly; nothing in it reaches a Command otherwise.
_Avoid_: oblakMetadata, auth metadata, session

**Access map**:
A Gateway's declaration of which Commands and Readmodels its callers may use, and with which filters.
_Avoid_: ACL, permissions, security package

**Worker**:
Application code that performs one kind of queued Message, usually a call to an external system, outside any Command or Reaction.
_Avoid_: job handler, consumer

**Task**:
A one-off script run against a live Application from the CLI.
_Avoid_: job, migration, script

**Services**:
The Application's own clients and helpers (provider clients, pools, CRUD models) that Steps, Reactions, Workers, plugins and Tasks receive. Supplied as `services` they are borrowed; built by `setupServices` they are owned and cleaned up on close. Readmodel Reactions get **Readmodel helpers** instead when the Application supplies them; otherwise they share these.
_Avoid_: container, dependencies, injector

**Readmodel helpers**:
The deterministic helpers an Application supplies as `readmodelServices` (typed by `Register.readmodelServices`). Once supplied, Readmodel Reactions get only them from `getServices()`, live and on rebuild; they are borrowed and read-only. Omitted, Readmodels keep the Application's Services.
_Avoid_: Readmodel services, helper map, helper object, Readmodel isolation

**Readmodel import**:
Taking over a Readmodel group's documents that another store already folded up to a Position, instead of rebuilding them from the log. They are loaded into a pending generation whose Checkpoint is that Position, which then continues as a rebuild of the later Events. It is correct only when the documents equal what the group's Reactions produce from the log up to that Position.
_Avoid_: adoption, seeding, Readmodel migration

**Rebuild cache**:
The memory, capped by `rebuildCacheBytes` for the whole process, in which a rebuild keeps what it reads until it ends. It holds the rebuilt group's own documents, which stay exact because the rebuild is their only writer. While it holds all of the group's documents, it also answers equality and `in` filters from indexes over them. It also holds reads of the groups it reads, each reused only while that group's Checkpoint covers the batch, and values an Application keeps with `rebuildMemo`.
_Avoid_: replay buffer, document cache
