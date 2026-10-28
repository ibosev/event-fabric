# Migrating an oblak Application

EventFabric keeps the way oblak Applications are written: Aggregates, Readmodels and Sagas stay small declarative definitions. So an existing Application moves over mostly by tooling, not by a rewrite. This guide covers the approach, the tool and the order of the work. [migration.md](migration.md) is the detailed reference behind each step.

## The idea

The migration has three parts:

- **The source.** A tool rewrites the Application's code so it runs on EventFabric while keeping its old behavior.
- **The data.** The historical Events, Saga states and Readmodel documents are imported into EventFabric's stores.
- **The evidence.** Before the switch, audits compare the migrated Application with the old one on the same data.

The old Application is never changed in place. All work happens on a migration branch or copy. The migration can be rebuilt from the original source at any time, by the tool plus a small set of the Application's own decisions.

## Assumptions

The tool works by convention. It expects an Application laid out the way oblak Applications usually are:

- Aggregates grouped by Context;
- Readmodels grouped by Readmodel group;
- Sagas, Gateways and Tasks in their own folders;
- the old framework's toolkit imported from its package;
- module aliases declared in the Application's package.

The further an Application strays from these conventions, the more it relies on review and on its own decisions (see below).

## What the tool does

`event-fabric migrate-app <folder>` walks the Application's source files and works out what each one is from where it sits: an Aggregate, a Readmodel, a Saga, a Gateway route, a library module or a Task. Then it migrates each file for its kind:

- **Imports.** References to the old framework's toolkit move to EventFabric's equivalents, or to its compatibility helpers.
- **Code.** Old API usage becomes today's API: accessors, guards, Event and Command names, Saga effects, and Gateway routes and queries. Where today's API behaves differently, the rewrite keeps the old behavior explicitly at the call site (`event-fabric/compat`), so the Application can adopt the new behavior later, case by case.
- **Composition root.** It writes what the Application needs to start on EventFabric, wherever a file is missing: the configuration, the declarations derived from the folders, the services the old code expects, and the Gateways on their old ports. It also adds EventFabric to the Application's dependencies.
- **Reviews.** Whatever it cannot decide safely is left unchanged and listed, with the reason.

The tool is repeatable. Running it again on its own output changes nothing. `--check` reports pending changes without writing, and `--report` writes what was done to each file. Tasks keep their old form; EventFabric's Task runner runs them as they are.

## The Application's own decisions

Some changes follow from no convention. Examples: a guard for malformed historical data, a field that moved, or a reviewed Readmodel schema. These are the Application's decisions, kept as a folder of patches that the tool applies after its own rules (`--patches`). A patch can change, add or delete files. Keeping decisions apart from the tool's output means:

- the migration stays reproducible from the original source;
- each decision stays visible and reviewable on its own;
- a new version of the tool can be adopted by running everything again.

## Phases

### 1. Coexistence

EventFabric is installed next to the old framework, under a separate package name. The migrated Application runs on EventFabric, while the old framework stays available, so both can run against the same data and be compared. This is where most of the work happens:

1. Run the tool.
2. Resolve every review entry.
3. Record the Application's decisions as patches.
4. Run the Application's lint and type checks.

Runtime roles stay disabled until the Application declares itself ready.

### 2. Verification

The migrated Application is compared with the old one on a copy of real data:

- Commands replayed through both;
- Readmodels rebuilt and compared document by document;
- Sagas continued from their saved state;
- Gateway answers compared over HTTP.

Every difference is either fixed or explained by an explicit decision. [migration.md](migration.md#offline-import-and-replay-audit) describes the import and the replay audit. Finally, a smoke test starts the whole Application: it serves a request, catches up, and restarts cleanly.

### 3. Cutover

The old framework is removed:

1. Move anything the Application still uses from the old framework into the Application's own code, through its patches.
2. Remove the old dependency.
3. Run the tool once more with `--packages cutover`. EventFabric then takes its own package name everywhere, the composition root included.
4. Retire the old entry points with a separate set of cutover patches.

Then repeat the checks, and install the Application from scratch, from its lockfile alone. A reused `node_modules` can hide packages the Application only ever received through the old framework's own dependencies.

The data cutover runs on a stopped system. Events can be imported ahead of time, so only the tail remains for the window. Readmodels are imported at the log head, or rebuilt. Saga states are imported with their Checkpoints at the head. The Outbox and Worker queues start empty. [migration.md](migration.md#final-cutover-and-rollback) gives the procedure and the rollback rule.

## Scaling after migration

A migrated Application scales like any other EventFabric Application, and the stored data stays where the cutover put it:

- **More replicas.** Readmodel groups and Sagas are shared between replicas through Leases. A group runs on one replica at a time, so more replicas spread groups out but don't speed up a single group.
- **More Partitions.** `partitions: N` splits a hot Readmodel group or Saga by document or Saga instance. Changing N drains the old layout and starts the new one where it stopped: nothing is replayed, moved or renamed.
- **Old Reactions that read their own group.** The old framework let a Readmodel's Reactions look up any model of their group. A partitioned group can't do that, and the runtime refuses it. Such a group stays at one Partition, or the models it reads move to a group of their own, which needs a rebuild.
- **Storage names.** Readmodels keep the database layout the composition root gave them, groups that shared a database included. Only a rebuild writes new collections. Saga state lives with the Event log in Postgres; the old Saga collections serve only as the cutover's import source.

[architecture.md](architecture.md#horizontal-scaling) explains how replicas and Partitions share the work.

## Recommended practice

- Work on a branch. Keep the Application's own fixes, the tool's output, the Application's decisions and any rehearsal-only tooling in separate commits, so the diff shows what the migration really takes.
- Rebuild the branch from the original source with the tools alone, and check that the result matches. A change to the tool or to a decision is then adopted by rebuilding, never by editing the output by hand.
- Rehearse the whole sequence, data included, on copies before the real cutover, and time each step.
- Keep providers and external services switched off by configuration during rehearsals.

## Where to go next

- [migration.md](migration.md): the detailed reference, covering the codemod options, the compatibility helpers, the composition root, the CLI, data import, audits and the final cutover.
- [comparison-with-oblak.md](comparison-with-oblak.md): what changes underneath, and why.
- [architecture.md](architecture.md): EventFabric on its own terms.
