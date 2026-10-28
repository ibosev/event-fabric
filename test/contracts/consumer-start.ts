import { expect, test } from "vitest";
import { randomUUID } from "node:crypto";
import pino from "pino";
import { compile } from "../../src/authoring/index.js";
import { createApplication } from "../../src/runtime/index.js";
import { createInMemoryTransport } from "../../src/transport/memory.js";
import { encodePosition } from "../../src/position.js";
import { workflowsApi } from "../../src/internal.js";
import type { Persistence } from "../../src/types.js";
import type { SagaDefinition, WorkflowDefinition } from "../../src/runtime/types.js";

const account = compile("c", "a", { commands: {}, events: { added() {} } });

/**
 * Where a new Saga or Workflow group starts reading the Event log. Each test gets
 * its own Event store, so `beginning` sees only the Events the test appended.
 */
export function consumerStartContract(adapter: string, persistence: () => Persistence | Promise<Persistence>) {
  const world = async () => {
    const store = await persistence();
    const warnings: { msg: string; [key: string]: unknown }[] = [];
    const logger = pino({ level: "warn" }, { write: (line: string) => void warnings.push(JSON.parse(line)) });
    /** Records `c.a.added` for `owner` with value `n`, stamped `at`. */
    const append = async (owner: string, n: number, at = new Date()) => {
      const id = randomUUID();
      const [event] = await store.appendToStream({ context: "c", aggregate: "a", id: randomUUID() }, 0, [
        {
          id,
          name: "added",
          payload: { owner, n },
          metadata: {},
          schemaVersion: 1,
          timestamp: at.toISOString(),
          commandId: id,
          correlationId: id,
        },
      ]);
      return event!;
    };
    const saga = (options: Pick<SagaDefinition, "from" | "partitions"> = {}): SagaDefinition => ({
      ...options,
      identity: { "c.a.added": (event: any) => event.payload.owner },
      reactions: {
        "c.a.added": (event: any, instance: any) => instance.set("seen", [...(instance.get("seen") ?? []), event.payload.n]),
      },
    });
    const make = (sagas: Record<string, SagaDefinition>, workflows: Record<string, WorkflowDefinition> = {}) =>
      createApplication({
        name: "start",
        store,
        transport: createInMemoryTransport(),
        aggregates: [account],
        sagas,
        workflows,
        pollInterval: 5,
        leaseDuration: 300,
        logger,
      });
    /** What Saga `name`'s instance for `owner` has seen, once it equals `expected`. */
    const sees = (name: string, owner: string, expected: number[]) =>
      expect
        .poll(async () => (await store.loadSaga(`saga:${name}`, owner))?.seen ?? [], { timeout: 5000, interval: 10 })
        .toEqual(expected);
    return { store, warnings, append, saga, make, sees };
  };

  test(`${adapter}: a new Saga starts at now by default, after every Event recorded before it`, async () => {
    const { append, saga, make, sees } = await world();
    await append("a", 1);
    const app = make({ s: saga() });
    await app.start();
    try {
      await append("a", 2);
      await sees("s", "a", [2]);
    } finally {
      await app.stop();
    }
  });

  test(`${adapter}: from beginning handles every Event recorded before the Saga started`, async () => {
    const { append, saga, make, sees } = await world();
    await append("a", 1);
    const app = make({ s: saga({ from: "beginning" }) });
    await app.start();
    try {
      await append("a", 2);
      await sees("s", "a", [1, 2]);
    } finally {
      await app.stop();
    }
  });

  test(`${adapter}: from a Date handles the Events recorded at or after it`, async () => {
    const { append, saga, make, sees } = await world();
    await append("a", 1, new Date("2026-01-01T00:00:00Z"));
    await append("a", 2, new Date("2026-03-01T00:00:00Z"));
    const app = make({
      between: saga({ from: new Date("2026-02-01T00:00:00Z") }),
      before: saga({ from: new Date("2025-01-01T00:00:00Z") }),
      after: saga({ from: new Date("2027-01-01T00:00:00Z") }),
    });
    await app.start();
    try {
      await append("a", 3);
      await sees("between", "a", [2, 3]);
      await sees("before", "a", [1, 2, 3]);
      await sees("after", "a", [3]);
    } finally {
      await app.stop();
    }
  });

  test(`${adapter}: from a Position token handles the Events after that Position`, async () => {
    const { append, saga, make, sees } = await world();
    const first = await append("a", 1);
    await append("a", 2);
    const app = make({ s: saga({ from: { position: encodePosition(first.position) } }) });
    await app.start();
    try {
      await append("a", 3);
      await sees("s", "a", [2, 3]);
    } finally {
      await app.stop();
    }
  });

  test(`${adapter}: two replicas starting a new partitioned group at once agree on one start`, async () => {
    const { store, append, saga, make, sees } = await world();
    const owners = ["a", "b", "c", "d", "e", "f"];
    for (const owner of owners) await append(owner, 1);
    const replicas = [make({ s: saga({ partitions: 3 }) }), make({ s: saga({ partitions: 3 }) })];
    await Promise.all(replicas.map((app) => app.start()));
    try {
      for (const owner of owners) await append(owner, 2);
      for (const owner of owners) await sees("s", owner, [2]);
      const checkpoints = (await store.consumers()).filter((s) => s.group === "saga:s").map((s) => s.checkpoint);
      expect(checkpoints).toHaveLength(3);
      expect(checkpoints.every(Boolean)).toBe(true);
    } finally {
      await Promise.all(replicas.map((app) => app.stop()));
    }
  });

  test(`${adapter}: a restart and a partitions change keep the Checkpoint, whatever from says`, async () => {
    const { append, saga, make, sees } = await world();
    await append("a", 1);
    const first = make({ s: saga() });
    await first.start();
    await append("a", 2);
    await sees("s", "a", [2]);
    await first.stop();
    await append("a", 3);
    const restarted = make({ s: saga() });
    await restarted.start();
    await sees("s", "a", [2, 3]);
    await restarted.stop();
    await append("a", 4);
    const repartitioned = make({ s: saga({ partitions: 2 }) });
    await repartitioned.start();
    try {
      await sees("s", "a", [2, 3, 4]);
    } finally {
      await repartitioned.stop();
    }
  });

  test(`${adapter}: a changed from on an existing group logs a warning and changes nothing`, async () => {
    const { warnings, append, saga, make, sees } = await world();
    await append("a", 1);
    const first = make({ s: saga() });
    await first.start();
    await append("a", 2);
    await sees("s", "a", [2]);
    await first.stop();
    const changed = make({ s: saga({ from: "beginning" }) });
    await changed.start();
    try {
      await append("a", 3);
      await sees("s", "a", [2, 3]);
      expect(warnings).toContainEqual(
        expect.objectContaining({ consumerGroup: "saga:s", from: "beginning", startedFrom: "now" }),
      );
    } finally {
      await changed.stop();
    }
  });

  test(`${adapter}: a retired group deployed again with from beginning replays the history`, async () => {
    const { append, saga, make, sees } = await world();
    await append("a", 1);
    const first = make({ s: saga() });
    await first.start();
    await append("a", 2);
    await sees("s", "a", [2]);
    await first.stop();
    await first.retireConsumer("saga:s");
    const replay = make({ s: saga({ from: "beginning" }) });
    await replay.start();
    try {
      await sees("s", "a", [1, 2]);
    } finally {
      await replay.stop();
    }
  });

  test(`${adapter}: a new Workflow starts instances only for Events after from; startWorkflow is unaffected`, async () => {
    const { store, append, make } = await world();
    await append("old", 1);
    const greet: WorkflowDefinition = {
      version: 1,
      start: "c.a.added",
      id: (event) => event.payload.owner,
      run: async () => "done",
    };
    const app = make({}, { greet });
    await app.start();
    try {
      await append("new", 2);
      await app[workflowsApi].start("greet", "manual", {});
      const status = (id: string) => store.loadWorkflow!("workflow:greet", id).then((record) => record?.status);
      await expect.poll(() => status("new"), { timeout: 5000, interval: 10 }).toBe("completed");
      await expect.poll(() => status("manual"), { timeout: 5000, interval: 10 }).toBe("completed");
      expect(await status("old")).toBeUndefined();
    } finally {
      await app.stop();
    }
  });
}

