import { afterEach, describe, expect, test } from "vitest";
import { partitionFor } from "../../src/transport/partition.js";
import type { Command, Message, Outcome, Transport } from "../../src/types.js";

/** Connects one more Transport to the same broker namespace. */
export type Connect = () => Promise<Transport>;

const command = (id: string, context = "sales", stream = "1"): Command => ({
  id,
  name: "create",
  stream: { context, aggregate: "order", id: stream },
  payload: { id },
  metadata: {},
  correlationId: id,
  schemaVersion: 1,
  timestamp: "2026-01-01T00:00:00.000Z",
});
const message = (id: string): Message =>
  ({ ...command(id), name: "deliver" }) as Message;
const all = Array.from({ length: 128 }, (_, p) => p);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const until = async (check: () => boolean, ms = 5000) => {
  const end = Date.now() + ms;
  while (!check()) {
    if (Date.now() > end) throw new Error("Condition not reached in time");
    await sleep(5);
  }
};
const accepted: Outcome = { status: "accepted", events: [] };

/** The behaviour every Transport adapter promises (ADR-0003, ADR-0008). `network` returns a fresh, isolated broker namespace. */
export function transportContract(adapter: string, network: () => Connect) {
  describe(`Transport contract: ${adapter}`, () => {
    let open: Transport[] = [];
    let connect: Connect;
    const transport = async () => {
      const t = await connect();
      open.push(t);
      return t;
    };
    afterEach(async () => {
      await Promise.all(open.map((t) => t.close()));
      open = [];
    });
    const setup = () => {
      connect = network();
    };

    test("Command outcomes round-trip from handler to sender", async () => {
      setup();
      const [sender, receiver] = [await transport(), await transport()];
      const received: Command[] = [];
      await receiver.handle("sales", all, async (c) => {
        received.push(c);
        return c.id === "no"
          ? {
              status: "rejected",
              error: { code: "rule", kind: "rule", message: "no" },
            }
          : { status: "accepted", events: [], result: { echoed: c.payload } };
      });
      expect(await sender.send(command("yes"), { deadline: 3000 })).toEqual({
        status: "accepted",
        events: [],
        result: { echoed: { id: "yes" } },
      });
      expect(await sender.send(command("no"), { deadline: 3000 })).toEqual({
        status: "rejected",
        error: { code: "rule", kind: "rule", message: "no" },
      });
      expect(received[0]).toEqual(command("yes"));
    });

    test("a throwing handler gives failed", async () => {
      setup();
      const t = await transport();
      await t.handle("sales", all, async () => {
        throw new Error("boom");
      });
      expect(await t.send(command("x"), { deadline: 3000 })).toMatchObject({
        status: "failed",
        error: { message: expect.any(String) },
      });
    });

    test("the deadline gives unknown and the Command stays queued for a later handler", async () => {
      setup();
      const [sender, receiver] = [await transport(), await transport()];
      expect(await sender.send(command("late"), { deadline: 50 })).toEqual({
        status: "unknown",
        commandId: "late",
      });
      const seen: string[] = [];
      await receiver.handle("sales", all, async (c) => {
        seen.push(c.id);
        return accepted;
      });
      await until(() => seen.length === 1);
      expect(seen).toEqual(["late"]);
    });

    test("one Partition has one active handler across instances, in order, handing over after in-flight work", async () => {
      setup();
      const [sender, first, second] = [
        await transport(),
        await transport(),
        await transport(),
      ];
      const partition = partitionFor(command("a").stream);
      const seen: string[] = [];
      let running = 0,
        maxRunning = 0;
      const handler = (name: string) => async (c: Command) => {
        maxRunning = Math.max(maxRunning, ++running);
        await sleep(20);
        running--;
        seen.push(`${name}:${c.id}`);
        return accepted;
      };
      const stops = {
        first: await first.handle("sales", [partition], handler("first")),
        second: await second.handle("sales", [partition], handler("second")),
      };
      for (const id of ["a", "b", "c", "d"])
        expect(
          (await sender.send(command(id), { deadline: 3000 })).status,
        ).toBe("accepted");
      // Which instance is active is the adapter's choice; it must be one, throughout.
      const active = seen[0]!.split(":")[0] as "first" | "second";
      const standby = active === "first" ? "second" : "first";
      const inFlight = sender.send(command("e"), { deadline: 3000 });
      await until(() => running === 1);
      const stopped = stops[active]();
      const later = sender.send(command("f"), { deadline: 3000 });
      await stopped;
      expect(seen).toContain(`${active}:e`);
      expect((await inFlight).status).toBe("accepted");
      expect((await later).status).toBe("accepted");
      expect(maxRunning).toBe(1);
      expect(seen).toEqual([
        ...["a", "b", "c", "d", "e"].map((id) => `${active}:${id}`),
        `${standby}:f`,
      ]);
    });

    test("Partitions spread across instances, and a joining instance takes its share without overlap", async () => {
      setup();
      const sender = await transport();
      const instances = [await transport()];
      const handledBy = new Map<string, Set<number>>();
      const running = new Map<number, number>();
      let overlap = false;
      const handler = (name: number) => async (c: Command) => {
        const p = partitionFor(c.stream);
        running.set(p, (running.get(p) ?? 0) + 1);
        if (running.get(p)! > 1) overlap = true;
        await sleep(2);
        running.set(p, running.get(p)! - 1);
        if (!handledBy.has(c.id)) handledBy.set(c.id, new Set());
        handledBy.get(c.id)!.add(name);
        return accepted;
      };
      await instances[0]!.handle("sales", all, handler(0));
      const round = async (prefix: string) => {
        const ids = Array.from({ length: 60 }, (_, i) => `${prefix}${i}`);
        await Promise.all(
          ids.map(async (id) =>
            expect(
              (await sender.send(command(id, "sales", id), { deadline: 5000 }))
                .status,
            ).toBe("accepted"),
          ),
        );
        return new Set(ids.flatMap((id) => [...handledBy.get(id)!]));
      };
      expect(await round("alone")).toEqual(new Set([0]));
      instances.push(await transport(), await transport());
      await instances[1]!.handle("sales", all, handler(1));
      await instances[2]!.handle("sales", all, handler(2));
      // A joining consumer takes a queue over once the current one has nothing in flight.
      await sleep(200);
      expect(await round("joined")).toEqual(new Set([0, 1, 2]));
      expect(overlap).toBe(false);
    });

    test("concurrent sends to one Stream are handled in send order", async () => {
      setup();
      const [sender, receiver] = [await transport(), await transport()];
      const seen: string[] = [];
      await receiver.handle("sales", all, async (c) => {
        seen.push(c.id);
        return accepted;
      });
      const ids = ["1", "2", "3", "4", "5", "6", "7", "8"];
      await Promise.all(
        ids.map((id) => sender.send(command(id), { deadline: 3000 })),
      );
      expect(seen).toEqual(ids);
    });

    test("a bulk backlog never delays an interactive Command in its Partition, and each lane keeps its order", async () => {
      setup();
      const [sender, receiver] = [await transport(), await transport()];
      let open!: () => void;
      const gate = new Promise<void>((resolve) => (open = resolve));
      const handled: string[] = [];
      await receiver.handle("sales", all, async (c) => {
        if (c.id.startsWith("bulk")) await gate;
        handled.push(c.id);
        return accepted;
      });
      const bulk = ["bulk-1", "bulk-2", "bulk-3"].map((id) =>
        sender.send(command(id), { deadline: 5000, lane: "bulk" }),
      );
      await sleep(50);
      expect(await sender.send(command("interactive"), { deadline: 3000 })).toEqual(
        accepted,
      );
      expect(handled).toEqual(["interactive"]);
      open();
      await Promise.all(bulk);
      expect(handled).toEqual(["interactive", "bulk-1", "bulk-2", "bulk-3"]);
    });

    test("a handler receives only its own Context's Commands", async () => {
      setup();
      const [sender, sales, billing] = [
        await transport(),
        await transport(),
        await transport(),
      ];
      const seen: string[] = [];
      await sales.handle("sales", all, async (c) => {
        seen.push(`sales:${c.id}`);
        return accepted;
      });
      expect(
        await sender.send(command("bill", "billing"), { deadline: 200 }),
      ).toEqual({ status: "unknown", commandId: "bill" });
      expect(seen).toEqual([]);
      await billing.handle("billing", all, async (c) => {
        seen.push(`billing:${c.id}`);
        return accepted;
      });
      await until(() => seen.length === 1);
      expect(seen).toEqual(["billing:bill"]);
    });

    test("invalid Command Partitions are refused", async () => {
      setup();
      const t = await transport();
      for (const partitions of [[128], [-1], [1.5]])
        await expect(
          t.handle("sales", partitions, async () => accepted),
        ).rejects.toThrow(/Invalid Command Partition/);
    });

    test("broadcast reaches current subscribers only, at most once, and a failing subscriber blocks no other", async () => {
      setup();
      const [publisher, a, b] = [
        await transport(),
        await transport(),
        await transport(),
      ];
      await publisher.publish("live", { id: "before" });
      const failing: string[] = [],
        healthy: string[] = [];
      await a.subscribe("live", (m) => {
        failing.push(m.id);
        throw new Error("subscriber broke");
      });
      await b.subscribe("live", (m) => {
        healthy.push(m.id);
      });
      await b.subscribe("other", (m) => {
        healthy.push(`other:${m.id}`);
      });
      await publisher.publish("live", { id: "one" });
      await publisher.publish("live", { id: "two" });
      await until(() => failing.length === 2 && healthy.length === 2);
      await sleep(50);
      expect(failing).toEqual(["one", "two"]);
      expect(healthy).toEqual(["one", "two"]);
    });

    test("queued Messages go to one of the competing consumers each", async () => {
      setup();
      const [producer, a, b] = [
        await transport(),
        await transport(),
        await transport(),
      ];
      const queue = `contract.${crypto.randomUUID()}`;
      const seen: string[] = [];
      for (const consumer of [a, b])
        await consumer.consume(queue, async (m) => {
          await sleep(5);
          seen.push(m.id);
        });
      const ids = Array.from({ length: 10 }, (_, i) => `m${i}`);
      for (const id of ids) await producer.enqueue(queue, message(id));
      await until(() => seen.length === ids.length);
      await sleep(50);
      expect([...seen].sort()).toEqual([...ids].sort());
    });

    test("a failed queue attempt is delivered again", async () => {
      setup();
      const t = await transport();
      const queue = `contract.${crypto.randomUUID()}`;
      let attempts = 0;
      await t.consume(queue, async (m) => {
        expect(m.payload).toEqual({ id: "retry" });
        if (++attempts === 1) throw new Error("connection lost");
      });
      await t.enqueue(queue, message("retry"));
      await until(() => attempts === 2);
      await sleep(50);
      expect(attempts).toBe(2);
    });

    test("concurrency bounds in-flight queued Messages", async () => {
      setup();
      const t = await transport();
      const queue = `contract.${crypto.randomUUID()}`;
      let running = 0,
        maxRunning = 0,
        done = 0;
      await t.consume(
        queue,
        async () => {
          maxRunning = Math.max(maxRunning, ++running);
          await sleep(30);
          running--;
          done++;
        },
        { concurrency: 2 },
      );
      for (let i = 0; i < 6; i++) await t.enqueue(queue, message(`c${i}`));
      await until(() => done === 6);
      expect(maxRunning).toBe(2);
      await expect(
        t.consume(queue, async () => {}, { concurrency: 0 }),
      ).rejects.toThrow(/Invalid concurrency/);
    });

    test("close resolves pending sends as unknown and reports disconnected", async () => {
      setup();
      const t = await connect();
      await t.connect?.();
      expect(t.connected()).toBe(true);
      const pending = t.send(command("pending"), { deadline: 5000 });
      await sleep(20);
      await t.close();
      expect(
        await Promise.race([pending, sleep(200).then(() => "still pending")]),
      ).toEqual({ status: "unknown", commandId: "pending" });
      expect(t.connected()).toBe(false);
    });
  });
}
