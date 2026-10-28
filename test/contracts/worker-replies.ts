import { expect, test, vi } from "vitest";
import pino from "pino";
import { compile } from "../../src/authoring/index.js";
import { createApplication } from "../../src/runtime/index.js";
import { permanent } from "../../src/runtime/delivery.js";
import { createInMemoryTransport } from "../../src/transport/memory.js";
import type { Persistence } from "../../src/types.js";
import type { SagaDefinition } from "../../src/runtime/types.js";

/** A Worker's reply reaches the Saga instance that queued its Message, on any store that keeps Saga Messages. */
export function workerReplyContract(adapter: string, persistence: () => Persistence | Promise<Persistence>) {
  test(`${adapter}: a Worker replies to the Saga instance that queued its Message, after success and after its dead letter`, async () => {
    const file = compile("crm", "file", {
      commands: { open: [(command, file) => file.apply.opened(command.payload)] },
      events: { opened() {} },
    });
    const replies: any[] = [];
    const saga: SagaDefinition = {
      reactions: {
        "crm.file.opened": (event, instance: any, app) => {
          instance.set("opened", true);
          app.queue("lookup", { found: event.payload.found }, { replyTo: "looked-up" });
        },
      },
      messages: {
        "looked-up": (message, instance: any) => {
          replies.push(message);
          instance.set("outcome", message.payload.outcome);
        },
      },
    };
    const store = await persistence();
    const app = createApplication({
      name: "worker-replies",
      store,
      aggregates: [file],
      transport: createInMemoryTransport(),
      sagas: { connector: saga },
      workers: {
        lookup: {
          handles: ["lookup"],
          execute: ({ payload }: any) => {
            if (!payload.found) throw permanent(new Error("No such file"));
            return { reference: "ref-1" };
          },
        },
      },
      pollInterval: 50,
      logger: pino({ level: "silent" }),
    });
    await app.start();
    try {
      const open = (id: string, found: boolean) =>
        app.sendCommand({ stream: { context: "crm", aggregate: "file", id }, name: "open", payload: { found } });
      const outcome = await open("f1", true);
      await open("f2", false);
      await vi.waitFor(() => expect(replies).toHaveLength(2), { timeout: 3000 });
      const [opened] = outcome.status === "accepted" ? outcome.events : [];
      expect(replies.find((reply) => reply.instance === "f1")).toMatchObject({
        saga: "connector",
        name: "looked-up",
        payload: { outcome: "ok", value: { reference: "ref-1" } },
        correlationId: opened!.correlationId,
      });
      expect(replies.find((reply) => reply.instance === "f2")).toMatchObject({
        name: "looked-up",
        payload: { outcome: "failed", error: "No such file", attempts: 1 },
      });
      // The dead letter stays for an operator; the reply only tells the instance.
      expect((await store.deadLetters()).filter((letter) => letter.kind === "worker")).toHaveLength(1);
      await vi.waitFor(async () =>
        expect(await store.loadSaga("saga:connector", "f2")).toMatchObject({ outcome: "failed" }),
      );
    } finally {
      await app.close();
    }
  });
}
