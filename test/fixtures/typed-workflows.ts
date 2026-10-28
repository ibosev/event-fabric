// Type-checked by test/typed-authoring.test.ts with NO generated Register: the domain, the Worker
// and the Workflow carry every type. Every @ts-expect-error must stay an error; no casts.
import { aggregate, defineDomain, message, Type, worker, type PluginContext, type ReactionApp } from "../../src/index.js";
import { signal, workflow, type WorkflowQuery } from "../../src/workflows/index.js";
import { processFixture, signal as signalStep, start, workflowStep } from "../../src/testing.js";
import type { MessagePayloads, RegisteredServices } from "../../src/registry.js";

const invoice = aggregate("billing", "invoice", {
  state: Type.Object({}),
  initialState: {},
  events: {
    issued: { schema: Type.Object({ amount: Type.Number() }), handler() {} },
    settled: { schema: Type.Object({ paid: Type.Number() }), handler() {} },
    voided: { schema: Type.Object({ reason: Type.String() }), handler() {} },
  },
  commands: {
    markOverdue: { schema: Type.Object({}), steps: [] },
    void: { schema: Type.Object({ reason: Type.String() }), steps: [] },
  },
});
const domain = defineDomain(invoice);
const remind = message("remind", Type.Object({ invoiceId: Type.String() }));
const mailer = worker(remind, { execute: () => ({ messageId: "m1" }) });

export const dunning = workflow("dunning", {
  version: 1,
  domain,
  start: domain.billing.invoice.issued,
  id: (event) => event.aggregate.id,
  correlate: { [domain.billing.invoice.settled]: (event) => event.aggregate.id },
  signals: { promised: signal<{ until: string }>(), checked: signal(Type.Object({ ok: Type.Boolean() })), poke: signal() },
  state: Type.Object({ stage: Type.String(), reminders: Type.Number() }),
  async run(ctx, issued) {
    const amount: number = issued.payload.amount;
    // @ts-expect-error the start Event has no such field
    void issued.payload.paid;

    const settled = await ctx.waitFor(domain.billing.invoice.settled, { timeout: 14 * 86_400_000 });
    if (!settled.timedOut) {
      const paid: number = settled.value.payload.paid;
      return { paid };
    }
    const plain = await ctx.waitFor(domain.billing.invoice.settled);
    const paid: number = plain.payload.paid;
    // @ts-expect-error an Event the Workflow does not correlate cannot be waited for
    await ctx.waitFor(domain.billing.invoice.voided);
    // @ts-expect-error the Event payload has no such field
    void plain.payload.reason;

    const promise = await ctx.waitFor(ctx.signals.promised);
    const until: string = promise.until;
    // @ts-expect-error the Signal payload has no such field
    void promise.date;
    const checked = await ctx.waitFor(ctx.signals.checked, { timeout: 1000 });
    if (!checked.timedOut) {
      const ok: boolean = checked.value.ok;
      void ok;
    }
    const poked: null = await ctx.waitFor(ctx.signals.poke);
    void poked;

    const reply = await ctx.ask(mailer, { invoiceId: issued.aggregate.id });
    if (reply.outcome === "ok") {
      const id: string = reply.value.messageId;
      void id;
    }
    // @ts-expect-error without a timeout the reply never times out
    if (reply.outcome === "timedOut") return;
    const timed = await ctx.ask(mailer, { invoiceId: "i" }, { timeout: 5000 });
    if (timed.outcome === "timedOut") await ctx.set({ stage: "silent", reminders: 0 });
    // @ts-expect-error the Worker's Message payload is checked
    await ctx.ask(mailer, { invoice: "i" });

    await ctx.tell(domain.billing.invoice.markOverdue(issued.aggregate.id, {}));
    // @ts-expect-error the Command payload is checked
    await ctx.tell(domain.billing.invoice.void(issued.aggregate.id, { why: "late" }));
    await ctx.queue(remind, { invoiceId: "i" });
    // @ts-expect-error the queued payload is checked
    await ctx.queue(remind, { id: "i" });

    await ctx.set({ stage: "overdue", reminders: 1 });
    // @ts-expect-error the view follows the state schema
    await ctx.set({ stage: 1 });
    // @ts-expect-error no Readmodel group is declared in reads
    await ctx.read("billing", "invoices", "i");
    const now: Date = await ctx.now();
    void [amount, until, now];
    return { paid };
  },
});

const notice = workflow("notice", { version: 1, async run(_ctx, input: { to: string }) { return input.to; } });
export const reminder = workflow("reminder", {
  version: 1,
  signals: { stop: signal() },
  async run(ctx, input: { invoiceId: string }) {
    await ctx.sleep("wait", 1000);
    await ctx.start(notice, input.invoiceId, { to: "ada" });
    // @ts-expect-error the started Workflow's input is checked
    await ctx.start(notice, input.invoiceId, { to: 1 });
    // @ts-expect-error a Signal of another Workflow cannot be waited for here
    await ctx.waitFor(dunning.signals.poke);
    return input.invoiceId.length;
  },
});

// The contexts take the same Handles.
export const fromPlugin = async (ctx: PluginContext) => {
  await ctx.startWorkflow(reminder, "i1", { invoiceId: "i1" });
  await ctx.startWorkflow(reminder, "i1", { invoiceId: "i1" }, { signal: reminder.signals.stop() });
  // @ts-expect-error the input is checked
  await ctx.startWorkflow(reminder, "i1", { invoice: "i1" });
  // @ts-expect-error the input is required
  await ctx.startWorkflow(reminder, "i1");
  // A Signal carries its Workflow.
  await ctx.sendSignal("i1", dunning.signals.promised({ until: "2026-12-01" }));
  await ctx.sendSignal("i1", reminder.signals.stop());
  // @ts-expect-error the Workflow is not named next to its Signal
  await ctx.sendSignal(dunning, "i1", dunning.signals.poke());
  // @ts-expect-error the Signal payload is checked
  await ctx.sendSignal("i1", dunning.signals.promised({ until: 1 }));
  // @ts-expect-error an undeclared Signal is refused
  await ctx.sendSignal("i1", dunning.signals.missing());
  // @ts-expect-error a start's Signal belongs to the Workflow it starts
  await ctx.startWorkflow(reminder, "i1", { invoiceId: "i1" }, { signal: dunning.signals.poke() });
  // @ts-expect-error a Signal with a payload needs it
  dunning.signals.promised();
  const query: WorkflowQuery<typeof dunning> | null = await ctx.queryWorkflow(dunning, "i1");
  const stage: string | undefined = query?.state?.stage;
  const paid: number | undefined = query?.result?.paid;
  // @ts-expect-error the state has no such field
  void query?.state?.missing;
  const length: number | undefined = (await ctx.queryWorkflow(reminder, "i1"))?.result;
  void [stage, paid, length];
};
export const fromSaga = (app: ReactionApp) => {
  // A Saga records the start with its state: nothing to await.
  const recorded: void = app.startWorkflow(reminder, "i1", { invoiceId: "i1" });
  app.sendSignal("i1", dunning.signals.poke());
  void recorded;
};

// Absolute times, as Saga Deadlines take them.
export const dated = workflow("dated", {
  version: 1,
  domain,
  correlate: { [domain.billing.invoice.settled]: (event) => event.aggregate.id },
  async run(ctx, input: { due: string }) {
    const paid = await ctx.waitFor(domain.billing.invoice.settled, { at: input.due });
    if (paid.timedOut) await ctx.sleep("grace", new Date(input.due));
    await ctx.sleep("pause", "2026-12-01T00:00:00.000Z");
    await ctx.start(notice, "n1", { to: "ada" }, { reuse: true });
    // @ts-expect-error a timeout and an absolute time exclude each other
    await ctx.waitFor(domain.billing.invoice.settled, { timeout: 1, at: input.due });
    return paid.timedOut ? "late" : "paid";
  },
});

// The process fixture types its steps and expectations through the Workflow under test.
export const fixtureSteps = async () => {
  await using flow = processFixture(reminder);
  await flow
    .given(start(reminder, "i1", { invoiceId: "i1" }), start(reminder, "i1", { invoiceId: "i1" }, { reuse: true }))
    .when(signalStep("i1", reminder.signals.stop()))
    .expectResult(2)
    .expectDeadline(workflowStep.sleep("wait"), "2026-12-01T00:00:00.000Z")
    .expectDeadline(workflowStep.waitFor(dunning.signals.poke, 1), "2026-12-01T00:00:00.000Z")
    .expectDeadline(workflowStep.waitFor(domain.billing.invoice.settled), "2026-12-01T00:00:00.000Z")
    .expectQueued([
      { name: "remind", payload: { invoiceId: "i" }, replyTo: { workflow: "dunning", instance: "i", step: workflowStep.ask(mailer) } },
    ]);
  // @ts-expect-error the input is checked
  start(reminder, "i1", { invoice: "i1" });
  // @ts-expect-error the Signal names its Workflow
  signalStep(reminder, "i1", reminder.signals.stop());
  // @ts-expect-error the Signal payload is checked
  signalStep("i1", dunning.signals.promised({ until: 1 }));
  // @ts-expect-error the result is a number
  processFixture(reminder).expectResult("done");
  const dating = processFixture(dated);
  dating.expectResult("late");
  // @ts-expect-error the result is "late" or "paid"
  dating.expectResult("early");
  // @ts-expect-error a Message name is not a Worker
  workflowStep.ask("remind");
};

// A Saga Reaction's `replyTo` names one of the Saga's own Saga Messages.
export const asking = (app: ReactionApp<MessagePayloads, RegisteredServices, "receipt">) => {
  app.queue(remind, { invoiceId: "i" }, { replyTo: "receipt" });
  // @ts-expect-error the Saga declares no such Saga Message
  app.queue(remind, { invoiceId: "i" }, { replyTo: "reciept" });
};
