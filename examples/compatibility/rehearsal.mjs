// Entirely synthetic data. Run from the repository root after `npm run build`.
import assert from "node:assert/strict";
import { isDeepStrictEqual } from "node:util";
import {
  compile,
  createApplication,
  createCommand,
  createKernel,
  DateTime,
  only,
  Rejection,
  Type,
} from "../../dist/index.js";
import {
  createInMemoryEventStore,
  createInMemoryTransport,
  createMemoryReadmodelStore,
} from "../../dist/testing.js";
import {
  auditReplay,
  migrateHistoricalEvent,
} from "../../dist/migration/audit.js";

const timestamp = "2025-01-01T00:00:00.000Z";
const stream = (id) => ({ context: "billing", aggregate: "invoice", id });
const source = [
  {
    eventId: "event-a-issued",
    stream: stream("invoice-a"),
    name: "domain.billing.invoice.issued",
    revision: 0,
    commandId: "issue-a",
    timestamp,
    metadata: { tenant: "tenant-demo" },
    payload: { amount: { EUR: 100, USD: 108 } },
  },
  {
    eventId: "event-a-paid",
    stream: stream("invoice-a"),
    name: "domain.billing.invoice.paymentRecorded",
    revision: 1,
    commandId: "pay-a",
    timestamp,
    metadata: { tenant: "tenant-demo" },
    payload: { amount: { EUR: 25, USD: 27 } },
  },
  {
    eventId: "event-b-issued",
    stream: stream("invoice-b"),
    name: "domain.billing.invoice.issued",
    revision: 0,
    commandId: "issue-b",
    timestamp,
    metadata: { tenant: "tenant-demo" },
    payload: { amount: { EUR: 40, USD: 44 } },
  },
];
const before = structuredClone(source);
const history = source.map((event, index) => {
  const mapped = migrateHistoricalEvent(event, {
    revisionBase: 0,
    sequence: index,
  });
  assert.deepEqual(mapped.reviews, []);
  return mapped.event;
});
assert.deepEqual(source, before, "Mapping must not mutate source input");
assert.deepEqual(
  history.map((event) => event.revision),
  [1, 2, 1],
);
assert.deepEqual(
  history.map((event) => event.id),
  source.map((event) => event.eventId),
);

const subtract = (a, b) => ({ EUR: a.EUR - b.EUR, USD: a.USD - b.USD });
const invoice = compile("billing", "invoice", {
  initialState: { balance: { EUR: 0, USD: 0 } },
  commands: {
    issue: [
      only.ifNotExists(),
      (command, aggregate) => aggregate.apply.issued(command.payload),
    ],
    pay: [
      only.ifExists(),
      (command, aggregate) => {
        const { amount } = command.payload,
          balance = aggregate.get("balance");
        if (
          amount.EUR < 0 ||
          amount.USD < 0 ||
          amount.EUR > balance.EUR ||
          amount.USD > balance.USD
        )
          throw new Rejection(
            "invalid-payment",
            "Payment exceeds the balance or is negative",
          );
        if (amount.EUR === 0 && amount.USD === 0) return;
        aggregate.apply.paymentRecorded(command.payload);
      },
    ],
  },
  events: {
    issued: (event, state) => state.set("balance", event.payload.amount),
    paymentRecorded: (event, state) =>
      state.set(
        "balance",
        subtract(state.get("balance"), event.payload.amount),
      ),
  },
});

const Money = Type.Object({ EUR: Type.Number(), USD: Type.Number() });
const Invoices = {
  schema: Type.Object({
    tenant: Type.String(),
    balance: Money,
    status: Type.String(),
    createdAt: DateTime(),
    note: Type.Optional(Type.String()),
    payments: Type.Array(
      Type.Object({ eventId: Type.String(), amount: Money }),
    ),
  }),
  indexes: [{ fields: ["tenant", "status"] }],
  reactions: {
    "billing.invoice.issued": (event, view) =>
      view.set({
        tenant: event.metadata.tenant,
        balance: event.payload.amount,
        status: "open",
        createdAt: new Date(event.timestamp),
        payments: [],
      }),
    "billing.invoice.paymentRecorded": [
      only.ifExists(),
      (event, view) =>
        view.set({
          balance: subtract(view.get("balance"), event.payload.amount),
          status: "partial",
          payments: [
            ...view.get("payments"),
            { eventId: event.id, amount: event.payload.amount },
          ],
        }),
    ],
  },
};

// Hand-worked expectations are independent of the new handlers.
const expectedStates = [
  { id: "invoice-a", balance: { EUR: 75, USD: 81 } },
  { id: "invoice-b", balance: { EUR: 40, USD: 44 } },
];
const expectedDocuments = [
  {
    _id: "invoice-a",
    tenant: "tenant-demo",
    balance: { EUR: 75, USD: 81 },
    status: "partial",
    createdAt: timestamp,
    note: null,
    payments: [{ eventId: "event-a-paid", amount: { EUR: 25, USD: 27 } }],
  },
  {
    _id: "invoice-b",
    tenant: "tenant-demo",
    balance: { EUR: 40, USD: 44 },
    status: "open",
    createdAt: timestamp,
    note: null,
    payments: [],
  },
];
const makeConfig = () => ({
  name: "synthetic-compatibility",
  aggregates: [invoice],
  store: createInMemoryEventStore(),
  transport: createInMemoryTransport(),
  readmodels: {
    api: {
      store: createMemoryReadmodelStore(),
      models: { invoices: Invoices },
    },
  },
});
const acceptedCases = history.map((event) => ({
  command: createCommand({
    id: event.commandId,
    stream: event.stream,
    name: event.name === "issued" ? "issue" : "pay",
    payload: event.payload,
    metadata: event.metadata,
    timestamp,
  }),
  history: history.filter(
    (previous) =>
      previous.stream.id === event.stream.id &&
      previous.revision < event.revision,
  ),
  expectedEvents: [event],
}));
const streamCases = expectedStates.map((state) => ({
  stream: stream(state.id),
  expectedState: state,
  events: history.filter((event) => event.stream.id === state.id),
}));
const readmodelCases = [
  { group: "api", model: "invoices", documents: expectedDocuments },
];

// Explicit expected ids prevent a smaller successful sample from looking complete.
const manifest = {
  provenance: "synthetic",
  streams: ["billing.invoice/invoice-a", "billing.invoice/invoice-b"],
  events: ["event-a-issued", "event-a-paid", "event-b-issued"],
  commands: ["issue-a", "pay-a", "issue-b"],
  readmodels: { "api.invoices": ["invoice-a", "invoice-b"] },
  extraCases: [
    "rejected-payment",
    "accepted-zero-event",
    "duplicate-command",
    "query-contract",
    "saved-document-drift",
  ],
};
const sameIds = (actual, expected) => {
  assert.equal(new Set(actual).size, actual.length, "Duplicate coverage id");
  assert.deepEqual(
    [...actual].sort(),
    [...expected].sort(),
    "Missing or unexpected coverage id",
  );
};
sameIds(
  history.map((event) => event.id),
  manifest.events,
);
sameIds(
  streamCases.map(
    (fixture) =>
      `${fixture.stream.context}.${fixture.stream.aggregate}/${fixture.stream.id}`,
  ),
  manifest.streams,
);
sameIds(
  acceptedCases.map((fixture) => fixture.command.id),
  manifest.commands,
);
sameIds(
  readmodelCases.map((fixture) => `${fixture.group}.${fixture.model}`),
  Object.keys(manifest.readmodels),
);
sameIds(
  expectedDocuments.map((document) => document._id),
  manifest.readmodels["api.invoices"],
);

const replay = await auditReplay({
  definitions: [invoice],
  streams: streamCases,
  commands: acceptedCases,
  readmodels: { config: makeConfig(), history, expected: readmodelCases },
});
assert.equal(replay.passed, true);
assert.deepEqual(replay.checked, { streams: 2, commands: 3, readmodels: 1 });
assert.deepEqual(replay.skipped, []);
assert.deepEqual(replay.differences, []);

// Demonstrate why auditReplay.passed alone is insufficient as a readiness gate.
const partial = await auditReplay({
  definitions: [invoice],
  streams: streamCases.slice(0, 1),
  commands: acceptedCases.map((fixture) => ({
    ...fixture,
    skip: "Synthetic omitted-case demonstration",
  })),
});
assert.equal(partial.passed, true);
assert.deepEqual(partial.checked, { streams: 1, commands: 0, readmodels: 0 });
assert.equal(partial.skipped.length, 3);
assert.throws(() =>
  sameIds(
    streamCases
      .slice(0, 1)
      .map(
        (fixture) =>
          `${fixture.stream.context}.${fixture.stream.aggregate}/${fixture.stream.id}`,
      ),
    manifest.streams,
  ),
);

const checked = [];
const store = createInMemoryEventStore();
try {
  const imported = await store.import(history);
  const log = await store.readAll({ limit: 10 });
  assert.deepEqual(
    log,
    imported,
    "Every imported envelope/payload must survive reading",
  );
  // Source Positions are replaced; undefined envelope fields have JSON absence semantics.
  const envelope = (events) =>
    JSON.parse(JSON.stringify(events.map(({ position, ...event }) => event)));
  assert.deepEqual(envelope(log), envelope(history));
  const kernel = createKernel({
    definitions: [invoice],
    store,
    clock: () => new Date(timestamp),
  });
  const pay = (id, amount) =>
    createCommand({
      id,
      stream: stream("invoice-a"),
      name: "pay",
      payload: { amount },
      timestamp,
    });
  const head = await store.head();
  const rejected = await kernel.handle(pay("reject", { EUR: 76, USD: 82 }));
  assert.equal(rejected.status, "rejected");
  assert.equal(rejected.error.code, "invalid-payment");
  assert.deepEqual(await store.head(), head);
  checked.push("rejected-payment");
  const zero = await kernel.handle(pay("zero", { EUR: 0, USD: 0 }));
  assert.equal(zero.status, "accepted");
  assert.deepEqual(zero.events, []);
  assert.deepEqual(await store.head(), head);
  checked.push("accepted-zero-event");
  const next = pay("new-payment", { EUR: 5, USD: 5 });
  const first = await kernel.handle(next),
    duplicate = await kernel.handle(next);
  assert.equal(first.status, "accepted");
  assert.equal(duplicate.status, "accepted");
  assert.equal(duplicate.duplicate, true);
  assert.deepEqual(duplicate.events, first.events);
  assert.equal((await store.readStream(stream("invoice-a"))).length, 3);
  checked.push("duplicate-command");
} finally {
  await store.close();
}

const config = makeConfig(),
  app = createApplication(config);
let savedDifferences;
try {
  await config.store.import(history);
  await app.rebuild("api");
  const invoices = app.getReadmodels().repository("api", "invoices");
  const documents = await invoices.find({}, { sort: { _id: 1 } });
  assert.deepEqual(JSON.parse(JSON.stringify(documents)), expectedDocuments);
  assert.equal(await invoices.count({ "balance.EUR": { gt: 50 } }), 1);
  assert.equal(
    (await invoices.find({ "payments.eventId": "event-a-paid" }))[0]._id,
    "invoice-a",
  );
  assert.equal(await invoices.count({ tenant: "another-tenant" }), 0);
  assert.deepEqual(
    await app.config.readmodels.api.store.checkpoint("api"),
    await config.store.head(),
  );
  assert.deepEqual(await config.store.pendingOutbox(10), []);
  checked.push("query-contract");

  // A fabricated saved projection disagrees with independently expected replay.
  // Real migrations need two independent inputs and reviewed normalizations.
  const saved = structuredClone(expectedDocuments);
  saved[0].status = "open";
  const rebuilt = new Map(
    JSON.parse(JSON.stringify(documents)).map(document => [document._id, document]),
  );
  savedDifferences = saved.filter(
    document => !isDeepStrictEqual(document, rebuilt.get(document._id)),
  );
  assert.deepEqual(
    savedDifferences.map((document) => document._id),
    ["invoice-a"],
  );
  checked.push("saved-document-drift");
} finally {
  await app.close();
}
sameIds(checked, manifest.extraCases);

console.log(
  JSON.stringify(
    {
      syntheticFixturePassed: true,
      applicationReady: false,
      provenance: manifest.provenance,
      checked: replay.checked,
      extraCases: checked,
      savedDocumentsWithDrift: savedDifferences.length,
      partialAuditPassed: partial.passed,
      notCovered: [
        "Application inventory",
        "real storage adapters and restart",
        "Saga continuation",
        "Worker effect intent",
        "HTTP/SSE clients",
        "service dependency inventory",
        "deployment latency",
        "operator logging requirements",
      ],
    },
    null,
    2,
  ),
);
