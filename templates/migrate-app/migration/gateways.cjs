'use strict';
// The old Gateways on EventFabric: each folder served unchanged by legacyGateway, with the Application's services
// built from app/services by legacyServices. Nothing leaves through the old buses here: external messages (scheduler
// tasks without `schedule`) are recorded in `published`, never sent, and the command bus is blocked; system
// Notifications go to `notify` (`systemNotifications`).
const fs = require('node:fs');
const path = require('node:path');
const { legacyGateway, legacyServices } = require('%%next%%/compat');
const local = require('./local-config.cjs');
const { aggregates, authoredReadmodels } = require('./declarations.cjs');
const { setupServices } = require('./services.cjs');
const { publisher } = require('./orchestration.cjs');
const errors = require('%%framework%%').tools.errors;
const ValidationService = require('%%framework%%/lib/ValidationService');

const gateways = %%gateways%%;
const blocked = name => () => { throw new Error(`${name} is disabled in the local migration`); };
// The old toolkit serialized responses with the Application's own schemas (app/validation).
const validation = new ValidationService({ validation: ValidationService.loader(path.join(local.root, 'app/validation')) });
// EventFabric reads an absent optional field as null; the old documents lacked it. The Application's serializer
// (fast-json-stringify) throws on null for a field typed object or array without null, so an old document never
// answered null there: such nulls are dropped before serializing. Scalar nulls serialize as the old ones did.
// JSON-stored fields hold the imported Events' ISO strings where the old documents held BSON Dates, which a
// `format: date` field printed as YYYY-MM-DD: such strings (Date#toISOString's form only) become Dates again.
const isoDate = /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/;
const schemas = validation.schemaStrigifier._schemas;
const resolve = (schema, root) => {
  for (let depth = 0; schema?.$ref && depth < 20; depth++) {
    const [id, pointer = ''] = schema.$ref.split('#');
    if (id) root = schemas[id];
    schema = pointer.split('/').filter(Boolean).reduce((at, part) => at?.[decodeURIComponent(part)], root);
  }
  return [schema, root];
};
const unserializableNull = schema => {
  const types = [schema?.type ?? (schema?.properties ? 'object' : schema?.items ? 'array' : undefined)].flat();
  return !schema?.nullable && !types.includes('null') && (types.includes('object') || types.includes('array'));
};
const prune = (value, schema, root) => {
  [schema, root] = resolve(schema, root);
  if (!schema || schema.anyOf || schema.oneOf) return value;
  for (const part of schema.allOf ?? []) value = prune(value, part, root);
  if (Array.isArray(value)) return schema.items ? value.map(item => prune(item, schema.items, root)) : value;
  if (schema.format === 'date' && typeof value === 'string' && isoDate.test(value)) return new Date(value);
  if (!value || typeof value !== 'object' || value instanceof Date) return value;
  const out = { ...value };
  for (const [key, child] of Object.entries(out)) {
    const [field] = resolve(schema.properties?.[key] ?? (typeof schema.additionalProperties === 'object' ? schema.additionalProperties : undefined), root);
    if (!field) continue;
    if (child === null && unserializableNull(field)) delete out[key];
    else out[key] = prune(child, field, root);
  }
  return out;
};
const serializer = schema => {
  const stringify = validation.getStrigifierFunction(schema);
  const [resolved, root] = typeof schema === 'string' ? [schemas[schema], schemas[schema]] : [schema, schema];
  return value => stringify(prune(value, resolved, root));
};
const readmodelSchemas = Object.fromEntries(Object.entries(authoredReadmodels).flatMap(([group, { models }]) =>
  Object.entries(models).filter(([, model]) => model.schema).map(([name, model]) => [`${group}.${name}`, model.schema])));

// The old description route's content (`/%%framework%%/app.json` on SPAs): the frontend client builds its Command and
// Readmodel proxies from its keys. Built from the EventFabric declarations; the old file paths and CRUD connection
// settings are left out.
const description = {
  readmodels: Object.fromEntries(Object.entries(authoredReadmodels).map(([group, { models }]) =>
    [group, Object.fromEntries(Object.entries(models).map(([name, model]) => [name, model.schema === undefined ? {} : { schema: model.schema }]))])),
  domain: aggregates.reduce((domain, { context, name, commands, events }) => {
    (domain[context] ??= {})[name] = {
      commands: Object.fromEntries(Object.entries(commands).map(([command, { schema }]) => [command, schema === undefined ? {} : { schema }])),
      events: Object.fromEntries(Object.keys(events).map(event => [event, {}])),
    };
    return domain;
  }, {}),
  crud: Object.fromEntries(fs.readdirSync(path.join(local.root, 'app/crud'), { withFileTypes: true }).filter(entry => entry.isDirectory()).map(({ name: group }) =>
    [group, { collections: Object.fromEntries(fs.readdirSync(path.join(local.root, 'app/crud', group)).filter(file => file.endsWith('.js')).map(file => [file.split('.')[0], {}])) }])),
};
const commandSchemas = Object.fromEntries(Object.entries(description.domain).flatMap(([context, aggregates]) =>
  Object.entries(aggregates).flatMap(([aggregate, { commands }]) =>
    Object.entries(commands).filter(([, { schema }]) => schema !== undefined).map(([name, { schema }]) => [`${context}.${aggregate}.${name}`, schema]))));
// The Application's own Gateway plugins, run through the old hook names. A Gateway folder's config file overrides its
// config section, as the old loader merged them.
const gatewayPlugins = [];
const plugins = (kind, name) => {
  const file = path.join(local.root, 'app/gateways', kind, name, 'config.yaml');
  const gateway = { name: `${kind}:${name}`, config: { ...local.setting(kind, name), ...(fs.existsSync(file) && require('js-yaml').load(fs.readFileSync(file, 'utf8'))) } };
  const hooks = {};
  const host = { addHook: (hook, fn) => (hooks[hook] ??= []).push(fn), validationService: validation };
  for (const plugin of gatewayPlugins) require(path.join(local.root, plugin))(host);
  const run = (hook, ...args) => Promise.all((hooks[hook] ?? []).map(fn => fn.call(gateway, ...args)));
  run('rest:beforeRoutes');
  return {
    onRoute: route => run('onRoute', route),
    afterRoutes: async router => {
      await run('rest:afterRoutes', { router, %%framework%%: { ...description, validationService: validation } });
      if (kind === 'spa') router.get('/%%framework%%/app.json', (_req, res) => res.json(description));
    },
  };
};

// What the old services read from the old runtime object.
class Message {
  constructor(fields) { Object.assign(this, fields); }
}
Message.EVENT_TYPES = { SYSTEM: 'system', DOMAIN: 'domain', READMODEL: 'readmodel' };
const published = [];
const bus = (name, publish) => ({ out: () => {}, in: () => {}, outgoing: { write: publish ?? blocked(name) }, incoming: { on: () => {} } });
const wire = { amqp: { Sender: class {}, Receiver: class {} } };

/**
 * The Application services the Gateways use, built the old way, with the CRUD models of `services.cjs` as
 * `crud`. `mongo` is a client of the Application's own Mongo driver; `notify(event)` receives the system
 * Notifications services publish.
 */
async function gatewayServices({ mongo, notify = () => {}, resources, schedule }) {
  const { crud } = await setupServices({ resources });
  let built;
  const app = {
    name: '%%name%%',
    config: local.setting(),
    errors,
    Event: Message,
    Command: Message,
    connections: { get: async name => { if (name === 'mongodb') return mongo; throw new Error(`No ${name} connection in the local migration`); } },
    eventbus: bus('eventbus', notify),
    commandbus: bus('commandbus'),
    // Services that schedule tasks on start: with `schedule(saga, name, payload)` (the Application's sendSagaMessage)
    // they go to the scheduler Saga.
    externalbus: bus('externalbus', message => {
      if (schedule && message?.name === 'scheduleTask') return publisher(schedule).scheduleTask(message.payload.payload);
      published.push(message);
    }),
    wires: { eventbus: wire, commandbus: wire, externalbus: wire },
    getServices: () => built.services,
  };
  built = legacyServices(path.join(local.root, 'app/services'), app, { schemaValidator: validation });
  resources?.defer(() => built.close());
  // `getServices().crud.<group>.<model>()`: an object, not a service accessor.
  return Object.assign(built.services, { crud });
}

/**
 * The old services' system messages as Application Notifications, which every
 * replica's live subscribers receive. `application()` is the Application once it exists.
 */
const systemNotifications = application => message => {
  if (message?.type === 'system') return application().notify(message.name, message.payload ?? null);
};
// Who receives which live items over the old socket, by Gateway; the old socket sent every Event to every client.
const live = {};
// Commands a route sent that no longer run as Commands, answered by the Gateway instead.
const replacedCommands = {};

// The old auth code records the Gateway as its channel: `<kind>:<name>`.
const definitions = ({ host = '0.0.0.0', ports = {} } = {}) => Object.fromEntries(gateways.map(([kind, name]) => [`${kind}:${name}`, legacyGateway({
  directory: path.join(local.root, 'app/gateways', kind, name),
  port: ports[`${kind}:${name}`] ?? local.setting(kind, name, 'port'),
  host,
  metadata: req => req.%%framework%%Metadata,
  requestFields: { %%framework%%App: `${kind}:${name}` },
  errors,
  serializer,
  readmodelSchemas,
  commandSchemas,
  replacedCommands,
  requestApp: app => ({ getCrud: () => app.getServices().crud }),
  live: live[`${kind}:${name}`],
  ...plugins(kind, name),
})]));

module.exports = { gateways, gatewayServices, definitions, validation, published, description, systemNotifications };
