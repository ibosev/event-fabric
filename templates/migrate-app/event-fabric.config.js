'use strict';
const { createPostgresEventStore, createLogger, createValidator } = require('%%next%%');
const path = require('node:path');
const { createMongoReadmodelStore } = require('%%next%%/mongo');
const { createRabbitMQTransport } = require('%%next%%/rabbitmq');
const { createRequire } = require('node:module');
const { MongoClient } = createRequire(require.resolve('%%next%%'))('mongodb');
const local = require('./migration/local-config.cjs');
const declarations = require('./migration/declarations.cjs');
const sourceMigrationReady = false;
const { readmodelServices, legacyApp } = require('./migration/services.cjs');
const { definitions, gatewayServices, systemNotifications } = require('./migration/gateways.cjs');
exports.config = async () => {
  // Every Readmodel group and Saga reads the log and renews its Lease through this pool (pg's default is 10).
  const store = createPostgresEventStore({ connectionString: local.databaseUrl, schema: local.eventSchema, poolOptions: { max: Number(process.env.PG_POOL_MAX ?? 10) } });
  const mongo = new MongoClient(local.mongoUrl);
  // The running Application, for what the old services do outside a request: a plugin receives it at start.
  const started = Promise.withResolvers();
  let running;
  started.promise.then(context => { running = context; });
  try {
    const transport = await createRabbitMQTransport({ url: local.amqpUrl, application: '%%name%%' });
    return {
      name: '%%name%%',
      logger: createLogger({ level: 'silent' }),
      aggregates: declarations.aggregates,
      // Command schemas refer to the Application's named schemas and formats.
      validate: createValidator({ schemasDir: path.join(local.root, 'app/validation/schemas'), formatsDir: path.join(local.root, 'app/validation/formats') }),
      readmodels: declarations.readmodels,
      // The Application's Sagas, the scheduler Saga and the orchestration Worker.
      sagas: { ...declarations.sagas, ...declarations.orchestrationSagas },
      workers: declarations.workers,
      store,
      readmodelStore: Object.assign(createMongoReadmodelStore({ client: mongo, database: local.readmodelDatabase, databases: local.readmodelDatabases }), { close: () => mongo.close() }),
      transport,
      readmodelServices,
      legacyApp,
      // The old Gateways on their old ports.
      gateways: definitions(),
      plugins: [context => started.resolve(context)],
      setupServices: async ({ resources }) => {
        // Source reviews and closed Readmodel schemas must be resolved before enabling runtime roles.
        if (!sourceMigrationReady) throw new Error('Migration runtime is disabled pending source reviews and Readmodel schemas; use the composition check');
        // The old services' `connections.get('mongodb')`, on the Application's own driver.
        const client = await require('mongodb').MongoClient.connect(local.mongoUrl);
        resources.defer(() => client.close());
        return gatewayServices({
          mongo: client, resources, store,
          // No live subscriber is connected before the Application runs.
          notify: message => running && systemNotifications(() => running)(message),
          // Old services schedule some tasks while they are built: those wait for the scheduler Saga to run.
          // shortcut: a Task run starts no plugin, so a task scheduled there is never sent; run it under `event-fabric run` if one must be.
          schedule: (...args) => started.promise.then(context => context.sendSagaMessage(...args)),
        });
      },
      typesMode: 'names', typesModule: '%%next%%',
      typesPath: 'migration/generated.d.ts',
    };
  } catch (error) {
    await Promise.allSettled([store.close(), mongo.close()]);
    throw error;
  }
};
// What an old Task's Application had beyond getDomain/getReadmodels/getServices/getLogger: the old config
// (`app.wire.app.config`, with local addresses) and CRUD. The old Event store query (`getEventstore()`) has no stand-in.
exports.legacyTaskApp = ctx => ({ wire: { app: { config: local.setting() } }, getCrud: () => ctx.services.crud });
