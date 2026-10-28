'use strict';
const fs = require('node:fs');
const path = require('node:path');
const JSON5 = require('json5');
const root = path.resolve(__dirname, '..');
const load = name => JSON5.parse(fs.readFileSync(path.join(root, 'config', name + '.json5'), 'utf8'));
// Top-level replacement matches the installed loader; only inspected settings leave this module.
const source = { ...load('config') };
// Groups the old configuration put in one database (`denormalizers.<group>.dbName`), by that database's name.
const sharedDatabases = () => {
  const names = Object.entries(source.denormalizers || {}).map(([group, settings]) => [group, settings.dbName.replace(/^\{\{appName\}\}-/, '').replace(/-readmodels$/, '')]);
  return Object.fromEntries(names.filter(([, name]) => names.filter(([, other]) => other === name).length > 1));
};
module.exports = {
  root,
  databaseUrl: process.env.DATABASE_URL,
  mongoUrl: process.env.MONGO_URL,
  amqpUrl: process.env.AMQP_URL,
  eventSchema: process.env.EVENT_SCHEMA || '%%database%%_events',
  readmodelDatabase: process.env.READMODEL_DATABASE || '%%database%%',
  // Command handlers write CRUD: each group's models in a database of their own.
  crudDatabase: group => `${process.env.CRUD_DATABASE || '%%database%%'}-${group}`,
  readmodelGroups: Object.keys(source.denormalizers || {}),
  // The old stack kept several Readmodel groups in one database, and route code joins their collections by name
  // (`$lookup`): EventFabric's Mongo store keeps the same groups together.
  readmodelDatabases: sharedDatabases(),
  sagaGroups: Object.keys(source.sagas || {}),
  // The Readmodel group whose denormalized Events each Saga group received.
  sagaBindings: Object.fromEntries(Object.entries(source.sagas || {}).filter(([group]) => !group.startsWith('$')).map(([group, settings]) => [group, settings.readmodels])),
  // The Application's config service read: sections of the merged config.
  setting: (...sections) => sections.reduce((section, key) => section?.[key] === undefined ? null : section[key], source),
};
