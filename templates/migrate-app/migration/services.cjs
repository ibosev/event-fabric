'use strict';
const fs = require('node:fs');
const path = require('node:path');
const local = require('./local-config.cjs');
require('module-alias').addAliases(%%aliases%%);
const DistributedIdGenerator = require('distributed-id-generator');
const identifier = new DistributedIdGenerator().loadDefinitions(path.join(local.root, 'app/ids'));
const blocked = () => { throw new Error('Provider and scheduled work are disabled in the local migration'); };
const { rebuildMemo } = require('%%next%%');
// Reactions read CRUD models: inherited behavior kept as is.
const crud = {};
// Readmodels get the same models, but a rebuild keeps each read for the rest of that rebuild (rebuildMemo).
const readmodelCrud = {};
const readmodelServices = { idGenerator: () => identifier, crud: readmodelCrud };
// A write through a model starts new keys for its reads, so no later read in this process sees an earlier one.
const versions = new Map();
const writes = /^(create|insert|update|replace|delete|remove|findOneAnd|findByIdAnd|bulkWrite|save)/;
const kept = (key, model) => new Proxy(model, {
  get(target, property) {
    const value = target[property];
    if (typeof value !== 'function') return value;
    if (['findById', 'findOne', 'find'].includes(property))
      return (...args) => args.length === 1
        ? rebuildMemo([key, versions.get(key) ?? 0, property, args[0]], () => value.apply(target, args))
        : value.apply(target, args);
    if (writes.test(property)) return (...args) => { versions.set(key, (versions.get(key) ?? 0) + 1); return value.apply(target, args); };
    return value.bind(target);
  },
});
// `store` gives Workers and Gateways the Stream reader a Task has (`events.readStream`), where old code queried the
// old Event store.
async function setupServices({ resources, store }) {
  const mongoose = require('mongoose');
  for (const group of Object.keys(crud)) delete crud[group];
  for (const group of Object.keys(readmodelCrud)) delete readmodelCrud[group];
  // app/services/config.js: get(...sections) returned null for a missing section.
  const settings = { get: local.setting };
  const services = { ...readmodelServices, crud, config: () => settings,
    ...(store ? { events: { readStream: store.readStream.bind(store) } } : {}) };
  const api = {
    getServices: () => services,
    getCrud: () => crud,
    getDomain: blocked,
    getWorkers: blocked,
    scheduleTask: blocked,
    errors: require('%%framework%%').tools.errors,
  };
  const groups = fs.readdirSync(path.join(local.root, 'app/crud'), { withFileTypes: true }).filter(entry => entry.isDirectory()).map(entry => entry.name).sort();
  for (const group of groups) {
    const connection = mongoose.createConnection(local.mongoUrl, { dbName: local.crudDatabase(group), useNewUrlParser: true, useUnifiedTopology: true, autoIndex: false, autoCreate: false });
    resources.defer(() => connection.close());
    await connection;
    crud[group] = {};
    readmodelCrud[group] = {};
    for (const entry of fs.readdirSync(path.join(local.root, 'app/crud', group), { withFileTypes: true }).filter(entry => entry.isFile() && entry.name.endsWith('.js')).sort((a, b) => a.name.localeCompare(b.name))) {
      const name = entry.name.split('.')[0];
      const factory = require(path.join(local.root, 'app/crud', group, entry.name));
      const model = factory({ mongoose: connection, Schema: mongoose.Schema, api });
      crud[group][name] = () => model;
      const readmodelModel = kept(`${group}/${name}`, model);
      readmodelCrud[group][name] = () => readmodelModel;
    }
  }
  return services;
}
// The old `app.getCrud()` on Command steps and Reactions, so the source keeps it.
const legacyApp = app => ({ getCrud: () => app.getServices().crud });
module.exports = { setupServices, readmodelServices, kept, legacyApp };
