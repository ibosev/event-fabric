'use strict';
// The Application's folders as EventFabric declarations; the convention-driven loading is EventFabric's
// legacyApplication. This file holds the Application's decisions that no convention supplies.
const path = require('node:path');
const { legacyApplication } = require('%%next%%/compat');
const local = require('./local-config.cjs');
require('module-alias').addAliases(%%aliases%%);
// Reviewed closed schemas, drafted from the old documents with draftReadmodelSchema.
const readmodelSchemas = require('./readmodel-schemas.json');
const orchestration = require('./orchestration.cjs');
const effects = require('./effects.cjs');

const declarations = legacyApplication({
  root: local.root,
  readmodelGroups: local.readmodelGroups,
  sagaGroups: local.sagaGroups,
  sagaBindings: local.sagaBindings,
  readmodelStorage: (group, name) => ({ schema: readmodelSchemas[group][name], indexes: [] }),
  orchestration: orchestration.options,
  effects: effects.options,
});
module.exports = declarations;
