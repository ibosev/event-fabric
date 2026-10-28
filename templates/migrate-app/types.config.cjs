'use strict';
const { aggregates, readmodels } = require('./migration/declarations.cjs');
exports.config = { aggregates, readmodels, typesMode: 'names', typesModule: '%%next%%', typesPath: 'migration/generated.d.ts' };
