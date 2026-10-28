'use strict';
// The configuration rebuild worker threads load. A rebuild runs only Readmodel Reactions, so it supplies
// setupServices, which the main configuration refuses until runtime roles are enabled.
const { setupServices } = require('./services.cjs');
exports.config = async () => ({ ...(await require('../event-fabric.config.js').config()), setupServices });
