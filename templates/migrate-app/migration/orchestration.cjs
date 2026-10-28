'use strict';
// The old `options` Aggregates, whose Events the old stack published without storing them, and the external
// scheduler; the machinery is EventFabric's legacyOrchestration. This file holds the Application's decisions about
// single Commands and Events.
const { legacyOrchestration, legacyScheduler: scheduler, legacyCommandOf: commandOf, LEGACY_SCHEDULER: SCHEDULER, LEGACY_ORCHESTRATION_MESSAGE: MESSAGE } = require('%%next%%/compat');
const effects = require('./effects.cjs');

// Command steps the Worker skips: old checks that read another Aggregate's State, which no Worker can load.
const skipped = {};
// Checks a Saga Message runs before its Reaction, by Event.
const checks = {};
// Commands whose Events reached no Saga.
const silent = {};
// The validators are provider calls: the services' own, else disabled.
const options = { publisherEffects: effects.options.publisherEffects, skipped, checks, silent };

const createOrchestration = () => legacyOrchestration(options);
const { withPublisher, publisher } = createOrchestration();
module.exports = { createOrchestration, scheduler, SCHEDULER, MESSAGE, withPublisher, publisher, commandOf, options };
