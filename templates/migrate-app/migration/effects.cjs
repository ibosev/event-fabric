'use strict';
// Saga Reactions with external effects run in Workers; the machinery is EventFabric's legacyEffects. This file holds
// the Application's decisions: which Reactions move, and which provider calls a Saga may still make.
const { legacyEffects, LEGACY_EFFECTS_STATE: STATE } = require('%%next%%/compat');

// By Saga, the Events (or Saga Messages, for the orchestration wake-ups) whose Reaction runs in the Worker.
const inWorker = {};
// Provider services, by name, with the calls a Saga may still make: reads that change nothing.
const providers = {};
// The external publisher's calls that change something; scheduleTask goes to the scheduler Saga.
const publisherEffects = [];
const options = { reactions: inWorker, providers, publisherEffects };

const createEffects = (table = inWorker) => legacyEffects({ ...options, reactions: table });
const { sagaServices, sagaPublisher, messageOf } = createEffects();
const runsInWorker = (saga, event, table = inWorker) => createEffects(table).runsInWorker(saga, event);
module.exports = { createEffects, sagaServices, sagaPublisher, runsInWorker, inWorker, messageOf, STATE, options };
