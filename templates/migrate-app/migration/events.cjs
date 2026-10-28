'use strict';
const path = require('node:path');
const { createRequire } = require('node:module');
const { ObjectId } = createRequire(require.resolve('%%next%%'))('mongodb');
const { migrateHistoricalEvent } = require(path.join(path.dirname(require.resolve('%%next%%')), 'migration/audit.js'));
// Reviewed paths where old Events hold a BSON identifier, stored as its hex string.
const identifierPaths = new Set([]);
function jsonIdentifiers(value, location = '') {
  if (value instanceof ObjectId) {
    if (!identifierPaths.has(location)) throw new Error('Unreviewed BSON identifier path');
    return value.toHexString();
  }
  if (Array.isArray(value)) return value.map(entry => jsonIdentifiers(entry, location + '[]'));
  if (value && Object.getPrototypeOf(value) === Object.prototype)
    return Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, jsonIdentifiers(entry, location ? location + '.' + key : key)]));
  return value;
}
// An old Event store row as an EventFabric Event.
function convert(row) {
  const source = row.payload;
  if (source.context !== row.context || source.aggregate.name !== row.aggregate || source.aggregate.id !== row.aggregateId) throw new Error('Storage and domain Stream identity differ');
  const payload = jsonIdentifiers(source.payload);
  const metadata = { ...source.metadata };
  return migrateHistoricalEvent({
    eventId: source.id,
    stream: { context: row.context, aggregate: row.aggregate, id: row.aggregateId },
    name: source.name,
    revision: row.streamRevision,
    payload,
    metadata,
    timestamp: source.metadata?.timestamp ?? row.commitStamp,
    commandId: source.__command?.id,
    causationId: source.metadata?.causationId,
    correlationId: source.metadata?.correlationId,
  }, { revisionBase: 0, dates: 'iso' });
}
module.exports = { convert };
