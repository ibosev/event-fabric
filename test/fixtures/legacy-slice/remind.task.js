// An old Task: it received (task, app) and reached everything through app.
module.exports = async (task, app) => {
  const open = await app.getReadmodels().dunning.cases().find({ filter: { status: 'open' } }).size(0);
  for (const doc of open)
    await app.sendCommand({ stream: { context: 'billing', aggregate: 'dunning', id: doc._id }, name: 'remind', payload: {} });
  // A data field that only looks like an old route option: it stays as written.
  return { event: 'readmodel.dunning.cases.reminded', count: open.length };
};
