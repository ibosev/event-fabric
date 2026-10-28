// Old QueryBuilder reads, as a Gateway controller or Task would write them.
module.exports = {
  openCases: (app) =>
    app.getReadmodels().dunning.cases().find({ filter: { status: ['open', 'reminded'] }, sort: [{ amount: 'desc' }] }).size(0),
  caseById: async (app, id) => await app.getReadmodels().dunning.cases(id),
  // A Node object stream, read with on('data') or pipe.
  customerCases: (app, customer) => app.getReadmodels().dunning.cases().find({ filter: { customer } }).stream(),
};
