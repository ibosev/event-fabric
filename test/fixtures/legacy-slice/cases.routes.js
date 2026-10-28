// An old Gateway list route: clients page, sort and filter through the query string and read { total, data }.
const { command, readmodels } = require('legacy-toolkit');

module.exports = {
  routes: [
    {
      method: 'get',
      path: '/cases',
      // The route's query overrides the client's: this list is always sorted by amount, largest first.
      handler: readmodels.list((models) => models.dunning.cases, {
        query: { sort: '-amount' },
        transforms: [(doc) => ({ id: doc._id, customer: doc.customer, amount: doc.amount, status: doc.status })],
      }),
    },
    {
      method: 'post',
      path: '/cases/:id/remind',
      // An old REST command descriptor: it answered with the Readmodel document once the Event reached it.
      handler: command((domain) => ({ command: domain.billing.dunning.remind, event: 'readmodel.dunning.cases.reminded' }), {
        id: (req) => req.params.id,
        payload: (req) => req.body,
      }),
    },
  ],
};
