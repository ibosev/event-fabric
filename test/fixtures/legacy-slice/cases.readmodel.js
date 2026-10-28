// An old-framework Readmodel: one document per dunning case.
const { only } = require('legacy-toolkit');

module.exports = {
  initialState: { reminders: 0 },
  // A grouped key with its own Identity: a running total per customer, in the same collection.
  identity: {
    'billing.dunning.opened:customer': (event) => `customer-${event.payload.customer}`,
  },
  // Settling one case settles every open case of the same customer, in the old filter language.
  query: {
    'billing.dunning.settled': (event) => ({ customer: event.payload.customer, status: ['open', 'reminded'] }),
  },
  reactions: {
    'billing.dunning.opened': [
      only.ifNotExists(),
      // The old marker: internal transfers never open a case.
      { settings: { shouldHandleEvent: (event) => event.payload.customer !== 'internal' } },
      (event, vm) => vm.set({ customer: event.payload.customer, amount: event.payload.amount, status: 'open' }),
    ],
    'billing.dunning.opened:customer': (event, vm) => vm.set('amount', (vm.get('amount') || 0) + event.payload.amount),
    'billing.dunning.reminded:count': (event, vm) => vm.set('reminders', vm.get('reminders') + 1),
    'billing.dunning.reminded:status': (event, vm) => vm.set('status', 'reminded'),
    'billing.dunning.settled': (event, vm) => vm.set('status', 'settled'),
  },
};
