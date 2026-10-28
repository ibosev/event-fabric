// An old-framework Aggregate in the billing Context, kept as it was written for the old stack.
const { only } = require('legacy-toolkit');

module.exports = {
  initialState: { status: 'none', level: 0 },
  commands: {
    open: [
      only.ifNotExists(),
      (cmd, aggregate) =>
        aggregate.apply.opened(
          { customer: cmd.payload.customer, amount: cmd.payload.amount },
          { tenant: { region: 'eu' }, source: 'dunning' },
        ),
    ],
    remind: [
      only.ifExists(),
      (cmd, aggregate) => {
        if (aggregate.get('_revision') > 10) throw new Error('Too many reminders');
        aggregate.apply('reminded', { level: aggregate.get('level') + 1 });
      },
    ],
    settle: [only.ifExists(), (cmd, aggregate) => aggregate.apply.settled({ customer: aggregate.get('customer') })],
    // An awaited read of another Aggregate: the old call resolved to its model.
    escalate: [
      only.ifExists(),
      async (cmd, aggregate, app) => {
        const lead = await app.getDomain().billing.dunning(cmd.payload.lead);
        aggregate.apply('reminded', { level: lead.get('level') + 1 });
      },
    ],
  },
  events: {
    opened: (event, state) =>
      state.set({ status: 'open', customer: event.payload.customer, amount: event.payload.amount, openedAt: event.metadata.timestamp }),
    reminded: (event, state) => state.set({ status: 'reminded', level: event.payload.level }),
    settled: (event, state) => state.set('status', 'settled'),
  },
  rules: [
    (state) => {
      if (state._revision > 20) throw new Error('Case history is too long');
    },
  ],
  // Old enrichers ran inside each apply: they set metadata in place and saw the State before the Event.
  eventEnricher: (event, aggregate) => {
    event.metadata.channel = 'letter';
    event.metadata.previousStatus = aggregate.get('status');
  },
};
