// An old-framework Saga: without an Identity, the old store started a new instance for every Event,
// and the old Saga builder ignored initialState, so every instance started empty.
module.exports = {
  initialState: { letters: 10 },
  reactions: {
    'billing.dunning.reminded': (event, saga, app) => {
      saga.set('letters', (saga.get('letters') || 0) + 1);
      app.queue('letter', { saga: saga.id, letters: saga.get('letters'), context: event.context });
    },
  },
};
