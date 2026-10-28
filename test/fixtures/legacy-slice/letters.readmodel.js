// An old-framework Readmodel: a letter exists only from the second reminder on.
const { only } = require('legacy-toolkit');

module.exports = {
  reactions: {
    'billing.dunning.reminded': [only.if((event) => event.payload.level >= 2), (event, vm) => vm.set('level', event.payload.level)],
  },
};
