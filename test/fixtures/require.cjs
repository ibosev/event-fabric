const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const manifest = JSON.parse(fs.readFileSync(path.join(__dirname, '../../package.json'), 'utf8'));
for (const target of Object.values(manifest.exports)) {
  const entry = require(path.join(__dirname, '../..', target.default));
  assert.equal(Object.hasOwn(entry, 'default'), false);
}
const { createApplication } = require(path.join(__dirname, '../../dist/index.js'));
const { createInMemoryEventStore, createInMemoryTransport } = require(path.join(__dirname, '../../dist/testing.js'));
// Managed services on the native resource stack, without top-level await.
(async () => {
  const log = [];
  const application = createApplication({
    name: 'commonjs',
    store: createInMemoryEventStore(),
    transport: createInMemoryTransport(),
    async setupServices({ resources }) {
      resources.defer(() => log.push('cleanup'));
      return { ready: true };
    },
  });
  await application.initialize();
  assert.equal(application.getServices().ready, true);
  await application.close();
  assert.deepEqual(log, ['cleanup']);
  process.stdout.write(`All exports load and manage services on ${process.version}\n`);
})().catch((error) => {
  process.exitCode = 1;
  throw error;
});
