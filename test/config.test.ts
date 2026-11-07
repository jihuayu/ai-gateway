import { test } from 'node:test';
import assert from 'node:assert/strict';
import { loadConfig } from '../src/config.js';
test('configuration rejects absent credentials and invalid endpoints', () => {
  assert.throws(() => loadConfig({}), /Invalid configuration/);
  assert.throws(() => loadConfig({DATABASE_URL:'https://example.com', REDIS_URL:'redis://localhost', ENCRYPTION_KEY:'0'.repeat(64)}), /PostgreSQL URL/);
  assert.equal(loadConfig({DATABASE_URL:'postgresql://localhost/gateway',REDIS_URL:'redis://localhost',ENCRYPTION_KEY:'0'.repeat(64)}).PORT, 3000);
  assert.throws(()=>loadConfig({DATABASE_URL:'postgresql://localhost/gateway',REDIS_URL:'redis://localhost',ENCRYPTION_KEY:'0'.repeat(64),NODE_ENV:'production',HOT_DURABILITY:'memory'}),/restricted to development/);
});
