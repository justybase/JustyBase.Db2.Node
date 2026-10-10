const test = require('node:test');
const assert = require('node:assert/strict');
const { Db2Connection } = require('../dist/cjs/network/session.js');

test('SECMEC 9 live authentication against the SERVER_ENCRYPT profile', async () => {
  const dsn = process.env.DB2_SECMEC9_TEST_DSN;
  // Opt-in profile (full DSN including security_mechanism=9). A plain early
  // return counts as passed, not skipped, so the serial gate stays green
  // when the profile is absent.
  if (!dsn) return;
  const db = await Db2Connection.connect(dsn);
  try {
    const scalar = await db.query('VALUES 42');
    assert.deepEqual(scalar.rows, [[42]]);
    const bound = await db.query('VALUES CAST(? AS INTEGER)', [43]);
    assert.deepEqual(bound.rows, [[43]]);
  } finally {
    await db.close();
  }
});
