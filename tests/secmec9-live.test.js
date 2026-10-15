const test = require('node:test');
const assert = require('node:assert/strict');
const { Db2Connection } = require('../dist/cjs/network/session.js');

// Opt-in profile (full DSN including security_mechanism=9). Without the variable the
// test is reported as skipped with its reason, never as passed. Gating lanes allow
// exactly this reason (see tools/ci/check-junit-no-skips.cjs).
const SECMEC9_SKIP = 'DB2_SECMEC9_TEST_DSN is not set: opt-in SECMEC 9 profile';

test('SECMEC 9 live authentication against the SERVER_ENCRYPT profile', { skip: process.env.DB2_SECMEC9_TEST_DSN ? false : SECMEC9_SKIP }, async () => {
  const db = await Db2Connection.connect(process.env.DB2_SECMEC9_TEST_DSN);
  try {
    const scalar = await db.query('VALUES 42');
    assert.deepEqual(scalar.rows, [[42]]);
    const bound = await db.query('VALUES CAST(? AS INTEGER)', [43]);
    assert.deepEqual(bound.rows, [[43]]);
  } finally {
    await db.close();
  }
});
