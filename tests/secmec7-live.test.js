const test = require('node:test');
const assert = require('node:assert/strict');
const { Db2Connection } = require('../dist/cjs/network/session.js');
const { Db2AuthenticationError, Db2UnsupportedFeatureError } = require('../dist/cjs/errors.js');

// Opt-in profile: a full DSN with security_mechanism=7 against a server running
// AUTHENTICATION=SERVER_ENCRYPT (verified on Db2 LUW 11.5.4.0 and 12.1.5.0).
// Without the variable the tests are reported as skipped with their reason, never
// as passed. Gating lanes allow exactly this reason (see tools/ci/check-junit-no-skips.cjs).
const SECMEC7_DSN = process.env.DB2_SECMEC7_TEST_DSN;
const SECMEC7_SKIP = 'DB2_SECMEC7_TEST_DSN is not set: opt-in SECMEC 7 profile';

function withMechanism(dsn, mechanism) {
  const url = new URL(dsn);
  url.searchParams.set('security_mechanism', String(mechanism));
  return url.toString();
}

test('SECMEC 7 live authentication against the SERVER_ENCRYPT profile', { skip: SECMEC7_DSN ? false : SECMEC7_SKIP }, async () => {
  const db = await Db2Connection.connect(String(SECMEC7_DSN));
  try {
    const scalar = await db.query('VALUES 42');
    assert.deepEqual(scalar.rows, [[42]]);
    const bound = await db.query('VALUES CAST(? AS INTEGER)', [43]);
    assert.deepEqual(bound.rows, [[43]]);
  } finally {
    await db.close();
  }
});

test('SECMEC 7 live profile rejects plaintext SECMEC 3 without a mechanism switch', { skip: SECMEC7_DSN ? false : SECMEC7_SKIP }, async () => {
  await assert.rejects(Db2Connection.connect(withMechanism(String(SECMEC7_DSN), 3)), (error) => {
    assert.ok(error instanceof Db2UnsupportedFeatureError);
    assert.match(error.message, /rejected security mechanism 3 .*accepts .*\b7\b/);
    return true;
  });
});

test('SECMEC 7 live profile reports a wrong password as an authentication failure', { skip: SECMEC7_DSN ? false : SECMEC7_SKIP }, async () => {
  const url = new URL(String(SECMEC7_DSN));
  url.password = `${decodeURIComponent(url.password)}-wrong`;
  await assert.rejects(Db2Connection.connect(url.toString()), (error) => {
    assert.ok(error instanceof Db2AuthenticationError);
    assert.match(error.message, /SECCHKCD=15/);
    return true;
  });
});
