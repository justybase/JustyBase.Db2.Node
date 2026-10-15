// Live checks for session context switches: CURRENT SCHEMA, database change and
// SQL authorization switch. These previously had only loopback coverage.
//
// The database switch needs a second database on the same instance. The integration
// script creates it and exports DB2_IT_SECOND_DATABASE; without it that single case
// is reported as skipped with its reason.
require('../load-env.cjs');
const test = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { Db2Connection } = require('../../dist/cjs/network/session.js');

const dsn = process.env.DB2_TEST_DSN;
const secondDatabase = process.env.DB2_IT_SECOND_DATABASE;

function uniqueName(prefix) {
  return `${prefix}_${randomUUID().replace(/-/gu, '').slice(0, 10).toUpperCase()}`;
}

test('live Db2 setCurrentSchema changes unqualified resolution and rejects invalid names before I/O', { skip: !dsn, timeout: 40_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    const schema = uniqueName('CTX_SCHEMA');
    await db.setCurrentSchema(schema);
    assert.deepEqual((await db.query('VALUES CURRENT SCHEMA')).rows, [[schema]], 'CURRENT SCHEMA reflects the change');

    await assert.rejects(() => db.setCurrentSchema('   '), TypeError, 'an empty schema is rejected before any statement');
    await assert.rejects(() => db.setCurrentSchema('BAD"NAME\0'), Error, 'a control character in the schema is rejected');
    assert.deepEqual((await db.query('VALUES CURRENT SCHEMA')).rows, [[schema]], 'rejected names leave the schema unchanged');
    assert.deepEqual((await db.query('VALUES 7')).rows, [[7]], 'session remains reusable after rejected names');
  } finally {
    await db.close();
  }
});

test('live Db2 changeDatabase moves the session to a second database and back', { skip: secondDatabase ? (dsn ? false : 'DB2_TEST_DSN is not set') : 'DB2_IT_SECOND_DATABASE is not set: the integration script creates the second database', timeout: 90_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  /** @type {string} */
  const originalDatabase = String((await db.query('VALUES CURRENT SERVER')).rows[0][0]);
  try {
    await db.changeDatabase(secondDatabase);
    // Db2 reports unquoted database names in upper case; the switch itself is case-insensitive.
    assert.equal((await db.query('VALUES CURRENT SERVER')).rows[0][0], secondDatabase.toUpperCase(), 'CURRENT SERVER reports the second database');
    assert.deepEqual((await db.query('VALUES 11')).rows, [[11]], 'session is usable on the second database');

    await db.changeDatabase(originalDatabase);
    assert.equal((await db.query('VALUES CURRENT SERVER')).rows[0][0], originalDatabase, 'the session switches back');
  } finally {
    await db.close();
  }
});

test('live Db2 changeDatabase refuses an active transaction and keeps the session on its database', { skip: !dsn, timeout: 40_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const before = (await db.query('VALUES CURRENT SERVER')).rows[0][0];
  try {
    await db.beginTransaction();
    await assert.rejects(() => db.changeDatabase(uniqueName('NOPE')), /transaction is active/u, 'switch is refused inside a transaction');
    await db.rollback();
    assert.equal(db.isClosed, false, 'the refused switch does not close the session');
    assert.equal((await db.query('VALUES CURRENT SERVER')).rows[0][0], before, 'the session stays on its original database');
  } finally {
    await db.close();
  }
});

test('live Db2 failed changeDatabase fails closed: the session is closed, never half-connected', { skip: !dsn, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    await assert.rejects(() => db.changeDatabase(uniqueName('MISSING')), Error, 'connecting to a missing database fails');
    assert.equal(db.isClosed, true, 'the session is closed after a failed reconnect');
    await assert.rejects(() => db.query('VALUES 1'), Error, 'queries on the closed session fail instead of reusing a broken transport');
  } finally {
    await db.close().catch(() => {});
  }
});

test('live Db2 switchUser without trusted context fails closed and keeps the original authorization id', { skip: !dsn, timeout: 40_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const original = (await db.query('VALUES SESSION_USER')).rows[0][0];
  try {
    // Plain password sessions cannot assume another authorization id, so the switch
    // must be rejected and the session must keep working under its own identity.
    await assert.rejects(() => db.switchUser(uniqueName('OTHER')), Error, 'switch without trusted context is rejected');
    assert.equal((await db.query('VALUES SESSION_USER')).rows[0][0], original, 'authorization id is unchanged after the rejected switch');
    assert.deepEqual((await db.query('VALUES 13')).rows, [[13]], 'session remains reusable after the rejected switch');
  } finally {
    await db.close();
  }
});
