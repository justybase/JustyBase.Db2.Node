require('../load-env.cjs');
const test = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { Db2BatchTimeoutError, Db2ServerError, Db2TimeoutError } = require('../../dist/cjs/errors.js');
const { Db2Connection } = require('../../dist/cjs/network/session.js');
const { Db2Pool } = require('../../dist/cjs/network/pool.js');
const { fixtureSchema } = require('./fixture-schema.cjs');

const dsn = process.env.DB2_TEST_DSN;

async function dropFixtureTable(db, table) {
  try {
    await db.execute(`DROP TABLE ${await fixtureSchema(db)}.${table}`);
  } catch (error) {
    if (!(error instanceof Db2ServerError && error.sqlCode === -204 && error.sqlState === '42704')) throw error;
  }
}

test('live Db2 command deadline interrupts a lock wait and deadline batch reports confirmed progress', { skip: !dsn, timeout: 40_000 }, async () => {
  const table = `TS_DB2_DL_${randomUUID().replaceAll('-', '').slice(0, 18).toUpperCase()}`;
  const locker = await Db2Connection.connect(dsn);
  const waiter = await Db2Connection.connect(dsn);
  let transaction;
  let created = false;
  try {
    await locker.execute(`CREATE TABLE ${await fixtureSchema(locker)}.${table} (ID INT NOT NULL PRIMARY KEY, V INT NOT NULL)`);
    created = true;
    await locker.execute(`INSERT INTO ${table} VALUES (1, 0), (2, 0)`);

    transaction = await locker.beginTransaction();
    await transaction.execute(`UPDATE ${table} SET V = V + 100 WHERE ID = 2`);
    const command = waiter.execute(`UPDATE ${table} SET V = V + 1 WHERE ID = 2`, undefined,
      { timeoutMs: 1_000, cancelCleanupTimeoutMs: 5_000 });
    await new Promise(resolve => setTimeout(resolve, 300));
    assert.equal(await Promise.race([command.then(() => true, () => true), new Promise(resolve => setTimeout(() => resolve(false), 0))]), false,
      'the update should still be waiting on the row lock before its deadline');
    await assert.rejects(command, error => error instanceof Db2TimeoutError && error.name === 'TimeoutError');
    assert.equal(waiter.isClosed, false, 'confirmed SQLCODE -952 should leave this live session reusable');
    await transaction.rollback();
    transaction = undefined;
    assert.deepEqual((await locker.query(`SELECT ID, V FROM ${table} ORDER BY ID`)).rows, [[1, 0], [2, 0]]);
    assert.deepEqual((await waiter.query('VALUES 41')).rows, [[41]]);

    transaction = await locker.beginTransaction();
    await transaction.execute(`UPDATE ${table} SET V = V + 100 WHERE ID = 2`);
    const batch = waiter.executeBatch(`UPDATE ${table} SET V = V + 1 WHERE ID = ?`, [[1], [2]],
      { timeoutMs: 1_000, cancelCleanupTimeoutMs: 5_000 });
    await new Promise(resolve => setTimeout(resolve, 300));
    assert.equal(await Promise.race([batch.then(() => true, () => true), new Promise(resolve => setTimeout(() => resolve(false), 0))]), false,
      'the second set should still be waiting on the row lock before its deadline');
    await assert.rejects(batch, error => error instanceof Db2BatchTimeoutError
      && error instanceof Db2TimeoutError
      && error.name === 'TimeoutError'
      && error.completedParameterSets === 1
      && error.inFlightParameterSetIndex === 1
      && error.rowsAffected === 1
      && error.cause instanceof Db2TimeoutError);
    await transaction.rollback();
    transaction = undefined;
    assert.deepEqual((await locker.query(`SELECT ID, V FROM ${table} ORDER BY ID`)).rows, [[1, 1], [2, 0]],
      'the first autocommitted set remains while the interrupted locked set does not');
    assert.equal(waiter.isClosed, false);
    assert.deepEqual((await waiter.query('VALUES 42')).rows, [[42]]);
  } finally {
    // Close the worker first: a failed assertion can leave a command holding
    // locks, and DROP TABLE must not wait indefinitely for that session.
    await waiter.close().catch(() => undefined);
    if (transaction) await transaction.rollback().catch(() => undefined);
    if (created) await dropFixtureTable(locker, table).catch(() => undefined);
    await locker.close();
  }
});

test('live Db2 command deadline starts after the pool lease is acquired', { skip: !dsn, timeout: 40_000 }, async () => {
  const pool = new Db2Pool(dsn, { maxConnections: 1 });
  let enterLease;
  const enteredLease = new Promise(resolve => { enterLease = resolve; });
  let releaseLease;
  const leaseGate = new Promise(resolve => { releaseLease = resolve; });
  let holder;
  try {
    holder = pool.withConnection(async () => {
      enterLease();
      await leaseGate;
    });
    await enteredLease;

    const query = pool.query('VALUES 43', undefined, { timeoutMs: 150 });
    await new Promise(resolve => setTimeout(resolve, 350));
    releaseLease();
    await holder;
    assert.deepEqual((await query).rows, [[43]], 'waiting for a pooled connection does not consume command timeout');
    assert.equal(pool.connectionCount, 1);
    assert.equal(pool.idleCount, 1);
  } finally {
    releaseLease?.();
    await holder?.catch(() => undefined);
    await pool.close();
  }
});
