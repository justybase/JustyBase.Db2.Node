// Minimal live smoke tests for scripts/test-db2-integration.sh (npm run test:integration:db2).
//
// The script starts a throwaway Db2 container and exports DB2_TEST_DSN plus
// DB2_IT_SCHEMA (a schema created for that run). Without DB2_TEST_DSN every test
// is skipped, like the rest of tests/live.
require('../load-env.cjs');
const test = require('node:test');
const assert = require('node:assert/strict');
const { Db2ServerError } = require('../../dist/cjs/errors.js');
const { Db2Connection } = require('../../dist/cjs/network/session.js');
const { fixtureSchema } = require('./fixture-schema.cjs');

const dsn = process.env.DB2_TEST_DSN;
const options = { skip: !dsn };

/** The run's schema from the script, or the session's current schema for manual runs. */
async function schemaOf(db) {
  const name = process.env.DB2_IT_SCHEMA;
  return name ? '"' + name.replaceAll('"', '""') + '"' : fixtureSchema(db);
}

let tableCounter = 0;
function uniqueTable(prefix) {
  tableCounter += 1;
  return `${prefix}_${process.pid}_${Date.now() % 1_000_000_000}_${tableCounter}`;
}

async function count(db, table) {
  return (await db.query(`SELECT COUNT(*) FROM ${table}`)).rows[0][0];
}

test('smoke: connect, simple query, close', options, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    assert.deepEqual((await db.query('VALUES 1')).rows, [[1]]);
  } finally {
    await db.close();
  }
});

test('smoke: CREATE/INSERT/SELECT/UPDATE/DELETE with types and NULLs', options, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    const table = `${await schemaOf(db)}.${uniqueTable('SMOKE_DML')}`;
    await db.execute(
      `CREATE TABLE ${table} (ID INTEGER NOT NULL PRIMARY KEY, BIG BIGINT, NAME VARCHAR(50), ` +
      'CODE CHAR(4), AMOUNT DECIMAL(12,3), BORN DATE, STAMP TIMESTAMP, FLAG BOOLEAN)');

    const inserted = await db.execute(
      `INSERT INTO ${table} VALUES ` +
      "(1, 9223372036854775807, 'alpha', 'AB', 123.456, '2024-02-29', '2024-02-29 23:59:58.123456', TRUE), " +
      "(2, -5, 'Zażółć 東京', 'CD', -0.001, '1999-12-31', '2000-01-01 00:00:00', FALSE), " +
      '(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL)');
    assert.equal(inserted, 3);

    const { rows } = await db.query(`SELECT ID, BIG, NAME, CODE, AMOUNT, BORN, STAMP, FLAG FROM ${table} ORDER BY ID`);
    assert.equal(rows.length, 3);
    assert.deepEqual(rows[0], [1, 9223372036854775807n, 'alpha', 'AB  ', '123.456', '2024-02-29', '2024-02-29-23.59.58.123456', true]);
    assert.equal(rows[1][1], -5n);
    assert.equal(rows[1][2], 'Zażółć 東京');
    assert.equal(rows[1][4], '-0.001');
    assert.equal(rows[1][7], false);
    assert.deepEqual(rows[2], [3, null, null, null, null, null, null, null]);

    assert.equal(await db.execute(`UPDATE ${table} SET NAME = 'changed' WHERE ID IN (1, 2)`), 2);
    assert.deepEqual((await db.query(`SELECT NAME FROM ${table} WHERE NAME IS NOT NULL ORDER BY ID`)).rows, [['changed'], ['changed']]);

    assert.equal(await db.execute(`DELETE FROM ${table} WHERE ID = 3`), 1);
    assert.equal(await count(db, table), 2);

    await db.execute(`DROP TABLE ${table}`);
  } finally {
    await db.close();
  }
});

test('smoke: parameterized and prepared statements', options, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    const table = `${await schemaOf(db)}.${uniqueTable('SMOKE_PARAM')}`;
    await db.execute(`CREATE TABLE ${table} (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR(30))`);

    assert.equal(await db.execute(`INSERT INTO ${table} VALUES (?, ?)`, [1, 'one']), 1);
    assert.equal(await db.execute(`INSERT INTO ${table} VALUES (?, ?)`, [2, null]), 1);

    const select = `SELECT NAME FROM ${table} WHERE ID = ?`;
    assert.deepEqual((await db.query(select, [1])).rows, [['one']]);
    assert.deepEqual((await db.query(select, [2])).rows, [[null]]);
    assert.deepEqual((await db.query(select, [99])).rows, []);

    const statement = await db.prepare(select);
    for (let i = 0; i < 3; i += 1) {
      assert.deepEqual((await db.queryStatement(statement, [1])).rows, [['one']]);
    }

    await db.execute(`DROP TABLE ${table}`);
  } finally {
    await db.close();
  }
});

test('smoke: transaction rollback, then commit', options, async () => {
  const db = await Db2Connection.connect(dsn);
  const other = await Db2Connection.connect(dsn);
  try {
    const table = `${await schemaOf(db)}.${uniqueTable('SMOKE_TX')}`;
    await db.execute(`CREATE TABLE ${table} (ID INTEGER NOT NULL PRIMARY KEY)`);

    let tx = await db.beginTransaction();
    await tx.execute(`INSERT INTO ${table} VALUES (1)`);
    await tx.rollback();
    assert.equal(await count(db, table), 0, 'rolled-back row must not exist');

    tx = await db.beginTransaction();
    await tx.execute(`INSERT INTO ${table} VALUES (2)`);
    await tx.commit();
    assert.equal(await count(db, table), 1, 'committed row must exist');
    assert.equal(await count(other, table), 1, 'a second session sees the committed row');

    await db.execute(`DROP TABLE ${table}`);
  } finally {
    await other.close();
    await db.close();
  }
});

test('smoke: result shape (column names and count)', options, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    const result = await db.query("SELECT 7 AS SEVEN, CAST('x' AS VARCHAR(5)) AS LETTER, CAST(NULL AS INTEGER) AS NOTHING FROM SYSIBM.SYSDUMMY1");
    assert.deepEqual(result.columns.map((column) => column.name), ['SEVEN', 'LETTER', 'NOTHING']);
    assert.deepEqual(result.rows, [[7, 'x', null]]);
  } finally {
    await db.close();
  }
});

test('smoke: invalid SQL is reported and the session stays usable', options, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    await assert.rejects(db.query('SELEC 1 FROM SYSIBM.SYSDUMMY1'), (error) => {
      assert.ok(error instanceof Db2ServerError, `unexpected error: ${error}`);
      assert.equal(error.sqlCode, -104);
      return true;
    });
    await assert.rejects(db.query('SELECT * FROM IT_SMOKE_NO_SUCH_TABLE'), (error) => {
      assert.ok(error instanceof Db2ServerError, `unexpected error: ${error}`);
      assert.equal(error.sqlCode, -204);
      return true;
    });
    assert.deepEqual((await db.query('VALUES 2')).rows, [[2]]);
  } finally {
    await db.close();
  }
});
