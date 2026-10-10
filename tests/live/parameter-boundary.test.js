require('../load-env.cjs');
const test = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { Db2ServerError } = require('../../dist/cjs/errors.js');
const { Db2Connection } = require('../../dist/cjs/network/session.js');
const { fixtureSchema } = require('./fixture-schema.cjs');

const dsn = process.env.DB2_TEST_DSN;

async function dropFixtureTable(db, table) {
  try {
    await db.execute(`DROP TABLE ${await fixtureSchema(db)}.${table}`);
  } catch (error) {
    if (!(error instanceof Db2ServerError && error.sqlCode === -204 && error.sqlState === '42704')) throw error;
  }
}

test('live Db2 preserves SQLDTA parameters across CPT boundaries and 148-marker INSERTs', { skip: !dsn, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const table = `WB_TS_${randomUUID().replace(/-/gu, '').slice(0, 12).toUpperCase()}`;
  let created = false;
  try {
    for (const count of [83, 84, 85]) {
      const sql = `VALUES (${Array.from({ length: count }, () => 'COALESCE(CAST(? AS INTEGER), 0)').join(' + ')})`;
      const parameters = Array.from({ length: count }, (_, index) => index % 11 === 0 ? null : index);
      const expected = parameters.reduce((sum, value) => sum + (value ?? 0), 0);
      const result = await db.query(sql, parameters);
      assert.deepEqual(result.rows, [[expected]], `${count}-marker SQLDTA query`);
    }

    await db.execute(`CREATE TABLE ${await fixtureSchema(db)}.${table} (ID INTEGER NOT NULL PRIMARY KEY, V1 INTEGER, V2 VARCHAR(32), V3 SMALLINT)`);
    created = true;
    const expectedRows = Array.from({ length: 37 }, (_, index) => [
      90_000 + index,
      index % 8 === 0 ? null : index * 13,
      index % 5 === 0 ? null : `row-${index}`,
      index % 7 === 0 ? null : index - 18,
    ]);
    const sql = `INSERT INTO ${table} VALUES ${expectedRows.map(() => '(?, ?, ?, ?)').join(', ')}`;
    const parameters = expectedRows.flat();
    assert.equal(parameters.length, 148);
    await db.execute(sql, parameters);

    const result = await db.query(`SELECT ID, V1, V2, V3 FROM ${table} ORDER BY ID`);
    assert.deepEqual(result.rows, expectedRows);
  } finally {
    if (created) await dropFixtureTable(db, table);
    await db.close();
  }
});
