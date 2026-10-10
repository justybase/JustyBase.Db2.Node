'use strict';
require('../tests/load-env.cjs');
const assert = require('node:assert/strict');
const { Db2Connection } = require('../dist/cjs/network/session.js');

(async () => {
  const [table, mode] = process.argv.slice(2);
  if (!/^DM_[A-F0-9]{32}$/.test(table) || !['direct', 'prepared'].includes(mode)) throw new Error('Invalid controlled fixture');
  const db = await Db2Connection.connect(process.env.DB2_TEST_DSN);
  try {
    const cases = [
      [`INSERT INTO ${table} VALUES (1,10)`, 1],
      [`INSERT INTO ${table} VALUES (2,20),(3,30),(4,40)`, 3],
      [`UPDATE ${table} SET V=V+1 WHERE ID=0`, 0],
      [`UPDATE ${table} SET V=V+1 WHERE ID=1`, 1],
      [`UPDATE ${table} SET V=V+1`, 4],
      [`DELETE FROM ${table} WHERE ID=0`, 0],
      [`DELETE FROM ${table} WHERE ID=1`, 1],
      [`DELETE FROM ${table}`, 3],
      [`DELETE FROM ${table}`, 0],
    ];
    for (const [sql, expected] of cases) {
      if (mode === 'direct') assert.equal(await db.execute(sql), expected, sql.split(' ')[0]);
      else { const stmt = await db.prepare(sql); try { assert.equal(await stmt.execute([]), expected); } finally { stmt.close(); } }
    }
    const sql = `INSERT INTO ${table} VALUES (?,?)`;
    assert.equal(await db.executeBatch(sql, [[5,50], [6,60], [7,70]]), 3);
    assert.equal(await db.executeBatch(sql, []), 0);
    assert.deepEqual((await db.query(`SELECT * FROM ${table} ORDER BY ID`)).rows, [[5,50], [6,60], [7,70]]);
  } finally { await db.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
