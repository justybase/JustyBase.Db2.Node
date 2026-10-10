'use strict';
require('../tests/load-env.cjs');
const assert = require('node:assert/strict');
const { Db2Connection } = require('../dist/cjs/network/session.js');

(async () => {
  const [sourceTable, destinationTable, mode, countArg = '10', batchArg = '3', bytesArg = '4096'] = process.argv.slice(2);
  const count = Number(countArg), batchRows = Number(batchArg), batchBytes = Number(bytesArg);
  if (![count, batchRows, batchBytes].every(n => Number.isSafeInteger(n) && n > 0)) throw new Error('Invalid fixture limits');
  if (![sourceTable, destinationTable].every(t => /^TW_[A-F0-9]{12}_[NTXG](?:_D)?$/.test(t))
      || !['direct', 'prepared', 'batch', 'atomic', 'checkpoint'].includes(mode)) throw new Error('Invalid controlled fixture');
  const source = await Db2Connection.connect(process.env.DB2_TEST_DSN);
  let destination;
  try {
    destination = await Db2Connection.connect(process.env.DB2_DEST_TEST_DSN ?? process.env.DB2_TEST_DSN);
    const sql = `SELECT * FROM ${sourceTable} ORDER BY ID`;
    const result = await source.query(sql);
    const columns = result.columns.map(c => c.name);
    const insert = `INSERT INTO ${destinationTable} VALUES (${columns.map(() => '?').join(',')})`;
    assert.equal(result.rows.length, count);
    if (mode === 'atomic' || mode === 'checkpoint') {
      async function* rows() { for await (const row of source.queryStream(sql)) yield row.values; }
      let progress;
      try { progress = await destination.importRows(rows(), { table: destinationTable, columns }, {
        policy: mode === 'atomic' ? 'atomic' : 'commitEveryRows', maxBatchRows: batchRows, maxBatchBytes: batchBytes,
      });
      } catch (error) {
        if (process.env.DB2_FAULT_EXPECTED_PROGRESS === undefined) throw error;
        assert.equal(error.progress.committedRows, Number(process.env.DB2_FAULT_EXPECTED_PROGRESS));
        assert.equal(destination.isClosed, true);
        console.log('PASS TS destination reset: confirmed progress and discarded session');
        return;
      }
      if (process.env.DB2_FAULT_EXPECTED_PROGRESS !== undefined) throw new Error('Expected destination reset did not fail import');
      assert.equal(progress.sourceRowsRead, count);
      assert.equal(progress.committedRows, count);
    } else if (mode === 'batch') assert.equal(await destination.executeBatch(insert, result.rows), count);
    else if (mode === 'prepared') {
      const stmt = await destination.prepare(insert);
      try { for (const row of result.rows) assert.equal(await stmt.execute(row), 1); } finally { stmt.close(); }
    } else for (const row of result.rows) assert.equal(await destination.execute(insert, row), 1);
  } finally { await destination?.close(); await source.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
