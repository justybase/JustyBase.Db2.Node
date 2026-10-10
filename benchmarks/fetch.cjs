require('../tests/load-env.cjs');
const { performance } = require('node:perf_hooks');
const { Db2Connection } = require('../dist/cjs/network/session.js');

const sql = 'SELECT a.COLNO, a.COLNAME, b.COLNO FROM SYSCAT.COLUMNS a CROSS JOIN SYSCAT.COLUMNS b FETCH FIRST 10000 ROWS ONLY';
const dsn = process.env.DB2_TEST_DSN;
if (!dsn) throw new Error('Set DB2_TEST_DSN');

(async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    await db.query(sql);
    const times = [];
    for (let iteration = 0; iteration < 7; iteration++) {
      const started = performance.now();
      const result = await db.query(sql);
      const elapsed = performance.now() - started;
      const checksum = result.rows.reduce((sum, [first, name, last]) => sum + Number(first) + name.length + Number(last), 0);
      if (result.rows.length !== 10000) throw new Error(`Expected 10000 rows; got ${result.rows.length}`);
      times.push(elapsed);
      console.log(`ts iteration=${iteration + 1} rows=${result.rows.length} checksum=${checksum} ms=${elapsed.toFixed(2)}`);
    }
    times.sort((a, b) => a - b);
    console.log(`median ts=${times[3].toFixed(2)} ms`);
  } finally { await db.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; });
