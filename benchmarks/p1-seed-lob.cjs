#!/usr/bin/env node
'use strict';
require('../tests/load-env.cjs');
// Seeds the large P1 LOB rows that cannot be built server-side because Db2's
// REPEAT/concatenation limits block >~64 KB string construction. Content is a
// deterministic function of the row id, so the dataset stays reproducible.
//
// Usage: DB2_TEST_DSN=... node p1-seed-lob.cjs
const { Db2Connection } = require('../dist/cjs/network/session.js');

const spec = [
  { table: 'P1BENCH.LOB_256K', rows: 100, size: 262144 },
  { table: 'P1BENCH.LOB_1M', rows: 10, size: 1048576 },
  { table: 'P1BENCH.LOB_8M', rows: 1, size: 8388608 },
];

function content(id, size) {
  const prefix = `id=${id};`;
  return Buffer.from(prefix + 'x'.repeat(size - prefix.length), 'utf8');
}

function byteSource(buffer, chunkSize = 65536) {
  return {
    async *[Symbol.asyncIterator]() {
      for (let offset = 0; offset < buffer.length; offset += chunkSize)
        yield buffer.subarray(offset, Math.min(offset + chunkSize, buffer.length));
    },
  };
}

(async () => {
  const dsn = process.env.DB2_TEST_DSN;
  if (!dsn) throw new Error('Set DB2_TEST_DSN');
  const db = await Db2Connection.connect(dsn);
  try {
    for (const { table, rows, size } of spec) {
      for (let id = 1; id <= rows; id++) {
        const bytes = content(id, size);
        await db.execute(`INSERT INTO ${table} (ID, B, C) VALUES (?, ?, ?)`, [
          id,
          { source: byteSource(bytes), length: bytes.length },
          { source: byteSource(bytes), length: bytes.length },
        ]);
      }
      const check = await db.query(`SELECT COUNT(*), MIN(LENGTH(B)), MAX(LENGTH(B)) FROM ${table}`);
      const [count, minLength, maxLength] = check.rows[0];
      if (Number(count) !== rows || Number(minLength) !== size || Number(maxLength) !== size)
        throw new Error(`${table} seed mismatch: count=${count}, min=${minLength}, max=${maxLength}`);
      console.log(`${table}: ${rows} rows of ${size} bytes seeded`);
    }
  } finally {
    await db.close();
  }
})().catch((error) => { console.error(error); process.exit(1); });
