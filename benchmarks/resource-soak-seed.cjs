#!/usr/bin/env node
'use strict';
// Creates the fixtures that benchmarks/resource-soak.cjs expects for one soak run:
//   SO_<TAG>_N   source rows (1000 typed VARCHAR rows)
//   SO_<TAG>_ND  destination for the row importers
//   SO_<TAG>_X   source LOB rows (2.5 MB BLOB, large CLOB/DBCLOB/XML, empty and NULL rows)
//   SO_<TAG>_XD  destination for the LOB importers
//   SO_<TAG>_P   procedure with three result sets and one OUT parameter
//
// Usage: DB2_TEST_DSN=db2://... node benchmarks/resource-soak-seed.cjs <TAG>
// TAG is 12 upper-case hex characters. Objects are created in the session's
// CURRENT SCHEMA and dropped by the caller (the integration script's container is
// removed after the run).
require('../tests/load-env.cjs');
const { Db2Connection } = require('../dist/cjs/network/session.js');

const tag = process.argv[2];
if (!/^[A-F0-9]{12}$/u.test(tag ?? '')) {
  console.error('usage: node resource-soak-seed.cjs <12 upper-case hex characters>');
  process.exit(2);
}
const dsn = process.env.DB2_TEST_DSN;
if (!dsn) {
  console.error('Set DB2_TEST_DSN');
  process.exit(2);
}

const names = {
  n: `SO_${tag}_N`,
  nd: `SO_${tag}_ND`,
  x: `SO_${tag}_X`,
  xd: `SO_${tag}_XD`,
  p: `SO_${tag}_P`,
};

const ROWS = 1000;
const BLOB_BYTES = 2_500_123;

function bytesForLob(length, salt = 11) {
  const bytes = Buffer.allocUnsafe(length);
  for (let i = 0; i < length; i++) bytes[i] = (i * 37 + salt) % 256;
  return bytes;
}

async function dropIfExists(db, name) {
  await db.execute(`DROP TABLE ${name}`).catch(() => {});
}

(async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    for (const table of [names.n, names.nd, names.x, names.xd]) await dropIfExists(db, table);
    await db.execute(`DROP PROCEDURE ${names.p}`).catch(() => {});

    await db.execute(`CREATE TABLE ${names.n} (ID INTEGER NOT NULL PRIMARY KEY, V VARCHAR(64) NOT NULL)`);
    await db.execute(`CREATE TABLE ${names.nd} (ID INTEGER NOT NULL PRIMARY KEY, V VARCHAR(64) NOT NULL)`);
    const sourceRows = Array.from({ length: ROWS }, (_, index) => [index + 1, `Zażółć 東京 ${index + 1}  `]);
    await db.executeBatch(`INSERT INTO ${names.n} (ID, V) VALUES (?, ?)`, sourceRows);

    const lobTable = (name) => `CREATE TABLE ${name} (ID INTEGER NOT NULL PRIMARY KEY, B BLOB(3M), C CLOB(1M), G DBCLOB(1M), X XML)`;
    await db.execute(lobTable(names.x));
    await db.execute(lobTable(names.xd));
    const clob = 'Zażółć 東京 🚀  '.repeat(10000);
    const dbclob = '漢字 Zażółć 🚀  '.repeat(10000);
    const xmlBody = `<root>${'x'.repeat(180000)}東京 🚀</root>`;
    await db.execute(`INSERT INTO ${names.x} (ID, B, C, G, X) VALUES (1, CAST(? AS BLOB(3M)), CAST(? AS CLOB(1M)), CAST(? AS DBCLOB(1M)), XMLPARSE(DOCUMENT CAST(? AS CLOB(1M)) PRESERVE WHITESPACE))`,
      [bytesForLob(BLOB_BYTES), clob, dbclob, xmlBody]);
    await db.execute(`INSERT INTO ${names.x} (ID, B, C, G, X) VALUES (2, CAST(? AS BLOB(3M)), CAST(? AS CLOB(1M)), CAST(? AS DBCLOB(1M)), XMLPARSE(DOCUMENT CAST(? AS CLOB(1M)) PRESERVE WHITESPACE))`,
      [Buffer.alloc(0), '', '', '<r/>']);
    await db.execute(`INSERT INTO ${names.x} (ID) VALUES (3)`);

    await db.execute(`CREATE PROCEDURE ${names.p} (OUT P INTEGER) LANGUAGE SQL DYNAMIC RESULT SETS 3 BEGIN `
      + `DECLARE C1 CURSOR WITH RETURN TO CALLER FOR SELECT 17 AS V FROM SYSIBM.SYSDUMMY1; `
      + `DECLARE C2 CURSOR WITH RETURN TO CALLER FOR SELECT 18 AS V FROM SYSIBM.SYSDUMMY1 WHERE 1 = 0; `
      + `DECLARE C3 CURSOR WITH RETURN TO CALLER FOR SELECT 19 AS V FROM SYSIBM.SYSDUMMY1; `
      + `SET P = 23; OPEN C1; OPEN C2; OPEN C3; END`);

    const seeded = Number((await db.query(`SELECT COUNT(*) FROM ${names.n}`)).rows[0][0]);
    if (seeded !== ROWS) throw new Error(`seeded ${seeded} source rows, expected ${ROWS}`);
    console.log(JSON.stringify({ type: 'soak-fixtures', tag, ...names, rows: ROWS, blobBytes: BLOB_BYTES }));
  } finally {
    await db.close();
  }
})().catch((error) => {
  console.error(`soak fixture seeding failed: ${error?.message ?? error}`);
  process.exitCode = 1;
});
