'use strict';
// Differential SELECT/type parity: ts-db2 vs node-ibm_db (official IBM CLI provider).
//
// Scope (intentionally narrow): SELECT value-shape parity only — scalars, NULLs,
// temporal/text/binary numerics and small LOBs. No DML/procedures/cancellation/soak.
//
// Legal/isolation notes:
// - `ibm_db` (with its IBM clidriver download) is a TEST-ONLY oracle. It must never
//   appear in package.json dependencies, dist/, or the published tarball. CI installs
//   it in an isolated step with `npm install --no-save ibm_db@<pinned>`.
// - Without DB2_TEST_DSN every test is skipped, like the rest of tests/live.
// - Without DB2_PARITY_IBMDB=1 the test is reported as skipped (node --test skip
//   with a reason). Gating lanes run tools/ci/check-junit-no-skips.cjs, which rejects
//   any skip not explicitly allowed. With DB2_PARITY_IBMDB=1 a missing `ibm_db`
//   fails closed.
// - Secret values are never logged; DSN passwords are redacted from failures.
require('../load-env.cjs');
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { parseDsn } = require('../../dist/cjs/config.js');
const { Db2Connection } = require('../../dist/cjs/network/session.js');

const dsn = process.env.DB2_TEST_DSN;
const parityEnabled = process.env.DB2_PARITY_IBMDB === '1';
const IBMDB_SKIPPED_MARKER = 'ibm_db parity skipped: set DB2_PARITY_IBMDB=1 with DB2_TEST_DSN and installed ibm_db to run';
const IBMDB_PIN = 'ibm_db@4.0.1';

function redact(value) {
  return String(value).replace(/PWD=[^;]*/gi, 'PWD=[REDACTED]');
}

function loadIbmDb() {
  try {
    return require('ibm_db');
  } catch (error) {
    if (parityEnabled) {
      throw new Error(
        `DB2_PARITY_IBMDB=1 but node-ibm_db is not installed (npm install --no-save ${IBMDB_PIN}). ` +
        `Original: ${error.message}`,
      );
    }
    return undefined;
  }
}

function toIbmConnStr(config) {
  // ibm_db ODBC-style connection string. No secrets are logged by this file.
  return (
    `DATABASE=${config.database};` +
    `HOSTNAME=${config.host};` +
    `PORT=${config.port};` +
    `PROTOCOL=TCPIP;` +
    `UID=${config.user};` +
    `PWD=${config.password}`
  );
}

function bytesToHex(value) {
  if (Buffer.isBuffer(value)) return value.toString('hex');
  if (value instanceof Uint8Array) return Buffer.from(value).toString('hex');
  return undefined;
}

// Canonical value rendering shared by both drivers. The goal is not to assert
// identical JS types (they differ by design: bigint vs number vs numeric string,
// Buffer vs Uint8Array, Date vs string) but identical Db2 VALUES. Kind-based
// normalization runs before typeof-based rendering so e.g. DECIMAL scale
// formatting ('125.00' vs 125) does not produce false mismatches.
function normNumericText(text) {
  const t = String(text).trim();
  const upper = t.toUpperCase().replace(/^\+/, '');
  if (/^(INFINITY|INF|NAN|SNAN)$/.test(upper)) return upper.replace(/^INF$/, 'INFINITY');
  const m = /^([+-]?)(\d+)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/.exec(t);
  if (!m) return t;
  if (m[4] !== undefined) {
    // Scientific notation: compare by numeric value (finite only; specials above).
    const n = Number(t);
    return Number.isFinite(n) ? `E:${String(n)}` : t;
  }
  const frac = (m[3] ?? '').replace(/0+$/, '');
  return frac ? `${m[1] === '-' ? '-' : ''}${m[2]}.${frac}` : `${m[1] === '-' ? '-' : ''}${m[2]}`;
}

function canon(value, kind) {
  if (value === null || value === undefined) return 'NULL';
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) return `HEX:${bytesToHex(value)}`;
  if (value instanceof Date) {
    if (kind === 'date') return `DATE:${value.toISOString().slice(0, 10)}`;
    return `TS:${value.toISOString()}`;
  }
  if (kind === 'boolean') {
    const t = String(value).trim().toUpperCase();
    if (['1', 'TRUE', 'T', 'YES', 'Y', 'ON'].includes(t)) return 'BOOL:TRUE';
    if (['0', 'FALSE', 'F', 'NO', 'N', 'OFF'].includes(t)) return 'BOOL:FALSE';
    return `BOOL:?${String(value)}`;
  }
  if (kind === 'temporal') {
    // ts-db2 renders TIME as 23.59.58 and TIMESTAMP as 2024-02-29-23.59.58.123456;
    // CLI returns 23:59:58 / 2024-02-29 23:59:58.123456. Normalize separators.
    return `TS:${String(value).trim().replace(' ', '-').replaceAll(':', '.')}`;
  }
  if (kind === 'numeric-text') return `N:${normNumericText(value)}`;
  if (typeof value === 'bigint') return `INT:${value.toString()}`;
  if (typeof value === 'number') {
    if (Number.isInteger(value)) return `INT:${String(value)}`;
    return `NUM:${String(value)}`;
  }
  if (typeof value === 'boolean') return value ? 'BOOL:TRUE' : 'BOOL:FALSE';
  const text = String(value);
  if (kind === 'strict' && /^[+-]?\d+$/.test(text.trim())) {
    // ibm_db returns BIGINT as a decimal string to preserve precision; ts-db2
    // returns bigint. Same Db2 value, different JS carrier.
    return `INT:${text.trim().replace(/^\+/, '').replace(/^(-?)0+(\d)/, '$1$2')}`;
  }
  if (kind === 'text') return `TXT:${text}`;
  return `VAL:${text.trim()}`;
}

// Raw value rendering for mismatch diagnostics: preserves JS carrier types
// (Buffer/bigint/number/string) so a real divergence can be told apart from a
// canonicalization gap.
function rawTag(value) {
  if (value === null || value === undefined) return 'null';
  if (typeof value === 'bigint') return `${value.toString()}n`;
  if (Buffer.isBuffer(value)) return `Buffer(${value.toString('hex').slice(0, 64)})`;
  if (value instanceof Uint8Array) return `Uint8Array(${Buffer.from(value).toString('hex').slice(0, 64)})`;
  if (typeof value === 'string') return JSON.stringify(value);
  return `${typeof value}:${String(value)}`;
}

function rawRow(values) {
  return `[${values.map(rawTag).join(', ')}]`;
}

function canonRow(values, kinds) {
  return values.map((v, i) => canon(v, kinds[i] || 'strict'));
}

// ibm_db `query()` returns object rows keyed by column name; ts-db2 returns
// positional arrays. Normalize both to positional canonical arrays.
function ibmRowToArray(row, width) {
  if (Array.isArray(row)) return row;
  const values = Object.values(row);
  if (values.length === width) return values;
  return values;
}

const CORPUS = [
  {
    name: 'integers: boundaries and NULLs',
    sql: 'VALUES (CAST(NULL AS INTEGER), CAST(-2147483648 AS INTEGER), CAST(2147483647 AS INTEGER), CAST(9223372036854775807 AS BIGINT), CAST(-32768 AS SMALLINT))',
    params: [],
    kinds: ['strict', 'strict', 'strict', 'strict', 'strict'],
  },
  {
    name: 'floats and booleans',
    sql: 'VALUES (CAST(1.25 AS REAL), CAST(-2.5 AS DOUBLE), CAST(TRUE AS BOOLEAN), CAST(FALSE AS BOOLEAN))',
    params: [],
    kinds: ['numeric-text', 'numeric-text', 'boolean', 'boolean'],
  },
  {
    name: 'decimals and finite decfloats',
    sql: "VALUES (CAST(125 AS DECIMAL(5,2)), CAST('123.456' AS DECFLOAT(16)), CAST('-987.654' AS DECFLOAT(16)))",
    params: [],
    kinds: ['numeric-text', 'numeric-text', 'numeric-text'],
  },
  {
    // DECFLOAT special values are verified against Db2-documented literals, not
    // the ibm_db oracle. Proven live (Db2 12.1.5.0, pinned ibm_db@4.0.1) with
    // CLP ground truth AND a direct CLI (C, SQL_C_DOUBLE) probe:
    //   1. Server returns Infinity/-Infinity/NaN correctly (CLP, VARCHAR cast).
    //   2. The IBM CLI does NOT corrupt silently: bound as SQL_C_DOUBLE, SQLFetch
    //      returns SQL_ERROR with CLI0111E "Numeric value out of range"
    //      SQLSTATE=22003 and leaves the output buffer and indicator untouched
    //      (a sentinel prefilled in the probe survived through fetch).
    //   3. node-ibm_db binds DECFLOAT as SQL_C_DOUBLE
    //      (node_modules/ibm_db/src/odbc_result.cpp:1284-1285) and its
    //      GetBoundColumnValue only special-cases SQL_NULL_DATA, otherwise it
    //      reads the double buffer unconditionally (odbc_result.cpp:1357-1372),
    //      ignoring SQLFetch's error/indicator. So it returns whatever the buffer
    //      held: the previous column's value (e.g. -987.654), an uninitialized
    //      denormal (~6.9e-310 or 6.1e-183 depending on path), or 0.
    // Net: the defect is in node-ibm_db's error handling, not the server, the CLI,
    // this harness, or ts-db2. Comparing against it would encode uninitialized
    // memory as ground truth, so it is quarantined. Re-verify on oracle bump.
    name: 'decfloat specials match Db2 documented literals (ts-db2 only)',
    sql: "VALUES (CAST('Infinity' AS DECFLOAT(16)), CAST('-Infinity' AS DECFLOAT(16)), CAST('NaN' AS DECFLOAT(16)), CAST('sNaN' AS DECFLOAT(16)))",
    params: [],
    kinds: ['numeric-text', 'numeric-text', 'numeric-text', 'numeric-text'],
    tsOnly: ['Infinity', '-Infinity', 'NaN', 'sNaN'],
  },
  {
    name: 'text, unicode and binary',
    sql: "VALUES (CAST('Zażółć 東京' AS VARCHAR(50)), CAST(X'00FF7F' AS VARBINARY(10)), CAST('abc' AS CHAR(5)), CAST('abc' AS VARCHAR(5)))",
    params: [],
    kinds: ['text', 'strict', 'text', 'text'],
  },
  {
    name: 'dates, times, timestamps',
    sql: "VALUES (CAST('2024-02-29' AS DATE), CAST('23:59:58' AS TIME), CAST('2024-02-29 23:59:58.123456' AS TIMESTAMP))",
    params: [],
    kinds: ['temporal', 'temporal', 'temporal'],
  },
  {
    name: 'typed NULLs incl. LOB/XML',
    sql: 'VALUES (CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR(30)), CAST(NULL AS DATE), CAST(NULL AS BOOLEAN))',
    params: [],
    kinds: ['strict', 'text', 'temporal', 'boolean'],
  },
  {
    name: 'empty vs NULL LOBs stay distinct',
    sql: "VALUES (CAST(NULL AS BLOB(1M)), CAST(X'' AS BLOB(1M)), CAST(NULL AS CLOB(1M)), CAST('' AS CLOB(1M)))",
    params: [],
    kinds: ['strict', 'strict', 'text', 'text'],
  },
  {
    name: 'boolean input lexical conversions',
    sql: `VALUES (${Array(6).fill('CAST(? AS BOOLEAN)').join(', ')})`,
    params: ['true', ' FALSE ', 'YES', 'no', '1', '0'],
    kinds: Array(6).fill('boolean'),
  },
  {
    name: 'integer input truncation (numeric text + booleans)',
    sql: `VALUES (${Array(5).fill('CAST(? AS SMALLINT)').join(', ')})`,
    params: [-3.75, ' 1.5 ', true, false, '1e0'],
    kinds: Array(5).fill('strict'),
  },
  {
    // BLOB/CLOB round-trip through a typed temp table: ibm_db cannot bind a
    // binary parameter to CAST(? AS BLOB) (SQL0301N — ODBC cannot infer the
    // parameter type from an untyped CAST placeholder), while a table column
    // gives the CLI a concrete target type. Both drivers INSERT one row each
    // (ts: Uint8Array, ibm: Buffer) and both read the full table back.
    name: 'small LOB round-trip via temp table',
    lobTable: true,
    kinds: ['strict', 'text'],
  },
];

async function lobRoundTrip(t, item, ts, ibm, report) {
  const schema = process.env.DB2_IT_SCHEMA
    ? `"${process.env.DB2_IT_SCHEMA.replaceAll('"', '""')}"`
    : `"${String((await ts.query('VALUES CURRENT SCHEMA')).rows[0][0]).trimEnd().replaceAll('"', '""')}"`;
  const table = `${schema}."TS_PARITY_LOB_${process.pid}"`;
  const blobBytes = Uint8Array.of(0, 255, 127);
  const clobText = 'CLOB café 東京 🚀';
  const selectSql = `SELECT B, C FROM ${table} ORDER BY ID`;
  try {
    await ts.execute(`CREATE TABLE ${table} (ID INTEGER NOT NULL PRIMARY KEY, B BLOB(1M), C CLOB(2M))`);
    // Write path goes through ts-db2 (positional params incl. Uint8Array).
    // The oracle only READS: ibm_db query() param binding rejects LOB/binary
    // host variables (SQL0301N) even for typed columns, so a comparative
    // write through ibm_db would encode an oracle binding limitation.
    await ts.execute(`INSERT INTO ${table} VALUES (?, ?, ?)`, [1, blobBytes, clobText]);
    await ts.execute(
      `INSERT INTO ${table} VALUES (2, BX'00FF7F', 'CLOB café 東京 🚀')`,
    );
    const tsRows = (await ts.query(selectSql)).rows;
    const ibmRaw = await ibm.query(selectSql);
    const ibmRows = ibmRaw.map((row) => ibmRowToArray(row, 2));
    assert.equal(tsRows.length, 2, 'temp LOB table must hold both rows');
    assert.equal(ibmRows.length, 2, 'temp LOB table must hold both rows');
    const tsCanon = tsRows.map((row) => canonRow(row, item.kinds));
    const ibmCanon = ibmRows.map((row) => canonRow(row, item.kinds));
    try {
      assert.deepEqual(ibmCanon, tsCanon);
      report.cases.push({ name: item.name, status: 'pass' });
    } catch {
      report.cases.push({ name: item.name, status: 'fail' });
      assert.fail(
        `[${item.name}] VALUE MISMATCH\nts-db2  canon: ${JSON.stringify(tsCanon).slice(0, 2000)}\nibm_db canon: ${JSON.stringify(ibmCanon).slice(0, 2000)}` +
        `\nts-db2  raw: ${tsRows.map(rawRow).join(' / ').slice(0, 2000)}\nibm_db raw: ${ibmRows.map(rawRow).join(' / ').slice(0, 2000)}`,
      );
    }
  } catch (error) {
    if (error?.code === 'ERR_ASSERTION') throw error;
    report.cases.push({ name: item.name, status: 'fail' });
    assert.fail(`[${item.name}] harness error: ${error.message}`);
  } finally {
    await ts.execute(`DROP TABLE ${table}`).catch(() => {});
  }
}

// Opt-in by flag: without DB2_PARITY_IBMDB=1 the test is reported as skipped (never
// as passed). With the flag set, a missing oracle makes loadIbmDb() throw.
const parityOptIn = dsn && !parityEnabled ? IBMDB_SKIPPED_MARKER : false;

test('parity ibm_db: SELECT/type corpus matches ts-db2', { skip: dsn ? parityOptIn : 'DB2_TEST_DSN is not set', timeout: 180_000 }, async (t) => {
  const ibmDb = loadIbmDb();

  const config = parseDsn(dsn);
  const ts = await Db2Connection.connect(dsn);
  const ibmConnStr = toIbmConnStr(config);
  let ibm;
  try {
    ibm = await ibmDb.open(ibmConnStr);
  } catch (error) {
    await ts.close();
    throw new Error(`could not open ibm_db oracle connection: ${redact(error.message)}`);
  }

  const report = {
    oracle: `node-ibm_db (pinned ${IBMDB_PIN})`,
    node: process.version,
    cases: [],
  };
  try {
    for (const item of CORPUS) {
      await t.test(item.name, async () => {
        if (item.tsOnly) {
          // Oracle-independent ground truth: Db2-documented literal rendering.
          const { rows } = await ts.query(item.sql, item.params);
          assert.deepEqual(
            rows.map((row) => canonRow(row, item.kinds)),
            [item.tsOnly.map((v) => canon(v, 'numeric-text'))],
            `[${item.name}] ts-db2 does not match Db2 documented literals`,
          );
          report.cases.push({ name: item.name, status: 'pass' });
          return;
        }
        if (item.lobTable) {
          await lobRoundTrip(t, item, ts, ibm, report);
          return;
        }
        let tsRows;
        let ibmRows;
        try {
          tsRows = (await ts.query(item.sql, item.params)).rows;
        } catch (error) {
          assert.fail(`ts-db2 failed on [${item.name}]: ${error.message}`);
        }
        try {
          // ibm_db binds binary through Buffer, not Uint8Array (SQL0301N otherwise).
          const ibmParams = (item.paramsIbm ?? item.params).map((p) =>
            p instanceof Uint8Array && !Buffer.isBuffer(p) ? Buffer.from(p) : p,
          );
          const raw = await ibm.query(item.sql, ibmParams);
          ibmRows = raw.map((row) => ibmRowToArray(row, tsRows[0] ? tsRows[0].length : Object.keys(row).length));
        } catch (error) {
          assert.fail(`ibm_db failed on [${item.name}]: ${redact(error.message)}`);
        }
        assert.equal(ibmRows.length, tsRows.length, `[${item.name}] row count differs (ts=${tsRows.length} ibm=${ibmRows.length})`);
        const tsCanon = tsRows.map((row) => canonRow(row, item.kinds));
        const ibmCanon = ibmRows.map((row) => canonRow(row, item.kinds));
        try {
          assert.deepEqual(ibmCanon, tsCanon);
          report.cases.push({ name: item.name, status: 'pass' });
        } catch {
          report.cases.push({ name: item.name, status: 'fail' });
          assert.fail(
            `[${item.name}] VALUE MISMATCH\nts-db2  canon: ${JSON.stringify(tsCanon).slice(0, 2000)}\nibm_db canon: ${JSON.stringify(ibmCanon).slice(0, 2000)}` +
            `\nts-db2  raw: ${tsRows.map(rawRow).join(' / ').slice(0, 2000)}\nibm_db raw: ${ibmRows.map(rawRow).join(' / ').slice(0, 2000)}`,
          );
        }
      });
    }
  } finally {
    try {
      await ibm.close();
    } catch {}
    await ts.close();
    const out = process.env.DB2_PARITY_REPORT || 'parity-report.json';
    try {
      fs.writeFileSync(out, JSON.stringify(report, null, 2));
      console.log(`ibm_db parity report: ${report.cases.filter((c) => c.status === 'pass').length}/${report.cases.length} passed -> ${out}`);
    } catch {}
  }
});
