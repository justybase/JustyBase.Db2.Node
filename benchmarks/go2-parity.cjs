#!/usr/bin/env node
'use strict';
// Go2Parity: TypeScript port of go-db2/benchmarks/suite/{conformance,perf}.go.
// Baseline in the report is go-db2; this harness emits the typescript column.
// Contract matches cs-db2/benchmarks/Go2Parity/Program.cs and
// rust-db2/examples/go2_parity.rs; aggregated by run-db2-go2-benchmark.cjs.
//
// Usage: DB2_TEST_DSN=... node benchmarks/go2-parity.cjs [--warmups 3] [--samples 7] [--fast] [--seed-only] [--no-seed]
require('../tests/load-env.cjs');
const crypto = require('node:crypto');
const { Db2Connection } = require('../dist/cjs/network/session.js');

function parseArgs(argv) {
  const o = { warmups: 3, samples: 7, fast: false, seedOnly: false, noSeed: false };
  for (let i = 2; i < argv.length; i++) {
    switch (argv[i]) {
      case '--warmups': o.warmups = Number(argv[++i]); break;
      case '--samples': o.samples = Number(argv[++i]); break;
      case '--fast': o.fast = true; break;
      case '--seed-only': o.seedOnly = true; break;
      case '--no-seed': o.noSeed = true; break;
      default: throw new Error(`unknown argument ${argv[i]}`);
    }
  }
  if (o.fast) { o.warmups = Math.min(o.warmups, 1); o.samples = Math.min(o.samples, 3); }
  o.iters = { ping: o.fast ? 5 : 500, simple: o.fast ? 5 : 1000, fetch: o.fast ? 2 : 50, fetch100k: o.fast ? 2 : 10, insert: o.fast ? 3 : 300, blob: o.fast ? 2 : 50 };
  return o;
}

const CREATE_CONF = `CREATE TABLE BENCH_CONF_TYPES (
  ID INT NOT NULL PRIMARY KEY, C_SMALLINT SMALLINT, C_INT INTEGER, C_BIGINT BIGINT,
  C_REAL REAL, C_DOUBLE DOUBLE, C_DECIMAL DECIMAL(15,2), C_VARCHAR VARCHAR(100),
  C_DECFLOAT DECFLOAT(34), C_BLOB BLOB(64K), C_CLOB CLOB(64K), C_VARGRAPHIC VARGRAPHIC(100))`;
const INSERT_CONF = `INSERT INTO BENCH_CONF_TYPES (ID, C_SMALLINT, C_INT, C_BIGINT, C_REAL, C_DOUBLE, C_DECIMAL,
  C_VARCHAR, C_DECFLOAT, C_BLOB, C_CLOB, C_VARGRAPHIC) VALUES (
  1, 32767, 2147483647, 9223372036854775807, 3.1415, 2.718281828459, 1234567.89,
  'Universal Db2 Conformance Suite', DECFLOAT('12345678901234567890.12345678901234', 34),
  ?, ?, 'Unicode / \u65e5\u672c\u8a9e / Portugu\u00eas')`;
const CLOB_SEED = 'Pure-Go-Db2-CLOB-Test-Payload-'.repeat(40);
const BLOB_SEED = Buffer.alloc(1024);
for (let i = 0; i < 256; i++) Buffer.from([0xde, 0xad, 0xbe, 0xef]).copy(BLOB_SEED, i * 4);
const PERF_BLOB = Buffer.alloc(Buffer.from('PureGoDb2DriverBenchmarkPayload1234567890!').length * 12000);
{ const u = Buffer.from('PureGoDb2DriverBenchmarkPayload1234567890!'); for (let i = 0; i < 12000; i++) u.copy(PERF_BLOB, i * u.length); }

const emit = (p) => process.stdout.write(JSON.stringify(p) + '\n');
const num = (v) => (typeof v === 'bigint' ? Number(v) : Number(v));
// Exact integer rendering: BIGINT exceeds Number.MAX_SAFE_INTEGER, so never
// route integers through Number() (9223372036854775807 -> ...6000).
const intStr = (v) => (typeof v === 'bigint' ? v.toString() : (typeof v === 'number' ? String(Math.trunc(v)) : str(v)));
const str = (v) => (Buffer.isBuffer(v) ? v.toString('utf8') : (v === null || v === undefined ? '' : String(v))).trim();

async function seed(db) {
  for (const ddl of ['DROP TABLE BENCH_CONF_TYPES', 'DROP TABLE BENCH_PERF_ROWS', 'DROP TABLE BENCH_PERF_ROWS_100K', 'DROP TABLE BENCH_PERF_INSERT', 'DROP TABLE BENCH_PERF_LOB']) {
    try { await db.execute(ddl); } catch { /* absent */ }
  }
  await db.execute(CREATE_CONF);
  await db.execute('CREATE TABLE BENCH_PERF_ROWS (ID INT NOT NULL PRIMARY KEY, NAME VARCHAR(50), VAL DOUBLE)');
  await db.execute('CREATE TABLE BENCH_PERF_ROWS_100K (ID INT NOT NULL PRIMARY KEY, NAME VARCHAR(50), VAL DOUBLE)');
  await db.execute('CREATE TABLE BENCH_PERF_INSERT (ID INT NOT NULL PRIMARY KEY, NAME VARCHAR(50), VAL DOUBLE)');
  await db.execute('CREATE TABLE BENCH_PERF_LOB (ID INT NOT NULL PRIMARY KEY, DATA BLOB(1M))');
  await db.execute(INSERT_CONF, [BLOB_SEED, CLOB_SEED]);
  const stmt = await db.prepare('INSERT INTO BENCH_PERF_ROWS (ID, NAME, VAL) VALUES (?, ?, ?)');
  try {
    for (let i = 1; i <= 1000; i++) await stmt.execute([i, `Customer-Record-${String(i).padStart(5, '0')}`, i * 1.5]);
  } finally { stmt.close(); }
  // 100k rows via recursive CTE (server-side, ~0.6s); cross-join ROW_NUMBER takes ~2min.
  await db.execute(`INSERT INTO BENCH_PERF_ROWS_100K (ID, NAME, VAL)
    WITH n(i) AS (VALUES (1) UNION ALL SELECT i+1 FROM n WHERE i < 100000)
    SELECT i, 'Customer-Record-' || CHAR(i), DOUBLE(i)*1.5 FROM n`);
  await db.execute('INSERT INTO BENCH_PERF_LOB (ID, DATA) VALUES (1, ?)', [PERF_BLOB]);
}

async function conformanceRow(db, name, sql, fmt) {
  try {
    const r = await db.query(sql);
    return { name, output: fmt(r.rows[0]), passed: true, diff: null };
  } catch (e) { return { name, output: `ERR: ${e.constructor.name}`, passed: false, diff: String(e.message || e).slice(0, 300) }; }
}

async function perf(db, scenario, iters, o, fn) {
  for (let w = 0; w < o.warmups; w++) await fn();
  const samples = [];
  for (let i = 0; i < o.samples; i++) {
    if (global.gc) global.gc();
    const memBefore = process.memoryUsage().heapUsed;
    const t0 = process.hrtime.bigint();
    for (let k = 0; k < iters; k++) await fn();
    const ms = Number(process.hrtime.bigint() - t0) / 1e6 / iters;
    const alloc = Math.max(0, process.memoryUsage().heapUsed - memBefore) / iters;
    samples.push({ ms, alloc });
    emit({ language: 'typescript', suite: 'perf', scenario, mode: 'native-materialized', warmup: false, iteration: i + 1, ops: iters, elapsedMs: ms, allocatedBytes: Math.round(alloc) });
  }
  samples.sort((a, b) => a.ms - b.ms);
  const allocs = samples.map((s) => s.alloc).sort((a, b) => a - b);
  emit({ language: 'typescript', suite: 'perf', scenario, mode: 'native-materialized', summary: true, ops: iters, medianMs: samples[Math.floor(samples.length / 2)].ms, minMs: samples[0].ms, maxMs: samples[samples.length - 1].ms, medianAllocatedBytes: Math.round(allocs[Math.floor(allocs.length / 2)]), peakRssBytes: process.resourceUsage().maxRSS * 1024 });
}

(async () => {
  const o = parseArgs(process.argv);
  const dsn = process.env.DB2_TEST_DSN;
  if (!dsn) throw new Error('Set DB2_TEST_DSN');
  const db = await Db2Connection.connect(dsn);
  try {
    if (!o.noSeed) await seed(db);
    if (o.seedOnly) return;
    emit({ language: 'typescript', suite: 'hello', dsn: 'REDACTED' });
    const rows = [];
    rows.push(await conformanceRow(db, 'Basic Scalar (Int, String, Float)', "SELECT 42, 'Pure Go Db2 Driver', 123.45 FROM SYSIBM.SYSDUMMY1",
      (r) => `i=${intStr(r[0])}, s="${str(r[1])}", f=${num(r[2]).toFixed(2)}`));
    rows.push(await conformanceRow(db, 'Null Handling (sql.Null*)', 'SELECT CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR(50)) FROM SYSIBM.SYSDUMMY1',
      (r) => `ni.Valid=${r[0] !== null}, ns.Valid=${r[1] !== null}`));
    rows.push(await conformanceRow(db, 'Temporal (Current Date Format)', "SELECT VARCHAR_FORMAT(CURRENT DATE, 'YYYY-MM-DD') FROM SYSIBM.SYSDUMMY1",
      (r) => str(r[0])));
    rows.push(await conformanceRow(db, 'Numeric Precision (Smallint, Int, Bigint)', 'SELECT C_SMALLINT, C_INT, C_BIGINT FROM BENCH_CONF_TYPES WHERE ID = 1',
      (r) => `s=${intStr(r[0])}, i=${intStr(r[1])}, b=${intStr(r[2])}`));
    rows.push(await conformanceRow(db, 'High Precision Decimal & DECFLOAT', 'SELECT VARCHAR(C_DECIMAL), VARCHAR(C_DECFLOAT) FROM BENCH_CONF_TYPES WHERE ID = 1',
      (r) => `dec=${str(r[0])}, decfloat=${str(r[1])}`));
    rows.push(await (async () => {
      try {
        const expected = crypto.createHash('sha256').update(BLOB_SEED).digest('hex');
        const r = await db.query('SELECT C_BLOB FROM BENCH_CONF_TYPES WHERE ID = 1');
        const actual = crypto.createHash('sha256').update(Buffer.from(r.rows[0][0])).digest('hex');
        return actual === expected
          ? { name: 'BLOB Binary Integrity (SHA-256)', output: actual, passed: true, diff: null }
          : { name: 'BLOB Binary Integrity (SHA-256)', output: actual, passed: false, diff: `Expected hash ${expected}, got ${actual}` };
      } catch (e) { return { name: 'BLOB Binary Integrity (SHA-256)', output: `ERR: ${e.constructor.name}`, passed: false, diff: String(e.message || e).slice(0, 300) }; }
    })());
    rows.push(await conformanceRow(db, 'CLOB Text Streaming', 'SELECT LENGTH(C_CLOB), SUBSTR(C_CLOB, 1, 30) FROM BENCH_CONF_TYPES WHERE ID = 1',
      (r) => `len=${intStr(r[0])}, prefix=${str(r[1])}`));
    rows.push(await conformanceRow(db, 'VARGRAPHIC International Strings', 'SELECT C_VARGRAPHIC FROM BENCH_CONF_TYPES WHERE ID = 1',
      (r) => str(r[0])));
    for (const r of rows) emit({ language: 'typescript', suite: 'conformance', scenario: r.name, output: r.output, passed: r.passed, diff: r.diff });

    await perf(db, 'Ping (Round-trip Latency)', o.iters.ping, o, () => db.ping());
    await perf(db, 'Simple Single-Row SELECT', o.iters.simple, o, async () => {
      const r = await db.query('SELECT 1 FROM SYSIBM.SYSDUMMY1');
      if (num(r.rows[0][0]) !== 1) throw new Error('expected 1');
    });
    await perf(db, 'Fetch 1,000 Rows Scan', o.iters.fetch, o, async () => {
      const r = await db.query('SELECT ID, NAME, VAL FROM BENCH_PERF_ROWS');
      if (r.rows.length !== 1000) throw new Error(`expected 1000 rows, got ${r.rows.length}`);
      for (const row of r.rows) { num(row[0]); str(row[1]); num(row[2]); }
    });
    await perf(db, 'Fetch 100,000 Rows Scan', o.iters.fetch100k, o, async () => {
      const r = await db.query('SELECT ID, NAME, VAL FROM BENCH_PERF_ROWS_100K');
      if (r.rows.length !== 100000) throw new Error(`expected 100000 rows, got ${r.rows.length}`);
      for (const row of r.rows) { num(row[0]); str(row[1]); num(row[2]); }
    });
    await perf(db, 'Prepared Statement INSERT', o.iters.insert, o, async () => {
      await db.execute('DELETE FROM BENCH_PERF_INSERT');
      const stmt = await db.prepare('INSERT INTO BENCH_PERF_INSERT (ID, NAME, VAL) VALUES (?, ?, ?)');
      try { for (let i = 1; i <= 10; i++) await stmt.execute([i, 'test_name', i * 2.5]); }
      finally { stmt.close(); }
    });
    await perf(db, 'Read 500KB BLOB Payload', o.iters.blob, o, async () => {
      const r = await db.query('SELECT DATA FROM BENCH_PERF_LOB WHERE ID = 1');
      Buffer.from(r.rows[0][0]);
    });
    if (rows.some((r) => !r.passed)) process.exitCode = 1;
  } finally { await db.close(); }
})().catch((e) => { console.error(e); process.exitCode = 1; });
