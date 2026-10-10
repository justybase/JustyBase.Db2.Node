#!/usr/bin/env node
'use strict';
require('../tests/load-env.cjs');

// Long-lived TS driver resource soak. Tables and procedure must be seeded by
// the companion fixture setup; this worker only deletes destination rows.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Db2Connection } = require('../dist/cjs/network/session.js');
const { Db2Pool } = require('../dist/cjs/network/pool.js');

const [sourceTable, destinationTable, lobSource, lobDestination, procedureName] = process.argv.slice(2);
const suffix = /^SO_[A-F0-9]{12}_(?:N|ND|X|XD)$/u;
if (![sourceTable, destinationTable, lobSource, lobDestination].every(v => suffix.test(v)) || !/^SO_[A-F0-9]{12}_P$/u.test(procedureName ?? '')) {
  console.error('usage: node resource-soak.cjs SO_<12HEX>_N SO_<12HEX>_ND SO_<12HEX>_X SO_<12HEX>_XD SO_<12HEX>_P');
  process.exit(2);
}
const dsn = process.env.DB2_TEST_DSN;
if (!dsn) { console.error('Set DB2_TEST_DSN'); process.exit(2); }

const cycles = 110;
const warmups = 10;
const durationMs = Number(process.env.DB2_SOAK_DURATION_SECONDS ?? 0) * 1000;
if (!Number.isSafeInteger(durationMs) || durationMs < 0 || durationMs > 3600000) throw new RangeError('Invalid soak duration (0..3600 seconds)');
const fdDir = '/proc/self/fd';
const ownSpoolRoot = os.tmpdir();
const spoolPrefix = 'ts-db2-import-';
function spoolPaths() {
  try { return fs.readdirSync(ownSpoolRoot).filter(n => n.startsWith(spoolPrefix)).sort(); }
  catch { return []; }
}
function fdCount() {
  try { return fs.readdirSync(fdDir).length; } catch { return null; }
}
function socketFdCount() {
  return fs.readdirSync(fdDir).filter(name => {
    try { return fs.readlinkSync(path.join(fdDir, name)).startsWith('socket:['); }
    catch { return false; } // The enumeration's own temporary FD has closed.
  }).length;
}
function sample(cycle, phase) {
  const m = process.memoryUsage();
  assert(m.rss <= 512 * 1024 * 1024, 'RSS exceeds the 512 MiB qualification budget');
  const value = { type: 'resource-sample', language: 'typescript', cycle, phase,
    rssBytes: m.rss, heapUsedBytes: m.heapUsed, fdCount: fdCount(), socketFdCount: socketFdCount(), ownSpoolCount: spoolPaths().length };
  console.log(JSON.stringify(value));
  return value;
}
function checkSpools(expected, where) {
  const current = spoolPaths();
  assert.deepEqual(current, expected, `driver-owned import spool directories changed at ${where}`);
}

async function* rowsOf(stream) {
  for await (const row of stream) yield row.values;
}
function bytesForLob(id, length, salt = 11) {
  const b = Buffer.allocUnsafe(length);
  for (let i = 0; i < length; i++) b[i] = (i * 37 + salt) % 256;
  return b;
}
async function verifyLobs(source, destination, sourceName, destinationName) {
  const expected = await source.query(`SELECT ID, B, C, G, X FROM ${sourceName} ORDER BY ID`);
  const actual = await destination.query(`SELECT ID, B, C, G, X FROM ${destinationName} ORDER BY ID`);
  assert.equal(expected.rows.length, 3); assert.equal(actual.rows.length, 3);
  assert.deepEqual(actual.rows, expected.rows, 'destination LOB rows differ from source');
  assert.equal(Number(actual.rows[0][0]), 1);
  assert.deepEqual(Buffer.from(actual.rows[0][1]), bytesForLob(1, 2500123));
  assert.equal(actual.rows[0][2], 'Zażółć 東京 🚀  '.repeat(10000));
  assert.equal(actual.rows[0][3], '漢字 Zażółć 🚀  '.repeat(10000));
  assert.ok(String(actual.rows[0][4]).includes('x'.repeat(180000)), 'large XML body must be preserved');
  assert.ok(String(actual.rows[0][4]).includes('東京 🚀'), 'XML Unicode must be preserved');
  assert.deepEqual(Buffer.from(actual.rows[1][1]), Buffer.alloc(0));
  assert.equal(actual.rows[1][2], ''); assert.equal(actual.rows[1][3], '');
  assert.equal(Number(actual.rows[2][0]), 3);
  assert.deepEqual(actual.rows[2].slice(1), [null, null, null, null]);
}

(async () => {
  // Initialize Node's measurement machinery before recording the process baseline.
  fdCount(); spoolPaths();
  const preConnectionFd = fdCount();
  const initialSocketFds = socketFdCount();
  const source = await Db2Connection.connect(dsn);
  const dest = await Db2Connection.connect(dsn);
  const pool = new Db2Pool(dsn, { maxConnections: 1 });
  const initialSpools = spoolPaths();
  let baseline;
  let primaryError;
  try {
    await pool.query('VALUES 30');
    assert.equal(pool.connectionCount, 1); assert.equal(pool.idleCount, 1);
    let retained;
    let measuredStart;
    let completedCycles = 0;
    for (let cycle = 1; cycle <= cycles || (measuredStart !== undefined && performance.now() - measuredStart < durationMs); cycle++) {
      const all = await source.query(`SELECT ID, V FROM ${sourceTable} ORDER BY ID`);
      assert.equal(all.rows.length, 1000);
      for (let i = 0; i < 1000; i++) {
        assert.equal(Number(all.rows[i][0]), i + 1);
        assert.equal(all.rows[i][1], `Zażółć 東京 ${i + 1}  `);
      }
      if (!retained) retained = all.rows[0];
      else assert.deepEqual(retained, [1, 'Zażółć 東京 1  '], 'previous query result row was mutated');
      assert.deepEqual((await source.query(`SELECT ID,V FROM ${sourceTable} WHERE ID=999`)).rows, [[999, 'Zażółć 東京 999  ']]);
      assert.deepEqual(retained, [1, 'Zażółć 東京 1  '], 'retained row changed during a different query');

      for (const queryStream of [source.queryStream(`SELECT ID, V FROM ${sourceTable} ORDER BY ID`), pool.queryStream(`SELECT ID, V FROM ${sourceTable} ORDER BY ID`)]) {
        const it = queryStream[Symbol.asyncIterator]();
        const first = await it.next();
        assert.equal(Number(first.value.values[0]), 1);
        await it.return();
        assert.deepEqual((await source.query('VALUES 31')).rows, [[31]]);
        assert.equal(pool.connectionCount, 1); assert.equal(pool.idleCount, 1);
      }

      await dest.execute(`DELETE FROM ${destinationTable}`);
      const atomicProgress = await dest.importRows(rowsOf(source.queryStream(`SELECT ID, V FROM ${sourceTable} ORDER BY ID`)),
        { table: destinationTable, columns: ['ID', 'V'] }, { policy: 'atomic', maxBatchRows: 127 + cycle % 3, maxBatchBytes: 65536 });
      assert.equal(atomicProgress.sourceRowsRead, 1000); assert.equal(atomicProgress.committedRows, 1000);
      assert.deepEqual((await dest.query(`SELECT ID, V FROM ${destinationTable} ORDER BY ID`)).rows, all.rows);

      await dest.execute(`DELETE FROM ${destinationTable}`);
      let lastProgress;
      const checkpointProgress = await dest.importRows(rowsOf(source.queryStream(`SELECT ID, V FROM ${sourceTable} ORDER BY ID`)),
        { table: destinationTable, columns: ['ID', 'V'] }, { policy: 'commitEveryRows', maxBatchRows: 127 + cycle % 3, maxBatchBytes: 65536, onProgress: p => { lastProgress = p; } });
      assert.equal(checkpointProgress.sourceRowsRead, 1000); assert.equal(checkpointProgress.committedRows, 1000);
      assert.equal(lastProgress.sourceRowsRead, 1000); assert.ok(lastProgress.committedRows < 1000, "progress callback precedes the final checkpoint acknowledgement");
      assert.deepEqual((await dest.query(`SELECT ID, V FROM ${destinationTable} ORDER BY ID`)).rows, all.rows);

      if (cycle % 10 === 0) {
        await dest.execute(`DELETE FROM ${lobDestination}`);
        const lobAtomic = await dest.importRows(rowsOf(source.queryStream(`SELECT ID, B, C, G, X FROM ${lobSource} ORDER BY ID`)),
          { table: lobDestination, columns: ['ID', 'B', 'C', 'G', 'X'] },
          { policy: 'atomic', maxBatchRows: 2, maxBatchBytes: 65536 });
        assert.equal(lobAtomic.sourceRowsRead, 3); assert.equal(lobAtomic.committedRows, 3);
        await verifyLobs(source, dest, lobSource, lobDestination);
        await dest.execute(`DELETE FROM ${lobDestination}`);
        let lobCheckpointSnapshot;
        const lobCheckpoint = await dest.importRows(rowsOf(source.queryStream(`SELECT ID, B, C, G, X FROM ${lobSource} ORDER BY ID`)),
          { table: lobDestination, columns: ['ID', 'B', 'C', 'G', 'X'] },
          { policy: 'commitEveryRows', maxBatchRows: 2, maxBatchBytes: 65536, onProgress: p => { lobCheckpointSnapshot = p; } });
        assert.equal(lobCheckpoint.sourceRowsRead, 3); assert.equal(lobCheckpoint.committedRows, 3);
        assert.equal(lobCheckpointSnapshot.sourceRowsRead, 3); assert.ok(lobCheckpointSnapshot.committedRows < 3, "progress callback precedes the final checkpoint acknowledgement");
        await verifyLobs(source, dest, lobSource, lobDestination);
        // Consume a single large locator only partially, then abandon its reader.
        const lobRows = source.queryStream(`SELECT B FROM ${lobSource} WHERE ID = 1`);
        const iter = lobRows[Symbol.asyncIterator]();
        const lobRow = await iter.next();
        const value = lobRow.value.values[0];
        assert.equal(value.kind, 'binary-lob');
        const chunkIter = value.stream()[Symbol.asyncIterator]();
        assert.equal((await chunkIter.next()).done, false);
        await chunkIter.return();
        await iter.return();
        assert.deepEqual((await source.query('VALUES 32')).rows, [[32]]);
        await verifyLobs(source, dest, lobSource, lobDestination);
      }

      const reader = await dest.executeProcedureStream(`CALL ${procedureName}(?)`, [{ value: 0, direction: 'output' }]);
      assert.equal(reader.resultSetIndex, 0);
      assert.deepEqual(await Array.fromAsync(reader.rows()), [[17]]);
      assert.equal(await reader.nextResultSet(), true);
      assert.equal(reader.resultSetIndex, 1);
      assert.deepEqual(await Array.fromAsync(reader.rows()), []);
      assert.equal(await reader.nextResultSet(), true);
      assert.equal(reader.resultSetIndex, 2);
      assert.deepEqual(await Array.fromAsync(reader.rows()), [[19]]);
      assert.equal(await reader.nextResultSet(), false);
      await reader.finish();
      assert.equal(Number(reader.outputParameters[0]), 23);
      if (cycle === warmups) { baseline = sample(cycle, 'post-warmup'); checkSpools(initialSpools, 'post-warmup'); measuredStart = performance.now(); }
      if (cycle > warmups && cycle % 10 === 0) { const current = sample(cycle, 'measured'); assert.ok(current.fdCount <= baseline.fdCount, 'FD count grew after warmup'); assert.equal(current.socketFdCount, baseline.socketFdCount); checkSpools(initialSpools, `cycle ${cycle}`); }
      assert(process.memoryUsage().rss <= 512 * 1024 * 1024, 'RSS exceeds the 512 MiB qualification budget');
      completedCycles = cycle;
    }
    console.log(JSON.stringify({ type: 'resource-duration', language: 'typescript', elapsedMs: performance.now() - measuredStart, measuredCycles: completedCycles - warmups }));
  } catch (error) {
    primaryError = error;
  } finally {
    const closeResults = await Promise.allSettled([source.close(), dest.close(), pool.close()]);
    const closeErrors = closeResults.filter(r => r.status === 'rejected').map(r => r.reason);
    if (primaryError && closeErrors.length) primaryError = new AggregateError([primaryError, ...closeErrors], 'resource soak failed and cleanup also failed');
    else if (!primaryError && closeErrors.length) primaryError = new AggregateError(closeErrors, 'resource soak cleanup failed');
  }
  if (primaryError) throw primaryError;
  // libuv processes socket-close callbacks on the next event-loop turn.
  await new Promise(resolve => setImmediate(resolve));
  const final = sample(cycles, 'final-closed');
  assert.equal(final.socketFdCount, initialSocketFds, 'driver sockets remain after close');
  assert.ok(final.fdCount <= baseline.fdCount, 'FDs remain above warmed baseline');
  checkSpools(initialSpools, 'final');
  if (!baseline) throw new Error('warmup baseline was not recorded');
  console.log(JSON.stringify({ type: 'resource-fd-summary', language: 'typescript', preConnectionFd, postWarmupFd: baseline.fdCount,
    finalClosedFd: final.fdCount, deltaFromPreConnection: preConnectionFd === null || final.fdCount === null ? null : final.fdCount - preConnectionFd,
    deltaFromPostWarmup: baseline.fdCount === null || final.fdCount === null ? null : final.fdCount - baseline.fdCount }));
})().catch(error => {
  console.error(`resource soak failed: ${error?.stack || error}`);
  const secrets = [dsn, process.env.DB2_LIVE_TEST_PASSWORD].filter(Boolean);
  for (let cause = error, depth = 0; cause && depth < 8; cause = cause.cause, depth++) {
    const message = secrets.reduce((text, secret) => text.split(secret).join('[REDACTED]'), String(cause.message ?? ''));
    console.error(JSON.stringify({ diagnosticDepth: depth, name: cause.name, category: cause.category, sqlCode: cause.sqlCode, sqlState: cause.sqlState, message, progress: cause.progress }));
  }
  process.exitCode = 1;
});
