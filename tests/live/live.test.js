require('../load-env.cjs');
const test = require('node:test');
const assert = require('node:assert/strict');
const { createHash, randomUUID } = require('node:crypto');
const { spawnSync } = require('node:child_process');
const path = require('node:path');
const { parseDsn } = require('../../dist/cjs/config.js');
const { Db2BatchAbortError, Db2BatchExecutionError, Db2ServerError } = require('../../dist/cjs/errors.js');
const { Db2Connection } = require('../../dist/cjs/network/session.js');
const { Db2Pool } = require('../../dist/cjs/network/pool.js');
const { open } = require('../../dist/cjs/ibmDbCompat.js');
const { fixtureSchema } = require('./fixture-schema.cjs');

const dsn = process.env.DB2_TEST_DSN;
const boundedProcedureOptions = { timeoutMs: 15_000, cancelCleanupTimeoutMs: 5_000 };
// Optional independent oracle. The harness that qualifies several drivers together
// (db2-conformance) supplies it: DB2_IBM_IMPORT_ORACLE names an executable that accepts
// `--verify-ts-import <table>...` and prints one JSON line with the rows the official IBM
// provider reads back. When it is not set, only the IBM cross-check is skipped (the
// marker below is printed so a qualification run can refuse that); every driver-side
// assertion still runs. This driver never depends on any other driver repository.
const ibmImportOracle = process.env.DB2_IBM_IMPORT_ORACLE;
const IBM_ORACLE_SKIPPED_MARKER = 'IBM import oracle verification skipped: DB2_IBM_IMPORT_ORACLE is not set';

function readTablesThroughOfficialIbmProvider(tableNames) {
  const result = spawnSync(ibmImportOracle, ['--verify-ts-import', ...tableNames], {
    encoding: 'utf8',
    timeout: 90_000,
    maxBuffer: 2 * 1024 * 1024,
    env: process.env,
  });
  if (result.error || result.status !== 0) {
    // Keep the tail of both streams so a transient or real build failure is diagnosable
    // instead of only "build failed".
    const stdoutTail = (result.stdout || '').trim().split(/\r?\n/u).slice(-20).join('\n');
    const stderrTail = (result.stderr || '').trim().split(/\r?\n/u).slice(-20).join('\n');
    throw new Error(`Official IBM import oracle failed (exit ${result.status ?? 'unavailable'}):\n${stderrTail}\n${stdoutTail}`);
  }
  const jsonLine = result.stdout.trim().split(/\r?\n/u).at(-1);
  assert.ok(jsonLine, 'official IBM import oracle must return JSON rows');
  return JSON.parse(jsonLine);
}

async function dropFixtureProcedure(procedure) {
  const cleanup = await Db2Connection.connect(dsn);
  try {
    await cleanup.execute(`DROP PROCEDURE ${await fixtureSchema(cleanup)}.${procedure}`, undefined, boundedProcedureOptions);
  } finally {
    await cleanup.close();
  }
}

async function dropFixtureTable(db, table) {
  try {
    await db.execute(`DROP TABLE ${await fixtureSchema(db)}.${table}`);
  } catch (error) {
    // If DB2 has already removed the fixture, cleanup must not hide the
    // assertion or protocol failure that triggered the finally block.
    if (!(error instanceof Db2ServerError && error.sqlCode === -204 && error.sqlState === '42704')) throw error;
  }
}

async function materializeStreamedLobValue(value) {
  if (value?.kind !== 'binary-lob' && value?.kind !== 'text-lob') return value;
  const chunks = [];
  for await (const chunk of value.stream()) chunks.push(chunk);
  if (value.kind === 'text-lob') return chunks.join('');
  const byteLength = chunks.reduce((length, chunk) => length + chunk.length, 0);
  const bytes = new Uint8Array(byteLength);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.length; }
  return bytes;
}

async function materializeStreamedRow(values) {
  const result = [];
  // LOB locators share one connection cursor; consume them in column order.
  for (const value of values) result.push(await materializeStreamedLobValue(value));
  return result;
}

test('IBM migration facade opens, queries objects, fetches with metadata, and closes on live Db2', { skip: !dsn, timeout: 40_000 }, async () => {
  const db = await open(dsn);
  try {
    const objects = await db.query('VALUES CAST(17 AS INTEGER)');
    assert.equal(objects.length, 1);
    assert.equal(Object.values(objects[0])[0], 17);

    const result = await db.queryResult('SELECT CAST(? AS INTEGER) AS N FROM SYSIBM.SYSDUMMY1', [29]);
    try {
      assert.equal(result.getColumnNames().length, 1);
      assert.deepEqual(result.getColumnNamesSync(), result.getColumnNames());
      assert.equal(result.getColumnMetadataSync()[0].SQL_DESC_TYPE_NAME, 'INTEGER');
      assert.equal(result.fieldCount, 1);
      assert.equal(result.getName(0), 'N');
      assert.equal(await result.read(), true);
      assert.equal(result.getValue(0), 29);
      assert.equal(await result.read(), false);
    } finally { await result.close(); }

    const reader = await db.queryResult('SELECT CAST(? AS INTEGER) AS N FROM SYSIBM.SYSDUMMY1', [31]);
    try {
      assert.equal(reader.getColumnNames().length, 1);
      const rows = await reader.fetchAll();
      assert.equal(rows.length, 1);
      assert.equal(Object.values(rows[0])[0], 31);
      assert.equal(await reader.fetch(), null);
    } finally { await reader.close(); }

    const command = await db.executeSql('SELECT CAST(? AS INTEGER) AS N FROM SYSIBM.SYSDUMMY1', [37]);
    assert.equal(command.recordsAffected, -1);
    assert.deepEqual(command.rows, [{ N: 37 }]);
    const commandAdapter = db.createCommand('SELECT CAST(? AS INTEGER) AS N FROM SYSIBM.SYSDUMMY1', [39]);
    const commandReader = await commandAdapter.executeReader();
    try {
      assert.equal(await commandReader.read(), true);
      assert.equal(commandReader.getValue(0), 39);
      assert.equal(await commandReader.read(), false);
      assert.equal(commandAdapter._recordsAffected, -1);
    } finally { await commandReader.close(); }
    const table = `TS_COMPAT_EXECUTE_SQL_${randomUUID().replaceAll('-', '').toUpperCase().slice(0, 18)}`;
    try {
      await db.connection.execute(`CREATE TABLE ${await fixtureSchema(db.connection)}.${table} (ID INTEGER)`);
      const insert = db.createCommand(`INSERT INTO ${table} VALUES (?)`, [41]);
      const emptyReader = await insert.executeReader();
      assert.equal(insert._recordsAffected, 1);
      assert.equal(await emptyReader.read(), false);
      await emptyReader.close();
    } finally {
      await db.connection.execute(`DROP TABLE ${await fixtureSchema(db.connection)}.${table}`).catch(() => {});
    }
  } finally { await db.close(); }
});

test('live Db2 warning listeners receive SQLCA warnings for materialized, prepared, and streamed rows', { skip: !dsn, timeout: 40_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const warnings = [];
  const unsubscribe = db.addWarningListener(warning => warnings.push(warning));
  const sql = 'SELECT COUNT(*), CAST(AVG(X) AS DOUBLE) FROM (VALUES (1), (CAST(NULL AS INTEGER)), (3)) AS T(X)';
  try {
    const result = await db.query(sql);
    assert.deepEqual(result.rows, [[3, 2]]);
    assert.equal(warnings.length, 1);
    assert.equal(warnings[0].sqlState, '01003');
    assert.match(warnings[0].warningFlags, /^W/u);

    warnings.length = 0;
    const prepared = await db.prepare(sql);
    assert.deepEqual((await prepared.query()).rows, [[3, 2]]);
    assert.equal(warnings.length, 1);
    assert.equal(warnings[0].sqlState, '01003');

    warnings.length = 0;
    const streamed = [];
    for await (const row of db.queryStream(sql)) streamed.push(row.values);
    assert.deepEqual(streamed, [[3, 2]]);
    assert.equal(warnings.length, 1);
    assert.equal(warnings[0].sqlState, '01003');

    unsubscribe();
    warnings.length = 0;
    await db.query(sql);
    assert.equal(warnings.length, 0);
  } finally {
    unsubscribe();
    await db.close();
  }
});

test('live Db2 interrupt ends the active statement with -952 and keeps the session reusable', { skip: !dsn, timeout: 40_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    assert.equal(await db.interrupt(), false);
    const query = db.query('SELECT COUNT(*) FROM SYSCAT.COLUMNS a, SYSCAT.COLUMNS b, SYSCAT.COLUMNS c');
    const queryCompletion = assert.rejects(query, /SQLCODE=-952 SQLSTATE=57014/);
    await new Promise(resolve => setTimeout(resolve, 500));
    const interruptStarted = performance.now();
    assert.equal(await db.interrupt(), true);
    const interruptAcknowledgmentMs = performance.now() - interruptStarted;
    await queryCompletion;
    const operationCompletionMs = performance.now() - interruptStarted;
    console.log(`Live SQLINTR: interrupt acknowledgment ${interruptAcknowledgmentMs.toFixed(1)} ms; operation completion ${operationCompletionMs.toFixed(1)} ms`);
    assert.deepEqual((await db.query('VALUES 17')).rows, [[17]]);
    assert.equal(await db.interrupt(), false);
  } finally { await db.close(); }
});

test('live Db2 AbortSignal sends a server interrupt and keeps the session reusable', { skip: !dsn, timeout: 40_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    const controller = new AbortController();
    const query = db.query('SELECT COUNT(*) FROM SYSCAT.COLUMNS a, SYSCAT.COLUMNS b, SYSCAT.COLUMNS c', undefined, { signal: controller.signal });
    await new Promise(resolve => setTimeout(resolve, 500));
    controller.abort();
    await assert.rejects(query, error => error.name === 'AbortError');
    assert.equal(db.isClosed, false);
    assert.deepEqual((await db.query('VALUES 17')).rows, [[17]]);
  } finally { await db.close(); }
});

test('live Db2 AbortSignal cancels prepared and pooled queries', { skip: !dsn, timeout: 50_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const pool = new Db2Pool(dsn, { maxConnections: 1 });
  try {
    const constantStatement = await db.prepare('VALUES 17');
    assert.deepEqual((await constantStatement.query()).rows, [[17]]);

    const statement = await db.prepare('SELECT COUNT(*) FROM SYSCAT.COLUMNS a, SYSCAT.COLUMNS b, SYSCAT.COLUMNS c');
    const preparedCancellation = new AbortController();
    const preparedQuery = statement.query([], { signal: preparedCancellation.signal });
    await new Promise(resolve => setTimeout(resolve, 500));
    preparedCancellation.abort();
    await assert.rejects(preparedQuery, error => error.name === 'AbortError');
    assert.deepEqual((await db.query('VALUES 18')).rows, [[18]]);

    const pooledCancellation = new AbortController();
    const pooledQuery = pool.query('SELECT COUNT(*) FROM SYSCAT.COLUMNS a, SYSCAT.COLUMNS b, SYSCAT.COLUMNS c', undefined, { signal: pooledCancellation.signal });
    await new Promise(resolve => setTimeout(resolve, 500));
    pooledCancellation.abort();
    await assert.rejects(pooledQuery, error => error.name === 'AbortError');
    assert.deepEqual((await pool.query('VALUES 19')).rows, [[19]]);
  } finally {
    await db.close();
    await pool.close();
  }
});

test('live Db2 interrupts a lock-waiting statement and decodes SQLERRRM diagnostics', { skip: !dsn, timeout: 40_000 }, async () => {
  const table = `TS_DB2_CANCEL_${Date.now().toString(36).toUpperCase()}`;
  const locker = await Db2Connection.connect(dsn);
  const waiter = await Db2Connection.connect(dsn);
  const schema = (await locker.query('VALUES CURRENT SCHEMA')).rows[0][0].trimEnd();
  const qualifiedTable = `"${schema.replaceAll('"', '""')}"."${table}"`;
  let transaction;
  let created = false;
  try {
    await locker.execute(`CREATE TABLE ${await fixtureSchema(locker)}.${table} (ID INT NOT NULL, V INT NOT NULL, PRIMARY KEY (ID))`);
    created = true;
    await locker.execute(`INSERT INTO ${table} VALUES (1, 0)`);
    transaction = await locker.beginTransaction();
    await transaction.execute(`UPDATE ${table} SET V = V + 1 WHERE ID = 1`);

    // Observe rejection immediately: interrupt cleanup may finish after the
    // main UPDATE receives -952, particularly when close awaits physical FIN.
    const blockedUpdate = waiter.execute(`UPDATE ${table} SET V = V + 1 WHERE ID = 1`)
      .then(() => ({ error: undefined }), error => ({ error }));
    await new Promise(resolve => setTimeout(resolve, 500));
    assert.equal(await waiter.interrupt(), true);
    const outcome = await blockedUpdate;
    assert.equal(outcome.error?.category, 'server');
    assert.equal(outcome.error?.sqlCode, -952);

    await transaction.rollback();
    transaction = undefined;
    assert.deepEqual((await waiter.query('VALUES 21')).rows, [[21]]);
  } finally {
    if (transaction) await transaction.rollback().catch(() => {});
    try { if (created) await locker.execute(`DROP TABLE ${qualifiedTable}`); }
    finally { await Promise.all([locker.close(), waiter.close()]); }
  }
});

test('live Db2 AbortSignal interrupts a later batch set and preserves confirmed writes', { skip: !dsn, timeout: 50_000 }, async () => {
  const table = `TS_DB2_BATCH_CANCEL_${Date.now().toString(36).toUpperCase()}`;
  const locker = await Db2Connection.connect(dsn);
  const direct = await Db2Connection.connect(dsn);
  const pool = new Db2Pool(dsn, { maxConnections: 1 });
  let transaction;
  let created = false;
  try {
    await locker.execute(`CREATE TABLE ${await fixtureSchema(locker)}.${table} (ID INT NOT NULL, V INT NOT NULL, PRIMARY KEY (ID))`);
    created = true;
    await locker.execute(`INSERT INTO ${table} VALUES (1, 0), (2, 0)`);

    transaction = await locker.beginTransaction();
    await transaction.execute(`UPDATE ${table} SET V = V + 1 WHERE ID = 2`);
    const directAbort = new AbortController();
    const directBatch = direct.executeBatch(`UPDATE ${table} SET V = V + 1 WHERE ID = ?`, [[1], [2]], { signal: directAbort.signal });
    await new Promise(resolve => setTimeout(resolve, 500));
    assert.equal(await Promise.race([directBatch.then(() => true, () => true), new Promise(resolve => setTimeout(() => resolve(false), 0))]), false,
      'the second direct batch set should still be waiting on the row lock');
    directAbort.abort();
    await assert.rejects(directBatch, error => error instanceof Db2BatchAbortError
      && error.name === 'AbortError'
      && error.completedParameterSets === 1
      && error.inFlightParameterSetIndex === 1
      && error.rowsAffected === 1);
    await transaction.rollback();
    transaction = undefined;
    assert.deepEqual((await locker.query(`SELECT ID, V FROM ${table} ORDER BY ID`)).rows, [[1, 1], [2, 0]],
      'the first autocommitted set remains while the cancelled locked set does not');
    assert.equal(direct.isClosed, false);
    assert.deepEqual((await direct.query('VALUES 29')).rows, [[29]]);

    transaction = await locker.beginTransaction();
    await transaction.execute(`UPDATE ${table} SET V = V + 1 WHERE ID = 2`);
    const pooledAbort = new AbortController();
    const pooledBatch = pool.executeBatch(`UPDATE ${table} SET V = V + 1 WHERE ID = ?`, [[1], [2]], { signal: pooledAbort.signal });
    await new Promise(resolve => setTimeout(resolve, 500));
    assert.equal(await Promise.race([pooledBatch.then(() => true, () => true), new Promise(resolve => setTimeout(() => resolve(false), 0))]), false,
      'the second pooled batch set should still be waiting on the row lock');
    pooledAbort.abort();
    await assert.rejects(pooledBatch, error => error instanceof Db2BatchAbortError
      && error.name === 'AbortError'
      && error.completedParameterSets === 1
      && error.inFlightParameterSetIndex === 1
      && error.rowsAffected === 1);
    await transaction.rollback();
    transaction = undefined;
    assert.deepEqual((await pool.query(`SELECT ID, V FROM ${table} ORDER BY ID`)).rows, [[1, 2], [2, 0]]);
    assert.deepEqual((await pool.query('VALUES 30')).rows, [[30]]);
    assert.equal(pool.idleCount, 1);
  } finally {
    if (transaction) await transaction.rollback().catch(() => undefined);
    if (created) await dropFixtureTable(locker, table).catch(() => undefined);
    await Promise.all([locker.close(), direct.close(), pool.close()]);
  }
});

test('live Db2 returns typed NULLs, integer boundaries, and floating point values', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    assert.deepEqual((await db.query('VALUES (CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR(30)), CAST(NULL AS DATE), CAST(NULL AS BLOB(100)))')).rows, [[null, null, null, null]]);
    assert.deepEqual((await db.query('VALUES (CAST(-2147483648 AS INTEGER), CAST(9223372036854775807 AS BIGINT), CAST(-32768 AS SMALLINT))')).rows, [[-2147483648, 9223372036854775807n, -32768]]);
    assert.deepEqual((await db.query('VALUES (CAST(1.25 AS REAL), CAST(-2.5 AS DOUBLE), CAST(TRUE AS BOOLEAN), CAST(FALSE AS BOOLEAN))')).rows, [[1.25, -2.5, true, false]]);
    assert.deepEqual((await db.query('VALUES (CAST(? AS SMALLINT), CAST(? AS SMALLINT), CAST(? AS SMALLINT), CAST(? AS SMALLINT), CAST(? AS SMALLINT), CAST(? AS SMALLINT), CAST(? AS SMALLINT), CAST(? AS SMALLINT))',
      [-3.75, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 3.75])).rows,
    [[-3, -2, -1, 0, 0, 1, 2, 3]]);
    assert.deepEqual((await db.query('VALUES CAST(? AS DECIMAL(5,2))', ['1.236'])).rows, [['1.23']]);
    await assert.rejects(db.query('VALUES CAST(? AS DECIMAL(2,2))', ['1.25']), /DECIMAL\(2,2\)/);
    await assert.rejects(db.query('VALUES CAST(? AS SMALLINT)', [32768]), RangeError);
    assert.deepEqual((await db.query('VALUES 29')).rows, [[29]]);
  } finally { await db.close(); }
});

test('live Db2 boolean parameters match IBM lexical and integer conversions', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  const sql = `VALUES (${Array(12).fill('CAST(? AS BOOLEAN)').join(', ')})`;
  const values = ['true', ' FALSE ', 'YES', 'no', 'y', 'N', 'On', 'OFF', '0', '2', '-0', ' +2 '];
  const expected = [[true, false, true, false, true, false, true, false, false, true, false, true]];
  try {
    assert.deepEqual((await db.query(sql, values)).rows, expected);
    const statement = await db.prepare(sql);
    try { assert.deepEqual((await statement.query(values)).rows, expected); }
    finally { statement.close(); }

    const streamed = [];
    for await (const row of db.queryStreamWithParams(sql, values)) streamed.push(row.values);
    assert.deepEqual(streamed, expected);
    assert.deepEqual((await db.query('VALUES (CAST(? AS BOOLEAN), CAST(? AS BOOLEAN), CAST(? AS BOOLEAN))', [0, 2, -1])).rows, [[false, true, true]]);

    for (const invalid of ['', ' ', '1.0', 'maybe', 1.5, Number.NaN, Number.POSITIVE_INFINITY, {}])
      await assert.rejects(db.query('VALUES CAST(? AS BOOLEAN)', [invalid]), TypeError);
    assert.equal(db.isClosed, false);
    assert.deepEqual((await db.query('VALUES 29')).rows, [[29]]);
  } finally { await db.close(); }
});

test('live Db2 integer parameters truncate numeric text and booleans like the IBM provider', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  const sql = `VALUES (${Array(9).fill('CAST(? AS SMALLINT)').join(', ')})`;
  const values = [-3.75, -1.5, ' 1.5 ', '1e0', '.5', '1.', '+0001.50', true, false];
  const expected = [[-3, -1, 1, 1, 0, 1, 1, 1, 0]];
  try {
    assert.deepEqual((await db.query(sql, values)).rows, expected);
    const statement = await db.prepare(sql);
    try { assert.deepEqual((await statement.query(values)).rows, expected); }
    finally { statement.close(); }

    const streamed = [];
    for await (const row of db.queryStreamWithParams(sql, values)) streamed.push(row.values);
    assert.deepEqual(streamed, expected);

    for (const invalid of ['1,5', 'abc', ''])
      await assert.rejects(db.query('VALUES CAST(? AS SMALLINT)', [invalid]), TypeError);
    await assert.rejects(db.query('VALUES CAST(? AS SMALLINT)', ['32768.0']), RangeError);
    assert.equal(db.isClosed, false);
    assert.deepEqual((await db.query('VALUES 29')).rows, [[29]]);
  } finally { await db.close(); }
});

test('live Db2 parameter source type conversions match IBM CLI for scalar, binary and LOB values', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    const textSql = 'VALUES (CAST(? AS VARCHAR(4)), CAST(? AS CHAR(4)), CAST(? AS GRAPHIC(4)), CAST(? AS VARGRAPHIC(4)))';
    const textValues = [true, false, true, false];
    const expectedText = [['1', '0   ', '1   ', '0']];
    const normalizedText = (rows) => rows.map(row => row.map(String));
    assert.deepEqual(normalizedText((await db.query(textSql, textValues)).rows), expectedText);
    const textStatement = await db.prepare(textSql);
    try { assert.deepEqual(normalizedText((await textStatement.query(textValues)).rows), expectedText); }
    finally { textStatement.close(); }
    const textStream = [];
    for await (const row of db.queryStreamWithParams(textSql, textValues)) textStream.push(row.values);
    assert.deepEqual(normalizedText(textStream), expectedText);

    const binarySql = 'VALUES (CAST(? AS VARCHAR(4)), CAST(? AS CHAR(4)), CAST(? AS VARBINARY(3)), CAST(? AS BLOB(3)), CAST(? AS CLOB(20)), CAST(? AS DBCLOB(20)), CAST(? AS XML))';
    const binaryValues = [
      Uint8Array.of(0xe6, 0x9d, 0xb1),
      Uint8Array.of(49, 50, 51),
      Uint8Array.of(49, 50, 51),
      Uint8Array.of(49, 50, 51),
      Uint8Array.of(0xc3, 0xa9),
      Uint8Array.of(0x67, 0x71),
      new TextEncoder().encode('<r/>'),
    ];
    const expectedBinary = [[
      '東', '123 ', Uint8Array.of(49, 50, 51), Uint8Array.of(49, 50, 51), 'é', '東', '<r/>',
    ]];
    assert.deepEqual((await db.query(binarySql, binaryValues)).rows, expectedBinary);
    const binaryStatement = await db.prepare(binarySql);
    try { assert.deepEqual((await binaryStatement.query(binaryValues)).rows, expectedBinary); }
    finally { binaryStatement.close(); }
    const binaryStream = [];
    for await (const row of db.queryStreamWithParams(binarySql, binaryValues)) binaryStream.push(await materializeStreamedRow(row.values));
    assert.deepEqual(binaryStream, expectedBinary);

    const hexBinarySql = 'VALUES (CAST(? AS BINARY(4)), CAST(? AS VARBINARY(4)), CAST(? AS BLOB(4)), CAST(? AS VARBINARY(4)))';
    const hexBinaryValues = ['313233', '313233', '313233', 'aBcD'];
    const hexBinaryExpected = [[Uint8Array.of(49, 50, 51, 0), Uint8Array.of(49, 50, 51), Uint8Array.of(49, 50, 51), Uint8Array.of(0xab, 0xcd)]];
    assert.deepEqual((await db.query(hexBinarySql, hexBinaryValues)).rows, hexBinaryExpected);
    const hexBinaryStatement = await db.prepare(hexBinarySql);
    try { assert.deepEqual((await hexBinaryStatement.query(hexBinaryValues)).rows, hexBinaryExpected); }
    finally { hexBinaryStatement.close(); }
    const hexBinaryStream = [];
    for await (const row of db.queryStreamWithParams(hexBinarySql, hexBinaryValues)) hexBinaryStream.push(row.values);
    assert.deepEqual(hexBinaryStream, hexBinaryExpected);
    assert.deepEqual((await db.query('VALUES (CAST(? AS VARBINARY(4)), CAST(? AS BLOB(4)))', ['', ''])).rows, [[new Uint8Array(), new Uint8Array()]]);

    const binaryTable = `TS_BINSTR_${randomUUID().replaceAll('-', '').toUpperCase()}`.slice(0, 30);
    let binaryTableCreated = false;
    try {
      await db.execute(`CREATE TABLE ${await fixtureSchema(db)}.${binaryTable} (ID INT NOT NULL PRIMARY KEY, F BINARY(4), V VARBINARY(4), B BLOB(4))`);
      binaryTableCreated = true;
      const insertSql = `INSERT INTO ${binaryTable} VALUES (?, ?, ?, ?)`;
      const expectedValues = [Uint8Array.of(49, 50, 51, 0), Uint8Array.of(49, 50, 51), Uint8Array.of(49, 50, 51)];
      assert.equal(await db.execute(insertSql, [1, '313233', '313233', '313233']), 1);
      const insert = await db.prepare(insertSql);
      try { assert.equal(await insert.execute([2, '313233', '313233', '313233']), 1); }
      finally { insert.close(); }
      assert.equal(await db.executeBatch(insertSql, [
        [3, '313233', '313233', '313233'],
        [4, '', '', ''],
      ]), 2);
      const stored = await db.query(`SELECT ID, F, V, B FROM ${binaryTable} ORDER BY ID`);
      assert.deepEqual(stored.rows, [
        [1, ...expectedValues],
        [2, ...expectedValues],
        [3, ...expectedValues],
        [4, new Uint8Array(4), new Uint8Array(), new Uint8Array()],
      ]);
    } finally {
      if (binaryTableCreated) await dropFixtureTable(db, binaryTable);
    }

    const numericSql = 'VALUES (CAST(? AS SMALLINT), CAST(? AS INTEGER), CAST(? AS BIGINT), CAST(? AS REAL), CAST(? AS DOUBLE), CAST(? AS DECIMAL(9,3)), CAST(? AS DECFLOAT(16)))';
    const numericValues = Array(7).fill(true);
    const expectedNumeric = [['1', '1', '1', '1', '1', '1.000', '1']];
    const normalizedNumeric = (rows) => rows.map(row => row.map(String));
    assert.deepEqual(normalizedNumeric((await db.query(numericSql, numericValues)).rows), expectedNumeric);
    const numericStatement = await db.prepare(numericSql);
    try { assert.deepEqual(normalizedNumeric((await numericStatement.query(numericValues)).rows), expectedNumeric); }
    finally { numericStatement.close(); }
    const numericStream = [];
    for await (const row of db.queryStreamWithParams(numericSql, numericValues)) numericStream.push(row.values);
    assert.deepEqual(normalizedNumeric(numericStream), expectedNumeric);

    const unsupported = [
      ['VALUES CAST(? AS SMALLINT)', Buffer.from('123')],
      ['VALUES CAST(? AS BLOB(20))', true],
      ['VALUES CAST(? AS VARBINARY(20))', 123],
      ['VALUES CAST(? AS CLOB(20))', true],
      ['VALUES CAST(? AS CLOB(20))', 12.5],
      ['VALUES CAST(? AS DBCLOB(20))', false],
      ['VALUES CAST(? AS XML)', true],
    ];
    for (const [sql, value] of unsupported)
      await assert.rejects(db.query(sql, [value]), TypeError, sql);
    await assert.rejects(db.query('VALUES CAST(? AS VARBINARY(20))', ['123']), TypeError);
    const invalidHexStatement = await db.prepare('VALUES CAST(? AS VARBINARY(20))');
    try { await assert.rejects(invalidHexStatement.query(['12G4']), TypeError); }
    finally { invalidHexStatement.close(); }
    await assert.rejects(async () => {
      for await (const _ of db.queryStreamWithParams('VALUES CAST(? AS VARBINARY(2))', ['313233'])) { }
    }, TypeError);
    const rejectedNumericSql = 'VALUES CAST(? AS SMALLINT)';
    const rejectedBinaryValue = [Buffer.from('123')];
    const rejectedNumericStatement = await db.prepare(rejectedNumericSql);
    try { await assert.rejects(rejectedNumericStatement.query(rejectedBinaryValue), TypeError); }
    finally { rejectedNumericStatement.close(); }
    await assert.rejects(async () => {
      for await (const _ of db.queryStreamWithParams(rejectedNumericSql, rejectedBinaryValue)) { }
    }, TypeError);
    assert.equal(db.isClosed, false);
    assert.deepEqual((await db.query('VALUES 29')).rows, [[29]]);
    const invalidUtf8HexSql = 'VALUES HEX(CAST(? AS VARCHAR(20)))';
    const invalidUtf8Bytes = [Uint8Array.of(0xe9)];
    assert.deepEqual((await db.query(invalidUtf8HexSql, invalidUtf8Bytes)).rows, [['E9']]);
    const invalidUtf8Statement = await db.prepare(invalidUtf8HexSql);
    try { assert.deepEqual((await invalidUtf8Statement.query(invalidUtf8Bytes)).rows, [['E9']]); }
    finally { invalidUtf8Statement.close(); }
    const invalidUtf8Stream = [];
    for await (const row of db.queryStreamWithParams(invalidUtf8HexSql, invalidUtf8Bytes)) invalidUtf8Stream.push(row.values);
    assert.deepEqual(invalidUtf8Stream, [['E9']]);
    assert.equal(db.isClosed, false);
    assert.deepEqual((await db.query('VALUES 29')).rows, [[29]]);
  } finally { await db.close(); }
});

test('live Db2 keeps empty and NULL LOB values distinct, including non-nullable DBCLOB expressions', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    const expressions = [
      'CAST(NULL AS BLOB(1M))', "CAST(X'' AS BLOB(1M))",
      'CAST(NULL AS CLOB(1M))', "CAST('' AS CLOB(1M))",
      'CAST(NULL AS DBCLOB(1M))', "CAST('' AS DBCLOB(1M))",
      "CAST('DBCLOB 東京' AS DBCLOB(1M))", 'CAST(NULL AS XML)',
    ];
    const expected = [null, new Uint8Array(), null, '', null, '', 'DBCLOB 東京', null];
    for (let index = 0; index < expressions.length; index++) {
      assert.deepEqual((await db.query(`VALUES ${expressions[index]}`)).rows, [[expected[index]]], expressions[index]);
    }
    assert.deepEqual((await db.query(`VALUES (${expressions.join(', ')})`)).rows, [expected]);

    const streamed = [];
    for await (const row of db.queryStream(`VALUES (${expressions.join(', ')})`)) streamed.push(await materializeStreamedRow(row.values));
    assert.deepEqual(streamed, [expected]);

    assert.deepEqual((await db.query('VALUES 29')).rows, [[29]]);
  } finally { await db.close(); }
});

test('live Db2 returns date, time, timestamp, Unicode, and binary values', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    assert.deepEqual((await db.query("VALUES (CAST('2024-02-29' AS DATE), CAST('23:59:58' AS TIME), CAST('2024-02-29 23:59:58.123456' AS TIMESTAMP))")).rows,
      [['2024-02-29', '23.59.58', '2024-02-29-23.59.58.123456']]);
    assert.deepEqual((await db.query("VALUES (CAST('Zażółć 東京' AS VARCHAR(50)), CAST(X'00FF7F' AS VARBINARY(10)))")).rows,
      [['Zażółć 東京', Uint8Array.of(0, 255, 127)]]);
  } finally { await db.close(); }
});

test('live Db2 temporal text parameters match IBM input conversions', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  const sql = 'VALUES (CAST(? AS DATE), CAST(? AS DATE), CAST(? AS TIME), CAST(? AS TIME), CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP))';
  const values = ['2024-2-9', '02/29/2024', '12:34', '23.59.58', '2024-02-29T23:59:58.123456', '2024-02-29'];
  const expected = [['2024-02-09', '2024-02-29', '12.34.00', '23.59.58', '2024-02-29-23.59.58.123456', '2024-02-29-00.00.00.000000']];
  try {
    assert.deepEqual((await db.query(sql, values)).rows, expected);
    const statement = await db.prepare(sql);
    try { assert.deepEqual((await statement.query(values)).rows, expected); }
    finally { statement.close(); }

    const streamed = [];
    for await (const row of db.queryStreamWithParams(sql, values)) streamed.push(row.values);
    assert.deepEqual(streamed, expected);

    const edgeSql = 'VALUES (CAST(? AS TIME), CAST(? AS DATE), CAST(? AS TIME))';
    const edgeValues = ['24:00:00', '2024-02-29T23:59:58.123456', '2024-02-29 23:59:58.123456'];
    assert.deepEqual((await db.query(edgeSql, edgeValues)).rows,
      [['24.00.00', '2024-02-29', '23.59.58']]);

    const timestampPrecision12Sql = 'VALUES CAST(CAST(? AS TIMESTAMP(12)) AS VARCHAR(32))';
    const timestampPrecision12Values = ['2024-02-29T23:59:58.123456789012'];
    const timestampPrecision12Expected = [['2024-02-29-23.59.58.123456789012']];
    assert.deepEqual((await db.query(timestampPrecision12Sql, timestampPrecision12Values)).rows,
      timestampPrecision12Expected);
    const precision12Statement = await db.prepare(timestampPrecision12Sql);
    try { assert.deepEqual((await precision12Statement.query(timestampPrecision12Values)).rows, timestampPrecision12Expected); }
    finally { precision12Statement.close(); }
    const precision12Stream = [];
    for await (const row of db.queryStreamWithParams(timestampPrecision12Sql, timestampPrecision12Values))
      precision12Stream.push(row.values);
    assert.deepEqual(precision12Stream, timestampPrecision12Expected);

    for (const [type, value] of [
      ['DATE', '2024-02-30'],
      ['TIME', '23:59:58.123456'],
      ['TIME', '24:00:01'],
      ['TIMESTAMP', '2024-02-30 23:59:58'],
      ['TIMESTAMP', '2024-02-29 23:59:58+02:00'],
    ])
      await assert.rejects(db.query('VALUES CAST(? AS ' + type + ')', [value]));
    assert.deepEqual((await db.query('VALUES 29')).rows, [[29]]);
  } finally { await db.close(); }
});

test('live Db2 fetches ordered multirow result sets', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    assert.deepEqual((await db.query('SELECT N FROM (VALUES 1, 2, 3, 4, 5) AS T(N) ORDER BY N DESC')).rows, [[5], [4], [3], [2], [1]]);
  } finally { await db.close(); }
});

test('live Db2 query stream crosses reply blocks and returns a drained pool lease after early exit', { skip: !dsn, timeout: 60_000 }, async () => {
  const query = 'SELECT ROW_NUMBER() OVER() AS N FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) A(X), (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) B(X), (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) C(X), (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) D(X), (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) E(X) FETCH FIRST 30000 ROWS ONLY';
  const db = await Db2Connection.connect(dsn);
  const pool = new Db2Pool(dsn, { maxConnections: 1 });
  try {
    let count = 0;
    let checksum = 0n;
    for await (const row of db.queryStream(query)) {
      assert.equal(row.columns.length, 1);
      count++;
      checksum += row.values[0];
    }
    assert.equal(count, 30_000);
    assert.equal(checksum, 450_015_000n);

    for await (const row of pool.queryStream(query)) {
      assert.equal(row.columns.length, 1);
      break;
    }
    assert.deepEqual((await pool.query('VALUES 17')).rows, [[17]]);

    const parameterRows = [];
    for await (const row of db.queryStreamWithParams('VALUES CAST(? AS INTEGER)', [42])) parameterRows.push(row.values);
    assert.deepEqual(parameterRows, [[42]]);
    const statement = await db.prepare('VALUES CAST(? AS INTEGER)');
    const preparedRows = [];
    for await (const row of statement.queryStream([43])) preparedRows.push(row.values);
    assert.deepEqual(preparedRows, [[43]]);
    const blob = Uint8Array.from({ length: 70_003 }, (_, index) => index * 31);
    const lobRows = [];
    for await (const row of db.queryStreamWithParams('VALUES CAST(? AS BLOB(1M))', [blob])) lobRows.push(row.values);
    assert.deepEqual(lobRows, [[blob]]);

    const encoder = new TextEncoder();
    const clobText = 'CLOB café 東京 🚀 '.repeat(5_000);
    const clobBytes = encoder.encode(clobText);
    const clobRows = [];
    for await (const row of db.queryStreamWithParams('VALUES CAST(? AS CLOB(2M))', [{ source: byteChunks(clobBytes), length: clobBytes.length }])) clobRows.push(await materializeStreamedRow(row.values));
    assert.deepEqual(clobRows, [[clobText]]);

    const dbclobText = 'DBCLOB 東京 🚀 '.repeat(5_000);
    const dbclobBytes = encoder.encode(dbclobText);
    const dbclobRows = [];
    for await (const row of db.queryStreamWithParams('VALUES CAST(? AS DBCLOB(2M))', [{ source: byteChunks(dbclobBytes), length: dbclobBytes.length }])) dbclobRows.push(await materializeStreamedRow(row.values));
    assert.deepEqual(dbclobRows, [[dbclobText]]);

    const xmlText = `<root><message>XML café 東京 🚀</message><body>${'x'.repeat(100_000)}</body></root>`;
    const xmlBytes = encoder.encode(xmlText);
    const xmlRows = [];
    for await (const row of db.queryStreamWithParams('VALUES XMLSERIALIZE(CAST(? AS XML) AS CLOB(2M))', [{ source: byteChunks(xmlBytes), length: xmlBytes.length }])) xmlRows.push(await materializeStreamedRow(row.values));
    assert.deepEqual(xmlRows, [[xmlText]]);

    const pooledRows = [];
    for await (const row of pool.queryStreamWithParams('VALUES CAST(? AS INTEGER)', [44])) pooledRows.push(row.values);
    assert.deepEqual(pooledRows, [[44]]);
  } finally {
    await db.close();
    await pool.close();
  }
});

test('live Db2 large CLOB and DBCLOB locators advertise byte budgets', { skip: !dsn, timeout: 120_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const table = `TS_BIG_TEXT_LOB_${randomUUID().replaceAll('-', '').toUpperCase().slice(0, 12)}`;
  let created = false;
  try {
    await db.execute(`CREATE TABLE ${await fixtureSchema(db)}.${table} (C CLOB(32M), D DBCLOB(32M))`);
    created = true;
    const encoder = new TextEncoder();
    const clobText = 'é'.repeat(4_000_000);
    const dbclobText = '漢'.repeat(4_000_000);
    const clobBytes = encoder.encode(clobText);
    const dbclobBytes = encoder.encode(dbclobText);
    await db.execute(
      `INSERT INTO ${table} VALUES (?, CAST(? AS DBCLOB(32M)))`,
      [
        { source: byteChunks(clobBytes), length: clobBytes.length },
        { source: byteChunks(dbclobBytes), length: dbclobBytes.length },
      ],
      { timeoutMs: 120_000 },
    );
    const stream = await db.queryStream(`SELECT C, D FROM ${table}`);
    let clobKind;
    let dbclobKind;
    let clobLength;
    let dbclobLength;
    let clobActual = '';
    let dbclobActual = '';
    for await (const row of stream) {
      const clob = row.values[0];
      const dbclob = row.values[1];
      clobKind = clob.kind;
      dbclobKind = dbclob.kind;
      clobLength = String(clob.length);
      dbclobLength = String(dbclob.length);
      for await (const chunk of clob.stream()) clobActual += chunk;
      for await (const chunk of dbclob.stream()) dbclobActual += chunk;
    }
    assert.equal(clobKind, 'text-lob', 'the large CLOB must be a progressive locator');
    assert.equal(dbclobKind, 'text-lob', 'the large DBCLOB must be a progressive locator');
    assert.equal(clobLength, String(clobBytes.length), 'CLOB locator length is UTF-8 bytes');
    assert.equal(dbclobLength, String(dbclobText.length * 2), 'DBCLOB locator length is UTF-16BE bytes');
    assert.equal(clobActual, clobText, 'progressive CLOB content round-trips');
    assert.equal(dbclobActual, dbclobText, 'progressive DBCLOB content round-trips');
  } finally {
    if (created) await dropFixtureTable(db, table);
    await db.close();
  }
});

test('live Db2 streams BLOB input in bounded chunks and closes broken protocol sessions', { skip: !dsn, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    const roundTrip = makePatternSource(80_003, 7_777);
    const tail = makePatternSource(16_003, 3_011);
    const mixed = await db.query(
      'VALUES (CAST(? AS BLOB(1M)), CAST(? AS INTEGER), LENGTH(CAST(? AS BLOB(1M))))',
      [{ source: roundTrip.source, length: 80_003 }, 73, { source: tail.source, length: 16_003 }],
    );
    const expected = Uint8Array.from({ length: 80_003 }, (_, index) => (index * 37 + 11) % 251);
    assert.deepEqual(mixed.rows, [[expected, 73, 16_003]]);
    assert.equal(roundTrip.bytesRead, 80_003);
    assert.equal(tail.bytesRead, 16_003);
    assert.equal(roundTrip.closed, false);

    const large = makePatternSource(2_000_123, 8_191);
    const length = await db.query('VALUES LENGTH(CAST(? AS BLOB(10M)))', [{ source: large.source, length: 2_000_123, leaveOpen: false }]);
    assert.deepEqual(length.rows, [[2_000_123]]);
    assert.equal(large.bytesRead, 2_000_123);
    assert.ok(large.maxChunk <= 8_191);
    assert.equal(large.closed, true);

    const empty = makePatternSource(0);
    assert.deepEqual((await db.query('VALUES LENGTH(CAST(? AS BLOB(1M)))', [{ source: empty.source, length: 0 }])).rows, [[0]]);
    assert.equal(empty.bytesRead, 0);

    const preparedSource = makePatternSource(70_001, 6_001);
    const prepared = await db.prepare('VALUES CAST(? AS BLOB(1M))');
    const preparedRows = await prepared.query([{ source: preparedSource.source, length: 70_001 }]);
    assert.deepEqual(preparedRows.rows, [[Uint8Array.from({ length: 70_001 }, (_, index) => (index * 37 + 11) % 251)]]);

    const streamedRows = [];
    const rowSource = makePatternSource(90_007, 7_003);
    for await (const row of db.queryStreamWithParams('VALUES LENGTH(CAST(? AS BLOB(1M)))', [{ source: rowSource.source, length: 90_007 }]))
      streamedRows.push(row.values);
    assert.deepEqual(streamedRows, [[90_007]]);

    const table = `TS_DB2_STREAM_${randomUUID().replaceAll('-', '').toUpperCase()}`;
    await db.execute(`CREATE TABLE ${await fixtureSchema(db)}.${table} (ID INT NOT NULL PRIMARY KEY, B BLOB(10M))`);
    try {
      const insertSource = makePatternSource(80_003, 7_777);
      assert.equal(await db.execute(`INSERT INTO ${table} VALUES (?, ?)`, [1, { source: insertSource.source, length: 80_003 }]), 1);
      const stored = await db.query(`SELECT B FROM ${table} WHERE ID = 1`);
      assert.deepEqual(stored.rows, [[Uint8Array.from({ length: 80_003 }, (_, index) => (index * 37 + 11) % 251)]]);

      assert.equal(await db.executeBatch(`INSERT INTO ${table} VALUES (?, ?)`, [
        [2, { source: makePatternSource(90_007, 8_003).source, length: 90_007 }],
        [3, { source: makePatternSource(100_009, 9_001).source, length: 100_009 }],
      ]), 2);

      const transaction = await db.beginTransaction();
      await transaction.execute(`UPDATE ${table} SET B = ? WHERE ID = ?`, [{ source: makePatternSource(200_011, 8_191).source, length: 200_011 }, 1]);
      await transaction.rollback();
      assert.deepEqual((await db.query(`SELECT LENGTH(B) FROM ${table} WHERE ID = 1`)).rows, [[80_003]]);

      const pool = new Db2Pool(dsn, { maxConnections: 1 });
      try {
        assert.deepEqual((await pool.query('VALUES LENGTH(CAST(? AS BLOB(1M)))', [{ source: makePatternSource(77_777, 7_777).source, length: 77_777 }])).rows, [[77_777]]);
      } finally { await pool.close(); }
    } finally { await dropFixtureTable(db, table); }

    const short = makePatternSource(40_000, 10_000);
    await assert.rejects(db.query('VALUES LENGTH(CAST(? AS BLOB(1M)))', [{ source: short.source, length: 100_000 }]), /ended before its declared length/);
    assert.equal(db.isClosed, true);
    await assert.rejects(db.query('VALUES 1'), /connection is closed/);
  } finally { await db.close(); }
});

test('live Db2 closes sessions after truncated CLOB, DBCLOB, and XML inputs', { skip: !dsn, timeout: 60_000 }, async () => {
  const cases = [
    ['VALUES LENGTH(CAST(? AS CLOB(1M)))', 'CLOB'],
    ['VALUES LENGTH(CAST(? AS DBCLOB(1M)))', 'DBCLOB'],
    ['VALUES XMLSERIALIZE(CAST(? AS XML) AS CLOB(1M))', 'XML'],
  ];
  for (const [sql, type] of cases) {
    const db = await Db2Connection.connect(dsn);
    try {
      const short = makePatternSource(40_000, 10_000);
      await assert.rejects(
        db.query(sql, [{ source: short.source, length: 100_000 }]),
        /ended before its declared length/,
      );
      assert.equal(db.isClosed, true, `${type} source truncation must close the desynchronized session`);
    } finally { await db.close(); }
  }

  const db = await Db2Connection.connect(dsn);
  try {
    const firstBytes = new TextEncoder().encode('first text LOB 🚀'.padEnd(40_000, 'a'));
    const second = makePatternSource(40_000, 10_000);
    await assert.rejects(
      db.query(
        'VALUES (LENGTH(CAST(? AS CLOB(1M))), LENGTH(CAST(? AS DBCLOB(1M))))',
        [
          { source: byteChunks(firstBytes), length: firstBytes.length },
          { source: second.source, length: 100_000 },
        ],
      ),
      /ended before its declared length/,
    );
    assert.equal(db.isClosed, true, 'failure in the second LOB stream must close the desynchronized session');
  } finally { await db.close(); }
});

test('live Db2 streams large UTF-8 CLOB, DBCLOB, and XML inputs through EXTDTA', { skip: !dsn, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    const encoder = new TextEncoder();
    const clobText = 'CLOB café 東京 🚀 '.repeat(5_000);
    const clobBytes = encoder.encode(clobText);
    const clob = await db.query('VALUES CAST(? AS CLOB(2M))', [{ source: byteChunks(clobBytes), length: clobBytes.length }]);
    assert.deepEqual(clob.rows, [[clobText]]);

    const dbclobText = 'DBCLOB 東京 🚀 '.repeat(5_000);
    const dbclobBytes = encoder.encode(dbclobText);
    const xmlText = `<root><message>XML café 東京 🚀</message><body>${'x'.repeat(100_000)}</body></root>`;
    const xmlBytes = encoder.encode(xmlText);
    const table = `TS_DB2_XML_${randomUUID().replaceAll('-', '').toUpperCase()}`.slice(0, 30);
    await db.execute(`CREATE TABLE ${await fixtureSchema(db)}.${table} (D DBCLOB(2M), X XML)`);
    try {
      assert.equal(await db.execute(`INSERT INTO ${table} (D) VALUES (?)`, [{ source: byteChunks(dbclobBytes), length: dbclobBytes.length }]), 1);
      const dbclob = await db.query(`SELECT LENGTH(D), SUBSTR(D, 1, 100), SUBSTR(D, LENGTH(D) - 99, 100) FROM ${table} WHERE D IS NOT NULL`);
      assert.deepEqual(dbclob.rows, [[dbclobText.length, dbclobText.slice(0, 100), dbclobText.slice(-100)]]);

      assert.equal(await db.execute(`INSERT INTO ${table} (X) VALUES (?)`, [{ source: byteChunks(xmlBytes), length: xmlBytes.length }]), 1);
      const stored = await db.query(`SELECT XMLSERIALIZE(X AS CLOB(2M)) FROM ${table} WHERE X IS NOT NULL`);
      assert.deepEqual(stored.rows, [[xmlText]]);
    } finally { await dropFixtureTable(db, table); }
  } finally { await db.close(); }
});

test('live Db2 AbortSignal cancels a streaming fetch and leaves the connection reusable', { skip: !dsn, timeout: 40_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    const controller = new AbortController();
    const iterator = db.queryStream('SELECT COUNT(*) FROM SYSCAT.COLUMNS a, SYSCAT.COLUMNS b, SYSCAT.COLUMNS c', { signal: controller.signal });
    const pendingRow = iterator.next();
    await new Promise(resolve => setTimeout(resolve, 500));
    controller.abort();
    await assert.rejects(pendingRow, error => error.name === 'AbortError');
    await iterator.return();
    assert.deepEqual((await db.query('VALUES 17')).rows, [[17]]);
  } finally { await db.close(); }
});

function inListQuery(minBytes) {
  let query = 'SELECT COUNT(*) FROM SYSIBM.SYSDUMMY1 WHERE 1 IN (1';
  for (let value = 2; query.length < minBytes; value++) query += `, ${value}`;
  return `${query})`;
}

function makePatternSource(length, chunkSize) {
  let bytesRead = 0;
  let maxChunk = 0;
  let closed = false;
  const source = {
    async *[Symbol.asyncIterator]() {
      try {
        while (bytesRead < length) {
          const count = Math.min(chunkSize, length - bytesRead);
          const chunk = Uint8Array.from({ length: count }, (_, index) => ((bytesRead + index) * 37 + 11) % 251);
          bytesRead += count;
          maxChunk = Math.max(maxChunk, count);
          yield chunk;
        }
      } finally { closed = true; }
    },
  };
  return { source, get bytesRead() { return bytesRead; }, get maxChunk() { return maxChunk; }, get closed() { return closed; } };
}

function byteChunks(bytes, chunkSize = 8_191) {
  return {
    async *[Symbol.asyncIterator]() {
      for (let offset = 0; offset < bytes.length; offset += chunkSize)
        yield bytes.subarray(offset, Math.min(offset + chunkSize, bytes.length));
    },
  };
}

test('live Db2 handles segmented SQL statements and large parameter blocks', { skip: !dsn, timeout: 120_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    for (const size of [16, 32, 64, 128, 1024].map(kib => kib * 1024)) {
      const query = inListQuery(size);
      assert.deepEqual((await db.query(query)).rows, [[1]], `direct SQL of ${size} bytes`);
      assert.deepEqual((await db.query(`${query} AND 1 = ?`, [1])).rows, [[1]], `prepared SQL of ${size} bytes`);
      await db.execute(`SET CURRENT QUERY OPTIMIZATION = 5${' '.repeat(size)}`);
    }
    for (const count of [1, 2, 4]) {
      const sql = `SELECT ${Array(count).fill('LENGTH(CAST(? AS VARCHAR(16000)))').join(' + ')} FROM SYSIBM.SYSDUMMY1`;
      const params = Array.from({ length: count }, (_, index) => String.fromCharCode(97 + index).repeat(16_000));
      assert.deepEqual((await db.query(sql, params)).rows, [[16_000 * count]]);
    }
  } finally { await db.close(); }
});

test('live Db2 preserves GRAPHIC padding and TIMESTAMP microseconds in binds', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    assert.deepEqual((await db.query('VALUES CAST(? AS CHAR(5))', ['abc'])).rows, [['abc  ']]);
    assert.deepEqual((await db.query('VALUES CAST(? AS VARCHAR(5))', ['abc'])).rows, [['abc']]);
    assert.deepEqual((await db.query('VALUES CAST(? AS GRAPHIC(5))', ['abc'])).rows, [['abc  ']]);
    assert.deepEqual((await db.query('VALUES CAST(? AS VARGRAPHIC(10))', ['東京'])).rows, [['東京']]);
    assert.deepEqual((await db.query('VALUES CAST(? AS TIMESTAMP)', ['2026-08-26 12:34:56.123456'])).rows,
      [['2026-08-26-12.34.56.123456']]);
  } finally { await db.close(); }
});

test('live Db2 handshake, common scalar types, server errors, and ping', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    const integers = await db.query('VALUES (CAST(42 AS INTEGER), CAST(7 AS SMALLINT), CAST(9 AS BIGINT))');
    assert.deepEqual(integers.rows, [[42, 7, 9n]]);

    const decimal = await db.query('VALUES CAST(125 AS DECIMAL(5,2))');
    assert.deepEqual(decimal.rows, [['125.00']]);

    const decfloat = await db.query("VALUES (CAST('123.456' AS DECFLOAT(16)), CAST('-987.654' AS DECFLOAT(16)), CAST('Infinity' AS DECFLOAT(16)), CAST('NaN' AS DECFLOAT(16)))");
    assert.deepEqual(decfloat.rows, [['123.456', '-987.654', 'Infinity', 'NaN']]);
    const wideDecfloat = await db.query("VALUES CAST('12345678901234567890.12345678901234' AS DECFLOAT(34))");
    assert.equal(wideDecfloat.rows[0]?.[0], '12345678901234567890.12345678901234');
    assert.deepEqual((await db.query('VALUES CAST(NULL AS DECFLOAT(16))')).rows, [[null]]);

    const text = await db.query('VALUES CAST(CURRENT SERVER AS VARCHAR(128))');
    assert.equal(typeof text.rows[0]?.[0], 'string');
    assert.ok(text.rows[0][0].length > 0);

    await assert.rejects(db.query('SELECT FROM SYSIBM.SYSDUMMY1'), /SQLCODE=/);
    await db.ping();
  } finally { await db.close(); }
});

test('live Db2 prepares, binds, queries, and executes positional parameters', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  const table = `TS_DB2_PARAMS_${Date.now().toString(36).toUpperCase()}`;
  let created = false;
  try {
    const reusable = await db.prepare('VALUES CAST(? AS INTEGER)');
    assert.equal(reusable.parameterColumns.length, 1);
    assert.deepEqual((await reusable.query([42])).rows, [[42]]);
    const otherStatement = await db.prepare('VALUES CAST(? AS BIGINT)');
    assert.deepEqual((await otherStatement.query([73])).rows, [[73n]]);
    assert.deepEqual((await reusable.query([73])).rows, [[73]]);
    const typed = await db.query('VALUES (CAST(? AS DECIMAL(7,2)), CAST(? AS VARCHAR(32)), CAST(? AS DATE), CAST(? AS BOOLEAN))', ['125.50', 'Teclado Mecânico', '2026-08-26', true]);
    assert.deepEqual(typed.rows, [['125.50', 'Teclado Mecânico', '2026-08-26', true]]);
    const typedNulls = await db.query('VALUES (CAST(? AS INTEGER), CAST(? AS VARCHAR(30)), CAST(? AS DECIMAL(9,3)), CAST(? AS DATE), CAST(? AS BOOLEAN), CAST(? AS XML))', [null, null, null, null, null, null]);
    assert.deepEqual(typedNulls.rows, [[null, null, null, null, null, null]]);
    const typedNullTypes = ['SMALLINT', 'INTEGER', 'BIGINT', 'REAL', 'DOUBLE', 'DECIMAL(9,3)', 'DECFLOAT(16)', 'DATE', 'TIME', 'TIMESTAMP', 'CHAR(5)', 'VARCHAR(12)', 'GRAPHIC(5)', 'VARGRAPHIC(10)', 'BINARY(4)', 'VARBINARY(10)', 'BLOB(100)', 'CLOB(100)', 'DBCLOB(100)', 'XML', 'BOOLEAN'];
    const typedNullMatrix = await db.query(`VALUES (${typedNullTypes.map(type => `CAST(? AS ${type})`).join(', ')})`, Array(typedNullTypes.length).fill(null));
    assert.deepEqual(typedNullMatrix.rows, [Array(typedNullTypes.length).fill(null)]);
    const varbinary = Uint8Array.of(0, 0x7f, 0x80, 0xff);
    assert.deepEqual((await db.query('VALUES CAST(? AS VARBINARY(10))', [varbinary])).rows, [[varbinary]]);
    const decimalPrecisions = await db.query('VALUES (CAST(? AS DECIMAL(4,2)), CAST(? AS DECIMAL(5,2)), CAST(? AS DECIMAL(8,3)), CAST(? AS DECIMAL(9,3)))', ['1.25', '1.25', '12345.678', '123456.789']);
    assert.deepEqual(decimalPrecisions.rows, [['1.25', '1.25', '12345.678', '123456.789']]);
    const parameterBoundaries = await db.query('VALUES (CAST(? AS SMALLINT), CAST(? AS INTEGER), CAST(? AS BIGINT), CAST(? AS DECIMAL(9,3)), CAST(? AS REAL), CAST(? AS DOUBLE))', [-32768, -2147483648, 9223372036854775807n, '-123456.789', 1.25, -2.5]);
    assert.deepEqual(parameterBoundaries.rows, [[-32768, -2147483648, 9223372036854775807n, '-123456.789', 1.25, -2.5]]);
    const typedDecfloat = await db.query('VALUES (CAST(? AS DECFLOAT(16)), CAST(? AS DECFLOAT(16)), CAST(? AS DECFLOAT(16)), CAST(? AS DECFLOAT(34)), CAST(? AS DECFLOAT(16)))', ['123.456', '-987.654', 'Infinity', '12345678901234567890.12345678901234', 'NaN']);
    assert.deepEqual(typedDecfloat.rows, [['123.456', '-987.654', 'Infinity', '12345678901234567890.12345678901234', 'NaN']]);
    const lobBytes = Uint8Array.from({ length: 70_003 }, (_, index) => index % 251);
    for (const size of [32_766, 32_767, 32_768, lobBytes.length]) {
      const boundLength = await db.query('VALUES LENGTH(CAST(? AS BLOB(1M)))', [lobBytes.subarray(0, size)]);
      assert.deepEqual(boundLength.rows, [[size]]);
    }
    const clobText = 'CLOB café '.repeat(80);
    const dbclobText = 'DBCLOB 東京 '.repeat(80);
    const xmlText = '<root><message>XML café</message></root>';
    const lobTable = `TS_DB2_LOB_${Date.now().toString(36).toUpperCase()}`;
    let lobTableCreated = false;
    try {
      await db.execute(`CREATE TABLE ${await fixtureSchema(db)}.${lobTable} (B BLOB(1M), C CLOB(1M), D DBCLOB(1M), X XML)`);
      lobTableCreated = true;
      await db.execute(`INSERT INTO ${lobTable} (B) VALUES (?)`, [lobBytes]);
      assert.equal((await db.query(`SELECT 1 FROM ${lobTable} WHERE B IS NOT NULL`)).rows.length, 1);
      await db.execute(`DELETE FROM ${lobTable}`);
      assert.equal(await db.execute(`INSERT INTO ${lobTable} VALUES (?, ?, ?, ?)`, [lobBytes, clobText, dbclobText, xmlText]), 1);
      const lobInputs = await db.query(`SELECT B, C, D, XMLSERIALIZE(X AS VARCHAR(1024)) FROM ${lobTable}`);
      const [returnedBytes, returnedClob, returnedDbClob, returnedXml] = lobInputs.rows[0] ?? [];
      assert.ok(returnedBytes instanceof Uint8Array);
      assert.equal(returnedBytes.length, lobBytes.length);
      assert.ok(Buffer.from(returnedBytes).equals(Buffer.from(lobBytes)));
      assert.equal(returnedClob, clobText);
      assert.equal(returnedDbClob, dbclobText);
      assert.equal(returnedXml, xmlText);
    } finally {
      if (lobTableCreated) await db.execute(`DROP TABLE ${await fixtureSchema(db)}.${lobTable}`);
    }
    otherStatement.close();
    reusable.close();

    await db.execute(`CREATE TABLE ${await fixtureSchema(db)}.${table} (V INTEGER NOT NULL UNIQUE)`);
    created = true;
    const insert = await db.prepare(`INSERT INTO ${table} (V) VALUES (?)`);
    assert.equal(await insert.execute([91]), 1);
    insert.close();
    assert.equal(await db.execute(`INSERT INTO ${table} (V) VALUES (?)`, [92]), 1);
    assert.equal(await db.executeBatch(`INSERT INTO ${table} (V) VALUES (?)`, [[93], [94]]), 2);
    assert.equal(await db.executeBatch(`INSERT INTO ${table} (V) VALUES (?)`, []), 0);
    await assert.rejects(db.executeBatch(`INSERT INTO ${table} (V) VALUES (?)`, [[95], [95]]), (error) =>
      error instanceof Db2BatchExecutionError
      && error.parameterSetIndex === 1
      && error.completedParameterSets === 1
      && error.rowsAffected === 1
      && error.sqlCode === -803
      && error.sqlState === '23505'
      && error.cause instanceof Db2ServerError,
    );
    assert.deepEqual((await db.query(`SELECT V FROM ${table} WHERE V = ?`, [91])).rows, [[91]]);
    assert.deepEqual((await db.query(`SELECT V FROM ${table} WHERE V > ? ORDER BY V`, [92])).rows, [[93], [94], [95]]);
    assert.deepEqual((await db.query(`SELECT V FROM ${table} WHERE V = 95`)).rows, [[95]]);
  } finally {
    if (created) await db.execute(`DROP TABLE ${await fixtureSchema(db)}.${table}`);
    await db.close();
  }
});

test('live Db2 row importer preserves typed values, NULLs, atomicity, checkpoints, and session reuse', { skip: !dsn, timeout: 60_000 }, async () => {
  const source = await Db2Connection.connect(dsn);
  const destination = await Db2Connection.connect(dsn);
  const suffix = randomUUID().replaceAll('-', '').toUpperCase().slice(0, 18);
  const sourceTable = `TS_IMP_SOURCE_${suffix}`;
  const atomicTable = `TS_IMP_ATOMIC_${suffix}`;
  const checkpointTable = `TS_IMP_CHECKPOINT_${suffix}`;
  const created = [];
  try {
    for (const table of [sourceTable, atomicTable, checkpointTable]) {
      await destination.execute(`CREATE TABLE ${await fixtureSchema(destination)}.${table} (ID INTEGER NOT NULL PRIMARY KEY, TXT VARCHAR(80), AMOUNT DECIMAL(12,2))`);
      created.push(table);
    }
    const inputs = [
      [1, 'Zażółć gęślą jaźń', '123.45'],
      [2, null, null],
      [3, '', '0.00'],
      [4, "O'Reilly", '-42.50'],
      [5, '東京 🚀', '999999.99'],
    ];
    await source.executeBatch(`INSERT INTO ${sourceTable} (ID, TXT, AMOUNT) VALUES (?, ?, ?)`, inputs);

    const expected = (await source.query(`SELECT ID, TXT, AMOUNT FROM ${sourceTable} ORDER BY ID`)).rows;
    for (const [table, policy] of [[atomicTable, 'atomic'], [checkpointTable, 'commitEveryRows']]) {
      async function* sourceRows() {
        for await (const row of source.queryStream(`SELECT ID, TXT, AMOUNT FROM ${sourceTable} ORDER BY ID`)) yield row.values;
      }
      const snapshots = [];
      const progress = await destination.importRows(sourceRows(), {
        table,
        columns: ['ID', 'TXT', 'AMOUNT'],
      }, {
        policy,
        maxBatchRows: 2,
        maxBatchBytes: 4096,
        onProgress: snapshot => snapshots.push(snapshot),
      });
      assert.equal(progress.sourceRowsRead, 5);
      assert.equal(progress.committedRows, 5);
      assert.equal(progress.committedBatches, policy === 'atomic' ? 1 : 3);
      assert.equal(snapshots.length, policy === 'atomic' ? 1 : 3);
      assert.deepEqual((await destination.query(`SELECT ID, TXT, AMOUNT FROM ${table} ORDER BY ID`)).rows, expected);
      assert.deepEqual((await destination.query('VALUES 29')).rows, [[29]], 'destination session remains reusable');
    }
  } finally {
    for (const table of created.reverse()) await dropFixtureTable(destination, table);
    await source.close();
    await destination.close();
  }
});

test('live Db2 row importer flushes row/byte boundaries, partial tails, and an oversized singleton', { skip: !dsn, timeout: 60_000 }, async () => {
  const source = await Db2Connection.connect(dsn);
  const destination = await Db2Connection.connect(dsn);
  const suffix = randomUUID().replaceAll('-', '').toUpperCase().slice(0, 18);
  const sourceTable = `TS_IMP_BOUND_SOURCE_${suffix}`;
  const atomicTable = `TS_IMP_BOUND_ATOMIC_${suffix}`;
  const checkpointTable = `TS_IMP_BOUND_CHECK_${suffix}`;
  const created = [];
  const inputs = [[1, 'a'], [2, 'bb'], [3, null], [4, null], [5, null], [6, 'ccc'], [7, 'd'], [8, null], [9, 'x'.repeat(40)]];
  try {
    for (const table of [sourceTable, atomicTable, checkpointTable]) {
      await destination.execute(`CREATE TABLE ${await fixtureSchema(destination)}.${table} (ID INTEGER NOT NULL PRIMARY KEY, TXT VARCHAR(80))`);
      created.push(table);
    }
    await destination.executeBatch(`INSERT INTO ${sourceTable} (ID, TXT) VALUES (?, ?)`, inputs);
    const expected = (await source.query(`SELECT ID, TXT FROM ${sourceTable} ORDER BY ID`)).rows;
    for (const [table, policy] of [[atomicTable, 'atomic'], [checkpointTable, 'commitEveryRows']]) {
      async function* sourceRows() {
        for await (const row of source.queryStream(`SELECT ID, TXT FROM ${sourceTable} ORDER BY ID`)) yield row;
      }
      const snapshots = [];
      const progress = await destination.importRows(sourceRows(), { table, columns: ['ID', 'TXT'] }, {
        policy, maxBatchRows: 3, maxBatchBytes: 39, onProgress: snapshot => snapshots.push(snapshot),
      });
      assert.deepEqual((await destination.query(`SELECT ID, TXT FROM ${table} ORDER BY ID`)).rows, expected);
      assert.equal(progress.sourceRowsRead, 9);
      assert.equal(progress.committedRows, 9);
      assert.equal(progress.estimatedCommittedBytes, 185);
      assert.equal(progress.committedBatches, policy === 'atomic' ? 1 : 5);
      assert.equal(snapshots.length, policy === 'atomic' ? 1 : 5);
      if (policy === 'commitEveryRows') {
        assert.deepEqual(snapshots.map(({ sourceRowsRead, committedRows, committedBatches, estimatedCommittedBytes }) => [sourceRowsRead, committedRows, committedBatches, estimatedCommittedBytes]), [
          [2, 0, 0, 0], [5, 2, 1, 39], [7, 5, 2, 75], [9, 6, 3, 96], [9, 8, 4, 127],
        ]);
      }
      assert.deepEqual((await destination.query('VALUES 43')).rows, [[43]]);
    }
  } finally {
    await source.close();
    for (const table of created.reverse()) await dropFixtureTable(destination, table);
    await destination.close();
  }
});

test('live Db2 row importer transfers progressive and materialized BLOB/CLOB/DBCLOB/XML under both policies', { skip: !dsn, timeout: 60_000 }, async () => {
  const source = await Db2Connection.connect(dsn);
  const destination = await Db2Connection.connect(dsn);
  const suffix = randomUUID().replaceAll('-', '').toUpperCase().slice(0, 18);
  const sourceTable = `TS_IMP_LOB_SOURCE_${suffix}`;
  const atomicTable = `TS_IMP_LOB_ATOMIC_${suffix}`;
  const checkpointTable = `TS_IMP_LOB_CHECKPOINT_${suffix}`;
  const created = [];
  const blob = Uint8Array.from({ length: 180_123 }, (_, index) => (index * 31) & 0xff);
  const clob = 'CLOB café 東京 🚀 '.repeat(7_000);
  const dbclob = 'DBCLOB Zażółć 東京 🚀 '.repeat(5_000);
  const xml = `<root><message>XML café 東京 🚀</message><body>${'x'.repeat(80_000)}</body></root>`;
  try {
    for (const table of [sourceTable, atomicTable, checkpointTable]) {
      await destination.execute(`CREATE TABLE ${await fixtureSchema(destination)}.${table} (ID INTEGER NOT NULL PRIMARY KEY, B BLOB(2M), C CLOB(2M), D DBCLOB(2M), X XML)`);
      created.push(table);
    }
    await source.execute(`INSERT INTO ${sourceTable} (ID, B, C, D, X) VALUES (?, ?, ?, ?, ?)`, [
      1,
      { source: byteChunks(blob), length: blob.length },
      { source: byteChunks(new TextEncoder().encode(clob)), length: Buffer.byteLength(clob, 'utf8') },
      { source: byteChunks(new TextEncoder().encode(dbclob)), length: Buffer.byteLength(dbclob, 'utf8') },
      { source: byteChunks(new TextEncoder().encode(xml)), length: Buffer.byteLength(xml, 'utf8') },
    ], { timeoutMs: 10_000 });
    await source.execute(`INSERT INTO ${sourceTable} (ID, B, C, D, X) VALUES (2, CAST(X'' AS BLOB(2M)), CAST('' AS CLOB(2M)), CAST('' AS DBCLOB(2M)), CAST(NULL AS XML))`);

    for (const [table, policy] of [[atomicTable, 'atomic'], [checkpointTable, 'commitEveryRows']]) {
      async function* rows() {
        for await (const row of source.queryStream(`SELECT ID, B, C, D, X FROM ${sourceTable} ORDER BY ID`, { timeoutMs: 10_000 })) yield row;
      }
      const progress = await destination.importRows(rows(), {
        table, columns: ['ID', 'B', 'C', 'D', 'X'],
      }, { policy, maxBatchRows: 1, maxBatchBytes: 64 * 1024, timeoutMs: 10_000, cancelCleanupTimeoutMs: 5_000 });
      assert.equal(progress.sourceRowsRead, 2);
      assert.equal(progress.committedRows, 2);
      assert.equal(progress.committedBatches, policy === 'atomic' ? 1 : 2);

      async function readBack(db, from) {
        const output = [];
        for await (const row of db.queryStream(`SELECT ID, B, C, D, XMLSERIALIZE(X AS CLOB(2M)) AS X FROM ${from} ORDER BY ID`, { timeoutMs: 10_000 })) {
          output.push(await materializeStreamedRow(row.values));
        }
        return output;
      }
      const result = await readBack(destination, table);
      assert.equal(result.length, 2);
      assert.equal(result[0][0], 1);
      assert.deepEqual(result[0][1], blob);
      assert.equal(result[0][2], clob);
      assert.equal(result[0][3], dbclob);
      assert.equal(result[0][4], xml);
      assert.equal(result[1][0], 2);
      assert.deepEqual(result[1].slice(1), [new Uint8Array(), '', '', null]);
      assert.deepEqual((await destination.query('VALUES 31', undefined, { timeoutMs: 10_000 })).rows, [[31]], 'destination remains reusable after streamed LOB import');
    }
    if (ibmImportOracle) {
      const ibmRows = readTablesThroughOfficialIbmProvider([atomicTable, checkpointTable]);
      const expectedIbmRows = [
        [1, Buffer.from(blob).toString('hex'), clob, dbclob, xml],
        [2, '', '', '', null],
      ];
      assert.deepEqual(ibmRows[atomicTable], expectedIbmRows, 'IBM provider independently reads Atomic imported values');
      assert.deepEqual(ibmRows[checkpointTable], expectedIbmRows, 'IBM provider independently reads CommitEveryRows imported values');
    } else console.log(IBM_ORACLE_SKIPPED_MARKER);
  } finally {
    await source.close();
    for (const table of created.reverse()) await dropFixtureTable(destination, table);
    await destination.close();
  }
});

test('live Db2 row importer cancellation rolls back the destination and discards an unfinished source LOB request', { skip: !dsn, timeout: 60_000 }, async () => {
  const source = await Db2Connection.connect(dsn);
  const destination = await Db2Connection.connect(dsn);
  const sendSourceInterrupt = source.requestInterrupt.bind(source);
  let interruptAcknowledged = false;
  source.requestInterrupt = async (...args) => {
    const acknowledged = await sendSourceInterrupt(...args);
    interruptAcknowledged = acknowledged;
    return acknowledged;
  };
  const suffix = randomUUID().replaceAll('-', '').toUpperCase().slice(0, 18);
  const sourceTable = `TS_IMP_CANCEL_SOURCE_${suffix}`;
  const targetTable = `TS_IMP_CANCEL_TARGET_${suffix}`;
  let sourceCreated = false;
  let targetCreated = false;
  try {
    await destination.execute(`CREATE TABLE ${await fixtureSchema(destination)}.${sourceTable} (ID INTEGER NOT NULL PRIMARY KEY, B BLOB(4M))`);
    sourceCreated = true;
    await destination.execute(`CREATE TABLE ${await fixtureSchema(destination)}.${targetTable} (ID INTEGER NOT NULL PRIMARY KEY, B BLOB(4M))`);
    targetCreated = true;
    const payload = makePatternSource(3_000_000, 32_768);
    await destination.execute(`INSERT INTO ${sourceTable} (ID, B) VALUES (1, ?)`, [{ source: payload.source, length: 3_000_000 }], { timeoutMs: 20_000 });

    const controller = new AbortController();
    let firstChunkSeen = false;
    let locatorRequests = 0;
    let locatorCancelIssued = false;
    const sendSourceRequest = source.send.bind(source);
    source.send = async (payload, ...args) => {
      const result = await sendSourceRequest(payload, ...args);
      if (payload[2] === 0x20 && payload[3] === 0x15 && ++locatorRequests === 2 && !locatorCancelIssued) {
        locatorCancelIssued = true;
        setTimeout(() => controller.abort(), 1);
      }
      return result;
    };
    async function* rows() {
      for await (const row of source.queryStream(`SELECT ID, B FROM ${sourceTable}`, {
        signal: controller.signal, timeoutMs: 20_000, cancelCleanupTimeoutMs: 25_000,
      })) {
        const lob = row.values[1];
        async function* abortingLob() {
          for await (const chunk of lob.stream()) {
            firstChunkSeen = true;
            yield chunk;
          }
        }
        yield { ...row, values: [row.values[0], { ...lob, stream: () => abortingLob() }] };
      }
    }
    await assert.rejects(destination.importRows(rows(), { table: targetTable, columns: ['ID', 'B'] }, {
      signal: controller.signal, timeoutMs: 20_000, cancelCleanupTimeoutMs: 25_000,
    }), error => error.name === 'AbortError');
    assert.equal(firstChunkSeen, true);
    assert.equal(locatorCancelIssued, true, 'SQLINTR is issued while a GETNXTCHK response is pending');
    assert.equal(interruptAcknowledged, true, 'Db2 acknowledges the separate SQLINTR request');
    assert.equal(source.isClosed, true, 'Db2 did not finish the in-flight progressive LOB request after acknowledging SQLINTR; the uncertain source session is discarded at the cleanup deadline');
    assert.equal(destination.isClosed, false, 'source locator cancellation must not interrupt the destination session');
    assert.deepEqual((await destination.query(`SELECT COUNT(*) FROM ${targetTable}`)).rows, [[0]], 'uncommitted source cancellation must not write a target row');
    assert.deepEqual((await destination.query('VALUES 17')).rows, [[17]], 'destination session remains reusable after import cancellation');
  } finally {
    await source.close();
    if (targetCreated) await dropFixtureTable(destination, targetTable);
    if (sourceCreated) await dropFixtureTable(destination, sourceTable);
    await destination.close();
  }
});

test('live Db2 import cancellation during a progressive LOB preserves an earlier CommitEveryRows checkpoint', { skip: !dsn, timeout: 90_000 }, async () => {
  const source = await Db2Connection.connect(dsn);
  const destination = await Db2Connection.connect(dsn);
  const sendSourceInterrupt = source.requestInterrupt.bind(source);
  let interruptAcknowledged = false;
  source.requestInterrupt = async (...args) => {
    const acknowledged = await sendSourceInterrupt(...args);
    interruptAcknowledged = acknowledged;
    return acknowledged;
  };
  const suffix = randomUUID().replaceAll('-', '').toUpperCase().slice(0, 18);
  const sourceTable = `TS_IMP_CKPT_SOURCE_${suffix}`;
  const targetTable = `TS_IMP_CKPT_TARGET_${suffix}`;
  let sourceCreated = false;
  let targetCreated = false;
  try {
    await destination.execute(`CREATE TABLE ${await fixtureSchema(destination)}.${sourceTable} (ID INTEGER NOT NULL PRIMARY KEY, B BLOB(4M), C CLOB(4M), D DBCLOB(4M), X XML)`);
    sourceCreated = true;
    await destination.execute(`CREATE TABLE ${await fixtureSchema(destination)}.${targetTable} (ID INTEGER NOT NULL PRIMARY KEY, B BLOB(4M), C CLOB(4M), D DBCLOB(4M), X XML)`);
    targetCreated = true;
    await destination.execute(`INSERT INTO ${sourceTable} VALUES (1, ?, CAST('checkpoint clob' AS CLOB(32)), CAST('checkpoint dbclob' AS DBCLOB(32)), XMLPARSE(DOCUMENT '<root>checkpoint</root>'))`, [Uint8Array.of(1, 2, 3, 0xff)]);
    const payload = makePatternSource(3_000_000, 32_768);
    await destination.execute(`INSERT INTO ${sourceTable} VALUES (2, ?, CAST('later clob' AS CLOB(32)), CAST('later dbclob' AS DBCLOB(32)), XMLPARSE(DOCUMENT '<root>later</root>'))`, [{ source: payload.source, length: 3_000_000 }], { timeoutMs: 20_000 });

    const controller = new AbortController();
    let currentSourceId = 0;
    let locatorRequests = 0;
    let locatorCancelIssued = false;
    let bytesReadFromSecondLob = 0;
    const progress = [];
    const sendSourceRequest = source.send.bind(source);
    source.send = async (request, ...args) => {
      const result = await sendSourceRequest(request, ...args);
      if (currentSourceId === 2 && request[2] === 0x20 && request[3] === 0x15 && ++locatorRequests === 2 && !locatorCancelIssued) {
        locatorCancelIssued = true;
        setTimeout(() => controller.abort(), 1);
      }
      return result;
    };

    async function* rows() {
      for await (const row of source.queryStream(`SELECT ID, B, C, D, X FROM ${sourceTable} ORDER BY ID`, {
        signal: controller.signal, timeoutMs: 20_000, cancelCleanupTimeoutMs: 25_000,
      })) {
        currentSourceId = row.values[0];
        if (currentSourceId !== 2) {
          yield row;
          continue;
        }
        const lob = row.values[1];
        assert.equal(lob?.kind, 'binary-lob', 'second source row must use a progressive BLOB locator');
        async function* monitoredLob() {
          for await (const chunk of lob.stream()) {
            bytesReadFromSecondLob += chunk.length;
            yield chunk;
          }
        }
        yield { ...row, values: [currentSourceId, { ...lob, stream: () => monitoredLob() }, ...row.values.slice(2)] };
      }
    }

    await assert.rejects(destination.importRows(rows(), { table: targetTable, columns: ['ID', 'B', 'C', 'D', 'X'] }, {
      policy: 'commitEveryRows', maxBatchRows: 1, maxBatchBytes: 64 * 1024,
      signal: controller.signal, timeoutMs: 30_000, cancelCleanupTimeoutMs: 25_000,
      onProgress: snapshot => progress.push(snapshot),
    }), error => error.name === 'AbortError');

    assert.deepEqual(progress.map(item => [item.sourceRowsRead, item.committedRows, item.committedBatches]), [[1, 0, 0]], 'first checkpoint was requested before its commit and second row never reached checkpoint');
    assert.equal(bytesReadFromSecondLob > 0 && bytesReadFromSecondLob < 3_000_000, true, 'cancellation interrupts a partially consumed LOB');
    assert.equal(locatorCancelIssued, true, 'SQLINTR is issued during the second row GETNXTCHK');
    assert.equal(interruptAcknowledged, true, 'Db2 acknowledges the separate source SQLINTR request');
    assert.equal(source.isClosed, true, 'the unfinished source locator is discarded after bounded cleanup');
    assert.equal(destination.isClosed, false, 'cancelling the source locator must not discard the destination session');
    assert.deepEqual((await destination.query(`SELECT COUNT(*) FROM ${targetTable}`)).rows, [[1]], 'the first confirmed checkpoint remains durable and the partial second row is absent');
    assert.deepEqual((await destination.query('VALUES 23')).rows, [[23]], 'destination session is reusable after rollback of the partial checkpoint');
    if (ibmImportOracle) {
      const ibmRows = readTablesThroughOfficialIbmProvider([targetTable]);
      assert.deepEqual(ibmRows[targetTable], [[1, '010203ff', 'checkpoint clob', 'checkpoint dbclob', '<root>checkpoint</root>']], 'IBM .NET independently reads the exact durable checkpoint values');
    } else console.log(IBM_ORACLE_SKIPPED_MARKER);
  } finally {
    await source.close();
    if (targetCreated) await dropFixtureTable(destination, targetTable);
    if (sourceCreated) await dropFixtureTable(destination, sourceTable);
    await destination.close();
  }
});

test('live Db2 keeps mixed NULL and value parameters aligned in direct, prepared, DML, and batch paths', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  const table = `TS_MIXED_PARAMS_${Date.now().toString(36).toUpperCase()}`;
  let created = false;
  const binary = Uint8Array.of(0, 0x7f, 0x80, 0xff);
  const mixedSql = 'VALUES (CAST(? AS SMALLINT), CAST(? AS VARCHAR(20)), CAST(? AS DECIMAL(9,3)), CAST(? AS BOOLEAN), CAST(? AS VARBINARY(10)), CAST(? AS INTEGER))';
  const mixedParameters = [12, null, '42.125', true, binary, null];
  const assertMixedRow = (rows) => assert.deepEqual(rows, [[12, null, '42.125', true, binary, null]]);

  try {
    await db.execute(`CREATE TABLE ${await fixtureSchema(db)}.${table} (ID INTEGER NOT NULL PRIMARY KEY, LABEL VARCHAR(30), AMOUNT DECIMAL(9,3), ENABLED BOOLEAN)`);
    created = true;

    assertMixedRow((await db.query(mixedSql, mixedParameters)).rows);
    const queryStatement = await db.prepare(mixedSql);
    try { assertMixedRow((await queryStatement.query(mixedParameters)).rows); }
    finally { queryStatement.close(); }

    const insertSql = `INSERT INTO ${table} (ID, LABEL, AMOUNT, ENABLED) VALUES (?, ?, ?, ?)`;
    const preparedInsert = await db.prepare(insertSql);
    try { assert.equal(await preparedInsert.execute([1, null, '12.375', true]), 1); }
    finally { preparedInsert.close(); }

    assert.equal(await db.execute(insertSql, [2, 'direct', null, false]), 1);
    assert.equal(await db.executeBatch(insertSql, [
      [3, null, null, null],
      [4, 'batch', '4.25', true],
    ]), 2);
    assert.deepEqual((await db.query(`SELECT ID, LABEL, AMOUNT, ENABLED FROM ${table} ORDER BY ID`)).rows, [
      [1, null, '12.375', true],
      [2, 'direct', null, false],
      [3, null, null, null],
      [4, 'batch', '4.250', true],
    ]);
  } finally {
    if (created) await db.execute(`DROP TABLE ${await fixtureSchema(db)}.${table}`);
    await db.close();
  }
});

test('live Db2 returns mixed typed parameter values and NULLs for scalar, LOB, and XML types', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  const table = `TS_TYPE_MATRIX_${Date.now().toString(36).toUpperCase()}_${Math.random().toString(36).slice(2, 8).toUpperCase()}`;
  let created = false;
  const binary = Uint8Array.of(0, 0x7f, 0x80, 0xff);
  const cases = [
    ['SMALLINT', 'CAST(? AS SMALLINT)', -32768, null, -32768, null],
    ['INTEGER', 'CAST(? AS INTEGER)', null, 2147483647, null, 2147483647],
    ['BIGINT', 'CAST(? AS BIGINT)', 9223372036854775807n, null, 9223372036854775807n, null],
    ['REAL', 'CAST(? AS REAL)', null, 1.25, null, 1.25],
    ['DOUBLE', 'CAST(? AS DOUBLE)', -2.5, null, -2.5, null],
    ['DECIMAL(9,3)', 'CAST(? AS DECIMAL(9,3))', null, '42.125', null, '42.125'],
    ['DECFLOAT(16)', 'CAST(? AS DECFLOAT(16))', 'Infinity', null, 'Infinity', null],
    ['DECFLOAT(34)', 'CAST(? AS DECFLOAT(34))', null, '1234567890123456789012345678901234', null, '1234567890123456789012345678901234'],
    ['DATE', 'CAST(? AS DATE)', '2026-08-26', null, '2026-08-26', null],
    ['TIME', 'CAST(? AS TIME)', null, '12:34:56', null, '12.34.56'],
    ['TIMESTAMP', 'CAST(? AS TIMESTAMP)', '2026-08-26 12:34:56.123456', null, '2026-08-26-12.34.56.123456', null],
    ['CHAR(5)', 'CAST(? AS CHAR(5))', 'ab', null, 'ab   ', null],
    ['VARCHAR(20)', 'CAST(? AS VARCHAR(20))', null, 'café', null, 'café'],
    ['GRAPHIC(5)', 'CAST(? AS GRAPHIC(5))', '東', null, '東    ', null],
    ['VARGRAPHIC(10)', 'CAST(? AS VARGRAPHIC(10))', null, '東京', null, '東京'],
    ['BINARY(4)', 'CAST(? AS BINARY(4))', binary, null, binary, null],
    ['VARBINARY(10)', 'CAST(? AS VARBINARY(10))', null, binary, null, binary],
    ['BLOB(100)', 'CAST(? AS BLOB(100))', binary, null, binary, null],
    ['CLOB(100)', 'CAST(? AS CLOB(100))', null, 'CLOB café', null, 'CLOB café'],
    ['DBCLOB(100)', 'CAST(? AS DBCLOB(100))', 'DBCLOB 東京', null, 'DBCLOB 東京', null],
    ['XML', 'CAST(? AS XML)', null, '<root/>', null, '<root/>'],
    ['BOOLEAN', 'CAST(? AS BOOLEAN)', true, null, true, null],
  ];
  const expressions = [];
  const parameters = [];
  const expected = [];
  for (const [, expression, firstParameter, secondParameter, firstValue, secondValue] of cases) {
    expressions.push(expression, expression);
    parameters.push(firstParameter, secondParameter);
    expected.push(firstValue, secondValue);
  }
  const sql = `VALUES (${expressions.join(', ')})`;
  try {
    const definitions = cases.map(([type], index) => `C${index} ${type}`);
    await db.execute(`CREATE TABLE ${await fixtureSchema(db)}.${table} (ID INTEGER NOT NULL PRIMARY KEY, ${definitions.join(', ')})`);
    created = true;

    const direct = await db.query(sql, parameters);
    assert.deepEqual(direct.rows, [expected]);

    const statement = await db.prepare(sql);
    try {
      assert.deepEqual((await statement.query(parameters)).rows, [expected]);
    } finally { statement.close(); }

    const streamed = [];
    for await (const row of db.queryStreamWithParams(sql, parameters)) streamed.push(row.values);
    assert.deepEqual(streamed, [expected]);

    const inputRow = (id, first) => [id, ...cases.map(([, , firstParameter, secondParameter]) => first ? firstParameter : secondParameter)];
    const expectedRow = (id, first) => [id, ...cases.map(([, , , , firstValue, secondValue]) => first ? firstValue : secondValue)];
    const insertSql = `INSERT INTO ${table} VALUES (${Array(cases.length + 1).fill('?').join(', ')})`;
    const insert = await db.prepare(insertSql);
    try { assert.equal(await insert.execute(inputRow(1, true)), 1); }
    finally { insert.close(); }

    assert.equal(await db.execute(insertSql, inputRow(2, false)), 1);
    assert.equal(await db.executeBatch(insertSql, [inputRow(3, true), inputRow(4, false)]), 2);
    assert.deepEqual((await db.query(`SELECT * FROM ${table} ORDER BY ID`)).rows, [
      expectedRow(1, true), expectedRow(2, false), expectedRow(3, true), expectedRow(4, false),
    ]);
  } finally {
    if (created) await db.execute(`DROP TABLE ${await fixtureSchema(db)}.${table}`);
    await db.close();
  }
});

test('live Db2 rejects invalid DML and batch parameter conversions without corrupting prior rows', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  const table = `TS_DB2_BAD_BATCH_${randomUUID().replaceAll('-', '').slice(0, 20).toUpperCase()}`;
  let created = false;
  try {
    await db.execute(`CREATE TABLE ${await fixtureSchema(db)}.${table} (V INTEGER NOT NULL)`);
    created = true;
    const insertSql = `INSERT INTO ${table} VALUES (?)`;
    const invalid = [Buffer.from('123')];

    await assert.rejects(db.execute(insertSql, invalid), TypeError);
    const prepared = await db.prepare(insertSql);
    try { await assert.rejects(prepared.execute(invalid), TypeError); }
    finally { prepared.close(); }

    await assert.rejects(db.executeBatch(insertSql, [[10], invalid]), (error) =>
      error instanceof Db2BatchExecutionError
      && error.parameterSetIndex === 1
      && error.completedParameterSets === 1
      && error.rowsAffected === 1
      && error.cause instanceof TypeError,
    );
    assert.deepEqual((await db.query(`SELECT V FROM ${table}`)).rows, [[10]]);
    assert.equal(db.isClosed, false);
    assert.deepEqual((await db.query('VALUES 29')).rows, [[29]]);

    // B1: every query-path invalid pair must also reject locally in DML
    // (direct, prepared, batch second position) without writing the bad row.
    const invalidDmlCases = [
      { name: 'SMALLINT', type: 'SMALLINT', invalid: Buffer.from('123'), valid: 1, expectedValid: 1 },
      { name: 'BLOB', type: 'BLOB(20)', invalid: true, valid: Uint8Array.of(1, 2), expectedValid: Uint8Array.of(1, 2) },
      { name: 'VARBINARY', type: 'VARBINARY(20)', invalid: 123, valid: Uint8Array.of(1), expectedValid: Uint8Array.of(1) },
      { name: 'CLOB_BOOL', type: 'CLOB(20)', invalid: true, valid: 'hi', expectedValid: 'hi' },
      { name: 'CLOB_DEC', type: 'CLOB(20)', invalid: 12.5, valid: 'hi', expectedValid: 'hi' },
      { name: 'DBCLOB', type: 'DBCLOB(20)', invalid: false, valid: 'hi', expectedValid: 'hi' },
      { name: 'XML', type: 'XML', invalid: true, valid: '<r/>', expectedValid: '<r/>' },
    ];
    for (const { name, type, invalid, valid, expectedValid } of invalidDmlCases) {
      const dmlTable = `${table}_${name}`;
      await db.execute(`CREATE TABLE ${await fixtureSchema(db)}.${dmlTable} (V ${type})`);
      try {
        const dmlInsert = `INSERT INTO ${dmlTable} VALUES (?)`;
        await assert.rejects(db.execute(dmlInsert, [invalid]), TypeError, name);
        const dmlPrepared = await db.prepare(dmlInsert);
        try { await assert.rejects(dmlPrepared.execute([invalid]), TypeError, name); }
        finally { dmlPrepared.close(); }
        await assert.rejects(db.executeBatch(dmlInsert, [[valid], [invalid]]), (error) =>
          error instanceof Db2BatchExecutionError
          && error.parameterSetIndex === 1
          && error.completedParameterSets === 1
          && error.rowsAffected === 1
          && error.cause instanceof TypeError, name);
        assert.deepEqual((await db.query(`SELECT V FROM ${dmlTable}`)).rows, [[expectedValid]], name);
        assert.equal(db.isClosed, false);
        assert.deepEqual((await db.query('VALUES 29')).rows, [[29]]);
      } finally {
        await dropFixtureTable(db, dmlTable);
      }
    }
  } finally {
    try {
      if (created && !db.isClosed) await dropFixtureTable(db, table);
    } finally {
      await db.close();
    }
  }
});

test('live Db2 materializes EXTDTA XML and large object values', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  try {
    const xml = await db.query(`SELECT XMLPARSE(DOCUMENT '<root><item id="1">x</item></root>') FROM SYSIBM.SYSDUMMY1`);
    assert.equal(xml.rows[0]?.[0], '<root><item id="1">x</item></root>');

    const nullXml = await db.query('SELECT CAST(NULL AS XML) FROM SYSIBM.SYSDUMMY1');
    assert.equal(nullXml.rows[0]?.[0], null);

    const largeXml = await db.query('SELECT XMLELEMENT(NAME "r", XMLAGG(XMLELEMENT(NAME "c", COLNAME))) FROM SYSCAT.COLUMNS');
    const xmlText = largeXml.rows[0]?.[0];
    assert.equal(typeof xmlText, 'string');
    assert.ok(xmlText.length >= 100_000, `large XML is ${xmlText.length} characters`);
    assert.ok(xmlText.startsWith('<r><c>'));
    assert.ok(xmlText.endsWith('</c></r>'));

    const blob = await db.query(`SELECT BLOB(CAST(REPEAT('z', 30000) AS VARCHAR(30000) FOR BIT DATA)) || BLOB(CAST(REPEAT('y', 30000) AS VARCHAR(30000) FOR BIT DATA)) FROM SYSIBM.SYSDUMMY1`);
    const bytes = blob.rows[0]?.[0];
    assert.ok(bytes instanceof Uint8Array);
    assert.equal(bytes.length, 60_000);
    assert.equal(Buffer.from(bytes).toString(), 'z'.repeat(30_000) + 'y'.repeat(30_000));

    const xmlRows = await db.query('SELECT XMLELEMENT(NAME "t", TABNAME) FROM SYSCAT.TABLES FETCH FIRST 5 ROWS ONLY');
    assert.equal(xmlRows.rows.length, 5);
    assert.ok(xmlRows.rows.every(([value]) => typeof value === 'string' && value.startsWith('<t>')));
  } finally { await db.close(); }
});

test('live Db2 parameterized BLOB query returns the complete value instead of a progressive reference', { skip: !dsn, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const table = `TS_DB2_LOB_DDF_${randomUUID().replaceAll('-', '').slice(0, 16).toUpperCase()}`;
  const size = 2_500_000;
  const expectedHash = createHash('sha256').update(Buffer.alloc(size, 0x5a)).digest('hex');
  const blobInput = () => ({
    source: (async function* () {
      let remaining = size;
      while (remaining > 0) {
        const count = Math.min(64 * 1024, remaining);
        yield Buffer.alloc(count, 0x5a);
        remaining -= count;
      }
    })(),
    length: size,
  });
  let created = false;
  try {
    await db.execute(`CREATE TABLE ${await fixtureSchema(db)}.${table} (ID INTEGER NOT NULL PRIMARY KEY, B BLOB(5M))`);
    created = true;
    await db.execute(`INSERT INTO ${table} VALUES (1, CAST(? AS BLOB(5M)))`, [blobInput()]);

    const assertBlob = (value) => {
      assert.ok(value instanceof Uint8Array);
      assert.equal(value.length, size);
      assert.equal(createHash('sha256').update(value).digest('hex'), expectedHash);
    };
    assertBlob((await db.query(`SELECT B FROM ${table} WHERE ID = ?`, [1])).rows[0]?.[0]);
    const statement = await db.prepare(`SELECT B FROM ${table} WHERE ID = ?`);
    try { assertBlob((await statement.query([1])).rows[0]?.[0]); }
    finally { statement.close(); }
    const rowStream = db.queryStreamWithParams(`SELECT B FROM ${table} WHERE ID = ?`, [1]);
    try {
      const first = await rowStream.next();
      assert.equal(first.done, false);
      const value = first.value.values[0];
      assert.equal(value.kind, 'binary-lob', 'queryStream exposes the progressive locator before the caller consumes it');
      assert.equal(value.length, BigInt(size));
      let byteCount = 0;
      const hash = createHash('sha256');
      for await (const chunk of value.stream()) {
        byteCount += chunk.length;
        hash.update(chunk);
      }
      assert.equal(byteCount, size);
      assert.equal(hash.digest('hex'), expectedHash);
      assert.equal((await rowStream.next()).done, true);
    } finally { await rowStream.return(); }
    assert.deepEqual((await db.query('VALUES 29')).rows, [[29]], 'the same physical session remains usable after streaming the LOB');
  } finally {
    if (created) await dropFixtureTable(db, table);
    await db.close();
  }
});

test('live Db2 transaction handles commit and roll back atomically', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  const table = `TS_DB2_TX_${Date.now().toString(36).toUpperCase()}`;
  let created = false;
  let transaction;
  try {
    await db.execute(`CREATE TABLE ${await fixtureSchema(db)}.${table} (V INTEGER)`);
    created = true;

    transaction = await db.beginTransaction();
    await transaction.execute(`INSERT INTO ${table} VALUES (1)`);
    await transaction.rollback();
    transaction = undefined;
    assert.deepEqual((await db.query(`SELECT COUNT(*) FROM ${table}`)).rows, [[0]]);
    assert.equal(db.isAutoCommit, true);

    transaction = await db.beginTransaction();
    await transaction.execute(`INSERT INTO ${table} VALUES (2)`);
    await transaction.commit();
    transaction = undefined;
    assert.deepEqual((await db.query(`SELECT V FROM ${table}`)).rows, [[2]]);
    assert.equal(db.isAutoCommit, true);
  } finally {
    if (transaction) await transaction.rollback().catch(() => undefined);
    if (created) await db.execute(`DROP TABLE ${await fixtureSchema(db)}.${table}`);
    await db.close();
  }
});

test('live Db2 returns generated identity values for direct and prepared inserts', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  const table = `TS_DB2_ID_${Date.now().toString(36).toUpperCase()}`;
  let created = false;
  try {
    await db.execute(`CREATE TABLE ${await fixtureSchema(db)}.${table} (ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY, V VARCHAR(32))`);
    created = true;
    assert.deepEqual(await db.executeInsert(`INSERT INTO ${table} (V) VALUES (?)`, ['direct']), { rowsAffected: 1, lastInsertId: 1n });
    const statement = await db.prepare(`INSERT INTO ${table} (V) VALUES (?)`);
    assert.deepEqual(await statement.executeInsert(['prepared']), { rowsAffected: 1, lastInsertId: 2n });
    statement.close();
    assert.deepEqual((await db.query(`SELECT ID, V FROM ${table} ORDER BY ID`)).rows, [[1, 'direct'], [2, 'prepared']]);
  } finally {
    if (created) await db.execute(`DROP TABLE ${await fixtureSchema(db)}.${table}`);
    await db.close();
  }
});

test('live Db2 updates client accounting registers on one session', { skip: !dsn }, async () => {
  const db = await Db2Connection.connect(dsn);
  const base = db.getClientInfo();
  try {
    await db.setClientInfo({ applicationName: "audit'service", workstationName: 'db2-test-worker', userId: 'audit_user', accounting: 'cost_center_7' });
    assert.deepEqual(db.getClientInfo(), { ...base, applicationName: "audit'service", workstationName: 'db2-test-worker', userId: 'audit_user', accounting: 'cost_center_7' });
    const current = await db.query("SELECT COALESCE(CURRENT CLIENT_APPLNAME, ''), COALESCE(CURRENT CLIENT_WRKSTNNAME, ''), COALESCE(CURRENT CLIENT_USERID, ''), COALESCE(CURRENT CLIENT_ACCTNG, '') FROM SYSIBM.SYSDUMMY1");
    assert.deepEqual(current.rows, [["audit'service", 'db2-test-worker', 'audit_user', 'cost_center_7']]);
    await db.resetClientInfo(base);
    assert.deepEqual(db.getClientInfo(), base);
    const restored = await db.query("SELECT COALESCE(CURRENT CLIENT_APPLNAME, ''), COALESCE(CURRENT CLIENT_WRKSTNNAME, ''), COALESCE(CURRENT CLIENT_USERID, ''), COALESCE(CURRENT CLIENT_ACCTNG, '') FROM SYSIBM.SYSDUMMY1");
    assert.deepEqual(restored.rows, [[base.applicationName, base.workstationName, base.userId, base.accounting]]);

    const config = parseDsn(dsn);
    Object.assign(config, { clientApplName: 'configured_app', clientWrkstnName: 'configured_worker', clientUserid: 'configured_user', clientAcctng: 'configured_acct' });
    const configured = await Db2Connection.connect(config);
    try {
      const initial = await configured.query("SELECT COALESCE(CURRENT CLIENT_APPLNAME, ''), COALESCE(CURRENT CLIENT_WRKSTNNAME, ''), COALESCE(CURRENT CLIENT_USERID, ''), COALESCE(CURRENT CLIENT_ACCTNG, '') FROM SYSIBM.SYSDUMMY1");
      assert.deepEqual(initial.rows, [['configured_app', 'configured_worker', 'configured_user', 'configured_acct']]);
    } finally { await configured.close(); }
  } finally { await db.close(); }
});

test('live Db2 pool reuses and closes a bounded session', { skip: !dsn }, async () => {
  const pool = new Db2Pool(dsn, { maxConnections: 1 });
  const table = `TS_DB2_POOL_TX_${Date.now().toString(36).toUpperCase()}`;
  let created = false;
  try {
    assert.deepEqual((await pool.query('VALUES CAST(42 AS INTEGER)')).rows, [[42]]);
    assert.equal(pool.connectionCount, 1);
    assert.equal(pool.idleCount, 1);
    assert.deepEqual((await pool.query('VALUES CAST(? AS INTEGER)', [73])).rows, [[73]]);
    assert.equal(pool.connectionCount, 1);
    assert.equal(pool.idleCount, 1);

    const baseline = await pool.withConnection((connection) => Promise.resolve(connection.getClientInfo()));
    await pool.withConnection((connection) => connection.setClientInfo({ applicationName: 'pooled_tenant', userId: 'tenant_user', accounting: 'tenant_batch' }));
    const registers = await pool.withConnection(async (connection) => (await connection.query("SELECT COALESCE(CURRENT CLIENT_APPLNAME, ''), COALESCE(CURRENT CLIENT_WRKSTNNAME, ''), COALESCE(CURRENT CLIENT_USERID, ''), COALESCE(CURRENT CLIENT_ACCTNG, '') FROM SYSIBM.SYSDUMMY1")).rows[0]);
    assert.deepEqual(registers, [baseline.applicationName, baseline.workstationName, baseline.userId, baseline.accounting]);

    await pool.execute(`CREATE TABLE ${await fixtureSchema(pool)}.${table} (V INTEGER)`);
    created = true;
    let transactionHandle;
    let abandonedConnection;
    await pool.withConnection(async (connection) => {
      abandonedConnection = connection;
      transactionHandle = await connection.beginTransaction();
      await transactionHandle.execute(`INSERT INTO ${table} VALUES (1)`);
    });
    await assert.rejects(async () => transactionHandle.execute(`INSERT INTO ${table} VALUES (2)`), /no longer active/);
    assert.equal(abandonedConnection.isClosed, true, 'a session returned with an unfinished transaction must be discarded');
    assert.equal(pool.connectionCount, 0);
    assert.equal(pool.idleCount, 0);

    assert.deepEqual((await pool.query(`SELECT COUNT(*) FROM ${table}`)).rows, [[0]]);
    assert.equal(pool.connectionCount, 1);
    assert.equal(pool.idleCount, 1);
  } finally {
    if (created && !pool.isClosed) await pool.execute(`DROP TABLE ${await fixtureSchema(pool)}.${table}`).catch(() => undefined);
    await pool.close();
  }
  assert.equal(pool.connectionCount, 0);
});

test('live Db2 pool restores schema and isolation registers after raw SET', { skip: !dsn }, async () => {
  const pool = new Db2Pool(dsn, { maxConnections: 1 });
  try {
    const baseline = await pool.withConnection(async (connection) => ({
      schema: (await connection.query('VALUES CURRENT SCHEMA')).rows,
      isolation: (await connection.query('VALUES CURRENT ISOLATION')).rows,
    }));
    // Borrower A changes session state through raw SQL, outside any transaction scope.
    await pool.withConnection(async (connection) => {
      await connection.execute('SET CURRENT SCHEMA JBL_LIVE');
      await connection.execute('SET CURRENT ISOLATION UR');
      assert.deepEqual((await connection.query('VALUES CURRENT SCHEMA')).rows, [['JBL_LIVE']]);
      assert.deepEqual((await connection.query('VALUES CURRENT ISOLATION')).rows, [['UR']]);
    });
    assert.equal(pool.connectionCount, 1, 'a clean lease return must keep the session pooled');
    // Borrower B observes the exact baseline and runs a prepared query on the same lease.
    await pool.withConnection(async (connection) => {
      assert.deepEqual((await connection.query('VALUES CURRENT SCHEMA')).rows, baseline.schema);
      assert.deepEqual((await connection.query('VALUES CURRENT ISOLATION')).rows, baseline.isolation);
      const statement = await connection.prepare('VALUES 29');
      try { assert.deepEqual((await statement.query([])).rows, [[29]]); }
      finally { statement.close(); }
    });
    assert.equal(pool.connectionCount, 1);
    assert.equal(pool.idleCount, 1);
  } finally { await pool.close(); }
});

test('live Db2 pool restores raw client registers before reusing a one-slot session', { skip: !dsn }, async () => {
  const pool = new Db2Pool(dsn, { maxConnections: 1 });
  const readRegisters = async (connection) => (await connection.query(
    "SELECT COALESCE(CURRENT CLIENT_APPLNAME, ''), COALESCE(CURRENT CLIENT_USERID, ''), COALESCE(CURRENT CLIENT_WRKSTNNAME, ''), COALESCE(CURRENT CLIENT_ACCTNG, '') FROM SYSIBM.SYSDUMMY1",
  )).rows[0];
  try {
    const baseline = await pool.withConnection(readRegisters);
    await pool.withConnection(async (connection) => {
      await connection.execute("CALL SYSPROC.WLM_SET_CLIENT_INFO('raw_pool_user', 'raw_pool_host', 'raw_pool_app', 'raw_pool_accounting', NULL)");
      assert.deepEqual(await readRegisters(connection), ['raw_pool_app', 'raw_pool_user', 'raw_pool_host', 'raw_pool_accounting']);
    });
    assert.equal(pool.connectionCount, 1);
    const restored = await pool.withConnection(readRegisters);
    assert.deepEqual(restored, baseline, 'raw client-register changes must be observed and reset on the server before reuse');
    assert.equal(pool.connectionCount, 1, 'the same healthy physical connection should remain pooled');
    assert.equal(pool.idleCount, 1);
  } finally { await pool.close(); }
});

test('live Db2 AbortSignal cancels queued pool queries without interrupting the active lease', { skip: !dsn }, async () => {
  const pool = new Db2Pool(dsn, { maxConnections: 1 });
  let enterActive;
  const activeEntered = new Promise((resolve) => { enterActive = resolve; });
  let releaseActive;
  const activeGate = new Promise((resolve) => { releaseActive = resolve; });
  let active;
  try {
    active = pool.withConnection(async () => {
      enterActive();
      await activeGate;
      return 17;
    });
    await activeEntered;

    const queryAbort = new AbortController();
    const waitingQuery = pool.query('VALUES CAST(41 AS INTEGER)', undefined, { signal: queryAbort.signal });
    queryAbort.abort();
    await assert.rejects(waitingQuery, (error) => error.name === 'AbortError');

    const streamAbort = new AbortController();
    const stream = pool.queryStream('VALUES CAST(42 AS INTEGER)', { signal: streamAbort.signal });
    const waitingStream = stream.next();
    streamAbort.abort();
    await assert.rejects(waitingStream, (error) => error.name === 'AbortError');

    assert.equal(pool.connectionCount, 1);
    assert.equal(pool.idleCount, 0);
    releaseActive();
    assert.equal(await active, 17);
    assert.equal(pool.idleCount, 1);
    assert.deepEqual((await pool.query('VALUES CAST(43 AS INTEGER)')).rows, [[43]]);
    assert.equal(pool.connectionCount, 1);
    assert.equal(pool.idleCount, 1);
  } finally {
    releaseActive();
    await active?.catch(() => undefined);
    await pool.close();
  }
  assert.equal(pool.connectionCount, 0);
});

test('live Db2 pool returns a session after a server SQL error', { skip: !dsn }, async () => {
  const pool = new Db2Pool(dsn, { maxConnections: 1 });
  try {
    await assert.rejects(pool.query('SELECT FROM SYSIBM.SYSDUMMY1'), /SQLCODE=/);
    assert.equal(pool.connectionCount, 1);
    assert.equal(pool.idleCount, 1);

    assert.deepEqual((await pool.query('VALUES 91')).rows, [[91]]);
    assert.equal(pool.connectionCount, 1);
    assert.equal(pool.idleCount, 1);
  } finally { await pool.close(); }
});

test('live Db2 procedure returns OUT and INOUT parameters', { skip: !dsn, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const procedure = `TS_OUT_${randomUUID().replaceAll('-', '').slice(0, 20).toUpperCase()}`;
  let created = false;
  try {
    await db.execute(`CREATE OR REPLACE PROCEDURE ${await fixtureSchema(db)}.${procedure} (
      IN a INT, IN b INT, OUT sum_val INT, OUT prod_val INT, INOUT msg VARCHAR(100)
    ) LANGUAGE SQL
    BEGIN
      SET sum_val = a + b;
      SET prod_val = a * b;
      SET msg = 'OK: ' || msg;
    END`);
    created = true;
    const statement = await db.prepare(`CALL ${procedure}(?, ?, ?, ?, ?)`);
    const streamed = await statement.executeStream([
      { value: 15, direction: 'input' },
      { value: 4, direction: 'input' },
      { value: 0, direction: 'output' },
      { value: 0, direction: 'output' },
      { value: 'Alpha', direction: 'inputOutput' },
    ]);
    assert.equal(streamed.resultSetCount, 0);
    assert.throws(() => streamed.outputParameters, /only after finish/);
    assert.deepEqual((await streamed.finish()).outputParameters, [null, null, 19, 60, 'OK: Alpha']);
    const result = await statement.executeWithOutputs([
      { value: 15, direction: 'input' },
      { value: 4, direction: 'input' },
      { value: 0, direction: 'output' },
      { value: 0, direction: 'output' },
      { value: 'Alpha', direction: 'inputOutput' },
    ]);
    assert.equal(result.rowsAffected, 0);
    assert.deepEqual(result.outputParameters, [null, null, 19, 60, 'OK: Alpha']);
  } finally {
    if (created) await db.execute(`DROP PROCEDURE ${await fixtureSchema(db)}.${procedure}`).catch(() => undefined);
    await db.close();
  }
});

test('live Db2 rejects an out-of-range INOUT value before sending and keeps the session', { skip: !dsn, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const procedure = `TS_INOUT_RANGE_${randomUUID().replaceAll('-', '').slice(0, 14).toUpperCase()}`;
  let created = false;
  try {
    await db.execute(`CREATE PROCEDURE ${await fixtureSchema(db)}.${procedure} (IN a INT, INOUT b SMALLINT, OUT c INT)
      LANGUAGE SQL
      BEGIN
        SET c = a + b;
        SET b = b + 1;
      END`);
    created = true;
    const statement = await db.prepare(`CALL ${procedure}(?, ?, ?)`);
    await assert.rejects(statement.executeWithOutputs([
      { value: 1, direction: 'input' },
      { value: 32768, direction: 'inputOutput' },
      { value: 0, direction: 'output' },
    ]), RangeError);
    // The encoder rejects the value before any SQLDTA is written, so the session must stay usable.
    assert.deepEqual((await db.query('VALUES 7')).rows, [[7]]);
  } finally {
    if (created) await db.execute(`DROP PROCEDURE ${await fixtureSchema(db)}.${procedure}`).catch(() => undefined);
    await db.close();
  }
});

test('live Db2 surfaces a procedure error raised after output assignment and keeps the session', { skip: !dsn, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const procedure = `TS_OUT_FAIL_${randomUUID().replaceAll('-', '').slice(0, 16).toUpperCase()}`;
  let created = false;
  try {
    await db.execute(`CREATE PROCEDURE ${await fixtureSchema(db)}.${procedure} (OUT a INT, OUT b VARCHAR(20))
      LANGUAGE SQL
      BEGIN
        SET a = 11;
        SET b = 'set-before-failure';
        SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT = 'invalid output value';
      END`);
    created = true;
    const statement = await db.prepare(`CALL ${procedure}(?, ?)`);
    await assert.rejects(statement.executeWithOutputs([
      { value: 0, direction: 'output' },
      { value: '', direction: 'output' },
    ]), (error) => /SQLCODE/u.test(error.message) && /75001/u.test(error.message));
    assert.deepEqual((await db.query('VALUES 7')).rows, [[7]]);
  } finally {
    if (created) await db.execute(`DROP PROCEDURE ${await fixtureSchema(db)}.${procedure}`).catch(() => undefined);
    await db.close();
  }
});

test('live Db2 procedure OUT values preserve scalar, binary, LOB, and XML types', { skip: !dsn, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const procedure = `TS_TYPED_OUT_${randomUUID().replaceAll('-', '').slice(0, 16).toUpperCase()}`;
  let created = false;
  let statement;
  const binary = Uint8Array.of(0, 0x7f, 0x80, 0xff);
  try {
    await db.execute(`CREATE PROCEDURE ${await fixtureSchema(db)}.${procedure} (
      OUT small_value SMALLINT,
      OUT decimal_value DECIMAL(9,3),
      OUT decfloat_value DECFLOAT(16),
      OUT date_value DATE,
      OUT timestamp_value TIMESTAMP,
      OUT text_value VARCHAR(30),
      OUT varbinary_value VARBINARY(10),
      OUT blob_value BLOB(100),
      OUT clob_value CLOB(100),
      OUT dbclob_value DBCLOB(100),
      OUT xml_value XML,
      OUT boolean_value BOOLEAN
    ) LANGUAGE SQL BEGIN
      SET small_value = -32768;
      SET decimal_value = 42.125;
      SET decfloat_value = DECFLOAT('Infinity');
      SET date_value = DATE('2026-08-26');
      SET timestamp_value = TIMESTAMP('2026-08-26 12:34:56.123456');
      SET text_value = 'Zażółć 東京';
      SET varbinary_value = CAST(X'007F80FF' AS VARBINARY(10));
      SET blob_value = BLOB(X'007F80FF');
      SET clob_value = 'CLOB café';
      SET dbclob_value = CAST('DBCLOB 東京' AS DBCLOB(100));
      SET xml_value = XMLPARSE(DOCUMENT '<root/>');
      SET boolean_value = TRUE;
    END`);
    created = true;

    statement = await db.prepare(`CALL ${procedure}(${Array(12).fill('?').join(', ')})`);
    const result = await statement.executeWithOutputs([
      { value: 0, direction: 'output' },
      { value: '0', direction: 'output' },
      { value: '0', direction: 'output' },
      { value: '2000-01-01', direction: 'output' },
      { value: '2000-01-01 00:00:00', direction: 'output' },
      { value: '', direction: 'output' },
      { value: new Uint8Array(), direction: 'output' },
      { value: new Uint8Array(), direction: 'output' },
      { value: '', direction: 'output' },
      { value: '', direction: 'output' },
      { value: '<empty/>', direction: 'output' },
      { value: false, direction: 'output' },
    ]);
    assert.equal(result.rowsAffected, 0);
    const actual = result.outputParameters;
    assert.equal(actual[0], -32768);
    assert.equal(actual[1], '42.125');
    assert.equal(actual[2], 'Infinity');
    assert.equal(actual[3], '2026-08-26');
    assert.equal(actual[4], '2026-08-26-12.34.56.123456');
    assert.equal(actual[5], 'Zażółć 東京');
    assert.deepEqual(actual[6], binary);
    assert.deepEqual(actual[7], binary);
    assert.equal(actual[8], 'CLOB café');
    assert.equal(actual[9], 'DBCLOB 東京');
    assert.equal(actual[10], '<root/>');
    assert.equal(actual[11], true);
  } finally {
    statement?.close();
    if (created) await db.execute(`DROP PROCEDURE ${await fixtureSchema(db)}.${procedure}`).catch(() => undefined);
    await db.close();
  }
});

test('live Db2 procedure OUT NULLs stay NULL across all types', { skip: !dsn, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const procedure = `TS_NULLOUT_${randomUUID().replaceAll('-', '').slice(0, 16).toUpperCase()}`;
  let created = false;
  let statement;
  try {
    await db.execute(`CREATE PROCEDURE ${await fixtureSchema(db)}.${procedure} (
      OUT small_value SMALLINT,
      OUT decimal_value DECIMAL(9,3),
      OUT decfloat_value DECFLOAT(16),
      OUT date_value DATE,
      OUT timestamp_value TIMESTAMP,
      OUT text_value VARCHAR(30),
      OUT varbinary_value VARBINARY(10),
      OUT blob_value BLOB(100),
      OUT clob_value CLOB(100),
      OUT dbclob_value DBCLOB(100),
      OUT xml_value XML,
      OUT boolean_value BOOLEAN
    ) LANGUAGE SQL BEGIN
      SET small_value = NULL;
      SET decimal_value = NULL;
      SET decfloat_value = NULL;
      SET date_value = NULL;
      SET timestamp_value = NULL;
      SET text_value = NULL;
      SET varbinary_value = NULL;
      SET blob_value = NULL;
      SET clob_value = NULL;
      SET dbclob_value = NULL;
      SET xml_value = NULL;
      SET boolean_value = NULL;
    END`);
    created = true;
    statement = await db.prepare(`CALL ${procedure}(${Array(12).fill('?').join(', ')})`);
    const result = await statement.executeWithOutputs([
      { value: 1, direction: 'output' },
      { value: '1', direction: 'output' },
      { value: '1', direction: 'output' },
      { value: '2026-01-02', direction: 'output' },
      { value: '2026-01-02 03:04:05.000001', direction: 'output' },
      { value: 'seed', direction: 'output' },
      { value: Uint8Array.of(1), direction: 'output' },
      { value: Uint8Array.of(2), direction: 'output' },
      { value: 'seed', direction: 'output' },
      { value: 'seed', direction: 'output' },
      { value: '<seed/>', direction: 'output' },
      { value: true, direction: 'output' },
    ]);
    assert.equal(result.outputParameters.length, 12);
    for (const value of result.outputParameters) assert.equal(value, null);
  } finally {
    statement?.close();
    if (created) await db.execute(`DROP PROCEDURE ${await fixtureSchema(db)}.${procedure}`).catch(() => undefined);
    await db.close();
  }
});

test('live Db2 procedure OUT LOB with result cursor routes EXTDTA to its owner', { skip: !dsn, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const procedure = `TS_OUTLOB_${randomUUID().replaceAll('-', '').slice(0, 16).toUpperCase()}`;
  let created = false;
  let statement;
  const phase = (name) => console.error(`[live ts OUT+cursor] ${name}`);
  try {
    phase('CREATE start');
    created = true; // Cleanup also handles a lost CREATE reply after the server committed the DDL.
    await db.execute(`CREATE PROCEDURE ${await fixtureSchema(db)}.${procedure} (
      OUT out_blob BLOB(100),
      OUT out_text VARCHAR(20)
    ) LANGUAGE SQL DYNAMIC RESULT SETS 1 BEGIN
      DECLARE C CURSOR WITH RETURN TO CALLER FOR SELECT 42 AS V, BLOB(X'007F80FF') AS PAYLOAD FROM SYSIBM.SYSDUMMY1;
      SET out_blob = BLOB(X'007F80FF');
      SET out_text = 'hello-世界';
      OPEN C;
    END`, undefined, boundedProcedureOptions);
    phase('CREATE done');
    phase('prepare start');
    statement = await db.prepare(`CALL ${procedure}(?, ?)`);
    phase('prepare done');
    phase('executeStream start');
    const streamed = await statement.executeStream([
      { value: new Uint8Array(), direction: 'output' },
      { value: '', direction: 'output' },
    ], boundedProcedureOptions);
    phase('executeStream done; rows start');
    assert.equal(streamed.resultSetCount, 1);
    assert.throws(() => streamed.outputParameters, /only after finish/);
    const rowIterator = streamed.rows()[Symbol.asyncIterator]();
    phase('first nextRow start');
    const firstRow = await rowIterator.next();
    phase(`first nextRow done=${firstRow.done}`);
    assert.deepEqual(firstRow, { value: [42, Uint8Array.of(0, 0x7f, 0x80, 0xff)], done: false });
    phase('second nextRow start');
    const terminalRow = await rowIterator.next();
    phase(`second nextRow done=${terminalRow.done}`);
    assert.deepEqual(terminalRow, { value: undefined, done: true });
    phase('rows done; finish start');
    assert.throws(() => streamed.outputParameters, /only after finish/);
    const streamedResult = await streamed.finish();
    phase('finish done');
    assert.deepEqual(streamedResult.outputParameters[0], Uint8Array.of(0, 0x7f, 0x80, 0xff));
    assert.equal(streamedResult.outputParameters[1], 'hello-世界');

    phase('executeWithOutputs start');
    const result = await statement.executeWithOutputs([
      { value: new Uint8Array(), direction: 'output' },
      { value: '', direction: 'output' },
    ], boundedProcedureOptions);
    phase('executeWithOutputs done; reuse query start');
    assert.deepEqual(result.outputParameters[0], Uint8Array.of(0, 0x7f, 0x80, 0xff));
    assert.equal(result.outputParameters[1], 'hello-世界');
    assert.equal(result.resultSets.length, 1);
    assert.equal(result.resultSets[0].columns[1].name, 'PAYLOAD');
    assert.deepEqual(result.resultSets[0].rows, [[42, Uint8Array.of(0, 0x7f, 0x80, 0xff)]]);
    assert.deepEqual((await db.query('VALUES 91', undefined, boundedProcedureOptions)).rows, [[91]]);
    phase('reuse query done');
  } finally {
    phase('cleanup: close original session start');
    statement?.close();
    await db.close();
    phase('cleanup: close original session done');
    if (created) {
      phase('cleanup: fresh-session DROP start');
      await dropFixtureProcedure(procedure).then(() => phase('cleanup: fresh-session DROP done'))
        .catch(() => phase('cleanup: fresh-session DROP failed'));
    }
  }
});

test('live Db2 CALL drains ordered procedure result sets and reuses the connection', { skip: !dsn, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const procedure = `TS_RS_${randomUUID().replaceAll('-', '').slice(0, 18).toUpperCase()}`;
  const values = Array.from({ length: 130 }, (_, index) => `(${index + 1})`).join(', ');
  const digits = Array.from({ length: 10 }, (_, index) => `(${index})`).join(', ');
  const dimensions = Array.from({ length: 5 }, (_, index) => `(VALUES ${digits}) AS D${index}(N)`).join(', ');
  const orderBy = Array.from({ length: 5 }, (_, index) => `D${index}.N`).join(', ');
  const largeQuery = `SELECT ROW_NUMBER() OVER (ORDER BY ${orderBy}) AS N FROM ${dimensions} FETCH FIRST 12000 ROWS ONLY`;
  let created = false;
  try {
    await db.execute(`CREATE PROCEDURE ${await fixtureSchema(db)}.${procedure} (IN P INTEGER) LANGUAGE SQL DYNAMIC RESULT SETS 4 BEGIN DECLARE C1 CURSOR WITH RETURN TO CALLER FOR SELECT P AS VALUE FROM SYSIBM.SYSDUMMY1; DECLARE C2 CURSOR WITH RETURN TO CALLER FOR SELECT N FROM (VALUES ${values}) AS T(N); DECLARE C3 CURSOR WITH RETURN TO CALLER FOR ${largeQuery}; DECLARE C4 CURSOR WITH RETURN TO CALLER FOR SELECT P AS VALUE FROM SYSIBM.SYSDUMMY1 WHERE 1 = 0; OPEN C1; OPEN C2; OPEN C3; OPEN C4; END`);
    created = true;

    const result = await db.query(`/* result-set path */ -- leading SQL comments\nCALL ${procedure}(?)`, [42]);
    assert.equal(result.resultSets?.length, 4);
    assert.deepEqual(result.resultSets[0].rows, [[42]]);
    assert.equal(result.resultSets[1].rows.length, 130);
    assert.deepEqual(result.resultSets[1].rows[0], [1]);
    assert.deepEqual(result.resultSets[1].rows.at(-1), [130]);
    assert.equal(result.resultSets[2].rows.length, 12_000);
    assert.deepEqual(result.resultSets[2].rows[0], [1n]);
    assert.deepEqual(result.resultSets[2].rows.at(-1), [12_000n]);
    assert.deepEqual(result.resultSets[3].rows, []);
    assert.equal(result.rows, result.resultSets[0].rows);
    assert.equal(await db.execute(`CALL ${procedure}(?)`, [42]), 0);
    assert.deepEqual((await db.query('VALUES 23')).rows, [[23]]);
  } finally {
    if (created) await db.execute(`DROP PROCEDURE ${await fixtureSchema(db)}.${procedure}`).catch(() => undefined);
    await db.close();
  }
});

test('live Db2 streaming procedure reader visits multiple blocks/result sets and preserves the connection', { skip: !dsn, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const procedure = `TS_RS_STREAM_${randomUUID().replaceAll('-', '').slice(0, 14).toUpperCase()}`;
  const digits = Array.from({ length: 10 }, (_, index) => `(${index})`).join(', ');
  const dimensions = Array.from({ length: 5 }, (_, index) => `(VALUES ${digits}) AS D${index}(N)`).join(', ');
  const orderBy = Array.from({ length: 5 }, (_, index) => `D${index}.N`).join(', ');
  const largeQuery = `SELECT ROW_NUMBER() OVER (ORDER BY ${orderBy}) AS N FROM ${dimensions} FETCH FIRST 12000 ROWS ONLY`;
  let created = false;
  try {
    created = true; // Cleanup also handles a lost CREATE reply after the server committed the DDL.
    await db.execute(`CREATE PROCEDURE ${await fixtureSchema(db)}.${procedure} () LANGUAGE SQL DYNAMIC RESULT SETS 3 BEGIN DECLARE C1 CURSOR WITH RETURN TO CALLER FOR ${largeQuery}; DECLARE C2 CURSOR WITH RETURN TO CALLER FOR SELECT 99 AS V FROM SYSIBM.SYSDUMMY1 WHERE 1 = 0; DECLARE C3 CURSOR WITH RETURN TO CALLER FOR SELECT 42 AS V FROM SYSIBM.SYSDUMMY1; OPEN C1; OPEN C2; OPEN C3; END`, undefined, boundedProcedureOptions);
    const reader = await db.executeProcedureStream(`CALL ${procedure}()`, [], boundedProcedureOptions);
    assert.equal(reader.resultSetCount, 3);
    let firstCount = 0;
    for await (const [value] of reader.rows()) {
      if (firstCount === 0) assert.equal(Number(value), 1);
      if (firstCount === 11_999) assert.equal(Number(value), 12_000);
      firstCount++;
    }
    assert.equal(firstCount, 12_000);
    assert.equal(await reader.nextResultSet(), true);
    assert.deepEqual(await Array.fromAsync(reader.rows()), []);
    assert.equal(await reader.nextResultSet(), true);
    assert.deepEqual(await Array.fromAsync(reader.rows()), [[42]]);
    assert.equal(await reader.nextResultSet(), false);
    await reader.finish();
    assert.deepEqual((await db.query('VALUES 23')).rows, [[23]]);
  } finally {
    await db.close();
    if (created) await dropFixtureProcedure(procedure).catch(() => undefined);
  }
});

test('live Db2 pooled procedure reader returns its lease only after finish', { skip: !dsn, timeout: 60_000 }, async () => {
  const pool = new Db2Pool(dsn, { maxConnections: 1 });
  const procedure = `TS_POOL_RS_${randomUUID().replaceAll('-', '').slice(0, 14).toUpperCase()}`;
  let created = false;
  try {
    created = true; // Cleanup also handles a lost CREATE reply after the server committed the DDL.
    await pool.execute(`CREATE PROCEDURE ${await fixtureSchema(pool)}.${procedure} () LANGUAGE SQL DYNAMIC RESULT SETS 1 BEGIN DECLARE C CURSOR WITH RETURN TO CALLER FOR SELECT 73 AS V FROM SYSIBM.SYSDUMMY1; OPEN C; END`, undefined, boundedProcedureOptions);
    const reader = await pool.executeProcedureStream(`CALL ${procedure}()`, [], boundedProcedureOptions);
    assert.equal(pool.idleCount, 0);
    assert.deepEqual(await Array.fromAsync(reader.rows()), [[73]]);
    await reader.finish();
    assert.equal(pool.idleCount, 1);
    assert.deepEqual((await pool.query('VALUES 74')).rows, [[74]]);
  } finally {
    await pool.close();
    if (created) await dropFixtureProcedure(procedure).catch(() => undefined);
  }
});

test('live Db2 procedure returns OUT values together with multiple result sets', { skip: !dsn, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const procedure = `TS_OUT_RS_${randomUUID().replaceAll('-', '').slice(0, 16).toUpperCase()}`;
  let created = false;
  try {
    await db.execute(`CREATE PROCEDURE ${await fixtureSchema(db)}.${procedure} (IN P INTEGER, OUT DOUBLED INTEGER) LANGUAGE SQL DYNAMIC RESULT SETS 2 BEGIN DECLARE C1 CURSOR WITH RETURN TO CALLER FOR SELECT P AS VALUE FROM SYSIBM.SYSDUMMY1; DECLARE C2 CURSOR WITH RETURN TO CALLER FOR SELECT N FROM (VALUES (P), (P + 1)) AS T(N); SET DOUBLED = P * 2; OPEN C1; OPEN C2; END`);
    created = true;
    const statement = await db.prepare(`CALL ${procedure}(?, ?)`);
    const result = await statement.executeWithOutputs([
      { value: 21, direction: 'input' },
      { value: 0, direction: 'output' },
    ]);
    assert.equal(result.rowsAffected, 0);
    assert.deepEqual(result.outputParameters, [null, 42]);
    assert.equal(result.resultSets?.length, 2);
    assert.deepEqual(result.resultSets[0].rows, [[21]]);
    assert.deepEqual(result.resultSets[1].rows, [[21], [22]]);
    assert.deepEqual((await db.query('VALUES 5')).rows, [[5]]);
  } finally {
    if (created) await db.execute(`DROP PROCEDURE ${await fixtureSchema(db)}.${procedure}`).catch(() => undefined);
    await db.close();
  }
});

test('live Db2 procedure result cursors return large BLOB, CLOB, and XML values', { skip: !dsn, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const procedure = `TS_RS_LOB_${randomUUID().replaceAll('-', '').slice(0, 14).toUpperCase()}`;
  let created = false;
  try {
    await db.execute(`CREATE PROCEDURE ${await fixtureSchema(db)}.${procedure} () LANGUAGE SQL DYNAMIC RESULT SETS 3 BEGIN DECLARE C1 CURSOR WITH RETURN TO CALLER FOR SELECT CAST(REPEAT('c', 30000) AS CLOB(100K)) || CAST(REPEAT('d', 30000) AS CLOB(100K)) || CAST(REPEAT('e', 10000) AS CLOB(100K)) AS VALUE FROM SYSIBM.SYSDUMMY1; DECLARE C2 CURSOR WITH RETURN TO CALLER FOR SELECT BLOB(CAST(REPEAT('x', 30000) AS VARCHAR(30000) FOR BIT DATA)) || BLOB(CAST(REPEAT('y', 30000) AS VARCHAR(30000) FOR BIT DATA)) || BLOB(CAST(REPEAT('z', 12000) AS VARCHAR(12000) FOR BIT DATA)) AS VALUE FROM SYSIBM.SYSDUMMY1; DECLARE C3 CURSOR WITH RETURN TO CALLER FOR SELECT XMLELEMENT(NAME "r", XMLAGG(XMLELEMENT(NAME "c", COLNAME))) AS VALUE FROM SYSCAT.COLUMNS; OPEN C1; OPEN C2; OPEN C3; END`);
    created = true;

    const result = await db.query(`CALL ${procedure}()`);
    assert.equal(result.resultSets?.length, 3);
    assert.deepEqual(result.resultSets[0].rows, [['c'.repeat(30_000) + 'd'.repeat(30_000) + 'e'.repeat(10_000)]]);
    const blob = result.resultSets[1].rows[0][0];
    assert.ok(blob instanceof Uint8Array);
    assert.equal(blob.length, 72_000);
    assert.equal(Buffer.from(blob).toString('ascii'), 'x'.repeat(30_000) + 'y'.repeat(30_000) + 'z'.repeat(12_000));
    const xml = result.resultSets[2].rows[0][0];
    assert.equal(typeof xml, 'string');
    assert.ok(xml.length >= 100_000, `procedure XML result is ${xml.length} characters`);
    assert.ok(xml.startsWith('<r><c>'));
    assert.ok(xml.endsWith('</c></r>'));
    assert.deepEqual((await db.query('VALUES 31')).rows, [[31]]);
  } finally {
    if (created) await db.execute(`DROP PROCEDURE ${await fixtureSchema(db)}.${procedure}`).catch(() => undefined);
    await db.close();
  }
});

test('live Db2 nullable and NOT NULL LOBs preserve a leading 0x00/NUL through query, stream, and procedure cursors', { skip: !dsn, timeout: 90_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const base = `TS_LOBNULL_${randomUUID().replaceAll('-', '').slice(0, 12).toUpperCase()}`;
  const table = base;
  const procedure = `${base}_P`;
  let created = false;
  let procedureCreated = false;
  try {
    await db.execute(`CREATE TABLE ${await fixtureSchema(db)}.${table} (ID INTEGER, BN BLOB(2M) NOT NULL, B BLOB(2M), CN CLOB(2M) NOT NULL, C CLOB(2M), DN DBCLOB(2M) NOT NULL, D DBCLOB(2M))`);
    created = true;
    const size = 70_000;
    const bn = new Uint8Array(size); for (let i = 1; i < size; i++) bn[i] = i & 0xff;
    const b = new Uint8Array(size); for (let i = 1; i < size; i++) b[i] = (i * 3) & 0xff;
    const cn = '\u0000' + 'a'.repeat(size - 1);
    const c = '\u0000' + 'b'.repeat(size - 1);
    const dn = '\u0000' + '漢'.repeat(size - 1);
    const d = '\u0000' + '字'.repeat(size - 1);
    await db.execute(`INSERT INTO ${table} VALUES (1, ?, ?, ?, ?, ?, ?)`, [bn, b, cn, c, dn, d], { timeoutMs: 120_000 });
    await db.execute(`INSERT INTO ${table} VALUES (2, CAST('' AS BLOB(2M)), NULL, CAST('' AS CLOB(2M)), NULL, CAST('' AS DBCLOB(2M)), NULL)`);
    await db.execute(`CREATE PROCEDURE ${await fixtureSchema(db)}.${procedure} () LANGUAGE SQL DYNAMIC RESULT SETS 1 BEGIN DECLARE CUR CURSOR WITH RETURN TO CALLER FOR SELECT ID, BN, B, CN, C, DN, D FROM ${table} ORDER BY ID; OPEN CUR; END`);
    procedureCreated = true;

    const assertRow = (label, row) => {
      const [id, bnv, bv, cnv, cv, dnv, dv] = row;
      assert.equal(id, 1, `${label} id`);
      assert.deepEqual(Buffer.from(bnv), Buffer.from(bn), `${label} BN`);
      assert.deepEqual(Buffer.from(bv), Buffer.from(b), `${label} B`);
      assert.equal(cnv, cn, `${label} CN`);
      assert.equal(cv, c, `${label} C`);
      assert.equal(dnv, dn, `${label} DN`);
      assert.equal(dv, d, `${label} D`);
    };
    const assertEmptyRow = (label, row) => {
      const [id, bnv, bv, cnv, cv, dnv, dv] = row;
      assert.equal(id, 2, `${label} id`);
      assert.equal(bnv.length, 0, `${label} empty BN`);
      assert.equal(bv, null, `${label} NULL B`);
      assert.equal(cnv, '', `${label} empty CN`);
      assert.equal(cv, null, `${label} NULL C`);
      assert.equal(dnv, '', `${label} empty DN`);
      assert.equal(dv, null, `${label} NULL D`);
    };

    const materialized = (await db.query(`SELECT ID, BN, B, CN, C, DN, D FROM ${table} ORDER BY ID`)).rows;
    assertRow('query row 1', materialized[0]);
    assertEmptyRow('query row 2', materialized[1]);

    const streamed = [];
    for await (const row of db.queryStreamWithParams(`SELECT ID, BN, B, CN, C, DN, D FROM ${table} ORDER BY ID`, [])) {
      const values = [];
      for (const value of row.values) {
        if (value && typeof value.stream === 'function') {
          const chunks = [];
          for await (const chunk of value.stream()) chunks.push(Buffer.from(chunk));
          values.push(value.kind === 'text-lob' ? chunks.join('') : Buffer.concat(chunks));
        } else values.push(value);
      }
      streamed.push(values);
    }
    assertRow('stream row 1', streamed[0]);
    assertEmptyRow('stream row 2', streamed[1]);

    const reader = await db.executeProcedureStream(`CALL ${procedure}()`);
    const procedureRows = [];
    for await (const row of reader.rows()) procedureRows.push(row);
    await reader.finish();
    assertRow('procedure row 1', procedureRows[0]);
    assertEmptyRow('procedure row 2', procedureRows[1]);
    assert.deepEqual((await db.query('VALUES 29')).rows, [[29]]);
  } finally {
    if (procedureCreated) await db.execute(`DROP PROCEDURE ${await fixtureSchema(db)}.${procedure}`).catch(() => undefined);
    if (created) await db.execute(`DROP TABLE ${await fixtureSchema(db)}.${table}`).catch(() => undefined);
    await db.close();
  }
});

test('live Db2 AbortSignal interrupts a procedure cursor fetch and preserves the session', { skip: !dsn, timeout: 40_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const procedure = `TS_RS_CANCEL_${randomUUID().replaceAll('-', '').slice(0, 12).toUpperCase()}`;
  const dimensions = Array.from({ length: 6 }, (_, index) => `(VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS D${index}(N)`).join(', ');
  const projection = Array.from({ length: 6 }, (_, index) => `D${index}.N`).join(' + ');
  let created = false;
  try {
    // Sorting one million generated rows keeps work on DB2 when cancellation is
    // requested; returning one row avoids measuring a long client-side fetch.
    await db.execute(`CREATE PROCEDURE ${await fixtureSchema(db)}.${procedure} () LANGUAGE SQL DYNAMIC RESULT SETS 1 BEGIN DECLARE C CURSOR WITH RETURN TO CALLER FOR SELECT ${projection} AS N FROM ${dimensions} ORDER BY RAND() FETCH FIRST 1 ROW ONLY; OPEN C; END`);
    created = true;

    const controller = new AbortController();
    const call = db.query(`CALL ${procedure}()`, undefined, { signal: controller.signal });
    let settled = false;
    void call.then(() => { settled = true; }, () => { settled = true; });
    await new Promise(resolve => setTimeout(resolve, 100));
    assert.equal(settled, false, 'the procedure cursor should still be fetching when cancellation is requested');
    const cancellationStarted = performance.now();
    controller.abort();
    await assert.rejects(call, error => error.name === 'AbortError');
    console.log(`procedure cursor AbortSignal completion: ${(performance.now() - cancellationStarted).toFixed(1)} ms; session closed: ${db.isClosed}`);
    assert.equal(db.isClosed, false);
    assert.deepEqual((await db.query('VALUES 29')).rows, [[29]]);
  } finally {
    await db.close();
    if (created) {
      const cleanup = await Db2Connection.connect(dsn);
      try {
        const remaining = await cleanup.query('SELECT COUNT(*) FROM SYSCAT.ROUTINES WHERE ROUTINENAME = ?', [procedure]);
        if (Number(remaining.rows[0][0]) > 0) await cleanup.execute(`DROP PROCEDURE ${await fixtureSchema(cleanup)}.${procedure}`);
      } finally { await cleanup.close(); }
    }
  }
});

test('live Db2 AbortSignal interrupts a streamed procedure cursor and discards open cursor state', { skip: !dsn, timeout: 40_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const procedure = `TS_RS_STREAM_CANCEL_${randomUUID().replaceAll('-', '').slice(0, 10).toUpperCase()}`;
  const dimensions = Array.from({ length: 6 }, (_, index) => `(VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS D${index}(N)`).join(', ');
  const projection = Array.from({ length: 6 }, (_, index) => `D${index}.N`).join(' + ');
  let created = false;
  try {
    await db.execute(`CREATE PROCEDURE ${await fixtureSchema(db)}.${procedure} () LANGUAGE SQL DYNAMIC RESULT SETS 1 BEGIN DECLARE C CURSOR WITH RETURN TO CALLER FOR SELECT ${projection} AS N FROM ${dimensions} ORDER BY RAND() FETCH FIRST 1 ROW ONLY; OPEN C; END`);
    created = true;
    const controller = new AbortController();
    const reader = await db.executeProcedureStream(`CALL ${procedure}()`, [], { signal: controller.signal });
    const nextRow = reader.rows().next();
    let settled = false;
    void nextRow.then(() => { settled = true; }, () => { settled = true; });
    await new Promise(resolve => setTimeout(resolve, 100));
    assert.equal(settled, false, 'the streamed procedure cursor should still be fetching');
    controller.abort();
    await assert.rejects(nextRow, error => error.name === 'AbortError');
    assert.equal(db.isClosed, true, 'an interrupted CALL with multiple server cursors cannot safely be reused');
  } finally {
    await db.close();
    if (created) {
      const cleanup = await Db2Connection.connect(dsn);
      try {
        const remaining = await cleanup.query('SELECT COUNT(*) FROM SYSCAT.ROUTINES WHERE ROUTINENAME = ?', [procedure]);
        if (Number(remaining.rows[0][0]) > 0) await cleanup.execute(`DROP PROCEDURE ${await fixtureSchema(cleanup)}.${procedure}`);
      } finally { await cleanup.close(); }
    }
  }
});

test('live Db2 AbortSignal interrupts the second procedure cursor and cleans every cursor', { skip: !dsn, timeout: 40_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const procedure = `TS_RS_CANCEL2_${randomUUID().replaceAll('-', '').slice(0, 10).toUpperCase()}`;
  const dimensions = Array.from({ length: 7 }, (_, index) => `(VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS D${index}(N)`).join(', ');
  const projection = Array.from({ length: 7 }, (_, index) => `D${index}.N`).join(' + ');
  let created = false;
  try {
    // The first cursor completes immediately; cancellation must land while the second
    // declared result set is still being fetched.
    // Sort ten million rows server-side before returning one; do not time cancellation
    // against client transfer of already-computed rows.
    await db.execute(`CREATE PROCEDURE ${await fixtureSchema(db)}.${procedure} () LANGUAGE SQL DYNAMIC RESULT SETS 2 BEGIN DECLARE C1 CURSOR WITH RETURN TO CALLER FOR SELECT 41 AS N FROM SYSIBM.SYSDUMMY1; DECLARE C2 CURSOR WITH RETURN TO CALLER FOR SELECT ${projection} AS N FROM ${dimensions} ORDER BY RAND() FETCH FIRST 1 ROW ONLY; OPEN C1; OPEN C2; END`);
    created = true;

    const controller = new AbortController();
    const call = db.query(`CALL ${procedure}()`, undefined, { signal: controller.signal });
    let settled = false;
    void call.then(() => { settled = true; }, () => { settled = true; });
    await new Promise(resolve => setTimeout(resolve, 200));
    assert.equal(settled, false, 'the second result cursor should still be fetching when cancellation is requested');
    controller.abort();
    await assert.rejects(call, error => error.name === 'AbortError');
    // The server-side sort makes this a cancellation during DB2 work, not late client draining.
    assert.equal(db.isClosed, false, 'a server-confirmed cancellation must preserve the session');
    assert.deepEqual((await db.query('VALUES 29')).rows, [[29]]);
  } finally {
    await db.close();
    if (created) {
      const cleanup = await Db2Connection.connect(dsn);
      try {
        await cleanup.execute(`DROP PROCEDURE ${await fixtureSchema(cleanup)}.${procedure}`).catch(() => undefined);
      } finally { await cleanup.close(); }
    }
  }
});

test('live Db2 executes parameterized ADMIN_CMD through a pooled session', { skip: !dsn, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(dsn);
  const pool = new Db2Pool(dsn, { maxConnections: 1 });
  const table = `TS_ADMIN_${randomUUID().replaceAll('-', '').slice(0, 20).toUpperCase()}`;
  let created = false;
  try {
    assert.throws(() => db.executeAdminCommand(' \t\r\n'), /cannot be empty/);
    assert.throws(() => pool.executeAdminCommand('RUNSTATS\0 ON TABLE X'), /NUL/);
    const schema = (await db.query('VALUES CURRENT SCHEMA')).rows[0][0];
    assert.match(schema, /^[A-Z][A-Z0-9_]*$/);
    await db.execute(`CREATE TABLE ${schema}.${table} (ID INTEGER NOT NULL)`);
    created = true;

    await pool.executeAdminCommand(`RUNSTATS ON TABLE ${schema}.${table}`);
    const stats = await db.query('SELECT STATS_TIME FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TABNAME = ?', [schema, table]);
    assert.equal(stats.rows.length, 1);
    assert.notEqual(stats.rows[0][0], null, 'RUNSTATS should update the table statistics');
  } finally {
    if (created) await db.execute(`DROP TABLE ${await fixtureSchema(db)}.${table}`).catch(() => undefined);
    await Promise.all([db.close(), pool.close()]);
  }
});
