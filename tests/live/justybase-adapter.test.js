'use strict';
require('../load-env.cjs');
const test = require('node:test');
const assert = require('node:assert/strict');
const { randomUUID } = require('node:crypto');
const { parseDsn } = require('../../dist/cjs/config.js');
const { Db2Connection } = require('../../dist/cjs/network/session.js');
const { Db2DatabaseConnection } = require('../../dist/cjs/ibmDbCompat.js');
const { fixtureSchema } = require('./fixture-schema.cjs');

const dsn = process.env.DB2_TEST_DSN;

function byteChunks(bytes, size = 64 * 1024) {
  return (async function* () {
    for (let offset = 0; offset < bytes.length; offset += size) yield bytes.subarray(offset, offset + size);
  })();
}

test('live JustyBase adapter applies current schema and streams procedure results through exhaustion', {
  skip: !dsn,
  timeout: 60_000,
}, async () => {
  const native = await Db2Connection.connect(dsn);
  let currentSchema;
  try {
    currentSchema = String((await native.query('VALUES CURRENT SCHEMA')).rows[0][0]).trim();
  } finally {
    await native.close();
  }

  const parsed = parseDsn(dsn);
  const connection = new Db2DatabaseConnection({
    host: parsed.host,
    port: parsed.port,
    database: parsed.database,
    user: parsed.user,
    password: parsed.password,
    options: { currentSchema, connectTimeout: 20 },
  });
  const procedure = `TS_JB_${randomUUID().replaceAll('-', '').slice(0, 20).toUpperCase()}`;
  let created = false;
  try {
    await connection.connect();
    assert.equal(connection._connected, true);

    const schemaReader = await connection.createCommand('VALUES CURRENT SCHEMA').executeReader();
    try {
      assert.equal(await schemaReader.read(), true);
      assert.equal(String(schemaReader.getValue(0)).trim(), currentSchema);
    } finally {
      await schemaReader.close();
    }

    created = true; // Cleanup also handles a lost CREATE reply after server-side success.
    await connection.createCommand(
      `CREATE PROCEDURE ${await fixtureSchema(connection.database.connection)}.${procedure} () LANGUAGE SQL DYNAMIC RESULT SETS 2 BEGIN `
      + 'DECLARE C1 CURSOR WITH RETURN TO CALLER FOR SELECT 17 AS V FROM SYSIBM.SYSDUMMY1; '
      + 'DECLARE C2 CURSOR WITH RETURN TO CALLER FOR SELECT 29 AS V FROM SYSIBM.SYSDUMMY1; '
      + 'OPEN C1; OPEN C2; END',
    ).execute();

    const command = connection.createCommand(`-- adapter procedure path\nCALL ${procedure}()`);
    command.commandTimeout = 30;
    const reader = await command.executeReader();
    try {
      assert.equal(reader.fieldCount, 1);
      assert.equal(reader.getName(0), 'V');
      assert.equal(await reader.read(), true);
      assert.equal(Number(reader.getValue(0)), 17);
      assert.equal(await reader.nextResult(), true);
      assert.equal(await reader.read(), true);
      assert.equal(Number(reader.getValue(0)), 29);
      assert.equal(await reader.nextResult(), false);
      assert.equal(await reader.read(), false);
      assert.equal(await reader.nextResult(), false);
      assert.equal(command._recordsAffected, 0);
    } finally {
      await reader.close();
    }

    const reuseReader = await connection.createCommand('VALUES 41').executeReader();
    try {
      assert.equal(await reuseReader.read(), true);
      assert.equal(Number(reuseReader.getValue(0)), 41);
    } finally {
      await reuseReader.close();
    }
  } finally {
    if (created) await connection.createCommand(`DROP PROCEDURE ${await fixtureSchema(connection.database.connection)}.${procedure}`).execute().catch(() => {});
    await connection.close();
    assert.equal(connection._connected, false);
  }
});

test('live JustyBase adapter materializes large BLOB and CLOB results into plain values', {
  skip: !dsn,
  timeout: 90_000,
}, async () => {
  const { createHash } = require('node:crypto');
  const parsed = parseDsn(dsn);
  const connection = new Db2DatabaseConnection({
    host: parsed.host,
    port: parsed.port,
    database: parsed.database,
    user: parsed.user,
    password: parsed.password,
    options: { connectTimeout: 20 },
  });
  const table = `TS_JB_LOB_${randomUUID().replaceAll('-', '').slice(0, 16).toUpperCase()}`;
  const blob = Buffer.alloc(2_500_000);
  for (let index = 0; index < blob.length; index++) blob[index] = (index * 37 + 11) % 251;
  const blobHash = createHash('sha256').update(blob).digest('hex');
  const text = 'é'.repeat(1_000_000);
  const textBytes = Buffer.from(text, 'utf8');
  let created = false;
  try {
    await connection.connect();
    created = true; // Cleanup also handles a lost CREATE reply after server-side success.
    await connection.createCommand(`CREATE TABLE ${await fixtureSchema(connection.database.connection)}.${table} (B BLOB(5M), C CLOB(32M))`).execute();
    await connection.createCommand(
      `INSERT INTO ${table} VALUES (CAST(? AS BLOB(5M)), CAST(? AS CLOB(32M)))`,
      [{ source: byteChunks(blob), length: blob.length }, { source: byteChunks(textBytes), length: textBytes.length }],
    ).execute();

    const reader = await connection.createCommand(`SELECT B, C FROM ${table}`).executeReader();
    try {
      assert.equal(await reader.read(), true);
      const readBlob = reader.getValue(0);
      const readText = reader.getValue(1);
      assert.ok(Buffer.isBuffer(readBlob), 'binary LOB must materialize to a Buffer');
      assert.equal(readBlob.length, blob.length);
      assert.equal(createHash('sha256').update(readBlob).digest('hex'), blobHash);
      assert.equal(typeof readText, 'string', 'text LOB must materialize to a string');
      assert.equal(readText, text);
      assert.equal(readBlob.kind, undefined, 'no raw locator object may leak through the adapter');
      assert.equal(await reader.read(), false);
    } finally {
      await reader.close();
    }

    const reuseReader = await connection.createCommand('VALUES 43').executeReader();
    try {
      assert.equal(await reuseReader.read(), true);
      assert.equal(Number(reuseReader.getValue(0)), 43);
    } finally {
      await reuseReader.close();
    }
  } finally {
    if (created) await connection.createCommand(`DROP TABLE ${await fixtureSchema(connection.database.connection)}.${table}`).execute().catch(() => {});
    await connection.close();
  }
});
