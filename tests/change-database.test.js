'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const { CodePoint } = require('../dist/cjs/network/codepoint.js');
const ddm = require('../dist/cjs/network/ddm.js');
const dss = require('../dist/cjs/network/dss.js');
const { Db2Connection } = require('../dist/cjs/network/session.js');
const { Db2Pool, clearAllDb2Pools } = require('../dist/cjs/network/pool.js');

class SocketReader {
  constructor(socket) { this.socket = socket; this.chunks = []; this.length = 0; this.error = null; this.waiters = new Set();
    socket.on('data', (chunk) => { this.chunks.push(chunk); this.length += chunk.length; this.notify(); });
    socket.on('end', () => { this.error ||= new Error('socket ended'); this.notify(); });
    socket.on('error', (error) => { this.error ||= error; this.notify(); });
    socket.on('close', () => { this.error ||= new Error('socket closed'); this.notify(); });
  }
  notify() { for (const resolve of this.waiters) resolve(); this.waiters.clear(); }
  async readExact(count) { while (this.length < count) { if (this.error) throw this.error; await new Promise((resolve) => this.waiters.add(resolve)); }
    const out = Buffer.alloc(count); let offset = 0;
    while (offset < count) { const head = this.chunks[0]; const n = Math.min(head.length, count - offset); head.copy(out, offset, 0, n); offset += n; this.length -= n;
      if (n === head.length) this.chunks.shift(); else this.chunks[0] = head.subarray(n); }
    return out;
  }
}

function reply(socket, codePoint, payload = new Uint8Array(), chained = false) {
  const packet = dss.encodeReplyDss(ddm.packDdmObject(codePoint, payload), chained, 1);
  return new Promise((resolve, reject) => socket.write(packet, (error) => error ? reject(error) : resolve()));
}

async function readChain(reader) {
  const packets = [];
  let chained;
  do { const packet = await dss.readDss(reader); packets.push(packet); chained = packet.header.chained; } while (chained);
  return packets;
}

function sqlCard(code = 0, state = '00000', affected = 0) {
  const data = new Uint8Array(54); data[0] = 0;
  const view = new DataView(data.buffer); view.setInt32(1, code, true); data.set(new TextEncoder().encode(state), 5); view.setInt32(27, affected, true);
  return data;
}

function mockConfig(port, over = {}) {
  return {
    host: '127.0.0.1', port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 2000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
    ...over,
  };
}

async function serveConnectionSetup(socket, interruptToken = Uint8Array.of(0x04, 0x1c, 0xa7, 0x32)) {
  const reader = new SocketReader(socket);
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
  await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
  await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
  await reply(socket, CodePoint.ACCRDBRM, ddm.packBytes(CodePoint.RDBINTTKN, interruptToken));
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
  await reply(socket, CodePoint.SQLCARD, sqlCard());
  return reader;
}

test('changeDatabase reconnects the session transport to the new database', async (t) => {
  let connections = 0;
  const tasks = [];
  const server = net.createServer((socket) => {
    connections++;
    tasks.push((async () => {
      await serveConnectionSetup(socket);
      await new Promise((resolve) => socket.once('close', resolve));
    })().catch(() => socket.destroy()));
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));
  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  try {
    assert.equal(connections, 1);
    assert.equal(connection.currentDatabase, 'TESTDB');
    await connection.changeDatabase('TESTDB');
    assert.equal(connections, 1, 'same database is a no-op without I/O');
    await connection.changeDatabase('  NEWDB  ');
    assert.equal(connections, 2, 'a new database reconnects the transport');
    assert.equal(connection.currentDatabase, 'NEWDB');
    assert.equal(connection.isClosed, false);
    assert.equal(connection.isAutoCommit, true);
    await connection.changeDatabase('NEWDB');
    assert.equal(connections, 2, 'repeating the current database is a no-op');
  } finally {
    await connection.close();
    await Promise.all(tasks);
  }
});

test('changeDatabase validates names before any I/O', async (t) => {
  let connections = 0;
  const server = net.createServer((socket) => {
    connections++;
    server.task = (async () => {
      await serveConnectionSetup(socket);
      await new Promise((resolve) => socket.once('close', resolve));
    })().catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));
  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  try {
    await assert.rejects(connection.changeDatabase('   '), /must not be empty/u);
    await assert.rejects(connection.changeDatabase('DB\0X'), /invalid DB2 database name/u);
    await assert.rejects(connection.changeDatabase('D'.repeat(129)), /invalid DB2 database name/u);
    assert.equal(connections, 1, 'invalid names never reach the server');
    assert.equal(connection.currentDatabase, 'TESTDB');
  } finally {
    await connection.close();
    await server.task;
  }
});

test('changeDatabase refuses while a transaction is active', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      await serveConnectionSetup(socket);
      await new Promise((resolve) => socket.once('close', resolve));
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));
  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  try {
    await connection.beginTransaction();
    await assert.rejects(connection.changeDatabase('OTHERDB'), /transaction is active/u);
    assert.equal(connection.currentDatabase, 'TESTDB');
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('changeDatabase refuses on a pooled session', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      await serveConnectionSetup(socket);
      await new Promise((resolve) => socket.once('close', resolve));
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));
  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  try {
    connection.poolBaselineCaptured = true;
    await assert.rejects(connection.changeDatabase('OTHERDB'), /pooled session/u);
    assert.equal(connection.currentDatabase, 'TESTDB');
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('setCurrentSchema sends a quoted SET CURRENT SCHEMA', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      await serveConnectionSetup(socket);
      await new Promise((resolve) => socket.once('close', resolve));
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));
  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  const statements = [];
  connection.executeLocked = async (sql) => { statements.push(sql); return 0; };
  try {
    await connection.setCurrentSchema('app');
    await connection.setCurrentSchema('we"ird');
    assert.deepEqual(statements, [
      'SET CURRENT SCHEMA "app"',
      'SET CURRENT SCHEMA "we""ird"',
    ]);
    await assert.rejects(connection.setCurrentSchema('   '), /must not be empty/u);
    await assert.rejects(connection.setCurrentSchema('A\0B'), /NUL/u);
    assert.equal(statements.length, 2, 'invalid schemas never reach the server');
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('clearAllDb2Pools drains idle sessions and keeps pools usable', async (t) => {
  let created = 0;
  const closed = [];
  const factory = async () => {
    created++;
    const connection = {
      isClosed: false,
      async query() { return { columns: [], rows: [] }; },
      async execute() { return 1; },
      async resetForPool() {},
      async close() { this.isClosed = true; closed.push(connection); },
    };
    return connection;
  };
  const poolA = new Db2Pool('db2://user:password@localhost:50000/TESTDB', { maxConnections: 2, connectionFactory: factory });
  const poolB = new Db2Pool('db2://user:password@localhost:50000/TESTDB', { maxConnections: 2, connectionFactory: factory });
  t.after(() => Promise.all([poolA.close(), poolB.close()]));
  await poolA.query('VALUES 1');
  await poolB.query('VALUES 1');
  assert.equal(created, 2);
  assert.equal(closed.length, 0);

  let releaseHeld;
  const held = poolA.withConnection(async (connection) => {
    await new Promise((resolve) => { releaseHeld = resolve; });
    return connection;
  });
  await new Promise((resolve) => setImmediate(resolve));
  await clearAllDb2Pools();
  assert.equal(closed.length, 1, 'only the idle session is closed; the active lease survives');
  assert.equal(poolA.idleCount, 0);
  assert.equal(poolB.idleCount, 0);
  assert.equal(poolB.connectionCount, 0);
  releaseHeld();
  const heldConnection = await held;
  assert.equal(heldConnection.isClosed, false, 'an active lease survives the cleanup');
  assert.equal(poolA.idleCount, 1, 'the released lease returns to its pool');
  await poolB.query('VALUES 1');
  assert.equal(created, 3, 'a drained pool reopens sessions on demand');
  await clearAllDb2Pools();
});
