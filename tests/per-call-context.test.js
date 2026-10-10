const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const { CodePoint } = require('../dist/cjs/network/codepoint.js');
const ddm = require('../dist/cjs/network/ddm.js');
const dss = require('../dist/cjs/network/dss.js');
const { Db2Connection } = require('../dist/cjs/network/session.js');

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

function sqlText(statement) {
  const payload = Buffer.from(statement.payload);
  const length = payload.readUInt32BE(1);
  return payload.toString('utf8', 5, 5 + length);
}

async function serveConnectionSetup(socket) {
  const reader = new SocketReader(socket);
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
  await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
  await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
  await reply(socket, CodePoint.ACCRDBRM, ddm.packBytes(CodePoint.RDBINTTKN, Uint8Array.of(4, 28, 167, 50)));
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
  await reply(socket, CodePoint.SQLCARD, sqlCard());
  return reader;
}

async function serveExecute(reader, socket, expectedSql, codePoint = CodePoint.EXCSQLIMM) {
  const chain = await readChain(reader);
  assert.deepEqual(chain.map((packet) => packet.codePoint), [codePoint, CodePoint.SQLSTT, CodePoint.RDBCMM]);
  const statement = chain.find((packet) => packet.codePoint === CodePoint.SQLSTT);
  assert.equal(sqlText(statement), expectedSql);
  await reply(socket, CodePoint.SQLCARD, sqlCard());
}

function mockConfig(port) {
  return {
    host: '127.0.0.1', port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 2000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '', gssapiTokenFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  };
}

test('per-operation user option switches identity only when it differs', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      const reader = await serveConnectionSetup(socket);
      await serveExecute(reader, socket, 'SET SESSION AUTHORIZATION = "BORROWER"');
      await serveExecute(reader, socket, 'VALUES 1');
      // The second call targets the same user, so no switch statement is sent.
      await serveExecute(reader, socket, 'VALUES 1');
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  try {
    await connection.execute('VALUES 1', undefined, { user: 'BORROWER' });
    await connection.execute('VALUES 1', undefined, { user: 'BORROWER' });
  } finally {
    await connection.close();
  }
  await serverTask;
});

test('per-operation clientInfo option applies only the differing registers', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      const reader = await serveConnectionSetup(socket);
      await serveExecute(reader, socket, "SET CLIENT APPLNAME 'svc'", CodePoint.EXCSQLSET);
      await serveExecute(reader, socket, 'VALUES 1');
      // The register is already set, so the second call sends nothing extra.
      await serveExecute(reader, socket, 'VALUES 1');
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  try {
    await connection.execute('VALUES 1', undefined, { clientInfo: { applicationName: 'svc' } });
    await connection.execute('VALUES 1', undefined, { clientInfo: { applicationName: 'svc' } });
  } finally {
    await connection.close();
  }
  await serverTask;
});

test('a denied per-operation register surfaces the server error instead of being ignored', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      const reader = await serveConnectionSetup(socket);
      const chain = await readChain(reader);
      assert.deepEqual(chain.map((packet) => packet.codePoint), [CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
      await reply(socket, CodePoint.SQLCARD, sqlCard(-551, '42501'));
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  try {
    await assert.rejects(connection.execute('VALUES 1', undefined, { clientInfo: { applicationName: 'svc' } }), /SQLCODE=-551/);
  } finally {
    await connection.close();
  }
  await serverTask;
});

test('per-operation context rejects malformed options before touching the protocol', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      await serveConnectionSetup(socket);
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  try {
    await assert.rejects(connection.execute('VALUES 1', undefined, { user: 42 }), TypeError);
    await assert.rejects(connection.execute('VALUES 1', undefined, { clientInfo: null }), TypeError);
  } finally {
    await connection.close();
  }
  await serverTask;
});
