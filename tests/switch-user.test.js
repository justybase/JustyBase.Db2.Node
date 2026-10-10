const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const { CodePoint } = require('../dist/cjs/network/codepoint.js');
const ddm = require('../dist/cjs/network/ddm.js');
const dss = require('../dist/cjs/network/dss.js');
const { Db2Connection } = require('../dist/cjs/network/session.js');
const { decodeCp500 } = require('../dist/cjs/converters/ebcdic.js');
const { Db2AuthenticationError, Db2UnsupportedFeatureError } = require('../dist/cjs/errors.js');

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

function mockConfig(port, overrides = {}) {
  return {
    host: '127.0.0.1', port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 2000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '', gssapiTokenFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
    ...overrides,
  };
}

async function serveFailingSessionSet(reader, socket) {
  const chain = await readChain(reader);
  assert.deepEqual(chain.map((packet) => packet.codePoint), [CodePoint.EXCSQLIMM, CodePoint.SQLSTT, CodePoint.RDBCMM]);
  const statement = chain.find((packet) => packet.codePoint === CodePoint.SQLSTT);
  await reply(socket, CodePoint.SQLCARD, sqlCard(-551, '42501'));
  return sqlText(statement);
}

test('switchUser falls back to a standalone SECCHK with the supplied password', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      const reader = await serveConnectionSetup(socket);
      assert.equal(await serveFailingSessionSet(reader, socket), 'SET SESSION AUTHORIZATION = "db2test2"');
      assert.equal(await serveFailingSessionSet(reader, socket), 'SET SESSION_USER = "db2test2"');
      const secchk = await readChain(reader);
      assert.deepEqual(secchk.map((packet) => packet.codePoint), [CodePoint.SECCHK]);
      const fields = ddm.parseDdmParameters(secchk[0].payload);
      assert.equal(new DataView(fields.get(CodePoint.SECMEC).buffer, fields.get(CodePoint.SECMEC).byteOffset, 2).getUint16(0, false), 3);
      assert.equal(fields.get(CodePoint.RDBNAM).length, 18);
      assert.equal(decodeCp500(fields.get(CodePoint.USRID)), 'db2test2');
      assert.equal(decodeCp500(fields.get(CodePoint.PASSWORD)), 'secret');
      await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)));
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  await connection.switchUser('db2test2', 'secret');
  await connection.close();
  await serverTask;
});

test('switchUser without a password still attempts the SECCHK fallback and surfaces SECCHKCD', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      const reader = await serveConnectionSetup(socket);
      await serveFailingSessionSet(reader, socket);
      await serveFailingSessionSet(reader, socket);
      const secchk = await readChain(reader);
      assert.deepEqual(secchk.map((packet) => packet.codePoint), [CodePoint.SECCHK]);
      const fields = ddm.parseDdmParameters(secchk[0].payload);
      assert.equal(fields.has(CodePoint.PASSWORD), false, 'no password was supplied, so none is sent');
      await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(4)));
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  await assert.rejects(connection.switchUser('db2test2'), (error) => {
    assert.ok(error instanceof Db2AuthenticationError);
    assert.match(error.message, /SECCHKCD=4/);
    return true;
  });
  await connection.close();
  await serverTask;
});

test('switchUser keeps the SQL path when the session-authorization statement succeeds', async (t) => {
  let serverTask;
  let secchkSeen = false;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      const reader = await serveConnectionSetup(socket);
      const chain = await readChain(reader);
      assert.deepEqual(chain.map((packet) => packet.codePoint), [CodePoint.EXCSQLIMM, CodePoint.SQLSTT, CodePoint.RDBCMM]);
      const statement = chain.find((packet) => packet.codePoint === CodePoint.SQLSTT);
      assert.equal(sqlText(statement), 'SET SESSION AUTHORIZATION = "db2test2"');
      await reply(socket, CodePoint.SQLCARD, sqlCard());
      // The next request must not be a SECCHK fallback.
      const next = await readChain(reader).catch(() => []);
      secchkSeen = next.some((packet) => packet.codePoint === CodePoint.SECCHK);
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  await connection.switchUser('db2test2', 'secret');
  await connection.close();
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(secchkSeen, false);
  await serverTask;
});

test('switchUser fallback fails closed when the session uses another security mechanism', async (t) => {
  let remote;
  let secchkSeen = false;
  const server = net.createServer((socket) => {
    remote = socket;
    const reader = new SocketReader(socket);
    (async () => {
      await serveFailingSessionSet(reader, socket);
      await serveFailingSessionSet(reader, socket);
      const next = await Promise.race([readChain(reader).catch(() => []), new Promise((resolve) => setTimeout(() => resolve([]), 80))]);
      secchkSeen = next.some((packet) => packet.codePoint === CodePoint.SECCHK);
    })().catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    remote?.destroy();
    await new Promise((resolve) => server.close(() => resolve()));
  });

  const socket = net.createConnection(server.address().port, '127.0.0.1');
  await new Promise((resolve) => socket.once('connect', resolve));
  const connection = new Db2Connection(socket, mockConfig(server.address().port, { securityMechanism: 9 }));
  await assert.rejects(connection.switchUser('db2test2', 'secret'), (error) => {
    assert.ok(error instanceof Db2UnsupportedFeatureError);
    assert.match(error.message, /not supported for mid-session re-authentication/);
    return true;
  });
  await new Promise((resolve) => setTimeout(resolve, 120));
  assert.equal(secchkSeen, false, 'mechanism 9 must not send a plaintext-password SECCHK fallback');
  socket.destroy();
});
