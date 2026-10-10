const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const net = require('node:net');
const tls = require('node:tls');
const { createTlsFixtures } = require('./helpers/tls-fixtures.cjs');
const { CodePoint } = require('../dist/cjs/network/codepoint.js');
const ddm = require('../dist/cjs/network/ddm.js');
const dss = require('../dist/cjs/network/dss.js');
const { Db2Connection } = require('../dist/cjs/network/session.js');

const tlsFixtures = createTlsFixtures();
const fixture = tlsFixtures.file;
test.after(() => tlsFixtures.cleanup());

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

function mutualTlsServer() {
  return tls.createServer({
    key: fs.readFileSync(fixture('server-key.pem')),
    cert: fs.readFileSync(fixture('server.pem')),
    ca: [fs.readFileSync(fixture('ca.pem'))],
    requestCert: true,
    rejectUnauthorized: true,
  });
}

function clientConfig(port, overrides = {}) {
  return {
    host: '127.0.0.1', port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: true,
    sslRootCaPath: fixture('ca.pem'), sslClientCertPath: fixture('client.pem'), sslClientKeyPath: fixture('client-key.pem'),
    timeoutMs: 3000, blockSize: 65535, securityMechanism: 3,
    kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '', gssapiTokenFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
    ...overrides,
  };
}

function listen(mutualServer, t) {
  let serverTask = Promise.resolve();
  mutualServer.on('secureConnection', (socket) => { serverTask = serveConnectionSetup(socket); serverTask.catch(() => socket.destroy()); });
  mutualServer.on('tlsClientError', () => {});
  t.after(async () => {
    mutualServer.closeAllConnections?.();
    await new Promise((resolve) => mutualServer.close(() => resolve()));
    await serverTask.catch(() => {});
  });
  return new Promise((resolve) => mutualServer.listen(0, '127.0.0.1', () => resolve(mutualServer.address().port)));
}

async function expectHandshakeRejected(config) {
  let timer;
  try {
    const outcome = await Promise.race([
      Db2Connection.connect(config).then(
        async (connection) => { await connection.close().catch(() => {}); return 'connected'; },
        () => 'rejected',
      ),
      new Promise((resolve) => { timer = setTimeout(() => resolve('timeout'), 8000); }),
    ]);
    assert.equal(outcome, 'rejected', 'a client without a trusted certificate must not establish a session');
  } finally {
    clearTimeout(timer);
  }
}

test('mutual TLS with a trusted client certificate completes the handshake', async (t) => {
  const server = mutualTlsServer();
  const port = await listen(server, t);
  const connection = await Db2Connection.connect(clientConfig(port));
  assert.equal(connection.isClosed, false);
  await connection.close();
});

test('mutual TLS rejects a client without a certificate', async (t) => {
  const server = mutualTlsServer();
  const port = await listen(server, t);
  await expectHandshakeRejected(clientConfig(port, { sslClientCertPath: '', sslClientKeyPath: '' }));
});

test('mutual TLS rejects a client certificate from an untrusted CA', async (t) => {
  const server = mutualTlsServer();
  const port = await listen(server, t);
  await expectHandshakeRejected(clientConfig(port, {
    sslClientCertPath: fixture('other-client.pem'), sslClientKeyPath: fixture('other-client-key.pem'),
  }));
});
