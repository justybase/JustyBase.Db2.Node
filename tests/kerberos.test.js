const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
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

function mockConfig(port, overrides = {}) {
  return {
    host: '127.0.0.1', port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 2000, blockSize: 65535,
    securityMechanism: 11, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '', gssapiTokenFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
    ...overrides,
  };
}

test('packSecchkKerberos carries the GSSAPI token without a password field', () => {
  const token = Uint8Array.of(0x60, 0x82, 1, 2, 3);
  const fields = ddm.parseDdmParameters(ddm.packSecchkKerberos('TESTDB', token, 'db2inst1').subarray(4));
  assert.equal(new DataView(fields.get(CodePoint.SECMEC).buffer, fields.get(CodePoint.SECMEC).byteOffset, 2).getUint16(0, false), 11);
  assert.deepEqual(Buffer.from(fields.get(CodePoint.SECTKN)), Buffer.from(token));
  assert.equal(fields.has(CodePoint.PASSWORD), false);
});

test('Kerberos handshake sends the token file bytes in SECCHK SECTKN', async (t) => {
  const token = Buffer.from([0x60, 0x82, 1, 2, 3, 4, 5, 6, 7, 8]);
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'db2-gssapi-'));
  const tokenFile = path.join(directory, 'token.bin');
  fs.writeFileSync(tokenFile, token);
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));

  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      const reader = new SocketReader(socket);
      const opening = await readChain(reader);
      assert.deepEqual(opening.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
      const accsec = ddm.parseDdmParameters(opening[1].payload);
      assert.equal(new DataView(accsec.get(CodePoint.SECMEC).buffer, accsec.get(CodePoint.SECMEC).byteOffset, 2).getUint16(0, false), 11);
      // No key-exchange token: the caller supplies the GSSAPI token.
      assert.equal(accsec.has(CodePoint.SECTKN), false);
      await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 11));

      const check = await readChain(reader);
      assert.deepEqual(check.map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
      const secchk = ddm.parseDdmParameters(check[0].payload);
      assert.deepEqual(Buffer.from(secchk.get(CodePoint.SECTKN)), token);
      assert.equal(secchk.has(CodePoint.PASSWORD), false);

      await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
      await reply(socket, CodePoint.ACCRDBRM, ddm.packBytes(CodePoint.RDBINTTKN, Uint8Array.of(4, 28, 167, 50)));
      const initialize = await readChain(reader);
      assert.deepEqual(initialize.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
      await reply(socket, CodePoint.SQLCARD, sqlCard());
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect(mockConfig(server.address().port, { gssapiTokenFile: tokenFile }));
  assert.equal(connection.isClosed, false);
  await connection.close();
  await serverTask;
});

test('mechanism 11 without a token file is rejected before opening a socket', async () => {
  await assert.rejects(Db2Connection.connect(mockConfig(1)), (error) => {
    assert.equal(error.name, 'Db2ConfigurationError');
    assert.match(error.message, /gssapi_token_file/);
    return true;
  });
});
