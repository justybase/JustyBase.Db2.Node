const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const { CodePoint } = require('../dist/cjs/network/codepoint.js');
const ddm = require('../dist/cjs/network/ddm.js');
const dss = require('../dist/cjs/network/dss.js');
const { Db2Connection } = require('../dist/cjs/network/session.js');
const { decodeCp500 } = require('../dist/cjs/converters/ebcdic.js');
const { encryptSecmec9, desCbcDecryptWith, secmec9PublicKey, secmec9SessionKey } = require('../dist/cjs/security/secmec9.js');

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

function collectSectkn(payload) {
  const tokens = [];
  let offset = 0;
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  while (offset + 4 <= payload.length) {
    const length = view.getUint16(offset, false);
    const codePoint = view.getUint16(offset + 2, false);
    if (length < 4 || offset + length > payload.length) break;
    if (codePoint === CodePoint.SECTKN) tokens.push(Buffer.from(payload.subarray(offset + 4, offset + length)));
    offset += length;
  }
  return tokens;
}

function sqlCard(code = 0, state = '00000', affected = 0) {
  const data = new Uint8Array(54); data[0] = 0;
  const view = new DataView(data.buffer); view.setInt32(1, code, true); data.set(new TextEncoder().encode(state), 5); view.setInt32(27, affected, true);
  return data;
}

function mockConfig(port, timeoutMs = 2000) {
  return {
    host: '127.0.0.1', port, database: 'TESTDB', user: 'DB2INST1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs, blockSize: 65535,
    securityMechanism: 9, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '', gssapiTokenFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  };
}

const FIXED_CLIENT_PRIVATE = 0x7f3a91c4d2e5b80716a4c9f0d3b8e2176a5c4d3e2f1908a7b6c5d4e3f2a1b0c9n;
const FIXED_SERVER_PRIVATE = 0x1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90an;
const FIXED_CLIENT_PUBLIC = Buffer.from('7bed3dc4a1117944e7c7e85f820aab5fe0ce78ea0c03f95dfbd4189ed855b88a', 'hex');
const FIXED_SERVER_PUBLIC = Buffer.from('2da57ee6ce7b212ededc6d07c1cdb28f6ed1ed5cbcade212e30d8ee51d69f3f1', 'hex');
const FIXED_SHARED = Buffer.from('303a7d3ca869a26d920662ac3b57d8960aca6c8f6b69f01e8bfc66474e09af53', 'hex');

test('SECMEC 9 Diffie-Hellman and DES match independently generated Derby/openssl vectors', () => {
  assert.deepEqual(Buffer.from(secmec9PublicKey(FIXED_CLIENT_PRIVATE)), FIXED_CLIENT_PUBLIC);
  assert.deepEqual(Buffer.from(secmec9PublicKey(FIXED_SERVER_PRIVATE)), FIXED_SERVER_PUBLIC);
  assert.deepEqual(Buffer.from(secmec9SessionKey(FIXED_SERVER_PUBLIC, FIXED_CLIENT_PRIVATE)), FIXED_SHARED);

  const encryptedPassword = Buffer.from(encryptSecmec9(Buffer.from('SecretPass123', 'utf8'), FIXED_SERVER_PUBLIC, FIXED_CLIENT_PRIVATE));
  assert.equal(encryptedPassword.toString('hex'), 'a64a5e61af1a35a65d2bc8ec7ae550f4');
  const encryptedUserid = Buffer.from(encryptSecmec9(Buffer.from('DB2INST1', 'utf8'), FIXED_SERVER_PUBLIC, FIXED_CLIENT_PRIVATE));
  assert.equal(encryptedUserid.toString('hex'), 'e3241d6a67a9646ce5fafe9f9bae7acc');

  // The server side mirrors the client: shared secret from the client public key,
  // IV from the server's own public key.
  const serverKey = Buffer.from(secmec9SessionKey(FIXED_CLIENT_PUBLIC, FIXED_SERVER_PRIVATE)).subarray(12, 20);
  assert.equal(Buffer.from(desCbcDecryptWith(serverKey, FIXED_SERVER_PUBLIC.subarray(12, 20), encryptedPassword)).toString('utf8'), 'SecretPass123');
  assert.equal(Buffer.from(desCbcDecryptWith(serverKey, FIXED_SERVER_PUBLIC.subarray(12, 20), encryptedUserid)).toString('utf8'), 'DB2INST1');
});

test('SECMEC 9 handshake exchanges DH keys in ACCSEC/ACCSECRD and encrypts userid and password', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      const reader = new SocketReader(socket);
      const opening = await readChain(reader);
      assert.deepEqual(opening.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
      const accsec = ddm.parseDdmParameters(opening[1].payload);
      assert.equal(new DataView(accsec.get(CodePoint.SECMEC).buffer, accsec.get(CodePoint.SECMEC).byteOffset, 2).getUint16(0, false), 9);
      const clientPublic = Buffer.from(accsec.get(CodePoint.SECTKN));
      assert.equal(clientPublic.length, 32);

      await reply(socket, CodePoint.ACCSECRD, new Uint8Array([
        ...ddm.packUint16(CodePoint.SECMEC, 9),
        ...ddm.packBytes(CodePoint.SECTKN, FIXED_SERVER_PUBLIC),
      ]));

      const check = await readChain(reader);
      assert.deepEqual(check.map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
      const secchk = ddm.parseDdmParameters(check[0].payload);
      assert.equal(new DataView(secchk.get(CodePoint.SECMEC).buffer, secchk.get(CodePoint.SECMEC).byteOffset, 2).getUint16(0, false), 9);
      // Db2 LUW carries both encrypted values as SECTKN fields, not USRID/PASSWORD.
      const tokens = collectSectkn(check[0].payload);
      assert.equal(tokens.length, 2);
      const serverKey = Buffer.from(secmec9SessionKey(clientPublic, FIXED_SERVER_PRIVATE)).subarray(12, 20);
      const decryptedUserid = decodeCp500(Buffer.from(desCbcDecryptWith(serverKey, FIXED_SERVER_PUBLIC.subarray(12, 20), tokens[0])));
      const decryptedPassword = decodeCp500(Buffer.from(desCbcDecryptWith(serverKey, FIXED_SERVER_PUBLIC.subarray(12, 20), tokens[1])));
      assert.equal(decryptedUserid, 'DB2INST1');
      assert.equal(decryptedPassword, 'password');

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

  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  assert.equal(connection.isClosed, false);
  await connection.close();
  await serverTask;
});

test('SECMEC 9 rejects a missing server security token before sending SECCHK', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      const reader = new SocketReader(socket);
      await readChain(reader);
      await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 9));
      await new Promise((resolve) => setTimeout(resolve, 50));
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  await assert.rejects(Db2Connection.connect(mockConfig(server.address().port)), /server security token/);
  await serverTask;
});
