const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const { CodePoint } = require('../dist/cjs/network/codepoint.js');
const ddm = require('../dist/cjs/network/ddm.js');
const dss = require('../dist/cjs/network/dss.js');
const { Db2Connection } = require('../dist/cjs/network/session.js');
const { Db2AuthenticationError, Db2UnsupportedFeatureError } = require('../dist/cjs/errors.js');
const { decodeCp500, encodeCp500 } = require('../dist/cjs/converters/ebcdic.js');
const { desCbcDecryptWith, secmec9PublicKey, secmec9SessionKey } = require('../dist/cjs/security/secmec9.js');

class SocketReader {
  constructor(socket) {
    this.socket = socket; this.chunks = []; this.length = 0; this.error = null; this.waiters = new Set();
    socket.on('data', (chunk) => { this.chunks.push(chunk); this.length += chunk.length; this.notify(); });
    socket.on('end', () => { this.error ||= new Error('socket ended'); this.notify(); });
    socket.on('error', (error) => { this.error ||= error; this.notify(); });
    socket.on('close', () => { this.error ||= new Error('socket closed'); this.notify(); });
  }
  notify() { for (const resolve of this.waiters) resolve(); this.waiters.clear(); }
  async readExact(count) {
    while (this.length < count) { if (this.error) throw this.error; await new Promise((resolve) => this.waiters.add(resolve)); }
    const out = Buffer.alloc(count); let offset = 0;
    while (offset < count) {
      const head = this.chunks[0]; const n = Math.min(head.length, count - offset);
      head.copy(out, offset, 0, n); offset += n; this.length -= n;
      if (n === head.length) this.chunks.shift(); else this.chunks[0] = head.subarray(n);
    }
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

function sqlCard() {
  const data = new Uint8Array(54); data[0] = 0;
  const view = new DataView(data.buffer); view.setInt32(1, 0, true); data.set(new TextEncoder().encode('00000'), 5);
  return data;
}

function mockConfig(port, securityMechanism) {
  return {
    host: '127.0.0.1', port, database: 'TESTDB', user: 'DB2INST1', password: 'clear-password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 2000, blockSize: 65535,
    securityMechanism, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '', gssapiTokenFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  };
}

const SERVER_PRIVATE = 0x1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90an;
const SERVER_PUBLIC = Buffer.from('2da57ee6ce7b212ededc6d07c1cdb28f6ed1ed5cbcade212e30d8ee51d69f3f1', 'hex');

function uint16(field) { return new DataView(field.buffer, field.byteOffset, 2).getUint16(0); }
function uint16List(codePoint, values) {
  const body = new Uint8Array(values.length * 2);
  values.forEach((value, index) => new DataView(body.buffer).setUint16(index * 2, value));
  return ddm.packBytes(codePoint, body);
}

async function mockServer(t, handler) {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = handler(socket, new SocketReader(socket));
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));
  return { port: server.address().port, done: () => serverTask };
}

async function assertNoFurtherRequest(reader, message) {
  const extraRequest = await reader.readExact(1).then(() => true, () => false);
  assert.equal(extraRequest, false, message);
}

for (const echoEncalg of [true, false]) {
  test(`SECMEC 7 sends the encrypted password in SECCHK/SECTKN (server ${echoEncalg ? 'echoes' : 'omits'} ENCALG)`, async (t) => {
    const mock = await mockServer(t, async (socket, reader) => {
      const opening = await readChain(reader);
      assert.deepEqual(opening.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
      const request = ddm.parseDdmParameters(opening[1].payload);
      assert.equal(uint16(request.get(CodePoint.SECMEC)), 7);
      assert.equal(uint16(request.get(CodePoint.ENCALG)), 1);
      const clientPublic = Buffer.from(request.get(CodePoint.SECTKN));
      assert.equal(clientPublic.length, 32);

      // Db2 LUW SERVER_ENCRYPT reply: SECMEC=7, SECTKN(32), ENCALG=1.
      await reply(socket, CodePoint.ACCSECRD, new Uint8Array([
        ...ddm.packUint16(CodePoint.SECMEC, 7),
        ...ddm.packBytes(CodePoint.SECTKN, SERVER_PUBLIC),
        ...(echoEncalg ? ddm.packUint16(CodePoint.ENCALG, 1) : []),
      ]));

      const authentication = await readChain(reader);
      assert.deepEqual(authentication.map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
      const parameters = [];
      ddm.readDdmObjectsCursor(authentication[0].payload, (codePoint, payload) => parameters.push([codePoint, Buffer.from(payload)]));
      assert.deepEqual(parameters.map(([codePoint]) => codePoint), [CodePoint.SECMEC, CodePoint.RDBNAM, CodePoint.USRID, CodePoint.SECTKN]);
      const fields = new Map(parameters);
      assert.equal(uint16(fields.get(CodePoint.SECMEC)), 7);
      assert.equal(decodeCp500(fields.get(CodePoint.USRID)), 'DB2INST1');
      assert.equal(Buffer.from(authentication[0].payload).includes(Buffer.from(encodeCp500('clear-password'))), false, 'the plaintext password must not appear anywhere in SECCHK');

      const shared = Buffer.from(secmec9SessionKey(clientPublic, SERVER_PRIVATE));
      const iv = Buffer.alloc(8); Buffer.from(encodeCp500('DB2INST1')).copy(iv, 0, 0, 8);
      const clearPassword = desCbcDecryptWith(shared.subarray(12, 20), iv, fields.get(CodePoint.SECTKN));
      assert.equal(decodeCp500(clearPassword), 'clear-password');

      await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
      await reply(socket, CodePoint.ACCRDBRM, ddm.packBytes(CodePoint.RDBINTTKN, Uint8Array.of(4, 28, 167, 50)));
      const initialize = await readChain(reader);
      assert.deepEqual(initialize.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
      await reply(socket, CodePoint.SQLCARD, sqlCard());
    });

    const connection = await Db2Connection.connect(mockConfig(mock.port, 7));
    assert.equal(connection.isClosed, false);
    await connection.close();
    await mock.done();
  });
}

test('SECMEC 3 against a SERVER_ENCRYPT server fails with the accepted list and no SECCHK', async (t) => {
  const mock = await mockServer(t, async (socket, reader) => {
    const opening = await readChain(reader);
    const request = ddm.parseDdmParameters(opening[1].payload);
    assert.equal(uint16(request.get(CodePoint.SECMEC)), 3);
    assert.equal(request.has(CodePoint.SECTKN), false);
    assert.equal(request.has(CodePoint.ENCALG), false);
    // Captured from Db2 LUW AUTHENTICATION=SERVER_ENCRYPT: one SECMEC parameter
    // listing 7, 9, 10, SECCHKCD=1 and an ENCALG list; no server SECTKN.
    await reply(socket, CodePoint.ACCSECRD, new Uint8Array([
      ...uint16List(CodePoint.SECMEC, [7, 9, 10]),
      ...ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(1)),
      ...uint16List(CodePoint.ENCALG, [1, 2]),
    ]));
    await assertNoFurtherRequest(reader, 'client must not send SECCHK, plaintext or otherwise, after a rejected ACCSEC');
  });

  await assert.rejects(Db2Connection.connect(mockConfig(mock.port, 3)), (error) => {
    assert.ok(error instanceof Db2UnsupportedFeatureError);
    assert.match(error.message, /rejected security mechanism 3 \(SECCHKCD=1\); the server accepts 7, 9, 10/);
    return true;
  });
  await mock.done();
});

test('SECMEC 7 against a SERVER-only server fails with the accepted list and no SECCHK', async (t) => {
  const mock = await mockServer(t, async (socket, reader) => {
    await readChain(reader);
    // Captured from Db2 LUW AUTHENTICATION=SERVER: SECMEC list 3, 5 and SECCHKCD=1.
    await reply(socket, CodePoint.ACCSECRD, new Uint8Array([
      ...uint16List(CodePoint.SECMEC, [3, 5]),
      ...ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(1)),
    ]));
    await assertNoFurtherRequest(reader, 'client must not downgrade to mechanism 3');
  });

  await assert.rejects(Db2Connection.connect(mockConfig(mock.port, 7)), /rejected security mechanism 7 \(SECCHKCD=1\); the server accepts 3, 5/);
  await mock.done();
});

test('SECMEC 7 rejects a malformed ACCSECRD SECMEC list', async (t) => {
  const mock = await mockServer(t, async (socket, reader) => {
    await readChain(reader);
    await reply(socket, CodePoint.ACCSECRD, ddm.packBytes(CodePoint.SECMEC, Uint8Array.of(0, 7, 0)));
    await assertNoFurtherRequest(reader);
  });

  await assert.rejects(Db2Connection.connect(mockConfig(mock.port, 7)), (error) => {
    assert.ok(error instanceof Db2AuthenticationError);
    assert.match(error.message, /malformed security mechanism list/);
    return true;
  });
  await mock.done();
});

test('SECMEC 7 rejects AES selection without sending SECCHK or downgrading', async (t) => {
  const mock = await mockServer(t, async (socket, reader) => {
    await readChain(reader);
    await reply(socket, CodePoint.ACCSECRD, new Uint8Array([
      ...ddm.packUint16(CodePoint.SECMEC, 7),
      ...ddm.packUint16(CodePoint.ENCALG, 2),
      ...ddm.packBytes(CodePoint.SECTKN, SERVER_PUBLIC),
    ]));
    await assertNoFurtherRequest(reader, 'client must fail before SECCHK rather than sending credentials');
  });

  await assert.rejects(Db2Connection.connect(mockConfig(mock.port, 7)), (error) => {
    assert.ok(error instanceof Db2UnsupportedFeatureError);
    assert.match(error.message, /ENCALG=2.*DES \(ENCALG=1\) only/);
    return true;
  });
  await mock.done();
});

test('SECMEC 7 rejects a missing server DH token before sending SECCHK', async (t) => {
  const mock = await mockServer(t, async (socket, reader) => {
    await readChain(reader);
    await reply(socket, CodePoint.ACCSECRD, new Uint8Array([
      ...ddm.packUint16(CodePoint.SECMEC, 7),
      ...ddm.packUint16(CodePoint.ENCALG, 1),
    ]));
    await assertNoFurtherRequest(reader);
  });

  await assert.rejects(Db2Connection.connect(mockConfig(mock.port, 7)), /key exchange did not return a usable server security token/);
  await mock.done();
});

test('SECMEC 7 surfaces a non-zero ACCSECRD SECCHKCD for the requested mechanism', async (t) => {
  const mock = await mockServer(t, async (socket, reader) => {
    await readChain(reader);
    await reply(socket, CodePoint.ACCSECRD, new Uint8Array([
      ...ddm.packUint16(CodePoint.SECMEC, 7),
      ...ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0x13)),
    ]));
    await assertNoFurtherRequest(reader);
  });

  await assert.rejects(Db2Connection.connect(mockConfig(mock.port, 7)), (error) => {
    assert.ok(error instanceof Db2AuthenticationError);
    assert.match(error.message, /SECCHKCD=19/);
    return true;
  });
  await mock.done();
});

test('SECMEC 7 authentication failure does not retry with plaintext mechanism 3', async (t) => {
  const mock = await mockServer(t, async (socket, reader) => {
    await readChain(reader);
    await reply(socket, CodePoint.ACCSECRD, new Uint8Array([
      ...ddm.packUint16(CodePoint.SECMEC, 7),
      ...ddm.packBytes(CodePoint.SECTKN, SERVER_PUBLIC),
      ...ddm.packUint16(CodePoint.ENCALG, 1),
    ]));
    const authentication = await readChain(reader);
    assert.deepEqual(authentication.map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
    assert.equal(uint16(ddm.parseDdmParameters(authentication[0].payload).get(CodePoint.SECMEC)), 7);
    // Db2 answers a wrong password with SECCHKCD=0x0F.
    await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0x0f)));
    await assertNoFurtherRequest(reader, 'client must close after failed encrypted authentication without a mechanism 3 retry');
  });

  await assert.rejects(Db2Connection.connect(mockConfig(mock.port, 7)), /authentication failed \(SECCHKCD=15\)/i);
  await mock.done();
});

// No configured mechanism (securityMechanism 0): the driver requests 3 and, when a
// SERVER_ENCRYPT server refuses it, sends a second ACCSEC on the same connection
// with the strongest encrypting mechanism the server listed. Verified live on
// Db2 LUW 11.5.4.0 and 12.1.5.0.
for (const [listed, expected] of [[[7, 9, 10], 9], [[7, 10], 7]]) {
  test(`unset security mechanism follows a server that accepts ${listed.join(', ')} to SECMEC ${expected}`, async (t) => {
    const mock = await mockServer(t, async (socket, reader) => {
      const opening = await readChain(reader);
      assert.deepEqual(opening.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
      assert.equal(uint16(ddm.parseDdmParameters(opening[1].payload).get(CodePoint.SECMEC)), 3);
      await reply(socket, CodePoint.ACCSECRD, new Uint8Array([
        ...uint16List(CodePoint.SECMEC, listed),
        ...ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(1)),
        ...uint16List(CodePoint.ENCALG, [1, 2]),
      ]));

      const retry = await readChain(reader);
      assert.deepEqual(retry.map((packet) => packet.codePoint), [CodePoint.ACCSEC], 'the retry is a lone ACCSEC, not a new EXCSAT or a SECCHK');
      const request = ddm.parseDdmParameters(retry[0].payload);
      assert.equal(uint16(request.get(CodePoint.SECMEC)), expected);
      const clientPublic = Buffer.from(request.get(CodePoint.SECTKN));
      assert.equal(clientPublic.length, 32);
      await reply(socket, CodePoint.ACCSECRD, new Uint8Array([
        ...ddm.packUint16(CodePoint.SECMEC, expected),
        ...ddm.packBytes(CodePoint.SECTKN, SERVER_PUBLIC),
        ...ddm.packUint16(CodePoint.ENCALG, 1),
      ]));

      const authentication = await readChain(reader);
      assert.deepEqual(authentication.map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
      const payload = Buffer.from(authentication[0].payload);
      assert.equal(payload.includes(Buffer.from(encodeCp500('clear-password'))), false, 'the plaintext password must not appear in SECCHK');
      const secchk = ddm.parseDdmParameters(authentication[0].payload);
      assert.equal(uint16(secchk.get(CodePoint.SECMEC)), expected);
      assert.equal(secchk.has(CodePoint.PASSWORD), false);
      assert.equal(secchk.has(CodePoint.USRID), expected === 7, 'only SECMEC 7 sends a clear user ID');

      await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
      await reply(socket, CodePoint.ACCRDBRM, ddm.packBytes(CodePoint.RDBINTTKN, Uint8Array.of(4, 28, 167, 50)));
      await readChain(reader);
      await reply(socket, CodePoint.SQLCARD, sqlCard());
    });

    const connection = await Db2Connection.connect(mockConfig(mock.port, 0));
    assert.equal(connection.isClosed, false);
    await connection.close();
    await mock.done();
  });
}

test('unset security mechanism does not follow a server that lists no encrypting mechanism', async (t) => {
  const mock = await mockServer(t, async (socket, reader) => {
    await readChain(reader);
    await reply(socket, CodePoint.ACCSECRD, new Uint8Array([
      ...uint16List(CodePoint.SECMEC, [10, 11]),
      ...ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(1)),
    ]));
    await assertNoFurtherRequest(reader, 'no second ACCSEC and no SECCHK');
  });

  await assert.rejects(Db2Connection.connect(mockConfig(mock.port, 0)), /rejected security mechanism 3 \(SECCHKCD=1\); the server accepts 10, 11/);
  await mock.done();
});

test('unset security mechanism stops when the server rejects the mechanism it listed', async (t) => {
  const mock = await mockServer(t, async (socket, reader) => {
    await readChain(reader);
    await reply(socket, CodePoint.ACCSECRD, new Uint8Array([
      ...uint16List(CodePoint.SECMEC, [7, 9]),
      ...ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(1)),
    ]));
    await readChain(reader);
    await reply(socket, CodePoint.ACCSECRD, new Uint8Array([
      ...uint16List(CodePoint.SECMEC, [3]),
      ...ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(1)),
    ]));
    await assertNoFurtherRequest(reader, 'no third ACCSEC and no SECCHK');
  });

  await assert.rejects(Db2Connection.connect(mockConfig(mock.port, 0)), (error) => {
    assert.ok(error instanceof Db2UnsupportedFeatureError);
    assert.match(error.message, /listed security mechanism 9 as accepted but then rejected it/);
    return true;
  });
  await mock.done();
});
