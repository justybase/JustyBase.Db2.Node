const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const { CodePoint } = require('../dist/cjs/network/codepoint.js');
const ddm = require('../dist/cjs/network/ddm.js');
const dss = require('../dist/cjs/network/dss.js');
const { Db2Connection } = require('../dist/cjs/network/session.js');
const { Db2AuthenticationError, Db2ConfigurationError, Db2ConnectionClosedError, Db2DatabaseNotFoundError, Db2ServerError, Db2TimeoutError, Db2UnsupportedFeatureError } = require('../dist/cjs/errors.js');

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

function mockConfig(port, timeoutMs = 2000) {
  return {
    host: '127.0.0.1', port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
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

test('connection timeout does not expire an idle session', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      const reader = await serveConnectionSetup(socket);
      await new Promise((resolve) => setTimeout(resolve, 120));
      assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.RDBCMM]);
      await reply(socket, CodePoint.SQLCARD, sqlCard());
      await new Promise((resolve) => socket.once('close', resolve));
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect(mockConfig(server.address().port, 60));
  await new Promise((resolve) => setTimeout(resolve, 140));
  await connection.ping();
  await connection.close();
  await serverTask;
  await new Promise((resolve) => server.close(() => resolve()));
});

test('setClientInfo strips control characters before sending SQL and updating cached state', async () => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      const reader = await serveConnectionSetup(socket);
      const update = await readChain(reader);
      assert.deepEqual(update.map((packet) => packet.codePoint), [CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
      const statement = update.find((packet) => packet.codePoint === CodePoint.SQLSTT);
      assert.ok(statement);
      const payload = Buffer.from(statement.payload);
      const length = payload.readUInt32BE(1);
      const sql = payload.toString('utf8', 5, 5 + length);
      assert.equal(sql, "SET CLIENT APPLNAME 'audit''app'");
      await reply(socket, CodePoint.SQLCARD, sqlCard());
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  try {
    await connection.setClientInfo({ applicationName: "audit\r\n'app" });
    assert.equal(connection.getClientInfo().applicationName, "audit'app");
  } finally {
    await connection.close();
    await serverTask;
    await new Promise((resolve) => server.close(() => resolve()));
  }
});

test('command deadline sends INTRDBRQS and keeps a session after confirmed SQLCODE -952', async (t) => {
  const interruptToken = Uint8Array.of(0x04, 0x1c, 0xa7, 0x32);
  let mainTask;
  let interruptTask;
  let commandSeen;
  const waitingForCommand = new Promise((resolve) => { commandSeen = resolve; });
  let mainSocket;
  const server = net.createServer((socket) => {
    if (!mainTask) {
      mainSocket = socket;
      mainTask = (async () => {
        const reader = await serveConnectionSetup(socket, interruptToken);
        assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
        commandSeen();
        assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.RDBCMM]);
        await reply(socket, CodePoint.SQLCARD, sqlCard());
        await new Promise((resolve) => socket.once('close', resolve));
      })();
      mainTask.catch(() => socket.destroy());
    } else {
      interruptTask = (async () => {
        const reader = new SocketReader(socket);
        assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT]);
        await reply(socket, CodePoint.EXCSATRD);
        const [interrupt] = await readChain(reader);
        assert.equal(interrupt.codePoint, CodePoint.INTRDBRQS);
        assert.deepEqual(ddm.parseDdmParameters(interrupt.payload).get(CodePoint.RDBINTTKN), interruptToken);
        await reply(socket, CodePoint.CMDCMPRM, ddm.packUint16(CodePoint.SVRCOD, 0));
        await reply(mainSocket, CodePoint.SQLERRRM, ddm.packBytes(CodePoint.SRVDGN, new TextEncoder().encode('SQLERRP:SQLRI2D0 SQLCODE:-952')));
        await new Promise((resolve) => socket.once('close', resolve));
      })();
      interruptTask.catch(() => socket.destroy());
    }
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  const operation = connection.query('VALUES 1', undefined, { timeoutMs: 30, cancelCleanupTimeoutMs: 500 });
  await waitingForCommand;
  await assert.rejects(operation, (error) => error?.name === 'TimeoutError');
  assert.equal(connection.isClosed, false, 'a confirmed -952 may leave the connection reusable');
  await connection.ping();
  await connection.close();
  await Promise.all([mainTask, interruptTask]);
  await new Promise((resolve) => server.close(() => resolve()));
});

test('command deadline discards the session when SQLINTR is rejected', async (t) => {
  let mainTask;
  let interruptTask;
  let commandSeen;
  const waitingForCommand = new Promise((resolve) => { commandSeen = resolve; });
  let mainSocket;
  const server = net.createServer((socket) => {
    if (!mainTask) {
      mainSocket = socket;
      mainTask = (async () => {
        const reader = await serveConnectionSetup(socket);
        assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
        commandSeen();
        await new Promise((resolve) => socket.once('close', resolve));
      })();
      mainTask.catch(() => socket.destroy());
    } else {
      interruptTask = (async () => {
        const reader = new SocketReader(socket);
        assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT]);
        await reply(socket, CodePoint.EXCSATRD);
        assert.equal((await readChain(reader))[0].codePoint, CodePoint.INTRDBRQS);
        await reply(socket, CodePoint.CMDCMPRM, ddm.packUint16(CodePoint.SVRCOD, 1));
        await new Promise((resolve) => socket.once('close', resolve));
      })();
      interruptTask.catch(() => socket.destroy());
    }
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  const operation = connection.query('VALUES 1', undefined, { timeoutMs: 30, cancelCleanupTimeoutMs: 500 });
  await waitingForCommand;
  await assert.rejects(operation, (error) => error?.name === 'TimeoutError');
  assert.equal(connection.isClosed, true, 'an unconfirmed interrupt must make the transport unusable');
  await connection.close();
  await Promise.all([mainTask, interruptTask]);
  await new Promise((resolve) => server.close(() => resolve()));
});

test('command cleanup timeout closes a session after SQLINTR acknowledgment without command completion', async () => {
  let mainTask;
  let interruptTask;
  let commandSeen;
  const waitingForCommand = new Promise((resolve) => { commandSeen = resolve; });
  const server = net.createServer((socket) => {
    if (!mainTask) {
      mainTask = (async () => {
        const reader = await serveConnectionSetup(socket);
        assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
        commandSeen();
        await new Promise((resolve) => socket.once('close', resolve));
      })();
      mainTask.catch(() => socket.destroy());
    } else {
      interruptTask = (async () => {
        const reader = new SocketReader(socket);
        assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT]);
        await reply(socket, CodePoint.EXCSATRD);
        assert.equal((await readChain(reader))[0].codePoint, CodePoint.INTRDBRQS);
        await reply(socket, CodePoint.CMDCMPRM, ddm.packUint16(CodePoint.SVRCOD, 0));
        await new Promise((resolve) => socket.once('close', resolve));
      })();
      interruptTask.catch(() => socket.destroy());
    }
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  const operation = connection.query('VALUES 1', undefined, { timeoutMs: 20, cancelCleanupTimeoutMs: 40 });
  await waitingForCommand;
  await assert.rejects(operation, (error) => error?.name === 'TimeoutError');
  assert.equal(connection.isClosed, true, 'the uncompleted exchange must not be returned for reuse');
  await connection.close();
  await Promise.all([mainTask, interruptTask]);
  await new Promise((resolve) => server.close(() => resolve()));
});

test('a late natural completion with a missing interrupt acknowledgment still discards the session', async () => {
  let mainTask;
  let interruptTask;
  let interruptSeen;
  const waitingForInterrupt = new Promise((resolve) => { interruptSeen = resolve; });
  let mainSocket;
  const server = net.createServer((socket) => {
    if (!mainTask) {
      mainSocket = socket;
      mainTask = (async () => {
        const reader = await serveConnectionSetup(socket);
        assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSQLIMM, CodePoint.SQLSTT, CodePoint.RDBCMM]);
        await waitingForInterrupt;
        await reply(socket, CodePoint.SQLCARD, sqlCard(0, '00000', 1));
        await new Promise((resolve) => socket.once('close', resolve));
      })();
      mainTask.catch(() => socket.destroy());
    } else {
      interruptTask = (async () => {
        const reader = new SocketReader(socket);
        assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT]);
        await reply(socket, CodePoint.EXCSATRD);
        assert.equal((await readChain(reader))[0].codePoint, CodePoint.INTRDBRQS);
        interruptSeen();
        // The main statement finishes naturally, but the sent interrupt gets no
        // CMDCMPRM acknowledgment before its bounded transport timeout.
        await new Promise((resolve) => socket.once('close', resolve));
      })();
      interruptTask.catch(() => socket.destroy());
    }
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  const operation = connection.execute('VALUES 1', undefined, { timeoutMs: 30, cancelCleanupTimeoutMs: 80 });
  await assert.rejects(operation, (error) => error?.name === 'TimeoutError');
  assert.equal(connection.isClosed, true, 'a missing INTRDBRQS acknowledgment must poison the transport even after a natural reply');
  await connection.close();
  await Promise.all([mainTask, interruptTask]);
  await new Promise((resolve) => server.close(() => resolve()));
});

test('connection negotiates DRDA, serializes commands, decodes a query row, and closes cleanly', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = serveMockDb2(socket);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));
  const port = server.address().port;
  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 2000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });
  const warnings = [];
  const unsubscribeWarnings = connection.addWarningListener((warning) => warnings.push(warning));
  await assert.rejects(connection.switchUser('alice"; DROP TABLE T'), /invalid DB2 session user identifier/);
  assert.equal(connection.currentSessionUser, 'db2inst1');
  await connection.ping();
  assert.equal(await connection.execute('VALUES 1'), 1);
  const result = await connection.query('VALUES CAST(42 AS INTEGER)');
  assert.deepEqual(result.columns, []);
  assert.deepEqual(result.rows, [[42]]);
  assert.equal(warnings.length, 1);
  assert.equal(warnings[0].sqlState, '01003');
  assert.match(warnings[0].warningFlags, /^W/u);
  const statement = await connection.prepare('VALUES CAST(? AS INTEGER)');
  assert.equal(statement.parameterColumns.length, 1);
  assert.deepEqual((await statement.query([42])).rows, [[42]]);
  assert.equal(warnings.length, 1);
  unsubscribeWarnings();
  statement.close();
  await assert.rejects(connection.query('SELECT 1'), (error) => {
    assert.ok(error instanceof Db2ServerError);
    assert.equal(error.sqlCode, -952);
    assert.equal(error.sqlState, undefined, 'SQLERRRM diagnostic does not carry SQLSTATE');
    assert.match(error.message, /SQLCODE=-952/u);
    return true;
  });
  await connection.ping();
  await connection.close();
  await assert.rejects(connection.ping(), Db2ConnectionClosedError);
  await serverTask;
});

test('row importer treats an empty async source as a no-op without preparing a target', async (t) => {
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
  let prepares = 0;
  let writes = 0;
  let commits = 0;
  connection.prepareLocked = async () => { prepares++; throw new Error('empty import must not prepare'); };
  connection.executePreparedLocked = async () => { writes++; return 1; };
  connection.executeControlLocked = async () => { commits++; };
  async function* emptySource() {}
  try {
    const progress = await connection.importRows(emptySource(), {
      table: 'TABLE_THAT_DOES_NOT_EXIST', columns: ['ID'],
    });
    assert.deepEqual(progress, { sourceRowsRead: 0, committedRows: 0, committedBatches: 0, estimatedCommittedBytes: 0 });
    assert.equal(prepares, 0);
    assert.equal(writes, 0);
    assert.equal(commits, 0);
    assert.equal(connection.isAutoCommit, true);
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('row importer batches rows atomically or at confirmed checkpoints with progress snapshots', async (t) => {
  for (const policy of ['atomic', 'commitEveryRows']) {
    let serverTask;
    const server = net.createServer((socket) => {
      serverTask = (async () => {
        await serveConnectionSetup(socket);
        await new Promise((resolve) => socket.once('close', resolve));
      })();
      serverTask.catch(() => socket.destroy());
    });
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const connection = await Db2Connection.connect(mockConfig(server.address().port));
    const writes = [];
    const commits = [];
    let prepares = 0;
    connection.prepareLocked = async (sql) => {
      prepares++;
      assert.equal(sql, 'INSERT INTO "IMPORT_TARGET" ("ID") VALUES (?)');
      return { sql, parameterColumns: [{ sqlType: 496 }] };
    };
    connection.executePreparedLocked = async (_statement, values) => { writes.push([...values]); return 1; };
    connection.executeControlLocked = async () => { commits.push(writes.length); };
    const observed = [];
    async function* source() { for (let value = 1; value <= 5; value++) yield [value]; }
    try {
      const progress = await connection.importRows(source(), { table: 'IMPORT_TARGET', columns: ['ID'], expectedSqlTypes: [496] }, {
        policy,
        maxBatchRows: 2,
        maxBatchBytes: 1000,
        onProgress: (snapshot) => observed.push(snapshot),
      });
      assert.equal(prepares, 1);
      assert.deepEqual(writes, [[1], [2], [3], [4], [5]]);
      assert.equal(progress.sourceRowsRead, 5);
      assert.equal(progress.committedRows, 5);
      assert.equal(progress.committedBatches, policy === 'atomic' ? 1 : 3);
      assert.equal(progress.estimatedCommittedBytes, 50);
      if (policy === 'atomic') {
        assert.deepEqual(commits, [5]);
        assert.deepEqual(observed.map((x) => [x.sourceRowsRead, x.committedRows]), [[5, 0]]);
      } else {
        assert.deepEqual(commits, [2, 4, 5]);
        assert.deepEqual(observed.map((x) => [x.sourceRowsRead, x.committedRows]), [[2, 0], [4, 2], [5, 4]]);
      }
      assert.equal(connection.isAutoCommit, true);
    } finally {
      await connection.close();
      await serverTask;
      await new Promise((resolve) => server.close(() => resolve()));
    }
  }
});

test('row importer reports periodic progress every notifyAfterRows staged rows', async (t) => {
  for (const policy of ['atomic', 'commitEveryRows']) {
    let serverTask;
    const server = net.createServer((socket) => {
      serverTask = (async () => {
        await serveConnectionSetup(socket);
        await new Promise((resolve) => socket.once('close', resolve));
      })();
      serverTask.catch(() => socket.destroy());
    });
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const connection = await Db2Connection.connect(mockConfig(server.address().port));
    const writes = [];
    connection.prepareLocked = async (sql) => ({ sql, parameterColumns: [{ sqlType: 496 }] });
    connection.executePreparedLocked = async (_statement, values) => { writes.push([...values]); return 1; };
    connection.executeControlLocked = async () => {};
    const observed = [];
    async function* source() { for (let value = 1; value <= 5; value++) yield [value]; }
    try {
      const progress = await connection.importRows(source(), { table: 'IMPORT_TARGET', columns: ['ID'] }, {
        policy, maxBatchRows: 2, maxBatchBytes: 1000, notifyAfterRows: 2,
        onProgress: (snapshot) => observed.push([snapshot.sourceRowsRead, snapshot.committedRows]),
      });
      assert.deepEqual(writes, [[1], [2], [3], [4], [5]]);
      assert.equal(progress.sourceRowsRead, 5);
      assert.equal(progress.committedRows, 5);
      if (policy === 'atomic') {
        assert.deepEqual(observed, [[2, 0], [4, 0], [5, 0]]);
      } else {
        assert.deepEqual(observed, [[2, 0], [4, 2], [5, 4]]);
      }
      assert.equal(connection.isAutoCommit, true);
    } finally {
      await connection.close();
      await serverTask;
      await new Promise((resolve) => server.close(() => resolve()));
    }
  }
});

test('row importer rejects tableLock with commitEveryRows before any I/O', async (t) => {
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
  let prepares = 0;
  let writes = 0;
  connection.prepareLocked = async () => { prepares++; throw new Error('rejected import must not prepare'); };
  connection.executePreparedLocked = async () => { writes++; return 1; };
  async function* source() { yield [1]; }
  try {
    await assert.rejects(connection.importRows(source(), { table: 'IMPORT_TARGET', columns: ['ID'] }, {
      policy: 'commitEveryRows', tableLock: true,
    }), /tableLock requires the atomic policy/u);
    assert.equal(prepares, 0);
    assert.equal(writes, 0);
    assert.equal(connection.isAutoCommit, true);
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('row importer takes an exclusive table lock before the first write', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      await serveConnectionSetup(socket);
      await new Promise((resolve) => socket.once('close', resolve));
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  const events = [];
  connection.prepareLocked = async (sql) => ({ sql, parameterColumns: [{ sqlType: 496 }] });
  connection.executeLocked = async (sql) => { events.push(`lock:${sql}`); return 0; };
  connection.executePreparedLocked = async (_statement, values) => { events.push(`write:${values[0]}`); return 1; };
  connection.executeControlLocked = async () => {};
  async function* source() { yield [1]; yield [2]; }
  try {
    const progress = await connection.importRows(source(), { table: 'IMPORT_TARGET', columns: ['ID'] }, {
      tableLock: true,
    });
    assert.deepEqual(events, [
      'lock:LOCK TABLE "IMPORT_TARGET" IN EXCLUSIVE MODE',
      'write:1',
      'write:2',
    ]);
    assert.equal(progress.sourceRowsRead, 2);
    assert.equal(progress.committedRows, 2);
    assert.equal(progress.committedBatches, 1);
    assert.equal(connection.isAutoCommit, true);
  } finally {
    await connection.close();
    await serverTask;
    await new Promise((resolve) => server.close(() => resolve()));
  }
});

test('row importer observer error at a periodic boundary keeps the last checkpoint', async (t) => {
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
  connection.prepareLocked = async (sql) => ({ sql, parameterColumns: [{ sqlType: 496 }] });
  connection.executePreparedLocked = async () => 1;
  connection.executeControlLocked = async () => {};
  async function* source() { for (let value = 1; value <= 5; value++) yield [value]; }
  try {
    await assert.rejects(connection.importRows(source(), { table: 'IMPORT_TARGET', columns: ['ID'] }, {
      policy: 'commitEveryRows', maxBatchRows: 2, notifyAfterRows: 1,
      onProgress: (snapshot) => { if (snapshot.sourceRowsRead === 3) throw new Error('periodic observer stopped import'); },
    }), (error) => {
      assert.equal(error.name, 'Db2ImportError');
      assert.match(error.message, /progress observer failed at source row 3/u);
      assert.equal(error.failedSourceRow, 3);
      assert.equal(error.progress.sourceRowsRead, 3);
      assert.equal(error.progress.committedRows, 2);
      assert.equal(error.progress.committedBatches, 1);
      return true;
    });
    assert.equal(connection.isAutoCommit, true);
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('row importer per-write command timeout discards the session fail-closed', async (t) => {
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
  connection.prepareLocked = async (sql) => ({ sql, parameterColumns: [{ sqlType: 496 }] });
  connection.executePreparedLocked = async () => new Promise(() => {});
  async function* source() { yield [1]; }
  try {
    await assert.rejects(connection.importRows(source(), { table: 'IMPORT_TARGET', columns: ['ID'] }, {
      commandTimeoutMs: 50,
    }), (error) => {
      assert.equal(error.name, 'Db2ImportError');
      assert.ok(error.cause instanceof Db2TimeoutError);
      assert.match(error.cause.message, /row insert for source row 1 timed out after 50 ms/u);
      assert.equal(error.failedSourceRow, 1);
      return true;
    });
    assert.equal(connection.isClosed, true);
  } finally {
    await connection.close().catch(() => {});
    await serverTask;
  }
});

test('row importer combines row and byte limits at the exact boundary and flushes an oversized singleton', async (t) => {
  const expectedRows = [[1, 'a'], [2, 'bb'], [3, null], [4, null], [5, null], [6, 'ccc'], [7, 'd'], [8, null], [9, 'x'.repeat(40)]];
  const expectedBytes = [19, 20, 12, 12, 12, 21, 19, 12, 58];
  for (const policy of ['atomic', 'commitEveryRows']) {
    let serverTask;
    const server = net.createServer((socket) => {
      serverTask = (async () => {
        await serveConnectionSetup(socket);
        await new Promise((resolve) => socket.once('close', resolve));
      })();
      serverTask.catch(() => socket.destroy());
    });
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const connection = await Db2Connection.connect(mockConfig(server.address().port));
    const writes = [];
    const commitAt = [];
    const snapshots = [];
    connection.prepareLocked = async (sql) => ({ sql, parameterColumns: [{ sqlType: 496 }, { sqlType: 448 }] });
    connection.executePreparedLocked = async (_statement, values) => { writes.push([...values]); return 1; };
    connection.executeControlLocked = async () => commitAt.push(writes.length);
    async function* source() { yield* expectedRows; }
    try {
      const progress = await connection.importRows(source(), { table: 'IMPORT_TARGET', columns: ['ID', 'TXT'] }, {
        policy, maxBatchRows: 3, maxBatchBytes: 39, onProgress: (snapshot) => snapshots.push(snapshot),
      });
      assert.deepEqual(writes, expectedRows);
      assert.equal(progress.sourceRowsRead, 9);
      assert.equal(progress.committedRows, 9);
      assert.equal(progress.estimatedCommittedBytes, expectedBytes.reduce((sum, value) => sum + value, 0));
      if (policy === 'atomic') {
        assert.equal(progress.committedBatches, 1);
        assert.deepEqual(commitAt, [9]);
        assert.deepEqual(snapshots.map(({ sourceRowsRead, committedRows, estimatedCommittedBytes }) => [sourceRowsRead, committedRows, estimatedCommittedBytes]), [[9, 0, 0]]);
      } else {
        assert.equal(progress.committedBatches, 5);
        assert.deepEqual(commitAt, [2, 5, 6, 8, 9]);
        assert.deepEqual(snapshots.map(({ sourceRowsRead, committedRows, committedBatches, estimatedCommittedBytes }) => [sourceRowsRead, committedRows, committedBatches, estimatedCommittedBytes]), [
          [2, 0, 0, 0], // two rows hit the exact 39-byte boundary
          [5, 2, 1, 39], // three small NULL rows hit the row-count cap below 39 bytes
          [7, 5, 2, 75], // row 6 plus row 7 would exceed the byte boundary
          [9, 6, 3, 96], // flush the partial tail before staging row 9
          [9, 8, 4, 127], // the 58-byte row is sent alone as an oversized singleton
        ]);
      }
      assert.equal(connection.isAutoCommit, true);
    } finally {
      await connection.close();
      await serverTask;
      await new Promise((resolve) => server.close(() => resolve()));
    }
  }
});

test('row importer rolls back a failing checkpoint batch and reports its source row', async (t) => {
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
  const writes = [];
  const controls = [];
  connection.prepareLocked = async (sql) => ({ sql, parameterColumns: [{ sqlType: 496 }] });
  connection.executePreparedLocked = async (_statement, values) => {
    writes.push(values[0]);
    if (values[0] === 3) throw new Error('duplicate key');
    return 1;
  };
  connection.executeControlLocked = async (control) => controls.push(control);
  async function* source() { for (let value = 1; value <= 4; value++) yield [value]; }
  try {
    await assert.rejects(connection.importRows(source(), { table: 'IMPORT_TARGET', columns: ['ID'] }, {
      policy: 'commitEveryRows', maxBatchRows: 2,
    }), (error) => {
      assert.equal(error.name, 'Db2ImportError');
      assert.equal(error.failedSourceRow, 3);
      assert.equal(error.progress.sourceRowsRead, 4, 'the second row of the failing batch was already read');
      assert.equal(error.progress.committedRows, 2);
      assert.equal(error.progress.committedBatches, 1);
      return true;
    });
    assert.deepEqual(writes, [1, 2, 3]);
    assert.equal(controls.length, 2, 'one earlier commit and one failing-batch rollback');
    assert.equal(connection.isAutoCommit, true);
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('row importer drains progressive LOBs before advancing source and binds typed EXTDTA streams', async (t) => {
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
  const payloads = [];
  const sourceLobFinished = [false, false, false, false];
  const expected = [Buffer.from('streamed-row-lob'), Buffer.from('CLOB 東京'), Buffer.from('DBCLOB 🚀'), Buffer.from('<root/>')];
  const columns = [
    { name: 'B', sqlType: 404, length: 1_000_000n, precision: 0, scale: 0, nullable: true },
    { name: 'C', sqlType: 408, length: 1_000_000n, precision: 0, scale: 0, nullable: true },
    { name: 'D', sqlType: 412, length: 1_000_000n, precision: 0, scale: 0, nullable: true },
    { name: 'X', sqlType: 988, length: 1_000_000n, precision: 0, scale: 0, nullable: true },
  ];
  connection.prepareLocked = async (sql) => ({ sql, parameterColumns: columns });
  connection.executePreparedLocked = async (_statement, values) => {
    const row = [];
    for (const parameter of values) {
      assert.equal(typeof parameter.source?.[Symbol.asyncIterator], 'function');
      const chunks = [];
      for await (const chunk of parameter.source) chunks.push(Buffer.from(chunk));
      row.push(Buffer.concat(chunks));
    }
    payloads.push(row);
    return 1;
  };
  connection.executeControlLocked = async () => {};
  async function* source() {
    yield {
      columns,
      values: expected.map((bytes, index) => ({
        kind: index === 0 ? 'binary-lob' : 'text-lob', length: BigInt(bytes.length),
        async *stream() {
          if (index === 0) {
            yield bytes.subarray(0, 4);
            yield bytes.subarray(4);
          } else yield bytes.toString('utf8');
          sourceLobFinished[index] = true;
        },
      })),
    };
    assert.deepEqual(sourceLobFinished, [true, true, true, true], 'all locators must be consumed before the source advances');
    yield { columns, values: [Uint8Array.of(0, 1, 0xff), 'materialized CLOB', 'materialized DBCLOB', '<root/>'] };
  }
  try {
    const progress = await connection.importRows(source(), { table: 'LOB_TARGET', columns: ['B', 'C', 'D', 'X'] }, {
      policy: 'commitEveryRows', maxBatchRows: 4,
    });
    assert.equal(progress.committedRows, 2);
    assert.deepEqual(payloads, [
      expected,
      [Buffer.from([0, 1, 0xff]), Buffer.from('materialized CLOB'), Buffer.from('materialized DBCLOB'), Buffer.from('<root/>')],
    ]);
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('row importer AbortSignal stops a stalled source LOB spool before destination DML', async (t) => {
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
  const controller = new AbortController();
  const reason = new DOMException('cancel import source', 'AbortError');
  let markStarted;
  const started = new Promise((resolve) => { markStarted = resolve; });
  let writes = 0;
  const columns = [{ name: 'B', sqlType: 404, length: 1_000_000n, precision: 0, scale: 0, nullable: true }];
  connection.prepareLocked = async (sql) => ({ sql, parameterColumns: columns });
  connection.executePreparedLocked = async () => { writes++; return 1; };
  connection.executeControlLocked = async () => {};
  connection.requestInterrupt = async () => true;
  async function* sourceLob() {
    yield Uint8Array.of(1, 2, 3);
    markStarted();
    await new Promise((_resolve, reject) => {
      controller.signal.addEventListener('abort', () => reject(controller.signal.reason), { once: true });
    });
  }
  async function* source() {
    yield {
      columns,
      values: [{ kind: 'binary-lob', length: 1_000_000n, stream: () => sourceLob() }],
    };
  }
  try {
    const pending = connection.importRows(source(), { table: 'LOB_CANCEL_TARGET', columns: ['B'] }, {
      signal: controller.signal, cancelCleanupTimeoutMs: 1_000,
    });
    await started;
    controller.abort(reason);
    await assert.rejects(pending, error => error === reason);
    assert.equal(writes, 0);
    assert.equal(connection.isAutoCommit, true);
    assert.equal(connection.isClosed, false);
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('row importer source LOB failure preserves confirmed checkpoints and rolls back the current work', async (t) => {
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
  const columns = [{ name: 'B', sqlType: 404, length: 1_000_000n, precision: 0, scale: 0, nullable: true }];
  let writes = 0;
  let controls = 0;
  connection.prepareLocked = async (sql) => ({ sql, parameterColumns: columns });
  connection.executePreparedLocked = async (_statement, values) => {
    writes++;
    for await (const _chunk of values[0].source) {}
    return 1;
  };
  connection.executeControlLocked = async () => { controls++; };
  async function* goodLob() { yield Uint8Array.of(1, 2, 3); }
  async function* brokenLob() {
    yield Uint8Array.of(4, 5, 6);
    throw new Error('source LOB read failed');
  }
  async function* source() {
    yield { columns, values: [{ kind: 'binary-lob', length: 3n, stream: () => goodLob() }] };
    yield { columns, values: [{ kind: 'binary-lob', length: 99n, stream: () => brokenLob() }] };
  }
  try {
    await assert.rejects(connection.importRows(source(), { table: 'LOB_FAILURE_TARGET', columns: ['B'] }, {
      policy: 'commitEveryRows', maxBatchRows: 1,
    }), error => {
      assert.equal(error.name, 'Db2ImportError');
      assert.equal(error.failedSourceRow, 2);
      assert.equal(error.cause.message, 'source LOB read failed');
      assert.equal(error.progress.sourceRowsRead, 2);
      assert.equal(error.progress.committedRows, 1);
      assert.equal(error.progress.committedBatches, 1);
      return true;
    });
    assert.equal(writes, 1, 'the failed source LOB must not reach destination DML');
    assert.equal(controls, 2, 'the first row commits and the current transaction rolls back');
    assert.equal(connection.isAutoCommit, true);
    assert.equal(connection.isClosed, false);
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('row importer preserves a checkpoint when source TCP disconnects during progressive LOB spooling', async (t) => {
  let sourceTask;
  const sourceServer = net.createServer((socket) => {
    sourceTask = serveProgressiveLobTransportDisconnect(socket);
    sourceTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => sourceServer.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => sourceServer.close(() => resolve())));

  let destinationTask;
  const destinationServer = net.createServer((socket) => {
    destinationTask = (async () => {
      await serveConnectionSetup(socket);
      await new Promise((resolve) => socket.once('close', resolve));
    })();
    destinationTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => destinationServer.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => destinationServer.close(() => resolve())));

  const source = await Db2Connection.connect(mockConfig(sourceServer.address().port));
  const destination = await Db2Connection.connect(mockConfig(destinationServer.address().port));
  const blobColumn = { name: 'B', sqlType: 405, length: 300_000n, precision: 0, scale: 0, nullable: true };
  const writes = [];
  const controls = [];
  destination.prepareLocked = async (sql) => ({ sql, parameterColumns: [blobColumn] });
  destination.executePreparedLocked = async (_statement, values) => { writes.push([...values]); return 1; };
  destination.executeControlLocked = async (payload) => {
    controls.push(new DataView(payload.buffer, payload.byteOffset, payload.byteLength).getUint16(2, false));
  };

  try {
    await assert.rejects(destination.importRows(source.queryStream('SELECT B FROM DISCONNECT_LOB_FIXTURE'), {
      table: 'DISCONNECT_LOB_TARGET', columns: ['B'],
    }, { policy: 'commitEveryRows', maxBatchRows: 1 }), (error) => {
      assert.equal(error.name, 'Db2ImportError');
      assert.equal(error.failedSourceRow, 2);
      assert.equal(error.progress.sourceRowsRead, 2);
      assert.equal(error.progress.committedRows, 1);
      assert.equal(error.progress.committedBatches, 1);
      assert.equal(error.progress.estimatedCommittedBytes, 2);
      assert.match(error.cause.message, /socket closed|socket ended/i);
      return true;
    });
    assert.deepEqual(writes, [[null]], 'only the earlier NULL row reaches destination DML');
    assert.deepEqual(controls, [CodePoint.RDBCMM, CodePoint.RDBRLLBCK]);
    assert.equal(source.isClosed, true, 'a source transport failure invalidates the source session');
    assert.equal(destination.isAutoCommit, true);
    assert.equal(destination.isClosed, false, 'the destination remains open after source transport loss');
  } finally {
    await source.close();
    await destination.close();
    await sourceTask;
    await destinationTask;
  }
});

test('row importer discards the destination session when its transport fails during a row write', async (t) => {
  let destinationTask;
  const destinationServer = net.createServer((socket) => {
    destinationTask = (async () => {
      await serveConnectionSetup(socket);
      await new Promise((resolve) => socket.once('close', resolve));
    })();
    destinationTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => destinationServer.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => destinationServer.close(() => resolve())));

  const destination = await Db2Connection.connect(mockConfig(destinationServer.address().port));
  const column = { name: 'N', sqlType: 497, length: 4n, precision: 0, scale: 0, nullable: true };
  const writes = [];
  const controls = [];
  destination.prepareLocked = async (sql) => ({ sql, parameterColumns: [column] });
  destination.executePreparedLocked = async (_statement, values) => {
    writes.push([...values]);
    if (writes.length === 2) {
      // Simulate the destination TCP connection dying mid-write; the session
      // itself marks the transport uncertain.
      destination.closed = true;
      destination.socket.destroy();
      throw new Error('socket closed');
    }
    return 1;
  };
  destination.executeControlLocked = async (payload) => {
    controls.push(new DataView(payload.buffer, payload.byteOffset, payload.byteLength).getUint16(2, false));
  };

  const source = (async function* () { yield [1]; yield [2]; yield [3]; })();
  try {
    await assert.rejects(destination.importRows(source, { table: 'DESTINATION_FAILURE_TARGET', columns: ['N'] },
      { policy: 'commitEveryRows', maxBatchRows: 1 }), (error) => {
      assert.equal(error.name, 'Db2ImportError');
      assert.equal(error.failedSourceRow, 2, 'the failing row is identified, not the batch');
      assert.equal(error.progress.sourceRowsRead, 2);
      assert.equal(error.progress.committedRows, 1, 'only the earlier checkpoint is confirmed committed');
      assert.equal(error.progress.committedBatches, 1);
      assert.match(error.cause.message, /socket closed/i);
      return true;
    });
    assert.deepEqual(writes, [[1], [2]], 'no replay of the uncertain destination write');
    assert.deepEqual(controls, [CodePoint.RDBCMM], 'no rollback is attempted on a dead destination transport');
    assert.equal(destination.isClosed, true, 'the destination session is discarded');
    await assert.rejects(destination.query('VALUES 29'), /closed|destroyed|socket/i);
  } finally {
    await destination.close().catch(() => {});
    await destinationTask;
  }
});

test('connection handshake classifies missing database and authentication failures', async (t) => {
  for (const scenario of [
    { name: 'missing database', reply: CodePoint.RDBNFNRM, expected: Db2DatabaseNotFoundError },
    { name: 'rejected credentials', reply: CodePoint.SECCHKRM, expected: Db2AuthenticationError },
  ]) {
    let serverTask;
    const server = net.createServer((socket) => {
      serverTask = (async () => {
        const reader = new SocketReader(socket);
        assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
        if (scenario.reply === CodePoint.RDBNFNRM) {
          await reply(socket, CodePoint.RDBNFNRM);
          return;
        }
        await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
        assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
        await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(8)));
      })();
      serverTask.catch(() => socket.destroy());
    });
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const config = {
      host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
      sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 2000, blockSize: 65535,
      securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
      clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
    };
    await assert.rejects(Db2Connection.connect(config), (error) => {
      assert.ok(error instanceof scenario.expected, `${scenario.name}: received ${error?.constructor?.name}`);
      assert.equal(error.category, scenario.expected === Db2AuthenticationError ? 'authentication' : 'database-not-found');
      if (error instanceof Db2AuthenticationError) assert.equal(error.securityCode, 8);
      return true;
    });
    await serverTask;
    await new Promise((resolve) => server.close(() => resolve()));
  }
});

test('connection refuses a server-selected security mechanism different from the request', async () => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      const reader = new SocketReader(socket);
      const initial = await readChain(reader);
      assert.deepEqual(initial.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
      const requested = ddm.parseDdmParameters(initial[1].payload).get(CodePoint.SECMEC);
      assert.equal(new DataView(requested.buffer, requested.byteOffset, 2).getUint16(0, false), 3);
      const closed = new Promise((resolve) => socket.once('close', resolve));
      await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 4));
      await closed;
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const config = { ...mockConfig(server.address().port), securityMechanism: 3 };
  await assert.rejects(Db2Connection.connect(config), (error) => {
    assert.ok(error instanceof Db2UnsupportedFeatureError);
    assert.match(error.message, /rejected security mechanism 3; the server accepts 4/);
    return true;
  });
  await serverTask;
  await new Promise((resolve) => server.close(resolve));
});

test('programmatic connection config rejects unsupported security mechanisms before opening a socket', async () => {
  await assert.rejects(Db2Connection.connect({ ...mockConfig(1), securityMechanism: 4 }), (error) => {
    assert.equal(error.name, 'Db2ConfigurationError');
    assert.match(error.message, /supported mechanisms/);
    return true;
  });
});

test('programmatic connection config validation exposes a stable category', async () => {
  await assert.rejects(Db2Connection.connect({ host: '', port: 50000, database: 'TESTDB', user: 'db2inst1', password: '', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 1000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  }), (error) => {
    assert.ok(error instanceof Db2ConfigurationError);
    assert.equal(error.category, 'invalid-configuration');
    return true;
  });
});

test('query decodes inline BLOB, CLOB, and DBCLOB values and external XML data', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = serveInlineLobProjection(socket);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 2000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });
  try {
    const result = await connection.query('VALUES CAST(1 AS BLOB(4))');
    assert.deepEqual(result.rows, [[
      Uint8Array.of(0, 0x7f, 0x80, 0xff), 'CLOB café', 'DBCLOB 東京', '<root/>',
    ]]);
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('query decodes a zero-length BOOLEAN parameter string as false, not undefined-driven true', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = serveZeroLengthBooleanProjection(socket);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 2000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });
  try {
    const result = await connection.query('VALUES CAST(1 AS BOOLEAN)');
    assert.deepEqual(result.rows, [[false]]);
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('query reports an unsupported field type as a typed unsupported-feature error', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = serveUnsupportedFieldTypeProjection(socket);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 2000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });
  try {
    await assert.rejects(connection.query('VALUES CAST(1 AS INTEGER)'), (error) => {
      assert.equal(error.name, 'Db2UnsupportedFeatureError');
      assert.equal(error.category, 'unsupported-feature');
      assert.match(error.message, /unsupported DB2 query field type 0x7f/u);
      return true;
    });
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('query fetches progressive LOB references through repeated GETNXTCHK exchanges', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = serveProgressiveLobQuery(socket);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 5000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });
  try {
    const result = await connection.query('SELECT B FROM PROGRESSIVE_LOB_FIXTURE');
    assert.deepEqual(result.rows, [[progressiveLobExpectedBytes()]]);
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('queryStream yields progressive LOB locators before fetching and streams bytes with early-close drain', async (t) => {
  let serverTask;
  let chunksServed = 0;
  const server = net.createServer((socket) => {
    serverTask = servePublicProgressiveLobQuery(socket, 0xc9, [Uint8Array.of(1, 2), Uint8Array.of(3, 4), Uint8Array.of(5)], () => chunksServed++);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));
  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  try {
    const rows = connection.queryStream('SELECT B FROM STREAM_LOB_FIXTURE');
    const first = await rows.next();
    assert.equal(first.done, false);
    const lob = first.value.values[0];
    assert.equal(lob.kind, 'binary-lob');
    assert.equal(lob.length, 5n);
    assert.equal(chunksServed, 0, 'the query iterator must yield before the locator is fetched');
    const stream = lob.stream()[Symbol.asyncIterator]();
    assert.deepEqual(await stream.next(), { value: Uint8Array.of(1, 2), done: false });
    await stream.return();
    assert.equal(chunksServed, 3, 'closing the value stream drains the remaining locator chunks');
    await assert.rejects(async () => { for await (const _chunk of lob.stream()) { } }, /one-shot/);
    assert.equal((await rows.next()).done, true);
    assert.equal(connection.isClosed, false);
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('queryStream decodes text LOBs incrementally across UTF-8 and UTF-16BE chunk boundaries', async (t) => {
  const payloads = [
    { type: 0xc7, chunks: [Uint8Array.of(0xf0, 0x9f), Uint8Array.of(0x98, 0x80, 0xc3), Uint8Array.of(0xa9)], expected: '😀é' },
    { type: 0xcd, chunks: [Uint8Array.of(0x00, 0x41, 0xd8), Uint8Array.of(0x3d), Uint8Array.of(0xde, 0x00)], expected: 'A😀' },
  ];
  for (const { type, chunks, expected } of payloads) {
    let serverTask;
    const server = net.createServer((socket) => {
      serverTask = servePublicProgressiveLobQuery(socket, type, chunks);
      serverTask.catch(() => socket.destroy());
    });
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const connection = await Db2Connection.connect(mockConfig(server.address().port));
    try {
      const rows = connection.queryStream('SELECT C FROM STREAM_TEXT_LOB_FIXTURE');
      const first = await rows.next();
      const lob = first.value.values[0];
      assert.equal(lob.kind, 'text-lob');
      let actual = '';
      for await (const chunk of lob.stream()) actual += chunk;
      assert.equal(actual, expected);
      assert.equal((await rows.next()).done, true);
    } finally {
      await connection.close();
      await serverTask;
      await new Promise((resolve) => server.close(() => resolve()));
    }
  }
});

test('queryStream preserves an empty progressive LOB as an empty one-shot stream', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = servePublicProgressiveLobQuery(socket, 0xc9, []);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));
  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  try {
    const rows = connection.queryStream('SELECT B FROM EMPTY_STREAM_LOB_FIXTURE');
    const first = await rows.next();
    const lob = first.value.values[0];
    assert.equal(lob.kind, 'binary-lob');
    assert.equal(lob.length, 0n);
    const chunks = [];
    for await (const chunk of lob.stream()) chunks.push(chunk);
    assert.deepEqual(chunks, []);
    assert.equal((await rows.next()).done, true);
    assert.equal(connection.isClosed, false);
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('query closes its session when a progressive LOB chunk makes no progress', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = serveProgressiveLobQuery(socket, true);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 5000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });
  try {
    await assert.rejects(connection.query('SELECT B FROM PROGRESSIVE_LOB_FIXTURE'), /made no progress before the final chunk/);
    assert.equal(connection.isClosed, true, 'an unresolved server locator must not leave a reusable session');
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('queryStream discards the session when the peer resets a progressive LOB chunk', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = serveProgressiveLobTransportDisconnect(socket);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  try {
    const rows = connection.queryStream('SELECT B FROM RESET_LOB_FIXTURE');
    const first = await rows.next();
    assert.equal(first.value.values[0], null, 'the first row is a typed NULL BLOB');
    const second = await rows.next();
    const lob = second.value.values[0];
    assert.equal(lob.kind, 'binary-lob');
    await assert.rejects(
      async () => { for await (const _chunk of lob.stream()) { /* consume */ } },
      /socket closed|socket ended/i,
    );
    assert.equal(connection.isClosed, true, 'a peer reset during GETNXTCHK leaves the locator uncertain');
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('early row-stream exit drains a later progressive LOB row and keeps the session reusable', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = serveEarlyExitProgressiveLobQuery(socket);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 5000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });
  try {
    const received = [];
    for await (const row of connection.queryStream('SELECT ID, B FROM EARLY_EXIT_LOB_FIXTURE')) {
      received.push(row.values[0]);
      break;
    }
    assert.deepEqual(received, [42]);
    assert.equal(connection.isClosed, false);
    assert.deepEqual((await connection.query('VALUES 29')).rows, [[29]]);
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('early row-stream exit yields before a later same-block LOB fetch and drains it before reuse', async (t) => {
  let serverTask;
  let locatorFetchObserved = false;
  const server = net.createServer((socket) => {
    serverTask = serveEarlyExitProgressiveLobQuery(socket, {
      locatorInSameBlock: true,
      onLocatorRequested: () => { locatorFetchObserved = true; },
    });
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 5000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });
  try {
    const received = [];
    for await (const row of connection.queryStream('SELECT ID, B FROM EARLY_EXIT_LOB_FIXTURE')) {
      assert.equal(locatorFetchObserved, false, 'a later row locator is not fetched before its row is yielded');
      received.push(row.values[0]);
      break;
    }
    assert.deepEqual(received, [42]);
    assert.equal(locatorFetchObserved, true, 'the unread locator is drained when the stream closes');
    assert.equal(connection.isClosed, false);
    assert.deepEqual((await connection.query('VALUES 29')).rows, [[29]]);
  } finally {
    await connection.close();
    await serverTask;
  }
});

test('AbortSignal interrupts a pending GETNXTCHK and the session is reusable after SQLCODE -952', async (t) => {
  const interruptToken = Uint8Array.of(0x04, 0x1c, 0xa7, 0x32);
  let getNextStarted;
  const waitingForGetNext = new Promise((resolve) => { getNextStarted = resolve; });
  let interruptAcknowledged;
  const waitingForInterruptAck = new Promise((resolve) => { interruptAcknowledged = resolve; });
  let interruptApplied;
  const waitingForInterruptApplied = new Promise((resolve) => { interruptApplied = resolve; });
  let mainTask;
  let interruptTask;
  let mainSocket;
  const server = net.createServer((socket) => {
    if (!mainTask) {
      mainSocket = socket;
      mainTask = serveInterruptedProgressiveLobQuery(socket, interruptToken, getNextStarted, waitingForInterruptApplied);
      mainTask.catch(() => socket.destroy());
    } else {
      interruptTask = serveProgressiveLobInterrupt(socket, mainSocket, interruptToken, interruptAcknowledged, interruptApplied);
      interruptTask.catch(() => socket.destroy());
    }
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 5000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });
  try {
    const cancellation = new AbortController();
    const query = connection.query('SELECT B FROM INTERRUPTED_PROGRESSIVE_LOB_FIXTURE', undefined, { signal: cancellation.signal });
    await waitingForGetNext;
    const cancelAt = performance.now();
    cancellation.abort();
    await assert.rejects(query, (error) => error.name === 'AbortError');
    const operationCompletedAt = performance.now();
    const interruptAckAt = await waitingForInterruptAck;
    assert.equal(connection.isClosed, false, 'SQLCODE -952 confirms that Db2 ended the active request');
    assert.deepEqual((await connection.query('VALUES 29')).rows, [[29]]);
    assert.ok(interruptAckAt >= cancelAt);
    assert.ok(operationCompletedAt >= interruptAckAt, 'the query must not complete before Db2 acknowledges the interrupt response');
    console.log(`GETNXTCHK cancellation: interrupt acknowledgment ${(interruptAckAt - cancelAt).toFixed(1)} ms; operation completion ${(operationCompletedAt - cancelAt).toFixed(1)} ms`);
  } finally {
    await connection.close();
    await Promise.all([mainTask, interruptTask]);
  }
});

test('AbortSignal interrupts pending GETNXTCHK during public row streaming and preserves the session', async (t) => {
  const interruptToken = Uint8Array.of(0x04, 0x1c, 0xa7, 0x32);
  let getNextStarted;
  const waitingForGetNext = new Promise((resolve) => { getNextStarted = resolve; });
  let interruptAcknowledged;
  const waitingForInterruptAck = new Promise((resolve) => { interruptAcknowledged = resolve; });
  let interruptApplied;
  const waitingForInterruptApplied = new Promise((resolve) => { interruptApplied = resolve; });
  let mainTask;
  let interruptTask;
  let mainSocket;
  const server = net.createServer((socket) => {
    if (!mainTask) {
      mainSocket = socket;
      mainTask = serveInterruptedProgressiveLobQuery(socket, interruptToken, getNextStarted, waitingForInterruptApplied);
      mainTask.catch(() => socket.destroy());
    } else {
      interruptTask = serveProgressiveLobInterrupt(socket, mainSocket, interruptToken, interruptAcknowledged, interruptApplied);
      interruptTask.catch(() => socket.destroy());
    }
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 5000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });
  try {
    const cancellation = new AbortController();
    const query = (async () => {
      for await (const _row of connection.queryStream('SELECT B FROM INTERRUPTED_PROGRESSIVE_LOB_FIXTURE', { signal: cancellation.signal })) { }
    })();
    await waitingForGetNext;
    const cancelAt = performance.now();
    cancellation.abort();
    await assert.rejects(query, (error) => error.name === 'AbortError');
    const operationCompletedAt = performance.now();
    const interruptAckAt = await waitingForInterruptAck;
    assert.equal(connection.isClosed, false, 'SQLCODE -952 must end the interrupted row stream without draining a canceled cursor');
    assert.deepEqual((await connection.query('VALUES 29')).rows, [[29]]);
    assert.ok(interruptAckAt >= cancelAt);
    assert.ok(operationCompletedAt >= interruptAckAt);
    console.log(`GETNXTCHK row-stream cancellation: interrupt acknowledgment ${(interruptAckAt - cancelAt).toFixed(1)} ms; operation completion ${(operationCompletedAt - cancelAt).toFixed(1)} ms`);
  } finally {
    await connection.close();
    await Promise.all([mainTask, interruptTask]);
  }
});

test('an acknowledged SQLINTR without GETNXTCHK completion discards the per-LOB session', async (t) => {
  const interruptToken = Uint8Array.of(0x04, 0x1c, 0xa7, 0x32);
  let getNextStarted;
  const waitingForGetNext = new Promise((resolve) => { getNextStarted = resolve; });
  let interruptAcknowledged;
  const waitingForInterruptAck = new Promise((resolve) => { interruptAcknowledged = resolve; });
  let mainTask;
  let interruptTask;
  const server = net.createServer((socket) => {
    if (!mainTask) {
      mainTask = serveAckOnlyInterruptedProgressiveLobQuery(socket, interruptToken, getNextStarted);
      mainTask.catch(() => socket.destroy());
    } else {
      interruptTask = serveAckOnlyProgressiveLobInterrupt(socket, interruptToken, interruptAcknowledged);
      interruptTask.catch(() => socket.destroy());
    }
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  try {
    const cancellation = new AbortController();
    const query = (async () => {
      for await (const _row of connection.queryStream('SELECT B FROM INTERRUPTED_PROGRESSIVE_LOB_FIXTURE', {
        signal: cancellation.signal,
        cancelCleanupTimeoutMs: 200,
      })) { }
    })();
    await waitingForGetNext;
    cancellation.abort();
    await assert.rejects(query);
    const interruptAckAt = await waitingForInterruptAck;
    assert.ok(interruptAckAt > 0);
    assert.equal(connection.isClosed, true, 'an unconfirmed GETNXTCHK outcome must discard the session');
  } finally {
    await connection.close();
    await Promise.all([mainTask, interruptTask]);
  }
});

async function serveInterruptedProgressiveLobQuery(socket, interruptToken, getNextStarted, interruptionApplied) {
  const reader = new SocketReader(socket);
  try {
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
    await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
    await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
    await reply(socket, CodePoint.ACCRDBRM, ddm.packBytes(CodePoint.RDBINTTKN, interruptToken));
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
    await reply(socket, CodePoint.SQLCARD, sqlCard());

    const query = await readChain(reader);
    assert.deepEqual(query.map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
    assert.deepEqual([...ddm.parseDdmParameters(query[2].payload).get(CodePoint.DYNDTAFMT)], [0xf1]);
    await reply(socket, CodePoint.QRYDSC, Uint8Array.of(6, 0x76, 0xd0, 0xc9, 0x80, 9), true);
    await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, new Uint8Array(8)));
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.CNTQRY]);

    const row = new Uint8Array(20);
    row.set([0xff, 0, 0, 3]);
    const rowView = new DataView(row.buffer);
    rowView.setBigUint64(4, 1n, false);
    rowView.setBigUint64(12, 0x1122334455667788n, false);
    await reply(socket, CodePoint.QRYDTA, row);
    const [getNext] = await readChain(reader);
    assert.equal(getNext.codePoint, CodePoint.GETNXTCHK);
    getNextStarted();
    await interruptionApplied;

    const reuseQuery = await readChain(reader);
    assert.deepEqual(reuseQuery.map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
    await reply(socket, CodePoint.QRYDSC, Uint8Array.of(6, 0x76, 0xd0, 0x02, 0, 4), true);
    await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, new Uint8Array(8)));
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.CNTQRY]);
    await reply(socket, CodePoint.QRYDTA, Uint8Array.of(0xff, 0, 29, 0, 0, 0), true);
    await reply(socket, CodePoint.ENDQRYRM);
  } finally { socket.end(); }
}

async function serveProgressiveLobInterrupt(socket, mainSocket, interruptToken, acknowledged, applied) {
  const reader = new SocketReader(socket);
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT]);
  await reply(socket, CodePoint.EXCSATRD);
  const [interrupt] = await readChain(reader);
  assert.equal(interrupt.codePoint, CodePoint.INTRDBRQS);
  assert.deepEqual(ddm.parseDdmParameters(interrupt.payload).get(CodePoint.RDBINTTKN), interruptToken);
  await reply(socket, CodePoint.CMDCMPRM, ddm.packUint16(CodePoint.SVRCOD, 0));
  acknowledged(performance.now());
  await reply(mainSocket, CodePoint.SQLCARD, sqlCard(-952, '57014'));
  applied();
}

async function serveAckOnlyInterruptedProgressiveLobQuery(socket, interruptToken, getNextStarted) {
  const reader = new SocketReader(socket);
  try {
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
    await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
    await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
    await reply(socket, CodePoint.ACCRDBRM, ddm.packBytes(CodePoint.RDBINTTKN, interruptToken));
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
    await reply(socket, CodePoint.SQLCARD, sqlCard());

    const query = await readChain(reader);
    assert.deepEqual(query.map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
    await reply(socket, CodePoint.QRYDSC, Uint8Array.of(6, 0x76, 0xd0, 0xc9, 0x80, 9), true);
    await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, new Uint8Array(8)));
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.CNTQRY]);

    const row = new Uint8Array(20);
    row.set([0xff, 0, 0, 3]);
    const rowView = new DataView(row.buffer);
    rowView.setBigUint64(4, 1n, false);
    rowView.setBigUint64(12, 0x1122334455667788n, false);
    await reply(socket, CodePoint.QRYDTA, row);
    const [getNext] = await readChain(reader);
    assert.equal(getNext.codePoint, CodePoint.GETNXTCHK);
    getNextStarted();
    // The interrupt is acknowledged, but this connection never completes GETNXTCHK.
    await new Promise((resolve) => socket.once('close', resolve));
  } finally { socket.end(); }
}

async function serveAckOnlyProgressiveLobInterrupt(socket, interruptToken, acknowledged) {
  const reader = new SocketReader(socket);
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT]);
  await reply(socket, CodePoint.EXCSATRD);
  const [interrupt] = await readChain(reader);
  assert.equal(interrupt.codePoint, CodePoint.INTRDBRQS);
  assert.deepEqual(ddm.parseDdmParameters(interrupt.payload).get(CodePoint.RDBINTTKN), interruptToken);
  await reply(socket, CodePoint.CMDCMPRM, ddm.packUint16(CodePoint.SVRCOD, 0));
  acknowledged(performance.now());
  await new Promise((resolve) => socket.once('close', resolve));
}

async function serveInlineLobProjection(socket) {
  const reader = new SocketReader(socket);
  try {
    const first = await readChain(reader);
    assert.deepEqual(first.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
    await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
    const second = await readChain(reader);
    assert.deepEqual(second.map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
    await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
    await reply(socket, CodePoint.ACCRDBRM);
    const setup = await readChain(reader);
    assert.deepEqual(setup.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
    await reply(socket, CodePoint.SQLCARD, sqlCard());

    const query = await readChain(reader);
    assert.deepEqual(query.map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
    const descriptor = new Uint8Array([
      6, 0x76, 0xd0, 0xc9, 0x80, 9,
      6, 0x76, 0xd0, 0xcf, 0x80, 9,
      6, 0x76, 0xd0, 0xcd, 0x80, 9,
      6, 0x76, 0xd0, 0xc7, 0x80, 9,
    ]);
    await reply(socket, CodePoint.QRYDSC, descriptor, true);
    const queryId = new Uint8Array(8);
    new DataView(queryId.buffer).setBigUint64(0, 1n, false);
    await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, queryId), true);

    const row = [0xff, 0];
    const inline = (value, length = value.length) => {
      row.push(0, 0x01);
      const encodedLength = new Uint8Array(8);
      new DataView(encodedLength.buffer).setBigUint64(0, BigInt(length), false);
      row.push(...encodedLength, ...value);
    };
    inline(Uint8Array.of(0, 0x7f, 0x80, 0xff));
    inline(new TextEncoder().encode('CLOB café'));
    // DBCLOB wire bytes are UTF-16 big-endian; use the exact same code units as DB2.
    const text = 'DBCLOB 東京';
    const dbclob = new Uint8Array(text.length * 2);
    const dbclobView = new DataView(dbclob.buffer);
    for (let i = 0; i < text.length; i++) dbclobView.setUint16(i * 2, text.charCodeAt(i), false);
    inline(dbclob, text.length);
    row.push(0, 0x02, ...new Uint8Array(8).fill(0xff));
    await reply(socket, CodePoint.QRYDTA, Uint8Array.from(row), true);
    await reply(socket, CodePoint.EXTDTA, Uint8Array.from([0, ...new TextEncoder().encode('<root/>')]), true);
    await reply(socket, CodePoint.ENDQRYRM);
  } finally { socket.end(); }
}

async function serveZeroLengthBooleanProjection(socket) {
  const reader = new SocketReader(socket);
  try {
    const first = await readChain(reader);
    assert.deepEqual(first.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
    await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
    const second = await readChain(reader);
    assert.deepEqual(second.map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
    await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
    await reply(socket, CodePoint.ACCRDBRM);
    const setup = await readChain(reader);
    assert.deepEqual(setup.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
    await reply(socket, CodePoint.SQLCARD, sqlCard());

    const query = await readChain(reader);
    assert.deepEqual(query.map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
    // Nullable BOOLEAN with a zero-length parameter string: a degenerate but
    // decodable field. The decoder must report false, never true from undefined.
    await reply(socket, CodePoint.QRYDSC, new Uint8Array([6, 0x76, 0xd0, 0xbf, 0x00, 0x00]), true);
    const queryId = new Uint8Array(8);
    new DataView(queryId.buffer).setBigUint64(0, 1n, false);
    await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, queryId), true);
    // 0xff 0x00 = row carries no SQLCA; 0x00 = the boolean is not NULL.
    await reply(socket, CodePoint.QRYDTA, Uint8Array.of(0xff, 0x00, 0x00), true);
    await reply(socket, CodePoint.ENDQRYRM);
  } finally { socket.end(); }
}

async function serveUnsupportedFieldTypeProjection(socket) {
  const reader = new SocketReader(socket);
  try {
    const first = await readChain(reader);
    assert.deepEqual(first.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
    await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
    const second = await readChain(reader);
    assert.deepEqual(second.map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
    await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
    await reply(socket, CodePoint.ACCRDBRM);
    const setup = await readChain(reader);
    assert.deepEqual(setup.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
    await reply(socket, CodePoint.SQLCARD, sqlCard());

    const query = await readChain(reader);
    assert.deepEqual(query.map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
    // A field type this decoder does not implement must fail as an explicit
    // unsupported-feature error, never as a generic Error.
    await reply(socket, CodePoint.QRYDSC, new Uint8Array([6, 0x76, 0xd0, 0x7f, 0x00, 0x00]), true);
    const queryId = new Uint8Array(8);
    new DataView(queryId.buffer).setBigUint64(0, 1n, false);
    await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, queryId), true);
    await reply(socket, CodePoint.QRYDTA, Uint8Array.of(0xff, 0x00, 0x00), true);
    await reply(socket, CodePoint.ENDQRYRM);
  } finally { socket.end(); }
}

async function serveProgressiveLobQuery(socket, returnNoProgress = false) {
  const reader = new SocketReader(socket);
  try {
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
    await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
    await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
    await reply(socket, CodePoint.ACCRDBRM);
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
    await reply(socket, CodePoint.SQLCARD, sqlCard());

    const query = await readChain(reader);
    assert.deepEqual(query.map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
    const openParams = ddm.parseDdmParameters(query[2].payload);
    assert.deepEqual([...openParams.get(CodePoint.DYNDTAFMT)], [0xf1]);
    await reply(socket, CodePoint.QRYDSC, Uint8Array.of(6, 0x76, 0xd0, 0xc9, 0x80, 9), true);
    await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, new Uint8Array(8)));

    const expected = progressiveLobExpectedBytes();
    const row = new Uint8Array(20);
    row.set([0xff, 0, 0, 3]);
    const view = new DataView(row.buffer);
    view.setBigUint64(4, BigInt(returnNoProgress ? 1 : expected.length), false);
    view.setBigUint64(12, 0xffff_ffff_e0b3_4079n, false);
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.CNTQRY]);
    await reply(socket, CodePoint.QRYDTA, row);

    if (returnNoProgress) {
      assert.equal((await readChain(reader))[0].codePoint, CodePoint.GETNXTCHK);
      await reply(socket, CodePoint.EXTDTA, Uint8Array.of(0), true);
      await reply(socket, CodePoint.ENDDTARM, ddm.packUint16(CodePoint.SVRCOD, 0));
      return;
    }

    for (const [chunkIndex, part] of [expected.subarray(0, 1_048_576), expected.subarray(1_048_576)].entries()) {
      const [request] = await readChain(reader);
      assert.equal(request.codePoint, CodePoint.GETNXTCHK);
      const params = ddm.parseDdmParameters(request.payload);
      const readU64 = (codePoint) => new DataView(params.get(codePoint).buffer, params.get(codePoint).byteOffset, 8).getBigUint64(0, false);
      assert.equal(readU64(CodePoint.QRYINSID), 0n);
      assert.equal(readU64(CodePoint.GETNXTREF), 0xffff_ffff_e0b3_4079n);
      assert.equal(readU64(CodePoint.GETNXTLEN), 1_048_576n);
      const final = chunkIndex === 1;
      const end = [ddm.packUint16(CodePoint.SVRCOD, 0), ddm.packString(CodePoint.RDBNAM, 'TESTDB')];
      if (final) end.push(ddm.packBytes(CodePoint.ENDREF, Uint8Array.of(0xf1)), ddm.packBytes(CodePoint.LSTCHK, Uint8Array.of(0xf1)));
      const externalData = new Uint8Array(part.length + 1); externalData.set(part, 1);
      await reply(socket, CodePoint.EXTDTA, externalData, true);
      await reply(socket, CodePoint.ENDDTARM, joinBytes(...end));
    }

    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.CNTQRY]);
    await reply(socket, CodePoint.ENDQRYRM);
  } finally { socket.end(); }
}

async function servePublicProgressiveLobQuery(socket, type, chunks, onChunk = () => {}) {
  try {
    const reader = await serveConnectionSetup(socket);
    const query = await readChain(reader);
    assert.deepEqual(query.map(packet => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
    await reply(socket, CodePoint.QRYDSC, Uint8Array.of(6, 0x76, 0xd0, type, 0x80, 9), true);
    await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, new Uint8Array(8)));
    assert.deepEqual((await readChain(reader)).map(packet => packet.codePoint), [CodePoint.CNTQRY]);
    const totalBytes = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
    // Db2 advertises a progressive DBCLOB locator length in UTF-16 code units while
    // CLOB/BLOB advertise bytes; mirror the verified server behavior here.
    const locatorLength = [0x1c, 0x1d, 0xcc, 0xcd, 0xf8, 0xf9].includes(type) ? totalBytes / 2 : totalBytes;
    const row = new Uint8Array(20);
    row.set([0xff, 0, 0, 3]);
    new DataView(row.buffer).setBigUint64(4, BigInt(locatorLength), false);
    new DataView(row.buffer).setBigUint64(12, 0x1122_3344_5566_7788n, false);
    await reply(socket, CodePoint.QRYDTA, row);
    for (let index = 0; index < chunks.length; index++) {
      const [request] = await readChain(reader);
      assert.equal(request.codePoint, CodePoint.GETNXTCHK);
      const parameters = ddm.parseDdmParameters(request.payload);
      assert.equal(new DataView(parameters.get(CodePoint.GETNXTLEN).buffer, parameters.get(CodePoint.GETNXTLEN).byteOffset, 8).getBigUint64(0, false), 65_536n);
      onChunk();
      const final = index === chunks.length - 1;
      const end = [ddm.packUint16(CodePoint.SVRCOD, 0)];
      if (final) end.push(ddm.packBytes(CodePoint.ENDREF, Uint8Array.of(0xf1)), ddm.packBytes(CodePoint.LSTCHK, Uint8Array.of(0xf1)));
      await reply(socket, CodePoint.EXTDTA, Uint8Array.from([0, ...chunks[index]]), true);
      await reply(socket, CodePoint.ENDDTARM, joinBytes(...end));
    }
    assert.deepEqual((await readChain(reader)).map(packet => packet.codePoint), [CodePoint.CNTQRY]);
    await reply(socket, CodePoint.ENDQRYRM);
  } finally { socket.end(); }
}

async function serveProgressiveLobTransportDisconnect(socket) {
  const reader = await serveConnectionSetup(socket);
  const query = await readChain(reader);
  assert.deepEqual(query.map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
  await reply(socket, CodePoint.QRYDSC, Uint8Array.of(6, 0x76, 0xd0, 0xc9, 0x80, 9), true);
  await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, new Uint8Array(8)));

  assert.equal((await readChain(reader))[0].codePoint, CodePoint.CNTQRY);
  await reply(socket, CodePoint.QRYDTA, Uint8Array.of(0xff, 0, 0xff)); // First row: typed NULL BLOB.
  assert.equal((await readChain(reader))[0].codePoint, CodePoint.CNTQRY);

  const row = new Uint8Array(20);
  row.set([0xff, 0, 0, 3]);
  new DataView(row.buffer).setBigUint64(4, 300_000n, false);
  new DataView(row.buffer).setBigUint64(12, 0x1122_3344_5566_7788n, false);
  await reply(socket, CodePoint.QRYDTA, row);
  const request = await readChain(reader);
  assert.equal(request[0].codePoint, CodePoint.GETNXTCHK);
  // Model a peer reset after the locator has been returned but before its first chunk arrives.
  socket.destroy();
}

async function serveEarlyExitProgressiveLobQuery(socket, { locatorInSameBlock = false, onLocatorRequested = () => {} } = {}) {
  const reader = new SocketReader(socket);
  try {
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
    await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
    await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
    await reply(socket, CodePoint.ACCRDBRM);
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
    await reply(socket, CodePoint.SQLCARD, sqlCard());

    const query = await readChain(reader);
    assert.deepEqual(query.map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
    const descriptor = Uint8Array.of(6, 0x76, 0xd0, 0x02, 0, 4, 6, 0x76, 0xd0, 0xc9, 0x80, 9);
    await reply(socket, CodePoint.QRYDSC, descriptor, true);
    await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, new Uint8Array(8)));
    assert.equal((await readChain(reader))[0].codePoint, CodePoint.CNTQRY);
    const ordinaryRow = Uint8Array.of(0xff, 0, 42, 0, 0, 0, 0xff);
    if (!locatorInSameBlock) {
      await reply(socket, CodePoint.QRYDTA, ordinaryRow);
      assert.equal((await readChain(reader))[0].codePoint, CodePoint.CNTQRY);
    }
    const locatorRow = new Uint8Array(24);
    locatorRow.set([0xff, 0, 43, 0, 0, 0, 0, 3]);
    const locatorView = new DataView(locatorRow.buffer);
    locatorView.setBigUint64(8, 1n, false);
    locatorView.setBigUint64(16, 0x1122_3344_5566_7788n, false);
    await reply(socket, CodePoint.QRYDTA, locatorInSameBlock
      ? Uint8Array.from([...ordinaryRow, ...locatorRow])
      : locatorRow);
    assert.equal((await readChain(reader))[0].codePoint, CodePoint.GETNXTCHK);
    onLocatorRequested();
    await reply(socket, CodePoint.EXTDTA, Uint8Array.of(0, 0xaa), true);
    await reply(socket, CodePoint.ENDDTARM, joinBytes(
      ddm.packUint16(CodePoint.SVRCOD, 0),
      ddm.packBytes(CodePoint.ENDREF, Uint8Array.of(0xf1)),
      ddm.packBytes(CodePoint.LSTCHK, Uint8Array.of(0xf1)),
    ));

    assert.equal((await readChain(reader))[0].codePoint, CodePoint.CNTQRY);
    await reply(socket, CodePoint.ENDQRYRM);

    const reuse = await readChain(reader);
    assert.deepEqual(reuse.map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
    await reply(socket, CodePoint.QRYDSC, Uint8Array.of(6, 0x76, 0xd0, 0x02, 0, 4), true);
    await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, new Uint8Array(8)));
    assert.equal((await readChain(reader))[0].codePoint, CodePoint.CNTQRY);
    await reply(socket, CodePoint.QRYDTA, Uint8Array.of(0xff, 0, 29, 0, 0, 0), true);
    await reply(socket, CodePoint.ENDQRYRM);
  } finally { socket.end(); }
}

function progressiveLobExpectedBytes() {
  return Uint8Array.from({ length: 1_048_581 }, (_, index) => index % 251);
}

function joinBytes(...parts) {
  const output = new Uint8Array(parts.reduce((length, part) => length + part.length, 0));
  let offset = 0;
  for (const part of parts) { output.set(part, offset); offset += part.length; }
  return output;
}

async function serveMockDb2(socket) {
  const reader = new SocketReader(socket);
  try {
    const first = await readChain(reader);
    assert.deepEqual(first.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
    const mechanism = ddm.packUint16(CodePoint.SECMEC, 3);
    await reply(socket, CodePoint.ACCSECRD, mechanism);

    const second = await readChain(reader);
    assert.deepEqual(second.map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
    const secchk = ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0));
    await reply(socket, CodePoint.SECCHKRM, secchk, true);
    await reply(socket, CodePoint.ACCRDBRM);

    const setup = await readChain(reader);
    assert.deepEqual(setup.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
    await reply(socket, CodePoint.SQLCARD, sqlCard());

    const ping = await readChain(reader);
    assert.deepEqual(ping.map((packet) => packet.codePoint), [CodePoint.RDBCMM]);
    await reply(socket, CodePoint.SQLCARD, sqlCard());

    const exec = await readChain(reader);
    assert.deepEqual(exec.map((packet) => packet.codePoint), [CodePoint.EXCSQLIMM, CodePoint.SQLSTT, CodePoint.RDBCMM]);
    await reply(socket, CodePoint.SQLCARD, sqlCard(0, '00000', 1));

    const query = await readChain(reader);
    assert.deepEqual(query.map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
    await reply(socket, CodePoint.QRYDSC, Uint8Array.of(6, 0x76, 0xd0, 0x02, 0, 4), true);
    const queryId = new Uint8Array(8); new DataView(queryId.buffer).setBigUint64(0, 1n, false);
    await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, queryId), true);
    const warningSqlca = [0x00, 0, 0, 0, 0, ...new TextEncoder().encode('01003'), ...new TextEncoder().encode('SQLRI6B1'), 0x00,
      ...new Uint8Array(24), ...new TextEncoder().encode('W W        '),
      0x00, 0x12, ...new TextEncoder().encode('TESTDB            '), 0x00, 0x00, 0x00, 0x00, 0xff, 0x00, 42, 0, 0, 0];
    await reply(socket, CodePoint.QRYDTA, Uint8Array.from(warningSqlca), true);
    await reply(socket, CodePoint.ENDQRYRM);

    const prepare = await readChain(reader);
    assert.deepEqual(prepare.map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.DSCSQLSTT]);
    const parameterDard = Uint8Array.of(0xff, 1, 1, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 241, 1, 0, 0);
    await reply(socket, CodePoint.SQLDARD, parameterDard);

    const parameterQuery = await readChain(reader);
    assert.deepEqual(parameterQuery.map((packet) => packet.codePoint), [CodePoint.OPNQRY, CodePoint.SQLDTA]);
    assert.deepEqual([...parameterQuery[1].payload], [
      0x00,0x10,0x00,0x10,0x06,0x76,0xd0,0x03,0x00,0x04,0x06,0x71,0xe4,0xd0,0x00,0x01,
      0x00,0x0a,0x14,0x7a,0x00,0x00,0x2a,0x00,0x00,0x00,
    ]);
    await reply(socket, CodePoint.QRYDSC, Uint8Array.of(6, 0x76, 0xd0, 0x02, 0, 4), true);
    await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, queryId), true);
    await reply(socket, CodePoint.QRYDTA, Uint8Array.of(0xff, 0, 42, 0, 0, 0), true);
    await reply(socket, CodePoint.ENDQRYRM);
    const commit = await readChain(reader);
    assert.deepEqual(commit.map((packet) => packet.codePoint), [CodePoint.RDBCMM]);
    await reply(socket, CodePoint.SQLCARD, sqlCard());

    const errorQuery = await readChain(reader);
    assert.deepEqual(errorQuery.map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
    await reply(socket, CodePoint.SQLERRRM, ddm.packBytes(CodePoint.SRVDGN, new TextEncoder().encode('SQLERRP:SQLRI2D0 SQLCODE:-952')));

    const postErrorPing = await readChain(reader);
    assert.deepEqual(postErrorPing.map((packet) => packet.codePoint), [CodePoint.RDBCMM]);
    await reply(socket, CodePoint.SQLCARD, sqlCard());

  } finally { socket.end(); }
}

test('failed transaction commit and rollback invalidate the connection', async () => {
  for (const [codePoint, operation] of [[CodePoint.RDBCMM, 'commit'], [CodePoint.RDBRLLBCK, 'rollback']]) {
    let serverTask;
    const server = net.createServer((socket) => {
      serverTask = serveTransactionControlError(socket, codePoint);
      serverTask.catch(() => socket.destroy());
    });
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const connection = await Db2Connection.connect({
      host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
      sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 2000, blockSize: 65535,
      securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
      clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
    });
    const transaction = await connection.beginTransaction();
    await assert.rejects(transaction[operation](), /SQLCODE=-911/);
    assert.equal(connection.isClosed, true);
    await connection.close();
    await serverTask;
    await new Promise((resolve) => server.close(() => resolve()));
  }
});

test('transaction-control transport failure invalidates the connection', async () => {
  for (const [codePoint, operation] of [[CodePoint.RDBCMM, 'commit'], [CodePoint.RDBRLLBCK, 'rollback']]) {
    let serverTask;
    const server = net.createServer((socket) => {
      serverTask = serveTransactionControlError(socket, codePoint, true);
      serverTask.catch(() => socket.destroy());
    });
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const connection = await Db2Connection.connect({
      host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
      sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 2000, blockSize: 65535,
      securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
      clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
    });
    const transaction = await connection.beginTransaction();
    await assert.rejects(transaction[operation]());
    assert.equal(connection.isClosed, true);
    await connection.close();
    await serverTask;
    await new Promise((resolve) => server.close(() => resolve()));
  }
});

test('aborting before transaction control keeps the scope available for rollback', async () => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = serveTransactionControlError(socket, CodePoint.RDBRLLBCK, false, true);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 2000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });
  const transaction = await connection.beginTransaction();
  const cancellation = new AbortController();
  cancellation.abort();

  await assert.rejects(transaction.commit({ signal: cancellation.signal }), /aborted/i);
  assert.equal(connection.isAutoCommit, false);
  await transaction.rollback();
  assert.equal(connection.isAutoCommit, true);
  await connection.close();
  await serverTask;
  await new Promise((resolve) => server.close(() => resolve()));
});

test('resetForPool with an active transaction discards the session', async () => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = serveAbandonedReset(socket);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 2000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });
  await connection.beginTransaction();
  await connection.resetForPool();
  assert.equal(connection.isClosed, true, 'resetForPool with an unfinished transaction must discard the session');
  await connection.close();
  await serverTask;
  await new Promise((resolve) => server.close(() => resolve()));
});

test('resetForPool reports a failed abandoned-transaction rollback after discarding the session', async () => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = serveAbandonedReset(socket, true);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 2000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });
  await connection.beginTransaction();
  await assert.rejects(connection.resetForPool(), /-911/);
  assert.equal(connection.isClosed, true, 'failed rollback must discard the session');
  await connection.close();
  await serverTask;
  await new Promise((resolve) => server.close(() => resolve()));
});

test('resetForPool discards the session when register restore fails', async () => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      const reader = await serveConnectionSetup(socket);
      // Clean-path reset detects CURRENT SCHEMA from the server first.
      assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
      await reply(socket, CodePoint.SQLCARD, sqlCard(-911, '40001'));
      await new Promise((resolve) => socket.once('close', resolve));
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  // White-box pool baseline: the pool captures this before the first lease, so
  // reset compares live server state instead of guessing which SQL ran.
  connection.baseSchema = 'DB2INST1';
  connection.baseIsolation = 'CS';
  connection.poolBaselineCaptured = true;
  await assert.rejects(connection.resetForPool(), /-911/);
  assert.equal(connection.isClosed, true, 'a failed register restore must discard the session');
  await connection.close();
  await serverTask;
  await new Promise((resolve) => server.close(() => resolve()));
});

async function servePoolClientRegisterReset(socket, state) {
  const reader = await serveConnectionSetup(socket);
  const applySet = (sql) => {
    const wlm = /CALL\s+SYSPROC\.WLM_SET_CLIENT_INFO\('((?:''|[^'])*)',\s*'((?:''|[^'])*)',\s*'((?:''|[^'])*)',\s*'((?:''|[^'])*)',\s*NULL\)/i.exec(sql);
    if (wlm) {
      if (state.failClientRestore) return false;
      const [userId, workstationName, applicationName, accounting] = wlm.slice(1, 5).map(value => value.replaceAll("''", "'"));
      Object.assign(state, { userId, workstationName, applicationName, accounting });
    }
    const schema = /SET CURRENT SCHEMA\s+"((?:""|[^"])*)"/i.exec(sql);
    if (schema) state.schema = schema[1].replaceAll('""', '"');
    const isolation = /SET CURRENT ISOLATION\s+(UR|CS|RS|RR|RESET)/i.exec(sql);
    if (isolation) state.isolation = isolation[1] === 'RESET' ? 'CS' : isolation[1];
    const clientRegister = /SET CLIENT (APPLNAME|USERID|WRKSTNNAME) '((?:''|[^'])*)'/i.exec(sql);
    if (clientRegister) {
      if (state.failClientRestore) return false;
      const key = { APPLNAME: 'applicationName', USERID: 'userId', WRKSTNNAME: 'workstationName' }[clientRegister[1].toUpperCase()];
      state[key] = clientRegister[2].replaceAll("''", "'");
    }
    const userChange = /SET SESSION(?:\s+AUTHORIZATION|_USER)\s*=\s*"((?:""|[^"])*)"/i.exec(sql);
    if (userChange) {
      const target = userChange[1].replaceAll('""', '"');
      if (state.failSessionUserRestore && target === state.sessionUserBaseline) return false;
      state.sessionUser = target;
    }
    return true;
  };
  while (true) {
    let packets;
    try { packets = await readChain(reader); }
    catch (error) { if (socket.destroyed || socket.readableEnded) return; throw error; }
    const sqlPacket = packets.find((packet) => packet.codePoint === CodePoint.SQLSTT);
    let sql = '';
    if (sqlPacket && sqlPacket.payload.length >= 5) {
      const length = new DataView(sqlPacket.payload.buffer, sqlPacket.payload.byteOffset, sqlPacket.payload.byteLength).getUint32(1, false);
      sql = new TextDecoder().decode(sqlPacket.payload.subarray(5, 5 + length)).trim();
    }
    if (packets.some((packet) => packet.codePoint === CodePoint.RDBCMM) && !sqlPacket) {
      await reply(socket, CodePoint.SQLCARD, sqlCard());
      await new Promise((resolve) => socket.once('close', resolve));
      return;
    }
    if (packets.some((packet) => packet.codePoint === CodePoint.OPNQRY)) {
      let values;
      if (/CURRENT SCHEMA/i.test(sql) && /CURRENT ISOLATION/i.test(sql) && /SESSION_USER/i.test(sql) && /CURRENT CLIENT_APPLNAME/i.test(sql)) {
        state.poolStateProbeCount = (state.poolStateProbeCount ?? 0) + 1;
        values = [state.sessionUser, state.schema, state.isolation, state.applicationName, state.workstationName, state.userId, state.accounting];
      } else if (/CURRENT SCHEMA/i.test(sql) && /CURRENT ISOLATION/i.test(sql) && /SESSION_USER/i.test(sql)) values = [state.schema, state.isolation, state.sessionUser];
      else if (/CURRENT SCHEMA/i.test(sql)) values = [state.schema];
      else if (/CURRENT ISOLATION/i.test(sql)) values = [state.isolation];
      else if (/SESSION_USER/i.test(sql)) values = [state.sessionUser];
      else if (/CURRENT CLIENT_APPLNAME/i.test(sql)) values = [state.applicationName, state.userId, state.workstationName, state.accounting];
      else throw new Error(`unexpected mock pooled baseline query: ${sql}`);
      const descriptor = Buffer.from([3 + values.length * 3, 0x76, 0xd0, ...values.flatMap(() => [0x33, 0, 128])]);
      await reply(socket, CodePoint.QRYDSC, descriptor, true);
      await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, new Uint8Array(8)), true);
      let row = Buffer.from([0xff, 0]);
      for (const value of values) {
        const bytes = Buffer.from(value, 'utf8');
        const length = Buffer.alloc(2); length.writeUInt16BE(bytes.length);
        row = Buffer.concat([row, Buffer.from([0]), length, bytes]);
      }
      await reply(socket, CodePoint.QRYDTA, row, true);
      await reply(socket, CodePoint.ENDQRYRM);
      continue;
    }
    const success = applySet(sql);
    await reply(socket, CodePoint.SQLCARD, success ? sqlCard() : sqlCard(-551, '42501'));
  }
}

test('pool reset probes and restores raw CURRENT CLIENT registers despite a matching local cache', async () => {
  const serverState = { schema: 'DB2INST1', isolation: 'CS', sessionUser: 'DB2INST1', sessionUserBaseline: 'DB2INST1', applicationName: '', workstationName: 'baseline_host', userId: '', accounting: '' };
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = servePoolClientRegisterReset(socket, serverState);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect({ ...mockConfig(server.address().port), user: 'DB2INST1', clientWrkstnName: 'baseline_host' });
  const originalExecuteLocked = connection.executeLocked.bind(connection);
  const originalExecutePreparedLocked = connection.executePreparedLocked.bind(connection);
  const setClientInfo = ([userId, workstationName, applicationName, accounting]) => {
    Object.assign(serverState, { applicationName, userId, workstationName, accounting });
  };
  connection.executeLocked = async (sql, params) => {
    const match = /WLM_SET_CLIENT_INFO\('((?:''|[^'])*)', '((?:''|[^'])*)', '((?:''|[^'])*)', '((?:''|[^'])*)', NULL\)/i.exec(sql);
    if (!match) return originalExecuteLocked(sql, params);
    setClientInfo(match.slice(1, 5).map((value) => value.replaceAll("''", "'")));
    return 0;
  };
  connection.executePreparedLocked = async (statement, params, ...rest) => {
    if (/WLM_SET_CLIENT_INFO/i.test(statement.sql)) { setClientInfo(params); return 0; }
    return originalExecutePreparedLocked(statement, params, ...rest);
  };
  try {
    await connection.capturePoolBaseline();
    const baseline = await connection.query("VALUES (COALESCE(CURRENT CLIENT_APPLNAME, ''), COALESCE(CURRENT CLIENT_USERID, ''), COALESCE(CURRENT CLIENT_WRKSTNNAME, ''), COALESCE(CURRENT CLIENT_ACCTNG, ''))");
    assert.deepEqual(baseline.rows[0], ['', '', 'baseline_host', '']);
    const baselineState = { ...serverState };

    const parameterizedWlmCall = { sql: 'CALL SYSPROC.WLM_SET_CLIENT_INFO(?, ?, ?, ?, NULL)', parameterColumns: [] };
    connection.preparedSql = parameterizedWlmCall.sql;
    await connection.executeStatement(parameterizedWlmCall, [
      'raw_pool_user', 'raw_pool_host', 'raw_pool_app', 'raw_pool_accounting',
    ]);
    assert.deepEqual((await connection.query("VALUES (COALESCE(CURRENT CLIENT_APPLNAME, ''), COALESCE(CURRENT CLIENT_USERID, ''), COALESCE(CURRENT CLIENT_WRKSTNNAME, ''), COALESCE(CURRENT CLIENT_ACCTNG, ''))")).rows[0], [
      'raw_pool_app', 'raw_pool_user', 'raw_pool_host', 'raw_pool_accounting',
    ]);

    // Simulate the borrower leaving and the pool returning the same physical connection.
    await connection.resetForPool();
    assert.deepEqual((await connection.query("VALUES (COALESCE(CURRENT CLIENT_APPLNAME, ''), COALESCE(CURRENT CLIENT_USERID, ''), COALESCE(CURRENT CLIENT_WRKSTNNAME, ''), COALESCE(CURRENT CLIENT_ACCTNG, ''))")).rows[0], baseline.rows[0]);
    baselineState.poolStateProbeCount = 2;
    assert.deepEqual(serverState, baselineState);
  } finally {
    await connection.close();
    await serverTask;
    await new Promise((resolve) => server.close(() => resolve()));
  }
});

test('pool reset probes once and restores all server state changed behind an innocuous CALL', async () => {
  const serverState = { schema: 'DB2INST1', isolation: 'CS', sessionUser: 'DB2INST1', sessionUserBaseline: 'DB2INST1', applicationName: '', workstationName: 'baseline_host', userId: '', accounting: '', poolStateProbeCount: 0 };
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = servePoolClientRegisterReset(socket, serverState);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect({ ...mockConfig(server.address().port), user: 'DB2INST1', clientWrkstnName: 'baseline_host' });
  const originalPrepareLocked = connection.prepareLocked.bind(connection);
  const originalExecuteLocked = connection.executeLocked.bind(connection);
  const originalExecuteProcedurePreparedLocked = connection.executeProcedurePreparedLocked.bind(connection);
  connection.prepareLocked = async (sql) => sql === 'CALL APP.HIDDEN_STATE_CHANGE()'
    ? { sql, outputColumns: [], parameterColumns: [] }
    : originalPrepareLocked(sql);
  connection.executeProcedurePreparedLocked = async (statement, params, ...rest) => {
    if (statement.sql === 'CALL APP.HIDDEN_STATE_CHANGE()') {
      Object.assign(serverState, {
        schema: 'BORROWER_SCHEMA', isolation: 'RR', sessionUser: 'BORROWER',
        applicationName: 'hidden_app', userId: 'hidden_user', workstationName: 'hidden_host', accounting: 'hidden_accounting',
      });
      return { queryResult: { columns: [], rows: [] }, outputParameters: [], rowsAffected: 0 };
    }
    return originalExecuteProcedurePreparedLocked(statement, params, ...rest);
  };
  connection.executeLocked = async (sql, params) => {
    const accounting = /CALL SYSPROC\.WLM_SET_CLIENT_INFO\('', '', '', '((?:''|[^'])*)', NULL\)/i.exec(sql);
    if (accounting) {
      if (serverState.failClientRestore) throw new Db2ServerError(-551, '42501', 'mock client register restore denied');
      serverState.accounting = accounting[1].replaceAll("''", "'");
      return 0;
    }
    return originalExecuteLocked(sql, params);
  };
  try {
    await connection.capturePoolBaseline();
    assert.equal(serverState.poolStateProbeCount, 1, 'pool creation captures state in one combined server query');
    const baseline = {
      schema: serverState.schema, isolation: serverState.isolation, sessionUser: serverState.sessionUser,
      applicationName: serverState.applicationName, workstationName: serverState.workstationName,
      userId: serverState.userId, accounting: serverState.accounting,
    };
    await connection.execute('CALL APP.HIDDEN_STATE_CHANGE()');
    assert.deepEqual({
      schema: serverState.schema, isolation: serverState.isolation, sessionUser: serverState.sessionUser,
      applicationName: serverState.applicationName, workstationName: serverState.workstationName,
      userId: serverState.userId, accounting: serverState.accounting,
    }, {
      schema: 'BORROWER_SCHEMA', isolation: 'RR', sessionUser: 'BORROWER', applicationName: 'hidden_app',
      workstationName: 'hidden_host', userId: 'hidden_user', accounting: 'hidden_accounting',
    });

    await connection.resetForPool();
    assert.equal(serverState.poolStateProbeCount, 2, 'each clean lease return uses one combined state probe');
    assert.equal(connection.isClosed, false, 'successful restoration keeps the same transport');
    assert.deepEqual({
      schema: serverState.schema, isolation: serverState.isolation, sessionUser: serverState.sessionUser,
      applicationName: serverState.applicationName, workstationName: serverState.workstationName,
      userId: serverState.userId, accounting: serverState.accounting,
    }, baseline);
    assert.deepEqual((await connection.query('VALUES (CURRENT SCHEMA, CURRENT ISOLATION, SESSION_USER)')).rows,
      [[baseline.schema, baseline.isolation, baseline.sessionUser]]);
  } finally {
    await connection.close();
    await serverTask;
    await new Promise((resolve) => server.close(() => resolve()));
  }
});

test('pool reset probes raw SESSION_USER and restores the same pooled transport', async () => {
  const serverState = { schema: 'DB2INST1', isolation: 'CS', sessionUser: 'DB2INST1', sessionUserBaseline: 'DB2INST1', applicationName: '', workstationName: 'baseline_host', userId: '', accounting: '' };
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = servePoolClientRegisterReset(socket, serverState);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect({ ...mockConfig(server.address().port), user: 'DB2INST1', clientWrkstnName: 'baseline_host' });
  const originalExecutePreparedLocked = connection.executePreparedLocked.bind(connection);
  connection.executePreparedLocked = async (statement, params, ...rest) => {
    if (/SET SESSION AUTHORIZATION/i.test(statement.sql)) { serverState.sessionUser = params[0]; return 0; }
    return originalExecutePreparedLocked(statement, params, ...rest);
  };
  try {
    await connection.capturePoolBaseline();
    const switchStatement = { sql: 'SET SESSION AUTHORIZATION = ?', parameterColumns: [] };
    connection.preparedSql = switchStatement.sql;
    await connection.executeStatement(switchStatement, ['BORROWER']);
    assert.deepEqual((await connection.query('VALUES SESSION_USER')).rows, [['BORROWER']]);

    await connection.resetForPool();
    assert.equal(connection.isClosed, false, 'a permitted identity reset must retain its session');
    assert.deepEqual((await connection.query('VALUES SESSION_USER')).rows, [['DB2INST1']]);
    assert.equal(serverState.sessionUser, 'DB2INST1');
  } finally {
    await connection.close();
    await serverTask;
    await new Promise((resolve) => server.close(() => resolve()));
  }
});

test('pool reset discards a session when raw SESSION_USER restoration is denied', async () => {
  const serverState = { schema: 'DB2INST1', isolation: 'CS', sessionUser: 'DB2INST1', sessionUserBaseline: 'DB2INST1', failSessionUserRestore: true, applicationName: '', workstationName: 'baseline_host', userId: '', accounting: '' };
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = servePoolClientRegisterReset(socket, serverState);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect({ ...mockConfig(server.address().port), user: 'DB2INST1', clientWrkstnName: 'baseline_host' });
  try {
    await connection.capturePoolBaseline();
    await connection.execute('SET SESSION_USER = "BORROWER"');
    assert.deepEqual((await connection.query('VALUES SESSION_USER')).rows, [['BORROWER']]);
    await assert.rejects(connection.resetForPool(), /SQLCODE=-551/);
    assert.equal(connection.isClosed, true, 'a denied restore must discard the uncertain session');
    assert.equal(serverState.sessionUser, 'BORROWER');
  } finally {
    await connection.close();
    await serverTask;
    await new Promise((resolve) => server.close(() => resolve()));
  }
});

test('pool reset discards a session when client-register restoration is denied', async () => {
  const serverState = { schema: 'DB2INST1', isolation: 'CS', sessionUser: 'DB2INST1', sessionUserBaseline: 'DB2INST1', failClientRestore: false, applicationName: '', workstationName: 'baseline_host', userId: '', accounting: '' };
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = servePoolClientRegisterReset(socket, serverState);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect({ ...mockConfig(server.address().port), user: 'DB2INST1', clientWrkstnName: 'baseline_host' });
  try {
    await connection.capturePoolBaseline();
    await connection.execute("SET CLIENT USERID 'borrower'");
    serverState.failClientRestore = true;
    await assert.rejects(connection.resetForPool(), /SQLCODE=-551/);
    assert.equal(connection.isClosed, true, 'a denied client-register restore must discard the session');
  } finally {
    await connection.close();
    await serverTask;
    await new Promise((resolve) => server.close(() => resolve()));
  }
});

test('AbortSignal interrupts active COMMIT and ROLLBACK through INTRDBRQS', async () => {
  for (const expectedControl of [CodePoint.RDBCMM, CodePoint.RDBRLLBCK]) {
    const interruptToken = Uint8Array.of(0x04, 0x1c, 0xa7, 0x32);
    let signalControlStarted;
    const controlStarted = new Promise((resolve) => { signalControlStarted = resolve; });
    let mainTask;
    let interruptTask;
    let mainSocket;
    const server = net.createServer((socket) => {
      if (!mainTask) {
        mainSocket = socket;
        mainTask = serveInterruptedTransactionControl(socket, expectedControl, interruptToken, signalControlStarted);
        mainTask.catch(() => socket.destroy());
      } else {
        interruptTask = serveInterruptRequest(socket, mainSocket, interruptToken);
        interruptTask.catch(() => socket.destroy());
      }
    });
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const connection = await Db2Connection.connect({
      host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
      sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 3000, blockSize: 65535,
      securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
      clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
    });
    const transaction = await connection.beginTransaction();
    const cancellation = new AbortController();
    const operation = expectedControl === CodePoint.RDBCMM
      ? transaction.commit({ signal: cancellation.signal })
      : transaction.rollback({ signal: cancellation.signal });

    await controlStarted;
    cancellation.abort();
    await assert.rejects(operation, /aborted/i);
    assert.equal(connection.isClosed, true);
    await connection.close();
    await Promise.all([mainTask, interruptTask]);
    await new Promise((resolve) => server.close(() => resolve()));
  }
});

test('procedure cursor fetches again after an exact full block', async () => {
  const rowCount = 169;
  const expectedPayloadLength = 1014; // 1024-byte DSS block minus its 10-byte headers.
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = serveExactFullBlockProcedure(socket, rowCount, expectedPayloadLength);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 3000, blockSize: 1024,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });

  const result = await connection.query('CALL TESTPROC()');
  assert.deepEqual(result.resultSets[0].rows.map(([value]) => value), Array.from({ length: rowCount }, (_, index) => index + 1));
  await connection.close();
  await serverTask;
  await new Promise((resolve) => server.close(() => resolve()));
});

test('streaming procedure reader closes an early cursor before reusing the session', async () => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = serveExactFullBlockProcedure(socket, 169, 1014, true);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 3000, blockSize: 1024,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });
  const reader = await connection.executeProcedureStream('CALL TESTPROC()');
  assert.equal(reader.resultSetCount, 1);
  assert.equal(reader.columns[0].name, 'VALUE');
  assert.throws(() => reader.outputParameters, /only after finish/);
  const firstRows = [];
  for await (const row of reader.rows()) { firstRows.push(row); break; }
  assert.deepEqual(firstRows, [[1]]);
  await reader.finish();
  assert.deepEqual(reader.outputParameters, []);
  assert.deepEqual((await connection.query('VALUES 29')).rows, [[29]]);
  await connection.close();
  await serverTask;
  await new Promise((resolve) => server.close(() => resolve()));
});

test('streaming procedure reader handles split rows and rejects stale QRYDTA fragments', async () => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = serveStreamingMultiCursorProcedure(socket);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 3000, blockSize: 1024,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });
  try {
    const reader = await connection.executeProcedureStream('CALL MULTIPROC()');
    assert.equal(reader.resultSetCount, 4);
    assert.equal(reader.resultSetIndex, 0);
    assert.deepEqual(await Array.fromAsync(reader.rows()), []);
    assert.equal(await reader.nextResultSet(), true);
    assert.equal(reader.resultSetIndex, 1);
    assert.deepEqual(await Array.fromAsync(reader.rows()), [[11]], 'a complete SQLCODE +100 row SQLCA closes this cursor without another fetch');
    assert.equal(await reader.nextResultSet(), true);
    assert.equal(reader.resultSetIndex, 2);
    assert.deepEqual(await Array.fromAsync(reader.rows()), [[22]]);
    assert.equal(await reader.nextResultSet(), true);
    assert.equal(reader.resultSetIndex, 3);
    await assert.rejects(Array.fromAsync(reader.rows()), /repeated a non-progressing procedure QRYDTA fragment/);
    assert.equal(connection.isClosed, true);
  } finally {
    await connection.close();
    await serverTask;
    await new Promise((resolve) => server.close(() => resolve()));
  }
});

test('AbortSignal interrupts a streaming procedure cursor and discards uncertain cursor state', async () => {
  const interruptToken = Uint8Array.of(4, 28, 167, 50);
  let getNextStarted;
  const waitingForGetNext = new Promise(resolve => { getNextStarted = resolve; });
  let interruptApplied;
  const waitingForInterrupt = new Promise(resolve => { interruptApplied = resolve; });
  let mainTask;
  let interruptTask;
  let mainSocket;
  const server = net.createServer(socket => {
    if (!mainTask) {
      mainSocket = socket;
      mainTask = serveInterruptedProcedureReader(socket, interruptToken, getNextStarted, waitingForInterrupt);
      mainTask.catch(() => socket.destroy());
    } else {
      interruptTask = serveProgressiveLobInterrupt(socket, mainSocket, interruptToken, () => {}, interruptApplied);
      interruptTask.catch(() => socket.destroy());
    }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect({ ...mockConfig(server.address().port), timeoutMs: 3000 });
  try {
    const controller = new AbortController();
    const reader = await connection.executeProcedureStream('CALL CANCELPROC()', [], { signal: controller.signal });
    const iterator = reader.rows();
    const nextRow = iterator.next();
    await waitingForGetNext;
    controller.abort();
    await assert.rejects(nextRow, error => error.name === 'AbortError');
    assert.equal(connection.isClosed, true, 'a canceled CALL cursor must not leave other procedure cursors reusable');
    await Promise.all([mainTask, interruptTask]);
  } finally {
    await connection.close();
    if (mainTask) await mainTask.catch(() => {});
    if (interruptTask) await interruptTask.catch(() => {});
    await new Promise(resolve => server.close(() => resolve()));
  }
});

async function serveTransactionControlError(socket, expectedCodePoint, dropTransport = false, successfulReply = false) {
  const reader = new SocketReader(socket);
  const initial = await readChain(reader);
  assert.deepEqual(initial.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
  await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
  const auth = await readChain(reader);
  assert.deepEqual(auth.map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
  await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
  await reply(socket, CodePoint.ACCRDBRM);
  const setup = await readChain(reader);
  assert.deepEqual(setup.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
  await reply(socket, CodePoint.SQLCARD, sqlCard());
  const control = await readChain(reader);
  assert.deepEqual(control.map((packet) => packet.codePoint), [expectedCodePoint]);
  if (dropTransport) {
    socket.destroy();
    return;
  }
  const clientClosed = new Promise((resolve) => {
    const timeout = setTimeout(() => { socket.off('close', onClose); resolve(false); }, 2000);
    function onClose() { clearTimeout(timeout); resolve(true); }
    socket.once('close', onClose);
  });
  await reply(socket, CodePoint.SQLCARD, successfulReply ? sqlCard() : sqlCard(-911, '40001'));
  assert.equal(await clientClosed, true, 'disposing an invalidated connection must release its socket');
}

async function serveAbandonedReset(socket, rollbackFailure = false) {
  const reader = new SocketReader(socket);
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
  await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
  await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
  await reply(socket, CodePoint.ACCRDBRM);
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
  await reply(socket, CodePoint.SQLCARD, sqlCard());
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.RDBRLLBCK]);
  await reply(socket, CodePoint.SQLCARD, rollbackFailure ? sqlCard(-911, '40001') : sqlCard());
  const clientClosed = new Promise((resolve) => {
    const timeout = setTimeout(() => resolve(false), 2000);
    socket.once('close', () => { clearTimeout(timeout); resolve(true); });
  });
  assert.equal(await clientClosed, true, 'discarded session must release its socket');
}

async function serveInterruptedTransactionControl(socket, expectedControl, interruptToken, controlStarted) {
  const reader = new SocketReader(socket);
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
  await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
  await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
  await reply(socket, CodePoint.ACCRDBRM, ddm.packBytes(CodePoint.RDBINTTKN, interruptToken));
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
  await reply(socket, CodePoint.SQLCARD, sqlCard());
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [expectedControl]);
  const clientClosed = new Promise((resolve) => socket.once('close', resolve));
  controlStarted();
  await clientClosed;
}

async function serveInterruptRequest(socket, mainSocket, interruptToken) {
  const reader = new SocketReader(socket);
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT]);
  await reply(socket, CodePoint.EXCSATRD);
  const interrupt = await readChain(reader);
  assert.deepEqual(interrupt.map((packet) => packet.codePoint), [CodePoint.INTRDBRQS]);
  assert.deepEqual(ddm.parseDdmParameters(interrupt[0].payload).get(CodePoint.RDBINTTKN), interruptToken);
  await reply(socket, CodePoint.CMDCMPRM, ddm.packUint16(CodePoint.SVRCOD, 0));
  assert.ok(mainSocket, 'main DB2 socket should be registered by the interrupt fixture');
  await reply(mainSocket, CodePoint.SQLCARD, sqlCard(-952, '57014'));
}

test('procedure cursor fetch failure invalidates the session', async () => {
  for (const [serverError, expected, pattern] of [
    [true, Error, /SQLCODE=-911/],
    [false, Error, /no query data or end-of-query reply/],
  ]) {
    let serverTask;
    const server = net.createServer((socket) => {
      serverTask = serveProcedureCursorFetchFailure(socket, serverError);
      serverTask.catch(() => socket.destroy());
    });
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const connection = await Db2Connection.connect({
      host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
      sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 3000, blockSize: 65535,
      securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
      clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
    });

    await assert.rejects(connection.query('CALL TESTPROC()'), (error) => error instanceof expected && pattern.test(error.message));
    assert.equal(connection.isClosed, true, 'a failed procedure-cursor fetch must not leave a reusable session');
    await serverTask;
    await new Promise((resolve) => server.close(() => resolve()));
  }
});

test('procedure cursor close failure invalidates the session', async () => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = serveProcedureCursorFetchFailure(socket, true, true);
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connection = await Db2Connection.connect({
    host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
    sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 3000, blockSize: 65535,
    securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
    clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
  });

  await assert.rejects(connection.query('CALL TESTPROC()'), /SQLCODE=-911/);
  assert.equal(connection.isClosed, true, 'a failed procedure-cursor close must not leave a reusable session');
  await serverTask;
  await new Promise((resolve) => server.close(() => resolve()));
});

async function serveProcedureCursorFetchFailure(socket, serverError, closeFailure = false) {
  const reader = new SocketReader(socket);
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
  await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
  await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
  await reply(socket, CodePoint.ACCRDBRM);
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
  await reply(socket, CodePoint.SQLCARD, sqlCard());

  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.DSCSQLSTT]);
  await reply(socket, CodePoint.SQLDARD);
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSQLSTT]);

  const section = ddm.packPackageNameCsn('TESTDB', 'SYSSH200', 'SYSLVL01', 65);
  await reply(socket, CodePoint.RSLSETRM, ddm.packDdmObject(CodePoint.PKGSNLST, section), true);
  const summary = new Uint8Array(14);
  summary[0] = 1;
  await reply(socket, CodePoint.SQLRSLRD, summary, true);
  const queryId = Buffer.alloc(8);
  queryId.writeBigUInt64BE(123n);
  await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, queryId), true);
  await reply(socket, CodePoint.SQLCINRD, procedureIntegerCursorInfo(), true);
  await reply(socket, CodePoint.QRYDSC, Uint8Array.of(6, 0x76, 0xd0, 0x02, 0, 4));

  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.CNTQRY]);
  if (closeFailure) {
    await reply(socket, CodePoint.QRYDTA, Uint8Array.of(0xff, 0, 42, 0, 0, 0));
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.CLSQRY]);
  }
  await reply(socket, CodePoint.SQLCARD, serverError ? sqlCard(-911, '40001') : sqlCard());
  await new Promise((resolve) => socket.once('close', resolve));
}

async function serveExactFullBlockProcedure(socket, rowCount, expectedPayloadLength, serveReuseQuery = false) {
  const reader = new SocketReader(socket);
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
  await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
  await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
  await reply(socket, CodePoint.ACCRDBRM);
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
  await reply(socket, CodePoint.SQLCARD, sqlCard());

  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.DSCSQLSTT]);
  await reply(socket, CodePoint.SQLDARD);
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.EXCSQLSTT]);

  const section = ddm.packPackageNameCsn('TESTDB', 'SYSSH200', 'SYSLVL01', 65);
  await reply(socket, CodePoint.RSLSETRM, ddm.packDdmObject(CodePoint.PKGSNLST, section), true);
  const summary = new Uint8Array(14);
  summary[0] = 1;
  await reply(socket, CodePoint.SQLRSLRD, summary, true);
  const queryId = Buffer.alloc(8);
  queryId.writeBigUInt64BE(123n);
  await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, queryId), true);
  await reply(socket, CodePoint.SQLCINRD, procedureIntegerCursorInfo(), true);
  await reply(socket, CodePoint.QRYDSC, Uint8Array.of(6, 0x76, 0xd0, 0x02, 0, 4));

  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.CNTQRY]);
  const rowData = Buffer.alloc(expectedPayloadLength);
  for (let index = 0; index < rowCount; index++) {
    const offset = index * 6;
    rowData[offset] = 0xff;
    rowData.writeInt32LE(index + 1, offset + 2);
  }
  assert.equal(rowData.length, expectedPayloadLength);
  await reply(socket, CodePoint.QRYDTA, rowData);

  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.CNTQRY]);
  await reply(socket, CodePoint.ENDQRYRM);
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.CLSQRY]);
  await reply(socket, CodePoint.SQLCARD, sqlCard());
  assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.RDBCMM]);
  await reply(socket, CodePoint.SQLCARD, sqlCard());
  if (serveReuseQuery) {
    const reuse = await readChain(reader);
    assert.deepEqual(reuse.map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
    await reply(socket, CodePoint.QRYDSC, Uint8Array.of(6, 0x76, 0xd0, 0x02, 0, 4), true);
    const queryId = Buffer.alloc(8); queryId.writeBigUInt64BE(124n);
    await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, queryId));
    assert.deepEqual((await readChain(reader)).map((packet) => packet.codePoint), [CodePoint.CNTQRY]);
    await reply(socket, CodePoint.QRYDTA, Uint8Array.of(0xff, 0, 29, 0, 0, 0), true);
    await reply(socket, CodePoint.ENDQRYRM);
  }
  await new Promise((resolve) => socket.once('close', resolve));
}

async function serveStreamingMultiCursorProcedure(socket) {
  const reader = new SocketReader(socket);
  try {
    assert.deepEqual((await readChain(reader)).map(packet => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
    await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
    assert.deepEqual((await readChain(reader)).map(packet => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
    await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
    await reply(socket, CodePoint.ACCRDBRM);
    assert.deepEqual((await readChain(reader)).map(packet => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
    await reply(socket, CodePoint.SQLCARD, sqlCard());
    assert.deepEqual((await readChain(reader)).map(packet => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.DSCSQLSTT]);
    await reply(socket, CodePoint.SQLDARD);
    const call = await readChain(reader);
    assert.deepEqual(call.map(packet => packet.codePoint), [CodePoint.EXCSQLSTT]);
    const sections = Buffer.concat([65, 66, 67, 68].map(number => Buffer.from(ddm.packPackageNameCsn('TESTDB', 'SYSSH200', 'SYSLVL01', number))));
    await reply(socket, CodePoint.RSLSETRM, ddm.packDdmObject(CodePoint.PKGSNLST, sections), true);
    const summary = new Uint8Array(50); summary[0] = 4;
    await reply(socket, CodePoint.SQLRSLRD, summary, true);
    for (const [index, id] of [101n, 102n, 103n, 104n].entries()) {
      const queryId = Buffer.alloc(8); queryId.writeBigUInt64BE(id);
      await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, queryId), true);
      await reply(socket, CodePoint.SQLCINRD, procedureIntegerCursorInfo(), true);
      await reply(socket, CodePoint.QRYDSC, Uint8Array.of(6, 0x76, 0xd0, 0x02, 0, 4), true);
      if (index === 1) {
        // Db2 can end a CALL cursor with SQLCODE +100 and no row-data marker.
        await reply(socket, CodePoint.QRYDTA, Buffer.concat([
          Buffer.from([0xff, 0, 11, 0, 0, 0]), Buffer.from(procedureEndSqlca()),
        ]), true);
      } else if (index === 2) {
        // This row is deliberately split across the initial CALL reply and later CNTQRY.
        await reply(socket, CodePoint.QRYDTA, Uint8Array.of(0xff, 0, 22, 0), true);
      } else if (index === 3) {
        const staleTail = new Uint8Array(80);
        staleTail[0] = 0x00;
        Buffer.from('01000').copy(staleTail, 5);
        staleTail[18] = 0x00;
        staleTail[54] = 0x10;
        await reply(socket, CodePoint.QRYDTA, Buffer.concat([Buffer.from([0xff, 0, 33, 0, 0, 0]), Buffer.from(staleTail)]), false);
      }
    }
    assert.deepEqual((await readChain(reader)).map(packet => packet.codePoint), [CodePoint.CNTQRY]);
    await reply(socket, CodePoint.ENDQRYRM);
    assert.deepEqual((await readChain(reader)).map(packet => packet.codePoint), [CodePoint.CLSQRY]);
    await reply(socket, CodePoint.SQLCARD, sqlCard());
    // The +100 SQLCA already ended this cursor; do not send CNTQRY.
    assert.deepEqual((await readChain(reader)).map(packet => packet.codePoint), [CodePoint.CLSQRY]);
    await reply(socket, CodePoint.SQLCARD, sqlCard());
    assert.deepEqual((await readChain(reader)).map(packet => packet.codePoint), [CodePoint.CNTQRY]);
    // Db2 commonly terminates procedure cursors with a short final block and no ENDQRYRM.
    await reply(socket, CodePoint.QRYDTA, Uint8Array.of(0, 0), false);
    assert.deepEqual((await readChain(reader)).map(packet => packet.codePoint), [CodePoint.CLSQRY]);
    await reply(socket, CodePoint.SQLCARD, sqlCard());
    assert.deepEqual((await readChain(reader)).map(packet => packet.codePoint), [CodePoint.CNTQRY]);
    const staleTail = new Uint8Array(80);
    staleTail[0] = 0x00;
    Buffer.from('01000').copy(staleTail, 5);
    staleTail[18] = 0x00;
    staleTail[54] = 0x10;
    for (let repeat = 0; repeat < 3; repeat++) {
      await reply(socket, CodePoint.QRYDTA, staleTail, false);
      if (repeat < 2) assert.deepEqual((await readChain(reader)).map(packet => packet.codePoint), [CodePoint.CNTQRY]);
    }
    await new Promise(resolve => socket.once('close', resolve));
  } finally { socket.end(); }
}

function procedureEndSqlca() {
  const data = new Uint8Array(80);
  const view = new DataView(data.buffer);
  data[0] = 0;
  view.setInt32(1, 100, true);
  Buffer.from('02000').copy(data, 5);
  data[18] = 0; // SQLCAXGRP is present.
  view.setUint16(54, 18, false); // SQLERRMC=18, SQLERRP/SQLERRD auxiliary strings empty.
  data[78] = 0xff; // SQLDIAGGRP absent.
  data[79] = 0xff; // terminal data indicator at the exact end of the record.
  return data;
}

async function serveInterruptedProcedureReader(socket, interruptToken, getNextStarted, interruptionApplied) {
  const reader = await serveConnectionSetup(socket, interruptToken);
  assert.deepEqual((await readChain(reader)).map(packet => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.DSCSQLSTT]);
  await reply(socket, CodePoint.SQLDARD);
  assert.deepEqual((await readChain(reader)).map(packet => packet.codePoint), [CodePoint.EXCSQLSTT]);
  const section = ddm.packPackageNameCsn('TESTDB', 'SYSSH200', 'SYSLVL01', 65);
  await reply(socket, CodePoint.RSLSETRM, ddm.packDdmObject(CodePoint.PKGSNLST, section), true);
  const summary = new Uint8Array(14); summary[0] = 1;
  await reply(socket, CodePoint.SQLRSLRD, summary, true);
  const queryId = Buffer.alloc(8); queryId.writeBigUInt64BE(55n);
  await reply(socket, CodePoint.OPNQRYRM, ddm.packBytes(CodePoint.QRYINSID, queryId), true);
  await reply(socket, CodePoint.SQLCINRD, procedureIntegerCursorInfo(), true);
  await reply(socket, CodePoint.QRYDSC, Uint8Array.of(6, 0x76, 0xd0, 0x02, 0, 4));
  assert.deepEqual((await readChain(reader)).map(packet => packet.codePoint), [CodePoint.CNTQRY]);
  getNextStarted();
  await interruptionApplied;
  await new Promise(resolve => socket.once('close', resolve));
}

function procedureIntegerCursorInfo() {
  const pair = (value) => Buffer.concat([Buffer.from([0, 0, 0, value.length]), Buffer.from(value)]);
  return Buffer.concat([
    Buffer.from([0xff, 1, 0, 10, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0xf1, 1, 0, 0, 0, 0, 0]),
    pair('VALUE'), pair('VALUE'), pair(''), Buffer.from([0xff, 0xff]),
  ]);
}

test('transport failure during CLOB, DBCLOB, and XML streams closes the connection', async () => {
  const dataLength = 16_000_000;
  for (const [sqlType, typeName] of [[409, 'CLOB'], [413, 'DBCLOB'], [989, 'XML']]) {
    let serverTask;
    const server = net.createServer((socket) => {
      serverTask = serveTextLobTransportFailure(socket, sqlType, dataLength);
      serverTask.catch(() => socket.destroy());
    });
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const connection = await Db2Connection.connect({
      host: '127.0.0.1', port: server.address().port, database: 'TESTDB', user: 'db2inst1', password: 'password', useSsl: false,
      sslRootCaPath: '', sslClientCertPath: '', sslClientKeyPath: '', timeoutMs: 5000, blockSize: 65535,
      securityMechanism: 3, kerberosSpn: '', kerberosRealm: '', krb5ConfigFile: '', krb5KeytabFile: '', krb5CcacheFile: '',
      clientApplName: '', clientWrkstnName: '', clientUserid: '', clientAcctng: '', clientCorrToken: '', params: {},
    });
    const sourceState = { bytesRead: 0 };
    const source = {
      async *[Symbol.asyncIterator]() {
        const chunk = Buffer.alloc(16_384, 0x61);
        while (true) {
          sourceState.bytesRead += chunk.length;
          yield chunk;
        }
      },
    };
    await assert.rejects(async () => {
      for await (const _row of connection.queryStreamWithParams(
        `VALUES CAST(? AS ${typeName}(${dataLength}))`,
        [{ source, length: dataLength }],
      )) { /* consume the public stream until the transport fails */ }
    });
    assert.equal(connection.isClosed, true, `${typeName} failure should poison the session`);
    assert.ok(sourceState.bytesRead < dataLength, `${typeName} source was fully consumed after TCP reset`);
    await connection.close();
    await serverTask;
    await new Promise((resolve) => server.close(() => resolve()));
  }
});

async function serveTextLobTransportFailure(socket, sqlType, dataLength) {
  const reader = new SocketReader(socket);
  const initial = await readChain(reader);
  assert.deepEqual(initial.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.ACCSEC]);
  await reply(socket, CodePoint.ACCSECRD, ddm.packUint16(CodePoint.SECMEC, 3));
  const auth = await readChain(reader);
  assert.deepEqual(auth.map((packet) => packet.codePoint), [CodePoint.SECCHK, CodePoint.ACCRDB]);
  await reply(socket, CodePoint.SECCHKRM, ddm.packBytes(CodePoint.SECCHKCD, Uint8Array.of(0)), true);
  await reply(socket, CodePoint.ACCRDBRM);
  const setup = await readChain(reader);
  assert.deepEqual(setup.map((packet) => packet.codePoint), [CodePoint.EXCSAT, CodePoint.EXCSQLSET, CodePoint.SQLSTT, CodePoint.RDBCMM]);
  await reply(socket, CodePoint.SQLCARD, sqlCard());

  const prepare = await readChain(reader);
  assert.deepEqual(prepare.map((packet) => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.DSCSQLSTT]);
  const parameterDard = new Uint8Array(20);
  parameterDard.set([0xff, 1, 1, 0, 0, 0, 0, 0]);
  const descriptor = new DataView(parameterDard.buffer);
  descriptor.setBigUint64(8, BigInt(dataLength), true);
  descriptor.setUint16(16, sqlType, true);
  await reply(socket, CodePoint.SQLDARD, parameterDard);

  assert.equal((await dss.readDss(reader)).codePoint, CodePoint.OPNQRY);
  assert.equal((await dss.readDss(reader)).codePoint, CodePoint.SQLDTA);

  // Read the first partial EXTDTA DSS segment, then reset TCP before the next segment.
  const header = await reader.readExact(6);
  const firstSegmentLength = new DataView(header.buffer, header.byteOffset, header.byteLength).getUint16(0, false) & 0x7fff;
  const body = await reader.readExact(firstSegmentLength - 6);
  assert.equal(new DataView(body.buffer, body.byteOffset, body.byteLength).getUint16(2, false), CodePoint.EXTDTA);
  socket.resetAndDestroy();
}

test('import refuses unconfirmed insertion counts and rolls back', async (t) => {
  let serverTask;
  const server = net.createServer(socket => {
    serverTask = (async () => { await serveConnectionSetup(socket); await new Promise(resolve => socket.once('close', resolve)); })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  let controls = 0;
  connection.prepareLocked = async sql => ({ sql, parameterColumns: [{ sqlType: 496 }] });
  connection.executePreparedLocked = async () => 0;
  connection.executeControlLocked = async () => { controls++; };
  async function* rows() { yield [1]; }
  try {
    await assert.rejects(connection.importRows(rows(), { table: 'COUNT_TARGET', columns: ['ID'] }), error => {
      assert.equal(error.progress.committedRows, 0);
      assert.match(error.cause.message, /confirmed 0 inserted rows/);
      return true;
    });
    assert.equal(controls, 1);
  } finally { await connection.close(); await serverTask; }
});

test('query discards the session when the reply chain exceeds the packet limit', async (t) => {
  let serverTask;
  const server = net.createServer((socket) => {
    serverTask = (async () => {
      const reader = await serveConnectionSetup(socket);
      const query = await readChain(reader);
      assert.deepEqual(query.map(packet => packet.codePoint), [CodePoint.PRPSQLSTT, CodePoint.SQLSTT, CodePoint.OPNQRY]);
      // 2049 tiny DDM replies, all chained except the last, exceed the 2048-packet cap.
      for (let index = 0; index <= 2048; index++) {
        await reply(socket, CodePoint.SQLCARD, new Uint8Array(), index < 2048);
      }
      await new Promise((resolve) => socket.once('close', resolve));
    })();
    serverTask.catch(() => socket.destroy());
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise((resolve) => server.close(() => resolve())));

  const connection = await Db2Connection.connect(mockConfig(server.address().port));
  try {
    await assert.rejects(connection.query('SELECT 1 FROM EXCESSIVE_CHAIN_FIXTURE'), /exceeded 2048 packets/);
    assert.equal(connection.isClosed, true, 'an oversized reply chain must not leave a reusable session');
  } finally {
    await connection.close();
    await serverTask;
  }
});
