const test = require('node:test');
const assert = require('node:assert/strict');
const { Db2Pool } = require('../dist/cjs/network/pool.js');
const { Db2AuthenticationError, Db2BatchAbortError, Db2ConnectionClosedError } = require('../dist/cjs/errors.js');

test('pool caps physical sessions, reuses released sessions, and closes idle sessions', async () => {
  let created = 0;
  let closed = 0;
  let firstConnection;
  const pool = new Db2Pool('db2://user:password@localhost:50000/TESTDB', {
    maxConnections: 1,
    connectionFactory: async () => {
      created++;
      firstConnection = {
        isClosed: false,
        async query(sql) { return { columns: [], rows: [[sql]] }; },
        async execute() { return 1; },
        async resetForPool() {},
        async close() { this.isClosed = true; closed++; },
      };
      return firstConnection;
    },
  });

  let releaseFirst;
  const holding = pool.withConnection(async (connection) => {
    await new Promise((resolve) => { releaseFirst = resolve; });
    return connection;
  });
  await new Promise((resolve) => setImmediate(resolve));
  const waiting = pool.query('VALUES 1');
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(created, 1);
  releaseFirst();

  assert.equal(await holding, firstConnection);
  assert.deepEqual((await waiting).rows, [['VALUES 1']]);
  assert.equal(created, 1);
  assert.equal(pool.connectionCount, 1);
  assert.equal(pool.idleCount, 1);

  await pool.close();
  assert.equal(closed, 1);
  assert.equal(pool.connectionCount, 0);
  await assert.rejects(pool.query('VALUES 1'), /pool is closed/);
});

test('pool rejects invalid capacity', () => {
  assert.throws(() => new Db2Pool('dsn', { maxConnections: 0 }), /positive integer/);
});

test('pooled procedure reader retains its lease until finish', async () => {
  let reader;
  const connection = {
    isClosed: false,
    async executeProcedureStream() {
      reader = {
        completion: undefined,
        attachCompletionHandler(handler) { this.completion = handler; },
        async finish() { await this.completion(); return { rowsAffected: 0, outputParameters: [] }; },
      };
      return reader;
    },
    async resetForPool() {},
    async close() { this.isClosed = true; },
  };
  const pool = new Db2Pool('db2://user:password@localhost:50000/TESTDB', {
    maxConnections: 1,
    connectionFactory: async () => connection,
  });

  const pooledReader = await pool.executeProcedureStream('CALL TESTPROC()');
  assert.equal(pooledReader, reader);
  assert.equal(pool.idleCount, 0);
  await pooledReader.finish();
  assert.equal(pool.idleCount, 1);
  await pool.close();
});

test('pool close rejects queued callers and closes an active connection when released', async () => {
  let releaseActive;
  let entered;
  const activeEntered = new Promise((resolve) => { entered = resolve; });
  let connection;
  let closed = 0;
  const pool = new Db2Pool('db2://user:password@localhost:50000/TESTDB', {
    maxConnections: 1,
    connectionFactory: async () => {
      connection = {
        isClosed: false,
        async query() { return { columns: [], rows: [] }; },
        async execute() { return 1; },
        async resetForPool() {},
        async close() { this.isClosed = true; closed++; },
      };
      return connection;
    },
  });
  const active = pool.withConnection(async () => {
    entered();
    await new Promise((resolve) => { releaseActive = resolve; });
  });
  await activeEntered;

  const waiting = pool.query('VALUES 1');
  const waitingFailure = assert.rejects(waiting, (error) => {
    assert.ok(error instanceof Db2ConnectionClosedError);
    assert.equal(error.category, 'connection-closed');
    return true;
  });
  await new Promise((resolve) => setImmediate(resolve));
  await pool.close();
  await waitingFailure;
  assert.equal(connection.isClosed, false, 'closing the pool leaves a checked-out session owned by its caller');
  assert.equal(pool.connectionCount, 1);

  releaseActive();
  await active;
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(closed, 1);
  assert.equal(pool.connectionCount, 0);
  await assert.rejects(pool.query('VALUES 1'), Db2ConnectionClosedError);
});

test('AbortSignal cancels a query waiting for a pool lease without interrupting the active lease', async () => {
  let enterActive;
  const activeEntered = new Promise((resolve) => { enterActive = resolve; });
  let finishActive;
  const activeGate = new Promise((resolve) => { finishActive = resolve; });
  let attempts = 0;
  let queryCalls = 0;
  let batchCalls = 0;
  const pool = new Db2Pool('db2://user:password@localhost:50000/TESTDB', {
    maxConnections: 1,
    connectionFactory: async () => {
      attempts++;
      return {
        isClosed: false,
        async query(sql) { queryCalls++; return { columns: [], rows: [[42, sql]] }; },
        async execute() { return 1; },
        async executeBatch() { batchCalls++; return 1; },
        async resetForPool() {},
        async close() { this.isClosed = true; },
      };
    },
  });

  const active = pool.withConnection(async () => {
    enterActive();
    await activeGate;
    return 29;
  });
  await activeEntered;
  const controller = new AbortController();
  const waiting = pool.query('VALUES 1', undefined, { signal: controller.signal });
  controller.abort();
  const leaseController = new AbortController();
  let leaseOperationCalls = 0;
  const waitingLease = pool.withConnection(async () => { leaseOperationCalls++; }, { signal: leaseController.signal });
  leaseController.abort();
  const batchController = new AbortController();
  const waitingBatch = pool.executeBatch('INSERT INTO T VALUES (?)', [[1]], { signal: batchController.signal });
  batchController.abort();
  let cancellationTimer;
  const cancellation = await Promise.race([
    waiting.then(() => 'resolved', (error) => error.name),
    new Promise((resolve) => { cancellationTimer = setTimeout(() => resolve('timeout'), 1000); }),
  ]);
  clearTimeout(cancellationTimer);
  await assert.rejects(waitingLease, (error) => error.name === 'AbortError');
  await assert.rejects(waitingBatch, (error) => error instanceof Db2BatchAbortError
    && error.name === 'AbortError'
    && error.completedParameterSets === 0
    && error.inFlightParameterSetIndex === undefined
    && error.rowsAffected === 0);

  assert.equal(pool.connectionCount, 1);
  assert.equal(pool.idleCount, 0);
  assert.equal(queryCalls, 0, 'an aborted waiter must never send SQL on the active lease');
  assert.equal(leaseOperationCalls, 0, 'an aborted acquisition must not invoke its callback');
  assert.equal(batchCalls, 0, 'an aborted queued batch must not be sent on the active lease');
  finishActive();
  assert.equal(await active, 29);
  assert.equal(cancellation, 'AbortError');
  assert.equal(pool.idleCount, 1);
  assert.deepEqual((await pool.query('VALUES 42')).rows, [[42, 'VALUES 42']]);
  assert.equal(attempts, 1);
  await pool.close();
  assert.equal(pool.connectionCount, 0);
});

test('AbortSignal rejects during a pool replacement open and closes the late session', async () => {
  let enterActive;
  const activeEntered = new Promise((resolve) => { enterActive = resolve; });
  let failReset;
  const resetGate = new Promise((resolve) => { failReset = resolve; });
  let signalReplacement;
  const replacementStarted = new Promise((resolve) => { signalReplacement = resolve; });
  let finishReplacement;
  const replacementGate = new Promise((resolve) => { finishReplacement = resolve; });
  let attempts = 0;
  let closed = 0;
  let lateSession;
  const pool = new Db2Pool('db2://user:password@localhost:50000/TESTDB', {
    maxConnections: 1,
    connectionFactory: async () => {
      const attempt = ++attempts;
      if (attempt === 2) {
        signalReplacement();
        await replacementGate;
      }
      const connection = {
        isClosed: false,
        async query(sql) { return { columns: [], rows: [[42, sql]] }; },
        async execute() { return 1; },
        async resetForPool() {
          if (attempt === 1) {
            await resetGate;
            throw new Error('injected reset failure');
          }
        },
        async close() { this.isClosed = true; closed++; },
      };
      if (attempt === 2) lateSession = connection;
      return connection;
    },
  });

  const active = pool.withConnection(async () => {
    enterActive();
    return 29;
  });
  await activeEntered;
  const controller = new AbortController();
  const waiting = pool.query('VALUES 1', undefined, { signal: controller.signal });
  failReset();
  assert.equal(await active, 29);
  await replacementStarted;

  controller.abort();
  let cancellationTimer;
  const cancellation = await Promise.race([
    waiting.then(() => 'resolved', (error) => error.name),
    new Promise((resolve) => { cancellationTimer = setTimeout(() => resolve('timeout'), 1000); }),
  ]);
  clearTimeout(cancellationTimer);
  finishReplacement();
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(cancellation, 'AbortError');
  assert.equal(attempts, 2);
  assert.equal(closed, 2, 'the failed original and late replacement must both be closed');
  assert.equal(lateSession.isClosed, true);
  assert.equal(pool.connectionCount, 0);
  assert.equal(pool.idleCount, 0);
  await pool.close();
});

test('pool close drains a connection that finishes opening afterward', async () => {
  let finishConnect;
  let connectStarted;
  const started = new Promise((resolve) => { connectStarted = resolve; });
  let closed = 0;
  const pool = new Db2Pool('db2://user:password@localhost:50000/TESTDB', {
    maxConnections: 1,
    connectionFactory: async () => {
      connectStarted();
      await new Promise((resolve) => { finishConnect = resolve; });
      return {
        isClosed: false,
        async query() { return { columns: [], rows: [] }; },
        async execute() { return 1; },
        async resetForPool() {},
        async close() { this.isClosed = true; closed++; },
      };
    },
  });
  const opening = pool.query('VALUES 1');
  await started;
  await pool.close();
  finishConnect();
  await assert.rejects(opening, /pool is closed/);
  assert.equal(closed, 1);
  assert.equal(pool.connectionCount, 0);
});

test('pool close during session reset discards the releasing connection', async () => {
  let releaseReset;
  let resetStarted;
  const started = new Promise((resolve) => { resetStarted = resolve; });
  let connection;
  let closed = 0;
  const pool = new Db2Pool('db2://user:password@localhost:50000/TESTDB', {
    maxConnections: 1,
    connectionFactory: async () => {
      connection = {
        isClosed: false,
        async query() { return { columns: [], rows: [] }; },
        async execute() { return 1; },
        async resetForPool() {
          resetStarted();
          await new Promise((resolve) => { releaseReset = resolve; });
        },
        async close() { this.isClosed = true; closed++; },
      };
      return connection;
    },
  });

  const operation = pool.withConnection(async () => 17);
  await started;
  await pool.close();
  releaseReset();
  assert.equal(await operation, 17);
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(connection.isClosed, true);
  assert.equal(closed, 1);
  assert.equal(pool.connectionCount, 0);
  assert.equal(pool.idleCount, 0);
});

test('pool close during a failed session reset does not retain the connection', async () => {
  let failReset;
  let resetStarted;
  const started = new Promise((resolve) => { resetStarted = resolve; });
  let connection;
  let closed = 0;
  const pool = new Db2Pool('db2://user:password@localhost:50000/TESTDB', {
    maxConnections: 1,
    connectionFactory: async () => {
      connection = {
        isClosed: false,
        async query() { return { columns: [], rows: [] }; },
        async execute() { return 1; },
        async resetForPool() {
          resetStarted();
          await new Promise((resolve) => { failReset = resolve; });
          throw new Error('reset failed');
        },
        async close() { this.isClosed = true; closed++; },
      };
      return connection;
    },
  });

  const operation = pool.withConnection(async () => 23);
  await started;
  await pool.close();
  failReset();
  assert.equal(await operation, 23);
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(connection.isClosed, true);
  assert.equal(closed, 1);
  assert.equal(pool.connectionCount, 0);
  assert.equal(pool.idleCount, 0);
});

test('pool replaces a session after reset fails and wakes a queued caller', async () => {
  let signalResetStarted;
  const resetStarted = new Promise((resolve) => { signalResetStarted = resolve; });
  let failReset;
  const resetGate = new Promise((resolve) => { failReset = resolve; });
  let created = 0;
  let closed = 0;
  let firstConnection;
  const pool = new Db2Pool('db2://user:password@localhost:50000/TESTDB', {
    maxConnections: 1,
    connectionFactory: async () => {
      const id = ++created;
      const connection = {
        isClosed: false,
        async query(sql) { return { columns: [], rows: [[id, sql]] }; },
        async execute() { return 1; },
        async resetForPool() {
          if (id !== 1) return;
          signalResetStarted();
          await resetGate;
          throw new Error('injected pool reset failure');
        },
        async close() { this.isClosed = true; closed++; },
      };
      if (id === 1) firstConnection = connection;
      return connection;
    },
  });

  const releasing = pool.withConnection(async (connection) => {
    assert.equal(connection, firstConnection);
    return 17;
  });
  await resetStarted;
  const waiting = pool.withConnection((connection) => connection.query('VALUES 1'));
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(created, 1, 'the queued caller must wait while the only slot is resetting');
  assert.equal(pool.connectionCount, 1);

  failReset();
  assert.equal(await releasing, 17);
  assert.deepEqual((await waiting).rows, [[2, 'VALUES 1']]);
  assert.equal(created, 2, 'a failed reset must reserve and connect a replacement');
  assert.equal(closed, 1, 'the session whose reset failed must be closed');
  assert.equal(pool.connectionCount, 1);
  assert.equal(pool.idleCount, 1);

  await pool.close();
  assert.equal(closed, 2);
  assert.equal(pool.connectionCount, 0);
});

test('pool attaches reset failure as cleanup context without masking the operation error', async () => {
  const resetError = new Error('injected reset failure');
  const unhandled = [];
  const onUnhandled = (error) => unhandled.push(error);
  process.on('unhandledRejection', onUnhandled);
  const pool = new Db2Pool('db2://user:password@localhost:50000/TESTDB', {
    maxConnections: 1,
    connectionFactory: async () => ({
      isClosed: false,
      async query() { throw new Error('injected operation failure'); },
      async execute() { return 1; },
      async resetForPool() { throw resetError; },
      async close() { this.isClosed = true; throw new Error('injected close failure'); },
    }),
  });
  try {
    const failure = await pool.query('VALUES 1').then(() => null, (error) => error);
    assert.ok(failure instanceof Error);
    assert.equal(failure.message, 'injected operation failure');
    assert.equal(failure.cleanupCause, resetError);
    assert.equal(pool.connectionCount, 0, 'the session with failed cleanup must be discarded');
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(unhandled, [], 'discard close failures must be observed');
    await pool.close();
  } finally {
    process.off('unhandledRejection', onUnhandled);
  }
});

test('pool preserves a successful operation result when reset and discard close fail', async () => {
  const unhandled = [];
  const onUnhandled = (error) => unhandled.push(error);
  process.on('unhandledRejection', onUnhandled);
  const pool = new Db2Pool('db2://user:password@localhost:50000/TESTDB', {
    maxConnections: 1,
    connectionFactory: async () => ({
      isClosed: false,
      async query() { return { columns: [], rows: [[42]] }; },
      async execute() { return 1; },
      async resetForPool() { throw new Error('injected rollback failure'); },
      async close() { this.isClosed = true; throw new Error('injected close failure'); },
    }),
  });
  try {
    assert.deepEqual(await pool.query('VALUES 42'), { columns: [], rows: [[42]] });
    assert.equal(pool.connectionCount, 0, 'the session with failed cleanup must be discarded');
    assert.equal(pool.idleCount, 0);
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(unhandled, [], 'discard close failures must be observed');
    await pool.close();
  } finally {
    process.off('unhandledRejection', onUnhandled);
  }
});

test('pool close during replacement open after reset failure rejects waiters and closes the replacement', async () => {
  let signalResetStarted;
  const resetStarted = new Promise((resolve) => { signalResetStarted = resolve; });
  let failReset;
  const resetGate = new Promise((resolve) => { failReset = resolve; });
  let signalReplacementStarted;
  const replacementStarted = new Promise((resolve) => { signalReplacementStarted = resolve; });
  let finishReplacement;
  const replacementGate = new Promise((resolve) => { finishReplacement = resolve; });
  let attempts = 0;
  let closed = 0;
  let replacement;
  const pool = new Db2Pool('db2://user:password@localhost:50000/TESTDB', {
    maxConnections: 1,
    connectionFactory: async () => {
      const attempt = ++attempts;
      if (attempt === 2) {
        signalReplacementStarted();
        await replacementGate;
      }
      const connection = {
        isClosed: false,
        async query(sql) { return { columns: [], rows: [[42, sql]] }; },
        async execute() { return 1; },
        async resetForPool() {
          if (attempt !== 1) return;
          signalResetStarted();
          await resetGate;
          throw new Error('injected pool reset failure');
        },
        async close() { this.isClosed = true; closed++; },
      };
      if (attempt === 2) replacement = connection;
      return connection;
    },
  });

  const releasing = pool.withConnection(async () => 29);
  await resetStarted;
  const openingWaiter = pool.query('VALUES 1');
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(attempts, 1, 'the queued operation waits for failed reset cleanup');
  failReset();
  assert.equal(await releasing, 29);
  await replacementStarted;
  const queuedWaiter = pool.query('VALUES 2');
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(attempts, 2, 'the first waiter owns the replacement handshake');

  await pool.close();
  await assert.rejects(queuedWaiter, Db2ConnectionClosedError);
  finishReplacement();
  await assert.rejects(openingWaiter, Db2ConnectionClosedError);

  assert.equal(attempts, 2, 'closing the pool must not start another replacement');
  assert.equal(closed, 2, 'both the reset-failed session and late replacement are closed');
  assert.equal(replacement.isClosed, true);
  assert.equal(pool.connectionCount, 0);
  assert.equal(pool.idleCount, 0);
});

test('pool recovers multiple waiters after reset, transport, and authentication failures', async () => {
  let signalResetStarted;
  const resetStarted = new Promise((resolve) => { signalResetStarted = resolve; });
  let failReset;
  const resetGate = new Promise((resolve) => { failReset = resolve; });
  let attempts = 0;
  let closed = 0;
  let firstConnection;
  const failedReplacements = 3;
  const pool = new Db2Pool('db2://user:password@localhost:50000/TESTDB', {
    maxConnections: 1,
    connectionFactory: async () => {
      const attempt = ++attempts;
      if (attempt === 2 || attempt === 3)
        throw new Db2ConnectionClosedError('injected replacement transport disconnect');
      if (attempt === 4)
        throw new Db2AuthenticationError('injected SECCHK failure', 8);
      const connection = {
        isClosed: false,
        async query(sql) { return { columns: [], rows: [[42, sql]] }; },
        async execute() { return 1; },
        async resetForPool() {
          if (attempt !== 1) return;
          signalResetStarted();
          await resetGate;
          throw new Error('injected pool reset failure');
        },
        async close() { this.isClosed = true; closed++; },
      };
      if (attempt === 1) firstConnection = connection;
      return connection;
    },
  });

  const releasing = pool.withConnection(async (connection) => {
    assert.equal(connection, firstConnection);
    return 29;
  });
  await resetStarted;
  const waiting = Array.from({ length: failedReplacements + 1 }, () => {
    let settled = false;
    const operation = pool.query('VALUES 1').finally(() => { settled = true; });
    return { operation, isSettled: () => settled };
  });
  await new Promise((resolve) => setImmediate(resolve));
  assert.ok(waiting.every((operation) => !operation.isSettled()), 'the only slot is still held by reset');
  assert.equal(attempts, 1, 'queued callers must not open replacement sessions before reset finishes');
  assert.equal(pool.connectionCount, 1);

  failReset();
  assert.equal(await releasing, 29, 'reset cleanup failure must not replace the user operation result');
  const outcomes = await Promise.allSettled(waiting.map((operation) => operation.operation));
  const failures = outcomes.filter((outcome) => outcome.status === 'rejected');
  const successes = outcomes.filter((outcome) => outcome.status === 'fulfilled');
  assert.equal(failures.length, failedReplacements);
  assert.equal(failures.filter((outcome) => outcome.reason instanceof Db2ConnectionClosedError).length, 2);
  assert.equal(failures.filter((outcome) => outcome.reason instanceof Db2AuthenticationError).length, 1);
  assert.equal(failures.find((outcome) => outcome.reason instanceof Db2AuthenticationError).reason.securityCode, 8);
  assert.equal(successes.length, 1);
  assert.deepEqual(successes[0].value.rows, [[42, 'VALUES 1']]);
  assert.equal(attempts, 5, 'later waiters must each be retried after a failed open');
  assert.equal(closed, 1, 'the session whose reset failed must be closed');
  assert.equal(pool.connectionCount, 1);
  assert.equal(pool.idleCount, 1);

  await pool.close();
  assert.equal(closed, 2);
  assert.equal(pool.connectionCount, 0);
  assert.equal(pool.idleCount, 0);
});

test('concurrent pool close callers wait for the same idle-session shutdown', async () => {
  let closeStarted;
  const started = new Promise((resolve) => { closeStarted = resolve; });
  let releaseClose;
  const closeGate = new Promise((resolve) => { releaseClose = resolve; });
  let closeCalls = 0;
  const connection = {
    isClosed: false,
    async query() { return { columns: [], rows: [] }; },
    async execute() { return 1; },
    async resetForPool() {},
    async close() {
      closeCalls++;
      closeStarted();
      await closeGate;
      this.isClosed = true;
    },
  };
  const pool = new Db2Pool('db2://user:password@localhost:50000/TESTDB', {
    maxConnections: 1,
    connectionFactory: async () => connection,
  });

  await pool.withConnection(async () => 7);
  const firstClose = pool.close();
  await started;
  let secondFinished = false;
  const secondClose = pool.close().then(() => { secondFinished = true; });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(secondFinished, false, 'every close caller waits until idle sessions are actually closed');

  releaseClose();
  await Promise.all([firstClose, secondClose]);
  assert.equal(secondFinished, true);
  assert.equal(closeCalls, 1);
  assert.equal(connection.isClosed, true);
  assert.equal(pool.connectionCount, 0);
});

test('pool retries failed connection factories for queued callers without leaking capacity', async () => {
  let attempts = 0;
  let closeCalls = 0;
  const pool = new Db2Pool('db2://user:password@localhost:50000/TESTDB', {
    maxConnections: 1,
    connectionFactory: async () => {
      attempts++;
      if (attempts <= 3) throw new Error(`injected connection failure ${attempts}`);
      return {
        isClosed: false,
        async query(sql) { return { columns: [], rows: [[sql]] }; },
        async execute() { return 1; },
        async resetForPool() {},
        async close() { this.isClosed = true; closeCalls++; },
      };
    },
  });

  const callers = Array.from({ length: 4 }, (_, index) =>
    pool.withConnection((connection) => connection.query(`VALUES ${index + 1}`)));
  let timer;
  const settled = await Promise.race([
    Promise.allSettled(callers),
    new Promise((resolve) => { timer = setTimeout(() => resolve(undefined), 3_000); }),
  ]);
  clearTimeout(timer);
  if (!settled) {
    await pool.close();
    await Promise.allSettled(callers);
    assert.fail('queued callers remained stuck after the factory released a failed reservation');
  }

  assert.equal(attempts, 4, 'each failed reservation must advance one queued caller');
  assert.equal(settled.filter((result) => result.status === 'rejected').length, 3);
  const successes = settled.filter((result) => result.status === 'fulfilled');
  assert.equal(successes.length, 1);
  assert.deepEqual(successes[0].value.rows, [['VALUES 4']]);
  assert.equal(pool.connectionCount, 1);
  assert.equal(pool.idleCount, 1);

  await pool.close();
  assert.equal(closeCalls, 1);
  assert.equal(pool.connectionCount, 0);
  assert.equal(pool.idleCount, 0);
});
