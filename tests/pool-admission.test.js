const test = require('node:test');
const assert = require('node:assert/strict');
const { Db2Pool } = require('../dist/cjs/network/pool.js');
const { observeDb2Diagnostics } = require('../dist/cjs/diagnostics.js');
const tick = () => new Promise(resolve => setImmediate(resolve));
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const fake = () => ({ isClosed: false, async close() { this.isClosed = true; }, async resetForPool() {}, async query() { return { rows: [[1]], columns: [] }; } });

test('bounded queue rejects overload, serves FIFO and preserves capacity after cancellation', async () => {
  const pool = new Db2Pool('fake', { maxConnections: 1, maxWaiters: 2, connectionFactory: async () => fake() });
  let release;
  const held = pool.withConnection(() => new Promise(resolve => { release = resolve; }));
  await tick();
  const order = [];
  const first = pool.withConnection(async () => { order.push(1); });
  const second = pool.withConnection(async () => { order.push(2); });
  await assert.rejects(pool.query('VALUES 1'), error => error.category === 'pool-exhausted');
  release(); await Promise.all([held, first, second]);
  assert.deepEqual(order, [1, 2]);
  assert.equal(pool.diagnostics.waiting, 0);
  assert.equal(pool.connectionCount, 1);
  await pool.close();
});

test('acquisition deadline removes its waiter without interrupting a lease', async () => {
  const pool = new Db2Pool('fake', { maxConnections: 1, acquireTimeoutMs: 10, connectionFactory: async () => fake() });
  let release;
  const held = pool.withConnection(() => new Promise(resolve => { release = resolve; }));
  await tick();
  await assert.rejects(pool.query('VALUES 1'), error => error.category === 'pool-timeout');
  assert.equal(pool.diagnostics.waiting, 0);
  release(); await held;
  assert.equal(pool.connectionCount, 1);
  await pool.close();
});

test('late connection after connect timeout is closed and cannot leak a reservation', async () => {
  const connection = fake(); let complete;
  const pool = new Db2Pool('fake', { connectTimeoutMs: 10, connectionFactory: () => new Promise(resolve => { complete = resolve; }) });
  await assert.rejects(pool.query('VALUES 1'), /timed out/);
  assert.equal(pool.connectionCount, 0);
  complete(connection); await tick();
  assert.equal(connection.isClosed, true);
  await pool.close();
});

test('lifetime expiry discards on return and idle reaper removes expired sessions', async () => {
  const pool = new Db2Pool('fake', { maxLifetimeMs: 10, idleTimeoutMs: 10, reapIntervalMs: 5, connectionFactory: async () => fake() });
  const connection = await pool.withConnection(async connection => { await sleep(20); return connection; });
  await tick(); assert.equal(connection.isClosed, true); assert.equal(pool.connectionCount, 0);
  await pool.query('VALUES 1'); await sleep(30);
  assert.equal(pool.connectionCount, 0);
  await pool.close();
});

test('safe diagnostic observers cannot replace results and receive no SQL', async () => {
  const events = [];
  const unsubscribe = observeDb2Diagnostics(event => { events.push(event); throw Error('observer'); });
  const pool = new Db2Pool('fake', { connectionFactory: async () => fake() });
  try { await pool.query('secret SQL'); }
  finally { await pool.close(); unsubscribe(); }
  assert(events.some(event => event.operation === 'execute'));
  assert(!JSON.stringify(events).includes('secret SQL'));
});

test('caller abort cancels pending creation and closes its late transport', async () => {
  const connection = fake(); let complete;
  const pool = new Db2Pool('fake', { connectionFactory: () => new Promise(resolve => { complete = resolve; }) });
  const abort = new AbortController();
  const pending = pool.query('VALUES 1', undefined, { signal: abort.signal });
  await tick(); abort.abort();
  await assert.rejects(pending, error => error.name === 'AbortError');
  assert.equal(pool.connectionCount, 0);
  complete(connection); await tick(); assert.equal(connection.isClosed, true);
  await pool.close();
});

test('shutdown closes a retained lease at its deadline and rejects admission', async () => {
  const connection = fake(); let release;
  const pool = new Db2Pool('fake', { maxConnections: 1, connectionFactory: async () => connection });
  const held = pool.withConnection(() => new Promise(resolve => { release = resolve; }));
  await tick();
  const queued = assert.rejects(pool.query('VALUES 1'), error => error.category === 'connection-closed');
  await pool.shutdown(10); await queued;
  assert.equal(connection.isClosed, true);
  await assert.rejects(pool.query('VALUES 1'), error => error.category === 'connection-closed');
  release(); await held;
  assert.equal(pool.connectionCount, 0); assert.equal(pool.diagnostics.active, 0);
});
