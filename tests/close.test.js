const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const { once } = require('node:events');
const { Db2Connection } = require('../dist/cjs/network/session.js');

// Real TCP transport tests; no database responses are simulated here.
async function pair() {
  let remote;
  const server = net.createServer({ allowHalfOpen: true }, socket => { remote = socket; socket.resume(); });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const accepted = once(server, 'connection');
  const socket = net.createConnection(server.address().port, '127.0.0.1');
  await Promise.all([once(socket, 'connect'), accepted]);
  const connection = new Db2Connection(socket, { database: 'CONTROL', user: 'CONTROL', securityMechanism: 3, blockSize: 65535 });
  return { socket, remote, connection, async dispose() {
    socket.destroy(); remote.destroy();
    await new Promise(resolve => server.close(resolve));
  } };
}

test('close waits for physical TCP close, shares concurrent cleanup and bounds a half-open peer', async () => {
  const p = await pair();
  try {
    const ended = once(p.remote, 'end');
    let completed = false;
    const first = p.connection.close().then(() => { completed = true; });
    const second = p.connection.close();
    await ended;
    assert.equal(completed, false, 'FIN transmission alone must not complete close');
    assert.equal(p.connection.isClosed, true);
    await Promise.all([first, second]);
    assert.equal(p.socket.closed, true);
    await p.connection.close();
    assert.equal(p.socket.listenerCount('close'), 1, 'only the SocketReader listener remains');
  } finally { await p.dispose(); }
});

test('close waits for a previously destroyed socket close event', async () => {
  const p = await pair();
  try {
    p.socket.destroy();
    await p.connection.close();
    assert.equal(p.socket.closed, true);
    await p.connection.close();
  } finally { await p.dispose(); }
});

test('close releases a session already marked unusable', async () => {
  const p = await pair();
  try {
    p.connection.closed = true;
    await p.connection.close();
    assert.equal(p.socket.closed, true);
  } finally { await p.dispose(); }
});
