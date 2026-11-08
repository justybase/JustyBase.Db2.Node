'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const {
  Db2AuthorizationError,
  Db2BatchAbortError,
  Db2BatchExecutionError,
  Db2BatchTimeoutError,
  Db2ConnectTimeoutError,
  Db2Error,
  Db2ServerError,
  Db2TimeoutError,
  canonicalErrorCategory,
} = require('../dist/cjs/errors.js');

test('language-specific categories map to canonical JDC identifiers', () => {
  assert.equal(canonicalErrorCategory('server'), 'sql-server');
  assert.equal(canonicalErrorCategory('invalid-connection-string'), 'configuration');
  assert.equal(canonicalErrorCategory('unsupported-feature'), 'unsupported');
  assert.equal(canonicalErrorCategory('authorization'), 'authorization');
  assert.equal(new Db2Error('x', 'server').canonicalCategory, 'sql-server');
});

test('a confirmed SQLCODE -952 is an operation cancellation', () => {
  const error = new Db2ServerError(-952, '57014', 'interrupted');
  assert.equal(error.category, 'operation-cancelled');
  assert.equal(error.canonicalCategory, 'operation-cancelled');
  assert.equal(error.outcome, 'confirmed');
  assert.equal(error.sqlCode, -952);
  assert.equal(error.sqlState, '57014');
  assert.equal(new Db2ServerError(-204, '42704').category, 'server');
});

test('authorization failures are not classified as authentication', () => {
  const error = new Db2AuthorizationError('not authorized');
  assert.equal(error.category, 'authorization');
  assert.equal(error.canonicalCategory, 'authorization');
});

test('timeout outcome follows the request-write phase', () => {
  const beforeWrite = new Db2TimeoutError('timeout', undefined, 'not-started');
  assert.equal(beforeWrite.category, 'timeout');
  assert.equal(beforeWrite.outcome, 'not-started');
  const afterWrite = new Db2TimeoutError('timeout');
  assert.equal(afterWrite.outcome, 'unknown');
  const connect = new Db2ConnectTimeoutError();
  assert.equal(connect.category, 'timeout');
  assert.equal(connect.outcome, 'not-started');
});

test('batch wrappers derive outcome from the failing set and cause', () => {
  const confirmed = new Db2BatchExecutionError(0, 1, 1, new Db2ServerError(-803, '23505'));
  assert.equal(confirmed.category, 'batch-execution');
  assert.equal(confirmed.outcome, 'confirmed');

  const setup = new Db2BatchExecutionError(undefined, 0, 0, new Db2Error('bad arity', 'parameter-conversion'));
  assert.equal(setup.outcome, 'not-started');

  const uncertain = new Db2BatchExecutionError(2, 2, 2, new Db2Error('socket closed', 'transport'));
  assert.equal(uncertain.outcome, 'unknown');

  const timedOut = new Db2BatchTimeoutError(1, 1, 1, new Db2TimeoutError('deadline'));
  assert.equal(timedOut.category, 'timeout');
  assert.equal(timedOut.outcome, 'unknown');
  assert.ok(timedOut instanceof Db2TimeoutError);
  assert.equal(new Db2BatchTimeoutError(0, undefined, 0, new Db2TimeoutError('deadline')).outcome, 'not-started');
});

test('an aborted batch reports cancellation and its write phase', () => {
  const inFlight = new Db2BatchAbortError(1, 1, 1, new DOMException('aborted', 'AbortError'));
  assert.equal(inFlight.category, 'operation-cancelled');
  assert.equal(inFlight.outcome, 'unknown');
  assert.equal(new Db2BatchAbortError(0, undefined, 0, new DOMException('aborted', 'AbortError')).outcome, 'not-started');
});
