// Live TLS checks against the TLS listener that scripts/test-db2-integration.sh --full
// provisions with an ephemeral CA. The certificate identity is 127.0.0.1 only.
//
// DB2_TLS_TEST_DSN is a db2s:// DSN that already carries ssl_root_ca=<ephemeral CA>.
// Without it the tests are reported as skipped with their reason; the integration
// script exports it for --full only.
require('../load-env.cjs');
const test = require('node:test');
const assert = require('node:assert/strict');
const { Db2Connection } = require('../../dist/cjs/network/session.js');

const tlsDsn = process.env.DB2_TLS_TEST_DSN;
const SKIP = 'DB2_TLS_TEST_DSN is not set: the TLS listener is provisioned by --full only';

/**
 * @param {string} dsn
 * @param {{ host?: string, keepCa: boolean }} options
 */
function variant(dsn, { host, keepCa }) {
  const url = new URL(dsn);
  if (host) url.hostname = host;
  if (!keepCa) url.search = '';
  return url.toString();
}

// A TLS verification failure, not a refused socket: the message must come from the
// certificate or name check, so a wrong port or a stopped listener cannot pass.
const TLS_VERIFY_ERROR = /altnames|does not match|certificate|self[- ]signed|unable to verify|hostname/iu;

test('live Db2 TLS connection verifies the ephemeral CA and the 127.0.0.1 identity', { skip: tlsDsn ? false : SKIP, timeout: 60_000 }, async () => {
  const db = await Db2Connection.connect(tlsDsn);
  try {
    assert.deepEqual((await db.query('VALUES 21')).rows, [[21]], 'query over TLS');
    assert.deepEqual((await db.query('VALUES CAST(? AS INTEGER)', [22])).rows, [[22]], 'bound parameter over TLS');
  } finally {
    await db.close();
  }
});

test('live Db2 TLS rejects a certificate whose name does not match the host (fail closed)', { skip: tlsDsn ? false : SKIP, timeout: 60_000 }, async () => {
  // The certificate names only the IP. Asking for another name must fail the identity check.
  await assert.rejects(() => Db2Connection.connect(variant(tlsDsn, { host: 'localhost', keepCa: true })),
    (error) => error instanceof Error && TLS_VERIFY_ERROR.test(error.message), 'a hostname mismatch must not connect');
});

test('live Db2 TLS without a trusted CA rejects the ephemeral server certificate', { skip: tlsDsn ? false : SKIP, timeout: 60_000 }, async () => {
  await assert.rejects(() => Db2Connection.connect(variant(tlsDsn, { keepCa: false })),
    (error) => error instanceof Error && TLS_VERIFY_ERROR.test(error.message), 'an unknown CA must not connect');
});
