'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

test('TLS fixture helper reports a missing OpenSSL instead of a raw ENOENT', () => {
  const helper = path.join(__dirname, 'helpers', 'tls-fixtures.cjs');
  const child = spawnSync(process.execPath, ['-e', `require(${JSON.stringify(helper)}).createTlsFixtures();`], {
    cwd: path.join(__dirname, '..'),
    // An empty PATH makes the preflight's `openssl` lookup fail on every platform.
    env: { ...process.env, PATH: '' },
    encoding: 'utf8',
  });

  assert.notEqual(child.status, 0, 'fixture creation must fail without OpenSSL');
  assert.doesNotMatch(child.stderr, /\bENOENT\b/);
  assert.match(child.stderr, /OpenSSL is required to generate the offline mutual-TLS test certificates/);
  assert.match(child.stderr, /openssl version/);
});
