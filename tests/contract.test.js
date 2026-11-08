'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');

const file = path.join(__dirname, '..', 'docs', 'DRIVER-CONTRACT.md');
const headerPattern = /^<!-- normative; v1; sha256:([0-9a-f]{64}); generated from the maintainer contract; do not edit -->$/;

test('the normative contract copy is self-consistent', () => {
  const text = fs.readFileSync(file, 'utf8').replace(/\r\n/g, '\n');
  const newline = text.indexOf('\n');
  assert.ok(newline > 0, 'contract has no header line');
  const match = headerPattern.exec(text.slice(0, newline));
  assert.ok(match, 'contract header is missing or malformed');
  const body = text.slice(newline + 1);
  assert.equal(match[1], crypto.createHash('sha256').update(body, 'utf8').digest('hex'));
  assert.equal(text.includes('db2-conformance'), false, 'contract must not reference the private harness');
});
