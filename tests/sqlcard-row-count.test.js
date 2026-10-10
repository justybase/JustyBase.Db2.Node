'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { parseSqlCard } = require('../dist/cjs/network/ddm.js');

test('SQLERRD3 starts at 27, independent of other diagnostics and byte order', () => {
  for (const littleEndian of [true, false]) for (const count of [0, 1, 257, -1]) {
    const card = new Uint8Array(54);
    card.set(new TextEncoder().encode('00000'), 5);
    const view = new DataView(card.buffer);
    for (let i = 0; i < 6; i++) view.setInt32(19 + i * 4, i === 2 ? count : 0x123456 + i, littleEndian);
    assert.equal(parseSqlCard(card, littleEndian).rowsAffected, count);
    card[18] = 0xff;
    assert.equal(parseSqlCard(card, littleEndian).rowsAffected, 0);
  }
});

test('a present truncated SQLCAXGRP is a protocol error', () => {
  for (const length of [19, 26, 31, 53]) assert.throws(() => parseSqlCard(new Uint8Array(length)), /truncated/i);
});
