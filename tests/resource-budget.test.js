const test = require('node:test');
const assert = require('node:assert/strict');
const { Db2ResourceBudget } = require('../dist/cjs/resources.js');
const { spoolImportLob } = require('../dist/cjs/import.js');
const lob = bytes => ({ kind: 'binary-lob', length: BigInt(bytes), async *stream() { yield new Uint8Array(bytes); } });
test('spools share a quota and reject before writing an over-budget chunk', async () => {
  const budget = new Db2ResourceBudget(10);
  const first = await spoolImportLob(lob(7), undefined, budget);
  try {
    await assert.rejects(spoolImportLob(lob(4), undefined, budget), error => error.category === 'resource-limit');
    assert.equal(budget.usedBytes, 7);
    const second = await spoolImportLob(lob(3), undefined, budget);
    assert.equal(budget.usedBytes, 10);
    await second.dispose(); await second.dispose();
    assert.equal(budget.usedBytes, 7);
  } finally { await first.dispose(); await first.dispose(); }
  assert.equal(budget.usedBytes, 0);
});
