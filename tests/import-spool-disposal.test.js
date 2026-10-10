const test = require('node:test');
const assert = require('node:assert/strict');
const { access } = require('node:fs/promises');
const { once } = require('node:events');
const { spoolImportLob } = require('../dist/cjs/import.js');
for (const size of [0, 3]) test(`disposing unused spool closes its file descriptor (${size} bytes)`, async () => {
  const spool = await spoolImportLob({ kind: 'binary-lob', length: BigInt(size), async *stream() { if (size) yield Uint8Array.of(0, 255, 1); } });
  const path = spool.source.path;
  if (spool.source.pending) await once(spool.source, 'open');
  assert.equal(typeof spool.source.fd, 'number');
  await spool.dispose();
  assert.equal(spool.source.closed, true);
  assert.equal(spool.source.fd, null);
  await assert.rejects(access(path), { code: 'ENOENT' });
  await spool.dispose();
});
