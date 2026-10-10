'use strict';
const { createHash } = require('node:crypto');
const { Db2Connection, isDb2LobStream } = require('../dist/cjs/network/session.js');

const [table, expectedHash] = process.argv.slice(2);
if (!/^LD_[A-F0-9]{12}$/.test(table ?? '') || !/^[A-Fa-f0-9]{64}$/.test(expectedHash ?? ''))
  throw new Error('Usage: large-data-reader.cjs LD_<12HEX> <sha256>');
const expectedBytes = 1n << 30n;

(async () => {
  const connection = await Db2Connection.connect(process.env.DB2_TEST_DSN);
  try {
    let count = 0n;
    const hash = createHash('sha256');
    let found = false;
    for await (const row of connection.queryStream(`SELECT PAYLOAD FROM ${table} WHERE ID = 1`)) {
      if (found) throw new Error('Large LOB query returned more than one row');
      found = true;
      const lob = row.values[0];
      if (!isDb2LobStream(lob) || lob.kind !== 'binary-lob')
        throw new Error('Db2 did not expose the 1 GiB value as a progressive binary LOB stream');
      if (lob.length !== expectedBytes) throw new Error(`LOB length mismatch: ${lob.length}`);
      for await (const chunk of lob.stream()) {
        hash.update(chunk);
        count += BigInt(chunk.byteLength);
      }
    }
    if (!found) throw new Error('Large LOB fixture row is missing');
    const actual = hash.digest('hex');
    if (count !== expectedBytes || actual.toLowerCase() !== expectedHash.toLowerCase())
      throw new Error(`Stream verification failed: read ${count} bytes, SHA-256 ${actual}`);
    console.log(JSON.stringify({
      type: 'large-data-evidence', language: 'typescript', bytes: Number(count),
      checksumMatched: true, expectedSha256: expectedHash.toLowerCase(), actualSha256: actual.toLowerCase(),
      mode: 'stream', maxRssBytes: process.resourceUsage().maxRSS * 1024,
      maxSpoolBytes: 0,
    }));
  } finally {
    await connection.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
