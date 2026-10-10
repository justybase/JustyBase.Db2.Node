const test = require('node:test');
const assert = require('node:assert/strict');
const { Db2DssStream } = require('../dist/cjs/network/stream.js');
const { encodeRequestDss } = require('../dist/cjs/network/dss.js');
function reader(bytes) {
  let offset = 0;
  return { async readExact(length) { if (offset + length > bytes.length) throw Error('EOF'); const result = bytes.subarray(offset, offset + length); offset += length; return result; }, get consumed() { return offset; } };
}
test('streamed continued DDM retains only a chunk and exactly matches payload', async () => {
  const payload = Buffer.alloc(100_000, 0xab);
  const ddm = Buffer.alloc(8 + payload.length); ddm.writeUInt16BE(0x8008); ddm.writeUInt16BE(0x146c, 2); ddm.writeUInt32BE(payload.length, 4); payload.copy(ddm, 8);
  const wire = encodeRequestDss(ddm, 42, false, true).bytes;
  const input = reader(wire); const stream = await Db2DssStream.open(input);
  assert.equal(stream.codePoint, 0x146c); assert.equal(stream.header.correlationId, 42);
  const chunks = []; let chunk;
  while ((chunk = await stream.nextChunk()) !== undefined) { assert(chunk.length <= 65536); chunks.push(chunk); }
  assert.deepEqual(Buffer.concat(chunks), payload); assert.equal(input.consumed, wire.length);
});
test('declared oversized DDM is rejected before reading payload', async () => {
  const input = reader(Buffer.from([0,14,0xd0,2,0,1,0x80,8,0x14,0x6c,0,0,1,0]));
  await assert.rejects(Db2DssStream.open(input, 255), error => error.category === 'resource-limit');
  assert.equal(input.consumed, 14);
});
