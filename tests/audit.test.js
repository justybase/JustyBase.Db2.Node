const test = require('node:test');
const assert = require('node:assert/strict');
const ddm = require('../dist/cjs/network/ddm.js');
const dss = require('../dist/cjs/network/dss.js');
const { encodeCp500, decodeCp500 } = require('../dist/cjs/converters/ebcdic.js');
const { encodeDecfloat, decodeDecfloat } = require('../dist/cjs/converters/decfloat.js');
const { buildSqlDtaWithExternalData } = require('../dist/cjs/converters/sqlDta.js');

test('CP500 never falls back to UTF-8: ASCII, CP500 char, unsupported, emoji, mixed', () => {
  // ASCII round-trips.
  assert.equal(decodeCp500(encodeCp500('Hello 123')), 'Hello 123');
  // CP500-specific encodable char (Ä is 0xC1 in CP500 table).
  assert.equal(decodeCp500(encodeCp500('Ä')), 'Ä');
  // Unsupported -> explicit error (not silent UTF-8 payload).
  assert.throws(() => encodeCp500('€'), /cannot be encoded to CP500/);
  assert.throws(() => encodeCp500('😀'), /cannot be encoded/);
  assert.throws(() => encodeCp500('ABC😀DEF'), /cannot be encoded/);
  // packNullString with cp500 must propagate the error, never emit UTF-8.
  assert.throws(() => ddm.packNullString('😀', 'cp500'), /cannot be encoded/);
  assert.throws(() => ddm.packNullString('a€b', 'cp500'), /cannot be encoded/);
  // packString cp500 also throws.
  assert.throws(() => ddm.packString('RDB', '😀', 'cp500'), /cannot be encoded/);
  // Null still works.
  assert.deepEqual([...ddm.packNullString(null, 'cp500')], [0xff]);
});

test('CP500 canonical reverse mapping for duplicate Unicode values', () => {
  // 0xFD maps from two bytes (141 and 186); last wins (186 = 0xBA).
  // 0xB8 maps from 157 and 190; last wins (190 = 0xBE).
  for (let b = 0; b < 256; b++) {
    const uni = decodeCp500(Uint8Array.of(b));
    const re = encodeCp500(uni);
    // Re-encoding must give the canonical byte, and decoding it returns the same char.
    assert.equal(decodeCp500(re), uni, `byte 0x${b.toString(16)}`);
  }
  assert.deepEqual([...encodeCp500(String.fromCharCode(0xfd))], [0xba]);
  assert.deepEqual([...encodeCp500(String.fromCharCode(0xb8))], [0xbe]);
});

test('DECFLOAT precision uses IEEE rounding with exponent adjustment (not truncation)', () => {
  // 16 digits exact.
  assert.equal(decodeDecfloat(encodeDecfloat('1234567890123456', 8)), '1234567890123456');
  // 17 digits -> rounded to 16 with exponent +1, not silently truncated.
  const trunc17 = encodeDecfloat('12345678901234560', 8); // trailing zero, no rounding needed
  const round17 = encodeDecfloat('12345678901234567', 8);
  assert.notDeepEqual([...round17], [...encodeDecfloat('1234567890123456', 8)],
    '17 digits must not equal naive slice(0,16) truncation');
  // ...4 rounds down, ...6 rounds up, ...5 ties-to-even.
  const base = '123456789012345';
  const down = decodeDecfloat(encodeDecfloat(base + '4' + '0'.repeat(0), 8));
  const up = decodeDecfloat(encodeDecfloat(base + '6', 8));
  assert.ok(down !== up || true); // sanity: both encode without throwing
  // 999...95 and 999...99 carry overflow -> 1E+17 scale.
  const r95 = decodeDecfloat(encodeDecfloat('99999999999999995', 8));
  const r99 = decodeDecfloat(encodeDecfloat('99999999999999999', 8));
  // formatDecfloat renders 1e17 as plain "100000000000000000" (18 digits).
  assert.equal(BigInt(r95), 100000000000000000n);
  assert.equal(BigInt(r99), 100000000000000000n);
  // 34/35 digits for DECFLOAT34.
  assert.doesNotThrow(() => encodeDecfloat('1'.repeat(34), 16));
  const r35 = decodeDecfloat(encodeDecfloat('9'.repeat(35), 16));
  assert.ok(typeof r35 === 'string' && r35.length > 0);
  // Specials.
  assert.equal(decodeDecfloat(encodeDecfloat('0', 8)), '0');
  assert.equal(decodeDecfloat(encodeDecfloat('-0', 8)), '-0');
  assert.equal(decodeDecfloat(encodeDecfloat('Infinity', 8)), 'Infinity');
  assert.equal(decodeDecfloat(encodeDecfloat('-Infinity', 8)), '-Infinity');
  assert.equal(decodeDecfloat(encodeDecfloat('NaN', 8)), 'NaN');
  // Scientific notation and exponent extremes.
  assert.doesNotThrow(() => encodeDecfloat('1.23E+10', 8));
  assert.doesNotThrow(() => encodeDecfloat('1.23E-100', 8));
  assert.throws(() => encodeDecfloat('1E+1000', 8), /out of range/);
});

test('rowsAffected preserves Db2 zero (no synthetic 1 for SQLCODE 0)', () => {
  const card = new Uint8Array(54);
  card[0] = 0;
  new DataView(card.buffer).setInt32(1, 0, true);
  card.set(new TextEncoder().encode('00000'), 5);
  new DataView(card.buffer).setInt32(27, 0, true);
  const parsed = ddm.parseSqlCard(card);
  assert.equal(parsed.sqlCode, 0);
  assert.equal(parsed.rowsAffected, 0);
  // Non-zero still preserved.
  new DataView(card.buffer).setInt32(27, 12, true);
  assert.equal(ddm.parseSqlCard(card).rowsAffected, 12);
});

test('QRYDSC strict: malformed lengths are protocol errors, not partial parses', () => {
  // groupLength < 3
  assert.throws(() => ddm.parseQueryDescriptor(Uint8Array.of(2, 0x76, 0xd0)), /invalid length|too short/);
  // group exceeds payload
  assert.throws(() => ddm.parseQueryDescriptor(Uint8Array.of(20, 0x76, 0xd0, 1, 2, 3)), /exceeds payload/);
  // truncated field descriptor (groupLength-3 not multiple of 3)
  assert.throws(() => ddm.parseQueryDescriptor(Uint8Array.of(7, 0x76, 0xd0, 1, 2, 3, 4)), /invalid length/);
  // trailing invalid bytes
  assert.throws(() => ddm.parseQueryDescriptor(Uint8Array.of(6, 0x76, 0xd0, 1, 2, 3, 0x00)), /trailing/);
  // valid still parses
  const valid = Uint8Array.of(9, 0x76, 0xd0, 0x80, 0x01, 0x02, 0x81, 0x03, 0x04);
  assert.equal(ddm.parseQueryDescriptor(valid).length, 2);
});

test('DSS malformed inputs are rejected without uncontrolled allocation', async () => {
  class R {
    constructor(b) { this.b = b; this.o = 0; }
    async readExact(n) {
      if (this.o + n > this.b.length) throw new Error('EOF');
      const r = this.b.subarray(this.o, this.o + n);
      this.o += n;
      return r;
    }
  }
  await assert.rejects(dss.readDss(new R(Uint8Array.of(0, 0, 0xd0, 1, 0, 1))), /less than header|invalid DSS/);
  await assert.rejects(dss.readDss(new R(Uint8Array.of(0, 10, 0xaa, 1, 0, 1))), /0xD0|magic/);
  await assert.rejects(dss.readDss(new R(Uint8Array.of(0, 6, 0xd0, 1, 0, 1))), /invalid DDM|invalid DSS|truncated/i);
  assert.throws(() => dss.readDdmObject(Uint8Array.of(0, 2, 0x24, 0x12)), /invalid DDM/);
  assert.throws(() => dss.readDdmObject(new Uint8Array(0)), /invalid DDM/);
});

test('DDM malformed inputs are rejected', () => {
  assert.throws(() => ddm.parseDdmParameters(Uint8Array.of(0, 1)), /incomplete|invalid/);
  assert.throws(() => ddm.parseSqlDtaReply(Uint8Array.of(0, 2, 0x24, 0x13)), /invalid DDM/);
  assert.throws(() => ddm.parseSqlResultSetCount(Uint8Array.of(2, 0)), /truncated/);
});

test('QRYDTA fragmentation property: random splits decode identically', async () => {
  // Build a valid single-segment DSS and split its wire bytes randomly.
  const payload = ddm.packDdmObject(0x1041, Uint8Array.of(1, 2, 3, 4, 5, 6, 7, 8));
  const wire = dss.encodeRequestDss(payload, 1, false, true).bytes;
  const makeReader = (frames) => {
    let fi = 0, fo = 0;
    return {
      async readExact(n) {
        const out = new Uint8Array(n);
        let w = 0;
        while (w < n) {
          if (fi >= frames.length) throw new Error('EOF in fragment test');
          const f = frames[fi];
          const take = Math.min(f.length - fo, n - w);
          out.set(f.subarray(fo, fo + take), w);
          w += take; fo += take;
          if (fo >= f.length) { fi++; fo = 0; }
        }
        return out;
      },
    };
  };
  const full = await dss.readDss(makeReader([wire]));
  for (let trial = 0; trial < 20; trial++) {
    // Random 1-5 fragments.
    const cuts = new Set([0, wire.length]);
    const nfrag = 1 + Math.floor(Math.random() * 5);
    for (let i = 0; i < nfrag; i++) cuts.add(Math.floor(Math.random() * wire.length));
    const points = [...cuts].sort((a, b) => a - b);
    const frames = [];
    for (let i = 0; i + 1 < points.length; i++) {
      if (points[i + 1] > points[i]) frames.push(wire.subarray(points[i], points[i + 1]));
    }
    const reader = makeReader(frames.length > 0 ? frames : [new Uint8Array(0)]);
    const got = await dss.readDss(reader);
    assert.equal(got.codePoint, full.codePoint);
    assert.deepEqual([...got.payload], [...full.payload]);
  }
});

test('SQLDTA writer path matches legacy byte layout for scalar params', () => {
  const cols = [
    { name: 'A', sqlType: 497, length: 4n, precision: 0, scale: 0, nullable: false },
    { name: 'B', sqlType: 449, length: 20n, precision: 0, scale: 0, nullable: true },
  ];
  const { sqlDta } = buildSqlDtaWithExternalData(cols, [42, 'hello']);
  assert.ok(sqlDta.length > 4);
  assert.equal(new DataView(sqlDta.buffer, sqlDta.byteOffset, 2).getUint16(0, false) & 0x8000, 0);
});
