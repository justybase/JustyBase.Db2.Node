const test = require('node:test');
const assert = require('node:assert/strict');
const { buildSqlDta, buildSqlDtaWithExternalData } = require('../dist/cjs/converters/sqlDta.js');
const { parseDdmParameters } = require('../dist/cjs/network/ddm.js');

test('SQLDTA matches the Go reference vector byte for byte', () => {
  const columns = [
    { name: 'PARAM1', sqlType: 497, length: 4n, precision: 0, scale: 0, nullable: true },
    { name: 'PARAM2', sqlType: 449, length: 100n, precision: 0, scale: 0, nullable: true },
    { name: 'PARAM3', sqlType: 481, length: 8n, precision: 0, scale: 0, nullable: true },
    { name: 'PARAM4', sqlType: 2437, length: 2n, precision: 0, scale: 0, nullable: true },
    { name: 'PARAM5', sqlType: 385, length: 10n, precision: 0, scale: 0, nullable: true },
  ];
  const actual = Buffer.from(buildSqlDta(columns, [1, 'Teclado Mecânico', 250.50, true, new Date('2026-08-26T00:00:00Z')])).toString('hex');
  assert.equal(actual, '00642412001c00101276d0030004393fff0b000805000221000a0671e4d000010044147a000001000000000010005400650063006c00610064006f0020004d0065006300e2006e00690063006f000000000000506f4000010000323032362d30382d3236');
});

test('SQLDTA rejects character parameters beyond the 65535 code-unit DRDA length', () => {
  const column = { name: 'P', sqlType: 449, length: 70000n, precision: 0, scale: 0, nullable: true };
  assert.throws(() => buildSqlDta([column], ['x'.repeat(65536)]), RangeError);
  assert.doesNotThrow(() => buildSqlDta([column], ['x'.repeat(65535)]));
});

test('SQLDTA uses CPT continuation groups and preserves all wide parameter values', () => {
  for (const [count, expectedGroups] of [[83, 1], [84, 1], [85, 2], [148, 2]]) {
    const columns = Array.from({ length: count }, (_, index) => ({
      name: 'P' + index, sqlType: 501, length: 2n, precision: 0, scale: 0, nullable: true,
    }));
    const values = Array.from({ length: count }, (_, index) => index % 11 === 0 ? null : index);
    const encoded = Buffer.from(buildSqlDta(columns, values));
    const objects = parseDdmParameters(new Uint8Array(encoded.subarray(4)));
    const descriptor = Buffer.from(objects.get(0x0010));
    const data = Buffer.from(objects.get(0x147a));

    let offset = 0;
    let groups = 0;
    let fields = 0;
    while (offset < descriptor.length - 6) {
      const groupLength = descriptor[offset];
      assert.ok(groupLength >= 6 && groupLength <= 255);
      assert.equal((groupLength - 3) % 3, 0);
      assert.equal(descriptor[offset + 1], groups === 0 ? 0x76 : 0x7f);
      assert.equal(descriptor[offset + 2], groups === 0 ? 0xd0 : 0x00);
      const groupFields = (groupLength - 3) / 3;
      for (let field = 0; field < groupFields; field++)
        assert.deepEqual([...descriptor.subarray(offset + 3 + field * 3, offset + 6 + field * 3)], [0x05, 0, 2]);
      fields += groupFields;
      groups++;
      offset += groupLength;
    }

    assert.equal(groups, expectedGroups);
    assert.equal(fields, count);
    assert.deepEqual([...descriptor.subarray(offset)], [0x06, 0x71, 0xe4, 0xd0, 0, 1]);
    assert.equal(data[0], 0); // FDODTA leading alignment byte.
    offset = 1;
    for (let index = 0; index < count; index++) {
      if (values[index] === null) {
        assert.equal(data[offset++], 0xff);
        continue;
      }
      assert.equal(data[offset++], 0);
      assert.equal(data.readInt16LE(offset), index);
      offset += 2;
    }
    assert.equal(offset, data.length);
  }
});

test('SQLDTA normalizes Db2 date and time text without changing its meaning', () => {
  const column = (sqlType, length) => ({ name: 'TEMPORAL', sqlType, length: BigInt(length), precision: 0, scale: 0, nullable: true });
  const assertText = (sqlType, length, value, expected) => {
    const encoded = Buffer.from(buildSqlDta([column(sqlType, length)], [value]));
    const suffix = Buffer.concat([Buffer.from([0]), Buffer.from(expected, 'ascii')]);
    assert.ok(encoded.subarray(-suffix.length).equals(suffix), sqlType + ' input ' + value + ' should encode as ' + expected);
  };

  assertText(385, 10, '2024-2-9', '2024-02-09');
  assertText(385, 10, '02/29/2024', '2024-02-29');
  assertText(389, 8, '12:34', '12:34:00');
  assertText(389, 8, '23.59.58', '23:59:58');
  assertText(393, 32, '2024-02-29T23:59:58.123456', '2024-02-29-23.59.58.123456      ');
  assertText(393, 32, '2024-02-29T23:59:58.123456789012', '2024-02-29-23.59.58.123456789012');
  assertText(393, 32, '2024-02-29', '2024-02-29-00.00.00.000000      ');

  for (const [sqlType, length, value] of [
    [385, 10, '2024-02-30'],
    [389, 8, '23:59:58.123456'],
    [389, 8, '24:00:01'],
    [393, 32, '2024-02-30 23:59:58'],
    [393, 32, '2024-02-29 23:59:58+02:00'],
  ])
    assert.throws(() => buildSqlDta([column(sqlType, length)], [value]));
});

test('SQLDTA rejects an argument count mismatch', () => {
  assert.throws(() => buildSqlDta([{ name: 'PARAM1', sqlType: 497, length: 4n, precision: 0, scale: 0, nullable: true }], []), /expected 1 parameters, got 0/);
});

test('SQLDTA includes the leading FDODTA alignment byte for even DECIMAL lengths', () => {
  const column = { name: 'DECIMAL', sqlType: 485, length: 0n, precision: 4, scale: 2, nullable: true };
  const encoded = Buffer.from(buildSqlDta([column], ['1.25']));
  assert.ok(encoded.toString('hex').endsWith('0009147a000000125c'));
});

test('SQLDTA rejects signed integer overflow instead of wrapping', () => {
  const small = { name: 'SMALL', sqlType: 501, length: 2n, precision: 0, scale: 0, nullable: true };
  const integer = { name: 'INTEGER', sqlType: 497, length: 4n, precision: 0, scale: 0, nullable: true };
  const bigint = { name: 'BIGINT', sqlType: 493, length: 8n, precision: 0, scale: 0, nullable: true };
  assert.throws(() => buildSqlDta([small], [32768]), RangeError);
  assert.throws(() => buildSqlDta([integer], [2147483648]), RangeError);
  const minimum = Buffer.from(buildSqlDta([bigint], ['-9223372036854775808.0']));
  assert.equal(minimum.readBigInt64LE(minimum.length - 8), -(1n << 63n));
  assert.throws(() => buildSqlDta([bigint], ['-9223372036854775809.0']), RangeError);
  assert.throws(() => buildSqlDta([bigint], ['-1e19']), RangeError);
  assert.doesNotThrow(() => buildSqlDta([small], [-32768]));
});

test('SQLDTA truncates fractional numeric values toward zero for integer parameters', () => {
  const small = { name: 'SMALL', sqlType: 501, length: 2n, precision: 0, scale: 0, nullable: true };
  for (const [value, expected] of [[-3.75, -3], [-1.5, -1], [-0.5, 0], [0.5, 0], [1.5, 1], [3.75, 3]]) {
    const encoded = Buffer.from(buildSqlDta([small], [value]));
    assert.equal(encoded.readInt16LE(encoded.length - 2), expected, `value ${value}`);
  }
});

test('SQLDTA integer parameters truncate Db2 numeric text and accept boolean values', () => {
  const small = { name: 'SMALL', sqlType: 501, length: 2n, precision: 0, scale: 0, nullable: true };
  const accepted = [
    ['-3.75', -3], ['1.5', 1], [' 1.5 ', 1], ['1e0', 1],
    ['.5', 0], ['1.', 1], ['0.000', 0], ['+0001.50', 1],
    [true, 1], [false, 0],
  ];
  for (const [value, expected] of accepted) {
    const encoded = Buffer.from(buildSqlDta([small], [value]));
    assert.equal(encoded.readInt16LE(encoded.length - 2), expected, `value ${String(value)}`);
  }

  for (const invalid of ['1,5', 'abc', ''])
    assert.throws(() => buildSqlDta([small], [invalid]));
  assert.throws(() => buildSqlDta([small], ['32768.0']), RangeError);
});

test('SQLDTA boolean parameters match Db2 text and integer conversions', () => {
  const boolean = { name: 'BOOLEAN', sqlType: 2437, length: 1n, precision: 0, scale: 0, nullable: true };
  const accepted = [
    [true, true], [false, false],
    ['true', true], [' FALSE ', false],
    ['YES', true], ['no', false], ['y', true], ['N', false],
    ['On', true], ['OFF', false], ['0', false], ['2', true],
    ['-0', false], [' +2 ', true], ['99999999999999999999999999999999', true],
    [0, false], [2, true], [-1, true], [9223372036854775807n, true],
  ];
  for (const [value, expected] of accepted) {
    assert.equal(Buffer.from(buildSqlDta([boolean], [value])).at(-1), expected ? 1 : 0, `value ${String(value)}`);
  }

  for (const invalid of ['', ' ', '1.0', 'maybe', 1.5, Number.NaN, Number.POSITIVE_INFINITY, {}])
    assert.throws(() => buildSqlDta([boolean], [invalid]), TypeError, `value ${String(invalid)}`);
});

test('SQLDTA matches IBM CLI source type rules for numeric, text, binary and LOB targets', () => {
  const column = (sqlType, length = 0, precision = 0, scale = 0) =>
    ({ name: 'PARAM', sqlType, length: BigInt(length), precision, scale, nullable: true });
  const varchar = column(449, 20);
  for (const [value, expected] of [[true, 49], [false, 48]]) {
    const encoded = Buffer.from(buildSqlDta([varchar], [value]));
    assert.deepEqual([...encoded.subarray(-2)], [0, expected]);
  }
  const utf8Text = Buffer.from(buildSqlDta([varchar], [Uint8Array.of(0xe6, 0x9d, 0xb1)]));
  assert.ok(utf8Text.includes(Buffer.from([0x3f, 0x7f, 0xff])));
  assert.deepEqual([...utf8Text.subarray(-6)], [0, 0, 3, 0xe6, 0x9d, 0xb1]);
  const invalidUtf8Text = Buffer.from(buildSqlDta([varchar], [Uint8Array.of(0xe9)]));
  assert.ok(invalidUtf8Text.includes(Buffer.from([0x3f, 0x7f, 0xff])));
  assert.deepEqual([...invalidUtf8Text.subarray(-4)], [0, 0, 1, 0xe9]);

  const numericTargets = [
    column(501, 2), column(497, 4), column(493, 8), column(481, 4),
    column(481, 8), column(485, 0, 9, 3), column(997, 16),
  ];
  for (const target of numericTargets) {
    assert.doesNotThrow(() => buildSqlDta([target], [true]), target.name + ' true');
    assert.doesNotThrow(() => buildSqlDta([target], [false]), target.name + ' false');
    assert.throws(() => buildSqlDta([target], [Uint8Array.of(49, 50, 51)]), TypeError, target.name + ' binary input');
  }

  const binaryTargets = [column(913, 4), column(909, 10), column(405, 10)];
  for (const target of binaryTargets) {
    for (const value of [true, 123, 12.5])
      assert.throws(() => buildSqlDta([target], [value]), TypeError, target.name + ' from ' + typeof value);
    assert.doesNotThrow(() => buildSqlDta([target], ['313233']), target.name + ' hexadecimal string');
    assert.doesNotThrow(() => buildSqlDta([target], [Uint8Array.of(49, 50, 51)]), target.name + ' bytes');
  }
  assert.throws(() => buildSqlDta([column(909, 10)], ['123']), TypeError, 'odd hexadecimal length');
  assert.throws(() => buildSqlDta([column(909, 10)], ['12G4']), TypeError, 'non-hexadecimal character');
  assert.throws(() => buildSqlDta([column(909, 2)], ['313233']), TypeError, 'decoded bytes exceed target length');
  const binaryText = Buffer.from(buildSqlDta([column(909, 4)], ['aBcD']));
  assert.deepEqual([...binaryText.subarray(-5)], [0, 0, 2, 0xab, 0xcd]);
  const fixedBinaryText = Buffer.from(buildSqlDta([column(913, 4)], ['313233']));
  assert.deepEqual([...fixedBinaryText.subarray(-5)], [0, 0x31, 0x32, 0x33, 0]);
  const emptyBinaryText = Buffer.from(buildSqlDta([column(909, 4)], ['']));
  assert.deepEqual([...emptyBinaryText.subarray(-3)], [0, 0, 0]);
  const blobText = buildSqlDtaWithExternalData([column(405, 4)], ['313233']);
  assert.equal(blobText.extDta.length, 1);
  assert.ok(Buffer.from(blobText.extDta[0].ddmObject).includes(Buffer.from([0, 0x31, 0x32, 0x33])));

  const textLobTargets = [column(409, 20), column(413, 20), column(988, 20)];
  for (const target of textLobTargets) {
    for (const value of [true, 123, 1.5, 12.5])
      assert.throws(() => buildSqlDta([target], [value]), TypeError, target.name + ' from ' + typeof value);
    assert.doesNotThrow(() => buildSqlDta([target], ['text']), target.name + ' text');
    assert.doesNotThrow(() => buildSqlDta([target], [new TextEncoder().encode('text')]), target.name + ' bytes');
  }
  const clobText = Buffer.from(buildSqlDta([textLobTargets[0]], [new TextEncoder().encode('text')]));
  assert.ok(clobText.includes(Buffer.from([0x3f, 0x7f, 0xff])));
  assert.deepEqual([...clobText.subarray(-7)], [0, 0, 4, 116, 101, 120, 116]);
  const dbClobText = Buffer.from(buildSqlDta([textLobTargets[1]], [Uint8Array.of(0x31, 0x32, 0x33)]));
  assert.deepEqual([...dbClobText.subarray(-5)], [0, 0, 1, 0x31, 0x32]);
  const xmlText = Buffer.from(buildSqlDta([textLobTargets[2]], [new TextEncoder().encode('<r/>')]));
  assert.ok(xmlText.includes(Buffer.from([0x3f, 0x7f, 0xff])));
  assert.deepEqual([...xmlText.subarray(-7)], [0, 0, 4, 0x3c, 0x72, 0x2f, 0x3e]);
});

test('SQLDTA decimal conversion truncates excess fractional digits and rejects integer overflow', () => {
  const decimal = { name: 'DECIMAL', sqlType: 485, length: 0n, precision: 5, scale: 2, nullable: true };
  const encoded = Buffer.from(buildSqlDta([decimal], ['1.236']));
  assert.ok(encoded.toString('hex').endsWith('000000123c'));
  const noIntegerDigits = { ...decimal, precision: 2, scale: 2 };
  assert.throws(() => buildSqlDta([noIntegerDigits], ['1.25']), RangeError);
  assert.throws(() => buildSqlDta([decimal], ['1x.23']), TypeError);
});

test('XML SQLDTA length uses UTF-16 code units', () => {
  const xml = '<root><message>XML café</message></root>';
  const column = { name: 'XML', sqlType: 988, length: 0n, precision: 0, scale: 0, nullable: true };
  const encoded = Buffer.from(buildSqlDta([column], [xml]));
  const expected = Buffer.concat([Buffer.from([0, xml.length >> 8, xml.length & 0xff]), Buffer.from(xml, 'utf16le').swap16()]);
  assert.ok(encoded.subarray(-expected.length).equals(expected));
});

test('large BLOB parameters use ordered EXTDTA objects and bounded SQLDTA', () => {
  const first = new Uint8Array(70_000).fill(0xa5);
  const second = new Uint8Array(65_537).fill(0x5a);
  const columns = [
    { name: 'BLOB1', sqlType: 405, length: 0n, precision: 0, scale: 0, nullable: true },
    { name: 'INTEGER', sqlType: 497, length: 4n, precision: 0, scale: 0, nullable: true },
    { name: 'BLOB2', sqlType: 405, length: 0n, precision: 0, scale: 0, nullable: true },
  ];
  const encoded = buildSqlDtaWithExternalData(columns, [first, 7, second]);
  assert.ok(encoded.sqlDta.length < 256);
  assert.equal(Buffer.from(encoded.sqlDta).toString('hex').match(/c98009/g)?.length, 2);
  assert.ok(Buffer.from(encoded.sqlDta).includes(Buffer.from([0x00, 0x00, 0x02, 0, 0, 0, 0, 0, 0x01, 0x11, 0x70])));
  assert.ok(Buffer.from(encoded.sqlDta).includes(Buffer.from([0x00, 0x00, 0x02, 0, 0, 0, 0, 0, 0x01, 0x00, 0x01])));
  assert.equal(encoded.extDta.length, 2);
  for (const [index, value] of [first, second].entries()) {
    const object = Buffer.from(encoded.extDta[index].ddmObject);
    assert.deepEqual(object.subarray(0, 4), Buffer.from([0x80, 0x08, 0x14, 0x6c]));
    assert.equal(object.readUInt32BE(4), value.length + 1);
    assert.equal(object[8], 0);
    assert.deepEqual(object.subarray(9), Buffer.from(value));
  }
});

test('small and NULL BLOB parameters use EXTDTA only for non-NULL values', () => {
  const column = { name: 'BLOB', sqlType: 405, length: 0n, precision: 0, scale: 0, nullable: true };
  const encoded = buildSqlDtaWithExternalData([column, column], [Uint8Array.of(0, 0xff, 0x2a), null]);
  assert.equal(encoded.extDta.length, 1);
  assert.match(Buffer.from(encoded.sqlDta).toString('hex'), /c98009/);
  assert.match(Buffer.from(encoded.sqlDta).toString('hex'), /00020000000000000003/);
  assert.deepEqual(Buffer.from(encoded.extDta[0].ddmObject).subarray(4), Buffer.from([0, 0, 0xff, 0x2a]));
});

test('SQLDTA records a streamed BLOB source without consuming it', () => {
  let reads = 0;
  const source = { async *[Symbol.asyncIterator]() { reads++; yield new Uint8Array(80_000); } };
  const column = { name: 'BLOB', sqlType: 405, length: 1_048_576n, precision: 0, scale: 0, nullable: true };
  const encoded = buildSqlDtaWithExternalData([column], [{ source, length: 80_000 }]);
  assert.ok(encoded.sqlDta.length < 256);
  assert.equal(Buffer.from(encoded.sqlDta).toString('hex').match(/c98009/g)?.length, 1);
  assert.ok(Buffer.from(encoded.sqlDta).includes(Buffer.from([0x00, 0x00, 0x02, 0, 0, 0, 0, 0, 0x01, 0x38, 0x80])));
  assert.deepEqual(encoded.extDta, [{ blobStream: { source, length: 80_000 } }]);
  assert.equal(reads, 0);
});

test('SQLDTA uses typed EXTDTA descriptors for streamed CLOB, DBCLOB, and XML inputs', () => {
  let reads = 0;
  const source = { async *[Symbol.asyncIterator]() { reads++; yield new Uint8Array(80_000); } };
  const clob = { name: 'CLOB', sqlType: 409, length: 1_048_576n, precision: 0, scale: 0, nullable: true };
  const dbclob = { name: 'DBCLOB', sqlType: 413, length: 1_048_576n, precision: 0, scale: 0, nullable: true };
  const xml = { name: 'XML', sqlType: 989, length: 1_048_576n, precision: 0, scale: 0, nullable: true };
  const encoded = buildSqlDtaWithExternalData([clob, dbclob, xml], [
    { source, length: 80_000 },
    { source, length: 90_000 },
    { source, length: 90_001 },
  ]);
  const sqlDta = Buffer.from(encoded.sqlDta);
  assert.ok(sqlDta.length < 256);
  assert.ok(sqlDta.includes(Buffer.from([0xcf, 0x80, 0x09])));
  assert.ok(sqlDta.includes(Buffer.from([0xc7, 0x80, 0x09])));
  assert.equal(encoded.extDta.length, 3);
  assert.deepEqual(encoded.extDta, [
    { textLobStream: { source, length: 80_000 } },
    { textLobStream: { source, length: 90_000 } },
    { textLobStream: { source, length: 90_001 } },
  ]);
  assert.equal(reads, 0);
});

test('SQLDTA rejects a streamed BLOB for a non-BLOB parameter and invalid lengths', () => {
  const integer = { name: 'INTEGER', sqlType: 497, length: 4n, precision: 0, scale: 0, nullable: true };
  const blob = { name: 'BLOB', sqlType: 405, length: 100n, precision: 0, scale: 0, nullable: true };
  const source = { async *[Symbol.asyncIterator]() { yield Uint8Array.of(1); } };
  assert.throws(() => buildSqlDtaWithExternalData([integer], [{ source, length: 1 }]), /LOB streams can only be bound/);
  assert.throws(() => buildSqlDtaWithExternalData([blob], [{ source, length: -1 }]), RangeError);
});

for (const sqlType of [409, 413, 989]) {
  test(`materialized text LOB ${sqlType} switches to UTF-8 EXTDTA above inline boundary`, () => {
    const column = { name: 'L', sqlType, length: 2000000n, precision: 0, scale: 0, nullable: true };
    for (const text of ['', 'x'.repeat(65535), '東京🚀' + 'x'.repeat(65536), null, 'small']) {
      const encoded = buildSqlDtaWithExternalData([column], [text]);
      assert.equal(encoded.extDta.length, text !== null && text.length > 65535 ? 1 : 0);
      if (encoded.extDta.length) {
        const bytes = Buffer.from(text, 'utf8');
        const ddm = Buffer.from(encoded.extDta[0].ddmObject);
        assert.deepEqual(ddm.subarray(ddm.length - bytes.length), bytes);
        assert.ok(encoded.sqlDta.length < 256);
      }
    }
  });
}
