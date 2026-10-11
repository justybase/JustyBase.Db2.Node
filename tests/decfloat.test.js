const test = require('node:test');
const assert = require('node:assert/strict');
const { decodeDecfloat, encodeDecfloat } = require('../dist/cjs/converters/decfloat.js');

const vectors = [
  [8, '0', '2238000000000000', '0'],
  [8, '123.456', '222c000000028e56', '123.456'],
  [8, '-987.654', 'a22c0000000e3f54', '-987.654'],
  [8, '1000', '2238000000000400', '1000'],
  [8, '0.000123', '22200000000000a3', '0.000123'],
  [8, '1.23E+2', '22380000000000a3', '123'],
  [8, '-Infinity', 'f800000000000000', '-Infinity'],
  [8, 'NaN', '7c00000000000000', 'NaN'],
  [8, 'sNaN', '7e00000000000000', 'sNaN'],
  [16, '0', '22080000000000000000000000000000', '0'],
  [16, '12345678901234567890.12345678901234', '2604934b9c1e28e56f3c127177823534', '12345678901234567890.12345678901234'],
  [16, '-98765432109876543210.98765432109876', 'ee04b7cb0d10e3f546845ef96322277c', '-98765432109876543210.98765432109876'],
  [16, '9.87654321E+20', '220b0000000000000000000038fd51a1', '9.87654321E+20'],
  [16, 'Infinity', '78000000000000000000000000000000', 'Infinity'],
  [16, 'NaN', '7c000000000000000000000000000000', 'NaN'],
  [16, 'sNaN', '7e000000000000000000000000000000', 'sNaN'],
];

test('DECFLOAT encoding matches Go DPD vectors and decodes them canonically', () => {
  for (const [length, input, expectedHex, expectedDecoded] of vectors) {
    const encoded = encodeDecfloat(input, length);
    assert.equal(Buffer.from(encoded).toString('hex'), expectedHex, `${input} (${length} bytes)`);
    assert.equal(decodeDecfloat(encoded), expectedDecoded, `${input} (${length} bytes)`);
  }
});

test('DECFLOAT encoding rejects malformed values, invalid widths, and out-of-range exponents', () => {
  assert.throws(() => encodeDecfloat('12x', 8), /invalid decimal floating-point value/);
  assert.throws(() => encodeDecfloat('1E+1000', 8), /exponent is out of range/);
  assert.throws(() => encodeDecfloat('1', 12), /invalid DFP byte length/);
});
