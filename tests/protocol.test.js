const test = require('node:test');
const assert = require('node:assert/strict');
const { CodePoint } = require('../dist/cjs/network/codepoint.js');
const { decodeCp500, encodeCp500 } = require('../dist/cjs/converters/ebcdic.js');
const ddm = require('../dist/cjs/network/ddm.js');
const dss = require('../dist/cjs/network/dss.js');
const { buildSqlDta } = require('../dist/cjs/converters/sqlDta.js');
const sqlTypes = require('../dist/cjs/types/sqltype.js');
const { applyOutputParameterReplyForTest, ensureOutputParameterDataReturnedForTest, hasUnresolvedExternalLocator, stitchOutputExtdtaForTest } = require('../dist/cjs/network/session.js');
const { Db2ServerError } = require('../dist/cjs/errors.js');

class MemoryReader {
  constructor(bytes) { this.bytes = bytes; this.offset = 0; }
  async readExact(length) {
    if (this.offset + length > this.bytes.length) throw new Error('unexpected EOF');
    const result = this.bytes.subarray(this.offset, this.offset + length);
    this.offset += length;
    return result;
  }
}

function ddmObjectWithExtendedLength(codePoint, data, extensionBytes = 0) {
  const headerExtension = extensionBytes === 0 ? 0 : extensionBytes;
  const result = new Uint8Array(4 + headerExtension + data.length);
  const view = new DataView(result.buffer);
  view.setUint16(0, 0x8000 | (4 + headerExtension), false);
  view.setUint16(2, codePoint, false);
  if (extensionBytes === 4) view.setUint32(4, data.length, false);
  if (extensionBytes === 8) view.setBigUint64(4, BigInt(data.length), false);
  result.set(data, 4 + headerExtension);
  return result;
}

function ddmCodePoints(data) {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const points = [];
  let offset = 4;
  while (offset < data.length) {
    const length = view.getUint16(offset, false);
    assert.ok(length >= 4 && offset + length <= data.length, `invalid child DDM length ${length}`);
    points.push(view.getUint16(offset + 2, false));
    offset += length;
  }
  return points;
}

test('CP500 round trips SQL and rejects unsupported characters', () => {
  for (const value of ['pydrda', 'go-db2', 'TESTDB', 'USER123', 'localhost', 'SELECT 1 FROM SYSIBM.SYSDUMMY1'])
    assert.equal(decodeCp500(encodeCp500(value)), value);
  assert.throws(() => encodeCp500('😀'), /cannot be encoded/);
});

test('interrupt request uses INTRDBRQS and the exact RDB interrupt token', () => {
  assert.equal(CodePoint.INTRDBRQS, 0x2003);
  assert.equal(CodePoint.DRPPKG, 0x2007);
  assert.deepEqual([...ddm.packInterruptRdbRequest(Uint8Array.of(0x04, 0x1c))],
    [0x00, 0x0a, 0x20, 0x03, 0x00, 0x06, 0x21, 0x03, 0x04, 0x1c]);
});

test('progressive LOB chunks encode GETNXTCHK and validate EXTDTA and ENDDTARM', () => {
  assert.equal(CodePoint.GETNXTCHK, 0x2015);
  assert.equal(CodePoint.GETNXTLEN, 0x214e);
  assert.equal(CodePoint.GETNXTREF, 0x214f);
  assert.equal(CodePoint.ENDDTARM, 0x2214);
  assert.equal(CodePoint.ENDREF, 0x2163);
  assert.equal(CodePoint.LSTCHK, 0x2164);

  const request = ddm.packGetNextChunk('SQLC0X0', '', 2, 'TESTDB', 0x0102030405060708n, 0x1122334455667788n, 0x7fffffffn);
  assert.equal(new DataView(request.buffer, request.byteOffset + 2, 2).getUint16(0, false), CodePoint.GETNXTCHK);
  const params = ddm.parseDdmParameters(request.subarray(4));
  const getBigUint64 = (bytes) => new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getBigUint64(0, false);
  assert.equal(params.get(CodePoint.PKGNAMCSN).length, 64);
  assert.equal(getBigUint64(params.get(CodePoint.QRYINSID)), 0x0102030405060708n);
  assert.equal(getBigUint64(params.get(CodePoint.GETNXTREF)), 0x1122334455667788n);
  assert.equal(getBigUint64(params.get(CodePoint.GETNXTLEN)), 0x7fffffffn);

  const intermediate = ddm.parseProgressiveChunkReply([
    { codePoint: CodePoint.EXTDTA, payload: Uint8Array.of(0, 1, 2, 3) },
    { codePoint: CodePoint.ENDDTARM, payload: join(ddm.packUint16(CodePoint.SVRCOD, 0), ddm.packString(CodePoint.RDBNAM, 'TESTDB')) },
  ]);
  assert.deepEqual([...intermediate.data], [1, 2, 3]);
  assert.equal(intermediate.endOfReference, false);
  assert.equal(intermediate.isLastChunk, false);

  const final = ddm.parseProgressiveChunkReply([
    { codePoint: CodePoint.EXTDTA, payload: Uint8Array.of(0, 4, 5) },
    { codePoint: CodePoint.ENDDTARM, payload: join(ddm.packUint16(CodePoint.SVRCOD, 0), ddm.packBytes(CodePoint.ENDREF, Uint8Array.of(0xf1)), ddm.packBytes(CodePoint.LSTCHK, Uint8Array.of(0xf1))) },
  ]);
  assert.deepEqual([...final.data], [4, 5]);
  assert.equal(final.endOfReference, true);
  assert.equal(final.isLastChunk, true);
  assert.throws(() => ddm.parseProgressiveChunkReply([]), /did not contain ENDDTARM/);
  assert.throws(() => ddm.parseProgressiveChunkReply([
    { codePoint: CodePoint.ENDDTARM, payload: ddm.packBytes(CodePoint.ENDREF, Uint8Array.of(0xf1)) },
  ]), /SVRCOD/);
});

test('progressive LOB reply parser rejects malformed boundaries and preserves empty terminal chunks', () => {
  const packet = (codePoint, payload) => ({ codePoint, payload: Uint8Array.from(payload) });
  const success = ddm.packUint16(CodePoint.SVRCOD, 0);
  const end = (...fields) => packet(CodePoint.ENDDTARM, join(...fields));
  const reject = (message, ...replies) => assert.throws(
    () => ddm.parseProgressiveChunkReply(replies),
    new RegExp(message),
  );

  reject('did not contain ENDDTARM');
  reject('invalid EXTDTA status byte', packet(CodePoint.EXTDTA, []), end(success));
  reject('invalid EXTDTA status byte', packet(CodePoint.EXTDTA, [1, 0xaa]), end(success));
  reject('omitted its two-byte SVRCOD', end());
  reject('omitted its two-byte SVRCOD', end(ddm.packBytes(CodePoint.SVRCOD, Uint8Array.of(0))));
  reject('returned SVRCOD 8', end(ddm.packUint16(CodePoint.SVRCOD, 8)));
  reject('multiple ENDDTARM', end(success), end(success));
  reject('EXTDTA after ENDDTARM', end(success), packet(CodePoint.EXTDTA, [0, 0x11]));
  reject('ENDREF Boolean value', end(success, ddm.packBytes(CodePoint.ENDREF, Uint8Array.of(1))));
  reject('LSTCHK Boolean value', end(success, ddm.packBytes(CodePoint.LSTCHK, Uint8Array.of(0xf2))));

  const emptyIntermediate = ddm.parseProgressiveChunkReply([
    packet(CodePoint.EXTDTA, [0]), end(success),
  ]);
  assert.deepEqual([...emptyIntermediate.data], []);
  assert.equal(emptyIntermediate.endOfReference, false);
  assert.equal(emptyIntermediate.isLastChunk, false);

  const emptyFinal = ddm.parseProgressiveChunkReply([
    packet(CodePoint.EXTDTA, [0]), end(success,
      ddm.packBytes(CodePoint.ENDREF, Uint8Array.of(0xf1)), ddm.packBytes(CodePoint.LSTCHK, Uint8Array.of(0xf1))),
  ]);
  assert.deepEqual([...emptyFinal.data], []);
  assert.equal(emptyFinal.endOfReference, true);
  assert.equal(emptyFinal.isLastChunk, true);
});

function join(...parts) {
  const result = new Uint8Array(parts.reduce((length, part) => length + part.length, 0));
  let offset = 0;
  for (const part of parts) { result.set(part, offset); offset += part.length; }
  return result;
}

test('query open requests dynamic LOB references now that chunk fetch is implemented', () => {
  const direct = ddm.packOpenQuery('SQLC0X0', '', 1, 'TESTDB', 32767);
  const parameterized = ddm.packOpenQueryWithParams('SQLC0X0', '', 1, 'TESTDB', 32767);
  assert.deepEqual(ddmCodePoints(parameterized), [...ddmCodePoints(direct)]);
  assert.equal(ddmCodePoints(parameterized).at(-1), CodePoint.DYNDTAFMT);
  assert.deepEqual([...ddm.parseDdmParameters(parameterized.subarray(4)).get(CodePoint.DYNDTAFMT)], [0xf1]);
});

test('procedure result-set requests advertise result capacity and continue with matching rowset state', () => {
  const execute = ddm.packExecuteSqlStatementForResultSets('SQLC0X0', '', 0, 'TESTDB');
  const executeParams = ddm.parseDdmParameters(execute.subarray(4 + new DataView(execute.buffer, execute.byteOffset + 4, 2).getUint16(0, false)));
  const getU16 = (bytes) => new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint16(0, false);
  const getU32 = (bytes) => new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, false);
  assert.deepEqual([...executeParams.get(CodePoint.OUTEXP)], [0xf0]);
  assert.equal(getU32(executeParams.get(CodePoint.QRYBLKSZ)), 32767);
  assert.equal(getU16(executeParams.get(CodePoint.MAXBLKEXT)), 0xffff);
  assert.equal(getU16(executeParams.get(CodePoint.MAXRSLCNT)), 0xffff);
  assert.equal(executeParams.get(CodePoint.RSLSETFLG)[0], 0x04);
  assert.equal(getU32(executeParams.get(CodePoint.QRYROWSET)), 64);
  assert.deepEqual(ddmCodePoints(execute), [CodePoint.PKGNAMCSN, CodePoint.RDBCMTOK, CodePoint.OUTEXP, CodePoint.QRYBLKSZ, CodePoint.MAXBLKEXT, CodePoint.MAXRSLCNT, CodePoint.RSLSETFLG, CodePoint.QRYROWSET]);

  const section = { database: new TextEncoder().encode('TESTDB'), schema: new TextEncoder().encode('NULLID'), packageName: new TextEncoder().encode('SQLC0X0'), consistencyToken: new Uint8Array(8).fill(1), sectionNumber: 2 };
  const continueQuery = ddm.packContinueResultSetQuery(section, 32767, 19n);
  const params = ddm.parseDdmParameters(continueQuery.subarray(4));
  assert.equal(new DataView(continueQuery.buffer, continueQuery.byteOffset + 2, 2).getUint16(0, false), CodePoint.CNTQRY);
  assert.equal(getU32(params.get(CodePoint.QRYBLKSZ)), 32767);
  assert.equal(getU16(params.get(CodePoint.MAXBLKEXT)), 0xffff);
  assert.equal(new DataView(params.get(CodePoint.QRYINSID).buffer, params.get(CodePoint.QRYINSID).byteOffset, 8).getBigUint64(0, false), 19n);
  assert.equal(getU32(params.get(CodePoint.QRYROWSET)), 64);
  assert.equal(params.has(CodePoint.RTNEXTDTA), false);
  assert.deepEqual(ddmCodePoints(continueQuery), [CodePoint.PKGNAMCSN, CodePoint.QRYBLKSZ, CodePoint.MAXBLKEXT, CodePoint.QRYINSID, CodePoint.QRYROWSET]);
  const allData = ddm.parseDdmParameters(ddm.packContinueResultSetQuery(section, 32767, 19n, 64, true).subarray(4));
  assert.deepEqual([...allData.get(CodePoint.RTNEXTDTA)], [0x02]);

  const procedureFetch = ddm.packContinueProcedureResultSet(section, 32767, 19n);
  const procedureParams = ddm.parseDdmParameters(procedureFetch.subarray(4));
  assert.equal(getU32(procedureParams.get(CodePoint.QRYBLKSZ)), 32767);
  assert.equal(new DataView(procedureParams.get(CodePoint.QRYINSID).buffer, procedureParams.get(CodePoint.QRYINSID).byteOffset, 8).getBigUint64(0, false), 19n);
  assert.deepEqual([...procedureParams.get(CodePoint.RTNEXTDTA)], [0x02]);
  assert.deepEqual([...procedureParams.get(CodePoint.FREPRVREF)], [0xf0]);
  assert.deepEqual(ddmCodePoints(procedureFetch), [CodePoint.PKGNAMCSN, CodePoint.QRYBLKSZ, CodePoint.QRYINSID, CodePoint.RTNEXTDTA, CodePoint.FREPRVREF]);

  const procedureClose = ddm.packCloseProcedureResultSet(section, 19n);
  assert.deepEqual(ddmCodePoints(procedureClose), [CodePoint.PKGNAMCSN, CodePoint.QRYINSID]);
  assert.equal(new DataView(procedureClose.buffer, procedureClose.byteOffset + 2, 2).getUint16(0, false), CodePoint.CLSQRY);
});

test('procedure result-set summaries preserve package sections and validate row counts', () => {
  const first = ddm.packPackageNameCsn('TESTDB', 'SQLC0X0', '', 1);
  const second = ddm.packPackageNameCsn('TESTDB', 'SQLC0X0', '', 2);
  const packageList = ddm.packDdmObject(CodePoint.PKGSNLST, Uint8Array.from([...first, ...second]));
  const sections = ddm.parseResultSetSections(packageList);
  assert.deepEqual(sections.map(section => section.sectionNumber), [1, 2]);
  assert.equal(new TextDecoder().decode(sections[0].packageName).trim(), 'SQLC0X0');
  assert.equal(ddm.parseSqlResultSetCount(Uint8Array.of(2, 0, ...new Uint8Array(24))), 2);
  assert.throws(() => ddm.parseSqlResultSetCount(Uint8Array.of(2, 0)), /truncated/);
});

test('SQLCINRD result cursor metadata parses nullable fields and names', () => {
  const bytes = [0xff, 1, 0, 10, 0, 0, 0, 4, ...new Array(7).fill(0), 0xf1, 1, 0, 0, 0, 0, 0];
  const stringPair = (value) => bytes.push(0, 0, value.length >> 8, value.length & 0xff, ...Buffer.from(value));
  stringPair('VALUE'); stringPair('VALUE'); stringPair(''); bytes.push(0xff, 0xff);
  const [column] = ddm.parseSqlCursorInfo(Uint8Array.from(bytes));
  assert.equal(column.name, 'VALUE');
  assert.equal(column.sqlType, 0x01f1);
  assert.equal(column.nullable, true);
  assert.equal(column.length, 4n);
  assert.equal(column.precision, 10);
});

test('SQLCINRD parses Db2 LUW 32-bit SQLDHGRP string lengths', () => {
  const bytes = [0x00, ...new Array(12).fill(0)];
  const string32 = (value) => {
    const encoded = Buffer.from(value);
    bytes.push((encoded.length >>> 24) & 0xff, (encoded.length >>> 16) & 0xff, (encoded.length >>> 8) & 0xff, encoded.length & 0xff, ...encoded);
  };
  string32('DB');
  string32(''); string32('APP');
  bytes.push(1, 0); // SQLNUMROW, little endian
  bytes.push(10, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0xf1, 1, 0, 0, 0xff);
  const [column] = ddm.parseSqlCursorInfo(Uint8Array.from(bytes));
  assert.equal(column.name, 'COLUMN1');
  assert.equal(column.sqlType, 0x01f1);
  assert.equal(column.length, 4n);
});

test('SQLCINRD legacy extension fallback keeps 32-bit SQLDHGRP alignment', () => {
  const bytes = [0x00, ...new Array(12).fill(0)];
  const string32 = (value) => {
    const encoded = Buffer.from(value);
    bytes.push(0, 0, 0, encoded.length, ...encoded);
  };
  string32('DB');
  string32(''); string32('APP');
  bytes.push(1, 0); // SQLNUMROW
  bytes.push(10, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0xf1, 1, 0, 0);
  bytes.push(0xff, 0xff, 0xff, 0x91, 0x92); // opaque SQLDA extensions
  const payload = Uint8Array.from(bytes);
  assert.equal(payload.length, 53);
  const [column] = ddm.parseSqlCursorInfo(payload);
  assert.equal(column.sqlType, 0x01f1);
  assert.equal(column.length, 4n);
});

test('timestamp scale is recovered from SQLLENGTH for query and procedure metadata', () => {
  for (const [length, expectedScale] of [[19n, 0], [23n, 3], [26n, 6], [32n, 12]]) {
    const sqlvar = new Uint8Array(16);
    const sqlvarView = new DataView(sqlvar.buffer);
    sqlvarView.setBigUint64(4, length, true);
    sqlvarView.setUint16(12, 393, true); // Nullable TIMESTAMP.
    const dard = Uint8Array.from([0xff, 0xff, 1, 0, ...sqlvar]);
    const [queryColumn] = ddm.parseSqlDard(dard);
    assert.equal(queryColumn.scale, expectedScale);

    const cursorInfo = Uint8Array.from([0xff, 1, 0, ...sqlvar, 0xff]);
    const [procedureColumn] = ddm.parseSqlCursorInfo(cursorInfo);
    assert.equal(procedureColumn.scale, expectedScale);
  }
});

test('DDM builders produce parseable EXCSAT, ACCSEC and PKGNAMCSN objects', () => {
  const excsat = ddm.packExcsat('ts-db2', 'myhost', 'ts-db2-v0.1', 'ts-db2');
  const view = new DataView(excsat.buffer, excsat.byteOffset, excsat.byteLength);
  assert.equal(view.getUint16(0), excsat.length);
  assert.equal(view.getUint16(2), CodePoint.EXCSAT);
  const fields = ddm.parseDdmParameters(excsat.subarray(4));
  assert.equal(decodeCp500(fields.get(CodePoint.EXTNAM)), 'ts-db2');
  assert.equal(decodeCp500(fields.get(CodePoint.SRVNAM)), 'myhost');
  assert.equal(fields.get(CodePoint.MGRLVLLS).length, ddm.defaultManagerLevels().length * 4);

  const accsec = ddm.parseDdmParameters(ddm.packAccsec('TESTDB', 3, Uint8Array.of(1, 2)).subarray(4));
  assert.equal(new DataView(accsec.get(CodePoint.SECMEC).buffer, accsec.get(CodePoint.SECMEC).byteOffset, 2).getUint16(0), 3);
  assert.equal(decodeCp500(accsec.get(CodePoint.RDBNAM)), 'TESTDB');
  assert.deepEqual([...accsec.get(CodePoint.SECTKN)], [1, 2]);

  const usrenpwdAccsec = ddm.parseDdmParameters(ddm.packAccsec('TESTDB', 7, Uint8Array.of(3, 4), 1).subarray(4));
  assert.equal(new DataView(usrenpwdAccsec.get(CodePoint.SECMEC).buffer, usrenpwdAccsec.get(CodePoint.SECMEC).byteOffset, 2).getUint16(0), 7);
  assert.equal(new DataView(usrenpwdAccsec.get(CodePoint.ENCALG).buffer, usrenpwdAccsec.get(CodePoint.ENCALG).byteOffset, 2).getUint16(0), 1);

  const clearUser = 'db2inst1';
  const ciphertext = Uint8Array.of(0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80);
  const usrencpwd = ddm.parseDdmParameters(ddm.packSecchkUsrencpwd('TESTDB', clearUser, ciphertext).subarray(4));
  assert.equal(new DataView(usrencpwd.get(CodePoint.SECMEC).buffer, usrencpwd.get(CodePoint.SECMEC).byteOffset, 2).getUint16(0), 7);
  assert.equal(decodeCp500(usrencpwd.get(CodePoint.USRID)), clearUser);
  // Db2 LUW requires the encrypted password in SECTKN; PASSWORD is rejected (SECCHKCD=16).
  assert.deepEqual([...usrencpwd.get(CodePoint.SECTKN)], [...ciphertext]);
  assert.equal(usrencpwd.has(CodePoint.PASSWORD), false);
  assert.equal(usrencpwd.has(CodePoint.ENCALG), false);

  const userOnly = ddm.parseDdmParameters(ddm.packSecchk(4, new Uint8Array(), 'TESTDB', 'db2inst1', new TextEncoder().encode('must-not-be-sent'), false, 'cp500').subarray(4));
  assert.equal(new DataView(userOnly.get(CodePoint.SECMEC).buffer, userOnly.get(CodePoint.SECMEC).byteOffset, 2).getUint16(0, false), 4);
  assert.equal(decodeCp500(userOnly.get(CodePoint.USRID)), 'db2inst1');
  assert.equal(userOnly.has(CodePoint.PASSWORD), false);
  assert.throws(() => ddm.packSecchk(9, new Uint8Array(), 'TESTDB', 'db2inst1', new TextEncoder().encode('must-not-be-sent'), false, 'cp500'), /Only USRIDPWD/);

  const packageName = ddm.packPackageNameCsn('TESTDB', 'SQLC0X0', '', 0);
  assert.equal(new DataView(packageName.buffer).getUint16(0), packageName.length);
  assert.equal(new DataView(packageName.buffer).getUint16(2), CodePoint.PKGNAMCSN);
  assert.equal(packageName.length, 4 + 18 + 18 + 18 + 8 + 2);
});

test('DSS framing handles correlation flags, DDM objects, continuation segments and extended lengths', async () => {
  const small = ddm.packDdmObject(CodePoint.EXCSAT, Uint8Array.of(0xde, 0xad, 0xbe, 0xef));
  const request = dss.encodeRequestDss(small, 1, false, true);
  assert.equal(request.nextId, 2);
  assert.equal(request.bytes[2], dss.DSS_MAGIC);
  const decoded = await dss.readDss(new MemoryReader(request.bytes));
  assert.equal(decoded.header.type, dss.DSS_TYPE_REQUEST);
  assert.equal(decoded.header.correlationId, 1);
  assert.equal(decoded.codePoint, CodePoint.EXCSAT);
  assert.deepEqual([...decoded.payload], [0xde, 0xad, 0xbe, 0xef]);

  const longPayload = new TextEncoder().encode('abcdefghij'.repeat(20_000));
  for (const extensionBytes of [0, 4, 8]) {
    const packet = dss.encodeReplyDss(ddmObjectWithExtendedLength(CodePoint.EXTDTA, longPayload, extensionBytes), true, 7);
    const reader = new MemoryReader(packet);
    const first = await dss.readDss(reader);
    assert.equal(first.header.chained, true);
    assert.equal(first.header.correlationId, 7);
    assert.equal(first.codePoint, CodePoint.EXTDTA);
    assert.deepEqual(first.payload, longPayload);
  }
});

test('DSS detects object DSS type, preserves same-id flag, and rejects malformed frames', async () => {
  const payload = ddm.packSqlStatement('SELECT 1');
  const encoded = dss.encodeRequestDss(payload, 9, true, false);
  const packet = await dss.readDss(new MemoryReader(encoded.bytes));
  assert.equal(packet.header.type, dss.DSS_TYPE_OBJECT);
  assert.equal(packet.header.chained, true);
  assert.equal(packet.header.sameId, true);
  assert.equal(packet.codePoint, CodePoint.SQLSTT);
  await assert.rejects(dss.readDss(new MemoryReader(Uint8Array.of(0, 10, 0xaa, 1, 0, 1))), /0xD0/);
  await assert.rejects(dss.readDss(new MemoryReader(Uint8Array.of(0, 4, 0xd0, 1, 0, 1))), /less than header/);
  assert.throws(() => dss.readDdmObject(Uint8Array.of(0, 2, 0x24, 0x12)), /invalid DDM object length/);
});

test('streamed BLOB DSS frames match materialized EXTDTA bytes across DDM and DSS boundaries', async () => {
  for (const size of [0, 32_762, 32_763, 32_767, 32_768, 70_003]) {
    const bytes = Uint8Array.from({ length: size }, (_, index) => index % 251);
    const writes = [];
    const destination = { write(chunk, callback) { writes.push(Buffer.from(chunk)); callback(); return true; } };
    async function* source() {
      for (let offset = 0; offset < bytes.length; offset += 4_003) yield bytes.subarray(offset, offset + 4_003);
    }
    const nextId = await dss.writeExternalBlobDss(destination, source(), size, 19, false, true);
    const expectedDdm = ddm.packDdmObject(CodePoint.EXTDTA, Uint8Array.from([0, ...bytes]));
    const expected = dss.encodeRequestDss(expectedDdm, 19, false, true);
    assert.deepEqual(Buffer.concat(writes), Buffer.from(expected.bytes), `BLOB length ${size}`);
    assert.equal(nextId, expected.nextId);
  }
});

test('streamed BLOB framing fails closed for early EOF, respects the declared length, and closes owned sources', async () => {
  const writes = [];
  const destination = { write(chunk, callback) { writes.push(Buffer.from(chunk)); callback(); return true; } };
  const shortDestination = { write(_chunk, callback) { callback(); return true; } };
  async function* shortSource() { yield Uint8Array.of(1, 2); }
  async function* longSource() { yield Uint8Array.of(1, 2, 3); }
  await assert.rejects(dss.writeExternalBlobDss(shortDestination, shortSource(), 3, 1, false, true), /ended before its declared length/);
  await dss.writeExternalBlobDss(destination, longSource(), 2, 1, false, true);
  const expected = dss.encodeRequestDss(ddm.packDdmObject(CodePoint.EXTDTA, Uint8Array.of(0, 1, 2)), 1, false, true);
  assert.deepEqual(Buffer.concat(writes), Buffer.from(expected.bytes));

  let returned = false;
  let yielded = false;
  const ownedSource = { [Symbol.asyncIterator]() { return { async next() { if (yielded) return { done: true }; yielded = true; return { value: Uint8Array.of(1), done: false }; }, async return() { returned = true; return { done: true }; } }; } };
  await assert.rejects(dss.writeExternalBlobDss(destination, ownedSource, 2, 1, false, true, false), /ended before its declared length/);
  assert.equal(returned, true);

  let emptyReturned = false;
  const ownedEmptySource = { [Symbol.asyncIterator]() { return { async next() { throw new Error('empty input must not be read'); }, async return() { emptyReturned = true; return { done: true }; } }; } };
  await dss.writeExternalBlobDss(destination, ownedEmptySource, 0, 1, false, true, false);
  assert.equal(emptyReturned, true);
});

test('SQLCARD and QRYDSC decoding preserve warnings, errors, and grouped descriptors', () => {
  const card = new Uint8Array(54); card[0] = 0; new DataView(card.buffer).setInt32(1, 0, true);
  card.set(new TextEncoder().encode('00000'), 5); new DataView(card.buffer).setInt32(27, 12, true);
  assert.deepEqual(ddm.parseSqlCard(card), { sqlCode: 0, sqlState: '00000', message: '', rowsAffected: 12, warningFlags: '\0'.repeat(11) });

  const warningCard = new Uint8Array(54); warningCard[0] = 0;
  warningCard.set(new TextEncoder().encode('01003'), 5);
  warningCard[18] = 0;
  warningCard.set(new TextEncoder().encode('W W        '), 43);
  const parsedWarning = ddm.parseSqlCard(warningCard);
  assert.equal(parsedWarning.warningFlags, 'W W        ');
  assert.equal(ddm.sqlCardWarning(parsedWarning).sqlState, '01003');

  const failedCard = new Uint8Array(54); failedCard[0] = 0;
  new DataView(failedCard.buffer).setInt32(1, -911, true);
  failedCard.set(new TextEncoder().encode('40001'), 5);
  assert.throws(() => ddm.parseSqlDard(failedCard), (error) => {
    assert.ok(error instanceof Db2ServerError);
    assert.equal(error.sqlCode, -911);
    assert.equal(error.sqlState, '40001');
    return true;
  });

  const fields = Uint8Array.of(9, 0x76, 0xd0, 0x80, 0x01, 0x02, 0x81, 0x03, 0x04, 6, 0x76, 0xd0, 0x82, 0x05, 0x06);
  const parsed = ddm.parseQueryDescriptor(fields);
  assert.deepEqual(parsed.map((item) => item.type), [0x80, 0x81, 0x82]);
  assert.deepEqual([...parsed[2].ps], [5, 6]);
});

test('QRYDSC follows CPT continuations and preserves every descriptor field', () => {
  for (const count of [83, 84, 85, 169, 170]) {
    const bytes = [];
    for (let start = 0; start < count; start += 84) {
      const groupCount = Math.min(84, count - start);
      bytes.push((groupCount + 1) * 3, start === 0 ? 0x76 : 0x7f, start === 0 ? 0xd0 : 0);
      for (let index = start; index < start + groupCount; index++)
        bytes.push(index & 0xff, (index >> 8) & 0xff, 255 - (index % 256));
    }
    const parsed = ddm.parseQueryDescriptor(Uint8Array.from(bytes));
    assert.equal(parsed.length, count);
    for (let index = 0; index < count; index++) {
      assert.equal(parsed[index].type, index & 0xff);
      assert.deepEqual([...parsed[index].ps], [(index >> 8) & 0xff, 255 - (index % 256)]);
    }
  }
  assert.throws(() => ddm.parseQueryDescriptor(Uint8Array.of(6, 0x7f, 0, 1, 2, 3)), /no preceding NGDA/);
});

test('SQLDTARD extracts output parameter descriptors and data', () => {
  const descriptor = Uint8Array.of(9, 0x76, 0xd0, 0x04, 0, 2, 0x03, 0, 4);
  const values = Uint8Array.of(0xff, 0, 0x2a, 0, 0, 0, 0x64, 0, 0, 0);
  const payload = new Uint8Array([
    ...ddm.packDdmObject(CodePoint.FDODSC, descriptor),
    ...ddm.packDdmObject(CodePoint.FDODTA, values),
  ]);
  const parsed = ddm.parseSqlDtaReply(payload);
  assert.deepEqual(parsed.fields.map(field => field.type), [0x04, 0x03]);
  assert.deepEqual([...parsed.values], [...values]);
  assert.throws(() => ddm.parseSqlDtaReply(Uint8Array.of(0, 2, 0x24, 0x13)), /invalid DDM parameter length/);
});

test('output parameter reply rejects rows shorter than the requested positions', () => {
  const descriptor = Uint8Array.of(9, 0x76, 0xd0, 0x04, 0, 2, 0x03, 0, 4);
  const values = Uint8Array.of(0xff, 0, 0x2a, 0, 0, 0, 0x64, 0, 0, 0);
  const payload = new Uint8Array([
    ...ddm.packDdmObject(CodePoint.FDODSC, descriptor),
    ...ddm.packDdmObject(CodePoint.FDODTA, values),
  ]);

  // The value row carries two values, so two output positions are complete.
  const covered = applyOutputParameterReplyForTest(payload, ['output', 'output']);
  assert.equal(covered.length, 2);

  // Requesting a third position must fail instead of silently reporting the missing output as null.
  assert.throws(
    () => applyOutputParameterReplyForTest(payload, ['output', 'output', 'output']),
    /position 2 was requested as an output/,
  );
});

test('empty SQLDTARD data is rejected when output parameters were requested', () => {
  assert.throws(
    () => applyOutputParameterReplyForTest(new Uint8Array(), ['output']),
    /no SQLDTARD values for requested output parameter at position 0/,
  );

  const descriptor = Uint8Array.of(6, 0x76, 0xd0, 0x03, 0, 4);
  const rowWithoutValues = new Uint8Array([
    ...ddm.packDdmObject(CodePoint.FDODSC, descriptor),
    ...ddm.packDdmObject(CodePoint.FDODTA, Uint8Array.of(0xff, 0)),
  ]);
  assert.throws(
    () => applyOutputParameterReplyForTest(rowWithoutValues, ['output']),
    /no SQLDTARD values for requested output parameter at position 0/,
  );

  assert.deepEqual(applyOutputParameterReplyForTest(new Uint8Array(), ['input']), [null]);
});

test('procedure calls reject a missing SQLDTARD reply for requested outputs', () => {
  assert.throws(
    () => ensureOutputParameterDataReturnedForTest(['input', 'inputOutput'], false),
    /no SQLDTARD values for requested output parameter at position 1/,
  );
  assert.doesNotThrow(() => ensureOutputParameterDataReturnedForTest(['output'], true));
  assert.doesNotThrow(() => ensureOutputParameterDataReturnedForTest(['input'], false));
});

test('SQLDTA describes nullable VARBINARY with its binary type and declared length', () => {
  const encoded = buildSqlDta([
    { name: 'B', sqlType: sqlTypes.SqlType.NVarBinary, length: 10n, precision: 0, scale: 0, nullable: true },
  ], [null]);
  assert.ok(Buffer.from(encoded).includes(Buffer.from([0xc3, 0, 10])));
});

test('SQL type identifiers preserve Db2 nullability and base types', () => {
  assert.equal(sqlTypes.isNullableSqlType(sqlTypes.SqlType.NDecimal), true);
  assert.equal(sqlTypes.baseSqlType(sqlTypes.SqlType.NDecimal), sqlTypes.SqlType.Decimal);
  assert.equal(sqlTypes.sqlTypeName(sqlTypes.SqlType.NDecimal), 'DECIMAL (NULLABLE)');
  assert.equal(sqlTypes.sqlTypeName(sqlTypes.SqlType.VarChar), 'VARCHAR');
});

test('missing output EXTDTA leaves an unresolved LOB locator', () => {
  const locator = (length) => {
    const result = new Uint8Array(9);
    result[0] = 0x02;
    new DataView(result.buffer).setBigUint64(1, BigInt(length), false);
    return result;
  };
  const external = (value) => Uint8Array.from(value);
  const fields = [
    { type: 0xc9, ps: Uint8Array.of(0x80, 9) },
    { type: 0xcf, ps: Uint8Array.of(0x80, 9) },
  ];

  const complete = [[locator(2), locator(1)]];
  const completeResult = stitchOutputExtdtaForTest(fields, complete, [external([0, 0xaa]), external([0, 0x43])]);
  assert.equal(completeResult.used, 2);
  assert.equal(completeResult.unresolved, false);
  // c9 is the nullable BLOB descriptor, so its external value carries the
  // 0x00 nullable indicator and the single data byte remains 0xaa.
  assert.deepEqual([...complete[0][0]], [0xaa]);
  assert.equal(complete[0][1], 'C');
  assert.equal(hasUnresolvedExternalLocator(fields, complete), false);

  const missing = [[locator(2), locator(1)]];
  const missingResult = stitchOutputExtdtaForTest(fields, missing, [external([0, 0xaa])]);
  assert.equal(missingResult.used, 1);
  assert.equal(missingResult.unresolved, true);
  assert.equal(hasUnresolvedExternalLocator(fields, missing), true);

  const emptyLocator = [[locator(0), null]];
  const emptyResult = stitchOutputExtdtaForTest(fields, emptyLocator, []);
  assert.equal(emptyResult.used, 0);
  assert.equal(emptyResult.unresolved, false);

  const nullCell = [[null, locator(1)]];
  const nullResult = stitchOutputExtdtaForTest(fields, nullCell, [external([0, 0x43])]);
  assert.equal(nullResult.used, 1);
  assert.equal(nullResult.unresolved, false);

  const outputParameter = [[locator(1), null]];
  const outputResult = stitchOutputExtdtaForTest(fields, outputParameter, [Uint8Array.of(0, 0xaa)], true);
  assert.equal(outputResult.used, 1);
  assert.deepEqual([...outputParameter[0][0]], [0xaa]);
});

test('materialized EXTDTA LOBs stop at the configured per-value budget', () => {
  const length = 16 * 1024 * 1024 + 1;
  const locator = new Uint8Array(9);
  locator[0] = 0x02;
  new DataView(locator.buffer).setBigUint64(1, BigInt(length), false);
  const field = { type: 0xc8, ps: Uint8Array.of(0x80, 9) };
  const values = [new Uint8Array(length)];

  assert.equal(dss.DSS_LIMITS.maxLobMaterializationBytes, 16 * 1024 * 1024);
  assert.throws(
    () => stitchOutputExtdtaForTest([field], [[locator]], values),
    error => error.name === 'Db2ResourceLimitError' && /materialization limit/.test(error.message),
  );
});

test('nullable LOB descriptors strip the indicator while NOT NULL ones preserve a leading 0x00', () => {
  const locator = (length) => {
    const result = new Uint8Array(9);
    result[0] = 0x02;
    new DataView(result.buffer).setBigUint64(1, BigInt(length), false);
    return result;
  };
  const external = (value) => Uint8Array.from(value);
  const blobNotNull = { type: 0xc8, ps: Uint8Array.of(0x80, 9) };
  const blobNullable = { type: 0xc9, ps: Uint8Array.of(0x80, 9) };
  const clobNotNull = { type: 0xce, ps: Uint8Array.of(0x80, 9) };
  const clobNullable = { type: 0xcf, ps: Uint8Array.of(0x80, 9) };

  const notNull = [[locator(3)]];
  stitchOutputExtdtaForTest([blobNotNull], notNull, [external([0, 1, 2])]);
  assert.deepEqual([...notNull[0][0]], [0, 1, 2]);

  const nullable = [[locator(3)]];
  stitchOutputExtdtaForTest([blobNullable], nullable, [external([0, 1, 2])]);
  assert.deepEqual([...nullable[0][0]], [1, 2]);

  const textNotNull = [[locator(2)]];
  stitchOutputExtdtaForTest([clobNotNull], textNotNull, [external([0, 0x61])]);
  assert.equal(textNotNull[0][0], '\u0000a');

  const textNullable = [[locator(2)]];
  stitchOutputExtdtaForTest([clobNullable], textNullable, [external([0, 0x61])]);
  assert.equal(textNullable[0][0], 'a');

  const outputValue = [[locator(3)]];
  stitchOutputExtdtaForTest([blobNotNull], outputValue, [external([0, 1, 2])], true);
  assert.deepEqual([...outputValue[0][0]], [1, 2]);
});

test('mixed empty BLOBs retain length descriptors without redundant EXTDTA objects', () => {
  const { buildSqlDtaWithExternalData } = require('../dist/cjs/converters/sqlDta.js');
  const columns = Array.from({ length: 3 }, () => ({ name: 'B', sqlType: 405, length: 100, precision: 0, scale: 0, nullable: true }));
  for (let empty = 0; empty < 3; empty++) {
    const values = [Uint8Array.of(1), Uint8Array.of(2), Uint8Array.of(3)];
    values[empty] = new Uint8Array(0);
    const result = buildSqlDtaWithExternalData(columns, values);
    assert.deepEqual(result.extDta.map(p => [...p.ddmObject]), values.filter(v => v.length).map(v => [...ddm.packDdmObject(CodePoint.EXTDTA, Uint8Array.of(0, ...v))]));
  }
  assert.equal(buildSqlDtaWithExternalData(columns.slice(0, 1), [new Uint8Array(0)]).extDta.length, 1);
});

test('QRYDSC rejects malformed descriptor boundaries instead of partial parses', () => {
  assert.throws(() => ddm.parseQueryDescriptor(Uint8Array.of(1, 2)), /too short/);
  assert.throws(() => ddm.parseQueryDescriptor(Uint8Array.of(2, 0x76, 0xd0)), /invalid length 2/);
  assert.throws(() => ddm.parseQueryDescriptor(Uint8Array.of(9, 0x76, 0xd0, 1, 2, 3)), /exceeds payload/);
  assert.throws(() => ddm.parseQueryDescriptor(Uint8Array.of(7, 0x76, 0xd0, 1, 2, 3, 4)), /invalid length/);
  assert.throws(() => ddm.parseQueryDescriptor(Uint8Array.of(3, 0x76, 0xd0, 0x00)), /trailing invalid bytes/);
  assert.throws(() => ddm.parseQueryDescriptor(Uint8Array.of(6, 0x7f, 0, 1, 2, 3)), /no preceding NGDA/);
});
