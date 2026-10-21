import { decodeCp500, encodeCp500 } from "../converters/ebcdic.js";
import { Db2ServerError, type Db2Warning } from "../errors.js";
import { CodePoint } from "./codepoint.js";
import { DSS_LIMITS } from "./dss.js";

export type StringEncoding = "cp500" | "utf-8";
export type ManagerLevel = { manager: CodePoint; level: number };
export type ColumnDescription = {
  name: string;
  sqlType: number;
  length: bigint;
  precision: number;
  scale: number;
  nullable: boolean;
};
export type FieldDescriptor = { type: number; ps: Uint8Array };
export type SqlCard = { sqlCode: number; sqlState: string; message: string; rowsAffected: number; warningFlags: string };
export type ProgressiveChunkReply = { data: Uint8Array; endOfReference: boolean; isLastChunk: boolean };
export type ResultSetPackageSection = {
  database: Uint8Array;
  schema: Uint8Array;
  packageName: Uint8Array;
  consistencyToken: Uint8Array;
  sectionNumber: number;
};

const defaultCorrelationToken = Uint8Array.of(0xd5,0xc6,0xf0,0xf0,0xf0,0xf0,0xf0,0xf1,0x2e,0xc3,0xf0,0xc1,0xf5,0x01,0x55,0x63,0x0d,0x5a,0x11);
const defaultTypeOverrides = Uint8Array.of(0x00,0x06,0x11,0x9c,0x04,0xb8,0x00,0x06,0x11,0x9d,0x04,0xb0,0x00,0x06,0x11,0x9e,0x04,0xb8);
const defaultConsistencyToken = Uint8Array.of(1,1,1,1,1,1,1,1);
const paramRdbCommitToken = Uint8Array.of(0x00,0x05,0x21,0x05,0xf1);
const paramReturnSqlda = Uint8Array.of(0x00,0x05,0x21,0x16,0xf1);
const paramTypeSqlda = Uint8Array.of(0x00,0x05,0x21,0x46,0x01);
const paramQueryCloseImplicit = Uint8Array.of(0x00,0x05,0x21,0x5d,0x01);

export function packDdmObject(codePoint: CodePoint | number, body: Uint8Array = new Uint8Array()): Uint8Array {
  const smallLength = body.length + 4;
  let extensionBytes = 0;
  if (smallLength > 0x7fff) extensionBytes = body.length > 0x7fffffff ? 8 : 4;
  const headerLength = extensionBytes === 0 ? 4 : 4 + extensionBytes;
  const output = new Uint8Array(headerLength + body.length);
  const view = new DataView(output.buffer);
  if (extensionBytes === 0) view.setUint16(0, smallLength, false);
  else view.setUint16(0, 0x8000 | headerLength, false);
  view.setUint16(2, codePoint, false);
  if (extensionBytes === 4) view.setUint32(4, body.length, false);
  if (extensionBytes === 8) view.setBigUint64(4, BigInt(body.length), false);
  output.set(body, headerLength);
  return output;
}

export const packBytes = (codePoint: CodePoint | number, value: Uint8Array): Uint8Array => packDdmObject(codePoint, value);

export function packUint16(codePoint: CodePoint | number, value: number): Uint8Array {
  const result = new Uint8Array(6); const view = new DataView(result.buffer);
  view.setUint16(0, 6, false); view.setUint16(2, codePoint, false); view.setUint16(4, value, false);
  return result;
}

export function packUint32(codePoint: CodePoint | number, value: number): Uint8Array {
  const result = new Uint8Array(8); const view = new DataView(result.buffer);
  view.setUint16(0, 8, false); view.setUint16(2, codePoint, false); view.setUint32(4, value, false);
  return result;
}

function packUint64(codePoint: CodePoint | number, value: bigint): Uint8Array {
  if (value < 0n || value > 0xffff_ffff_ffff_ffffn) throw new RangeError(`${CodePoint[codePoint as CodePoint] ?? codePoint} must fit in an unsigned 64-bit integer`);
  const result = new Uint8Array(12); const view = new DataView(result.buffer);
  view.setUint16(0, result.length, false); view.setUint16(2, codePoint, false); view.setBigUint64(4, value, false);
  return result;
}

export function packString(codePoint: CodePoint | number, value: string, encoding: StringEncoding = "utf-8"): Uint8Array {
  return packDdmObject(codePoint, encoding === "cp500" ? encodeCp500(value) : utf8Encoder.encode(value));
}

// Module-level encoder/decoder cache: avoids per-call allocation on hot paths.
// Decoders are never used with stream:true across independent operations.
const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();
const utf8FatalDecoder = new TextDecoder("utf-8", { fatal: true });

export function packNullString(value: string | null, encoding: StringEncoding): Uint8Array {
  if (value === null) return Uint8Array.of(0xff);
  // CP500 must never silently fall back to UTF-8: an unencodable character
  // is an explicit error, otherwise a UTF-8 payload would be sent as a
  // DRDA field that the server expects in CP500.
  const bytes: Uint8Array = encoding === "cp500" ? encodeCp500(value) : utf8Encoder.encode(value);
  const result = new Uint8Array(5 + bytes.length); result[0] = 0;
  new DataView(result.buffer).setUint32(1, bytes.length, false); result.set(bytes, 5);
  return result;
}

export function defaultManagerLevels(): ManagerLevel[] {
  return [
    { manager: CodePoint.AGENT, level: 10 }, { manager: CodePoint.SQLAM, level: 11 },
    { manager: CodePoint.CMNTCPIP, level: 5 }, { manager: CodePoint.RDB, level: 12 },
    { manager: CodePoint.SECMGR, level: 9 }, { manager: CodePoint.UNICODEMGR, level: 1208 },
  ];
}

function concat(parts: readonly Uint8Array<ArrayBufferLike>[]): Uint8Array<ArrayBuffer> {
  const length = parts.reduce((sum, part) => sum + part.length, 0);
  const output = new Uint8Array(length); let offset = 0;
  for (const part of parts) { output.set(part, offset); offset += part.length; }
  return output;
}

function packManagerLevels(levels: readonly ManagerLevel[]): Uint8Array {
  const data = new Uint8Array(levels.length * 4); const view = new DataView(data.buffer);
  levels.forEach((item, i) => { view.setUint16(i * 4, item.manager, false); view.setUint16(i * 4 + 2, item.level, false); });
  return data;
}

export function packExcsat(extName: string, serverName: string, releaseLevel: string, serverClass: string, levels = defaultManagerLevels()): Uint8Array {
  return packDdmObject(CodePoint.EXCSAT, concat([
    packString(CodePoint.EXTNAM, extName, "cp500"), packString(CodePoint.SRVNAM, serverName, "cp500"),
    packString(CodePoint.SRVRLSLV, releaseLevel, "cp500"), packBytes(CodePoint.MGRLVLLS, packManagerLevels(levels)),
    packString(CodePoint.SRVCLSNM, serverClass, "cp500"),
  ]));
}

export function packExcsatManagerLevels(levels: readonly ManagerLevel[]): Uint8Array {
  return packDdmObject(CodePoint.EXCSAT, packBytes(CodePoint.MGRLVLLS, packManagerLevels(levels)));
}

export function packAccsec(database: string, securityMechanism: number, securityToken: Uint8Array = new Uint8Array(), encryptionAlgorithm?: number): Uint8Array {
  const params = [packUint16(CodePoint.SECMEC, securityMechanism), packString(CodePoint.RDBNAM, database, "cp500")];
  if (encryptionAlgorithm !== undefined) params.push(packUint16(CodePoint.ENCALG, encryptionAlgorithm));
  if (securityToken.length) params.push(packBytes(CodePoint.SECTKN, securityToken));
  return packDdmObject(CodePoint.ACCSEC, concat(params));
}

export function packSecchk(securityMechanism: number, securityToken: Uint8Array, database: string, user: string, password: Uint8Array, encrypted: boolean, encoding: StringEncoding): Uint8Array {
  const params = [packUint16(CodePoint.SECMEC, securityMechanism)];
  if (securityToken.length) params.push(packBytes(CodePoint.SECTKN, securityToken));
  params.push(packString(CodePoint.RDBNAM, database, encoding), packString(CodePoint.USRID, user, encoding));
  if (securityMechanism === 3 && password.length) params.push(encrypted ? packBytes(CodePoint.PASSWORD, password) : packString(CodePoint.PASSWORD, utf8Decoder.decode(password), encoding));
  else if (securityMechanism !== 3 && securityMechanism !== 4) throw new RangeError(`Only USRIDPWD (3) and USRIDONL (4) SECCHK requests are supported; got ${securityMechanism}`);
  return packDdmObject(CodePoint.SECCHK, concat(params));
}

/**
 * Builds the EUSRIDPWD (SECMEC 9) SECCHK as Db2 LUW expects it: the client
 * Diffie-Hellman public key travels in ACCSEC, and SECCHK carries two SECTKN
 * fields — the DES-CBC-encrypted userid followed by the DES-CBC-encrypted
 * password — with no cleartext USRID/PASSWORD. Verified against IBM CLI wire
 * on Db2 LUW (classic Derby EUSRIDPWD uses USRID/PASSWORD fields instead).
 */
export function packSecchkEusridpwd(database: string, encryptedUserid: Uint8Array, encryptedPassword: Uint8Array): Uint8Array {
  return packDdmObject(CodePoint.SECCHK, concat([
    packUint16(CodePoint.SECMEC, 9),
    packString(CodePoint.RDBNAM, database, "cp500"),
    packBytes(CodePoint.SECTKN, encryptedUserid),
    packBytes(CodePoint.SECTKN, encryptedPassword),
  ]));
}

/**
 * Builds the USRENCPWD (SECMEC 7) SECCHK: cleartext USRID plus one SECTKN
 * carrying the DES-CBC-encrypted password (Derby's layout). The client
 * Diffie-Hellman public key already travelled in ACCSEC. Verified against
 * Db2 LUW SERVER_ENCRYPT: an encrypted PASSWORD parameter instead of SECTKN
 * is rejected with SECCHKCD=16, and an extra SECTKN with AGNPRMRM.
 */
export function packSecchkUsrencpwd(database: string, user: string, encryptedPassword: Uint8Array): Uint8Array {
  return packDdmObject(CodePoint.SECCHK, concat([
    packUint16(CodePoint.SECMEC, 7),
    packString(CodePoint.RDBNAM, database, "cp500"),
    packString(CodePoint.USRID, user, "cp500"),
    packBytes(CodePoint.SECTKN, encryptedPassword),
  ]));
}

/**
 * Builds the Kerberos/GSSAPI (SECMEC 11) SECCHK. The caller-supplied GSSAPI
 * token goes into SECTKN; there is no password field.
 */
export function packSecchkKerberos(database: string, token: Uint8Array, user: string): Uint8Array {
  return packDdmObject(CodePoint.SECCHK, concat([
    packUint16(CodePoint.SECMEC, 11),
    packBytes(CodePoint.SECTKN, token),
    packString(CodePoint.RDBNAM, database, "cp500"),
    packString(CodePoint.USRID, user, "cp500"),
  ]));
}

export function packAccrdb(productId: string, database: string, encoding: StringEncoding): Uint8Array {
  return packDdmObject(CodePoint.ACCRDB, concat([
    packString(CodePoint.RDBNAM, database, encoding), packUint16(CodePoint.RDBACCCL, CodePoint.SQLAM),
    packString(CodePoint.PRDID, productId, encoding), packString(CodePoint.TYPDEFNAM, "QTDSQLX86", encoding),
    packBytes(CodePoint.CRRTKN, defaultCorrelationToken), packBytes(CodePoint.TYPDEFOVR, defaultTypeOverrides),
  ]));
}

export const packCommit = (): Uint8Array => packDdmObject(CodePoint.RDBCMM);
export const packRollback = (): Uint8Array => packDdmObject(CodePoint.RDBRLLBCK);

function packageNameSize(database: string, packageId: string, consistencyToken: string): number {
  return 4 + Math.max(18, BufferlessUtf8Length(database)) + 18 + Math.max(18, BufferlessUtf8Length(packageId)) + Math.max(8, BufferlessUtf8Length(consistencyToken)) + 2;
}

function BufferlessUtf8Length(value: string): number { return utf8Encoder.encode(value).length; }

const NULLID_BYTES = utf8Encoder.encode("NULLID            ");

export function packPackageNameCsn(database: string, packageId: string, consistencyToken: string, sectionNumber: number): Uint8Array {
  const db = utf8Encoder.encode(database); const pkg = utf8Encoder.encode(packageId); const token = utf8Encoder.encode(consistencyToken);
  const size = packageNameSize(database, packageId, consistencyToken); const output = new Uint8Array(size);
  const view = new DataView(output.buffer); view.setUint16(0, size, false); view.setUint16(2, CodePoint.PKGNAMCSN, false);
  let offset = 4;
  output.set(db, offset); output.fill(0x20, offset + db.length, offset + Math.max(18, db.length)); offset += Math.max(18, db.length);
  output.set(NULLID_BYTES, offset); offset += 18;
  output.set(pkg, offset); output.fill(0x20, offset + pkg.length, offset + Math.max(18, pkg.length)); offset += Math.max(18, pkg.length);
  if (token.length === 0) { output.set(defaultConsistencyToken, offset); offset += 8; }
  else if (token.length < 8) { output.fill(0x20, offset, offset + 8 - token.length); output.set(token, offset + 8 - token.length); offset += 8; }
  else { output.set(token, offset); offset += token.length; }
  view.setUint16(offset, sectionNumber, false);
  return output;
}

function packCommandWithPackage(codePoint: CodePoint, database: string, packageId: string, token: string, section: number, tail = new Uint8Array()): Uint8Array {
  return packDdmObject(codePoint, concat([packPackageNameCsn(database, packageId, token, section), tail]));
}

export const packExecSqlImmediate = (pkg: string, token: string, section: number, db: string): Uint8Array => packCommandWithPackage(CodePoint.EXCSQLIMM, db, pkg, token, section, paramRdbCommitToken);
export const packPrepareSqlStatement = (pkg: string, token: string, section: number, db: string): Uint8Array => packCommandWithPackage(CodePoint.PRPSQLSTT, db, pkg, token, section, paramReturnSqlda);
export const packDescribeSqlStatement = (pkg: string, token: string, section: number, db: string): Uint8Array => packCommandWithPackage(CodePoint.DSCSQLSTT, db, pkg, token, section, paramTypeSqlda);
export const packExecuteSqlStatement = (pkg: string, token: string, section: number, db: string): Uint8Array => packCommandWithPackage(CodePoint.EXCSQLSTT, db, pkg, token, section, paramRdbCommitToken);
export function packExecuteSqlStatementForResultSets(pkg: string, token: string, section: number, db: string, maximumResultSets = 0xffff, queryBlockSize = 32767, queryRowsetSize: number | null = 64, outputExpected = false): Uint8Array {
  const options = [
    paramRdbCommitToken,
    packBytes(CodePoint.OUTEXP, Uint8Array.of(outputExpected ? 0xf1 : 0xf0)),
    packUint32(CodePoint.QRYBLKSZ, queryBlockSize),
  ];
  if (queryRowsetSize !== null) options.push(packUint16(CodePoint.MAXBLKEXT, 0xffff));
  options.push(packUint16(CodePoint.MAXRSLCNT, maximumResultSets));
  options.push(packBytes(CodePoint.RSLSETFLG, Uint8Array.of(0x04)));
  if (queryRowsetSize !== null) options.push(packUint32(CodePoint.QRYROWSET, queryRowsetSize));
  return packCommandWithPackage(CodePoint.EXCSQLSTT, db, pkg, token, section, concat(options));
}
export const packExecuteSqlSet = (pkg: string, token: string, section: number, db: string): Uint8Array => packCommandWithPackage(CodePoint.EXCSQLSET, db, pkg, token, section);
export const packInterruptRdbRequest = (token: Uint8Array): Uint8Array => packDdmObject(CodePoint.INTRDBRQS, packBytes(CodePoint.RDBINTTKN, token));

/** Builds one locator-based GETNXTCHK request; Db2 replies with an EXTDTA chunk and ENDDTARM. */
export function packGetNextChunk(packageId: string, token: string, section: number, database: string, queryId: bigint, locator: bigint, requestedLength: bigint): Uint8Array {
  return packDdmObject(CodePoint.GETNXTCHK, concat([
    packPackageNameCsn(database, packageId, token, section),
    packUint64(CodePoint.QRYINSID, queryId),
    packUint64(CodePoint.GETNXTREF, locator),
    packUint64(CodePoint.GETNXTLEN, requestedLength),
  ]));
}

/** Parses one GETNXTCHK reply, stripping the EXTDTA status byte from each value object. */
export function parseProgressiveChunkReply(replies: readonly { codePoint: number; payload: Uint8Array }[]): ProgressiveChunkReply {
  const chunks: Uint8Array[] = [];
  let endOfReference = false;
  let isLastChunk = false;
  let sawEnd = false;
  for (const reply of replies) {
    if (reply.codePoint === CodePoint.EXTDTA) {
      if (sawEnd) throw new Error("GETNXTCHK reply contained EXTDTA after ENDDTARM");
      if (reply.payload.length === 0 || reply.payload[0] !== 0)
        throw new Error("GETNXTCHK reply contained an invalid EXTDTA status byte");
      chunks.push(reply.payload.subarray(1));
    } else if (reply.codePoint === CodePoint.ENDDTARM) {
      if (sawEnd) throw new Error("GETNXTCHK reply contained multiple ENDDTARM messages");
      sawEnd = true;
      const fields = parseDdmParameters(reply.payload);
      const serverCode = fields.get(CodePoint.SVRCOD);
      if (!serverCode || serverCode.length !== 2)
        throw new Error("GETNXTCHK ENDDTARM omitted its two-byte SVRCOD");
      const serverCodeValue = new DataView(serverCode.buffer, serverCode.byteOffset, serverCode.byteLength).getUint16(0, false);
      if (serverCodeValue !== 0) throw new Error(`GETNXTCHK ENDDTARM returned SVRCOD ${serverCodeValue}`);
      endOfReference = parseOptionalDdmBoolean(fields, CodePoint.ENDREF);
      isLastChunk = parseOptionalDdmBoolean(fields, CodePoint.LSTCHK);
    }
  }
  if (!sawEnd) throw new Error("GETNXTCHK reply did not contain ENDDTARM");
  const length = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
  const data = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) { data.set(chunk, offset); offset += chunk.length; }
  return { data, endOfReference, isLastChunk };
}

function parseOptionalDdmBoolean(values: ReadonlyMap<number, Uint8Array>, codePoint: CodePoint): boolean {
  const value = values.get(codePoint);
  if (value === undefined) return false;
  if (value.length !== 1 || (value[0] !== 0xf0 && value[0] !== 0xf1))
    throw new Error(`ENDDTARM contained an invalid ${CodePoint[codePoint]} Boolean value`);
  return value[0] === 0xf1;
}

function packQueryCommand(codePoint: CodePoint, pkg: string, token: string, section: number, db: string, blockSize: number, queryInstanceId?: bigint): Uint8Array {
  const queryBlockSize = new Uint8Array(8); const view = new DataView(queryBlockSize.buffer);
  view.setUint16(0, 8, false); view.setUint16(2, CodePoint.QRYBLKSZ, false); view.setUint32(4, blockSize, false);
  const maxBlock = new Uint8Array(6); const maxView = new DataView(maxBlock.buffer);
  maxView.setUint16(0, 6, false); maxView.setUint16(2, CodePoint.MAXBLKEXT, false); maxView.setUint16(4, blockSize & 0xffff, false);
  const params = [packPackageNameCsn(db, pkg, token, section), queryBlockSize];
  if (queryInstanceId !== undefined) {
    const queryId = new Uint8Array(12); const idView = new DataView(queryId.buffer);
    idView.setUint16(0, 12, false); idView.setUint16(2, CodePoint.QRYINSID, false); idView.setBigUint64(4, queryInstanceId, false);
    params.push(queryId, Uint8Array.of(0,5,0x21,0x48,0x02));
  } else {
    params.push(maxBlock, paramQueryCloseImplicit);
    if (codePoint === CodePoint.OPNQRY) params.push(packBytes(CodePoint.DYNDTAFMT, Uint8Array.of(0xf1)));
  }
  return packDdmObject(codePoint, concat(params));
}

export const packOpenQuery = (pkg: string, token: string, section: number, db: string, blockSize: number): Uint8Array => packQueryCommand(CodePoint.OPNQRY, pkg, token, section, db, blockSize);
export const packOpenQueryWithParams = (pkg: string, token: string, section: number, db: string, blockSize: number): Uint8Array => packQueryCommand(CodePoint.OPNQRY, pkg, token, section, db, blockSize);
export const packContinueQuery = (pkg: string, token: string, section: number, db: string, blockSize: number, queryInstanceId: bigint): Uint8Array => packQueryCommand(CodePoint.CNTQRY, pkg, token, section, db, blockSize, queryInstanceId);

function packResultSetPackageNameCsn(section: ResultSetPackageSection): Uint8Array {
  const output = new Uint8Array(68); const view = new DataView(output.buffer);
  view.setUint16(0, 68, false); view.setUint16(2, CodePoint.PKGNAMCSN, false);
  const writeName = (source: Uint8Array, offset: number, description: string) => {
    if (source.length > 18) throw new Error(`result-set ${description} exceeds its DRDA field width`);
    output.fill(0x20, offset, offset + 18); output.set(source, offset);
  };
  writeName(section.database, 4, "database"); writeName(section.schema, 22, "schema"); writeName(section.packageName, 40, "package name");
  if (section.consistencyToken.length !== 8) throw new Error("result-set consistency token must be eight bytes long");
  output.set(section.consistencyToken, 58); view.setUint16(66, section.sectionNumber, false);
  return output;
}

export function packContinueResultSetQuery(section: ResultSetPackageSection, blockSize: number, queryInstanceId: bigint, rowsetSize: number | null = 64, returnAllData = false): Uint8Array {
  const id = new Uint8Array(12); const idView = new DataView(id.buffer);
  idView.setUint16(0, 12, false); idView.setUint16(2, CodePoint.QRYINSID, false); idView.setBigUint64(4, queryInstanceId, false);
  const options = [packResultSetPackageNameCsn(section), packUint32(CodePoint.QRYBLKSZ, blockSize)];
  if (rowsetSize !== null) options.push(packUint16(CodePoint.MAXBLKEXT, 0xffff));
  options.push(id);
  if (rowsetSize !== null) options.push(packUint32(CodePoint.QRYROWSET, rowsetSize));
  if (returnAllData) options.push(packBytes(CodePoint.RTNEXTDTA, Uint8Array.of(0x02)));
  return packDdmObject(CodePoint.CNTQRY, concat(options));
}

/** Fetches a procedure cursor using the shape emitted by IBM's Linux CLI driver. */
export function packContinueProcedureResultSet(section: ResultSetPackageSection, blockSize: number, queryInstanceId: bigint): Uint8Array {
  const id = new Uint8Array(12); const view = new DataView(id.buffer);
  view.setUint16(0, 12, false); view.setUint16(2, CodePoint.QRYINSID, false); view.setBigUint64(4, queryInstanceId, false);
  return packDdmObject(CodePoint.CNTQRY, concat([
    packResultSetPackageNameCsn(section), packUint32(CodePoint.QRYBLKSZ, blockSize), id,
    packBytes(CodePoint.RTNEXTDTA, Uint8Array.of(0x02)), packBytes(CodePoint.FREPRVREF, Uint8Array.of(0xf0)),
  ]));
}

export function packCloseProcedureResultSet(section: ResultSetPackageSection, queryInstanceId: bigint): Uint8Array {
  const id = new Uint8Array(12); const view = new DataView(id.buffer);
  view.setUint16(0, 12, false); view.setUint16(2, CodePoint.QRYINSID, false); view.setBigUint64(4, queryInstanceId, false);
  return packDdmObject(CodePoint.CLSQRY, concat([packResultSetPackageNameCsn(section), id]));
}

export function packSqlStatement(sql: string): Uint8Array {
  const bytes = utf8Encoder.encode(sql);
  if (10 + bytes.length <= 0x7fff) {
    const output = new Uint8Array(10 + bytes.length); const view = new DataView(output.buffer);
    view.setUint16(0, output.length, false); view.setUint16(2, CodePoint.SQLSTT, false); output[4] = 0;
    view.setUint32(5, bytes.length, false); output.set(bytes, 9); output[9 + bytes.length] = 0xff;
    return output;
  }
  return packDdmObject(CodePoint.SQLSTT, concat([packNullString(sql, "utf-8"), packNullString(null, "utf-8")]));
}

export function parseDdmParameters(data: Uint8Array): Map<number, Uint8Array> {
  return new Map(readDdmObjects(data).map(item => [item.codePoint, item.payload]));
}

/** Zero-alloc cursor over DDM parameters: manual offset loop, no array/Map. */
export function readDdmObjectsCursor(
  data: Uint8Array,
  visit: (codePoint: number, payload: Uint8Array) => void,
): void {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let offset = 0;
  while (offset < data.length) {
    if (offset + 4 > data.length) throw new Error("incomplete DDM parameter header in reply");
    const encodedLength = view.getUint16(offset, false);
    const codePoint = view.getUint16(offset + 2, false);
    let headerLength = 4;
    let length = encodedLength;
    if (encodedLength & 0x8000) {
      headerLength = encodedLength & 0x7fff;
      if (headerLength !== 8 && headerLength !== 12) throw new Error(`invalid extended DDM header length ${headerLength}`);
      if (offset + headerLength > data.length) throw new Error("truncated extended DDM parameter header");
      const bodyLength = headerLength === 8 ? BigInt(view.getUint32(offset + 4, false)) : view.getBigUint64(offset + 4, false);
      if (bodyLength > BigInt(Number.MAX_SAFE_INTEGER - headerLength)) throw new Error("extended DDM parameter exceeds the supported buffer size");
      if (bodyLength > BigInt(DSS_LIMITS.maxDdmObjectBytes)) throw new Error("extended DDM parameter exceeds the resource limit");
      length = headerLength + Number(bodyLength);
    }
    if (length < headerLength || offset + length > data.length) throw new Error(`invalid DDM parameter length ${length} at offset ${offset}`);
    visit(codePoint, data.subarray(offset + headerLength, offset + length));
    offset += length;
  }
}

/** Finds a single DDM parameter without building a full Map. */
export function findDdmObject(data: Uint8Array, codePoint: number): Uint8Array | undefined {
  let found: Uint8Array | undefined;
  readDdmObjectsCursor(data, (cp, payload) => {
    if (found === undefined && cp === codePoint) found = payload;
  });
  return found;
}

function readDdmObjects(data: Uint8Array): { codePoint: number; payload: Uint8Array }[] {
  const results: { codePoint: number; payload: Uint8Array }[] = [];
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let offset = 0;
  while (offset < data.length) {
    if (offset + 4 > data.length) throw new Error("incomplete DDM parameter header in reply");
    const encodedLength = view.getUint16(offset, false); const codePoint = view.getUint16(offset + 2, false);
    let headerLength = 4; let length = encodedLength;
    if (encodedLength & 0x8000) {
      headerLength = encodedLength & 0x7fff;
      if (headerLength !== 8 && headerLength !== 12) throw new Error(`invalid extended DDM header length ${headerLength}`);
      if (offset + headerLength > data.length) throw new Error("truncated extended DDM parameter header");
      const bodyLength = headerLength === 8 ? BigInt(view.getUint32(offset + 4, false)) : view.getBigUint64(offset + 4, false);
      if (bodyLength > BigInt(Number.MAX_SAFE_INTEGER - headerLength)) throw new Error("extended DDM parameter exceeds the supported buffer size");
      length = headerLength + Number(bodyLength);
    }
    if (length < headerLength || offset + length > data.length) throw new Error(`invalid DDM parameter length ${length} at offset ${offset}`);
    results.push({ codePoint, payload: data.subarray(offset + headerLength, offset + length) });
    offset += length;
  }
  return results;
}

export function parseSqlResultSetCount(data: Uint8Array): number {
  if (data.length < 2) throw new Error("SQLRSLRD is missing its result-set count");
  const count = new DataView(data.buffer, data.byteOffset, data.byteLength).getUint16(0, true);
  if (data.length - 2 < count * 12) throw new Error(`SQLRSLRD declares ${count} result sets but its row summaries are truncated`);
  return count;
}

export function parseResultSetSections(data: Uint8Array): ResultSetPackageSection[] {
  const packageList = readDdmObjects(data).find(item => item.codePoint === CodePoint.PKGSNLST)?.payload;
  if (!packageList) throw new Error("RSLSETRM does not contain the required PKGSNLST object");
  return readDdmObjects(packageList).filter(item => item.codePoint === CodePoint.PKGNAMCSN).map(item => {
    if (item.payload.length < 64) throw new Error("RSLSETRM contains a truncated PKGNAMCSN object");
    return {
      database: item.payload.slice(0, 18), schema: item.payload.slice(18, 36), packageName: item.payload.slice(36, 54),
      consistencyToken: item.payload.slice(54, 62), sectionNumber: new DataView(item.payload.buffer, item.payload.byteOffset, item.payload.byteLength).getUint16(62, false),
    };
  });
}

export function parseSqlCursorInfo(data: Uint8Array, littleEndian = true): ColumnDescription[] {
  if (data.length === 0) throw new Error("Empty SQLCINRD payload");
  // The result is self-delimiting, so try each supported SQLAM layout and
  // accept only a parse that consumes the entire SQLCINRD object. Db2 LUW
  // uses 32-bit SQLDHGRP string lengths; older servers use 16-bit lengths.
  for (const level of [10, 9, 8, 7] as const) {
    for (const width of [4, 2] as const) {
      try { return parseSqlCursorInfoStrict(data, width, level); }
      catch { /* Try the next exact negotiated-format candidate. */ }
    }
  }
  for (const width of [4, 2] as const) {
    try { return parseSqlCursorInfoLegacy(data, littleEndian, width); }
    catch { /* Legacy name scanning still must honor the SQLDHGRP string width. */ }
  }
  throw new Error("SQLCINRD does not match a supported SQLDA layout");
}

function parseSqlCursorInfoLegacy(data: Uint8Array, littleEndian: boolean, headerStringWidth: 2 | 4): ColumnDescription[] {
  let rest: Uint8Array;
  const hasNames = data[0] === 0;
  if (data[0] === 0xff) rest = data.subarray(1);
  else {
    if (data.length < 13) throw new Error("Truncated SQLCINRD header group");
    rest = data.subarray(13);
    [, rest] = parseFdocaStringWidth(rest, headerStringWidth);
    [, rest] = parseFdocaNameWidth(rest, headerStringWidth);
  }
  if (rest.length < 2) throw new Error("SQLCINRD is missing SQLNUMROW");
  const count = readUint16(rest, 0, littleEndian); rest = rest.subarray(2);
  const columns: ColumnDescription[] = [];
  for (let index = 0; index < count; index++) {
    if (rest.length < 16) throw new Error(`SQLCINRD column ${index + 1} is missing its fixed descriptor`);
    const precision = readUint16(rest, 0, littleEndian); const scale = readUint16(rest, 2, littleEndian);
    const length = readBigUint64(rest, 4, littleEndian); const sqlType = readUint16(rest, 12, littleEndian);
    rest = rest.subarray(16);
    let name = ""; let delimiter = -1;
    for (let position = 0; position + 2 < rest.length; position++)
      if (rest[position] === 0xff && rest[position + 1] === 0xff && rest[position + 2] === 0xff) { delimiter = position; break; }
    if (delimiter >= 0) {
      const nameChunk = rest.subarray(0, delimiter); rest = rest.subarray(delimiter + 3);
      for (let position = 0; position + 1 < nameChunk.length; position++) {
        const candidateLength = nameChunk[position]!;
        if (candidateLength > 0 && position + 1 + candidateLength <= nameChunk.length) {
          try {
            const candidate = utf8FatalDecoder.decode(nameChunk.subarray(position + 1, position + 1 + candidateLength));
            if (!/[\u0000-\u001f\u007f-\u009f]/u.test(candidate)) { name = candidate; break; }
          } catch { /* Continue scanning the variable descriptor bytes for a name. */ }
        }
      }
    } else if (hasNames && rest.length >= 9) {
      rest = rest.subarray(9);
      let candidate: string; [candidate, rest] = parseName(rest);
      let label: string; [label, rest] = parseName(rest);
      [, rest] = parseName(rest);
      if (rest.length >= 7) rest = rest.subarray(7);
      name = label || candidate;
    } else if (!hasNames && rest.length >= 29) rest = rest.subarray(29);
    columns.push({ name: name || `COLUMN${index + 1}`, sqlType, length, precision, scale: normalizeScale(sqlType, length, scale), nullable: (sqlType & 1) !== 0 });
  }
  return columns;
}

function parseFdocaStringWidth(data: Uint8Array, width: 2 | 4): [string, Uint8Array] {
  if (data.length < width) throw new Error("Truncated SQLCINRD FDOCA string length");
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const length = width === 4 ? view.getUint32(0, false) : view.getUint16(0, false);
  if (length > Number.MAX_SAFE_INTEGER || data.length < width + length) throw new Error("Truncated SQLCINRD FDOCA string");
  return [utf8Decoder.decode(data.subarray(width, width + length)), data.subarray(width + length)];
}

function parseFdocaNameWidth(data: Uint8Array, width: 2 | 4): [string, Uint8Array] {
  const [first, afterFirst] = parseFdocaStringWidth(data, width);
  const [second, afterSecond] = parseFdocaStringWidth(afterFirst, width);
  return [first || second, afterSecond];
}

function parseSqlCursorInfoStrict(data: Uint8Array, headerStringWidth: 2 | 4, sqlamLevel: number): ColumnDescription[] {
  let offset = 0;
  const requireBytes = (length: number): void => {
    if (length < 0 || offset + length > data.length) throw new Error("Truncated SQLCINRD FDOCA data");
  };
  const nullableGroup = (): boolean => { requireBytes(1); return data[offset++] !== 0xff; };
  const fdocaString = (width: 2 | 4 = 2): string => {
    requireBytes(width);
    const view = new DataView(data.buffer, data.byteOffset + offset, width);
    const length = width === 4 ? view.getUint32(0, false) : view.getUint16(0, false); offset += width;
    requireBytes(length);
    const value = utf8Decoder.decode(data.subarray(offset, offset + length)); offset += length;
    return value;
  };
  const fdocaStringPair = (width: 2 | 4 = 2): string => {
    const mbcs = fdocaString(width); const sbcs = fdocaString(width);
    if (mbcs.length > 0 && sbcs.length > 0) throw new Error("SQLCINRD VCM/VCS string has both encodings");
    return mbcs || sbcs;
  };

  if (nullableGroup()) {
    requireBytes(12); offset += 12;
    fdocaString(headerStringWidth);
    fdocaStringPair(headerStringWidth);
  }
  requireBytes(2);
  const count = new DataView(data.buffer, data.byteOffset + offset, 2).getUint16(0, true); offset += 2;
  const columns: ColumnDescription[] = [];
  for (let index = 0; index < count; index++) {
    requireBytes(16);
    const view = new DataView(data.buffer, data.byteOffset + offset, 16);
    const precision = view.getUint16(0, true); const scale = view.getUint16(2, true);
    const length = view.getBigUint64(4, true); const sqlType = view.getUint16(12, true); offset += 16;
    if (sqlamLevel >= 9) { requireBytes(8); offset += 8; }
    if (sqlamLevel >= 10) { requireBytes(2); offset += 2; }
    let name = "";
    if (nullableGroup()) {
      requireBytes(2); offset += 2;
      const sqlName = fdocaStringPair();
      const sqlLabel = fdocaStringPair();
      fdocaStringPair(); // SQLCOMMENTS.
      let sqlXName = "";
      if (nullableGroup()) {
        if ((sqlType & 0xfffe) === 2000) {
          requireBytes(4); offset += 4;
          fdocaString();
          fdocaStringPair();
          fdocaStringPair();
          if (sqlamLevel >= 10) fdocaStringPair();
        }
      }
      if (nullableGroup()) {
        requireBytes(sqlamLevel >= 9 ? 12 : 8); offset += sqlamLevel >= 9 ? 12 : 8;
        fdocaString();
        fdocaStringPair(); // SQLXCORNAME.
        fdocaStringPair(); // SQLXBASENAME.
        fdocaStringPair(); // SQLXSCHEMA.
        sqlXName = fdocaStringPair();
        if (sqlamLevel >= 10) fdocaStringPair();
      }
      if (sqlamLevel >= 10) { requireBytes(1); offset++; }
      name = sqlLabel || sqlXName || sqlName;
    }
    columns.push({ name: name || `COLUMN${index + 1}`, sqlType, length, precision, scale: normalizeScale(sqlType, length, scale), nullable: (sqlType & 1) !== 0 });
  }
  if (offset !== data.length) throw new Error("SQLCINRD contains trailing descriptor bytes");
  return columns;
}

function readUint16(data: Uint8Array, offset: number, littleEndian: boolean): number { return new DataView(data.buffer, data.byteOffset, data.byteLength).getUint16(offset, littleEndian); }
function readUint32(data: Uint8Array, offset: number, littleEndian: boolean): number { return new DataView(data.buffer, data.byteOffset, data.byteLength).getUint32(offset, littleEndian); }
function readBigUint64(data: Uint8Array, offset: number, littleEndian: boolean): bigint { return new DataView(data.buffer, data.byteOffset, data.byteLength).getBigUint64(offset, littleEndian); }

function normalizeScale(sqlType: number, length: bigint, scale: number): number {
  // SQLDA encodes TIMESTAMP(p) in SQLLENGTH as 19 for p=0, otherwise 20+p.
  // Some Db2 LUW DRDA replies leave SQLSCALE at zero, unlike CLI/ODBC.
  if ((sqlType & 0xfffe) !== 392) return scale;
  if (length === 19n) return 0;
  return length >= 21n && length <= 32n ? Number(length - 20n) : scale;
}

export function parseSqlCard(data: Uint8Array, littleEndian = true): SqlCard {
  if (data.length === 0) throw new Error("empty SQLCARD payload");
  if (data[0] === 0xff) return { sqlCode: 0, sqlState: "", message: "", rowsAffected: 0, warningFlags: "" };
  if (data.length < 18) throw new Error(`SQLCARD payload too short: ${data.length} bytes`);
  const sqlCode = readUint32(data, 1, littleEndian) | 0;
  const sqlState = utf8Decoder.decode(data.subarray(5, 10));
  const warningFlags = data.length >= 54 && data[18] === 0 ? utf8Decoder.decode(data.subarray(43, 54)) : "";
  // Nullable SQLCAXGRP starts at 18; its six I4 fields start at 19.
  // SQLERRD(3) is at 27, not inside SQLERRD(1/2) at 22.
  const hasExtension = data.length > 18 && data[18] !== 0xff;
  if (hasExtension && data.length < 54) throw new Error('SQLCARD SQLCAXGRP is truncated');
  const rowsAffected = hasExtension ? readUint32(data, 27, littleEndian) | 0 : 0;
  let message = "";
  if (hasExtension && data.length > 54) {
    let rest = data.subarray(54);
    if (rest.length >= 2) { const length = readUint16(rest, 0, false); if (rest.length >= 2 + length) rest = rest.subarray(2 + length); }
    if (rest.length >= 2) { const length = readUint16(rest, 0, false); if (rest.length >= 2 + length) message = utf8Decoder.decode(rest.subarray(2, 2 + length)).replaceAll("\xff", ", "); }
  }
  return { sqlCode, sqlState, message, rowsAffected, warningFlags };
}

export function sqlCardWarning(card: SqlCard): Db2Warning | undefined {
  const isWarning = (card.sqlCode > 0 && card.sqlCode !== 100)
    || card.sqlState.startsWith("01")
    || (card.warningFlags.length > 0 && card.warningFlags[0] === "W");
  if (!isWarning) return undefined;
  return {
    sqlCode: card.sqlCode,
    sqlState: card.sqlState,
    message: card.message || `DB2 returned a warning (SQLCODE=${card.sqlCode}, SQLSTATE=${card.sqlState}).`,
    warningFlags: card.warningFlags,
  };
}

export function parseQueryDescriptor(data: Uint8Array): FieldDescriptor[] {
  if (data.length < 3) throw new Error("QRYDSC payload too short");
  const fields: FieldDescriptor[] = []; let position = 0;
  let hasDescriptorBase = false;
  while (position + 3 <= data.length) {
    const groupLength = data[position]!;
    // Strict protocol parsing: malformed lengths are errors, never silent
    // partial parses. A truncated group or trailing garbage must surface.
    if (groupLength < 3) throw new Error(`QRYDSC group at offset ${position} has invalid length ${groupLength}`);
    if (position + groupLength > data.length) throw new Error(`QRYDSC group at offset ${position} with length ${groupLength} exceeds payload ${data.length}`);
    const tripletType = data[position + 1]!;
    const tripletId = data[position + 2]!;
    if (tripletType === 0x76 && tripletId === 0xd0) hasDescriptorBase = true;
    else if (tripletType === 0x7f && tripletId === 0 && !hasDescriptorBase)
      throw new Error("QRYDSC CPT continuation has no preceding NGDA descriptor group");
    if ((tripletType === 0x76 && tripletId === 0xd0) || (tripletType === 0x7f && tripletId === 0)) {
      if ((groupLength - 3) % 3 !== 0) throw new Error("QRYDSC descriptor group has an invalid length");
      const count = Math.floor(groupLength / 3) - 1; let fieldPosition = position + 3;
      for (let i = 0; i < count && fieldPosition + 3 <= position + groupLength; i++, fieldPosition += 3)
        fields.push({ type: data[fieldPosition]!, ps: data.subarray(fieldPosition + 1, fieldPosition + 3) });
    }
    position += groupLength;
  }
  if (position !== data.length) throw new Error(`QRYDSC has ${data.length - position} trailing invalid bytes`);
  return fields;
}

export function parseSqlDtaReply(data: Uint8Array): { fields: FieldDescriptor[]; values: Uint8Array } {
  const objects = parseDdmParameters(data);
  const descriptor = objects.get(CodePoint.FDODSC);
  const values = objects.get(CodePoint.FDODTA);
  // The FDODTA payload is immutable for the lifetime of the reply chain;
  // keep a zero-copy view instead of slice() unless the caller needs ownership.
  return { fields: descriptor ? parseQueryDescriptor(descriptor) : [], values: values ?? new Uint8Array() };
}

function parseString(data: Uint8Array): [string, Uint8Array] {
  // Strict: distinguish valid "" (length 0) from truncated input (error).
  if (data.length < 2) throw new Error("Truncated DRDA string length header");
  const length = readUint16(data, 0, false);
  if (length === 0) return ["", data.subarray(2)];
  if (data.length < 2 + length) throw new Error(`Truncated DRDA string: declared ${length} bytes but only ${data.length - 2} remain`);
  return [utf8Decoder.decode(data.subarray(2, 2 + length)), data.subarray(2 + length)];
}

function parseName(data: Uint8Array): [string, Uint8Array] {
  const [first, afterFirst] = parseString(data); const [second, afterSecond] = parseString(afterFirst);
  return first.length > 0 ? [first, afterSecond] : [second, afterSecond];
}

function printableUtf8(data: Uint8Array): boolean {
  try { const text = utf8FatalDecoder.decode(data); return !/[\u0000-\u001f\u007f-\u009f]/u.test(text); }
  catch { return false; }
}

export function parseSqlDard(data: Uint8Array, littleEndian = true): ColumnDescription[] {
  if (data.length === 0) return [];
  const hasName = data[0] === 0;
  let rest: Uint8Array;
  if (data[0] === 0xff) rest = data.subarray(1);
  else {
    const card = parseSqlCard(data, littleEndian);
    if (card.sqlCode < 0) throw new Db2ServerError(card.sqlCode, card.sqlState, card.message);
    if (data.length > 54) {
      rest = data.subarray(54);
      for (let i = 0; i < 3 && rest.length >= 2; i++) { const length = readUint16(rest, 0, false); if (rest.length >= 2 + length) rest = rest.subarray(2 + length); }
      if (rest.length && rest[0] === 0xff) rest = rest.subarray(1);
    } else rest = data;
  }
  if (rest.length < 2) return [];
  if (rest[0] === 0) {
    if (rest.length > 13) { rest = rest.subarray(13); [, rest] = parseString(rest); [, rest] = parseName(rest); }
  } else rest = rest.subarray(1);
  if (rest.length < 2) return [];
  const count = readUint16(rest, 0, littleEndian); rest = rest.subarray(2);
  const columns: ColumnDescription[] = [];
  for (let index = 0; index < count && rest.length >= 16; index++) {
    const precision = readUint16(rest, 0, littleEndian); const scale = readUint16(rest, 2, littleEndian);
    const length = readBigUint64(rest, 4, littleEndian); const sqlType = readUint16(rest, 12, littleEndian); rest = rest.subarray(16);
    let name = ""; let delimiter = -1;
    for (let i = 0; i + 2 < rest.length; i++) if (rest[i] === 0xff && rest[i + 1] === 0xff && rest[i + 2] === 0xff) { delimiter = i; break; }
    if (delimiter >= 0) {
      const nameChunk = rest.subarray(0, delimiter); rest = rest.subarray(delimiter + 3);
      for (let i = 0; i + 1 < nameChunk.length; i++) {
        const candidateLength = nameChunk[i]!;
        if (candidateLength > 0 && i + 1 + candidateLength <= nameChunk.length && printableUtf8(nameChunk.subarray(i + 1, i + 1 + candidateLength))) {
          name = utf8Decoder.decode(nameChunk.subarray(i + 1, i + 1 + candidateLength)); break;
        }
      }
    } else if (hasName) {
      if (rest.length >= 9) {
        rest = rest.subarray(9); let candidate: string; [candidate, rest] = parseName(rest);
        let label: string; [label, rest] = parseName(rest); [, rest] = parseName(rest);
        if (rest.length >= 7) rest = rest.subarray(7);
        name = label || candidate;
      }
    } else if (rest.length >= 29) rest = rest.subarray(29);
    if (!name) name = `PARAM${index + 1}`;
    columns.push({ name, sqlType, length, precision, scale: normalizeScale(sqlType, length, scale), nullable: sqlType % 2 !== 0 });
  }
  return columns;
}

export function decodeCp500String(value: Uint8Array): string { return decodeCp500(value); }
