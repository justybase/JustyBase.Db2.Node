import type { ColumnDescription } from "../network/ddm.js";
import { CodePoint } from "../network/codepoint.js";
import { packDdmObject } from "../network/ddm.js";
import { encodeDecfloat } from "./decfloat.js";

const varcharDescriptor = Uint8Array.of(0x39, 0x3f, 0xff);
const binaryCharacterDescriptor = Uint8Array.of(0x3f, 0x7f, 0xff);

const utf8EncoderShared = new TextEncoder();
const utf8FatalDecoderShared = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

/** Minimal growable DRDA writer: geometric growth, no per-field allocations. */
export class DrdaWriter {
  private buf: Uint8Array;
  private view: DataView;
  private off = 0;

  constructor(initialCapacity = 1024) {
    this.buf = new Uint8Array(Math.max(64, initialCapacity));
    this.view = new DataView(this.buf.buffer);
  }

  get length(): number { return this.off; }

  private ensureCapacity(additional: number): void {
    const required = this.off + additional;
    if (required <= this.buf.length) return;
    let capacity = this.buf.length * 2;
    if (capacity < required) capacity = required;
    // Reasonable cap: single SQLDTA must fit the DSS limit (256 MiB).
    if (capacity > 256 * 1024 * 1024) {
      if (required > 256 * 1024 * 1024) throw new RangeError("DRDA SQLDTA exceeds the 256 MiB limit");
      capacity = required;
    }
    const next = new Uint8Array(capacity);
    next.set(this.buf.subarray(0, this.off), 0);
    this.buf = next;
    this.view = new DataView(this.buf.buffer);
  }

  writeUint8(value: number): void {
    this.ensureCapacity(1);
    this.buf[this.off++] = value & 0xff;
  }

  writeUint16BE(value: number): void {
    this.ensureCapacity(2);
    this.view.setUint16(this.off, value, false);
    this.off += 2;
  }

  writeUint32BE(value: number): void {
    this.ensureCapacity(4);
    this.view.setUint32(this.off, value >>> 0, false);
    this.off += 4;
  }

  writeBigUint64BE(value: bigint): void {
    this.ensureCapacity(8);
    this.view.setBigUint64(this.off, value, false);
    this.off += 8;
  }

  writeInt16LE(value: number): void {
    this.ensureCapacity(2);
    this.view.setInt16(this.off, value, true);
    this.off += 2;
  }

  writeInt32LE(value: number): void {
    this.ensureCapacity(4);
    this.view.setInt32(this.off, value, true);
    this.off += 4;
  }

  writeBigInt64LE(value: bigint): void {
    this.ensureCapacity(8);
    this.view.setBigInt64(this.off, value, true);
    this.off += 8;
  }

  writeFloat32LE(value: number): void {
    this.ensureCapacity(4);
    this.view.setFloat32(this.off, value, true);
    this.off += 4;
  }

  writeFloat64LE(value: number): void {
    this.ensureCapacity(8);
    this.view.setFloat64(this.off, value, true);
    this.off += 8;
  }

  writeBytes(data: Uint8Array): void {
    if (data.length === 0) return;
    this.ensureCapacity(data.length);
    this.buf.set(data, this.off);
    this.off += data.length;
  }

  /** Writes a UTF-16BE string directly without an intermediate buffer. */
  writeUtf16Be(text: string): void {
    this.ensureCapacity(text.length * 2);
    for (let i = 0; i < text.length; i++) {
      this.view.setUint16(this.off, text.charCodeAt(i), false);
      this.off += 2;
    }
  }

  /** Reserves `size` bytes, returns the offset for later backpatch. */
  reserve(size: number): number {
    this.ensureCapacity(size);
    const offset = this.off;
    this.off += size;
    return offset;
  }

  patchUint16BE(offset: number, value: number): void {
    this.view.setUint16(offset, value, false);
  }

  toUint8Array(): Uint8Array {
    return this.buf.slice(0, this.off);
  }
}

/** Builds a complete SQLDTA object with FDODSC and FDODTA parameter blocks. */
export function buildSqlDta(columns: readonly ColumnDescription[], values: readonly unknown[]): Uint8Array {
  return buildSqlDtaWithExternalData(columns, values).sqlDta;
}

/** A one-shot BLOB parameter source. Node Readable streams and async byte generators are supported. */
export type Db2BlobStream = { source: AsyncIterable<Uint8Array>; length: number | bigint; leaveOpen?: boolean };
/** A one-shot CLOB byte source encoded as UTF-8. */
export type Db2ClobStream = { source: AsyncIterable<Uint8Array>; length: number | bigint; leaveOpen?: boolean };
/** A one-shot DBCLOB byte source encoded as UTF-8. Db2 converts it to the graphic target type. */
export type Db2DbClobStream = { source: AsyncIterable<Uint8Array>; length: number | bigint; leaveOpen?: boolean };
/** A one-shot serialized XML byte source encoded as UTF-8. */
export type Db2XmlStream = { source: AsyncIterable<Uint8Array>; length: number | bigint; leaveOpen?: boolean };
type Db2ByteStream = Db2BlobStream | Db2ClobStream | Db2DbClobStream | Db2XmlStream;
export type SqlDtaExternalParameter = { ddmObject: Uint8Array } | { blobStream: Db2BlobStream } | { textLobStream: Db2ByteStream };
export type SqlDtaExternalData = { sqlDta: Uint8Array; extDta: readonly SqlDtaExternalParameter[] };
type ExternalLobValue = { bytes: Uint8Array; length: bigint } | { blobStream: Db2BlobStream; length: bigint } | { textLobStream: Db2ByteStream; length: bigint; type: 408 | 412 | 988 };

/** Builds SQLDTA plus separate EXTDTA objects for BLOBs larger than one DSS segment. */
export function buildSqlDtaWithExternalData(columns: readonly ColumnDescription[], values: readonly unknown[]): SqlDtaExternalData {
  if (columns.length !== values.length) throw new Error(`DB2 expected ${columns.length} parameters, got ${values.length}`);
  const count = columns.length;
  // Single loops, no map/flatMap closures on the hot encoder path.
  const normalized: unknown[] = new Array(count);
  for (let i = 0; i < count; i++) normalized[i] = normalizeParameterSource(columns[i]!, values[i]);
  const external: (ExternalLobValue | undefined)[] = new Array(count);
  let hasNonEmptyExternal = false;
  for (let i = 0; i < count; i++) {
    const value = external[i] = externalLob(columns[i]!, normalized[i]);
    if (value !== undefined && value.length > 0n) hasNonEmptyExternal = true;
  }

  // FDODSC via growable writer: no per-descriptor Uint8Array except dynamic LOB cases.
  const dscWriter = new DrdaWriter(64 + count * 3 + 6);
  for (let offset = 0; offset < count; offset += 84) {
    const groupCount = Math.min(84, count - offset);
    // NGDA (0x76/0xD0) for the first group, CPT (0x7F/0x00) for continuations.
    dscWriter.writeUint8((groupCount + 1) * 3);
    dscWriter.writeUint8(offset === 0 ? 0x76 : 0x7f);
    dscWriter.writeUint8(offset === 0 ? 0xd0 : 0x00);
    for (let i = 0; i < groupCount; i++) {
      const idx = offset + i;
      writeFieldDescriptor(dscWriter, columns[idx]!, external[idx] !== undefined, normalized[idx]);
    }
  }
  dscWriter.writeBytes(Uint8Array.of(0x06, 0x71, 0xe4, 0xd0, 0x00, 0x01));
  const fdodsc = dscWriter.toUint8Array();

  // FDODTA via writer: direct writes, no per-parameter concat.
  const dtaWriter = new DrdaWriter(64 + count * 16);
  if (count > 0) dtaWriter.writeUint8(0);
  for (let i = 0; i < count; i++) writeParameter(dtaWriter, columns[i]!, normalized[i], external[i]);
  const fdodta = dtaWriter.toUint8Array();

  const sqlDta = packDdmObject(CodePoint.SQLDTA, concat([
    packDdmObject(CodePoint.FDODSC, fdodsc),
    packDdmObject(CodePoint.FDODTA, fdodta),
  ]));
  const extDta: SqlDtaExternalParameter[] = [];
  for (let i = 0; i < count; i++) {
    const value = external[i];
    if (value === undefined) continue;
    if ("bytes" in value && value.bytes.length === 0 && hasNonEmptyExternal) continue;
    if ("blobStream" in value) extDta.push({ blobStream: value.blobStream });
    else if ("textLobStream" in value) extDta.push({ textLobStream: value.textLobStream });
    else extDta.push({ ddmObject: packDdmObject(CodePoint.EXTDTA, concat([Uint8Array.of(0), value.bytes])) });
  }
  return { sqlDta, extDta };
}

function writeFieldDescriptor(writer: DrdaWriter, column: ColumnDescription, externalBlobValue: boolean, value: unknown): void {
  const type = baseType(column.sqlType);
  // Value-dependent descriptors are written inline; all others are constant triples.
  if (value instanceof Uint8Array && isBinaryToCharacterTarget(type)) { writer.writeBytes(binaryCharacterDescriptor); return; }
  switch (type) {
    case 408: case 412:
      if (externalBlobValue) { writer.writeUint8(0xcf); writer.writeUint8(0x80); writer.writeUint8(9); }
      else writer.writeBytes(varcharDescriptor);
      return;
    case 988:
      if (externalBlobValue) { writer.writeUint8(0xc7); writer.writeUint8(0x80); writer.writeUint8(9); }
      else writer.writeBytes(varcharDescriptor);
      return;
    case 448: case 452: case 464: case 472: writer.writeBytes(varcharDescriptor); return;
    case 500: writer.writeUint8(0x05); writer.writeUint8(0); writer.writeUint8(2); return;
    case 496: writer.writeUint8(0x03); writer.writeUint8(0); writer.writeUint8(4); return;
    case 492: writer.writeUint8(0x17); writer.writeUint8(0); writer.writeUint8(8); return;
    case 480:
      if (column.length === 4n) { writer.writeUint8(0x0d); writer.writeUint8(0); writer.writeUint8(4); }
      else { writer.writeUint8(0x0b); writer.writeUint8(0); writer.writeUint8(8); }
      return;
    case 384: writer.writeUint8(0x21); writer.writeUint8(0); writer.writeUint8(10); return;
    case 388: writer.writeUint8(0x23); writer.writeUint8(0); writer.writeUint8(8); return;
    case 392: writer.writeUint8(0x25); writer.writeUint8(0); writer.writeUint8(32); return;
    case 2436:
      if (column.length === 2n) { writer.writeUint8(0x05); writer.writeUint8(0); writer.writeUint8(2); }
      else { writer.writeUint8(0xbf); writer.writeUint8(0); writer.writeUint8(1); }
      return;
    case 404:
      if (externalBlobValue) { writer.writeUint8(0xc9); writer.writeUint8(0x80); writer.writeUint8(9); }
      else { writer.writeUint8(0xc9); writer.writeUint8(0x80); writer.writeUint8(2); }
      return;
    case 468: writer.writeUint8(0x37); writeUint16BytePair(writer, column.length); return;
    case 908: writer.writeUint8(0xc3); writeUint16BytePair(writer, column.length); return;
    case 912: writer.writeUint8(0x27); writeUint16BytePair(writer, column.length); return;
    case 484: case 504:
      writer.writeUint8(0x0f); writer.writeUint8(column.precision & 0xff); writer.writeUint8(column.scale & 0xff);
      return;
    case 996:
      if (column.length === 16n) { writer.writeUint8(0xbb); writer.writeUint8(0); writer.writeUint8(16); }
      else { writer.writeUint8(0xbb); writer.writeUint8(0); writer.writeUint8(8); }
      return;
    default: writer.writeBytes(varcharDescriptor); return;
  }
}

function writeUint16BytePair(writer: DrdaWriter, length: bigint): void {
  writer.writeUint8(Number(length >> 8n & 0xffn));
  writer.writeUint8(Number(length & 0xffn));
}

/** Direct writer version: string -> final DrdaWriter without intermediate buffers. */
function writeParameter(writer: DrdaWriter, column: ColumnDescription, value: unknown, externalBlobValue: ExternalLobValue | undefined): void {
  if (value === null || value === undefined) {
    writer.writeUint8(0xff);
    return;
  }
  if (externalBlobValue !== undefined) {
    writer.writeUint8(0);
    writer.writeUint8(0x02);
    writer.writeBigUint64BE(externalBlobValue.length);
    return;
  }
  const base = baseType(column.sqlType);
  if (value instanceof Uint8Array && isBinaryToCharacterTarget(base)) {
    if (value.length > 0xffff) throw new RangeError("DRDA parameter values cannot exceed 65535 bytes in this SQLDTA encoder");
    writer.writeUint8(0);
    writer.writeUint16BE(value.length);
    writer.writeBytes(value);
    return;
  }
  switch (base) {
    case 500: {
      const v = toBigInt(value);
      const bits = 16n;
      if (v < -(1n << 15n) || v > (1n << 15n) - 1n) throw new RangeError(`Db2 integer value ${v} is outside the signed ${bits}-bit range`);
      writer.writeUint8(0);
      writer.writeInt16LE(Number(v));
      return;
    }
    case 496: {
      const v = toBigInt(value);
      if (v < -(1n << 31n) || v > (1n << 31n) - 1n) throw new RangeError(`Db2 integer value ${v} is outside the signed 32-bit range`);
      writer.writeUint8(0);
      writer.writeInt32LE(Number(v));
      return;
    }
    case 492: {
      const v = toBigInt(value);
      const min = -(1n << 63n);
      const max = (1n << 63n) - 1n;
      if (v < min || v > max) throw new RangeError(`Db2 integer value ${v} is outside the signed 64-bit range`);
      writer.writeUint8(0);
      writer.writeBigInt64LE(v);
      return;
    }
    case 480: {
      writer.writeUint8(0);
      if (column.length === 4n) writer.writeFloat32LE(Number(value));
      else writer.writeFloat64LE(Number(value));
      return;
    }
    case 2436: {
      const enabled = toBoolean(value);
      if (column.length === 2n) {
        writer.writeUint8(0);
        writer.writeInt16LE(enabled ? 1 : 0);
      } else {
        writer.writeUint8(0);
        writer.writeUint8(enabled ? 1 : 0);
      }
      return;
    }
    case 448: case 452: case 456: case 460: case 408: case 464: case 472: case 476: case 988: {
      writeStringParameter(writer, value instanceof Uint8Array ? decodeBinaryText(value) : characterText(value));
      return;
    }
    case 412: {
      const text = value instanceof Uint8Array ? decodeBinaryDbClob(value) : characterText(value);
      writeStringParameter(writer, text);
      return;
    }
    case 468: {
      const text = characterText(value);
      const chars = column.length > 0n ? Number(column.length) : text.length;
      writer.writeUint8(0);
      const count = Math.min(chars, text.length);
      for (let i = 0; i < count; i++) writer.writeUint16BE(text.charCodeAt(i));
      for (let i = count; i < chars; i++) writer.writeUint16BE(0x20);
      return;
    }
    case 384: {
      const bytes = utf8EncoderShared.encode(formatTemporal(value, "date"));
      writer.writeUint8(0);
      writer.writeBytes(bytes);
      return;
    }
    case 388: {
      const bytes = utf8EncoderShared.encode(formatTemporal(value, "time"));
      writer.writeUint8(0);
      writer.writeBytes(bytes);
      return;
    }
    case 392: {
      const bytes = utf8EncoderShared.encode(formatTemporal(value, "timestamp").padEnd(32, " "));
      writer.writeUint8(0);
      writer.writeBytes(bytes);
      return;
    }
    case 912: {
      const bytes = toBytes(value);
      const length = Number(column.length);
      writer.writeUint8(0);
      const count = Math.min(length, bytes.length);
      if (count > 0) writer.writeBytes(bytes.subarray(0, count));
      for (let i = count; i < length; i++) writer.writeUint8(0);
      return;
    }
    case 908: case 404: {
      const bytes = toBytes(value);
      if (bytes.length > 0xffff) throw new RangeError("DRDA parameter values cannot exceed 65535 bytes in this SQLDTA encoder");
      writer.writeUint8(0);
      writer.writeUint16BE(bytes.length);
      writer.writeBytes(bytes);
      return;
    }
    case 484: case 504: {
      const bytes = encodePackedDecimal(value, column.precision, column.scale);
      writer.writeBytes(bytes);
      return;
    }
    case 996: {
      const bytes = encodeDecfloat(typeof value === "boolean" ? (value ? 1 : 0) : value, column.length === 16n ? 16 : 8);
      writer.writeUint8(0);
      writer.writeBytes(bytes);
      return;
    }
    default: {
      writeStringParameter(writer, value instanceof Uint8Array ? decodeBinaryText(value) : characterText(value));
      return;
    }
  }
}

function writeStringParameter(writer: DrdaWriter, text: string): void {
  if (text.length > 0xffff) throw new RangeError("DRDA character parameter exceeds 65535 UTF-16 code units");
  writer.writeUint8(0);
  writer.writeUint16BE(text.length);
  // Direct UTF-16BE without an intermediate buffer (charCodeAt -> BE u16).
  writer.writeUtf16Be(text);
}

function externalLob(column: ColumnDescription, value: unknown): ExternalLobValue | undefined {
  const type = baseType(column.sqlType);
  if (isDb2ByteStream(value)) {
    if (type !== 404 && type !== 408 && type !== 412 && type !== 988) throw new TypeError(`LOB streams can only be bound to BLOB, CLOB, DBCLOB, or XML parameters, not ${column.name}`);
    if (!value.source || typeof value.source[Symbol.asyncIterator] !== "function") throw new TypeError("LOB source must be an AsyncIterable<Uint8Array>");
    const length = typeof value.length === "bigint" ? value.length : Number.isSafeInteger(value.length) ? BigInt(value.length) : -1n;
    if (length < 0n || length > 0xffff_ffff_ffff_fffen) throw new RangeError("LOB length must be a non-negative integer that fits in the DRDA length field");
    if (type === 404) return { blobStream: value, length };
    if (type === 408 || type === 412) return { textLobStream: value, length, type };
    return { textLobStream: value, length, type: 988 };
  }
  // Match the existing UTF-8 text-stream binding once inline length no longer fits.
  if ((type === 408 || type === 412 || type === 988) && typeof value === "string" && value.length > 0xffff) {
    const bytes = utf8EncoderShared.encode(value);
    return { bytes, length: BigInt(bytes.length) };
  }
  if (type !== 404 || value === null || value === undefined) return undefined;
  const bytes = toBytes(value);
  return { bytes, length: BigInt(bytes.length) };
}

function isDb2ByteStream(value: unknown): value is Db2ByteStream {
  return typeof value === "object" && value !== null && "source" in value && "length" in value;
}

function decodeBinaryText(value: Uint8Array): string {
  return utf8FatalDecoderShared.decode(value);
}

function decodeBinaryDbClob(value: Uint8Array): string {
  let text = "";
  for (let offset = 0; offset + 1 < value.length; offset += 2)
    text += String.fromCharCode((value[offset]! << 8) | value[offset + 1]!);
  return text;
}

function encodePackedDecimal(value: unknown, precision: number, scale: number): Uint8Array {
  if (!Number.isInteger(precision) || precision < 1 || precision > 31 || !Number.isInteger(scale) || scale < 0 || scale > precision)
    throw new Error(`invalid Db2 decimal precision (${precision}) or scale (${scale})`);
  let text = typeof value === "boolean" ? (value ? "1" : "0")
    : typeof value === "number" ? numberToPlainDecimal(value) : String(value).trim();
  const negative = text.startsWith("-");
  if (text.startsWith("-") || text.startsWith("+")) text = text.slice(1);
  const parts = text.split(".");
  if (parts.length > 2 || (parts.length === 1 && parts[0] === "") || parts.every(part => part === ""))
    throw new TypeError(`invalid Db2 decimal value '${String(value)}'`);
  const whole = parts[0] || "0";
  const fraction = parts[1] || "";
  if (!/^\d+$/u.test(whole) || (fraction !== "" && !/^\d+$/u.test(fraction)))
    throw new TypeError(`invalid Db2 decimal value '${String(value)}'`);
  const integerDigits = precision - scale;
  const significantWhole = whole.replace(/^0+/u, "");
  if (significantWhole.length > integerDigits)
    throw new RangeError(`Db2 decimal value '${String(value)}' exceeds DECIMAL(${precision},${scale})`);
  const bytesCount = Math.floor((precision + 2) / 2);
  const result = new Uint8Array(1 + bytesCount); result[0] = 0;
  const nibbleCount = bytesCount * 2; const start = (nibbleCount - 1) - precision;
  for (let i = 0; i < precision; i++) {
    let digit = 0;
    if (i < integerDigits) { const index = whole.length - integerDigits + i; if (index >= 0) digit = whole.charCodeAt(index) - 48; }
    else if (i - integerDigits < fraction.length) digit = fraction.charCodeAt(i - integerDigits) - 48;
    setNibble(result, 1 + ((start + i) >> 1), (start + i) % 2 === 0, digit);
  }
  const signPosition = nibbleCount - 1;
  setNibble(result, 1 + (signPosition >> 1), signPosition % 2 === 0, negative ? 0x0d : 0x0c);
  return result;
}

function setNibble(bytes: Uint8Array, index: number, high: boolean, digit: number): void { bytes[index] = high ? bytes[index]! | digit << 4 : bytes[index]! | digit; }

function numberToPlainDecimal(value: number): string {
  if (!Number.isFinite(value)) throw new TypeError(`invalid Db2 decimal value '${String(value)}'`);
  const raw = value.toString().toLowerCase();
  const exponentIndex = raw.indexOf("e");
  if (exponentIndex < 0) return raw;
  const mantissa = raw.slice(0, exponentIndex);
  const exponent = Number(raw.slice(exponentIndex + 1));
  const negative = mantissa.startsWith("-");
  const unsigned = negative || mantissa.startsWith("+") ? mantissa.slice(1) : mantissa;
  const [whole = "", fraction = ""] = unsigned.split(".");
  const digits = whole + fraction;
  const point = whole.length + exponent;
  const expanded = point <= 0
    ? `0.${"0".repeat(-point)}${digits}`
    : point >= digits.length
      ? `${digits}${"0".repeat(point - digits.length)}`
      : `${digits.slice(0, point)}.${digits.slice(point)}`;
  return `${negative ? "-" : ""}${expanded}`;
}

function toBigInt(value: unknown): bigint {
  if (typeof value === "bigint") return value;
  if (typeof value === "boolean") return value ? 1n : 0n;
  if (typeof value === "number" && Number.isFinite(value)) return BigInt(Math.trunc(value));
  if (typeof value === "string") return truncateNumericText(value);
  throw new Error(`cannot encode ${String(value)} as a Db2 integer`);
}

function truncateNumericText(value: string): bigint {
  const text = value.trim();
  const match = /^([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/u.exec(text);
  if (!match) throw new TypeError(`cannot encode ${JSON.stringify(value)} as a Db2 integer`);
  const whole = match[2] ?? "";
  const fraction = match[3] ?? "";
  if (whole.length === 0 && fraction.length === 0)
    throw new TypeError(`cannot encode ${JSON.stringify(value)} as a Db2 integer`);

  const significant = `${whole}${fraction}`.replace(/^0+/u, "");
  if (significant.length === 0) return 0n;
  const exponent = BigInt(match[4] ?? "0");
  const fractionalDigits = BigInt(fraction.length);
  if (exponent > fractionalDigits + 19n)
    throw new RangeError(`cannot encode ${JSON.stringify(value)} as a Db2 integer: signed BIGINT overflow`);
  const integerLength = BigInt(significant.length) - fractionalDigits + exponent;
  if (integerLength <= 0n) return 0n;
  if (integerLength > 19n)
    throw new RangeError(`cannot encode ${JSON.stringify(value)} as a Db2 integer: signed BIGINT overflow`);

  const keptDigits = Number(integerLength);
  const digits = significant.slice(0, keptDigits) + "0".repeat(Math.max(0, keptDigits - significant.length));
  return BigInt(`${match[1] === "-" ? "-" : ""}${digits}`);
}

function toBoolean(value: unknown): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value === "bigint") return value !== 0n;
  if (typeof value === "number") {
    if (!Number.isInteger(value)) throw new TypeError(`cannot encode ${String(value)} as a Db2 boolean`);
    return value !== 0;
  }
  if (typeof value === "string") {
    const text = value.trim();
    const normalized = text.toLowerCase();
    if (normalized === "true" || normalized === "yes" || normalized === "y" || normalized === "on") return true;
    if (normalized === "false" || normalized === "no" || normalized === "n" || normalized === "off") return false;
    if (!/^[+-]?\d+$/u.test(text)) throw new TypeError(`cannot encode ${JSON.stringify(value)} as a Db2 boolean`);
    return /[1-9]/u.test(text);
  }
  throw new TypeError(`cannot encode ${String(value)} as a Db2 boolean`);
}

function formatTemporal(value: unknown, kind: "date" | "time" | "timestamp"): string {
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) throw new TypeError("cannot convert an invalid Date to a Db2 " + kind);
    const iso = value.toISOString();
    if (kind === "date") return iso.slice(0, 10);
    if (kind === "time") return iso.slice(11, 19);
    return iso.slice(0, 10) + "-" + iso.slice(11, 19).replaceAll(":", ".") + "." + iso.slice(20, 23) + "000";
  }
  if (typeof value !== "string") throw new TypeError("cannot convert " + String(value) + " to a Db2 " + kind);
  const text = value.trim();
  if (kind === "date") {
    const date = parseDb2DateText(text);
    if (date) return date;
    const timestamp = parseDb2TimestampText(text);
    if (timestamp) return timestamp.slice(0, 10);
  }
  if (kind === "time") {
    const time = parseDb2TimeText(text);
    if (time) return time;
    const timestamp = parseDb2TimestampText(text);
    if (timestamp) return timestamp.slice(11, 19).replaceAll(".", ":");
  }
  if (kind === "timestamp") {
    const timestamp = parseDb2TimestampText(text);
    if (timestamp) return timestamp;
  }
  throw new TypeError("cannot convert " + JSON.stringify(value) + " to a Db2 " + kind);
}

function parseDb2DateText(text: string): string | undefined {
  const iso = /^(\d{4})-(\d{1,2})-(\d{1,2})$/u.exec(text);
  const us = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/u.exec(text);
  if (!iso && !us) return undefined;
  const year = Number(iso?.[1] ?? us?.[3]);
  const month = Number(iso?.[2] ?? us?.[1]);
  const day = Number(iso?.[3] ?? us?.[2]);
  if (year < 1 || year > 9999 || month < 1 || month > 12 || day < 1) return undefined;
  const daysPerMonth = [31, isLeapYear(year) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (day > daysPerMonth[month - 1]!) return undefined;
  return String(year).padStart(4, "0") + "-" + String(month).padStart(2, "0") + "-" + String(day).padStart(2, "0");
}

function isLeapYear(year: number): boolean {
  return year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
}

function parseDb2TimeText(text: string): string | undefined {
  const match = /^(\d{1,2})[:.](\d{2})(?:[:.](\d{2}))?$/u.exec(text);
  if (!match) return undefined;
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  const second = Number(match[3] ?? 0);
  if (minute > 59 || second > 59 || hour > 24 || (hour === 24 && (minute !== 0 || second !== 0))) return undefined;
  return String(hour).padStart(2, "0") + ":" + String(minute).padStart(2, "0") + ":" + String(second).padStart(2, "0");
}

function parseDb2TimestampText(text: string): string | undefined {
  const dateOnly = parseDb2DateText(text);
  if (dateOnly) return dateOnly + "-00.00.00.000000";
  const match = /^(\d{4}-\d{1,2}-\d{1,2})[ T-](\d{1,2})[:.](\d{2})[:.](\d{2})(?:\.(\d{1,12}))?$/u.exec(text);
  if (!match) return undefined;
  const date = parseDb2DateText(match[1]!);
  const hour = Number(match[2]);
  const minute = Number(match[3]);
  const second = Number(match[4]);
  if (!date || hour > 23 || minute > 59 || second > 59) return undefined;
  const fraction = match[5] || "000000";
  return date + "-" + String(hour).padStart(2, "0") + "." + String(minute).padStart(2, "0") + "." + String(second).padStart(2, "0") + "." + fraction;
}

function toBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  if (typeof value === "string") return utf8EncoderShared.encode(value);
  return utf8EncoderShared.encode(String(value));
}

function normalizeParameterSource(column: ColumnDescription, value: unknown): unknown {
  if (value === null || value === undefined || isDb2ByteStream(value)) return value;
  const type = baseType(column.sqlType);
  const binary = value instanceof Uint8Array;
  if ([500, 496, 492, 480, 484, 504, 996, 2436].includes(type)) {
    if (binary) throw new TypeError(`a binary value cannot be bound to Db2 SQL type ${type}`);
    return value;
  }
  if ([912, 908, 404].includes(type)) {
    if (binary) return value;
    if (typeof value !== "string") throw new TypeError(`Db2 binary SQL type ${type} requires Uint8Array or hexadecimal string input`);
    return encodeHexBinary(column, value);
  }
  if ([408, 412, 988].includes(type)) {
    if (!binary && typeof value !== "string")
      throw new TypeError(`Db2 text LOB/XML SQL type ${type} requires string or Uint8Array input`);
    return value;
  }
  if ([464, 468, 472, 476].includes(type) && binary)
    throw new TypeError(`a binary value cannot be bound to Db2 graphic SQL type ${type}`);
  return value;
}

function encodeHexBinary(column: ColumnDescription, value: string): Uint8Array {
  if (value.length % 2 !== 0) throw new TypeError(`Db2 binary SQL type ${baseType(column.sqlType)} requires an even-length hexadecimal string`);
  const bytes = new Uint8Array(value.length / 2);
  for (let index = 0; index < value.length; index += 2) {
    const high = hexNibble(value.charCodeAt(index));
    const low = hexNibble(value.charCodeAt(index + 1));
    if (high < 0 || low < 0) throw new TypeError(`Db2 binary SQL type ${baseType(column.sqlType)} requires an even-length hexadecimal string`);
    bytes[index / 2] = (high << 4) | low;
  }
  if (column.length > 0n && BigInt(bytes.length) > column.length)
    throw new TypeError(`Db2 binary SQL type ${baseType(column.sqlType)} input exceeds its ${column.length}-byte target length`);
  return bytes;
}

function hexNibble(code: number): number {
  if (code >= 0x30 && code <= 0x39) return code - 0x30;
  if (code >= 0x41 && code <= 0x46) return code - 0x41 + 10;
  if (code >= 0x61 && code <= 0x66) return code - 0x61 + 10;
  return -1;
}

function isBinaryToCharacterTarget(type: number): boolean {
  return type === 448 || type === 452 || type === 456 || type === 408 || type === 988;
}

function characterText(value: unknown): string {
  if (typeof value === "boolean") return value ? "1" : "0";
  return String(value);
}

function baseType(sqlType: number): number { return sqlType % 2 === 1 ? sqlType - 1 : sqlType; }
function concat(parts: readonly Uint8Array[]): Uint8Array {
  const output = new Uint8Array(parts.reduce((sum, item) => sum + item.length, 0)); let offset = 0;
  for (const item of parts) { output.set(item, offset); offset += item.length; }
  return output;
}
