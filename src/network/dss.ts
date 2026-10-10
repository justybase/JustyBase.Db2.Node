import { CodePoint } from "./codepoint.js";

export const DSS_MAGIC = 0xd0;
export const DSS_TYPE_REQUEST = 1;
export const DSS_TYPE_REPLY = 2;
export const DSS_TYPE_OBJECT = 3;
export const DSS_TYPE_COMM = 4;
export const DSS_FLAG_CHAINED = 0x40;
export const DSS_FLAG_ERROR = 0x20;
export const DSS_FLAG_SAME_ID = 0x10;
const CONTINUATION = 0x8000;
const MAX_SEGMENT = 0x7fff;

export type DssByteWriter = { write(chunk: Uint8Array, callback: (error?: Error | null) => void): boolean };

export type DssHeader = { length: number; type: number; chained: boolean; sameId: boolean; hasError: boolean; correlationId: number };
export type DssPacket = { header: DssHeader; codePoint: number; payload: Uint8Array; moreData: false };
export type ByteReader = {
  readExact(length: number): Promise<Uint8Array>;
  /** Optional zero-copy bulk read directly into a destination buffer. */
  readInto?(destination: Uint8Array, offset: number, length: number): Promise<void>;
};

/** Resource limits: a corrupt/malicious server must not force unbounded allocation. */
export const DSS_LIMITS = {
  /** Maximum single DSS object body (payload across all continuation segments). */
  maxDssObjectBytes: 256 * 1024 * 1024,
  /** Maximum accumulated bytes in one reply chain (readReplyChain). */
  maxReplyChainBytes: 512 * 1024 * 1024,
  /** Maximum DSS packets in one reply chain. */
  maxReplyChainPackets: 2048,
  /** Maximum single DDM object body. */
  maxDdmObjectBytes: 256 * 1024 * 1024,
  /** Maximum materialized LOB value. */
  maxLobMaterializationBytes: 16 * 1024 * 1024,
  /** Maximum retained data in one materialized query result. */
  maxMaterializedResultBytes: 32 * 1024 * 1024,
};

export function buildDssHeader(payloadLength: number, type: number, chained: boolean, sameId: boolean, hasError: boolean, correlationId: number): Uint8Array {
  const header = new Uint8Array(6); const view = new DataView(header.buffer);
  view.setUint16(0, payloadLength + 6, false); header[2] = DSS_MAGIC;
  let flags = type & 0x0f;
  if (chained) flags |= DSS_FLAG_CHAINED;
  if (sameId) flags |= DSS_FLAG_SAME_ID;
  if (hasError) flags |= DSS_FLAG_ERROR;
  header[3] = flags; view.setUint16(4, correlationId, false);
  return header;
}

export function encodeRequestDss(payload: Uint8Array, currentId: number, nextHasSameId: boolean, lastPacket: boolean): { bytes: Uint8Array; nextId: number } {
  if (payload.length < 4) throw new Error("payload too short for DDM object");
  const codePoint = new DataView(payload.buffer, payload.byteOffset, payload.byteLength).getUint16(2, false);
  const type = codePoint === CodePoint.SQLSTT || codePoint === CodePoint.SQLATTR || codePoint === CodePoint.SQLDTA || codePoint === CodePoint.EXTDTA ? DSS_TYPE_OBJECT : DSS_TYPE_REQUEST;
  // Small request fast path: single DSS segment, one exact-size allocation,
  // no chunks array, no reduce, no second copy layer.
  if (payload.length + 6 <= MAX_SEGMENT) {
    const bytes = new Uint8Array(payload.length + 6);
    const view = new DataView(bytes.buffer);
    view.setUint16(0, bytes.length, false);
    bytes[2] = DSS_MAGIC;
    bytes[3] = type | (!lastPacket ? DSS_FLAG_CHAINED : 0) | (nextHasSameId ? DSS_FLAG_SAME_ID : 0);
    view.setUint16(4, currentId, false);
    bytes.set(payload, 6);
    return { bytes, nextId: nextHasSameId ? currentId : (currentId + 1) & 0xffff };
  }
  // Large request: precompute total length, allocate once, write headers +
  // payload slices directly (no intermediate chunks array).
  let segments = 0;
  let remaining = payload.length;
  let first = true;
  while (remaining > 0) {
    const capacity = first ? MAX_SEGMENT - 6 : MAX_SEGMENT - 2;
    const count = Math.min(capacity, remaining);
    remaining -= count;
    segments++;
    first = false;
  }
  // Each continuation after the first adds a 2-byte header.
  const totalLength = payload.length + 6 + (segments - 1) * 2;
  if (payload.length > DSS_LIMITS.maxDssObjectBytes)
    throw new Error(`DSS request of ${payload.length} bytes exceeds the ${DSS_LIMITS.maxDssObjectBytes}-byte limit`);
  const bytes = new Uint8Array(totalLength);
  const view = new DataView(bytes.buffer);
  let offset = 0;
  let payloadOffset = 0;
  first = true;
  while (payloadOffset < payload.length) {
    const headerSize = first ? 6 : 2;
    const count = Math.min(first ? MAX_SEGMENT - 6 : MAX_SEGMENT - 2, payload.length - payloadOffset);
    const moreSegments = payloadOffset + count < payload.length;
    view.setUint16(offset, (headerSize + count) | (moreSegments ? CONTINUATION : 0), false);
    if (first) {
      bytes[offset + 2] = DSS_MAGIC;
      bytes[offset + 3] = type | (!lastPacket ? DSS_FLAG_CHAINED : 0) | (nextHasSameId ? DSS_FLAG_SAME_ID : 0);
      view.setUint16(offset + 4, currentId, false);
    }
    offset += headerSize;
    bytes.set(payload.subarray(payloadOffset, payloadOffset + count), offset);
    offset += count;
    payloadOffset += count;
    first = false;
  }
  return { bytes, nextId: nextHasSameId ? currentId : (currentId + 1) & 0xffff };
}

/** Compatibility name for the bounded EXTDTA LOB writer. */
export async function writeExternalBlobDss(
  destination: DssByteWriter,
  source: AsyncIterable<Uint8Array>,
  dataLength: number | bigint,
  currentId: number,
  nextHasSameId: boolean,
  lastPacket: boolean,
  leaveOpen = true,
): Promise<number> {
  return writeExternalLobDss(destination, source, dataLength, currentId, nextHasSameId, lastPacket, leaveOpen);
}

/** Writes a LOB EXTDTA object from an async byte source using bounded DSS segments. */
export async function writeExternalLobDss(
  destination: DssByteWriter,
  source: AsyncIterable<Uint8Array>,
  dataLength: number | bigint,
  currentId: number,
  nextHasSameId: boolean,
  lastPacket: boolean,
  leaveOpen = true,
): Promise<number> {
  if (!destination || typeof destination.write !== "function") throw new TypeError("destination must be writable");
  if (!source || typeof source[Symbol.asyncIterator] !== "function") throw new TypeError("LOB source must be an AsyncIterable<Uint8Array>");
  const length = typeof dataLength === "bigint" ? dataLength : Number.isSafeInteger(dataLength) ? BigInt(dataLength) : -1n;
  if (length < 0n || length > 0xffff_ffff_ffff_fffen) throw new RangeError("LOB length must be a non-negative integer that fits in the DRDA length field");

  const bodyLength = length + 1n;
  let ddmHeader: Uint8Array;
  if (bodyLength + 4n <= 0x7fffn) {
    ddmHeader = new Uint8Array(4);
    new DataView(ddmHeader.buffer).setUint16(0, Number(bodyLength + 4n), false);
    new DataView(ddmHeader.buffer).setUint16(2, CodePoint.EXTDTA, false);
  } else if (bodyLength <= 0x7fff_ffffn) {
    ddmHeader = new Uint8Array(8);
    const view = new DataView(ddmHeader.buffer);
    view.setUint16(0, 0x8008, false); view.setUint16(2, CodePoint.EXTDTA, false);
    view.setUint32(4, Number(bodyLength), false);
  } else {
    ddmHeader = new Uint8Array(12);
    const view = new DataView(ddmHeader.buffer);
    view.setUint16(0, 0x800c, false); view.setUint16(2, CodePoint.EXTDTA, false);
    view.setBigUint64(4, bodyLength, false);
  }
  const prefix = new Uint8Array(ddmHeader.length + 1);
  prefix.set(ddmHeader); // The EXTDTA status byte following the DDM header is zero.
  let prefixOffset = 0;
  let remaining = BigInt(prefix.length) + length;
  let first = true;
  let sourceRemaining = length;
  let chunk: Uint8Array | undefined;
  let chunkOffset = 0;
  const iterator = length > 0n || !leaveOpen ? source[Symbol.asyncIterator]() : undefined;

  try {
    while (remaining > 0n) {
      const headerLength = first ? 6 : 2;
      const count = Number(remaining < BigInt(MAX_SEGMENT - headerLength) ? remaining : BigInt(MAX_SEGMENT - headerLength));
      const more = BigInt(count) < remaining;
      const frame = new Uint8Array(headerLength + count);
      const view = new DataView(frame.buffer);
      view.setUint16(0, frame.length | (more ? CONTINUATION : 0), false);
      if (first) {
        frame[2] = DSS_MAGIC;
        frame[3] = DSS_TYPE_OBJECT | (!lastPacket ? DSS_FLAG_CHAINED : 0) | (nextHasSameId ? DSS_FLAG_SAME_ID : 0);
        view.setUint16(4, currentId, false);
      }

      let written = 0;
      if (prefixOffset < prefix.length) {
        const take = Math.min(count, prefix.length - prefixOffset);
        frame.set(prefix.subarray(prefixOffset, prefixOffset + take), headerLength);
        prefixOffset += take;
        written += take;
      }
      while (written < count) {
        if (sourceRemaining === 0n) throw new Error("LOB source contains more bytes than its declared length");
        if (!chunk || chunkOffset === chunk.length) {
          const next = await iterator!.next();
          if (next.done) throw new Error("LOB source ended before its declared length");
          if (!(next.value instanceof Uint8Array)) throw new TypeError("LOB source chunks must be Uint8Array values");
          chunk = next.value;
          chunkOffset = 0;
          if (chunk.length === 0) continue;
        }
        const available = Math.min(count - written, chunk.length - chunkOffset);
        const take = Number(BigInt(available) < sourceRemaining ? BigInt(available) : sourceRemaining);
        frame.set(chunk.subarray(chunkOffset, chunkOffset + take), headerLength + written);
        chunkOffset += take;
        written += take;
        sourceRemaining -= BigInt(take);
      }
      await writeSocketFrame(destination, frame);
      remaining -= BigInt(count);
      first = false;
    }
    return nextHasSameId ? currentId : (currentId + 1) & 0xffff;
  } finally {
    if (!leaveOpen && iterator?.return) await iterator.return();
  }
}

function writeSocketFrame(destination: DssByteWriter, frame: Uint8Array): Promise<void> {
  return new Promise((resolve, reject) => destination.write(frame, (error) => error ? reject(error) : resolve()));
}

export function readDdmObject(body: Uint8Array): { codePoint: number; payload: Uint8Array } {
  if (body.length < 4) throw new Error(`invalid DDM object length: DSS body holds ${body.length} bytes`);
  if (body.length > DSS_LIMITS.maxDdmObjectBytes + 12) throw new Error(`DDM object of ${body.length} bytes exceeds the ${DSS_LIMITS.maxDdmObjectBytes}-byte limit`);
  const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
  const encodedLength = view.getUint16(0, false); const codePoint = view.getUint16(2, false);
  if ((encodedLength & 0x8000) === 0) {
    if (encodedLength < 4) throw new Error(`invalid DDM object length: ${encodedLength}`);
    if (encodedLength > body.length) throw new Error(`DDM object length ${encodedLength} exceeds DSS frame payload capacity ${body.length}`);
    return { codePoint, payload: body.subarray(4, encodedLength) };
  }
  const extendedBytes = (encodedLength & 0x7fff) - 4;
  if (extendedBytes === 0) return { codePoint, payload: body.subarray(4) };
  if (extendedBytes < 0 || extendedBytes > 8 || 4 + extendedBytes > body.length) throw new Error(`invalid DDM extended length field 0x${encodedLength.toString(16).padStart(4, "0")}`);
  let dataLength = 0n; for (const value of body.subarray(4, 4 + extendedBytes)) dataLength = (dataLength << 8n) | BigInt(value);
  if (dataLength > BigInt(DSS_LIMITS.maxDdmObjectBytes)) throw new Error(`DDM extended length ${dataLength} exceeds the ${DSS_LIMITS.maxDdmObjectBytes}-byte limit`);
  const capacity = BigInt(body.length - 4 - extendedBytes);
  if (dataLength > capacity) throw new Error(`DDM extended length ${dataLength} exceeds DSS payload ${capacity}`);
  const start = 4 + extendedBytes; return { codePoint, payload: body.subarray(start, start + Number(dataLength)) };
}

export async function readDss(reader: ByteReader): Promise<DssPacket> {
  const rawHeader = await reader.readExact(6);
  if (rawHeader.length < 6) throw new Error(`invalid DSS packet: truncated 6-byte header (got ${rawHeader.length} bytes)`);
  if (rawHeader[2] !== DSS_MAGIC) throw new Error(`invalid DSS packet: missing 0xD0 magic identifier (got 0x${rawHeader[2]!.toString(16).padStart(2, "0")})`);
  const view = new DataView(rawHeader.buffer, rawHeader.byteOffset, rawHeader.byteLength);
  const length = view.getUint16(0, false); const flags = rawHeader[3]!;
  const header: DssHeader = { length, type: flags & 0x0f, chained: (flags & DSS_FLAG_CHAINED) !== 0, sameId: (flags & DSS_FLAG_SAME_ID) !== 0, hasError: (flags & DSS_FLAG_ERROR) !== 0, correlationId: view.getUint16(4, false) };
  const firstLength = length & ~CONTINUATION;
  if (firstLength < 6) throw new Error(`invalid DSS frame length ${firstLength}: less than header size 6`);
  if (firstLength - 6 > DSS_LIMITS.maxDssObjectBytes) throw new Error(`DSS frame of ${firstLength - 6} bytes exceeds the ${DSS_LIMITS.maxDssObjectBytes}-byte limit`);
  const continued = (length & CONTINUATION) !== 0;
  if (!continued) {
    // Single-segment fast path: no second copy (parts[0] used directly).
    // Normalize Buffer -> Uint8Array view (zero-copy) to preserve the public
    // binary type (Buffer is a subclass but strict deepEqual distinguishes).
    const raw = await reader.readExact(firstLength - 6);
    if (raw.length !== firstLength - 6) throw new Error(`DSS body truncated: expected ${firstLength - 6} bytes, got ${raw.length}`);
    const body = toUint8View(raw);
    const object = readDdmObject(body);
    // readDdmObject returns subarray views; ensure Uint8Array type as well.
    const payload = toUint8View(object.payload);
    return { header, codePoint: object.codePoint, payload, moreData: false };
  }
  // Continuation path: growable destination, socket queue -> final body
  // (no temporary per-segment copies) via readInto when available.
  let total = 0;
  let body = new Uint8Array(Math.min(Math.max(firstLength - 6, 32768), DSS_LIMITS.maxDssObjectBytes));
  const append = async (length: number): Promise<void> => {
    if (total + length > DSS_LIMITS.maxDssObjectBytes)
      throw new Error(`DSS continuation chain exceeds the ${DSS_LIMITS.maxDssObjectBytes}-byte limit`);
    if (total + length > body.length) {
      const grown = Math.min(
        DSS_LIMITS.maxDssObjectBytes,
        Math.max(total + length, body.length * 2),
      );
      const next = new Uint8Array(grown);
      next.set(body.subarray(0, total), 0);
      body = next;
    }
    if (reader.readInto) await reader.readInto(body, total, length);
    else body.set(await reader.readExact(length), total);
    total += length;
  };
  await append(firstLength - 6);
  let more = true;
  let segments = 1;
  while (more) {
    if (++segments > 65536) throw new Error("DSS continuation chain exceeds the supported segment count");
    const continuationHeader = await reader.readExact(2);
    const continuationLength = new DataView(continuationHeader.buffer, continuationHeader.byteOffset, 2).getUint16(0, false);
    more = (continuationLength & CONTINUATION) !== 0;
    const segmentLength = continuationLength & ~CONTINUATION;
    if (segmentLength < 2) throw new Error(`invalid DSS continuation length ${segmentLength}`);
    await append(segmentLength - 2);
  }
  const finalBody = total === body.length ? body : body.subarray(0, total);
  const object = readDdmObject(finalBody);
  return { header, codePoint: object.codePoint, payload: toUint8View(object.payload), moreData: false };
}

function toUint8View(data: Uint8Array): Uint8Array {
  // Buffer.subarray() returns a Buffer; wrap the same memory as a plain
  // Uint8Array view so strict equality and API types stay stable (zero-copy).
  if (!Buffer.isBuffer(data)) return data;
  return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
}

export function encodeReplyDss(ddmObject: Uint8Array, chained: boolean, correlationId: number): Uint8Array {
  const parts: Uint8Array[] = []; let offset = 0; let first = true;
  while (offset < ddmObject.length) {
    const headerSize = first ? 6 : 2;
    const count = Math.min(first ? MAX_SEGMENT - 6 : MAX_SEGMENT - 2, ddmObject.length - offset);
    const more = offset + count < ddmObject.length; const header = new Uint8Array(headerSize); const view = new DataView(header.buffer);
    view.setUint16(0, headerSize + count | (more ? CONTINUATION : 0), false);
    if (first) { header[2] = DSS_MAGIC; header[3] = DSS_TYPE_REPLY | (chained ? DSS_FLAG_CHAINED : 0); view.setUint16(4, correlationId, false); }
    parts.push(header, ddmObject.subarray(offset, offset + count)); offset += count; first = false;
  }
  const size = parts.reduce((sum, item) => sum + item.length, 0); const output = new Uint8Array(size); offset = 0;
  for (const part of parts) { output.set(part, offset); offset += part.length; }
  return output;
}
