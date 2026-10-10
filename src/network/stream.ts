import type { ByteReader, DssHeader } from "./dss.js";
import { Db2Error, Db2ResourceLimitError } from "../errors.js";
import { DB2_RESOURCE_DEFAULTS } from "../resources.js";

/** One DSS/DDM payload. Consume it fully before reading the next packet. */
export class Db2DssStream {
  private segments = 1;
  private total = 0;
  private done = false;
  private remaining: bigint | undefined;
  codePoint = 0;
  private constructor(private reader: ByteReader, readonly header: DssHeader,
    private segmentRemaining: number, private continued: boolean, private readonly limit: number) {}

  static async open(reader: ByteReader, limit = DB2_RESOURCE_DEFAULTS.maxStreamedLobBytes): Promise<Db2DssStream> {
    if (!Number.isSafeInteger(limit) || limit < 1) throw new RangeError("invalid streamed payload limit");
    const raw = await reader.readExact(6);
    const view = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
    const length = view.getUint16(0, false);
    if (raw[2] !== 0xd0 || (length & 0x7fff) < 6) throw new Db2Error("Invalid DSS header", "protocol");
    const flags = raw[3]!;
    const packet = new Db2DssStream(reader, { length, type: flags & 15, chained: !!(flags & 64),
      sameId: !!(flags & 16), hasError: !!(flags & 32), correlationId: view.getUint16(4, false) },
      (length & 0x7fff) - 6, !!(length & 0x8000), limit);
    const ddm = await packet.readHeaderBytes(4);
    const encoded = (ddm[0]! << 8) | ddm[1]!;
    packet.codePoint = (ddm[2]! << 8) | ddm[3]!;
    if (!(encoded & 0x8000)) {
      if (encoded < 4) throw new Db2Error("Invalid DDM length", "protocol");
      packet.remaining = BigInt(encoded - 4);
    } else {
      const width = (encoded & 0x7fff) - 4;
      if (width < 0 || width > 8) throw new Db2Error("Invalid extended DDM length", "protocol");
      if (width) {
        let length = 0n;
        for (const byte of await packet.readHeaderBytes(width)) length = (length << 8n) | BigInt(byte);
        packet.remaining = length;
      }
    }
    if (packet.remaining !== undefined && packet.remaining > BigInt(limit)) throw new Db2ResourceLimitError("DDM payload exceeds streamed limit");
    return packet;
  }
  private async nextSegment(): Promise<boolean> {
    if (this.segmentRemaining > 0) return true;
    if (!this.continued) return false;
    if (++this.segments > 65536) throw new Db2ResourceLimitError("DSS continuation count exceeds limit");
    const raw = await this.reader.readExact(2);
    const length = (raw[0]! << 8) | raw[1]!;
    if ((length & 0x7fff) <= 2) throw new Db2Error("Empty or invalid DSS continuation", "protocol");
    this.continued = !!(length & 0x8000);
    this.segmentRemaining = (length & 0x7fff) - 2;
    return true;
  }
  private async readHeaderBytes(count: number): Promise<Uint8Array> {
    const bytes = new Uint8Array(count); let offset = 0;
    while (offset < count) {
      if (!await this.nextSegment()) throw new Db2Error("Truncated DDM header", "protocol");
      const take = Math.min(count - offset, this.segmentRemaining);
      bytes.set(await this.reader.readExact(take), offset);
      offset += take; this.segmentRemaining -= take;
    }
    return bytes;
  }
  async nextChunk(): Promise<Uint8Array | undefined> {
    if (this.done) return undefined;
    if (this.remaining === 0n) {
      if (await this.nextSegment()) throw new Db2Error("Trailing DSS bytes after DDM payload", "protocol");
      this.done = true; return undefined;
    }
    if (!await this.nextSegment()) {
      if (this.remaining !== undefined && this.remaining !== 0n) throw new Db2Error("Truncated DDM payload", "protocol");
      this.done = true; return undefined;
    }
    const count = Math.min(this.segmentRemaining, DB2_RESOURCE_DEFAULTS.chunkBytes,
      this.remaining === undefined ? Number.MAX_SAFE_INTEGER : Number(this.remaining));
    if (count > this.limit - this.total) throw new Db2ResourceLimitError("DSS payload exceeds streamed limit");
    const chunk = await this.reader.readExact(count);
    this.segmentRemaining -= count; this.total += count;
    if (this.remaining !== undefined) this.remaining -= BigInt(count);
    return chunk;
  }
}
