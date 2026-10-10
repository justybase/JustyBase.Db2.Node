import { Db2ResourceLimitError } from "./errors.js";

/** A process-wide spool quota, reserved before disk writes. */
export class Db2ResourceBudget {
  private used = 0;
  constructor(readonly limitBytes = 10 * 1024 ** 3) {
    if (!Number.isSafeInteger(limitBytes) || limitBytes < 1) throw new RangeError("invalid spool byte limit");
  }
  get usedBytes(): number { return this.used; }
  reserve(bytes: number): void {
    if (!Number.isSafeInteger(bytes) || bytes < 0) throw new RangeError("invalid resource reservation");
    if (bytes > this.limitBytes - this.used) throw new Db2ResourceLimitError("DB2 process spool quota exceeded");
    this.used += bytes;
  }
  release(bytes: number): void {
    if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > this.used) throw new RangeError("invalid resource release");
    this.used -= bytes;
  }
}

export const db2SpoolBudget = new Db2ResourceBudget();
export const DB2_RESOURCE_DEFAULTS = Object.freeze({ chunkBytes: 64 * 1024, receiveBufferBytes: 4 * 1024 ** 2,
  materializedLobBytes: 16 * 1024 ** 2, materializedResultBytes: 32 * 1024 ** 2, maxStreamedLobBytes: 1024 ** 3 });
