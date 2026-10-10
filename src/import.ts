import { createReadStream } from "node:fs";
import { mkdtemp, open, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { db2SpoolBudget, Db2ResourceBudget, DB2_RESOURCE_DEFAULTS } from "./resources.js";
import { Db2ResourceLimitError } from "./errors.js";
import { publishDb2Diagnostic } from "./diagnostics.js";

/** Selects whether imported rows become visible all at once or after each bounded batch. */
export type Db2ImportCommitPolicy = "atomic" | "commitEveryRows";

export type Db2ImportTarget = {
  readonly schema?: string;
  readonly table: string;
  /** Ordered target columns corresponding to the source row ordinals. */
  readonly columns: readonly string[];
  /** Optional expected Db2 SQL type code for each mapped target column. */
  readonly expectedSqlTypes?: readonly (number | undefined)[];
};

export type Db2ImportProgress = {
  /** Number of source rows consumed, including rows in an uncommitted batch. */
  readonly sourceRowsRead: number;
  /** Rows whose importer-owned transactions received a successful commit reply. */
  readonly committedRows: number;
  /** Importer-owned transactions confirmed committed. */
  readonly committedBatches: number;
  /** Estimated encoded bytes in confirmed committed source rows. */
  readonly estimatedCommittedBytes: number;
};

export type Db2ImportOptions = {
  readonly policy?: Db2ImportCommitPolicy;
  /** Maximum rows staged in one batch; CommitEveryRows also commits at each batch. */
  readonly maxBatchRows?: number;
  /** Soft estimated-byte boundary. A single oversized row is sent alone. */
  readonly maxBatchBytes?: number;
  readonly signal?: AbortSignal;
  readonly timeoutMs?: number;
  readonly cancelCleanupTimeoutMs?: number;
  readonly onProgress?: (progress: Db2ImportProgress) => void;
  /** Report progress after each multiple of this row count. Defaults to 10,000. */
  readonly notifyAfterRows?: number;
  /** Acquire an exclusive DB2 table lock for an atomic import. Rejected with commitEveryRows. */
  readonly tableLock?: boolean;
  /** Per-INSERT write deadline in milliseconds. Zero (the default) keeps no per-write deadline. */
  readonly commandTimeoutMs?: number;
};

export class Db2ImportError extends Error {
  constructor(
    message: string,
    readonly cause: unknown,
    readonly failedSourceRow: number | undefined,
    readonly progress: Db2ImportProgress,
  ) {
    super(message, { cause });
    this.name = "Db2ImportError";
  }
}

export const emptyImportProgress = (): Db2ImportProgress => ({
  sourceRowsRead: 0,
  committedRows: 0,
  committedBatches: 0,
  estimatedCommittedBytes: 0,
});

export function estimateImportRowBytes(row: readonly unknown[]): number {
  let size = 0;
  for (const value of row) {
    if (value === null || value === undefined) size += 2;
    else if (typeof value === "boolean") size += 3;
    else if (typeof value === "number" || typeof value === "bigint") size += 10;
    else if (typeof value === "string") size += Buffer.byteLength(value, "utf8") + 8;
    else if (value instanceof Uint8Array) size += value.byteLength + 8;
    else if (isImportLobStream(value)) size += Math.min(Number.MAX_SAFE_INTEGER - size, Number(value.length)) + 8;
    else if (value instanceof Date) size += 40;
    else size += Buffer.byteLength(String(value), "utf8") + 8;
  }
  return size;
}

export type ImportLobInput = {
  readonly kind: "binary-lob" | "text-lob";
  readonly length: bigint;
  stream(): AsyncIterable<Uint8Array | string>;
};

export type SpooledImportLob = {
  readonly source: AsyncIterable<Uint8Array>;
  readonly length: bigint;
  dispose(): Promise<void>;
};

/** Spools one progressive source LOB to a private temporary file before advancing its row. */
export async function spoolImportLob(value: ImportLobInput, signal?: AbortSignal, budget: Db2ResourceBudget = db2SpoolBudget): Promise<SpooledImportLob> {
  const directory = await mkdtemp(join(tmpdir(), "ts-db2-import-"));
  const path = join(directory, "lob.bin");
  let file;
  let reserved = 0;
  const iterator = value.stream()[Symbol.asyncIterator]();
  let sourceDone = false;
  try {
    if (signal?.aborted) throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
    file = await open(path, "wx", 0o600);
    let length = 0n;
    for (;;) {
      if (signal?.aborted) throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
      const next = await iterator.next();
      if (signal?.aborted) throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
      if (next.done) { sourceDone = true; break; }
      const chunk = next.value;
      const byteLength = typeof chunk === "string" ? Buffer.byteLength(chunk, "utf8") : chunk.byteLength;
      if (length + BigInt(byteLength) > BigInt(DB2_RESOURCE_DEFAULTS.maxStreamedLobBytes)) throw new Db2ResourceLimitError("DB2 LOB exceeds the streamed-value limit");
      budget.reserve(byteLength);
      reserved += byteLength;
      const bytes = typeof chunk === "string" ? Buffer.from(chunk, "utf8") : chunk;
      let offset = 0;
      while (offset < bytes.length) {
        const result = await file.write(bytes, offset, Math.min(bytes.length - offset, DB2_RESOURCE_DEFAULTS.chunkBytes));
        if (result.bytesWritten <= 0) throw new Error("temporary LOB spool write made no progress");
        offset += result.bytesWritten;
      }
      length += BigInt(bytes.length);
    }
    await file.close();
    file = undefined;
    const source = createReadStream(path);
    let disposed: Promise<void> | undefined;
    publishDb2Diagnostic({ operation: "spool", durationMs: 0, success: true, bytes: reserved });
    return {
      source,
      length,
      dispose: () => disposed ??= (async () => {
        // Removing a spool path does not close an unopened/unconsumed ReadStream.
        // Wait for its pending open/destroy to finish before unlinking the file.
        try {
          await new Promise<void>((resolve, reject) => source.close(error => error && error.code !== "ERR_STREAM_PREMATURE_CLOSE" ? reject(error) : resolve()));
        } finally {
          await rm(directory, { recursive: true, force: true });
          budget.release(reserved); reserved = 0;
        }
      })(),
    };
  } catch (error) {
    await file?.close().catch(() => {});
    try {
      await rm(directory, { recursive: true, force: true });
      budget.release(reserved); reserved = 0;
    } catch (cleanupError) {
      // Keep the quota charged while bytes may still exist on disk.
      throw new AggregateError([error, cleanupError], "LOB spool failed and its temporary files could not be removed");
    }
    throw error;
  } finally {
    // Db2 progressive locators own cursor cleanup. Once the shared signal fires,
    // let the query stream handle its interrupted locator instead of concurrently
    // returning its inner generator (which would try to drain a canceled cursor).
    if (!sourceDone && !signal?.aborted && iterator.return) void iterator.return().catch(() => {});
  }
}

function isImportLobStream(value: unknown): value is { length: bigint } {
  return typeof value === "object" && value !== null && "length" in value
    && typeof value.length === "bigint" && "kind" in value
    && (value.kind === "binary-lob" || value.kind === "text-lob");
}

export function validateImportTarget(target: Db2ImportTarget): void {
  if (!target || typeof target !== "object") throw new TypeError("DB2 import target is required");
  if (!Array.isArray(target.columns) || target.columns.length === 0)
    throw new TypeError("DB2 import target must map at least one column");
  if (target.expectedSqlTypes && target.expectedSqlTypes.length !== target.columns.length)
    throw new TypeError("DB2 expected SQL type count must match the mapped column count");
  quoteImportIdentifier(target.table);
  if (target.schema !== undefined) quoteImportIdentifier(target.schema);
  for (const column of target.columns) quoteImportIdentifier(column);
  for (const sqlType of target.expectedSqlTypes ?? [])
    if (sqlType !== undefined && (!Number.isInteger(sqlType) || sqlType < 0 || sqlType > 0xffff))
      throw new RangeError("DB2 expected SQL type codes must be unsigned 16-bit integers");
}

export function quoteImportIdentifier(value: string): string {
  if (typeof value !== "string" || value.length === 0 || value.length > 128 || value.includes("\0"))
    throw new TypeError(`invalid DB2 import identifier ${JSON.stringify(value)}`);
  return `"${value.replaceAll('"', '""')}"`;
}
