import { readFile } from "node:fs/promises";
import { hostname } from "node:os";
import { connect as connectTcp, isIP, type Socket } from "node:net";
import { checkServerIdentity, connect as connectTls, type ConnectionOptions } from "node:tls";
import { type Config, normalizeCurrentSchema, parseDsn } from "../config.js";
import { adminCommandCallSql, normalizeAdminCommand } from "../admin.js";
import { decodeCp500, encodeCp500 } from "../converters/ebcdic.js";
import { decodeDecfloat } from "../converters/decfloat.js";
import { buildSqlDtaWithExternalData, type Db2BlobStream, type Db2ClobStream, type Db2DbClobStream, type Db2XmlStream, type SqlDtaExternalParameter } from "../converters/sqlDta.js";
import { Db2ImportError, emptyImportProgress, estimateImportRowBytes, quoteImportIdentifier, spoolImportLob, validateImportTarget, type Db2ImportOptions, type Db2ImportProgress, type Db2ImportTarget, type SpooledImportLob } from "../import.js";
import { baseSqlType, SqlType } from "../types/sqltype.js";
import { Db2AuthenticationError, Db2BatchAbortError, Db2BatchExecutionError, Db2BatchTimeoutError, Db2ConnectionClosedError, Db2ConfigurationError, Db2DatabaseNotFoundError, Db2Error, Db2ResourceLimitError, Db2ServerError, Db2TimeoutError, Db2UnsupportedFeatureError, type Db2Warning } from "../errors.js";
import { CodePoint } from "./codepoint.js";
import {
  ColumnDescription, FieldDescriptor, packAccrdb, packAccsec, packCommit,
  packCloseProcedureResultSet, packContinueProcedureResultSet, packContinueQuery, packDescribeSqlStatement, packExecSqlImmediate, packExecuteSqlStatement,
  packExecuteSqlSet, packExcsat, packExcsatManagerLevels, packOpenQuery,
  packExecuteSqlStatementForResultSets, packOpenQueryWithParams, packPrepareSqlStatement, packRollback, packSecchk, packSqlStatement,
  packGetNextChunk, packInterruptRdbRequest, packSecchkEusridpwd, packSecchkKerberos, packSecchkUsrencpwd,
  findDdmObject, parseDdmParameters, parseProgressiveChunkReply, parseQueryDescriptor, parseResultSetSections, parseSqlCard, sqlCardWarning, parseSqlCursorInfo, parseSqlDard, parseSqlDtaReply, parseSqlResultSetCount,
} from "./ddm.js";
import { DSS_LIMITS, DssPacket, encodeRequestDss, readDss, writeExternalLobDss } from "./dss.js";
import { Db2DssStream } from "./stream.js";
import { encryptSecmec7, encryptSecmec9, generateSecmec9PrivateKey, secmec9PublicKey } from "../security/secmec9.js";

type Db2Socket = Socket;
export type QueryResult = { columns: ColumnDescription[]; rows: unknown[][]; resultSets?: Db2ResultSet[] };
export type Db2ResultSet = { columns: ColumnDescription[]; rows: unknown[][] };
export type Db2BinaryLobStream = {
  readonly kind: "binary-lob";
  /** Source byte length reported by Db2. */
  readonly length: bigint;
  /** Returns the one-shot byte stream. Breaking early drains the locator before the query advances. */
  stream(): AsyncIterable<Uint8Array>;
};
export type Db2TextLobStream = {
  readonly kind: "text-lob";
  /** Encoded source byte length reported by Db2 (UTF-16BE bytes for DBCLOB). */
  readonly length: bigint;
  /** Returns the one-shot text stream, decoded incrementally as UTF-8 or UTF-16BE. */
  stream(): AsyncIterable<string>;
};
export type Db2LobStream = Db2BinaryLobStream | Db2TextLobStream;
export type QueryStreamRow = { columns: readonly ColumnDescription[]; values: unknown[] };
/** Per-command controls. timeoutMs=0 (the default) means no command deadline. */
/** Db2 LUW dynamic SQL isolation. Omit to preserve the current register/package default. */
export type Db2IsolationLevel = "UR" | "CS" | "RS" | "RR";
export type QueryOptions = {
  signal?: AbortSignal;
  timeoutMs?: number;
  /** Maximum time to await the original command after cancellation; defaults to 30 seconds. */
  cancelCleanupTimeoutMs?: number;
  /** Applies this session user before the operation (go-db2's context `WithUser`). */
  user?: string;
  /** Applies these client-info registers before the operation (go-db2's context `WithClientInfo`).
   * Omitted fields keep their current session value, matching `setClientInfo` semantics. */
  clientInfo?: Partial<Db2ClientInfo>;
};
export type Db2InsertResult = { rowsAffected: number; lastInsertId: bigint };
export type Db2ParameterDirection = "input" | "output" | "inputOutput";
/** Output parameters carry a typed placeholder in value for the SQLDTA request. */
export type Db2Parameter = { value: unknown; direction: Db2ParameterDirection };
/** Output values align with the supplied parameter positions; input-only positions are null. */
export type Db2ExecutionResult = { rowsAffected: number; outputParameters: unknown[]; resultSets?: Db2ResultSet[] };
export type Db2ClientInfo = {
  applicationName: string;
  workstationName: string;
  userId: string;
  accounting: string;
  correlationToken: string;
};
type Reply = DssPacket;

/** Module-level decoder cache: never used with stream:true across independent ops. */
const utf8EncoderShared = new TextEncoder();
const utf8DecoderShared = new TextDecoder();
const utf8FatalDecoderShared = new TextDecoder("utf-8", { fatal: true });
const utf16BeDecoderShared: TextDecoder | undefined = (() => {
  try {
    const d = new TextDecoder("utf-16be");
    // Validate runtime actually supports utf-16be (Node >=22 does).
    const probe = d.decode(Uint8Array.of(0x00, 0x41));
    if (probe !== "A") return undefined;
    return d;
  } catch { return undefined; }
})();
const utf16LeDecoderShared = new TextDecoder("utf-16le");

const TRACE_PROCEDURE_CURSOR = process.env.TS_DB2_TRACE_PROCEDURE_CURSOR === "1";

const EMPTY_BYTES = new Uint8Array(0);

/** Precompiled row-decoder metadata: built once per result set, reused per QRYDTA block. */
type RowDecoderPlan = {
  readonly types: Int32Array;
  readonly ps: Int32Array;
  readonly nullable: Uint8Array;
  readonly columnCount: number;
  readonly hasLobs: boolean;
};

function compileRowDecoderPlan(fields: readonly FieldDescriptor[]): RowDecoderPlan {
  const count = fields.length;
  const types = new Int32Array(count);
  const ps = new Int32Array(count);
  const nullable = new Uint8Array(count);
  let hasLobs = false;
  for (let i = 0; i < count; i++) {
    const field = fields[i]!;
    types[i] = field.type;
    ps[i] = field.ps.length >= 2 ? ((field.ps[0]! << 8) | field.ps[1]!) : -1;
    nullable[i] = nullableTypes.has(field.type) ? 1 : 0;
    if (isLobType(field.type)) hasLobs = true;
  }
  return { types, ps, nullable, columnCount: count, hasLobs };
}

type QueryStreamState = {
  columns: ColumnDescription[];
  fields: FieldDescriptor[];
  decoderPlan: RowDecoderPlan | undefined;
  pendingRowData: Uint8Array;
  extdta: Uint8Array[];
  extdtaHead: number;
  open: boolean;
  ended: boolean;
  failed: boolean;
  queryId: bigint;
  nextCorrelation: number;
  activeProgressiveLob?: PendingProgressiveLob | undefined;
};
type ProgressiveLobReference = { readonly kind: "progressive-lob"; readonly length: bigint; readonly reference: bigint };
type ProcedureCursorState = {
  section: import("./ddm.js").ResultSetPackageSection;
  columns: ColumnDescription[];
  fields: FieldDescriptor[];
  decoderPlan: RowDecoderPlan | undefined;
  rowData: Uint8Array[];
  rowDataHead: number;
  extdta: Uint8Array[];
  extdtaHead: number;
  ended: boolean;
  queryId: bigint;
  nextCorrelation: number;
  pendingRowData: Uint8Array;
  readyRows: unknown[][];
  readyRowsHead: number;
  lastBlock: boolean;
  closed: boolean;
  stalledFrame?: Uint8Array | undefined;
  stalledFrameCount: number;
};
type ActiveOperation = {
  throwIfCancelled(): void;
  cancellationCompletion(): Promise<boolean> | undefined;
  pauseDeadline(): void;
  resumeDeadline(): void;
  finish(): Promise<void>;
};
type PendingProgressiveLob = { readonly streamValue: Db2LobStream; drain(): Promise<void> };
type PoolSessionState = {
  schema: string;
  isolation: string;
  sessionUser: string;
  clientInfo: Pick<Db2ClientInfo, "applicationName" | "workstationName" | "userId" | "accounting">;
};
type ProcedureReaderBackend = {
  currentResultSetIndex: number;
  resultSetCount: number;
  columns(): readonly ColumnDescription[];
  nextRow(): Promise<unknown[] | undefined>;
  drainCurrent(): Promise<void>;
  nextResultSet(): Promise<boolean>;
  finish(): Promise<Db2ExecutionResult>;
  fail(error: unknown): Promise<void>;
};

/** Streaming reader for a CALL's dynamic result cursors. Iterate rows() for the
 * current set, use nextResultSet() to advance, and call finish() to close all
 * remaining cursors and obtain terminal OUT/INOUT values. */
export class Db2ProcedureReader {
  private busy = false;
  private failed = false;
  private failure: unknown;
  private finishedResult: Db2ExecutionResult | undefined;
  private completionHandler: (() => Promise<void>) | undefined;
  private completionNotified = false;

  constructor(private readonly backend: ProcedureReaderBackend) {}

  get resultSetIndex(): number { return this.backend.currentResultSetIndex; }
  get resultSetCount(): number { return this.backend.resultSetCount; }
  get columns(): readonly ColumnDescription[] { return this.backend.columns(); }
  get outputParameters(): readonly unknown[] {
    if (!this.finishedResult) throw new Error("DB2 procedure output parameters are available only after finish()");
    return this.finishedResult.outputParameters;
  }

  async *rows(): AsyncGenerator<unknown[]> {
    if (this.finishedResult) throw new Error("DB2 procedure reader is already finished");
    if (this.busy) throw new Error("DB2 procedure reader already has an active operation");
    this.busy = true;
    let completed = false;
    try {
      while (true) {
        const row = await this.backend.nextRow();
        if (row === undefined) { completed = true; return; }
        yield row;
      }
    } catch (error) {
      await this.fail(error);
      throw error;
    } finally {
      try {
        if (!completed && this.finishedResult === undefined && !this.failed) {
          try { await this.backend.drainCurrent(); }
          catch (error) { await this.fail(error); throw error; }
        }
      } finally { this.busy = false; }
    }
  }

  /** Drains/closes the current cursor before selecting the next result set. */
  async nextResultSet(): Promise<boolean> {
    this.ensureAvailable();
    this.busy = true;
    try { return await this.backend.nextResultSet(); }
    catch (error) { await this.fail(error); throw error; }
    finally { this.busy = false; }
  }

  /** Drains and closes every remaining cursor. OUT/INOUT values become available on success. */
  async finish(): Promise<Db2ExecutionResult> {
    if (this.finishedResult) return this.finishedResult;
    this.ensureAvailable();
    this.busy = true;
    try {
      this.finishedResult = await this.backend.finish();
      await this.notifyCompletion();
      return this.finishedResult;
    } catch (error) { await this.fail(error); await this.notifyCompletion(); throw error; }
    finally { this.busy = false; }
  }

  async close(): Promise<void> { await this.finish(); }
  async [Symbol.asyncDispose](): Promise<void> { await this.finish(); }

  /** @internal Used by Db2Pool to return a lease after finish or failure. */
  attachCompletionHandler(handler: () => Promise<void>): void {
    if (this.completionHandler) throw new Error("DB2 procedure reader already belongs to a pool");
    this.completionHandler = handler;
  }

  private ensureAvailable(): void {
    if (this.failed) throw this.failure instanceof Error ? this.failure : new Error("DB2 procedure reader failed");
    if (this.finishedResult) throw new Error("DB2 procedure reader is already finished");
    if (this.busy) throw new Error("DB2 procedure reader already has an active operation");
  }

  private async fail(error: unknown): Promise<void> {
    // A failed cursor exchange leaves server cursor state unknown. The backend's
    // finish path discards the transport and releases the operation reservation.
    if (this.failed) return;
    this.failed = true;
    this.failure = error;
    try { await this.backend.fail(error); } catch { /* preserve the cursor's primary error */ }
    await this.notifyCompletion();
  }

  private async notifyCompletion(): Promise<void> {
    if (this.completionNotified) return;
    this.completionNotified = true;
    await this.completionHandler?.();
  }
}

class SocketReader {
  private chunks: Buffer[] = [];
  private headIndex = 0;
  private headOffset = 0;
  private buffered = 0;
  private failure: Error | undefined;
  private readonly listeners = new Set<() => void>();
  private readonly highWaterMark: number;
  private readonly lowWaterMark: number;
  private readonly maxBufferedBytes: number;
  private paused = false;

  constructor(
    private readonly socket: Db2Socket,
    options: { highWaterMark?: number; lowWaterMark?: number; maxBufferedBytes?: number } = {},
  ) {
    // Bounded inbound buffering: pause the socket when the userland queue
    // exceeds highWaterMark so a slow consumer (streaming 1M rows) cannot
    // grow RSS proportionally to the result set. Resume below lowWaterMark.
    this.highWaterMark = options.highWaterMark ?? 4 * 1024 * 1024;
    this.lowWaterMark = options.lowWaterMark ?? 1 * 1024 * 1024;
    this.maxBufferedBytes = options.maxBufferedBytes ?? 32 * 1024 * 1024;
    socket.on("data", (chunk: Buffer) => {
      this.chunks.push(chunk);
      this.buffered += chunk.length;
      if (this.buffered > this.maxBufferedBytes) {
        this.failure ??= new Error(`DB2 inbound buffer exceeded ${this.maxBufferedBytes} bytes; server is sending faster than the consumer`);
        // Stop further growth even though the failure will abort the next read.
        if (!this.paused) { this.paused = true; try { socket.pause(); } catch { /* best-effort */ } }
      } else if (!this.paused && this.buffered > this.highWaterMark) {
        this.paused = true;
        try { socket.pause(); } catch { /* pause is best-effort backpressure */ }
      }
      this.notify();
    });
    socket.on("error", (error) => { this.failure = error as Error; this.notify(); });
    socket.on("end", () => { this.failure ??= new Error("DB2 socket closed by peer"); this.notify(); });
    socket.on("close", () => { this.failure ??= new Error("DB2 socket closed"); this.notify(); });
  }

  async readExact(length: number): Promise<Uint8Array> {
    if (!Number.isInteger(length) || length < 0) throw new RangeError("read length must be a non-negative integer");
    if (length === 0) return new Uint8Array(0);
    if (length > 256 * 1024 * 1024) throw new RangeError(`DB2 read of ${length} bytes exceeds the 256 MiB DSS limit`);
    while (this.buffered < length) {
      if (this.failure) throw this.failure;
      await new Promise<void>((resolve) => this.listeners.add(resolve));
    }
    if (this.failure && this.buffered < length) throw this.failure;
    // Zero-copy fast path: head chunk holds the entire request.
    // Returns a subarray view (no copy) — ideal for short headers.
    // Callers that retain values long-term should copy small slices from
    // large parents (see retained-parent-buffer notes in readReplyChain).
    const head = this.chunks[this.headIndex]!;
    if (head.length - this.headOffset >= length) {
      const view = head.subarray(this.headOffset, this.headOffset + length);
      this.headOffset += length;
      if (this.headOffset === head.length) {
        this.headIndex++;
        this.headOffset = 0;
      }
      this.buffered -= length;
      this.maybeResume();
      this.maybeCompact();
      return view;
    }
    const result = Buffer.allocUnsafe(length);
    let written = 0;
    while (written < length) {
      const current = this.chunks[this.headIndex]!;
      const from = this.headOffset;
      const count = Math.min(current.length - from, length - written);
      current.copy(result, written, from, from + count);
      written += count;
      this.buffered -= count;
      if (from + count === current.length) {
        this.headIndex++;
        this.headOffset = 0;
      } else {
        this.headOffset = from + count;
      }
    }
    this.maybeResume();
    this.maybeCompact();
    return result;
  }

  /** Bulk read directly into a preallocated destination (DSS continuation fast path). */
  async readInto(destination: Uint8Array, offset: number, length: number): Promise<void> {
    if (!Number.isInteger(offset) || !Number.isInteger(length) || offset < 0 || length < 0 || offset + length > destination.length)
      throw new RangeError("invalid readInto range");
    if (length === 0) return;
    if (length > 256 * 1024 * 1024) throw new RangeError(`DB2 read of ${length} bytes exceeds the 256 MiB DSS limit`);
    let written = 0;
    while (written < length) {
      while (this.buffered < length - written) {
        if (this.failure) throw this.failure;
        await new Promise<void>((resolve) => this.listeners.add(resolve));
      }
      if (this.failure && this.buffered <= 0) throw this.failure;
      const chunk = this.chunks[this.headIndex]!;
      const from = this.headOffset;
      const count = Math.min(chunk.length - from, length - written);
      // Buffer.from shares memory; set() copies without an intermediate alloc.
      destination.set(chunk.subarray(from, from + count), offset + written);
      written += count;
      this.buffered -= count;
      if (from + count === chunk.length) {
        this.headIndex++;
        this.headOffset = 0;
      } else {
        this.headOffset = from + count;
      }
    }
    this.maybeResume();
    this.maybeCompact();
  }

  private maybeResume(): void {
    if (this.paused && this.buffered < this.lowWaterMark && !this.socket.destroyed) {
      this.paused = false;
      try { this.socket.resume(); } catch { /* resume is best-effort */ }
    }
  }

  private maybeCompact(): void {
    // Avoid Array.shift() per chunk: advance headIndex, compact rarely.
    if (this.headIndex > 128 && this.headIndex * 2 >= this.chunks.length) {
      this.chunks = this.chunks.slice(this.headIndex);
      this.headIndex = 0;
    }
    if (this.chunks.length === 0) {
      this.chunks = [];
      this.headIndex = 0;
      this.headOffset = 0;
    }
  }

  private notify(): void {
    for (const listener of this.listeners) listener();
    this.listeners.clear();
  }
}

/** A single, serialized DRDA connection to an IBM Db2 database. */
export class Db2Connection {
  private reader: SocketReader;
  private correlationId = 1;
  private closed = false;
  private closePromise: Promise<void> | undefined;
  private operationTail: Promise<void> = Promise.resolve();
  private database: string;
  private readonly packageId = "SYSSH200";
  private readonly consistencyToken = "SYSLVL01";
  private readonly sectionNumber = 65;
  private readonly littleEndian = true;
  private readonly queryBlockSize: number;
  private autoCommit = true;
  private transactionActive = false;
  private transactionGeneration = 0;
  private transactionIsolationBaseline: string | undefined;
  private currentUser: string;
  private readonly baseUser: string;
  private baseSessionUser: string | undefined;
  private preparedSql: string | undefined;
  private rdbInterruptToken: Uint8Array | undefined;
  private operationActive = false;
  private interruptSuppressionDepth = 0;
  private interruptPending: Promise<boolean> | undefined;
  private clientInfo: Db2ClientInfo = { applicationName: "", workstationName: "", userId: "", accounting: "", correlationToken: "" };
  private baseClientInfo: Db2ClientInfo = { ...this.clientInfo };
  private baseSchema: string | undefined;
  private baseIsolation: string | undefined;
  private poolBaselineCaptured = false;
  private readonly warningListeners = new Set<(warning: Db2Warning) => void>();

  private constructor(private socket: Db2Socket, private config: Config) {
    this.reader = new SocketReader(socket);
    this.database = config.database.padEnd(18, " ").slice(0, 18);
    this.queryBlockSize = config.blockSize >= 1024 ? config.blockSize : 65535;
    this.currentUser = config.user;
    this.baseUser = config.user;
  }

  /** Opens a connection from a parsed DSN or a Config object and completes DRDA negotiation. */
  static async connect(input: string | Config): Promise<Db2Connection> {
    const config = typeof input === "string" ? parseDsn(input) : { ...input };
    if (!config.host) throw new Db2ConfigurationError("DB2 host is required");
    if (!config.port) config.port = config.useSsl ? 50001 : 50000;
    if (!config.database.trim()) throw new Db2ConfigurationError("DB2 database name is required");
    if (!config.user) throw new Db2ConfigurationError("DB2 user name is required");
    if (![0, 3, 7, 9, 11].includes(config.securityMechanism))
      throw new Db2ConfigurationError(`Unsupported DB2 authentication mechanism ${config.securityMechanism}; supported mechanisms are 3 (USRIDPWD), 7 (USRENCPWD), 9 (EUSRIDPWD), and 11 (Kerberos/GSSAPI)`);
    if (config.securityMechanism === 11 && !config.gssapiTokenFile)
      throw new Db2ConfigurationError("Security mechanism 11 (Kerberos/GSSAPI) requires gssapi_token_file with a caller-supplied GSSAPI token; krb5_keytab/krb5_ccache ticket synthesis is not supported");
    const timeout = config.timeoutMs > 0 ? config.timeoutMs : 30_000;
    // Fail closed before any socket: an invalid schema must never reach the handshake.
    const currentSchema = normalizeCurrentSchema(config.currentSchema);
    let socket = await openTcp(config.host, config.port, timeout);
    try {
      if (config.useSsl) socket = await wrapTls(socket, config, timeout);
      // The DSN timeout bounds connection establishment and DRDA negotiation only.
      // A socket inactivity timeout would incorrectly kill a healthy idle session.
      socket.setTimeout(timeout, () => socket.destroy(new Error(`DB2 handshake timed out after ${timeout} ms`)));
      const connection = new Db2Connection(socket, config);
      await connection.handshake();
      if (currentSchema) {
        await connection.execute(`SET CURRENT SCHEMA ${quoteIdentifier(currentSchema)}`);
      }
      socket.setTimeout(0);
      return connection;
    } catch (error) {
      socket.destroy();
      throw error;
    }
  }

  get isClosed(): boolean { return this.closed || this.socket.destroyed; }
  get isAutoCommit(): boolean { return this.autoCommit; }
  get currentSessionUser(): string { return this.currentUser; }
  private get securityMechanism(): number { return this.config.securityMechanism || 3; }

  /** Subscribes to successful SQLCARD and row-level SQLCA warnings. Returns an unsubscribe function. */
  addWarningListener(listener: (warning: Db2Warning) => void): () => void {
    this.warningListeners.add(listener);
    return () => this.warningListeners.delete(listener);
  }

  /** Sends a cancellation request for the active command. `true` means Db2 acknowledged
   * receipt of the request, not that the command was stopped. Await the original command:
   * it can return SQLCODE -952, or complete normally if cancellation lost a race with it.
   */
  interrupt(): Promise<boolean> {
    return this.requestInterrupt();
  }

  private requestInterrupt(timeoutMs?: number): Promise<boolean> {
    if (this.interruptPending) return this.interruptPending;
    if (!this.operationActive || this.isClosed) return Promise.resolve(false);
    if (!this.rdbInterruptToken?.length) return Promise.reject(new Error("DB2 did not provide an interrupt token"));
    const pending = this.sendInterrupt(this.rdbInterruptToken, timeoutMs);
    this.interruptPending = pending;
    void pending.finally(() => { if (this.interruptPending === pending) this.interruptPending = undefined; }).catch(() => {});
    return pending;
  }

  private async sendInterrupt(token: Uint8Array, timeoutMs?: number): Promise<boolean> {
    const configuredTimeout = this.config.timeoutMs > 0 ? this.config.timeoutMs : 30_000;
    const timeout = timeoutMs === undefined ? configuredTimeout : Math.min(configuredTimeout, timeoutMs);
    let socket = await openTcp(this.config.host, this.config.port, timeout);
    try {
      if (this.config.useSsl) socket = await wrapTls(socket, this.config, timeout);
      socket.setTimeout(timeout, () => socket.destroy(new Error(`DB2 interrupt timed out after ${timeout} ms`)));
      const reader = new SocketReader(socket);
      await writeSocket(socket, encodeRequestDss(packExcsat("ts-db2-interrupt", hostname() || "ts-db2-client", "SQL12010", "ts-db2"), 1, false, true).bytes);
      const negotiation = await readDss(reader);
      if (negotiation.codePoint !== CodePoint.EXCSATRD) throw new Error(`DB2 interrupt negotiation failed: 0x${negotiation.codePoint.toString(16)}`);
      await writeSocket(socket, encodeRequestDss(packInterruptRdbRequest(token), 2, false, true).bytes);
      const reply = await readDss(reader);
      if (reply.codePoint !== CodePoint.CMDCMPRM) throw new Error(`DB2 interrupt rejected: 0x${reply.codePoint.toString(16)}`);
      const severity = parseDdmParameters(reply.payload).get(CodePoint.SVRCOD);
      if (severity && severity.length >= 2 && (severity[0] !== 0 || severity[1] !== 0)) throw new Error("DB2 interrupt returned an error severity");
      return true;
    } finally { socket.destroy(); }
  }

  /** Switches the active SQL authorization identifier within a Db2 trusted context.
   * `password` enables the DRDA SECCHK re-authentication fallback when the SQL
   * session-authorization statements are rejected, mirroring `go-db2`. */
  async switchUser(user: string, password?: string): Promise<void> {
    const target = validateUserIdentifier(user);
    await this.exclusive(async () => {
      this.ensureOpen();
      await this.switchUserLocked(target, password);
    });
  }

  /** Executes a query and returns Db2 column metadata with each row as a value array. */
  async query(sql: string, params?: readonly unknown[], options: QueryOptions = {}): Promise<QueryResult> {
    return this.exclusive(() => this.queryLocked(sql, params), options);
  }

  /** Yields query rows block by block and drains an open cursor if the consumer stops early. */
  queryStream(sql: string, options: QueryOptions = {}): AsyncGenerator<QueryStreamRow> {
    return this.streamQuery(sql, undefined, options);
  }

  /** Streams rows from a positional parameterized query. */
  queryStreamWithParams(sql: string, params: readonly unknown[], options: QueryOptions = {}): AsyncGenerator<QueryStreamRow> {
    return this.streamQuery(sql, params, options);
  }

  private async *streamQuery(sql: string, params: readonly unknown[] | undefined, options: QueryOptions): AsyncGenerator<QueryStreamRow> {
    validateQueryOptions(options);
    const previous = this.operationTail;
    let release!: () => void;
    this.operationTail = new Promise<void>((resolve) => { release = resolve; });
    try {
      await previous;
      if (options.signal?.aborted) throw options.signal.reason ?? new DOMException("The operation was aborted", "AbortError");
      if (this.interruptPending) await this.interruptPending.catch(() => {});
      if (options.signal?.aborted) throw options.signal.reason ?? new DOMException("The operation was aborted", "AbortError");
      const active = this.startOperation(options);
      try {
        try {
          for await (const row of this.queryStreamLocked(sql, params, active, options)) {
            active.throwIfCancelled();
            active.pauseDeadline();
            try { yield row; }
            finally { active.resumeDeadline(); }
          }
        } catch (error) {
          active.throwIfCancelled();
          throw error;
        }
        active.throwIfCancelled();
      } finally {
        await active.finish();
      }
    } finally { release(); }
  }

  private async *queryStreamLocked(sql: string, params: readonly unknown[] | undefined, active: ActiveOperation, options: QueryOptions): AsyncGenerator<QueryStreamRow> {
    this.ensureOpen();
    let describedColumns: readonly ColumnDescription[] = [];
    let preparedWithParams = false;
    if (params !== undefined) {
      const statement = await this.prepareLocked(sql);
      if (params.length !== statement.parameterColumns.length)
        throw new Error(`DB2 expected ${statement.parameterColumns.length} parameters, got ${params.length}`);
      if (statement.parameterColumns.length > 0) {
        const encoded = buildSqlDtaWithExternalData(statement.parameterColumns, params);
        this.correlationId = 1;
        const nextId = await this.send(packOpenQueryWithParams(this.packageId, this.consistencyToken, this.sectionNumber, this.database, this.queryBlockSize), true, false);
        await this.send(encoded.sqlDta, encoded.extDta.length > 0, encoded.extDta.length === 0, nextId);
        for (let index = 0; index < encoded.extDta.length; index++)
          await this.sendExternalData(encoded.extDta[index]!, index < encoded.extDta.length - 1, index === encoded.extDta.length - 1);
        describedColumns = statement.outputColumns;
        preparedWithParams = true;
      } else {
        await this.startDirectStreamQuery(sql);
      }
    } else {
      await this.startDirectStreamQuery(sql);
    }

    const state: QueryStreamState = { columns: [...describedColumns], fields: [], decoderPlan: undefined, pendingRowData: new Uint8Array(), extdta: [], extdtaHead: 0, open: false, ended: false, failed: false, queryId: 0n, nextCorrelation: 1 };
    const pendingLobs: PendingProgressiveLob[] = [];
    const exposeRows = (rows: unknown[][]): { row: QueryStreamRow; lobs: PendingProgressiveLob[] }[] => rows.map(values => {
      const rowLobs: PendingProgressiveLob[] = [];
      const row: QueryStreamRow = {
        columns: state.columns,
        values: values.map((value, index) => {
          const progressive = getProgressiveLobReference(value);
          const field = state.fields[index];
          if (!progressive || !field) return value;
          const lob = this.createProgressiveLobStream(progressive, field, state, active, options);
          pendingLobs.push(lob);
          rowLobs.push(lob);
          return lob.streamValue;
        }),
      };
      return { row, lobs: rowLobs };
    });
    const drainLobs = async (lobs: readonly PendingProgressiveLob[]): Promise<void> => {
      for (const lob of lobs) await lob.drain();
      for (const lob of lobs) {
        const index = pendingLobs.indexOf(lob);
        if (index >= 0) pendingLobs.splice(index, 1);
      }
    };
    const drainPendingLobs = async (): Promise<void> => drainLobs([...pendingLobs]);
    let completedNormally = false;
    try {
      const first = await this.readReplyChain();
      let firstRows: unknown[][];
      try {
        firstRows = this.consumeStreamingReplies(first, state);
      } catch (error) {
        state.failed = true;
        if (isConfirmedInterrupt(error)) state.ended = true;
        throw error;
      }
      const plainRows = !state.fields.some((field) => isLobType(field.type));
      if (plainRows) { for (const values of firstRows) yield { columns: state.columns, values }; }
      else for (const item of exposeRows(firstRows)) { yield item.row; if (item.lobs.length > 0) await drainLobs(item.lobs); }
      while (state.open && !state.ended) {
        const priorPending = state.pendingRowData.length;
        const priorExternal = state.extdta.length - state.extdtaHead;
        await this.send(packContinueQuery(this.packageId, this.consistencyToken, this.sectionNumber, this.database, this.queryBlockSize, state.queryId), false, true, state.nextCorrelation);
        const replies = await this.readReplyChain();
        let rows: unknown[][];
        try {
          rows = this.consumeStreamingReplies(replies, state);
        } catch (error) {
          state.failed = true;
          if (isConfirmedInterrupt(error)) state.ended = true;
          throw error;
        }
        const gotData = replies.some(reply => reply.codePoint === CodePoint.QRYDTA || reply.codePoint === CodePoint.EXTDTA);
        if (!gotData && state.pendingRowData.length === priorPending && state.extdta.length - state.extdtaHead === priorExternal && !state.ended)
          throw new Error("DB2 CNTQRY returned neither query data nor an end-of-query reply");
        if (plainRows) { for (const values of rows) yield { columns: state.columns, values }; }
        else for (const item of exposeRows(rows)) { yield item.row; if (item.lobs.length > 0) await drainLobs(item.lobs); }
      }
      if (state.pendingRowData.length !== 0) throw new Error("DB2 ended a streamed query in the middle of a row");
      completedNormally = true;
    } finally {
      try { await drainPendingLobs(); }
      catch (error) {
        state.failed = true;
        if (isConfirmedInterrupt(error) || state.ended) state.ended = true;
        else if (!this.closed) { this.closed = true; this.socket.destroy(); }
        if (completedNormally) throw error;
      }
      let drainedSuccessfully = state.ended && !state.failed;
      if (state.open && !state.ended) {
        try {
          while (state.open && !state.ended) {
            await this.send(packContinueQuery(this.packageId, this.consistencyToken, this.sectionNumber, this.database, this.queryBlockSize, state.queryId), false, true, state.nextCorrelation);
            const drainedRows = this.consumeStreamingReplies(await this.readReplyChain(), state);
            exposeRows(drainedRows);
            await drainPendingLobs();
          }
          drainedSuccessfully = state.ended && !state.failed;
        } catch (error) {
          if (isConfirmedInterrupt(error)) {
            state.ended = true;
            state.failed = true;
            drainedSuccessfully = true;
          } else if (!state.ended) {
            this.closed = true;
            this.socket.destroy();
          }
        }
      }
      if (preparedWithParams && this.autoCommit && state.ended && !state.failed && state.pendingRowData.length === 0 && (completedNormally || drainedSuccessfully) && !this.closed) {
        try {
          this.correlationId = 1;
          await this.send(packCommit(), false, true, 1);
          await this.readReplyChain();
        } catch (error) {
          this.closed = true;
          this.socket.destroy();
          throw error;
        }
      }
    }
  }

  private async startDirectStreamQuery(sql: string): Promise<void> {
    this.correlationId = 1;
    this.preparedSql = undefined;
    await this.send(packPrepareSqlStatement(this.packageId, this.consistencyToken, this.sectionNumber, this.database), true, false);
    await this.send(packSqlStatement(sql), false, false);
    this.preparedSql = sql;
    await this.send(packOpenQuery(this.packageId, this.consistencyToken, this.sectionNumber, this.database, this.queryBlockSize), false, true);
  }

  /** Executes a DDL or DML statement without parameter binding. */
  async execute(sql: string, params?: readonly unknown[], options: QueryOptions = {}): Promise<number> {
    return this.exclusive(() => this.executeLocked(sql, params), options);
  }

  /** Executes one Db2 administrative command through SYSPROC.ADMIN_CMD. */
  executeAdminCommand(command: string, options: QueryOptions = {}): Promise<number> {
    const normalized = normalizeAdminCommand(command);
    return this.execute(adminCommandCallSql, [normalized], options);
  }

  /** Executes an INSERT and reads IDENTITY_VAL_LOCAL() before another command can use this session. */
  async executeInsert(sql: string, params?: readonly unknown[], options: QueryOptions = {}): Promise<Db2InsertResult> {
    return this.exclusive(async () => {
      const rowsAffected = await this.executeLocked(sql, params);
      return { rowsAffected, lastInsertId: await this.lastInsertIdLocked() };
    }, options);
  }

  /** Executes positional parameter sets in order; Db2BatchExecutionError reports acknowledged partial progress. */
  async executeBatch(sql: string, parameterSets: readonly (readonly unknown[])[], options: QueryOptions = {}): Promise<number> {
    let affected = 0;
    let completed = 0;
    let inFlight: number | undefined;
    try {
      return await this.exclusive(async () => {
        this.ensureOpen();
        if (parameterSets.length === 0) return 0;
        let statement: Db2Statement;
        try { statement = await this.prepareLocked(sql); }
        catch (error) { throw new Db2BatchExecutionError(undefined, 0, 0, error); }

        for (const params of parameterSets) {
          inFlight = completed;
          let currentAffected: number;
          try { currentAffected = await this.executePreparedLocked(statement, params, true); }
          catch (error) { throw new Db2BatchExecutionError(completed, completed, affected, error); }
          affected += currentAffected;
          completed++;
          inFlight = undefined;
        }
        return affected;
      }, options);
    } catch (error) {
      if (options.signal?.aborted) throw new Db2BatchAbortError(completed, inFlight, affected, error);
      if (error instanceof DOMException && error.name === "TimeoutError")
        throw new Db2BatchTimeoutError(completed, inFlight, affected, error);
      throw error;
    }
  }

  /** Copies value arrays or QueryStreamRows from another session using bounded typed batches. */
  async importRows(
    source: AsyncIterable<readonly unknown[] | QueryStreamRow>,
    target: Db2ImportTarget,
    options: Db2ImportOptions = {},
  ): Promise<Db2ImportProgress> {
    validateQueryOptions(options);
    validateImportTarget(target);
    const policy = options.policy ?? "atomic";
    if (policy !== "atomic" && policy !== "commitEveryRows") throw new TypeError("invalid DB2 import commit policy");
    const maxBatchRows = options.maxBatchRows ?? 256;
    const maxBatchBytes = options.maxBatchBytes ?? 8 * 1024 * 1024;
    if (!Number.isSafeInteger(maxBatchRows) || maxBatchRows < 1) throw new RangeError("DB2 import maxBatchRows must be a positive safe integer");
    if (!Number.isSafeInteger(maxBatchBytes) || maxBatchBytes < 1) throw new RangeError("DB2 import maxBatchBytes must be a positive safe integer");
    if (!source || typeof source[Symbol.asyncIterator] !== "function") throw new TypeError("DB2 import source must be an AsyncIterable of rows");
    const notifyAfterRows = options.notifyAfterRows ?? 10_000;
    const tableLock = options.tableLock ?? false;
    const commandTimeoutMs = options.commandTimeoutMs ?? 0;
    if (!Number.isSafeInteger(notifyAfterRows) || notifyAfterRows < 1) throw new RangeError("DB2 import notifyAfterRows must be a positive safe integer");
    if (tableLock && policy !== "atomic") throw new TypeError("DB2 import tableLock requires the atomic policy; commitEveryRows must not be used");
    if (!Number.isSafeInteger(commandTimeoutMs) || commandTimeoutMs < 0 || commandTimeoutMs > 2_147_483_647) throw new RangeError("DB2 import commandTimeoutMs must be an integer between 0 and 2147483647");
    const deadlineController = new AbortController();
    const signal = options.signal === undefined
      ? deadlineController.signal
      : AbortSignal.any([options.signal, deadlineController.signal]);
    const queryOptions: QueryOptions = {
      signal,
      ...(options.cancelCleanupTimeoutMs === undefined ? {} : { cancelCleanupTimeoutMs: options.cancelCleanupTimeoutMs }),
    };

    return this.exclusive(async () => {
      this.ensureOpen();
      if (!this.autoCommit || this.transactionActive)
        throw new Db2ImportError("DB2 import requires an autocommit destination outside a caller transaction", undefined, undefined, emptyImportProgress());

      const progress = { ...emptyImportProgress() };
      const iterator = source[Symbol.asyncIterator]();
      let iteratorDone = false;
      let statement: Db2Statement | undefined;
      let transactionStarted = false;
      let primaryError: unknown;
      let sourceHasProgressiveLob = false;
      const spools = new Set<SpooledImportLob>();
      const deadlineTimer = options.timeoutMs && options.timeoutMs > 0
        ? setTimeout(() => deadlineController.abort(new Db2TimeoutError(`DB2 import timed out after ${options.timeoutMs} ms`)), options.timeoutMs)
        : undefined;
      const readSource = async <T>(read: () => Promise<T>): Promise<T> => {
        this.interruptSuppressionDepth++;
        try {
          throwIfImportAborted(signal);
          const result = await read();
          throwIfImportAborted(signal);
          return result;
        } finally { this.interruptSuppressionDepth--; }
      };
      // Per-write deadline for LOCK TABLE and row INSERTs. A timed-out write has
      // an uncertain outcome, so the session is discarded fail-closed instead of
      // risking protocol desynchronization on reuse.
      const runWithCommandTimeout = async <T>(write: () => Promise<T>, what: string): Promise<T> => {
        if (!(commandTimeoutMs > 0)) return write();
        let timer: NodeJS.Timeout | undefined;
        try {
          return await Promise.race([
            write(),
            new Promise<never>((_, reject) => {
              timer = setTimeout(() => reject(new Db2TimeoutError(`DB2 import ${what} timed out after ${commandTimeoutMs} ms`)), commandTimeoutMs);
            }),
          ]);
        } catch (error) {
          if (error instanceof Db2TimeoutError) {
            this.closed = true;
            this.socket.destroy();
          }
          throw error;
        } finally {
          if (timer !== undefined) clearTimeout(timer);
        }
      };
      try {
        throwIfImportAborted(signal);
        let first: IteratorResult<readonly unknown[] | QueryStreamRow>;
        try { first = await readSource(() => iterator.next()); }
        catch (cause) { throw new Db2ImportError("DB2 import failed while reading source row 1", cause, 1, { ...progress }); }
        if (first.done) { iteratorDone = true; return { ...progress }; }

        const insertTable = target.schema === undefined
          ? quoteImportIdentifier(target.table)
          : `${quoteImportIdentifier(target.schema)}.${quoteImportIdentifier(target.table)}`;
        const insertSql = `INSERT INTO ${insertTable} (${target.columns.map(quoteImportIdentifier).join(", ")}) VALUES (${target.columns.map(() => "?").join(", ")})`;
        try { statement = await this.prepareLocked(insertSql); }
        catch (cause) { throw new Db2ImportError("DB2 import could not prepare the destination INSERT", cause, undefined, { ...progress }); }
        if (statement.parameterColumns.length !== target.columns.length)
          throw new Db2ImportError(`DB2 import INSERT described ${statement.parameterColumns.length} parameters for ${target.columns.length} mapped columns`, undefined, undefined, { ...progress });
        for (let index = 0; index < target.columns.length; index++) {
          const expected = target.expectedSqlTypes?.[index];
          const actual = statement.parameterColumns[index]!;
          if (expected !== undefined && actual.sqlType !== expected)
            throw new Db2ImportError(`DB2 import target column ${target.columns[index]} at position ${index} has SQL type ${actual.sqlType}, expected ${expected}`, undefined, undefined, { ...progress });
        }

        type StagedRow = { readonly sourceRow: number; readonly values: readonly unknown[]; readonly bytes: number };
        let batch: StagedRow[] = [];
        let batchBytes = 0;
        let committedBytes = 0;
        const startTransaction = (): void => {
          if (transactionStarted) return;
          transactionStarted = true;
          this.transactionActive = true;
          this.autoCommit = false;
          this.transactionGeneration++;
        };
        const commitBatch = async (): Promise<void> => {
          if (batch.length === 0) return;
          startTransaction();
          for (const row of batch) {
            throwIfImportAborted(signal);
            try {
              const affected = await runWithCommandTimeout(
                () => this.executePreparedLocked(statement!, row.values, true),
                `row insert for source row ${row.sourceRow}`,
              );
              if (affected !== 1) throw new Error(`Db2 confirmed ${affected} inserted rows; expected one for source row ${row.sourceRow}`);
            }
            catch (cause) {
              throw new Db2ImportError(`DB2 import failed writing source row ${row.sourceRow}`, cause, row.sourceRow, { ...progress });
            }
          }
          const batchRows = batch.length;
          const bytes = batchBytes;
          if (policy === "commitEveryRows") {
            const snapshot = { ...progress };
            try { options.onProgress?.(snapshot); }
            catch (cause) { throw new Db2ImportError("DB2 import progress observer failed before checkpoint", cause, undefined, snapshot); }
            throwIfImportAborted(signal);
            try { await this.executeControlLocked(packCommit()); }
            catch (cause) { this.closed = true; this.socket.destroy(); throw new Db2ImportError("DB2 import checkpoint commit failed; destination session was discarded", cause, undefined, snapshot); }
            progress.committedRows += batchRows;
            progress.committedBatches++;
            progress.estimatedCommittedBytes += bytes;
          } else {
            committedBytes += bytes;
          }
          batch = [];
          batchBytes = 0;
        };
        const stage = async (sourceItem: readonly unknown[] | QueryStreamRow): Promise<void> => {
          progress.sourceRowsRead++;
          const sourceRow = progress.sourceRowsRead;
          const sourceColumns = isQueryStreamRow(sourceItem) ? sourceItem.columns : undefined;
          const values = isQueryStreamRow(sourceItem) ? sourceItem.values : sourceItem;
          if (!Array.isArray(values)) throw new Db2ImportError(`DB2 import source row ${sourceRow} is not an array`, undefined, sourceRow, { ...progress });
          if (values.length !== target.columns.length)
            throw new Db2ImportError(`DB2 import source row ${sourceRow} has ${values.length} values for ${target.columns.length} mapped columns`, undefined, sourceRow, { ...progress });
          if (values.some((value) => isDb2LobStream(value))) sourceHasProgressiveLob = true;
          const bytes = estimateImportRowBytes(values);
          if (batch.length > 0 && (batch.length >= maxBatchRows || batchBytes + bytes > maxBatchBytes)) await commitBatch();
          let importValues: readonly unknown[];
          try {
            importValues = await readSource(() => prepareImportValues(values, sourceColumns, statement!.parameterColumns, spools, signal));
          } catch (cause) {
            throw new Db2ImportError(`DB2 import failed preparing source row ${sourceRow}`, cause, sourceRow, { ...progress });
          }
          batch.push({ sourceRow, values: importValues, bytes });
          batchBytes += bytes;
          const flushed = batch.length >= maxBatchRows || batchBytes >= maxBatchBytes;
          if (flushed) await commitBatch();
          // Periodic progress report, skipped when this row just flushed through a
          // checkpoint gate with the identical snapshot. The observer may still see
          // the same snapshot from a gate and a periodic report on different rows;
          // treat it as idempotent. An observer error aborts the import, keeping
          // the last confirmed checkpoint.
          if (!(flushed && policy === "commitEveryRows") && progress.sourceRowsRead % notifyAfterRows === 0) {
            try { options.onProgress?.({ ...progress }); }
            catch (cause) { throw new Db2ImportError(`DB2 import progress observer failed at source row ${sourceRow}`, cause, sourceRow, { ...progress }); }
          }
        };

        if (tableLock) {
          startTransaction();
          try {
            await runWithCommandTimeout(
              () => this.executeLocked(`LOCK TABLE ${insertTable} IN EXCLUSIVE MODE`),
              "table lock",
            );
          }
          catch (cause) { throw new Db2ImportError("DB2 import could not lock the destination table", cause, undefined, { ...progress }); }
        }
        await stage(first.value);
        for (;;) {
          throwIfImportAborted(signal);
          let next: IteratorResult<readonly unknown[] | QueryStreamRow>;
          try { next = await readSource(() => iterator.next()); }
          catch (cause) { throw new Db2ImportError(`DB2 import failed while reading source row ${progress.sourceRowsRead + 1}`, cause, progress.sourceRowsRead + 1, { ...progress }); }
          if (next.done) { iteratorDone = true; break; }
          await stage(next.value);
        }
        await commitBatch();
        if (policy === "atomic" && progress.sourceRowsRead > 0) {
          const snapshot = { ...progress };
          try { options.onProgress?.(snapshot); }
          catch (cause) { throw new Db2ImportError("DB2 import progress observer failed before atomic commit", cause, undefined, snapshot); }
          throwIfImportAborted(signal);
          try { await this.executeControlLocked(packCommit()); }
          catch (cause) { this.closed = true; this.socket.destroy(); throw new Db2ImportError("DB2 import commit failed; destination session was discarded", cause, undefined, snapshot); }
          progress.committedRows = progress.sourceRowsRead;
          progress.committedBatches = 1;
          progress.estimatedCommittedBytes = committedBytes;
        }
        return { ...progress };
      } catch (cause) {
        primaryError = cause;
        if (transactionStarted && !this.closed) {
          try { await this.executeControlLocked(packRollback()); }
          catch { this.closed = true; this.socket.destroy(); }
        }
        if (cause instanceof Db2ImportError) throw cause;
        throw new Db2ImportError("DB2 row import failed", cause, undefined, { ...progress });
      } finally {
        for (const spool of spools) await spool.dispose().catch(() => {});
        if (!iteratorDone && sourceHasProgressiveLob && signal.aborted) {
          let cleanupTimer: NodeJS.Timeout | undefined;
          try {
            const cleanupTimeout = options.cancelCleanupTimeoutMs ?? 30_000;
            const resumed = await Promise.race([
              iterator.next(),
              new Promise<never>((_resolve, reject) => { cleanupTimer = setTimeout(() => reject(new Error("source iterator did not finish after import cancellation")), cleanupTimeout); }),
            ]);
            if (resumed.done) iteratorDone = true;
          } catch { iteratorDone = true; }
          finally { if (cleanupTimer) clearTimeout(cleanupTimer); }
        }
        if (!iteratorDone && iterator.return) {
          try { await iterator.return(); }
          catch (cause) { if (primaryError === undefined) throw cause; }
        }
        if (transactionStarted) {
          this.autoCommit = true;
          this.transactionActive = false;
          this.transactionGeneration++;
        }
        if (deadlineTimer) clearTimeout(deadlineTimer);
      }
    }, queryOptions);
  }

  async executeWithOutputs(sql: string, parameters: readonly Db2Parameter[], options: QueryOptions = {}): Promise<Db2ExecutionResult> {
    return this.exclusive(async () => {
      this.ensureOpen();
      const statement = await this.prepareLocked(sql);
      return this.executePreparedWithOutputsLocked(statement, parameters, true);
    }, options);
  }

  /** Starts a streaming CALL reader. OUT/INOUT values are available only after reader.finish(). */
  async executeProcedureStream(sql: string, parameters: readonly Db2Parameter[] = [], options: QueryOptions = {}): Promise<Db2ProcedureReader> {
    if (!isCallStatement(sql)) throw new TypeError("executeProcedureStream requires a CALL statement");
    return this.openProcedureReader(async () => this.prepareLocked(sql), parameters, options);
  }

  /** @internal Starts a streaming CALL reader from a prepared statement. */
  async executeStatementStream(statement: Db2Statement, parameters: readonly Db2Parameter[], options: QueryOptions = {}): Promise<Db2ProcedureReader> {
    if (!isCallStatement(statement.sql)) throw new TypeError("executeStream requires a CALL statement");
    return this.openProcedureReader(async () => {
      if (this.preparedSql !== statement.sql) return this.prepareLocked(statement.sql);
      return statement;
    }, parameters, options);
  }

  /** Prepares SQL and describes its result columns and positional parameters. */
  async prepare(sql: string, options: QueryOptions = {}): Promise<Db2Statement> {
    return this.exclusive(async () => {
      this.ensureOpen();
      return this.prepareLocked(sql);
    }, options);
  }

  /** @internal Executes a previously prepared statement. */
  async executeStatement(statement: Db2Statement, params: readonly unknown[], options: QueryOptions = {}): Promise<number> {
    return this.exclusive(async () => {
      this.ensureOpen();
      return this.executePreparedLocked(statement, params);
    }, options);
  }

  async executeStatementWithOutputs(statement: Db2Statement, parameters: readonly Db2Parameter[], options: QueryOptions = {}): Promise<Db2ExecutionResult> {
    return this.exclusive(() => this.executePreparedWithOutputsLocked(statement, parameters), options);
  }

  /** @internal Executes a prepared INSERT and reads its generated identity under the same session lock. */
  async executeInsertStatement(statement: Db2Statement, params: readonly unknown[], options: QueryOptions = {}): Promise<Db2InsertResult> {
    return this.exclusive(async () => {
      this.ensureOpen();
      const rowsAffected = await this.executePreparedLocked(statement, params);
      return { rowsAffected, lastInsertId: await this.lastInsertIdLocked() };
    }, options);
  }

  /** @internal Queries a previously prepared statement. */
  async queryStatement(statement: Db2Statement, params: readonly unknown[], options: QueryOptions = {}): Promise<QueryResult> {
    return this.exclusive(async () => {
      this.ensureOpen();
      return this.queryPreparedLocked(statement, params);
    }, options);
  }

  async ping(): Promise<void> {
    return this.exclusive(async () => {
      this.ensureOpen();
      this.correlationId = 1;
      await this.send(packCommit(), false, true);
      await this.readReplyChain();
    });
  }

  async commit(): Promise<void> {
    await this.executeControl(packCommit());
  }

  async rollback(): Promise<void> {
    await this.executeControl(packRollback());
  }

  getClientInfo(): Db2ClientInfo { return { ...this.clientInfo }; }

  clientInfoMatches(info: Db2ClientInfo): boolean {
    return (Object.keys(info) as (keyof Db2ClientInfo)[]).every((key) => this.clientInfo[key] === info[key]);
  }

  /** Applies non-empty Db2 client registers on this physical session. */
  async setClientInfo(info: Partial<Db2ClientInfo>): Promise<void> {
    await this.exclusive(async () => {
      this.ensureOpen();
      const target = { ...this.clientInfo };
      for (const key of Object.keys(info) as (keyof Db2ClientInfo)[]) {
        const value = info[key];
        if (value !== undefined && value !== "") target[key] = value;
      }
      await this.applyClientInfoLocked(target);
    });
  }

  /** Replaces all tracked client registers, allowing empty strings to reset them. */
  async resetClientInfo(info: Db2ClientInfo): Promise<void> {
    await this.exclusive(async () => {
      this.ensureOpen();
      await this.applyClientInfoLocked(info);
    });
  }

  /** The database this session is currently connected to. */
  get currentDatabase(): string { return this.config.database; }

  /** Switches to another database by reconnecting this session's transport.
   * Fails before any I/O on an empty or invalid name, when a transaction is
   * active, or on a pooled session (acquire with the target DSN instead, so a
   * lease can never return to the pool on the wrong database). Open row streams
   * serialize through the operation lock and finish before the switch. A failed
   * reconnect leaves the session closed, never half-connected. */
  async changeDatabase(database: string, options: QueryOptions = {}): Promise<void> {
    const target = database.trim();
    if (!target) throw new TypeError("DB2 database name must not be empty");
    if (target.length > 128 || target.includes("\0")) throw new RangeError(`invalid DB2 database name ${JSON.stringify(database)}`);
    if (target === this.config.database) return;
    return this.exclusive(async () => {
      this.ensureOpen();
      if (this.transactionActive || !this.autoCommit)
        throw new Error("cannot change database while a transaction is active");
      if (this.poolBaselineCaptured)
        throw new Error("cannot change database on a pooled session; acquire with the target DSN instead");
      if (target === this.config.database) return;
      await this.reconnectLocked({ ...this.config, database: target });
    }, options);
  }

  /** Sets CURRENT SCHEMA for subsequent SQL on this session. Pooled leases keep
   * working: the pool reset restores the baseline schema on release. */
  async setCurrentSchema(schema: string, options: QueryOptions = {}): Promise<void> {
    const target = normalizeCurrentSchema(schema);
    if (!target) throw new TypeError("DB2 current schema must not be empty");
    return this.exclusive(async () => { await this.executeLocked(`SET CURRENT SCHEMA ${quoteIdentifier(target)}`); }, options);
  }

  /** Replaces this session's transport with a fresh DRDA negotiation. The caller
   * holds the operation lock; streams and commands serialize around the switch.
   * A failed reconnect leaves the session closed, never half-connected. */
  private async reconnectLocked(config: Config): Promise<void> {
    const timeout = config.timeoutMs > 0 ? config.timeoutMs : 30_000;
    const currentSchema = normalizeCurrentSchema(config.currentSchema);
    let socket = await openTcp(config.host, config.port, timeout);
    try {
      if (config.useSsl) socket = await wrapTls(socket, config, timeout);
      socket.setTimeout(timeout, () => socket.destroy(new Error(`DB2 handshake timed out after ${timeout} ms`)));
      // Abandon the old transport before the new handshake: the two sessions must
      // never share one SocketReader, and a failed reconnect must not resurrect
      // the previous database.
      this.socket.destroy();
      this.socket = socket;
      this.reader = new SocketReader(socket);
      this.config = config;
      this.database = config.database.padEnd(18, " ").slice(0, 18);
      this.closed = false;
      this.closePromise = undefined;
      this.autoCommit = true;
      this.transactionActive = false;
      this.transactionGeneration++;
      this.transactionIsolationBaseline = undefined;
      this.preparedSql = undefined;
      this.currentUser = config.user;
      this.baseSessionUser = undefined;
      this.baseSchema = undefined;
      this.baseIsolation = undefined;
      this.poolBaselineCaptured = false;
      this.rdbInterruptToken = undefined;
      await this.handshake();
      if (currentSchema) {
        await this.executeLocked(`SET CURRENT SCHEMA ${quoteIdentifier(currentSchema)}`);
      }
      socket.setTimeout(0);
    } catch (error) {
      this.closed = true;
      try { socket.destroy(); } catch { /* already closing */ }
      throw error;
    }
  }

  /** @internal Restores transaction, identity, and client state before pooled reuse. */
  async resetForPool(): Promise<void> {
    await this.exclusive(async () => {
      this.ensureOpen();
      if (this.transactionActive || !this.autoCommit) {
        // Abandoned unit of work (Rust drops and discards). Attempt rollback to clean
        // server state, then discard anyway so the pool cannot reuse this transport.
        this.transactionGeneration++;
        let rollbackError: unknown;
        try {
          await this.executeControlLocked(packRollback());
        } catch (error) {
          rollbackError = error;
        } finally {
          this.autoCommit = true;
          this.transactionActive = false;
          this.closed = true;
        }
        if (rollbackError !== undefined) throw rollbackError;
        return;
      }
      try {
        // Strict reset: a pooled session must always probe actual server state
        // before reuse. Local dirty tracking cannot be trusted because arbitrary
        // SQL (CALL, DYNAMIC) can change registers behind the driver's back
        // (see the "innocuous CALL" regression test). The probe runs the
        // state-dependent restore only for registers that actually drifted.
        if (this.poolBaselineCaptured) {
          await this.restorePoolStateLocked(await this.readPoolStateLocked());
        } else {
          await this.switchUserLocked(this.baseSessionUser ?? this.baseUser, undefined, false);
          await this.applyClientInfoLocked(this.baseClientInfo);
        }
      } catch (error) {
        // A failed register restore leaves the server-side session state uncertain.
        // The pool discards this transport instead of handing it to the next borrower.
        this.closed = true;
        this.socket.destroy();
        throw error;
      }
      // Lease-specific warning subscriptions must not leak into the next borrower.
      this.warningListeners.clear();
    });
  }

  /** @internal Captures the server-side session baseline before a pooled session reaches its first caller. */
  async capturePoolBaseline(): Promise<void> {
    await this.exclusive(async () => {
      this.ensureOpen();
      if (this.poolBaselineCaptured) return;
      try {
        const state = await this.readPoolStateLocked();
        this.baseSchema = state.schema;
        this.baseIsolation = state.isolation;
        this.baseSessionUser = state.sessionUser;
        this.currentUser = this.baseSessionUser;
        const clientInfo = state.clientInfo;
        this.baseClientInfo = { ...this.baseClientInfo, ...clientInfo };
        this.clientInfo = { ...this.clientInfo, ...clientInfo };
        this.preparedSql = undefined;
        this.poolBaselineCaptured = true;
      } catch (error) {
        this.closed = true;
        this.socket.destroy();
        throw error;
      }
    });
  }

  /** @internal Whether the pool baseline was captured for this session. */
  get hasPoolBaseline(): boolean { return this.poolBaselineCaptured; }

  private async readPoolStateLocked(): Promise<PoolSessionState> {
    const sql = "SELECT RTRIM(SESSION_USER), CURRENT SCHEMA, CURRENT ISOLATION, COALESCE(CURRENT CLIENT_APPLNAME, ''), COALESCE(CURRENT CLIENT_WRKSTNNAME, ''), COALESCE(CURRENT CLIENT_USERID, ''), COALESCE(CURRENT CLIENT_ACCTNG, '') FROM SYSIBM.SYSDUMMY1";
    const rows = (await this.queryLocked(sql)).rows;
    const row = rows.length === 1 ? rows[0] : undefined;
    if (!row || row.length !== 7 || row.some(value => typeof value !== "string"))
      throw new Error("Invalid DB2 pooled session state values");
    const sessionUser = (row[0] as string).trim();
    const schema = (row[1] as string).trim();
    const isolation = (row[2] as string).trim();
    if (schema.length === 0 || schema.length > 128) throw new Error("Invalid CURRENT SCHEMA register");
    if (!["", "UR", "CS", "RS", "RR"].includes(isolation)) throw new Error("Invalid CURRENT ISOLATION register");
    if (sessionUser.length === 0 || sessionUser.length > 128) throw new Error("Invalid SESSION_USER register");
    return {
      schema,
      isolation,
      sessionUser,
      clientInfo: {
        applicationName: row[3] as string,
        workstationName: row[4] as string,
        userId: row[5] as string,
        accounting: row[6] as string,
      },
    };
  }

  private async restorePoolStateLocked(current: PoolSessionState): Promise<void> {
    const baselineUser = this.baseSessionUser ?? this.baseUser;
    this.currentUser = current.sessionUser;
    if (current.sessionUser !== baselineUser) await this.switchUserLocked(baselineUser, undefined, false);
    if (current.schema !== this.baseSchema) {
      await this.executeSqlSetLocked(`SET CURRENT SCHEMA ${quoteIdentifier(this.baseSchema ?? "")}`);
      this.preparedSql = undefined;
    }
    if (current.isolation !== this.baseIsolation) {
      await this.executeSqlSetLocked(`SET CURRENT ISOLATION ${this.baseIsolation || "RESET"}`);
      this.preparedSql = undefined;
    }
    this.clientInfo = { ...this.clientInfo, ...current.clientInfo };
    await this.applyClientInfoLocked(this.baseClientInfo);
    if (!this.autoCommit) this.autoCommit = true;
  }

  /** Starts a transaction and returns a handle that restores auto-commit after commit or rollback. */
  async beginTransaction(isolation?: Db2IsolationLevel, options: QueryOptions = {}): Promise<Db2Transaction> {
    if (isolation !== undefined && !["UR", "CS", "RS", "RR"].includes(isolation)) throw new RangeError("Unsupported Db2 LUW isolation level");
    return this.exclusive(async () => {
      this.ensureOpen();
      if (this.transactionActive || !this.autoCommit) throw new Error("a DB2 transaction is already active");
      if (isolation !== undefined) {
        const rows = (await this.queryLocked("VALUES CURRENT ISOLATION")).rows;
        const baseline = rows.length === 1 && rows[0]?.length === 1 && typeof rows[0][0] === "string" ? rows[0][0].trim() : undefined;
        if (baseline === undefined || !["", "UR", "CS", "RS", "RR"].includes(baseline)) throw new Error("Invalid CURRENT ISOLATION register");
        try {
          // SET ISOLATION is not transaction controlled; preserve it explicitly.
          // https://www.ibm.com/docs/en/db2/11.5.x?topic=statements-set-current-isolation
          await this.executeSqlSetLocked(`SET CURRENT ISOLATION ${isolation}`);
          this.preparedSql = undefined;
          this.transactionIsolationBaseline = baseline;
        } catch(error) { this.closed = true; this.socket.destroy(); throw error; }
      }
      this.transactionActive = true;
      this.autoCommit = false;
      return new Db2Transaction(this, ++this.transactionGeneration);
    }, options);
  }

  /** @internal Completes the transaction while holding the session operation lock. */
  async endTransaction(commit: boolean, generation: number, options: QueryOptions = {}): Promise<void> {
    await this.exclusive(async () => {
      this.ensureOpen();
      if (!this.isTransactionActive(generation)) throw new Error("DB2 transaction is no longer active");
      try {
        await this.executeControlLocked(commit ? packCommit() : packRollback());
        if (this.transactionIsolationBaseline !== undefined) {
          await this.executeSqlSetLocked(`SET CURRENT ISOLATION ${this.transactionIsolationBaseline || "RESET"}`);
          this.preparedSql = undefined;
        }
      } catch (error) {
        // Transaction-control errors leave the server-side unit of work uncertain.
        // Force the pool to discard this transport instead of reusing it.
        this.closed = true;
        throw error;
      } finally {
        this.autoCommit = true;
        this.transactionActive = false;
        this.transactionIsolationBaseline = undefined;
        this.transactionGeneration++;
      }
    }, options);
  }

  /** @internal Used to reject transaction handles after their session has been reset or reused. */
  isTransactionActive(generation: number): boolean {
    return this.transactionActive && this.transactionGeneration === generation;
  }

  setAutoCommit(enabled: boolean): void { this.autoCommit = enabled; }

  async close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    const wasClosed = this.closed;
    this.closed = true;
    if (this.socket.closed) return;
    this.closePromise = new Promise<void>((resolve) => {
      let timer: NodeJS.Timeout | undefined;
      const complete = (): void => {
        if (timer) clearTimeout(timer);
        resolve();
      };
      // The end callback only confirms FIN was written. Physical resource
      // release is confirmed by close, including a previously destroyed socket.
      this.socket.once('close', complete);
      timer = setTimeout(() => this.socket.destroy(), 250);
      if (this.socket.destroyed) return;
      if (wasClosed) this.socket.destroy();
      else this.socket.end();
    });
    return this.closePromise;
  }

  private async handshake(): Promise<void> {
    this.correlationId = 1;
    const mechanism = this.securityMechanism;
    let clientPrivateKey: bigint | undefined;
    let accsecToken: Uint8Array = new Uint8Array();
    let gssapiToken: Uint8Array = new Uint8Array();
    if (mechanism === 7 || mechanism === 9) {
      clientPrivateKey = generateSecmec9PrivateKey();
      accsecToken = secmec9PublicKey(clientPrivateKey);
    } else if (mechanism === 11) {
      gssapiToken = new Uint8Array(await readFile(this.config.gssapiTokenFile));
    }
    await this.send(packExcsat("ts-db2", hostname() || "ts-db2-client", "SQL12010", "ts-db2"), false, false);
    await this.send(packAccsec(this.database, mechanism, accsecToken, mechanism === 7 ? 1 : undefined), false, true);
    const negotiation = await this.readReplyChain();
    // ACCSECRD echoes the requested SECMEC on success. On rejection it carries
    // the list of mechanisms the server accepts (one SECMEC parameter holding
    // several 2-byte values), SECCHKCD and, for encrypting servers, the ENCALG
    // list -- but no SECTKN, so the client cannot switch mechanisms in SECCHK.
    let serverMechanisms: number[] = [mechanism];
    let accsecCheckCode = 0;
    let securityToken: Uint8Array = new Uint8Array();
    let encryptionAlgorithms: number[] | undefined;
    for (const reply of negotiation) {
      if (reply.codePoint === CodePoint.RDBNFNRM) throw new Db2DatabaseNotFoundError();
      if (reply.codePoint === CodePoint.ACCSECRD) {
        const fields = parseDdmParameters(reply.payload);
        const mechanisms = fields.get(CodePoint.SECMEC);
        if (mechanisms) serverMechanisms = parseUint16List(mechanisms, "security mechanism");
        accsecCheckCode = fields.get(CodePoint.SECCHKCD)?.[0] ?? accsecCheckCode;
        securityToken = fields.get(CodePoint.SECTKN) ?? securityToken;
        const algorithms = fields.get(CodePoint.ENCALG);
        if (algorithms) encryptionAlgorithms = parseUint16List(algorithms, "encryption algorithm");
      }
    }
    if (serverMechanisms.length !== 1 || serverMechanisms[0] !== mechanism) {
      throw new Db2UnsupportedFeatureError(`DB2 rejected security mechanism ${mechanism}${accsecCheckCode ? ` (SECCHKCD=${accsecCheckCode})` : ""}; the server accepts ${serverMechanisms.join(", ")}. Set security_mechanism explicitly; mechanism changes are not negotiated automatically`);
    }
    if (accsecCheckCode !== 0) throw new Db2AuthenticationError(`DB2 rejected the security exchange (SECCHKCD=${accsecCheckCode})`, accsecCheckCode);
    const negotiated = mechanism;

    if (negotiated === 3) {
      await this.send(packSecchk(3, securityToken, this.database, this.config.user, utf8EncoderShared.encode(this.config.password), false, "cp500"), false, false);
    } else if (negotiated === 9) {
      if (!clientPrivateKey) throw new Db2UnsupportedFeatureError("DB2 EUSRIDPWD requires a client Diffie-Hellman key pair");
      if (securityToken.length < 20) throw new Db2AuthenticationError("DB2 EUSRIDPWD key exchange did not return a usable server security token");
      const encryptedUserid = encryptSecmec9(encodeCp500(this.config.user), securityToken, clientPrivateKey);
      const encryptedPassword = encryptSecmec9(encodeCp500(this.config.password), securityToken, clientPrivateKey);
      await this.send(packSecchkEusridpwd(this.database, encryptedUserid, encryptedPassword), false, false);
    } else if (negotiated === 7) {
      if (!clientPrivateKey) throw new Db2UnsupportedFeatureError("DB2 USRENCPWD requires a client Diffie-Hellman key pair");
      // ENCALG is absent on older servers (DES implied); Db2 LUW echoes ENCALG=1.
      if (encryptionAlgorithms && (encryptionAlgorithms.length !== 1 || encryptionAlgorithms[0] !== 1)) {
        throw new Db2UnsupportedFeatureError(`DB2 selected USRENCPWD encryption algorithm ENCALG=${encryptionAlgorithms.join(",")}; this driver supports DES (ENCALG=1) only`);
      }
      if (securityToken.length < 20) throw new Db2AuthenticationError("DB2 USRENCPWD key exchange did not return a usable server security token");
      let encryptedPassword: Uint8Array;
      try {
        encryptedPassword = encryptSecmec7(encodeCp500(this.config.password), this.config.user, securityToken, clientPrivateKey);
      } catch (error) {
        throw new Db2AuthenticationError(`DB2 USRENCPWD key exchange failed: ${error instanceof Error ? error.message : String(error)}`);
      }
      await this.send(packSecchkUsrencpwd(this.database, this.config.user, encryptedPassword), false, false);
    } else {
      await this.send(packSecchkKerberos(this.database, gssapiToken, this.config.user), false, false);
    }
    await this.send(packAccrdb("SQL12010", this.database, "cp500"), false, true);
    const authenticated = await this.readReplyChain();
    for (const reply of authenticated) {
      if (reply.codePoint === CodePoint.SECCHKRM) {
        const code = parseDdmParameters(reply.payload).get(CodePoint.SECCHKCD);
        if (code?.length && code[0] !== 0) throw new Db2AuthenticationError(`DB2 authentication failed (SECCHKCD=${code[0]})`, code[0]);
      } else if (reply.codePoint === CodePoint.ACCRDBRM) {
        const token = parseDdmParameters(reply.payload).get(CodePoint.RDBINTTKN);
        if (token?.length) this.rdbInterruptToken = token.slice();
      } else if (reply.codePoint === CodePoint.RDBAFLRM || reply.codePoint === CodePoint.RDBATHRM) {
        throw new Db2AuthenticationError("DB2 authorization failed while connecting to the database");
      } else if (reply.codePoint === CodePoint.SQLERRRM) throw serverError(reply.payload);
    }
    await this.initializeSession();
  }

  private async initializeSession(): Promise<void> {
    this.correlationId = 1;
    await this.send(packExcsatManagerLevels([{ manager: CodePoint.CCSIDMGR, level: 1208 }]), false, false);
    await this.send(packExecuteSqlSet(this.packageId, "", 1, this.database), true, false);
    const workstation = sanitizeClientRegister(this.config.clientWrkstnName || hostname() || "ts-db2-client");
    const escaped = escapeSqlValue(workstation);
    await this.send(packSqlStatement(`SET CLIENT WRKSTNNAME '${escaped}'`), false, false);
    await this.send(packCommit(), false, true);
    await this.readReplyChain();
    this.clientInfo.workstationName = workstation;
    await this.applyClientInfoLocked({
      applicationName: this.config.clientApplName,
      workstationName: workstation,
      userId: this.config.clientUserid,
      accounting: this.config.clientAcctng,
      correlationToken: this.config.clientCorrToken,
    });
    this.baseClientInfo = { ...this.clientInfo };
  }

  private async applyClientInfoLocked(target: Db2ClientInfo): Promise<void> {
    target = sanitizeClientInfo(target);
    const values: { key: keyof Db2ClientInfo; commands: (value: string) => string[] }[] = [
      { key: "applicationName", commands: (value) => [`SET CLIENT APPLNAME '${escapeSqlValue(value)}'`] },
      { key: "workstationName", commands: (value) => [`SET CLIENT WRKSTNNAME '${escapeSqlValue(value)}'`] },
      { key: "userId", commands: (value) => [`SET CLIENT USERID '${escapeSqlValue(value)}'`] },
      { key: "correlationToken", commands: (value) => [`SET CLIENT CORR_TOKEN '${escapeSqlValue(value)}'`, `SET CLIENT_CORR_TOKEN = '${escapeSqlValue(value)}'`, `SET CLIENT PROGRAMID '${escapeSqlValue(value)}'`] },
    ];
    for (const { key, commands } of values) {
      if (this.clientInfo[key] === target[key]) continue;
      let lastError: unknown;
      for (const sql of commands(target[key])) {
        try {
          await this.executeSqlSetLocked(sql);
          this.clientInfo[key] = target[key];
          lastError = undefined;
          break;
        } catch (error) { lastError = error; }
      }
      if (lastError !== undefined) throw lastError;
    }
    if (this.clientInfo.accounting !== target.accounting) {
      const accounting = escapeSqlValue(target.accounting);
      await this.executeLocked(`CALL SYSPROC.WLM_SET_CLIENT_INFO('', '', '', '${accounting}', NULL)`);
      await this.executeSqlSetLocked(`SET CLIENT APPLNAME '${escapeSqlValue(target.applicationName)}'`);
      await this.executeSqlSetLocked(`SET CLIENT WRKSTNNAME '${escapeSqlValue(target.workstationName)}'`);
      await this.executeSqlSetLocked(`SET CLIENT USERID '${escapeSqlValue(target.userId)}'`);
      this.clientInfo.accounting = target.accounting;
    }
  }

  /** Applies go-db2-style per-operation context: target user and client-info registers. */
  private async applyContextStateLocked(user?: string, clientInfo?: Partial<Db2ClientInfo>): Promise<void> {
    if (user !== undefined) {
      const target = validateUserIdentifier(user);
      if (target !== this.currentUser) await this.switchUserLocked(target);
    }
    if (clientInfo !== undefined) {
      await this.applyClientInfoLocked({
        applicationName: clientInfo.applicationName ?? this.clientInfo.applicationName,
        workstationName: clientInfo.workstationName ?? this.clientInfo.workstationName,
        userId: clientInfo.userId ?? this.clientInfo.userId,
        accounting: clientInfo.accounting ?? this.clientInfo.accounting,
        correlationToken: clientInfo.correlationToken ?? this.clientInfo.correlationToken,
      });
    }
  }

  private async switchUserLocked(target: string, password?: string, allowProtocolFallback = true): Promise<void> {
    if (target === this.currentUser) return;
    let lastError: unknown;
    for (const sql of [`SET SESSION AUTHORIZATION = "${target}"`, `SET SESSION_USER = "${target}"`]) {
      try {
        await this.executeLocked(sql);
        this.currentUser = target;
        return;
      } catch (error) { lastError = error; }
    }
    if (!allowProtocolFallback) {
      if (lastError instanceof Error) throw lastError;
      throw new Error("DB2 session user switch failed");
    }
    // DRDA re-authentication fallback, mirroring go-db2: a standalone SECCHK
    // revalidates the new identity on the existing connection context.
    if (this.securityMechanism !== 3) {
      throw new Db2UnsupportedFeatureError(`DB2 user switch fallback requires security mechanism 3; current mechanism ${this.securityMechanism} is not supported for mid-session re-authentication`);
    }
    this.correlationId = 1;
    const passwordBytes = password === undefined ? new Uint8Array() : utf8EncoderShared.encode(password);
    await this.send(packSecchk(3, new Uint8Array(), this.database, target, passwordBytes, false, "cp500"), false, true);
    let sawSecchkRm = false;
    for (const reply of await this.readReplyChain()) {
      if (reply.codePoint === CodePoint.SECCHKRM) {
        sawSecchkRm = true;
        const code = parseDdmParameters(reply.payload).get(CodePoint.SECCHKCD);
        if (code?.length && code[0] !== 0) throw new Db2AuthenticationError(`DB2 user switch failed (SECCHKCD=${code[0]})`, code[0]);
      } else if (reply.codePoint === CodePoint.SQLERRRM) throw serverError(reply.payload);
    }
    if (!sawSecchkRm) throw new Db2Error("DB2 user switch SECCHK response did not contain SECCHKRM", "server");
    this.currentUser = target;
  }

  private async executeSqlSetLocked(sql: string): Promise<void> {
    this.correlationId = 1;
    await this.send(packExecuteSqlSet(this.packageId, "", 1, this.database), true, false);
    await this.send(packSqlStatement(sql), false, !this.autoCommit);
    if (this.autoCommit) await this.send(packCommit(), false, true);
    for (const reply of await this.readReplyChain()) {
      if (reply.codePoint === CodePoint.SQLCARD) {
        const card = this.parseSqlCardAndReportWarning(reply.payload);
        if (card.sqlCode < 0) throw sqlError(card.sqlCode, card.sqlState, card.message);
      } else if (reply.codePoint === CodePoint.SQLERRRM) throw serverError(reply.payload);
    }
  }

  private async fetchQuery(firstReplies: Reply[], describedColumns: readonly ColumnDescription[] = []): Promise<QueryResult> {
    const state: QueryStreamState = {
      columns: [...describedColumns], fields: [], decoderPlan: undefined, pendingRowData: new Uint8Array(), extdta: [], extdtaHead: 0, open: false, ended: false, failed: false, queryId: 0n, nextCorrelation: 1,
    };
    const rows: unknown[][] = [];
    let materializedResultBytes = 0;
    const consumeAndResolve = async (replies: Reply[]): Promise<void> => {
      const blockRows = this.consumeStreamingReplies(replies, state);
      await this.resolveProgressiveLobs(blockRows, state.fields, state.queryId, state.nextCorrelation);
      for (const row of blockRows) {
        const rowBytes = estimateMaterializedRowBytes(row);
        if (rowBytes > DSS_LIMITS.maxMaterializedResultBytes - materializedResultBytes)
          throw new Db2ResourceLimitError("DB2 materialized result exceeds the " + DSS_LIMITS.maxMaterializedResultBytes + "-byte limit");
        materializedResultBytes += rowBytes;
      }
      rows.push(...blockRows);
    };
    await consumeAndResolve(firstReplies);
    while (state.open && !state.ended) {
      const priorPending = state.pendingRowData.length;
      const priorExternal = state.extdta.length - state.extdtaHead;
      await this.send(packContinueQuery(this.packageId, this.consistencyToken, this.sectionNumber, this.database, this.queryBlockSize, state.queryId), false, true, state.nextCorrelation);
      const replies = await this.readReplyChain();
      const gotData = replies.some(reply => reply.codePoint === CodePoint.QRYDTA || reply.codePoint === CodePoint.EXTDTA);
      await consumeAndResolve(replies);
      if (!gotData && state.pendingRowData.length === priorPending && state.extdta.length - state.extdtaHead === priorExternal && !state.ended)
        throw new Error("DB2 CNTQRY returned neither query data nor an end-of-query reply");
    }
    if (state.pendingRowData.length !== 0) throw new Error("DB2 ended a query in the middle of a row");
    if (state.extdta.length - state.extdtaHead !== 0 || hasUnresolvedExternalLocator(state.fields, rows))
      throw new Error("DB2 ended a query with unresolved LOB data");
    return { columns: state.columns, rows };
  }

  private async resolveProgressiveLobs(
    rows: unknown[][],
    fields: readonly FieldDescriptor[],
    queryId: bigint,
    correlationId: number,
  ): Promise<void> {
    try {
      await this.resolveProgressiveLobsCore(rows, fields, queryId, correlationId);
    } catch (error) {
      // A locator failure leaves the open cursor's position uncertain. Only the
      // server-confirmed -952 response proves that Db2 ended the active request.
      if (!isConfirmedInterrupt(error)) {
        this.closed = true;
        this.socket.destroy();
      }
      throw error;
    }
  }

  private async resolveProgressiveLobsCore(
    rows: unknown[][],
    fields: readonly FieldDescriptor[],
    queryId: bigint,
    correlationId: number,
  ): Promise<void> {
    for (const row of rows) {
      for (let column = 0; column < fields.length && column < row.length; column++) {
        const progressive = getProgressiveLobReference(row[column]);
        if (!progressive) continue;
        const field = fields[column]!;
        if (progressive.length === 0n) {
          row[column] = emptyExternalLobValue(field);
          continue;
        }

        const chunks: Uint8Array[] = [];
        let totalLength = 0;
        while (true) {
          await this.send(
            packGetNextChunk(this.packageId, this.consistencyToken, this.sectionNumber, this.database, queryId, progressive.reference, 1_048_576n),
            false,
            true,
            correlationId,
          );
          const replies = await this.readProgressiveChunkReplyChain();
          for (const reply of replies) {
            if (reply.codePoint === CodePoint.SQLERRRM) throw serverError(reply.payload);
            if (reply.codePoint === CodePoint.SQLCARD) {
              const card = this.parseSqlCardAndReportWarning(reply.payload);
              if (card.sqlCode < 0) throw sqlError(card.sqlCode, card.sqlState, card.message);
            }
          }
          const chunk = parseProgressiveChunkReply(replies);
          if (totalLength + chunk.data.length > DSS_LIMITS.maxLobMaterializationBytes)
            throw new Error("DB2 progressive LOB exceeds the supported materialized-value size");
          chunks.push(chunk.data);
          totalLength += chunk.data.length;
          if (chunk.endOfReference || chunk.isLastChunk) break;
          if (chunk.data.length === 0) throw new Error("DB2 GETNXTCHK made no progress before the final chunk");
        }
        row[column] = decodeExternalLobValue(field, concat(chunks));
      }
    }
  }

  private createProgressiveLobStream(
    progressive: ProgressiveLobReference,
    field: FieldDescriptor,
    state: QueryStreamState,
    active: ActiveOperation,
    options: QueryOptions,
  ): PendingProgressiveLob {
    const text = isClobType(field.type);
    let ended = progressive.length === 0n;
    let claimed = false;
    let remaining = progressive.length;
    let pendingLob!: PendingProgressiveLob;
    let drainTask: Promise<void> | undefined;
    let requestTail: Promise<void> = Promise.resolve();
    const serialized = async <T>(work: () => Promise<T>): Promise<T> => {
      const previous = requestTail;
      let release!: () => void;
      requestTail = new Promise<void>(resolve => { release = resolve; });
      await previous;
      try { return await work(); }
      finally { release(); }
    };
    const nextChunk = (): Promise<Uint8Array | undefined> => serialized(async () => {
      if (ended) return undefined;
      if (state.activeProgressiveLob && state.activeProgressiveLob !== pendingLob)
        throw new Error("Consume or close the current Db2 LOB stream before reading another locator");
      state.activeProgressiveLob = pendingLob;
      try { active.throwIfCancelled(); }
      catch (error) {
        const cancellation = active.cancellationCompletion();
        if (cancellation) {
          try {
            const acknowledged = await cancellation;
            if (!acknowledged) throw new Error("DB2 did not confirm cancellation of the active query");
            const interruptedReplies = await this.readReplyChain();
            let confirmed = false;
            for (const reply of interruptedReplies) {
              if (reply.codePoint === CodePoint.SQLERRRM) throw serverError(reply.payload);
              if (reply.codePoint === CodePoint.SQLCARD) {
                const card = this.parseSqlCardAndReportWarning(reply.payload);
                if (card.sqlCode < 0) {
                  const serverFailure = sqlError(card.sqlCode, card.sqlState, card.message);
                  if (!isConfirmedInterrupt(serverFailure)) throw serverFailure;
                  confirmed = true;
                }
              }
            }
            if (!confirmed) throw new Error("DB2 cancellation completed without a confirmed SQLCODE -952 response");
            ended = true;
            if (state.activeProgressiveLob === pendingLob) state.activeProgressiveLob = undefined;
            state.ended = true;
            state.failed = true;
          } catch (cleanupError) {
            this.closed = true;
            this.socket.destroy();
            throw cleanupError;
          }
        }
        throw error;
      }
      active.resumeDeadline();
      try {
        // 64 KiB bounds the driver's per-step receive/decode buffer. Db2 may
        // return less; each chunk is consumed before requesting the next one.
        await this.send(packGetNextChunk(this.packageId, this.consistencyToken, this.sectionNumber, this.database, state.queryId, progressive.reference, 65_536n), false, true, state.nextCorrelation);
        const replies = await this.readProgressiveChunkReplyChain();
        for (const reply of replies) {
          if (reply.codePoint === CodePoint.SQLERRRM) throw serverError(reply.payload);
          if (reply.codePoint === CodePoint.SQLCARD) {
            const card = this.parseSqlCardAndReportWarning(reply.payload);
            if (card.sqlCode < 0) throw sqlError(card.sqlCode, card.sqlState, card.message);
          }
        }
        const chunk = parseProgressiveChunkReply(replies);
        if (BigInt(chunk.data.length) > remaining) throw new Error("DB2 GETNXTCHK returned more bytes than the advertised LOB length");
        remaining -= BigInt(chunk.data.length);
        ended = chunk.endOfReference || chunk.isLastChunk || remaining === 0n;
        if (ended && state.activeProgressiveLob === pendingLob) state.activeProgressiveLob = undefined;
        if (!ended && chunk.data.length === 0) throw new Error("DB2 GETNXTCHK made no progress before the final chunk");
        if (ended && remaining !== 0n) throw new Error("DB2 ended a progressive LOB before its advertised length");
        return chunk.data;
      } catch (error) {
        if (isConfirmedInterrupt(error)) {
          ended = true;
          if (state.activeProgressiveLob === pendingLob) state.activeProgressiveLob = undefined;
          state.ended = true;
          state.failed = true;
        }
        else { this.closed = true; this.socket.destroy(); }
        throw error;
      } finally { active.pauseDeadline(); }
    });
    const drain = (): Promise<void> => {
      if (drainTask) return drainTask;
      const cleanupMs = options.cancelCleanupTimeoutMs ?? 30_000;
      drainTask = (async () => {
        let timer: NodeJS.Timeout | undefined;
        const consume = async (): Promise<void> => { while (!ended) await nextChunk(); };
        try {
          await Promise.race([
            consume(),
            new Promise<never>((_, reject) => {
              timer = setTimeout(() => {
                if (!this.closed) { this.closed = true; this.socket.destroy(new Error(`DB2 LOB locator was not drained within ${cleanupMs} ms`)); }
                reject(new Error(`DB2 LOB locator was not drained within ${cleanupMs} ms`));
              }, cleanupMs);
            }),
          ]);
        } catch (error) {
          if (!isConfirmedInterrupt(error) && !state.ended && !this.closed) { this.closed = true; this.socket.destroy(); }
          throw error;
        } finally { if (timer) clearTimeout(timer); }
      })();
      return drainTask;
    };
    const chunks = async function* (owner: Db2Connection): AsyncGenerator<Uint8Array | string> {
      if (claimed) throw new Error("Db2 LOB streams are one-shot; this locator has already been consumed");
      claimed = true;
      let completed = false;
      let primaryError: unknown;
      const decoder = text ? new TextDecoder(isDbClobType(field.type) ? "utf-16be" : "utf-8") : undefined;
      try {
        while (true) {
          const chunk = await nextChunk();
          if (chunk === undefined) break;
          if (decoder) {
            const decoded = decoder.decode(chunk, { stream: true });
            if (decoded.length > 0) yield decoded;
          } else if (chunk.length > 0) yield chunk;
        }
        if (decoder) {
          const tail = decoder.decode();
          if (tail.length > 0) yield tail;
        }
        completed = true;
      } catch (error) {
        primaryError = error;
        throw error;
      } finally {
        const locatorBusy = primaryError instanceof Error
          && primaryError.message === "Consume or close the current Db2 LOB stream before reading another locator";
        if (!completed && !ended && !locatorBusy) {
          try { await drain(); }
          catch (error) {
            if (primaryError === undefined) throw error;
            if (!owner.closed) { owner.closed = true; owner.socket.destroy(); }
          }
        }
      }
    };
    const streamValue: Db2LobStream = text
      ? { kind: "text-lob", length: progressive.length, stream: () => chunks(this) as AsyncIterable<string> }
      : { kind: "binary-lob", length: progressive.length, stream: () => chunks(this) as AsyncIterable<Uint8Array> };
    pendingLob = {
      streamValue,
      drain: async () => {
        if (ended) return;
        if (!claimed) claimed = true;
        await drain();
      },
    };
    return pendingLob;
  }

  private async openProcedureReader(
    prepare: () => Promise<Db2Statement>,
    parameters: readonly Db2Parameter[],
    options: QueryOptions,
  ): Promise<Db2ProcedureReader> {
    validateQueryOptions(options);
    if (!Array.isArray(parameters)) throw new TypeError("DB2 procedure parameters must be an array");
    for (const parameter of parameters) {
      if (!parameter || !("value" in parameter) || !["input", "output", "inputOutput"].includes(parameter.direction))
        throw new TypeError("Invalid DB2 procedure parameter");
    }
    const previous = this.operationTail;
    let release!: () => void;
    this.operationTail = new Promise<void>((resolve) => { release = resolve; });
    let active: ActiveOperation | undefined;
    let serverCursorOpened = false;
    let operationReleased = false;
    const releaseOperation = async (): Promise<void> => {
      if (operationReleased) return;
      operationReleased = true;
      if (active) await active.finish();
      release();
    };
    try {
      await previous;
      if (options.signal?.aborted) throw options.signal.reason ?? new DOMException("The operation was aborted", "AbortError");
      if (this.interruptPending) await this.interruptPending.catch(() => {});
      if (options.signal?.aborted) throw options.signal.reason ?? new DOMException("The operation was aborted", "AbortError");
      this.ensureOpen();
      active = this.startOperation(options);
      const statement = await prepare();
      if (parameters.length !== statement.parameterColumns.length)
        throw new Error(`DB2 expected ${statement.parameterColumns.length} parameters, got ${parameters.length}`);

      const directions = parameters.map(parameter => parameter.direction);
      const outputDirections = directions.some(direction => direction !== "input") ? directions : undefined;
      const params = parameters.map(parameter => parameter.value);
      const encoded = params.length === 0 ? undefined : buildSqlDtaWithExternalData(statement.parameterColumns, params);
      this.correlationId = 1;
      await this.send(packExecuteSqlStatementForResultSets(
        this.packageId, this.consistencyToken, this.sectionNumber, this.database,
        0xffff, this.queryBlockSize, null, outputDirections !== undefined,
      ), encoded !== undefined, encoded === undefined);
      if (encoded) {
        await this.send(encoded.sqlDta, encoded.extDta.length > 0, encoded.extDta.length === 0);
        for (let index = 0; index < encoded.extDta.length; index++)
          await this.sendExternalData(encoded.extDta[index]!, index < encoded.extDta.length - 1, index === encoded.extDta.length - 1);
      }

      let sections: ReturnType<typeof parseResultSetSections> = [];
      let declaredCount: number | undefined;
      let rowsAffected = 0;
      let outputSqlDtaReply: Uint8Array | undefined;
      const outputExtdta: Uint8Array[] = [];
      const cursors: ProcedureCursorState[] = [];
      let current: ProcedureCursorState | undefined;
      let seenInitialQrydta = false;
      const initialCallReplies = await this.readReplyChain();
      if (TRACE_PROCEDURE_CURSOR) traceProcedureCursor(`CALL reply: ${initialCallReplies.map(reply => `0x${reply.codePoint.toString(16)}`).join(',')}`);
      for (const reply of initialCallReplies) {
        switch (reply.codePoint) {
          case CodePoint.RSLSETRM: sections = parseResultSetSections(reply.payload); break;
          case CodePoint.SQLRSLRD: declaredCount = parseSqlResultSetCount(reply.payload); break;
          case CodePoint.SQLDTARD: if (outputDirections) outputSqlDtaReply = reply.payload; break;
          case CodePoint.OPNQRYRM: {
            if (cursors.length >= sections.length) throw new Error("DB2 opened more procedure cursors than RSLSETRM declared");
            current = {
              section: sections[cursors.length]!, columns: [], fields: [], decoderPlan: undefined, rowData: [], rowDataHead: 0, extdta: [], extdtaHead: 0, ended: false,
              queryId: 0n, nextCorrelation: reply.header.correlationId, pendingRowData: new Uint8Array(), readyRows: [], readyRowsHead: 0,
              lastBlock: false, closed: false, stalledFrameCount: 0,
            };
            serverCursorOpened = true;
            const values = findDdmObject(reply.payload, CodePoint.QRYINSID);
            if (values && values.length >= 8) current.queryId = new DataView(values.buffer, values.byteOffset, 8).getBigUint64(0, false);
            cursors.push(current);
            break;
          }
          case CodePoint.SQLCINRD:
            if (!current) throw new Error("DB2 returned SQLCINRD without an open procedure cursor");
            current.columns = parseSqlCursorInfo(reply.payload, this.littleEndian);
            if (TRACE_PROCEDURE_CURSOR) traceProcedureCursor(`SQLCINRD payloadLength=${reply.payload.length} columns=${current.columns.length} ${current.columns.map((column, index) => `#${index + 1}:sqlType=0x${column.sqlType.toString(16)} length=${column.length}`).join(' ')}`);
            break;
          case CodePoint.QRYDSC:
            if (!current) throw new Error("DB2 returned QRYDSC without an open procedure cursor");
            current.fields = parseQueryDescriptor(reply.payload);
            current.decoderPlan = current.fields.length > 0 ? compileRowDecoderPlan(current.fields) : undefined;
            if (TRACE_PROCEDURE_CURSOR) traceProcedureCursor(`QRYDSC fields=${current.fields.length} ${current.fields.map((field, index) => `#${index + 1}:type=0x${field.type.toString(16)} ps=${[...field.ps].map(byte => byte.toString(16).padStart(2, '0')).join('')}`).join(' ')} countMatches=${current.fields.length === current.columns.length}`);
            break;
          case CodePoint.QRYDTA:
            if (!current) throw new Error("DB2 returned QRYDTA without an open procedure cursor");
            if (TRACE_PROCEDURE_CURSOR) traceProcedureCursor(`initial QRYDTA payloadLength=${reply.payload.length}`);
            current.rowData.push(reply.payload);
            // Initial CALL reply data is not a terminal indication. Fetch the open
            // cursor once after consuming its rows so Db2 can return ENDQRYRM / SQLCODE 100.
            seenInitialQrydta = true;
            break;
          case CodePoint.EXTDTA:
            if (!current || (outputDirections && !seenInitialQrydta)) outputExtdta.push(reply.payload);
            else current.extdta.push(reply.payload);
            break;
          case CodePoint.ENDQRYRM: if (current) current.ended = true; break;
          case CodePoint.SQLCARD: {
            const card = this.parseSqlCardAndReportWarning(reply.payload);
            if (card.sqlCode < 0 || card.sqlCode === 464) throw sqlError(card.sqlCode, card.sqlState, card.message);
            rowsAffected += card.rowsAffected;
            if (card.sqlCode === 100 && current) current.ended = true;
            break;
          }
          case CodePoint.SQLERRRM: throw serverError(reply.payload);
        }
      }
      if (declaredCount !== undefined && (declaredCount !== sections.length || cursors.length !== sections.length))
        throw new Error(`DB2 declared ${declaredCount} procedure result sets, returned ${sections.length} package sections and opened ${cursors.length} cursors`);
      if (sections.length > 0 && declaredCount === undefined)
        throw new Error("DB2 returned procedure cursor sections without SQLRSLRD result-set count");
      for (const cursor of cursors) {
        if (cursor.queryId === 0n) throw new Error("DB2 left a procedure cursor open without its query instance ID");
        // SQLCINRD and QRYDSC describe the same cursor. A zero-column SQLCINRD
        // caused by misreading SQLDHGRP lengths must not silently reach callers.
        if (cursor.fields.length !== cursor.columns.length)
          throw new Error(`DB2 returned ${cursor.fields.length} procedure result columns in QRYDSC but ${cursor.columns.length} in SQLCINRD metadata`);
      }
      const outputs = params.map(() => null as unknown);
      let cursorIndex = 0;
      let finalized = false;
      const withActive = async <T>(operation: () => Promise<T>): Promise<T> => {
        active!.throwIfCancelled();
        active!.resumeDeadline();
        try {
          const value = await operation();
          active!.throwIfCancelled();
          return value;
        } catch (error) {
          active!.throwIfCancelled();
          throw error;
        } finally { active!.pauseDeadline(); }
      };
      const closeCursor = async (cursor: ProcedureCursorState): Promise<void> => {
        if (cursor.closed) return;
        await withActive(async () => {
          const closeRequest = packCloseProcedureResultSet(cursor.section, cursor.queryId);
          if (TRACE_PROCEDURE_CURSOR) traceProcedureCursor(`CLSQRY send cursor=${cursorIndex} correlation=${cursor.nextCorrelation} ddm=${traceDdmCodePoints(closeRequest)}`);
          await this.send(closeRequest, false, true, cursor.nextCorrelation);
          const replies = await this.readReplyChain();
          if (TRACE_PROCEDURE_CURSOR) traceProcedureCursor(`CLSQRY reply cursor=${cursorIndex}: ${replies.map(reply => `0x${reply.codePoint.toString(16)}`).join(',')}`);
          this.consumeProcedureCursorReplies(replies, cursor);
        });
        cursor.closed = true;
        cursor.ended = true;
      };
      const readRow = async (): Promise<unknown[] | undefined> => {
        const cursor = cursors[cursorIndex];
        if (!cursor || cursor.closed) return undefined;
        const queuedRowData = (): number => cursor.rowData.length - cursor.rowDataHead;
        const queuedReady = (): number => cursor.readyRows.length - cursor.readyRowsHead;
        const queuedExtdta = (): number => cursor.extdta.length - cursor.extdtaHead;
        const compactRowDataIfNeeded = (): void => {
          if (cursor.rowDataHead > 128 && cursor.rowDataHead * 2 >= cursor.rowData.length) {
            cursor.rowData = cursor.rowData.slice(cursor.rowDataHead);
            cursor.rowDataHead = 0;
          }
        };
        while (true) {
          if (queuedReady() > 0) {
            const row = cursor.readyRows[cursor.readyRowsHead++]!;
            if (cursor.readyRowsHead > 128 && cursor.readyRowsHead * 2 >= cursor.readyRows.length) {
              cursor.readyRows = cursor.readyRows.slice(cursor.readyRowsHead);
              cursor.readyRowsHead = 0;
            }
            return row;
          }
          if (queuedRowData() > 0) {
            const block = cursor.rowData[cursor.rowDataHead++]!;
            compactRowDataIfNeeded();
            const pendingBefore = cursor.pendingRowData;
            let raw: Uint8Array;
            if (pendingBefore.length === 0) {
              raw = block;
            } else {
              raw = new Uint8Array(pendingBefore.length + block.length);
              raw.set(pendingBefore, 0);
              raw.set(block, pendingBefore.length);
            }
            if (!cursor.decoderPlan && cursor.fields.length > 0)
              cursor.decoderPlan = compileRowDecoderPlan(cursor.fields);
            const decoded = cursor.decoderPlan
              ? decodeRowsWithPlan(cursor.decoderPlan, raw, this.littleEndian, warning => this.reportWarning(warning))
              : { rows: [] as unknown[][], consumed: 0, ended: false };
            if (decoded.ended) cursor.ended = true;
            // Pending tail: copy only the remainder, not the full block.
            cursor.pendingRowData = decoded.consumed >= raw.length ? EMPTY_BYTES : raw.subarray(decoded.consumed).slice();
            if (TRACE_PROCEDURE_CURSOR) {
              const pendingFirst = cursor.pendingRowData[0];
              const pendingPrefixPossible = pendingFirst === 0
                || (pendingFirst === 0xff && (cursor.pendingRowData.length === 1 || cursor.pendingRowData[1] === 0));
              let pendingSqlcaRecognized = false;
              let pendingSqlcaShape = "none";
              if (cursor.pendingRowData.length > 0) {
                pendingSqlcaShape = describeSqlcaPrefix(cursor.pendingRowData, this.littleEndian);
                try { pendingSqlcaRecognized = readRowSqlca(cursor.pendingRowData, 0, this.littleEndian) !== undefined; }
                catch { pendingSqlcaRecognized = false; }
              }
              const repeatsPendingPrefix = block.length > 0 && pendingBefore.length >= block.length
                && equalBytes(pendingBefore.subarray(0, block.length), block);
              traceProcedureCursor(`decode QRYDTA rows=${decoded.rows.length} consumed=${decoded.consumed} pendingBefore=${pendingBefore.length} block=${block.length} pendingAfter=${cursor.pendingRowData.length} pendingOffset=${decoded.consumed} first=${pendingFirst === undefined ? 'none' : `0x${pendingFirst.toString(16)}`} prefixPossible=${pendingPrefixPossible} sqlcaRecognized=${pendingSqlcaRecognized} sqlcaShape=${pendingSqlcaShape} repeatsPendingPrefix=${repeatsPendingPrefix} queued=${queuedRowData()} short=${cursor.lastBlock} ended=${cursor.ended}`);
              if (decoded.rows.length === 0 && decoded.consumed === 0 && cursor.pendingRowData.length > 0
                && isFixedScalarProcedureCursor(cursor.fields) && !pendingSqlcaRecognized) {
                cursor.stalledFrameCount = cursor.stalledFrame && equalBytes(cursor.stalledFrame, block)
                  ? cursor.stalledFrameCount + 1 : 1;
                cursor.stalledFrame = block.slice();
                traceProcedureCursor(`no-progress frame repeat=${cursor.stalledFrameCount} frameLength=${block.length} pendingLength=${cursor.pendingRowData.length}`);
                if (cursor.stalledFrameCount >= 3)
                  throw new Error(`DB2 repeated a non-progressing procedure QRYDTA fragment (${block.length} bytes); ${pendingSqlcaShape}`);
              } else {
                cursor.stalledFrame = undefined;
                cursor.stalledFrameCount = 0;
              }
              if (decoded.rows.length === 0 && decoded.consumed === 0 && cursor.pendingRowData.length >= 2 && !pendingPrefixPossible)
                throw new Error("DB2 returned procedure QRYDTA bytes that cannot start a row after no decoder progress");
            } else {
              // Fast stall detection without diagnostic strings.
              if (decoded.rows.length === 0 && decoded.consumed === 0 && cursor.pendingRowData.length > 0
                && isFixedScalarProcedureCursor(cursor.fields)) {
                let recognized = false;
                try { recognized = readRowSqlca(cursor.pendingRowData, 0, this.littleEndian) !== undefined; }
                catch { recognized = false; }
                if (!recognized) {
                  cursor.stalledFrameCount = cursor.stalledFrame && equalBytes(cursor.stalledFrame, block)
                    ? cursor.stalledFrameCount + 1 : 1;
                  cursor.stalledFrame = block.slice();
                  if (cursor.stalledFrameCount >= 3)
                    throw new Error(`DB2 repeated a non-progressing procedure QRYDTA fragment (${block.length} bytes)`);
                } else {
                  cursor.stalledFrame = undefined;
                  cursor.stalledFrameCount = 0;
                }
              } else {
                cursor.stalledFrame = undefined;
                cursor.stalledFrameCount = 0;
              }
              if (decoded.rows.length === 0 && decoded.consumed === 0 && cursor.pendingRowData.length >= 2) {
                const first = cursor.pendingRowData[0];
                const possible = first === 0 || (first === 0xff && (cursor.pendingRowData.length === 1 || cursor.pendingRowData[1] === 0));
                if (!possible) throw new Error("DB2 returned procedure QRYDTA bytes that cannot start a row after no decoder progress");
              }
            }
            // A short query block is Db2's normal end-of-cursor signal, even when
            // ENDQRYRM is omitted. But a block ending inside a row is only a
            // transport fragment: fetch another block before deciding it ended.
            if (cursor.lastBlock && queuedRowData() === 0 && cursor.pendingRowData.length === 0) {
              cursor.ended = true;
              if (TRACE_PROCEDURE_CURSOR) traceProcedureCursor("short QRYDTA ended at row boundary");
            }
            if (decoded.rows.length > 0 && queuedExtdta() > 0 && cursor.decoderPlan?.hasLobs) {
              const available = cursor.extdtaHead === 0 ? cursor.extdta : cursor.extdta.slice(cursor.extdtaHead);
              const used = stitchExtdta(cursor.fields, decoded.rows, available);
              cursor.extdtaHead += used;
              if (cursor.extdtaHead > 128 && cursor.extdtaHead * 2 >= cursor.extdta.length) {
                cursor.extdta = cursor.extdta.slice(cursor.extdtaHead);
                cursor.extdtaHead = 0;
              }
              if (cursor.extdtaHead >= cursor.extdta.length) {
                cursor.extdta = [];
                cursor.extdtaHead = 0;
              }
            }
            // Procedure cursors on the verified profile send LOBs as EXTDTA, but
            // resolve a progressive locator if a server ever returns one so no
            // internal locator object can leak through the public reader.
            if (decoded.rows.length > 0 && cursor.decoderPlan?.hasLobs) {
              await this.resolveProgressiveLobs(decoded.rows, cursor.fields, cursor.queryId, cursor.nextCorrelation);
            }
            for (const r of decoded.rows) cursor.readyRows.push(r);
            if (queuedReady() > 0) {
              const row = cursor.readyRows[cursor.readyRowsHead++]!;
              if (cursor.readyRowsHead > 128 && cursor.readyRowsHead * 2 >= cursor.readyRows.length) {
                cursor.readyRows = cursor.readyRows.slice(cursor.readyRowsHead);
                cursor.readyRowsHead = 0;
              }
              return row;
            }
            continue;
          }
          if (cursor.ended) {
            if (cursor.pendingRowData.length > 0) throw new Error("DB2 ended a procedure cursor with an incomplete row");
            if (queuedExtdta() > 0) throw new Error("DB2 returned procedure-cursor EXTDTA values without matching rows");
            await closeCursor(cursor);
            return undefined;
          }
          const replies = await withActive(async () => {
            const fetchRequest = packContinueProcedureResultSet(cursor.section, this.queryBlockSize, cursor.queryId);
            if (TRACE_PROCEDURE_CURSOR) traceProcedureCursor(`CNTQRY send cursor=${cursorIndex} correlation=${cursor.nextCorrelation} ddm=${traceDdmCodePoints(fetchRequest)}`);
            await this.send(fetchRequest, false, true, cursor.nextCorrelation);
            return this.readReplyChain();
          });
          if (TRACE_PROCEDURE_CURSOR) traceProcedureCursor(`CNTQRY reply cursor=${cursorIndex}: ${replies.map(reply => `0x${reply.codePoint.toString(16)}`).join(',')}`);
          this.consumeProcedureCursorReplies(replies, cursor);
          const data = replies.some(reply => reply.codePoint === CodePoint.QRYDTA);
          if (!data && !cursor.ended) throw new Error("DB2 CNTQRY returned no query data or end-of-query reply for a procedure cursor");
        }
      };
      const drainCurrent = async (): Promise<void> => { while (await readRow() !== undefined) { /* drain rows to keep the cursor protocol synchronized */ } };
      const finish = async (): Promise<Db2ExecutionResult> => {
        if (finalized) return { rowsAffected, outputParameters: outputs, resultSets: [] };
        try {
          while (cursorIndex < cursors.length) {
            await drainCurrent();
            cursorIndex++;
          }
          if (outputDirections) {
            if (outputSqlDtaReply) {
              this.applyOutputParameterReply(outputSqlDtaReply, outputDirections, outputs, outputExtdta);
              // applyOutputParameterValues validates and associates every EXTDTA
              // object with the SQLDTARD row. Do not mistake those consumed bytes
              // for additional, unmatched output objects below.
              outputExtdta.length = 0;
            }
            else if (outputExtdta.length > 0) throw new Error("DB2 returned output-parameter EXTDTA without SQLDTARD metadata");
            else ensureOutputParameterDataReturned(outputDirections, false);
          }
          if (outputExtdta.length > 0) throw new Error("DB2 returned unmatched output-parameter EXTDTA values");
          if (this.autoCommit) await withActive(() => this.commitProcedureCallLocked());
          finalized = true;
          await releaseOperation();
          return { rowsAffected, outputParameters: outputs };
        } catch (error) {
          this.closed = true;
          this.socket.destroy();
          await releaseOperation();
          throw error;
        }
      };
      const fail = async (): Promise<void> => {
        this.closed = true;
        this.socket.destroy();
        await releaseOperation();
      };
      const backend: ProcedureReaderBackend = {
        get currentResultSetIndex() { return cursorIndex; },
        get resultSetCount() { return cursors.length; },
        columns: () => cursors[cursorIndex]?.columns ?? [],
        nextRow: () => withActive(readRow),
        drainCurrent: () => withActive(drainCurrent),
        nextResultSet: async () => withActive(async () => {
          if (cursorIndex >= cursors.length) return false;
          await drainCurrent();
          if (cursorIndex + 1 >= cursors.length) { cursorIndex = cursors.length; return false; }
          cursorIndex++;
          return true;
        }),
        finish: () => withActive(finish),
        fail,
      };
      active.pauseDeadline();
      return new Db2ProcedureReader(backend);
    } catch (error) {
      if (serverCursorOpened) {
        // Once execute was sent, the error path may have left one or more server cursors open.
        this.closed = true;
        this.socket.destroy();
      }
      await releaseOperation();
      throw error;
    }
  }

  private async executeProcedurePreparedLocked(
    statement: Db2Statement,
    params: readonly unknown[],
    outputDirections?: readonly Db2ParameterDirection[],
  ): Promise<{ queryResult: QueryResult; outputParameters: unknown[]; rowsAffected: number }> {
    if (params.length !== statement.parameterColumns.length)
      throw new Error(`DB2 expected ${statement.parameterColumns.length} parameters, got ${params.length}`);
    if (outputDirections && outputDirections.length !== params.length)
      throw new Error("DB2 output parameter directions must match the prepared parameter count");

    const outputParameters = params.map(() => null as unknown);
    const encoded = params.length === 0 ? undefined : buildSqlDtaWithExternalData(statement.parameterColumns, params);
    this.correlationId = 1;
    await this.send(packExecuteSqlStatementForResultSets(
      this.packageId, this.consistencyToken, this.sectionNumber, this.database,
      0xffff, this.queryBlockSize, null, outputDirections?.some(direction => direction !== "input") ?? false,
    ), encoded !== undefined, encoded === undefined);
    if (encoded) {
      await this.send(encoded.sqlDta, encoded.extDta.length > 0, encoded.extDta.length === 0);
      for (let index = 0; index < encoded.extDta.length; index++)
        await this.sendExternalData(encoded.extDta[index]!, index < encoded.extDta.length - 1, index === encoded.extDta.length - 1);
    }

    let sections: ReturnType<typeof parseResultSetSections> = [];
    let declaredCount: number | undefined;
    let rowsAffected = 0;
    let outputSqlDtaReply: Uint8Array | undefined;
    const outputExtdta: Uint8Array[] = [];
    const cursors: ProcedureCursorState[] = [];
    let current: ProcedureCursorState | undefined;
    let seenInitialQrydta = false;
    try {
      for (const reply of await this.readReplyChain()) {
        switch (reply.codePoint) {
          case CodePoint.RSLSETRM:
            sections = parseResultSetSections(reply.payload);
            break;
          case CodePoint.SQLRSLRD:
            declaredCount = parseSqlResultSetCount(reply.payload);
            break;
          case CodePoint.SQLDTARD:
            if (outputDirections) outputSqlDtaReply = reply.payload;
            break;
          case CodePoint.OPNQRYRM: {
            if (cursors.length >= sections.length)
              throw new Error("DB2 opened more procedure cursors than RSLSETRM declared");
            const cursor: ProcedureCursorState = {
              section: sections[cursors.length]!, columns: [], fields: [], decoderPlan: undefined, rowData: [], rowDataHead: 0, extdta: [], extdtaHead: 0, ended: false,
              queryId: 0n, nextCorrelation: reply.header.correlationId,
              pendingRowData: new Uint8Array(), readyRows: [], readyRowsHead: 0, lastBlock: false, closed: false, stalledFrameCount: 0,
            };
            const values = findDdmObject(reply.payload, CodePoint.QRYINSID);
            if (values && values.length >= 8) cursor.queryId = new DataView(values.buffer, values.byteOffset, 8).getBigUint64(0, false);
            current = cursor;
            cursors.push(cursor);
            break;
          }
          case CodePoint.SQLCINRD:
            if (!current) throw new Error("DB2 returned SQLCINRD without an open procedure cursor");
            current.columns = parseSqlCursorInfo(reply.payload, this.littleEndian);
            break;
          case CodePoint.QRYDSC:
            if (!current) throw new Error("DB2 returned QRYDSC without an open procedure cursor");
            current.fields = parseQueryDescriptor(reply.payload);
            current.decoderPlan = current.fields.length > 0 ? compileRowDecoderPlan(current.fields) : undefined;
            break;
          case CodePoint.QRYDTA:
            if (!current) throw new Error("DB2 returned QRYDTA without an open procedure cursor");
            current.rowData.push(reply.payload);
            seenInitialQrydta = true;
            break;
          case CodePoint.EXTDTA:
            if (!current) {
              if (!outputDirections) throw new Error("DB2 returned EXTDTA without an open procedure cursor or output parameters");
              outputExtdta.push(reply.payload);
            } else if (outputDirections && !seenInitialQrydta) {
              // Db2 11.5.8 sends output-parameter EXTDTA after OPNQRYRM/SQLCINRD/QRYDSC
              // but before any cursor QRYDTA. Route pre-data EXTDTA to outputs.
              outputExtdta.push(reply.payload);
            } else current.extdta.push(reply.payload);
            break;
          case CodePoint.ENDQRYRM:
            if (current) current.ended = true;
            break;
          case CodePoint.SQLCARD: {
            const card = this.parseSqlCardAndReportWarning(reply.payload);
            if (card.sqlCode < 0 || card.sqlCode === 464) throw sqlError(card.sqlCode, card.sqlState, card.message);
            rowsAffected += card.rowsAffected;
            if (card.sqlCode === 100 && current) current.ended = true;
            break;
          }
          case CodePoint.SQLERRRM:
            throw serverError(reply.payload);
        }
      }

      if (outputDirections) {
        if (outputSqlDtaReply) this.applyOutputParameterReply(outputSqlDtaReply, outputDirections, outputParameters, outputExtdta);
        else if (outputExtdta.length > 0) throw new Error("DB2 returned output-parameter EXTDTA without SQLDTARD metadata");
        else ensureOutputParameterDataReturned(outputDirections, false);
      }

      if (sections.length === 0 && cursors.length === 0) {
        if (declaredCount !== undefined && declaredCount !== 0)
          throw new Error(`DB2 declared ${declaredCount} procedure result sets but returned no cursor package sections`);
        if (this.autoCommit) await this.commitProcedureCallLocked();
        return { queryResult: { columns: [], rows: [], resultSets: [] }, outputParameters, rowsAffected };
      }
      if (declaredCount !== sections.length || cursors.length !== sections.length)
        throw new Error(`DB2 declared ${declaredCount} procedure result sets, returned ${sections.length} package sections and opened ${cursors.length} cursors`);

      const resultSets: Db2ResultSet[] = [];
      for (const cursor of cursors) {
        if (cursor.queryId === 0n) throw new Error("DB2 left a procedure cursor open without its query instance ID");
        let fetchCount = 0;
        while (!cursor.ended) {
          if (++fetchCount > 65_535) throw new Error("DB2 procedure cursor exceeded the supported fetch-block count");
          await this.send(packContinueProcedureResultSet(cursor.section, this.queryBlockSize, cursor.queryId), false, true, cursor.nextCorrelation);
          const replies = await this.readReplyChain();
          this.consumeProcedureCursorReplies(replies, cursor);
          if (cursor.ended) break;
          const lastData = [...replies].reverse().find(reply => reply.codePoint === CodePoint.QRYDTA);
          if (!lastData) throw new Error("DB2 CNTQRY returned no query data or end-of-query reply for a procedure cursor");
          if (lastData.payload.length < maximumProcedureQueryDataPayload(this.queryBlockSize)) break;
        }

        await this.send(packCloseProcedureResultSet(cursor.section, cursor.queryId), false, true, cursor.nextCorrelation);
        this.consumeProcedureCursorReplies(await this.readReplyChain(), cursor);
        cursor.ended = true;
        const pendingFrames = cursor.rowDataHead === 0 ? cursor.rowData : cursor.rowData.slice(cursor.rowDataHead);
        const availableExtdta = cursor.extdtaHead === 0 ? cursor.extdta : cursor.extdta.slice(cursor.extdtaHead);
        const plan = cursor.decoderPlan ?? (cursor.fields.length > 0 ? compileRowDecoderPlan(cursor.fields) : undefined);
        const rows = plan
          ? decodeRowsWithPlan(plan, concat(pendingFrames), this.littleEndian, (warning) => this.reportWarning(warning)).rows
          : [];
        const extdtaUsed = stitchExtdta(cursor.fields, rows, availableExtdta);
        if (extdtaUsed !== availableExtdta.length || hasUnresolvedExternalLocator(cursor.fields, rows))
          throw new Error("DB2 returned procedure-cursor EXTDTA values that do not match the QRYDSC LOB descriptors");
        resultSets.push({ columns: cursor.columns, rows });
      }

      if (this.autoCommit) await this.commitProcedureCallLocked();
      const first = resultSets[0]!;
      return { queryResult: { columns: first.columns, rows: first.rows, resultSets }, outputParameters, rowsAffected };
    } catch (error) {
      if (cursors.some(cursor => !cursor.ended)) {
        this.closed = true;
        this.socket.destroy();
      }
      throw error;
    }
  }

  private applyOutputParameterReply(
    payload: Uint8Array,
    directions: readonly Db2ParameterDirection[],
    outputParameters: unknown[],
    extdta: readonly Uint8Array[],
  ): void {
    const { fields, values } = parseSqlDtaReply(payload);
    applyOutputParameterValues(fields, values, directions, outputParameters, extdta, this.littleEndian, (warning) => this.reportWarning(warning));
  }

  private consumeProcedureCursorReplies(replies: Reply[], cursor: ProcedureCursorState): void {
    for (const reply of replies) {
      switch (reply.codePoint) {
        case CodePoint.QRYDTA: {
          const maximumPayload = maximumProcedureQueryDataPayload(this.queryBlockSize);
          cursor.rowData.push(reply.payload);
          cursor.lastBlock = reply.payload.length < maximumPayload;
          if (TRACE_PROCEDURE_CURSOR) traceProcedureCursor(`QRYDTA payloadLength=${reply.payload.length} maximumPayload=${maximumPayload} short=${cursor.lastBlock}`);
          break;
        }
        case CodePoint.EXTDTA: cursor.extdta.push(reply.payload); break;
        case CodePoint.ENDQRYRM: cursor.ended = true; break;
        case CodePoint.QRYDSC:
          cursor.fields = parseQueryDescriptor(reply.payload);
          cursor.decoderPlan = cursor.fields.length > 0 ? compileRowDecoderPlan(cursor.fields) : undefined;
          break;
        case CodePoint.OPNQRYRM: {
          cursor.nextCorrelation = reply.header.correlationId;
          const values = findDdmObject(reply.payload, CodePoint.QRYINSID);
          if (values && values.length >= 8) cursor.queryId = new DataView(values.buffer, values.byteOffset, 8).getBigUint64(0, false);
          break;
        }
        case CodePoint.SQLCARD: {
          const card = this.parseSqlCardAndReportWarning(reply.payload);
          if (card.sqlCode < 0 || card.sqlCode === 464) throw sqlError(card.sqlCode, card.sqlState, card.message);
          if (card.sqlCode === 100) cursor.ended = true;
          break;
        }
        case CodePoint.SQLERRRM: throw serverError(reply.payload);
      }
    }
  }

  private async commitProcedureCallLocked(): Promise<void> {
    this.correlationId = 1;
    await this.send(packCommit(), false, true);
    for (const reply of await this.readReplyChain()) {
      if (reply.codePoint === CodePoint.SQLCARD) {
        const card = this.parseSqlCardAndReportWarning(reply.payload);
        if (card.sqlCode < 0) throw sqlError(card.sqlCode, card.sqlState, card.message);
      } else if (reply.codePoint === CodePoint.SQLERRRM) throw serverError(reply.payload);
    }
  }

  private consumeStreamingReplies(replies: Reply[], state: QueryStreamState): unknown[][] {
    let rowData: Uint8Array[] | undefined;
    let rowDataCount = 0;
    for (const reply of replies) {
      switch (reply.codePoint) {
        case CodePoint.SQLDARD: state.columns = parseSqlDard(reply.payload, this.littleEndian); break;
        case CodePoint.QRYDSC:
          state.fields = parseQueryDescriptor(reply.payload);
          state.decoderPlan = state.fields.length > 0 ? compileRowDecoderPlan(state.fields) : undefined;
          break;
        case CodePoint.QRYDTA:
          if (rowData === undefined) rowData = [];
          rowData.push(reply.payload);
          rowDataCount++;
          break;
        case CodePoint.EXTDTA: state.extdta.push(reply.payload); break;
        case CodePoint.OPNQRYRM: {
          const id = findDdmObject(reply.payload, CodePoint.QRYINSID);
          if (id?.length && id.length >= 8) state.queryId = new DataView(id.buffer, id.byteOffset, 8).getBigUint64(0, false);
          state.open = true;
          state.nextCorrelation = reply.header.correlationId;
          break;
        }
        case CodePoint.ENDQRYRM: state.ended = true; break;
        case CodePoint.SQLCARD: {
          const card = this.parseSqlCardAndReportWarning(reply.payload);
          if (card.sqlCode < 0) { state.ended = true; state.failed = true; throw sqlError(card.sqlCode, card.sqlState, card.message); }
          if (card.sqlCode === 100) state.ended = true;
          break;
        }
        case CodePoint.SQLERRRM: state.ended = true; state.failed = true; throw serverError(reply.payload);
      }
    }
    // Pending fast path: no pending bytes + single QRYDTA frame decodes
    // directly from the frame without concat([pending, ...rowData]).
    let block: Uint8Array;
    if (state.pendingRowData.length === 0) {
      if (rowDataCount === 0) {
        block = EMPTY_BYTES;
      } else if (rowDataCount === 1) {
        block = rowData![0]!;
      } else {
        block = concat(rowData!);
      }
    } else if (rowDataCount === 0) {
      block = state.pendingRowData;
    } else if (rowDataCount === 1) {
      const next = rowData![0]!;
      const combined = new Uint8Array(state.pendingRowData.length + next.length);
      combined.set(state.pendingRowData, 0);
      combined.set(next, state.pendingRowData.length);
      block = combined;
    } else {
      // Multiple frames with pending: copy pending + frames once.
      // Only the incomplete tail is copied again on the next block.
      const total = state.pendingRowData.length + rowData!.reduce((s, p) => s + p.length, 0);
      const combined = new Uint8Array(total);
      let off = 0;
      combined.set(state.pendingRowData, off);
      off += state.pendingRowData.length;
      for (const part of rowData!) { combined.set(part, off); off += part.length; }
      block = combined;
    }
    const plan = state.decoderPlan ?? (state.fields.length > 0 ? (state.decoderPlan = compileRowDecoderPlan(state.fields)) : undefined);
    let decoded: { rows: unknown[][]; consumed: number; ended: boolean };
    try {
      decoded = plan
        ? decodeRowsWithPlan(plan, block, this.littleEndian, (warning) => this.reportWarning(warning))
        : { rows: [], consumed: 0, ended: false };
    } catch (error) { state.failed = true; throw error; }
    // Pending tail: only the unconsumed remainder is copied, never the full block.
    state.pendingRowData = decoded.consumed >= block.length ? EMPTY_BYTES : block.subarray(decoded.consumed).slice();
    if (decoded.ended) state.ended = true;
    if (plan?.hasLobs && decoded.rows.length > 0 && state.extdta.length > state.extdtaHead) {
      const available = state.extdtaHead === 0 ? state.extdta : state.extdta.slice(state.extdtaHead);
      const used = stitchExtdta(state.fields, decoded.rows, available);
      state.extdtaHead += used;
      if (state.extdtaHead > 128 && state.extdtaHead * 2 >= state.extdta.length) {
        state.extdta = state.extdta.slice(state.extdtaHead);
        state.extdtaHead = 0;
      }
      if (state.extdtaHead >= state.extdta.length) {
        state.extdta = [];
        state.extdtaHead = 0;
      }
    }
    return decoded.rows;
  }

  private async prepareLocked(sql: string): Promise<Db2Statement> {
    this.correlationId = 1;
    this.preparedSql = undefined;
    await this.send(packPrepareSqlStatement(this.packageId, this.consistencyToken, this.sectionNumber, this.database), true, false);
    await this.send(packSqlStatement(sql), false, false);
    await this.send(packDescribeSqlStatement(this.packageId, this.consistencyToken, this.sectionNumber, this.database), false, true);
    let outputColumns: ColumnDescription[] = [];
    let parameterColumns: ColumnDescription[] = [];
    const replies = await this.readReplyChain();
    for (const reply of replies) {
      if (reply.codePoint === CodePoint.SQLDARD) {
        const columns = parseSqlDard(reply.payload, this.littleEndian);
        if (reply.payload[0] === 0xff) parameterColumns = columns;
        else outputColumns = columns;
      } else if (reply.codePoint === CodePoint.SQLCARD) {
        const card = this.parseSqlCardAndReportWarning(reply.payload);
        if (card.sqlCode < 0) throw sqlError(card.sqlCode, card.sqlState, card.message);
      } else if (reply.codePoint === CodePoint.SQLERRRM) throw serverError(reply.payload);
    }
    this.preparedSql = sql;
    return new Db2Statement(this, sql, outputColumns, parameterColumns);
  }

  private async queryLocked(sql: string, params?: readonly unknown[]): Promise<QueryResult> {
    this.ensureOpen();
    if (isCallStatement(sql)) {
      const prepared = await this.prepareLocked(sql);
      return (await this.executeProcedurePreparedLocked(prepared, params ?? [])).queryResult;
    }
    if (params !== undefined) {
      const prepared = await this.prepareLocked(sql);
      return this.queryPreparedLocked(prepared, params, true);
    }
    this.correlationId = 1;
    this.preparedSql = undefined;
    await this.send(packPrepareSqlStatement(this.packageId, this.consistencyToken, this.sectionNumber, this.database), true, false);
    await this.send(packSqlStatement(sql), false, false);
    this.preparedSql = sql;
    await this.send(packOpenQuery(this.packageId, this.consistencyToken, this.sectionNumber, this.database, this.queryBlockSize), false, true);
    return this.fetchQuery(await this.readReplyChain());
  }

  private async executeLocked(sql: string, params?: readonly unknown[]): Promise<number> {
    this.ensureOpen();
    if (isCallStatement(sql)) {
      const prepared = await this.prepareLocked(sql);
      return (await this.executeProcedurePreparedLocked(prepared, params ?? [])).rowsAffected;
    }
    if (params !== undefined) {
      const prepared = await this.prepareLocked(sql);
      return this.executePreparedLocked(prepared, params, true);
    }
    this.correlationId = 1;
    await this.send(packExecSqlImmediate(this.packageId, this.consistencyToken, this.sectionNumber, this.database), true, false);
    await this.send(packSqlStatement(sql), false, !this.autoCommit);
    if (this.autoCommit) await this.send(packCommit(), false, true);
    const replies = await this.readReplyChain();
    let affected = 0;
    for (const reply of replies) {
      if (reply.codePoint === CodePoint.SQLCARD) {
        const card = this.parseSqlCardAndReportWarning(reply.payload);
        if (card.sqlCode < 0) throw sqlError(card.sqlCode, card.sqlState, card.message);
        affected += card.rowsAffected;
      } else if (reply.codePoint === CodePoint.SQLERRRM) throw serverError(reply.payload);
    }
    return affected;
  }

  private async lastInsertIdLocked(): Promise<bigint> {
    const result = await this.queryLocked("SELECT IDENTITY_VAL_LOCAL() AS LAST_ID FROM SYSIBM.SYSDUMMY1");
    return parseIdentityValue(result.rows[0]?.[0]);
  }

  private async executePreparedLocked(statement: Db2Statement, params: readonly unknown[], alreadyPrepared = false, outputDirections?: readonly Db2ParameterDirection[], outputParameters?: unknown[]): Promise<number> {
    if (!alreadyPrepared && this.preparedSql !== statement.sql) statement = await this.prepareLocked(statement.sql);
    if (params.length !== statement.parameterColumns.length)
      throw new Error(`DB2 expected ${statement.parameterColumns.length} parameters, got ${params.length}`);
    if (isCallStatement(statement.sql)) {
      const result = await this.executeProcedurePreparedLocked(statement, params, outputDirections);
      if (outputParameters) outputParameters.splice(0, outputParameters.length, ...result.outputParameters);
      return result.rowsAffected;
    }
    // Db2 expects an SQLDTA FDODSC/FDODTA pair for the parameter protocol. The current zero-value
    // encoding is accepted by the server without executing the prepared section, so route an
    // explicitly parameterless statement through the direct SQL path instead.
    if (statement.parameterColumns.length === 0) return this.executeLocked(statement.sql);
    const { sqlDta, extDta } = buildSqlDtaWithExternalData(statement.parameterColumns, params);
    this.correlationId = 1;
    const nextId = await this.send(packExecuteSqlStatement(this.packageId, this.consistencyToken, this.sectionNumber, this.database), true, false);
    await this.send(sqlDta, extDta.length > 0, extDta.length === 0 && !this.autoCommit, nextId);
    for (let index = 0; index < extDta.length; index++)
      await this.sendExternalData(extDta[index]!, index < extDta.length - 1, index === extDta.length - 1 && !this.autoCommit);
    if (this.autoCommit) await this.send(packCommit(), false, true, 1);
    let replies = await this.readReplyChain();
    if (replies.length > 0 && replies.every((reply) => reply.codePoint === CodePoint.SQLDARD))
      replies = replies.concat(await this.readReplyChain());
    let affected = 0;
    let outputDataReceived = false;
    for (const reply of replies) {
      if (reply.codePoint === CodePoint.SQLCARD) {
        const card = this.parseSqlCardAndReportWarning(reply.payload);
        if (card.sqlCode < 0) throw sqlError(card.sqlCode, card.sqlState, card.message);
        affected += card.rowsAffected;
      } else if (reply.codePoint === CodePoint.SQLDTARD && outputDirections && outputParameters) {
        const { fields, values } = parseSqlDtaReply(reply.payload);
        applyOutputParameterValues(fields, values, outputDirections, outputParameters, [], this.littleEndian, (warning) => this.reportWarning(warning));
        outputDataReceived = true;
      } else if (reply.codePoint === CodePoint.SQLERRRM) throw serverError(reply.payload);
    }
    if (outputDirections && outputParameters)
      ensureOutputParameterDataReturned(outputDirections, outputDataReceived);
    return affected;
  }

  private async executePreparedWithOutputsLocked(statement: Db2Statement, parameters: readonly Db2Parameter[], alreadyPrepared = false): Promise<Db2ExecutionResult> {
    if (!Array.isArray(parameters)) throw new TypeError("DB2 procedure parameters must be an array");
    if (!alreadyPrepared && this.preparedSql !== statement.sql) statement = await this.prepareLocked(statement.sql);
    if (parameters.length !== statement.parameterColumns.length)
      throw new Error(`DB2 expected ${statement.parameterColumns.length} parameters, got ${parameters.length}`);
    if (!parameters.some(parameter => parameter.direction !== "input"))
      throw new Error("At least one parameter must have output or inputOutput direction");
    for (const parameter of parameters) {
      if (!("value" in parameter) || !["input", "output", "inputOutput"].includes(parameter.direction))
        throw new TypeError("Invalid DB2 procedure parameter");
    }
    if (isCallStatement(statement.sql)) {
      const result = await this.executeProcedurePreparedLocked(
        statement,
        parameters.map(parameter => parameter.value),
        parameters.map(parameter => parameter.direction),
      );
      return { rowsAffected: result.rowsAffected, outputParameters: result.outputParameters, resultSets: result.queryResult.resultSets ?? [] };
    }
    const outputParameters = parameters.map(() => null as unknown);
    const rowsAffected = await this.executePreparedLocked(
      statement,
      parameters.map(parameter => parameter.value),
      true,
      parameters.map(parameter => parameter.direction),
      outputParameters,
    );
    return { rowsAffected, outputParameters };
  }

  private async queryPreparedLocked(statement: Db2Statement, params: readonly unknown[], alreadyPrepared = false): Promise<QueryResult> {
    if (!alreadyPrepared && this.preparedSql !== statement.sql) statement = await this.prepareLocked(statement.sql);
    if (params.length !== statement.parameterColumns.length)
      throw new Error(`DB2 expected ${statement.parameterColumns.length} parameters, got ${params.length}`);
    if (isCallStatement(statement.sql)) return (await this.executeProcedurePreparedLocked(statement, params)).queryResult;
    if (statement.parameterColumns.length === 0) return this.queryLocked(statement.sql);
    const { sqlDta, extDta } = buildSqlDtaWithExternalData(statement.parameterColumns, params);
    this.correlationId = 1;
    const nextId = await this.send(packOpenQueryWithParams(this.packageId, this.consistencyToken, this.sectionNumber, this.database, this.queryBlockSize), true, false);
    await this.send(sqlDta, extDta.length > 0, extDta.length === 0, nextId);
    for (let index = 0; index < extDta.length; index++)
      await this.sendExternalData(extDta[index]!, index < extDta.length - 1, index === extDta.length - 1);
    const result = await this.fetchQuery(await this.readReplyChain(), statement.outputColumns);
    if (this.autoCommit) {
      this.correlationId = 1;
      await this.send(packCommit(), false, true);
      await this.readReplyChain();
    }
    return result;
  }

  private async executeControl(payload: Uint8Array): Promise<void> {
    await this.exclusive(async () => {
      this.ensureOpen();
      await this.executeControlLocked(payload);
    });
  }

  private async executeControlLocked(payload: Uint8Array): Promise<void> {
    this.correlationId = 1;
    await this.send(payload, false, true);
    const replies = await this.readReplyChain();
    for (const reply of replies) if (reply.codePoint === CodePoint.SQLCARD) {
      const card = this.parseSqlCardAndReportWarning(reply.payload);
      if (card.sqlCode < 0) throw sqlError(card.sqlCode, card.sqlState, card.message);
    }
  }

  private async send(payload: Uint8Array, nextHasSameId: boolean, lastPacket: boolean, correlationId = this.correlationId): Promise<number> {
    const encoded = encodeRequestDss(payload, correlationId, nextHasSameId, lastPacket);
    await writeSocket(this.socket, encoded.bytes);
    this.correlationId = encoded.nextId;
    return encoded.nextId;
  }

  private async sendExternalData(data: SqlDtaExternalParameter, nextHasSameId: boolean, lastPacket: boolean): Promise<void> {
    if ("ddmObject" in data) {
      await this.send(data.ddmObject, nextHasSameId, lastPacket);
      return;
    }
    const stream = "blobStream" in data ? data.blobStream : data.textLobStream;
    try {
      this.correlationId = await writeExternalLobDss(
        this.socket,
        stream.source,
        stream.length,
        this.correlationId,
        nextHasSameId,
        lastPacket,
        stream.leaveOpen !== false,
      );
    } catch (error) {
      this.closed = true;
      this.socket.destroy();
      throw error;
    }
  }

  private async readReplyChain(): Promise<Reply[]> {
    const replies: Reply[] = [];
    let chained: boolean;
    let totalBytes = 0;
    do {
      if (replies.length >= DSS_LIMITS.maxReplyChainPackets) {
        this.closed = true;
        this.socket.destroy();
        throw new Error(`DB2 reply chain exceeded ${DSS_LIMITS.maxReplyChainPackets} packets`);
      }
      const reply = await readDss(this.reader);
      totalBytes += reply.payload.length + 6;
      if (totalBytes > DSS_LIMITS.maxReplyChainBytes) {
        this.closed = true;
        this.socket.destroy();
        throw new Error(`DB2 reply chain exceeded ${DSS_LIMITS.maxReplyChainBytes} bytes`);
      }
      replies.push(reply);
      chained = reply.header.chained;
    } while (chained);
    if (replies.some((reply) => reply.codePoint === CodePoint.DTAMCHRM))
      throw new Error("DB2 server returned DTAMCHRM (SQLDTA descriptor and parameter data do not match)");
    return replies;
  }

  /** GETNXTCHK replies pass through the incremental DSS reader and stay below one bounded chunk. */
  private async readProgressiveChunkReplyChain(): Promise<Reply[]> {
    const replies: Reply[] = [];
    let totalBytes = 0;
    let chained: boolean;
    do {
      if (replies.length >= DSS_LIMITS.maxReplyChainPackets)
        throw new Db2ResourceLimitError("DB2 progressive LOB reply chain exceeded the packet limit");
      const packet = await Db2DssStream.open(this.reader, 4 * 1024 * 1024);
      const chunks: Uint8Array[] = [];
      let payloadBytes = 0;
      while (true) {
        const chunk = await packet.nextChunk();
        if (!chunk) break;
        if (chunk.length > 4 * 1024 * 1024 - payloadBytes)
          throw new Db2ResourceLimitError("DB2 progressive LOB reply exceeded the receive-buffer limit");
        payloadBytes += chunk.length;
        chunks.push(chunk);
      }
      totalBytes += payloadBytes + 6;
      if (totalBytes > 4 * 1024 * 1024)
        throw new Db2ResourceLimitError("DB2 progressive LOB reply chain exceeded the receive-buffer limit");
      replies.push({ header: packet.header, codePoint: packet.codePoint, payload: concat(chunks), moreData: false });
      chained = packet.header.chained;
    } while (chained);
    if (replies.some(reply => reply.codePoint === CodePoint.DTAMCHRM))
      throw new Error("DB2 server returned DTAMCHRM while fetching a progressive LOB");
    return replies;
  }

  private async exclusive<T>(operation: () => Promise<T>, options: QueryOptions = {}): Promise<T> {
    validateQueryOptions(options);
    const previous = this.operationTail;
    let release!: () => void;
    this.operationTail = new Promise<void>((resolve) => { release = resolve; });
    try {
      await previous;
      if (options.signal?.aborted) throw options.signal.reason ?? new DOMException("The operation was aborted", "AbortError");
      if (this.interruptPending) await this.interruptPending.catch(() => {});
      if (options.signal?.aborted) throw options.signal.reason ?? new DOMException("The operation was aborted", "AbortError");
      const active = this.startOperation(options);
      try {
        if (options.user !== undefined || options.clientInfo !== undefined) {
          await this.applyContextStateLocked(options.user, options.clientInfo);
        }
        const result = await operation();
        active.throwIfCancelled();
        return result;
      } catch (error) {
        active.throwIfCancelled();
        throw error;
      } finally {
        await active.finish();
      }
    } finally { release(); }
  }

  private startOperation(options: QueryOptions): ActiveOperation {
    let cancellationReason: unknown;
    let interruptRequest: Promise<boolean> | undefined;
    let deadlineTimer: NodeJS.Timeout | undefined;
    let cleanupTimer: NodeJS.Timeout | undefined;
    let finished = false;
    const cleanupMs = options.cancelCleanupTimeoutMs ?? 30_000;
    let deadlineRemaining = options.timeoutMs ?? 0;
    const deadlineEnabled = deadlineRemaining > 0;
    let deadlineStartedAt = 0;
    const timeoutError = (): Db2TimeoutError => new Db2TimeoutError(`DB2 command timed out after ${options.timeoutMs} ms`);
    const armDeadline = (): void => {
      if (finished || deadlineRemaining <= 0 || cancellationReason !== undefined) return;
      deadlineStartedAt = Date.now();
      deadlineTimer = setTimeout(() => {
        deadlineTimer = undefined;
        deadlineRemaining = 0;
        cancel(timeoutError());
      }, deadlineRemaining);
    };
    const pauseDeadline = (): void => {
      if (!deadlineTimer) return;
      clearTimeout(deadlineTimer);
      deadlineTimer = undefined;
      deadlineRemaining = Math.max(0, deadlineRemaining - (Date.now() - deadlineStartedAt));
      if (deadlineEnabled && deadlineRemaining === 0) cancel(timeoutError());
    };
    const cancel = (reason: unknown): void => {
      if (finished || cancellationReason !== undefined) return;
      cancellationReason = reason;
      if (this.interruptSuppressionDepth > 0) return;
      cleanupTimer = setTimeout(() => {
        if (!finished) this.socket.destroy(new Error(`DB2 command did not finish within ${cleanupMs} ms after cancellation`));
      }, cleanupMs);
      // A failed/late SQLINTR handshake leaves the command outcome uncertain. Break
      // the main transport so neither this caller nor the pool can reuse it.
      interruptRequest = this.requestInterrupt(cleanupMs).catch((error: unknown) => {
        // Even if the main reply arrived first, a sent-but-unacknowledged interrupt
        // may still affect the next command. Discard unless the auxiliary exchange
        // completed successfully.
        this.socket.destroy(new Error("DB2 command cancellation could not be confirmed", { cause: error }));
        return false;
      });
    };
    const onAbort = (): void => cancel(options.signal?.reason ?? new DOMException("The operation was aborted", "AbortError"));
    this.operationActive = true;
    options.signal?.addEventListener("abort", onAbort, { once: true });
    if (options.signal?.aborted) onAbort();
    armDeadline();
    return {
      throwIfCancelled() { if (cancellationReason !== undefined) throw cancellationReason; },
      cancellationCompletion: () => interruptRequest,
      pauseDeadline,
      resumeDeadline: () => {
        if (deadlineEnabled && deadlineRemaining === 0) cancel(timeoutError());
        else armDeadline();
      },
      finish: async () => {
        finished = true;
        this.operationActive = false;
        if (deadlineTimer) clearTimeout(deadlineTimer);
        if (cleanupTimer) clearTimeout(cleanupTimer);
        options.signal?.removeEventListener("abort", onAbort);
        if (interruptRequest) await interruptRequest.catch(() => {});
      },
    };
  }

  private parseSqlCardAndReportWarning(payload: Uint8Array) {
    const card = parseSqlCard(payload, this.littleEndian);
    const warning = sqlCardWarning(card);
    if (warning) this.reportWarning(warning);
    return card;
  }

  private reportWarning(warning: Db2Warning): void {
    for (const listener of this.warningListeners) listener(warning);
  }

  private ensureOpen(): void { if (this.closed || this.socket.destroyed) throw new Db2ConnectionClosedError(); }
}

/** A prepared SQL statement. Parameter values are positional and follow the server's SQLDARD metadata. */
export class Db2Statement {
  private closed = false;

  constructor(
    private readonly connection: Db2Connection,
    public readonly sql: string,
    public readonly outputColumns: readonly ColumnDescription[],
    public readonly parameterColumns: readonly ColumnDescription[],
  ) {}

  get isClosed(): boolean { return this.closed; }

  async execute(params: readonly unknown[] = [], options: QueryOptions = {}): Promise<number> {
    this.ensureOpen();
    return this.connection.executeStatement(this, params, options);
  }

  async executeWithOutputs(parameters: readonly Db2Parameter[], options: QueryOptions = {}): Promise<Db2ExecutionResult> {
    this.ensureOpen();
    return this.connection.executeStatementWithOutputs(this, parameters, options);
  }

  /** Starts a streaming CALL reader; output parameters remain hidden until finish(). */
  async executeStream(parameters: readonly Db2Parameter[] = [], options: QueryOptions = {}): Promise<Db2ProcedureReader> {
    this.ensureOpen();
    return this.connection.executeStatementStream(this, parameters, options);
  }

  async executeInsert(params: readonly unknown[] = [], options: QueryOptions = {}): Promise<Db2InsertResult> {
    this.ensureOpen();
    return this.connection.executeInsertStatement(this, params, options);
  }

  async query(params: readonly unknown[] = [], options: QueryOptions = {}): Promise<QueryResult> {
    this.ensureOpen();
    return this.connection.queryStatement(this, params, options);
  }

  queryStream(params: readonly unknown[] = [], options: QueryOptions = {}): AsyncGenerator<QueryStreamRow> {
    this.ensureOpen();
    return this.connection.queryStreamWithParams(this.sql, params, options);
  }

  close(): void { this.closed = true; }

  private ensureOpen(): void {
    if (this.closed) throw new Error("DB2 statement is closed");
  }
}

/** A transaction scope over one Db2 connection. Commit or roll back it before reuse. */
export class Db2Transaction {
  private completed = false;

  constructor(private readonly connection: Db2Connection, private readonly generation: number) {}

  query(sql: string, params?: readonly unknown[], options: QueryOptions = {}): Promise<QueryResult> {
    this.ensureActive();
    return this.connection.query(sql, params, options);
  }

  execute(sql: string, params?: readonly unknown[], options: QueryOptions = {}): Promise<number> {
    this.ensureActive();
    return this.connection.execute(sql, params, options);
  }

  executeInsert(sql: string, params?: readonly unknown[], options: QueryOptions = {}): Promise<Db2InsertResult> {
    this.ensureActive();
    return this.connection.executeInsert(sql, params, options);
  }

  async commit(options: QueryOptions = {}): Promise<void> { await this.finish(true, options); }
  async rollback(options: QueryOptions = {}): Promise<void> { await this.finish(false, options); }

  private async finish(commit: boolean, options: QueryOptions): Promise<void> {
    this.ensureActive();
    try {
      await this.connection.endTransaction(commit, this.generation, options);
      this.completed = true;
    } catch (error) {
      // An abort before the operation lock is acquired sends no RDBCMM/RDBRLLBCK.
      // Keep the scope usable in that case; an attempted exchange advances generation.
      if (!this.connection.isTransactionActive(this.generation)) this.completed = true;
      throw error;
    }
  }

  private ensureActive(): void {
    if (this.completed) throw new Error("DB2 transaction is already complete");
    if (!this.connection.isTransactionActive(this.generation)) throw new Error("DB2 transaction is no longer active");
  }
}

export function connectDb2(input: string | Config): Promise<Db2Connection> { return Db2Connection.connect(input); }

function validateQueryOptions(options: QueryOptions): void {
  for (const [name, value, allowZero] of [
    ["timeoutMs", options.timeoutMs, true],
    ["cancelCleanupTimeoutMs", options.cancelCleanupTimeoutMs, false],
  ] as const) {
    if (value === undefined) continue;
    if (!Number.isSafeInteger(value) || value < (allowZero ? 0 : 1) || value > 2_147_483_647)
      throw new RangeError(`${name} must be an integer between ${allowZero ? 0 : 1} and 2147483647`);
  }
  if (options.user !== undefined && typeof options.user !== "string")
    throw new TypeError("user must be a string");
  if (options.clientInfo !== undefined && (typeof options.clientInfo !== "object" || options.clientInfo === null))
    throw new TypeError("clientInfo must be an object");
}

function openTcp(host: string, port: number, timeoutMs: number): Promise<Socket> {
  return new Promise((resolve, reject) => {
    const socket = connectTcp({ host, port });
    const timeout = setTimeout(() => socket.destroy(new Error(`DB2 TCP connection timed out after ${timeoutMs} ms`)), timeoutMs);
    socket.once("connect", () => { clearTimeout(timeout); socket.setNoDelay(true); resolve(socket); });
    socket.once("error", (error) => { clearTimeout(timeout); reject(error); });
  });
}

async function wrapTls(socket: Socket, config: Config, timeoutMs: number): Promise<Socket> {
  const targetHost = config.host.startsWith("[") && config.host.endsWith("]")
    ? config.host.slice(1, -1)
    : config.host;
  const options: ConnectionOptions = {
    socket,
    minVersion: "TLSv1.2",
    checkServerIdentity: (_serverName, certificate) => checkServerIdentity(targetHost, certificate),
  };
  if (isIP(targetHost) === 0) options.servername = targetHost;
  if (config.sslRootCaPath) options.ca = await readFile(config.sslRootCaPath);
  if (config.sslClientCertPath) options.cert = await readFile(config.sslClientCertPath);
  if (config.sslClientKeyPath) options.key = await readFile(config.sslClientKeyPath);
  const tlsSocket = connectTls(options);
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => tlsSocket.destroy(new Error(`DB2 TLS handshake timed out after ${timeoutMs} ms`)), timeoutMs);
    tlsSocket.once("secureConnect", () => { clearTimeout(timeout); resolve(tlsSocket); });
    tlsSocket.once("error", (error) => { clearTimeout(timeout); reject(error); });
  });
}

function writeSocket(socket: Socket, data: Uint8Array): Promise<void> {
  return new Promise((resolve, reject) => socket.write(data, (error) => error ? reject(error) : resolve()));
}

function sqlError(code: number, state: string, message: string): Db2ServerError {
  return new Db2ServerError(code, state, message);
}

function parseIdentityValue(value: unknown): bigint {
  if (typeof value === "bigint") return value;
  if (typeof value === "number" && Number.isFinite(value)) return BigInt(Math.trunc(value));
  const text = String(value ?? "").trim().split(".", 1)[0]!;
  if (!/^[+-]?\d+$/u.test(text)) throw new Error(`DB2 returned an invalid identity value: ${String(value)}`);
  return BigInt(text);
}

/** Decodes a DDM parameter holding one or more big-endian 2-byte values (SECMEC, ENCALG lists). */
function parseUint16List(value: Uint8Array, name: string): number[] {
  if (value.length === 0 || value.length % 2 !== 0) throw new Db2AuthenticationError(`DB2 returned a malformed ${name} list in ACCSECRD`);
  const view = new DataView(value.buffer, value.byteOffset, value.byteLength);
  const out: number[] = [];
  for (let offset = 0; offset < value.length; offset += 2) out.push(view.getUint16(offset, false));
  return out;
}

function sanitizeClientRegister(value: string): string { return value.replace(/[\0\r\n]/gu, ""); }

function sanitizeClientInfo(info: Db2ClientInfo): Db2ClientInfo {
  return {
    applicationName: sanitizeClientRegister(info.applicationName),
    workstationName: sanitizeClientRegister(info.workstationName),
    userId: sanitizeClientRegister(info.userId),
    accounting: sanitizeClientRegister(info.accounting),
    correlationToken: sanitizeClientRegister(info.correlationToken),
  };
}

function escapeSqlValue(value: string): string { return sanitizeClientRegister(value).replaceAll("'", "''"); }

/** Quotes a schema identifier as a delimited Db2 identifier. */
function quoteIdentifier(value: string): string {
  const target = value.trim();
  if (target.length === 0 || target.length > 128) throw new Error(`invalid DB2 identifier ${JSON.stringify(value)}`);
  return `"${target.replaceAll('"', '""')}"`;
}

/** Narrows an unknown row value to the public one-shot progressive LOB stream type. */
export function isDb2LobStream(value: unknown): value is Db2LobStream {
  return typeof value === "object" && value !== null
    && "kind" in value && (value.kind === "binary-lob" || value.kind === "text-lob")
    && "stream" in value && typeof value.stream === "function";
}

function isQueryStreamRow(value: readonly unknown[] | QueryStreamRow): value is QueryStreamRow {
  return !Array.isArray(value) && typeof value === "object" && value !== null
    && "columns" in value && Array.isArray(value.columns)
    && "values" in value && Array.isArray(value.values);
}

async function prepareImportValues(
  values: readonly unknown[],
  sourceColumns: readonly ColumnDescription[] | undefined,
  targetColumns: readonly ColumnDescription[],
  spools: Set<SpooledImportLob>,
  signal: AbortSignal | undefined,
): Promise<readonly unknown[]> {
  const result: unknown[] = [];
  for (let index = 0; index < values.length; index++) {
    const value = values[index];
    const targetType = baseSqlType(targetColumns[index]!.sqlType);
    const sourceColumn = sourceColumns?.[index];
    if (sourceColumn && baseSqlType(sourceColumn.sqlType) !== targetType)
      throw new TypeError(`source SQL type ${baseSqlType(sourceColumn.sqlType)} for column ${sourceColumn.name} does not match target SQL type ${targetType}`);

    if (isDb2LobStream(value)) {
      validateImportLobKind(value, targetType);
      const spool = await spoolImportLob(value, signal);
      spools.add(spool);
      if (spool.length === 0n && (targetType === SqlType.Blob || targetType === SqlType.Clob || targetType === SqlType.DbClob)) {
        await spool.dispose();
        spools.delete(spool);
        result.push(targetType === SqlType.Blob ? new Uint8Array(0) : "");
        continue;
      }
      if (spool.length === 0n && targetType === SqlType.XML)
        throw new TypeError("an empty stream is not a valid Db2 XML document");
      result.push(importLobParameter(targetType, spool.source, spool.length));
      continue;
    }

    if (value === null || value === undefined) {
      result.push(value);
      continue;
    }
    if (targetType === SqlType.Blob && value instanceof Uint8Array) {
      result.push(value.byteLength === 0 ? value : importLobParameter(targetType, byteChunks(value), BigInt(value.byteLength)));
      continue;
    }
    if ((targetType === SqlType.Clob || targetType === SqlType.DbClob || targetType === SqlType.XML)
      && typeof value === "string") {
      if (value.length === 0) {
        if (targetType === SqlType.XML) throw new TypeError("an empty string is not a valid Db2 XML document");
        result.push("");
        continue;
      }
      result.push(importLobParameter(targetType, utf8Chunks(value), BigInt(Buffer.byteLength(value, "utf8"))));
      continue;
    }
    if ((targetType === SqlType.Clob || targetType === SqlType.DbClob || targetType === SqlType.XML)
      && value instanceof Uint8Array) {
      const text = utf8FatalDecoderShared.decode(value);
      if (text.length === 0) {
        if (targetType === SqlType.XML) throw new TypeError("an empty byte sequence is not a valid Db2 XML document");
        result.push("");
        continue;
      }
      result.push(importLobParameter(targetType, byteChunks(value), BigInt(value.byteLength)));
      continue;
    }
    result.push(value);
  }
  return result;
}

function validateImportLobKind(value: Db2LobStream, targetType: number): void {
  const binaryTarget = targetType === SqlType.Blob;
  const textTarget = targetType === SqlType.Clob || targetType === SqlType.DbClob || targetType === SqlType.XML;
  if ((value.kind === "binary-lob" && !binaryTarget) || (value.kind === "text-lob" && !textTarget))
    throw new TypeError(`source ${value.kind} cannot be imported into Db2 SQL type ${targetType}`);
}

function importLobParameter(
  targetType: number,
  source: AsyncIterable<Uint8Array>,
  length: bigint,
): Db2BlobStream | Db2ClobStream | Db2DbClobStream | Db2XmlStream {
  const common = { source, length, leaveOpen: false };
  if (targetType === SqlType.Blob) return common;
  if (targetType === SqlType.Clob) return common;
  if (targetType === SqlType.DbClob) return common;
  if (targetType === SqlType.XML) return common;
  throw new TypeError(`Db2 SQL type ${targetType} is not a streamed LOB target`);
}

async function* byteChunks(value: Uint8Array): AsyncGenerator<Uint8Array> {
  const chunkSize = 64 * 1024;
  for (let offset = 0; offset < value.byteLength; offset += chunkSize)
    yield value.subarray(offset, Math.min(offset + chunkSize, value.byteLength));
}

async function* utf8Chunks(value: string): AsyncGenerator<Uint8Array> {
  const chunkCharacters = 16 * 1024;
  for (let offset = 0; offset < value.length;) {
    let end = Math.min(offset + chunkCharacters, value.length);
    if (end < value.length) {
      const finalUnit = value.charCodeAt(end - 1);
      if (finalUnit >= 0xd800 && finalUnit <= 0xdbff) end--;
    }
    yield Buffer.from(value.slice(offset, end), "utf8");
    offset = end;
  }
}

function throwIfImportAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted) throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
}

function validateUserIdentifier(value: string): string {
  const target = value.trim();
  if (!/^[A-Za-z0-9_#$]{1,128}$/.test(target)) throw new Error(`invalid DB2 session user identifier ${JSON.stringify(value)}`);
  return target;
}

function serverError(payload: Uint8Array): Error {
  try {
    const diagnostic = parseDdmParameters(payload).get(CodePoint.SRVDGN);
    const message = diagnostic ? decodeServerDiagnostic(diagnostic) : "DB2 server returned SQLERRRM";
    const match = /SQLCODE\s*[:=]\s*(-?\d+)(?:[^\S\r\n]+SQLSTATE\s*[:=]\s*([A-Z0-9]{5}))?/iu.exec(message);
    if (match) return new Db2ServerError(Number(match[1]), match[2], message.replace(match[0], "").trim());
    return new Error(message);
  } catch { return new Error("DB2 server returned SQLERRRM"); }
}

function isConfirmedInterrupt(error: unknown): boolean {
  return error instanceof Db2ServerError && error.sqlCode === -952;
}

function decodeServerDiagnostic(value: Uint8Array): string {
  try { return utf8FatalDecoderShared.decode(value); }
  catch { return decodeCp500(value); }
}

function concat(parts: Uint8Array[]): Uint8Array {
  const length = parts.reduce((sum, part) => sum + part.length, 0);
  const output = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) { output.set(part, offset); offset += part.length; }
  return output;
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false;
  for (let index = 0; index < left.length; index++) if (left[index] !== right[index]) return false;
  return true;
}

function traceDdmCodePoints(packet: Uint8Array): string {
  // Never called on the hot path when tracing is disabled (call sites guard
  // with TRACE_PROCEDURE_CURSOR first).
  try {
    return [...parseDdmParameters(packet.subarray(4)).keys()]
      .map(codePoint => `0x${codePoint.toString(16)}`).join(',');
  } catch { return "unparsed"; }
}

function traceProcedureCursor(message: string): void {
  if (TRACE_PROCEDURE_CURSOR)
    console.error(`[ts-db2 procedure cursor] ${message}`);
}

function maximumProcedureQueryDataPayload(blockSize: number): number {
  // The DRDA block includes a six-byte DSS header and a 4- or 8-byte DDM header.
  return Math.max(0, blockSize - (blockSize > 0x7fff ? 14 : 10));
}

function isCallStatement(sql: string): boolean {
  let position = sql.charCodeAt(0) === 0xfeff ? 1 : 0;
  while (true) {
    while (position < sql.length && /\s/u.test(sql[position]!)) position++;
    if (position + 1 >= sql.length) return false;
    if (sql.startsWith("/*", position)) {
      const end = sql.indexOf("*/", position + 2);
      if (end < 0) return false;
      position = end + 2;
      continue;
    }
    if (sql.startsWith("--", position)) {
      const end = sql.indexOf("\n", position + 2);
      const carriage = sql.indexOf("\r", position + 2);
      const lineEnd = end < 0 ? carriage : carriage < 0 ? end : Math.min(end, carriage);
      if (lineEnd < 0) return false;
      position = lineEnd + 1;
      continue;
    }
    break;
  }
  const first = sql.slice(position, position + 4);
  const after = sql[position + 4];
  return first.toUpperCase() === "CALL" && (after === undefined || /\s/u.test(after) || after === "(");
}

const nullableTypes = new Set([0x03,0x05,0x07,0x0b,0x0d,0x0f,0x17,0x19,0x1b,0x1d,0x1f,0x21,0x23,0x25,0x27,0x29,0x2b,0x31,0x33,0x35,0x37,0x39,0x3b,0x3d,0x3f,0x41,0x43,0xbb,0xbf,0xc1,0xc3,0xc7,0xc9,0xcd,0xcf,0xf5,0xf7,0xf9]);

function decodeRows(fields: FieldDescriptor[], data: Uint8Array, littleEndian: boolean, onWarning?: (warning: Db2Warning) => void): unknown[][] {
  return decodeRowsIncremental(fields, data, littleEndian, onWarning).rows;
}

function decodeRowsIncremental(
  fields: FieldDescriptor[],
  data: Uint8Array,
  littleEndian: boolean,
  onWarning?: (warning: Db2Warning) => void,
): { rows: unknown[][]; consumed: number; ended: boolean } {
  if (fields.length === 0 || data.length === 0) return { rows: [], consumed: 0, ended: false };
  return decodeRowsWithPlan(compileRowDecoderPlan(fields), data, littleEndian, onWarning);
}

function decodeRowsWithPlan(
  plan: RowDecoderPlan,
  data: Uint8Array,
  littleEndian: boolean,
  onWarning?: (warning: Db2Warning) => void,
): { rows: unknown[][]; consumed: number; ended: boolean } {
  if (plan.columnCount === 0 || data.length === 0) return { rows: [], consumed: 0, ended: false };
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const rows: unknown[][] = [];
  const count = plan.columnCount;
  const types = plan.types;
  const psMeta = plan.ps;
  const nullable = plan.nullable;
  const utf8 = utf8DecoderShared;
  let offset = 0;
  // Hoisted offset helpers: one closure pair for the whole call instead of two
  // per field per row.
  const ensure = (length: number): void => { if (offset + length > data.length) throw new Error("truncated DB2 field"); };
  const readLength = (): number => { if (offset + 2 > data.length) throw new Error("truncated DB2 variable field length"); const n = view.getUint16(offset, false); offset += 2; return n; };
  while (offset + 2 <= data.length) {
    const rowStart = offset;
    const sqlca = readRowSqlca(data, rowStart, littleEndian);
    if (!sqlca) return { rows, consumed: rowStart, ended: false };
    if (sqlca.terminal) return { rows, consumed: sqlca.nextOffset, ended: true };
    offset = sqlca.nextOffset;
    const row = new Array<unknown>(count);
    try { for (let index = 0; index < count; index++) {
      const type = types[index]!;
      const ps = psMeta[index]!;
      if (ps < 0) throw new Error("DB2 query descriptor has invalid PS metadata");
      if (nullable[index] === 1) {
        if (offset >= data.length) throw new Error("truncated DB2 null indicator");
        const indicator = data[offset++]!;
        if (indicator === 0xff || indicator === 0x80 || indicator === 0x7f) { row[index] = null; continue; }
      }
      switch (type) {
        case 0x02: case 0x03: ensure(4); row[index] = view.getInt32(offset, littleEndian); offset += 4; break;
        case 0x04: case 0x05: ensure(2); row[index] = view.getInt16(offset, littleEndian); offset += 2; break;
        case 0x16: case 0x17: ensure(8); row[index] = view.getBigInt64(offset, littleEndian); offset += 8; break;
        case 0x06: case 0x07: ensure(1); row[index] = BigInt((data[offset]! << 24) >> 24); offset += 1; break;
        case 0x0a: case 0x0b: ensure(8); row[index] = view.getFloat64(offset, littleEndian); offset += 8; break;
        case 0x0c: case 0x0d: ensure(4); row[index] = view.getFloat32(offset, littleEndian); offset += 4; break;
        case 0x30: case 0x31: case 0x3c: case 0x3d: { ensure(ps); row[index] = utf8.decode(data.subarray(offset, offset + ps)); offset += ps; break; }
        case 0x32: case 0x33: case 0x34: case 0x35: case 0x3e: case 0x3f: case 0x40: case 0x41: { const length = readLength(); ensure(length); row[index] = utf8.decode(data.subarray(offset, offset + length)); offset += length; break; }
        case 0x36: case 0x37: { const length = ps * 2; ensure(length); row[index] = decodeUtf16Be(data.subarray(offset, offset + length)); offset += length; break; }
        case 0x38: case 0x39: case 0x3a: case 0x3b: { const length = readLength() * 2; ensure(length); row[index] = decodeUtf16Be(data.subarray(offset, offset + length)); offset += length; break; }
        case 0x20: case 0x21: case 0x22: case 0x23: case 0x24: case 0x25: { ensure(ps); row[index] = utf8.decode(data.subarray(offset, offset + ps)).trimEnd(); offset += ps; break; }
        case 0x0e: case 0x0f: { const length = Math.floor(((ps >> 8) + 2) / 2); ensure(length); row[index] = decodePackedDecimal(data.subarray(offset, offset + length), ps & 0xff); offset += length; break; }
        case 0xba: case 0xbb: { const length = (ps >> 8) === 16 || (ps & 0xff) === 16 ? 16 : 8; ensure(length); row[index] = decodeDecfloat(data.subarray(offset, offset + length)); offset += length; break; }
        case 0xbe: case 0xbf: { ensure(ps); row[index] = ps > 0 && data[offset + ps - 1] !== 0; offset += ps; break; }
        case 0x26: case 0x27: case 0xc0: case 0xc1: { const length = ps & 0x7fff; ensure(length); row[index] = data.subarray(offset, offset + length); offset += length; break; }
        case 0x28: case 0x29: case 0x2a: case 0x2b: case 0xc2: case 0xc3: { const length = readLength(); ensure(length); row[index] = data.subarray(offset, offset + length); offset += length; break; }
        case 0x10: case 0x11: case 0x18: case 0x19: case 0x1a: case 0x1b: case 0x1c: case 0x1d:
        case 0xcc: // Non-nullable DBCLOB, observed in direct DBCLOB expression results.
        case 0xcd: case 0xc6: case 0xc7: case 0xc8: case 0xc9: case 0xce: case 0xcf:
        case 0xf4: case 0xf5: case 0xf6: case 0xf7: case 0xf8: case 0xf9:
          {
            const locatorLength = (ps & 0x7fff) || 4;
            ensure(locatorLength);
            const locatorStart = offset;
            const ps0 = ps >> 8;
            const ps1 = ps & 0xff;
            if ((ps0 & 0x80) !== 0 && ps1 === 9 && locatorLength === 9 && data[locatorStart] === 0x03) {
              const locatorChars = view.getBigUint64(locatorStart + 1, false);
              // Progressive DBCLOB advertises UTF-16 code units in its locator while the
              // GETNXTCHK chunks are UTF-16BE bytes, so the byte budget is twice the
              // locator value. CLOB/BLOB locators already advertise bytes.
              const length = isDbClobType(type) ? locatorChars * 2n : locatorChars;
              ensure(9 + 8);
              const reference = view.getBigUint64(locatorStart + 9, false);
              row[index] = { kind: "progressive-lob", length, reference } satisfies ProgressiveLobReference;
              offset = locatorStart + 17;
            } else if ((ps0 & 0x80) !== 0 && ps1 === 9 && locatorLength === 9 && data[locatorStart] === 0x01) {
              let byteLength = view.getBigUint64(locatorStart + 1, false);
              if (isDbClobType(type)) byteLength *= 2n;
              if (byteLength > BigInt(DSS_LIMITS.maxLobMaterializationBytes))
                throw new Db2ResourceLimitError("DB2 inline LOB exceeds the " + DSS_LIMITS.maxLobMaterializationBytes + "-byte materialization limit");
              if (byteLength > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error("DB2 inline LOB length exceeds JavaScript's safe integer range");
              offset = locatorStart + 9;
              const length = Number(byteLength);
              ensure(length);
              const value = data.subarray(offset, offset + length);
              offset += length;
              if (isDbClobType(type)) row[index] = decodeUtf16Be(value);
              else if (isClobType(type)) row[index] = utf8.decode(value);
              else row[index] = value;
            } else { row[index] = data.subarray(locatorStart, locatorStart + locatorLength); offset = locatorStart + locatorLength; }
            break;
          }
        default: throw new Db2UnsupportedFeatureError(`unsupported DB2 query field type 0x${type.toString(16).padStart(2, "0")}`);
      }
    } } catch (error) {
      if (error instanceof Error && error.message.startsWith("truncated DB2")) return { rows, consumed: rowStart, ended: false };
      throw error;
    }
    rows.push(row);
    if (sqlca.warning) onWarning?.(sqlca.warning);
  }
  return { rows, consumed: offset, ended: false };
}

function readRowSqlca(
  data: Uint8Array,
  rowStart: number,
  littleEndian: boolean,
): { nextOffset: number; warning?: Db2Warning; terminal?: boolean } | undefined {
  let offset = rowStart;
  const sqlcaIndicator = data[offset++];
  if (sqlcaIndicator === 0xff) {
    if (offset >= data.length || data[offset] !== 0) return undefined;
    return { nextOffset: offset + 1 };
  }
  if (sqlcaIndicator !== 0) return undefined;
  if (data.length - offset < 18) return undefined;

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const sqlCode = view.getInt32(offset, littleEndian);
  const sqlState = utf8DecoderShared.decode(data.subarray(offset + 4, offset + 9));
  offset += 17; // SQLCODE, SQLSTATE, SQLERRPROC; next byte is SQLCAXGRP indicator.
  const xGroupIndicator = data[offset++];
  let warningFlags = "";
  const messages: string[] = [];
  if (xGroupIndicator === 0) {
    if (data.length - offset < 35) return undefined;
    offset += 24; // SQLERRD(1..6).
    warningFlags = utf8DecoderShared.decode(data.subarray(offset, offset + 11));
    offset += 11;
    const strings: string[] = [];
    for (let index = 0; index < 3; index++) {
      if (data.length - offset < 2) return undefined;
      const length = view.getUint16(offset, false);
      offset += 2;
      if (data.length - offset < length) return undefined;
      strings.push(utf8DecoderShared.decode(data.subarray(offset, offset + length)));
      offset += length;
    }
    if (strings[1]?.trim()) messages.push(strings[1]);
    if (strings[2]?.trim()) messages.push(strings[2]);
  } else if (xGroupIndicator !== 0xff) {
    throw new Error(`invalid SQLCAXGRP indicator 0x${xGroupIndicator?.toString(16) ?? "??"} in query row`);
  }

  if (offset >= data.length) return undefined;
  const diagnosticIndicator = data[offset++];
  if (diagnosticIndicator !== 0xff) throw new Db2UnsupportedFeatureError("SQLDIAGGRP in a query row is not supported by this driver");
  if (sqlCode < 0) throw sqlError(sqlCode, sqlState, "error in query row");
  if (sqlCode === 100 && offset === data.length - 1 && data[offset] === 0xff)
    return { nextOffset: offset + 1, terminal: true };
  if (offset >= data.length || data[offset] !== 0) return undefined;
  offset++;

  const hasWarning = (sqlCode > 0 && sqlCode !== 100)
    || sqlState.startsWith("01")
    || (warningFlags.length > 0 && warningFlags[0] === "W");
  const warning = hasWarning
    ? {
        sqlCode,
        sqlState,
        message: messages.length > 0 ? messages.join("\n") : `DB2 returned a row warning (SQLCODE=${sqlCode}, SQLSTATE=${sqlState}).`,
        warningFlags,
      }
    : undefined;
  return warning ? { nextOffset: offset, warning } : { nextOffset: offset };
}

/** Redacted structural description for diagnosing an incomplete SQLCA prefix.
 * It reports integer fields, offsets, and declared string lengths only. */
function describeSqlcaPrefix(data: Uint8Array, littleEndian: boolean): string {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  if (data.length === 0) return "empty";
  if (data[0] === 0xff) return data.length < 2 ? "indicator=ff truncated-at=1 need=1" : `indicator=ff sqlDiag=${data[1]}`;
  if (data[0] !== 0) return `indicator=0x${data[0]!.toString(16)} invalid-at=0`;
  if (data.length < 19) return `indicator=00 truncated-at=${data.length} need=19`;
  const sqlCode = view.getInt32(1, littleEndian);
  const xGroup = data[18]!;
  let offset = 19;
  const lengths: number[] = [];
  if (xGroup === 0) {
    if (data.length < offset + 35) return `indicator=00 sqlcode=${sqlCode} xgroup=00 truncated-at=${data.length} need=${offset + 35}`;
    offset += 35;
    for (let index = 0; index < 3; index++) {
      if (data.length < offset + 2) return `indicator=00 sqlcode=${sqlCode} xgroup=00 string-lengths=${lengths.join(',')} truncated-at=${data.length} need=${offset + 2}`;
      const length = view.getUint16(offset, false); offset += 2; lengths.push(length);
      if (data.length < offset + length) return `indicator=00 sqlcode=${sqlCode} xgroup=00 string-lengths=${lengths.join(',')} truncated-at=${data.length} need=${offset + length}`;
      offset += length;
    }
  } else if (xGroup !== 0xff) {
    return `indicator=00 sqlcode=${sqlCode} xgroup=0x${xGroup.toString(16)} invalid-at=18`;
  }
  if (data.length <= offset) return `indicator=00 sqlcode=${sqlCode} xgroup=0x${xGroup.toString(16)} string-lengths=${lengths.join(',')} truncated-at=${data.length} need=${offset + 1}`;
  const diagnostic = data[offset]!;
  if (diagnostic !== 0xff) return `indicator=00 sqlcode=${sqlCode} xgroup=0x${xGroup.toString(16)} string-lengths=${lengths.join(',')} diagnostic=0x${diagnostic.toString(16)} unsupported-at=${offset}`;
  if (data.length <= offset + 1) return `indicator=00 sqlcode=${sqlCode} xgroup=0x${xGroup.toString(16)} string-lengths=${lengths.join(',')} diagnostic=ff truncated-at=${data.length} need=${offset + 2}`;
  return `indicator=00 sqlcode=${sqlCode} xgroup=0x${xGroup.toString(16)} string-lengths=${lengths.join(',')} diagnostic=ff row-marker=0x${data[offset + 1]!.toString(16)} row-marker-at=${offset + 1}`;
}

function isFixedScalarProcedureCursor(fields: readonly FieldDescriptor[]): boolean {
  if (fields.length === 0) return false;
  return fields.every(field => [0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x16, 0x17, 0x0, 0xbe, 0xbf].includes(field.type));
}

function stitchExtdta(
  fields: readonly FieldDescriptor[],
  rows: unknown[][],
  values: readonly Uint8Array[],
  externalValuesHaveNullIndicator = false,
): number {
  const lobColumns = fields.map((field, index) => isLobType(field.type) ? index : -1).filter((index) => index >= 0);
  let valueIndex = 0;
  for (const row of rows) {
    for (const column of lobColumns) {
      const field = fields[column]!;
      const externalLength = getExternalLobLength(field, row[column]);
      if (externalLength === undefined) continue;
      if (externalLength === 0n) {
        row[column] = isClobType(field.type) ? "" : new Uint8Array();
        continue;
      }
      if (valueIndex >= values.length) continue;
      const raw = values[valueIndex++]!;
      // SQLDTARD output EXTDTA always carries a nullable value indicator.
      // Query and procedure-cursor EXTDTA carry it only for a nullable LOB
      // descriptor; a NOT NULL LOB is sent verbatim, so its first byte may
      // legitimately be 0x00 and must not be stripped.
      const hasLeadingNullIndicator = externalValuesHaveNullIndicator || isNullableLobType(field.type);
      const data = hasLeadingNullIndicator && raw.length > 0 && raw[0] === 0 ? raw.subarray(1) : raw;
      row[column] = decodeExternalLobValue(field, data);
    }
  }
  return valueIndex;
}

function applyOutputParameterValues(
  fields: FieldDescriptor[],
  values: Uint8Array,
  directions: readonly Db2ParameterDirection[],
  outputParameters: unknown[],
  extdta: readonly Uint8Array[],
  littleEndian: boolean,
  onWarning?: (warning: Db2Warning) => void,
): void {
  if (fields.length === 0 || values.length === 0) {
    if (extdta.length > 0) throw new Error("DB2 returned output-parameter EXTDTA without decodable SQLDTARD values");
    ensureOutputParameterDataReturned(directions, false);
    return;
  }
  const rowData = values[0] === 0xff ? values : concat([Uint8Array.of(0xff, 0), values]);
  const rows = decodeRows(fields, rowData, littleEndian, onWarning);
  const extdtaUsed = stitchExtdta(fields, rows, extdta, true);
  if (extdtaUsed !== extdta.length || hasUnresolvedExternalLocator(fields, rows))
    throw new Error("DB2 output-parameter EXTDTA values do not match SQLDTARD LOB descriptors");
  const decoded = rows[0];
  if (!decoded) {
    ensureOutputParameterDataReturned(directions, false);
    return;
  }
  // A short SQLDTARD row would leave a requested output position at its placeholder
  // value without any error. Go skips those positions silently; reject the reply
  // instead of reporting a missing output as null.
  let lastRequested = -1;
  for (let index = 0; index < directions.length; index++)
    if (directions[index] === "output" || directions[index] === "inputOutput") lastRequested = index;
  if (lastRequested >= decoded.length)
    throw new Error(`DB2 returned output-parameter SQLDTARD values for ${decoded.length} parameters, but position ${lastRequested} was requested as an output`);
  for (let index = 0; index < outputParameters.length && index < decoded.length; index++)
    if (directions[index] === "output" || directions[index] === "inputOutput") outputParameters[index] = decoded[index];
}

function ensureOutputParameterDataReturned(
  directions: readonly Db2ParameterDirection[],
  dataReturned: boolean,
): void {
  if (dataReturned) return;
  for (let index = directions.length - 1; index >= 0; index--) {
    if (directions[index] === "input") continue;
    throw new Error(`DB2 returned no SQLDTARD values for requested output parameter at position ${index}`);
  }
}

export function hasUnresolvedExternalLocator(fields: readonly FieldDescriptor[], rows: readonly unknown[][]): boolean {
  for (const row of rows) {
    for (let column = 0; column < fields.length && column < row.length; column++) {
      const length = getExternalLobLength(fields[column]!, row[column]);
      if (length !== undefined && length !== 0n) return true;
      const progressive = getProgressiveLobReference(row[column]);
      if (progressive) return true;
    }
  }
  return false;
}

export function stitchOutputExtdtaForTest(
  fields: readonly FieldDescriptor[],
  rows: unknown[][],
  values: readonly Uint8Array[],
  externalValuesHaveNullIndicator = false,
): { used: number; unresolved: boolean } {
  const used = stitchExtdta(fields, rows, values, externalValuesHaveNullIndicator);
  return { used, unresolved: hasUnresolvedExternalLocator(fields, rows) };
}

export function applyOutputParameterReplyForTest(
  payload: Uint8Array,
  directions: readonly Db2ParameterDirection[],
): unknown[] {
  const outputParameters: unknown[] = directions.map(() => null);
  const { fields, values } = parseSqlDtaReply(payload);
  applyOutputParameterValues(fields, values, directions, outputParameters, [], false);
  return outputParameters;
}

export function ensureOutputParameterDataReturnedForTest(
  directions: readonly Db2ParameterDirection[],
  dataReturned: boolean,
): void {
  ensureOutputParameterDataReturned(directions, dataReturned);
}

function getExternalLobLength(field: FieldDescriptor, value: unknown): bigint | undefined {
  if (!isLobType(field.type) || field.ps.length < 2 || (field.ps[0]! & 0x80) === 0 || field.ps[1] !== 9
    || !(value instanceof Uint8Array) || value.length !== 9 || value[0] !== 0x02) return undefined;
  return new DataView(value.buffer, value.byteOffset + 1, 8).getBigUint64(0, false);
}

function getProgressiveLobReference(value: unknown): ProgressiveLobReference | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  const candidate = value as Partial<ProgressiveLobReference>;
  if (candidate.kind !== "progressive-lob" || typeof candidate.length !== "bigint" || typeof candidate.reference !== "bigint")
    return undefined;
  return candidate as ProgressiveLobReference;
}

function emptyExternalLobValue(field: FieldDescriptor): string | Uint8Array {
  return isClobType(field.type) ? "" : new Uint8Array();
}

function decodeExternalLobValue(field: FieldDescriptor, data: Uint8Array): string | Uint8Array {
  if (data.byteLength > DSS_LIMITS.maxLobMaterializationBytes)
    throw new Db2ResourceLimitError("DB2 LOB exceeds the " + DSS_LIMITS.maxLobMaterializationBytes + "-byte materialization limit");
  if (isDbClobType(field.type)) return decodeUtf16Be(data);
  if (isClobType(field.type)) return utf8DecoderShared.decode(data);
  return data;
}

function estimateMaterializedRowBytes(row: readonly unknown[]): number {
  let total = 0;
  for (const value of row) {
    const bytes = value === null || value === undefined ? 1
      : typeof value === "string" ? Buffer.byteLength(value, "utf8")
      : value instanceof Uint8Array ? value.byteLength
      : typeof value === "number" || typeof value === "bigint" ? 8
      : typeof value === "boolean" ? 1
      : value instanceof Date ? 8
      : 16;
    if (bytes > DSS_LIMITS.maxMaterializedResultBytes - total)
      throw new Db2ResourceLimitError("DB2 materialized result exceeds the " + DSS_LIMITS.maxMaterializedResultBytes + "-byte limit");
    total += bytes;
  }
  return total;
}

function isLobType(type: number): boolean {
  return [0x10,0x11,0x18,0x19,0x1a,0x1b,0x1c,0x1d,0xc6,0xc7,0xc8,0xc9,0xcc,0xcd,0xce,0xcf,0xf4,0xf5,0xf6,0xf7,0xf8,0xf9].includes(type);
}

function isClobType(type: number): boolean {
  return [0x1a,0x1b,0x1c,0x1d,0xc6,0xc7,0xcc,0xcd,0xce,0xcf,0xf6,0xf7,0xf8,0xf9].includes(type);
}

/** Nullable LOB descriptors use the odd member of each type pair (e.g. 0xc9 BLOB, 0xcd DBCLOB). */
function isNullableLobType(type: number): boolean {
  return isLobType(type) && (type & 1) === 1;
}


function isDbClobType(type: number): boolean {
  return [0x1c,0x1d,0xcc,0xcd,0xf8,0xf9].includes(type);
}

function decodeUtf16Be(data: Uint8Array): string {
  const aligned = data.length % 2 === 0 ? data : data.subarray(0, data.length - 1);
  if (aligned.length === 0) return "";
  // Direct UTF-16BE decode without slice + byte-swap copy when supported.
  if (utf16BeDecoderShared) {
    try { return utf16BeDecoderShared.decode(aligned); }
    catch { /* fall through to byte-swap path */ }
  }
  const copy = aligned.slice();
  for (let i = 0; i < copy.length; i += 2) { const a = copy[i]!; copy[i] = copy[i + 1]!; copy[i + 1] = a; }
  return utf16LeDecoderShared.decode(copy);
}

function decodePackedDecimal(data: Uint8Array, scale: number): string {
  if (!data.length) return "0";
  let digits = "";
  for (let i = 0; i < data.length - 1; i++) digits += `${data[i]! >> 4}${data[i]! & 0x0f}`;
  const last = data.at(-1)!;
  digits += String(last >> 4);
  const negative = (last & 0x0f) === 0x0d || (last & 0x0f) === 0x0b;
  digits = digits.replace(/^0+(?=\d)/u, "");
  if (scale > 0) digits = digits.length <= scale ? `0.${"0".repeat(scale - digits.length)}${digits}` : `${digits.slice(0, -scale)}.${digits.slice(-scale)}`;
  return `${negative ? "-" : ""}${digits}`;
}
