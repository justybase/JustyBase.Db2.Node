import { EventEmitter } from "node:events";
import { applyParam, newConfig, normalizeCurrentSchema, parseDsn, type Config } from "./config.js";
import { Db2Error, Db2ServerError } from "./errors.js";
import { DSS_LIMITS } from "./network/dss.js";
import { Db2Connection, Db2ProcedureReader, isDb2LobStream, type Db2Parameter, type QueryOptions, type QueryStreamRow } from "./network/session.js";
import type { ColumnDescription } from "./network/ddm.js";
import { baseSqlType, sqlTypeName, SqlType } from "./types/sqltype.js";

/** Metadata names exposed by node-ibm_db's ODBCResult.getColumnMetadata() (verified against node-ibm_db 4.0.1 on Db2 LUW 12.1.5). */
export type Db2CompatColumnMetadata = ColumnDescription & {
  /** 1-based position of the column in the result. */
  readonly index: number;
  readonly SQL_DESC_NAME: string;
  readonly SQL_DESC_TYPE_NAME: string;
  /** ODBC SQL type code (for example 4 for INTEGER, 12 for VARCHAR). */
  readonly SQL_DESC_CONSIZE_TYPE: number;
  readonly SQL_DESC_DISPLAY_SIZE: number;
  readonly SQL_DESC_PRECISION: number;
  readonly SQL_DESC_SCALE: number;
  readonly SQL_DESC_LENGTH: number;
};
/** Opt-in value shapes for code migrating from node-ibm_db. Defaults keep exact values:
 * DECIMAL as a string and BIGINT as a bigint. node-ibm_db returns a number and a string. */
export type Db2CompatValueOptions = {
  readonly decimal?: "string" | "number";
  readonly bigint?: "bigint" | "string";
};
export type Db2CompatExecutionResult = {
  columns: ColumnDescription[];
  rows: Record<string, unknown>[];
  /** Db2 command record count; -1 for a row-returning query. */
  recordsAffected: number;
};

/** Structural configuration shape used by JustyBase's DatabaseConnection contract. */
export type Db2DatabaseConnectionConfig = {
  host: string;
  port?: number;
  database: string;
  user: string;
  password?: string;
  options?: Readonly<Record<string, string | number | boolean>>;
};

type WarningEvent = { message: string; sqlCode: number; sqlState: string; warningFlags: string };

/** Async cursor facade for code migrating from ibm_db queryResult(). */
export class Db2CompatResult {
  private closed = false;
  private procedureFinished = false;
  private currentRow: Record<string, unknown> | undefined;
  private procedureRows: AsyncGenerator<unknown[]> | undefined;
  private procedureOutputs: readonly unknown[] | undefined;

  constructor(
    private readonly rows: AsyncGenerator<QueryStreamRow>,
    private readonly initialColumns: readonly ColumnDescription[],
    private readonly procedure?: Db2ProcedureReader,
    private readonly onProcedureFinished?: (rowsAffected: number) => void,
    private readonly valueOptions?: Db2CompatValueOptions,
  ) {
    if (procedure) this.procedureRows = procedure.rows();
  }

  private get columns(): readonly ColumnDescription[] {
    return this.procedure?.columns ?? this.initialColumns;
  }

  /** Object-row keys for the current column set. Duplicate or empty labels are
   * suffixed (`ID`, `ID__1`, ...) so no value is silently overwritten; metadata
   * keeps the original server labels. */
  private get columnKeys(): string[] {
    return suffixedColumnKeys(this.columns);
  }

  getColumnMetadata(): Db2CompatColumnMetadata[] {
    return this.columns.map((column, index) => {
      const typeName = ibmTypeName(column);
      const displaySize = ibmDisplaySize(typeName, column);
      return {
        ...column,
        index: index + 1,
        SQL_DESC_NAME: column.name,
        SQL_DESC_TYPE_NAME: typeName,
        SQL_DESC_CONSIZE_TYPE: odbcTypeCode(typeName),
        SQL_DESC_DISPLAY_SIZE: displaySize,
        // node-ibm_db reports 0 here for every probed column type; this facade reports the declared precision.
        SQL_DESC_PRECISION: column.precision,
        SQL_DESC_SCALE: column.scale,
        // Binary and LOB types report the declared length; other types report the display size.
        SQL_DESC_LENGTH: LENGTH_IS_DECLARED.has(typeName) ? safeLength(column.length) : displaySize,
      };
    });
  }

  getColumnNames(): string[] { return this.columnKeys; }

  /** Metadata is local after queryResult() resolves, so this IBM spelling can stay synchronous. */
  getColumnMetadataSync(): Db2CompatColumnMetadata[] { return this.getColumnMetadata(); }
  getColumnNamesSync(): string[] { return this.getColumnNames(); }

  async fetch(): Promise<Record<string, unknown> | null> {
    return withCompatErrors(() => this.fetchRow());
  }

  private async fetchRow(): Promise<Record<string, unknown> | null> {
    if (this.closed) return null;
    if (this.procedure) {
      const result = await this.procedureRows!.next();
      if (result.done) {
        this.currentRow = undefined;
        return null;
      }
      this.currentRow = valuesObject(this.columns, await materializeRowValues(result.value), this.valueOptions);
      return this.currentRow;
    }

    const result = await this.rows.next();
    if (result.done) {
      this.closed = true;
      this.currentRow = undefined;
      return null;
    }
    this.currentRow = valuesObject(result.value.columns, await materializeRowValues(result.value.values), this.valueOptions);
    return this.currentRow;
  }

  /** JustyBase-style reader operation; unlike ibm_db, this is asynchronous. */
  async read(): Promise<boolean> { return (await this.fetch()) !== null; }
  get fieldCount(): number { return this.columns.length; }
  getName(index: number): string { return this.columnKeys[index] ?? ""; }
  getTypeName(index: number): string { return this.columns[index] ? sqlTypeName(baseSqlType(this.columns[index]!.sqlType)) : ""; }
  getValue(index: number): unknown {
    const key = this.columnKeys[index];
    return key === undefined ? undefined : this.currentRow?.[key];
  }
  /** Terminal OUT/INOUT values of a procedure cursor; available after finish(). */
  get outputParameters(): readonly unknown[] {
    if (!this.procedure) throw new Error("DB2 compatibility cursor has no procedure outputs");
    if (this.procedureOutputs === undefined) throw new Error("DB2 procedure output parameters are available only after finish()");
    return this.procedureOutputs;
  }

  async nextResult(): Promise<boolean> {
    if (this.closed || !this.procedure) return false;
    // Returning the generator drains the active cursor if the caller did not read it fully.
    await this.procedureRows?.return(undefined);
    this.currentRow = undefined;
    const hasNext = await this.procedure.nextResultSet();
    this.procedureRows = hasNext ? this.procedure.rows() : undefined;
    if (!hasNext) {
      await this.finishProcedure();
      this.closed = true;
    }
    return hasNext;
  }

  async fetchAll(): Promise<Record<string, unknown>[]> {
    const result: Record<string, unknown>[] = [];
    for (;;) {
      const row = await this.fetch();
      if (row === null) return result;
      result.push(row);
    }
  }

  /** Ends the cursor; an early close drains remaining Db2 cursor state before reuse. */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.currentRow = undefined;
    if (this.procedure) {
      await this.procedureRows?.return(undefined);
      await this.finishProcedure();
      return;
    }
    await this.rows.return(undefined);
  }

  private async finishProcedure(): Promise<void> {
    if (this.procedureFinished || !this.procedure) return;
    const result = await this.procedure.finish();
    this.procedureFinished = true;
    this.procedureOutputs = result.outputParameters;
    this.onProcedureFinished?.(result.rowsAffected);
  }
}

/** Promise based open/query facade for consumers moving from node-ibm_db.
 * It intentionally does not emulate synchronous ibm_db methods.
 */
export class Db2CompatDatabase {
  private constructor(readonly connection: Db2Connection, readonly valueOptions?: Db2CompatValueOptions) {}

  static async open(input: string | Config, valueOptions?: Db2CompatValueOptions): Promise<Db2CompatDatabase> {
    return new Db2CompatDatabase(await Db2Connection.connect(input), valueOptions);
  }

  /** Like ibm_db Database.query(): materializes rows as objects keyed by column name. */
  async query(sql: string, params?: readonly unknown[], options: QueryOptions = {}): Promise<Record<string, unknown>[]> {
    return withCompatErrors(async () => {
      const result = await this.connection.query(sql, params, options);
      return result.rows.map((values) => valuesObject(result.columns, values, this.valueOptions));
    });
  }

  /** Async counterpart of JustyBase's executeSql result shape. */
  async executeSql(sql: string, params?: readonly unknown[], options: QueryOptions = {}): Promise<Db2CompatExecutionResult> {
    return withCompatErrors(async () => {
      if (isResultQuery(sql)) {
        const result = await this.connection.query(sql, params, options);
        return {
          columns: result.columns,
          rows: result.rows.map((values) => valuesObject(result.columns, values, this.valueOptions)),
          recordsAffected: -1,
        };
      }
      return {
        columns: [],
        rows: [],
        recordsAffected: await this.connection.execute(sql, params, options),
      };
    });
  }

  /** Command factory shaped like JustyBase's DatabaseConnection.createCommand(). */
  createCommand(sql: string, params?: readonly unknown[], options: QueryOptions = {}): Db2CompatCommand {
    return new Db2CompatCommand(this, sql, params, options);
  }

  /** Like ibm_db Database.queryResult(): returns metadata and a fetchable async cursor. */
  async queryResult(sql: string, params?: readonly unknown[], options: QueryOptions = {}): Promise<Db2CompatResult> {
    return withCompatErrors(async () => {
      const statement = await this.connection.prepare(sql, options);
      const columns = statement.outputColumns;
      statement.close();
      const rows = params === undefined
        ? this.connection.queryStream(sql, options)
        : this.connection.queryStreamWithParams(sql, params, options);
      return new Db2CompatResult(rows, columns, undefined, undefined, this.valueOptions);
    });
  }

  async procedureResult(
    sql: string,
    params: readonly unknown[] = [],
    options: QueryOptions = {},
    onFinished?: (rowsAffected: number) => void,
  ): Promise<Db2CompatResult> {
    return withCompatErrors(async () => {
      const parameters: Db2Parameter[] = params.map((value) => ({ value, direction: "input" }));
      const procedure = await this.connection.executeProcedureStream(sql, parameters, options);
      async function* emptyRows(): AsyncGenerator<QueryStreamRow> {}
      return new Db2CompatResult(emptyRows(), procedure.columns, procedure, onFinished, this.valueOptions);
    });
  }

  async close(): Promise<void> { await this.connection.close(); }
}

/** Promise-based command adapter for consumers with a DatabaseCommand-style API. */
export class Db2CompatCommand {
  commandTimeout = 0;
  _recordsAffected = -1;
  private readonly cancellation = new AbortController();
  private activeReader: Db2CompatResult | undefined;
  private cancelled = false;

  constructor(
    private readonly database: Db2CompatDatabase,
    private readonly sql: string,
    private readonly parameters?: readonly unknown[],
    private readonly options: QueryOptions = {},
  ) {}

  async executeReader(): Promise<Db2CompatResult> {
    if (this.cancelled) throw this.cancellation.signal.reason;
    const signal = this.options.signal === undefined
      ? this.cancellation.signal
      : AbortSignal.any([this.options.signal, this.cancellation.signal]);
    const queryOptions: QueryOptions = {
      ...this.options,
      signal,
      ...(this.commandTimeout > 0 ? { timeoutMs: this.commandTimeout * 1_000 } : {}),
    };
    if (isCallStatement(this.sql)) {
      this._recordsAffected = -1;
      this.activeReader = await this.database.procedureResult(
        this.sql,
        this.parameters,
        queryOptions,
        (rowsAffected) => { this._recordsAffected = rowsAffected; },
      );
      return this.activeReader;
    }
    if (isResultQuery(this.sql)) {
      this._recordsAffected = -1;
      this.activeReader = await this.database.queryResult(this.sql, this.parameters, queryOptions);
      return this.activeReader;
    }

    this._recordsAffected = await withCompatErrors(() =>
      this.database.connection.execute(this.sql, this.parameters, queryOptions));
    async function* noRows(): AsyncGenerator<QueryStreamRow> {}
    this.activeReader = new Db2CompatResult(noRows(), []);
    return this.activeReader;
  }

  async execute(): Promise<void> {
    const reader = await this.executeReader();
    try {
      do { while (await reader.read()) { /* consume each result set */ } }
      while (await reader.nextResult());
    } finally { await reader.close(); }
  }

  async cancel(): Promise<void> {
    if (!this.cancelled) {
      this.cancelled = true;
      this.cancellation.abort(new DOMException("Query cancelled.", "AbortError"));
    }
    await this.activeReader?.close().catch(() => {});
  }
}

/**
 * DatabaseConnection-shaped adapter for JustyBase and similar async consumers.
 * `commandTimeout` follows that contract's seconds unit; the native API uses ms.
 */
export class Db2DatabaseConnection extends EventEmitter {
  public _connected = false;
  private database: Db2CompatDatabase | undefined;
  private removeWarningListener: (() => void) | undefined;
  private pendingConnect: Promise<void> | undefined;
  private connectAttempt = 0;

  constructor(public readonly config: Db2DatabaseConnectionConfig) { super(); }

  async connect(): Promise<void> {
    if (this._connected) return;
    if (this.pendingConnect) { await this.pendingConnect; return; }
    const attempt = ++this.connectAttempt;
    const task = (async () => {
      // toNativeConfig throws before any socket when options are invalid.
      const nativeConfig = toNativeConfig(this.config);
      const database = await Db2CompatDatabase.open(nativeConfig);
      if (attempt !== this.connectAttempt || this.database) {
        // A close() (or a second connect path) won the race; do not leak the session.
        await database.close().catch(() => {});
        if (attempt !== this.connectAttempt) throw new Error("Db2 connection was closed during connect.");
        return;
      }
      this.database = database;
      this.removeWarningListener = database.connection.addWarningListener((warning) => {
        const event: WarningEvent = { ...warning };
        this.emit("notice", event);
      });
      this._connected = true;
    })();
    this.pendingConnect = task;
    try { await task; }
    finally { if (this.pendingConnect === task) this.pendingConnect = undefined; }
  }

  async close(): Promise<void> {
    this.connectAttempt++;
    const database = this.database;
    this.database = undefined;
    this.removeWarningListener?.();
    this.removeWarningListener = undefined;
    if (!database) {
      this._connected = false;
      return;
    }
    try { await database.close(); }
    finally {
      this._connected = false;
      this.emit("close");
    }
  }

  createCommand(sql: string, params?: readonly unknown[], options: QueryOptions = {}): Db2CompatCommand {
    if (!this.database) throw new Error("Db2 connection is not open.");
    return this.database.createCommand(sql, params, options);
  }
}

/** Opens a native TypeScript DRDA connection with the Promise shape of ibm_db.open. */
export async function open(input: string | Config, valueOptions?: Db2CompatValueOptions): Promise<Db2CompatDatabase> {
  return Db2CompatDatabase.open(typeof input === "string" ? parseDsn(input) : input, valueOptions);
}

function toNativeConfig(input: Db2DatabaseConnectionConfig): Config {
  const config = newConfig();
  config.host = input.host;
  config.port = input.port ?? config.port;
  config.database = input.database;
  config.user = input.user;
  config.password = input.password ?? "";

  for (const [key, value] of Object.entries(input.options ?? {})) {
    if (value === undefined || value === null || value === "") continue;
    const normalized = key.toLowerCase();
    if ((normalized === "connecttimeout" || normalized === "connect_timeout") && typeof value === "number") {
      config.timeoutMs = Math.max(0, Math.round(value * 1_000));
    } else if (normalized === "clientcodepage" || normalized === "client_codepage") {
      // The DRDA implementation uses protocol-defined encodings; this CLI option has no equivalent.
      continue;
    } else {
      applyParam(config, key, String(value));
    }
  }
  // Fail fast on an invalid schema before any socket is opened.
  config.currentSchema = normalizeCurrentSchema(config.currentSchema);
  return config;
}

/** Materializes each one-shot progressive LOB stream so adapter consumers receive
 * plain values (Buffer for binary LOBs, string for text LOBs) like ibm_db.
 * Locators are consumed strictly in column order: the driver allows only one
 * active progressive LOB read at a time. */
async function materializeRowValues(values: readonly unknown[]): Promise<unknown[]> {
  const result: unknown[] = [];
  for (const value of values) result.push(await materializeLobValue(value));
  return result;
}

async function materializeLobValue(value: unknown): Promise<unknown> {
  if (!isDb2LobStream(value)) return value;
  if (value.length > BigInt(DSS_LIMITS.maxLobMaterializationBytes))
    throw new Error("DB2 progressive LOB exceeds the supported materialized-value size");
  if (value.kind === "binary-lob") {
    const chunks: Uint8Array[] = [];
    for await (const chunk of value.stream()) chunks.push(chunk);
    return Buffer.concat(chunks);
  }
  let text = "";
  for await (const chunk of value.stream()) text += chunk;
  return text;
}

function valuesObject(columns: readonly ColumnDescription[], values: readonly unknown[], options?: Db2CompatValueOptions): Record<string, unknown> {
  const result: Record<string, unknown> = {};
  const keys = suffixedColumnKeys(columns);
  for (let index = 0; index < columns.length; index++) {
    result[keys[index]!] = ibmCompatValue(values[index], columns[index]!, options);
  }
  return result;
}

/** node-ibm_db returns binary values as Buffer; inline BLOB values arrive as a plain Uint8Array. */
function ibmCompatValue(value: unknown, column: ColumnDescription, options?: Db2CompatValueOptions): unknown {
  if (value instanceof Uint8Array && !Buffer.isBuffer(value)) {
    return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  }
  const base = baseSqlType(column.sqlType);
  if (options?.decimal === "number" && typeof value === "string" && (base === SqlType.Decimal || base === SqlType.Numeric)) {
    return Number(value);
  }
  if (options?.bigint === "string" && typeof value === "bigint" && base === SqlType.BigInt) {
    return value.toString();
  }
  return value;
}

/** Object-row keys with duplicate/empty labels disambiguated (`ID`, `ID__1`, ...).
 * The first occurrence keeps its label; metadata accessors keep server labels. */
function suffixedColumnKeys(columns: readonly ColumnDescription[]): string[] {
  const seen = new Map<string, number>();
  return columns.map((column, index) => {
    const base = column.name || `COLUMN${index + 1}`;
    const count = seen.get(base) ?? 0;
    seen.set(base, count + 1);
    return count === 0 ? base : `${base}__${count}`;
  });
}

/** Rejects with the driver's typed error. For migrating consumers it also exposes the
 * IBM-spelled SQLCODE/SQLSTATE aliases and node-ibm_db's lower-case sqlcode/sqlstate
 * properties, and it rewrites a server error message to carry the SQLnnnnN token that
 * node-ibm_db messages contain. */
async function withCompatErrors<T>(operation: () => Promise<T>): Promise<T> {
  try {
    return await operation();
  } catch (error) {
    if (error instanceof Db2Error) {
      const compat = error as Db2Error & {
        SQLCODE?: number | undefined;
        SQLSTATE?: string | undefined;
        sqlcode?: number | undefined;
        sqlstate?: string | undefined;
      };
      compat.SQLCODE ??= error.sqlCode;
      compat.SQLSTATE ??= error.sqlState;
      compat.sqlcode ??= error.sqlCode;
      compat.sqlstate ??= error.sqlState;
      if (error instanceof Db2ServerError) rewriteServerMessage(error);
    }
    throw error;
  }
}

/** Replaces the driver's "DB2 SQLCODE=... SQLSTATE=..." prefix with the node-ibm_db shape
 * "[JustyBase][DB2] SQL0204N  <server text>  SQLSTATE=42704". Only the first rewrite applies. */
function rewriteServerMessage(error: Db2ServerError): void {
  const original = error.message;
  if (!original.startsWith("DB2 SQLCODE=") || error.sqlCode === undefined) return;
  const serverText = original.replace(/^DB2 SQLCODE=-?\d+(?: SQLSTATE=[A-Z0-9]{5})?\s*/u, "").trim();
  const token = `SQL${String(Math.abs(error.sqlCode)).padStart(4, "0")}${error.sqlCode < 0 ? "N" : "W"}`;
  const state = error.sqlState ? `  SQLSTATE=${error.sqlState}` : "";
  const message = `[JustyBase][DB2] ${token}  ${serverText}${state}`.trimEnd();
  error.message = message;
  if (error.stack) error.stack = error.stack.replace(original, message);
}

function safeLength(length: bigint): number {
  return Number(length > BigInt(Number.MAX_SAFE_INTEGER) ? BigInt(Number.MAX_SAFE_INTEGER) : length);
}

/** Column type name as node-ibm_db reports it. Db2 FLOAT is REAL for a 4-byte column, DOUBLE otherwise. */
function ibmTypeName(column: ColumnDescription): string {
  const base = baseSqlType(column.sqlType);
  if (base === SqlType.Float) return column.length === 4n ? "REAL" : "DOUBLE";
  return sqlTypeName(base);
}

/** ODBC SQL type codes, which node-ibm_db reports in SQL_DESC_CONSIZE_TYPE. Unlisted types report 0. */
const ODBC_TYPE_CODES: ReadonlyMap<string, number> = new Map([
  ["SMALLINT", 5], ["INTEGER", 4], ["BIGINT", -5], ["REAL", 7], ["DOUBLE", 8], ["DECIMAL", 3],
  ["CHAR", 1], ["VARCHAR", 12], ["BINARY", -2], ["VARBINARY", -3],
  ["DATE", 91], ["TIME", 92], ["TIMESTAMP", 93], ["BLOB", -98], ["CLOB", -99],
]);

function odbcTypeCode(typeName: string): number {
  return ODBC_TYPE_CODES.get(typeName) ?? 0;
}

/** Types whose SQL_DESC_LENGTH is the declared length rather than the display size. */
const LENGTH_IS_DECLARED: ReadonlySet<string> = new Set(["BINARY", "VARBINARY", "BLOB"]);

/** ODBC display size: fixed widths for numeric and temporal types, the declared length for
 * character types, and twice the declared length for binary types. */
function ibmDisplaySize(typeName: string, column: ColumnDescription): number {
  const length = safeLength(column.length);
  switch (typeName) {
    case "SMALLINT": return 6;
    case "INTEGER": return 11;
    case "BIGINT": return 20;
    case "REAL": return 13;
    case "DOUBLE": return 22;
    case "DECIMAL": return column.precision + 2;
    case "DATE": return 10;
    case "TIME": return 8;
    case "TIMESTAMP": return column.scale > 0 ? 19 + column.scale + 1 : 19;
    case "BINARY":
    case "VARBINARY":
    case "BLOB": return length * 2;
    default: return length;
  }
}

function stripLeadingSqlComments(sql: string): string {
  let result = sql.trimStart();
  while (result) {
    if (result.startsWith("--")) {
      const lineEnd = result.indexOf("\n");
      result = lineEnd < 0 ? "" : result.slice(lineEnd + 1).trimStart();
      continue;
    }
    if (result.startsWith("/*")) {
      const commentEnd = result.indexOf("*/", 2);
      result = commentEnd < 0 ? result : result.slice(commentEnd + 2).trimStart();
      if (commentEnd < 0) return "";
      continue;
    }
    break;
  }
  return result;
}

function isCallStatement(sql: string): boolean {
  return /^CALL\b/i.test(stripLeadingSqlComments(sql).replace(/;+$\s*/, ""));
}

function isResultQuery(sql: string): boolean {
  return /^(?:SELECT|WITH|VALUES)\b/i.test(stripLeadingSqlComments(sql).replace(/;+$\s*/, ""));
}
