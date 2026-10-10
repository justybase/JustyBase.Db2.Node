/** Stable, machine-readable categories for failures returned by the native Db2 client. */
export type Db2ErrorCategory =
  | "invalid-connection-string"
  | "invalid-configuration"
  | "connection-closed"
  | "database-not-found"
  | "authentication"
  | "server"
  | "batch-execution"
  | "unsupported-feature"
  | "pool-timeout"
  | "pool-exhausted"
  | "transport"
  | "protocol"
  | "resource-limit";

/** Confirmation status, independent of whether the caller may safely retry. */
export type Db2OperationOutcome = "not-started" | "confirmed" | "unknown";

/** A non-error diagnostic returned by Db2 for a successful command or row. */
export type Db2Warning = {
  readonly sqlCode: number;
  readonly sqlState: string;
  readonly message: string;
  readonly warningFlags: string;
};

export class Db2Error extends Error {
  readonly outcome: Db2OperationOutcome;
  readonly sqlCode: number | undefined;
  readonly sqlState: string | undefined;
  readonly securityCode: number | undefined;

  constructor(
    message: string,
    readonly category: Db2ErrorCategory,
    options: { cause?: unknown; sqlCode?: number; sqlState?: string; securityCode?: number } = {},
  ) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.outcome = ["invalid-connection-string", "invalid-configuration", "pool-timeout", "pool-exhausted"].includes(category)
      ? "not-started" : category === "server" && options.sqlCode !== undefined ? "confirmed" : "unknown";
    this.sqlCode = options.sqlCode;
    this.sqlState = options.sqlState;
    this.securityCode = options.securityCode;
    this.name = "Db2Error";
  }
}

export class Db2ConfigurationError extends Db2Error {
  constructor(message: string, cause?: unknown) {
    super(message, "invalid-configuration", cause === undefined ? {} : { cause });
    this.name = "Db2ConfigurationError";
  }
}

export class Db2ConnectionClosedError extends Db2Error {
  constructor(message = "DB2 connection is closed") {
    super(message, "connection-closed");
    this.name = "Db2ConnectionClosedError";
  }
}

export class Db2DatabaseNotFoundError extends Db2Error {
  constructor(message = "DB2 relational database was not found") {
    super(message, "database-not-found");
    this.name = "Db2DatabaseNotFoundError";
  }
}

export class Db2AuthenticationError extends Db2Error {
  constructor(message: string, securityCode?: number) {
    super(message, "authentication", securityCode === undefined ? {} : { securityCode });
    this.name = "Db2AuthenticationError";
  }
}

export class Db2ServerError extends Db2Error {
  constructor(sqlCode: number, sqlState?: string, message = "") {
    const stateText = sqlState ? ` SQLSTATE=${sqlState}` : "";
    super(`DB2 SQLCODE=${sqlCode}${stateText}${message ? ` ${message}` : ""}`, "server", sqlState ? { sqlCode, sqlState } : { sqlCode });
    this.name = "Db2ServerError";
  }
}

/** A parameter set failed after earlier sets may already have committed. */
export class Db2BatchExecutionError extends Db2Error {
  constructor(
    readonly parameterSetIndex: number | undefined,
    readonly completedParameterSets: number,
    readonly rowsAffected: number,
    cause: unknown,
  ) {
    const detail = cause instanceof Error ? cause.message : String(cause);
    const message = parameterSetIndex === undefined
      ? `DB2 batch setup failed before a parameter set completed: ${detail}`
      : `DB2 batch parameter set ${parameterSetIndex} failed after ${completedParameterSets} completed sets and ${rowsAffected} affected rows: ${detail}`;
    super(message, "batch-execution", {
      cause,
      ...(cause instanceof Db2Error && cause.sqlCode !== undefined ? { sqlCode: cause.sqlCode } : {}),
      ...(cause instanceof Db2Error && cause.sqlState !== undefined ? { sqlState: cause.sqlState } : {}),
    });
    this.name = "Db2BatchExecutionError";
  }
}

/** An aborted sequential batch with the confirmed work preceding its in-flight set. */
export class Db2BatchAbortError extends DOMException {
  constructor(
    readonly completedParameterSets: number,
    readonly inFlightParameterSetIndex: number | undefined,
    readonly rowsAffected: number,
    readonly cause: unknown,
  ) {
    super(cause instanceof Error ? cause.message : "The operation was aborted", "AbortError");
  }
}

/** A command deadline expired. `name` remains `TimeoutError` for DOM compatibility. */
export class Db2TimeoutError extends DOMException {
  constructor(message = "The DB2 command timed out", readonly cause?: unknown) {
    super(message, "TimeoutError");
  }
}

/** A timed out sequential batch with the confirmed work preceding its in-flight set. */
export class Db2BatchTimeoutError extends Db2TimeoutError {
  constructor(
    readonly completedParameterSets: number,
    readonly inFlightParameterSetIndex: number | undefined,
    readonly rowsAffected: number,
    cause: unknown,
  ) {
    super(cause instanceof Error ? cause.message : "The operation timed out", cause);
  }
}

export class Db2UnsupportedFeatureError extends Db2Error {
  constructor(message: string) {
    super(message, "unsupported-feature");
    this.name = "Db2UnsupportedFeatureError";
  }
}

/** Waiting for a pool slot exceeded the configured acquisition deadline. */
export class Db2PoolTimeoutError extends Db2Error {
  constructor() { super("DB2 pool acquisition timed out", "pool-timeout"); this.name = "Db2PoolTimeoutError"; }
}
/** The bounded pool waiting queue is full. */
export class Db2PoolExhaustedError extends Db2Error {
  constructor() { super("DB2 pool waiting queue is full", "pool-exhausted"); this.name = "Db2PoolExhaustedError"; }
}
/** A configured resource bound was reached before an allocation or write. */
export class Db2ResourceLimitError extends Db2Error {
  constructor(message: string) { super(message, "resource-limit"); this.name = "Db2ResourceLimitError"; }
}

export class Db2ConnectTimeoutError extends Db2Error {
  constructor() { super("DB2 connection timed out", "transport"); this.name = "Db2ConnectTimeoutError"; }
}
