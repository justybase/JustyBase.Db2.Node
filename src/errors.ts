/** Stable, machine-readable categories for failures returned by the native Db2 client. */
export type Db2ErrorCategory =
  | "invalid-connection-string"
  | "invalid-configuration"
  | "connection-closed"
  | "database-not-found"
  | "authentication"
  | "authorization"
  | "parameter-conversion"
  | "server"
  | "server-response"
  | "batch-execution"
  | "operation-cancelled"
  | "timeout"
  | "unsupported-feature"
  | "pool-timeout"
  | "pool-exhausted"
  | "transport"
  | "protocol"
  | "resource-limit"
  | "other";

/** Canonical JDC identifier used in serialized and cross-driver diagnostics. */
export type Db2CanonicalErrorCategory =
  | "configuration"
  | "connection-closed"
  | "database-not-found"
  | "authentication"
  | "authorization"
  | "parameter-conversion"
  | "sql-server"
  | "server-response"
  | "transport"
  | "protocol"
  | "resource-limit"
  | "operation-cancelled"
  | "timeout"
  | "unsupported"
  | "pool-timeout"
  | "pool-exhausted"
  | "batch-execution"
  | "other";

const CANONICAL_CATEGORY: Readonly<Record<Db2ErrorCategory, Db2CanonicalErrorCategory>> = Object.freeze({
  "invalid-connection-string": "configuration",
  "invalid-configuration": "configuration",
  "connection-closed": "connection-closed",
  "database-not-found": "database-not-found",
  "authentication": "authentication",
  "authorization": "authorization",
  "parameter-conversion": "parameter-conversion",
  "server": "sql-server",
  "server-response": "server-response",
  "batch-execution": "batch-execution",
  "operation-cancelled": "operation-cancelled",
  "timeout": "timeout",
  "unsupported-feature": "unsupported",
  "pool-timeout": "pool-timeout",
  "pool-exhausted": "pool-exhausted",
  "transport": "transport",
  "protocol": "protocol",
  "resource-limit": "resource-limit",
  "other": "other",
});

/** Maps a language-specific category to its canonical JDC identifier. */
export function canonicalErrorCategory(category: Db2ErrorCategory): Db2CanonicalErrorCategory {
  return CANONICAL_CATEGORY[category];
}

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
    options: {
      cause?: unknown;
      sqlCode?: number;
      sqlState?: string;
      securityCode?: number;
      outcome?: Db2OperationOutcome;
    } = {},
  ) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.outcome = options.outcome ?? (
      ["invalid-connection-string", "invalid-configuration", "pool-timeout", "pool-exhausted"].includes(category)
        ? "not-started"
        : category === "server" && options.sqlCode !== undefined ? "confirmed"
          : category === "operation-cancelled" && options.sqlCode === -952 ? "confirmed"
            : "unknown"
    );
    this.sqlCode = options.sqlCode;
    this.sqlState = options.sqlState;
    this.securityCode = options.securityCode;
    this.name = "Db2Error";
  }

  /** Canonical JDC identifier for this error category. */
  get canonicalCategory(): Db2CanonicalErrorCategory {
    return canonicalErrorCategory(this.category);
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

export class Db2AuthorizationError extends Db2Error {
  constructor(message: string) {
    super(message, "authorization");
    this.name = "Db2AuthorizationError";
  }
}

export class Db2ParameterConversionError extends Db2Error {
  constructor(message: string, cause?: unknown) {
    super(message, "parameter-conversion", cause === undefined ? {} : { cause });
    this.name = "Db2ParameterConversionError";
  }
}

export class Db2ServerResponseError extends Db2Error {
  constructor(message: string) {
    super(message, "server-response");
    this.name = "Db2ServerResponseError";
  }
}

export class Db2OperationCancelledError extends Db2Error {
  constructor(message = "The DB2 operation was cancelled", outcome: Db2OperationOutcome = "unknown", cause?: unknown) {
    super(message, "operation-cancelled", { cause, outcome });
    this.name = "Db2OperationCancelledError";
  }
}

export class Db2ServerError extends Db2Error {
  constructor(sqlCode: number, sqlState?: string, message = "") {
    const stateText = sqlState ? ` SQLSTATE=${sqlState}` : "";
    const cancelled = sqlCode === -952;
    super(
      `DB2 SQLCODE=${sqlCode}${stateText}${message ? ` ${message}` : ""}`,
      cancelled ? "operation-cancelled" : "server",
      {
        ...(sqlState ? { sqlCode, sqlState } : { sqlCode }),
        ...(cancelled ? { outcome: "confirmed" as const } : {}),
      },
    );
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
    const outcome = cause instanceof Db2Error && cause.outcome === "confirmed"
      ? "confirmed"
      : parameterSetIndex === undefined
        ? "not-started"
        : cause instanceof Db2Error && cause.outcome === "not-started"
          ? "not-started"
          : "unknown";
    super(message, "batch-execution", {
      cause,
      outcome,
      ...(cause instanceof Db2Error && cause.sqlCode !== undefined ? { sqlCode: cause.sqlCode } : {}),
      ...(cause instanceof Db2Error && cause.sqlState !== undefined ? { sqlState: cause.sqlState } : {}),
    });
    this.name = "Db2BatchExecutionError";
  }
}

/** An aborted sequential batch with the confirmed work preceding its in-flight set. */
export class Db2BatchAbortError extends DOMException {
  readonly category = "operation-cancelled" as const;
  readonly canonicalCategory = "operation-cancelled" as const;
  readonly outcome: Db2OperationOutcome;
  readonly sqlCode = undefined;
  readonly sqlState = undefined;
  readonly securityCode = undefined;
  constructor(
    readonly completedParameterSets: number,
    readonly inFlightParameterSetIndex: number | undefined,
    readonly rowsAffected: number,
    readonly cause: unknown,
  ) {
    super(cause instanceof Error ? cause.message : "The operation was aborted", "AbortError");
    this.outcome = inFlightParameterSetIndex === undefined ? "not-started" : "unknown";
  }
}

/** A command deadline expired. `name` remains `TimeoutError` for DOM compatibility. */
export class Db2TimeoutError extends DOMException {
  readonly category = "timeout" as const;
  readonly canonicalCategory = "timeout" as const;
  readonly outcome: Db2OperationOutcome;
  readonly sqlCode = undefined;
  readonly sqlState = undefined;
  readonly securityCode = undefined;
  constructor(message = "The DB2 command timed out", readonly cause?: unknown, outcome: Db2OperationOutcome = "unknown") {
    super(message, "TimeoutError");
    this.outcome = outcome;
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
    super(
      cause instanceof Error ? cause.message : "The operation timed out",
      cause,
      inFlightParameterSetIndex === undefined ? "not-started" : "unknown",
    );
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
  constructor() { super("DB2 connection timed out", "timeout", { outcome: "not-started" }); this.name = "Db2ConnectTimeoutError"; }
}

/** Returns the phase-derived outcome when an error exposes one. */
export function operationOutcomeOf(error: unknown): Db2OperationOutcome | undefined {
  if (error instanceof Db2Error) return error.outcome;
  if (error instanceof Db2TimeoutError || error instanceof Db2BatchAbortError) return error.outcome;
  return undefined;
}
