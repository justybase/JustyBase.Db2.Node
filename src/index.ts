// Stable alpha package surface. Keep DRDA/DDM codecs, converters, test helpers
// and transport internals behind the package boundary.
export {
  DsnError,
  newConfig,
  parseDsn,
} from "./config.js";
export type { Config } from "./config.js";

export {
  Db2ImportError,
} from "./import.js";
export type {
  Db2ImportCommitPolicy,
  Db2ImportOptions,
  Db2ImportProgress,
  Db2ImportTarget,
} from "./import.js";

export * from "./errors.js";

export {
  Db2Connection,
  Db2ProcedureReader,
  Db2Statement,
  Db2Transaction,
  connectDb2,
} from "./network/session.js";
export type {
  Db2BinaryLobStream,
  Db2ClientInfo,
  Db2ExecutionResult,
  Db2InsertResult,
  Db2IsolationLevel,
  Db2LobStream,
  Db2Parameter,
  Db2ParameterDirection,
  Db2ResultSet,
  Db2TextLobStream,
  QueryOptions,
  QueryResult,
  QueryStreamRow,
} from "./network/session.js";

export {
  Db2Pool,
  clearAllDb2Pools,
} from "./network/pool.js";
export type {
  Db2ConnectionFactory,
  Db2PoolAcquireOptions,
  Db2PoolOptions,
} from "./network/pool.js";

export {
  Db2ResourceBudget,
  DB2_RESOURCE_DEFAULTS,
} from "./resources.js";
