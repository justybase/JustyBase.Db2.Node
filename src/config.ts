import { Db2Error } from "./errors.js";

export type Config = {
  host: string;
  port: number;
  database: string;
  currentSchema?: string;
  user: string;
  password: string;
  useSsl: boolean;
  sslRootCaPath: string;
  sslClientCertPath: string;
  sslClientKeyPath: string;
  timeoutMs: number;
  blockSize: number;
  securityMechanism: number;
  kerberosSpn: string;
  kerberosRealm: string;
  krb5ConfigFile: string;
  krb5KeytabFile: string;
  krb5CcacheFile: string;
  gssapiTokenFile: string;
  clientApplName: string;
  clientWrkstnName: string;
  clientUserid: string;
  clientAcctng: string;
  clientCorrToken: string;
  params: Record<string, string>;
};

export class DsnError extends Db2Error {
  constructor(message: string) {
    super(message, "invalid-connection-string");
    this.name = "DsnError";
  }
}

export function newConfig(): Config {
  return {
    host: "localhost",
    port: 50000,
    database: "",
    currentSchema: "",
    user: "",
    password: "",
    useSsl: false,
    sslRootCaPath: "",
    sslClientCertPath: "",
    sslClientKeyPath: "",
    timeoutMs: 30_000,
    blockSize: 65_535,
    securityMechanism: 0,
    kerberosSpn: "",
    kerberosRealm: "",
    krb5ConfigFile: "",
    krb5KeytabFile: "",
    krb5CcacheFile: "",
    gssapiTokenFile: "",
    clientApplName: "",
    clientWrkstnName: "",
    clientUserid: "",
    clientAcctng: "",
    clientCorrToken: "",
    params: Object.create(null) as Record<string, string>,
  };
}

export function parseDsn(input: string): Config {
  const dsn = input.trim();
  if (!dsn) throw new DsnError("invalid connection string: empty DSN");
  if (/^db2s?:\/\//i.test(dsn)) return parseUrlDsn(dsn);
  return parseKeyValueDsn(dsn);
}

function parseUrlDsn(dsn: string): Config {
  let url: URL;
  try {
    url = new URL(dsn);
  } catch (error) {
    throw new DsnError(`invalid connection string: ${redactDsn(errorMessage(error))}`);
  }
  if (url.protocol !== "db2:" && url.protocol !== "db2s:") {
    throw new DsnError("invalid connection string: expected scheme 'db2' or 'db2s'");
  }

  const config = newConfig();
  if (url.protocol === "db2s:") config.useSsl = true;
  // Node's URL serializes an IPv6 hostname with brackets ("[::1]"), which is not
  // a connectable host string; C#/Rust store the bare address. Match them.
  if (url.hostname) config.host = stripHostBrackets(url.hostname);
  if (url.port) {
    const port = Number(url.port);
    if (!Number.isInteger(port) || port < 1 || port > 65_535) {
      throw new DsnError(`invalid connection string: invalid port ${url.port}`);
    }
    config.port = port;
  }

  let database: string;
  try {
    database = decodeURIComponent(url.pathname.replace(/^\//, ""));
  } catch {
    throw new DsnError("invalid connection string: malformed database escape");
  }
  if (!database) throw new DsnError("invalid connection string: missing database name");
  config.database = database;

  try {
    config.user = decodeURIComponent(url.username);
    config.password = decodeURIComponent(url.password);
  } catch {
    throw new DsnError("invalid connection string: malformed user-info escape");
  }

  const seen = new Set<string>();
  for (const [key, value] of url.searchParams) {
    if (seen.has(key)) continue;
    seen.add(key);
    applyParam(config, key, value);
  }
  return config;
}

function parseKeyValueDsn(dsn: string): Config {
  const config = newConfig();
  for (const pair of dsn.split(";")) {
    const trimmed = pair.trim();
    if (!trimmed) continue;
    const equals = trimmed.indexOf("=");
    if (equals < 0) continue;
    applyParam(config, trimmed.slice(0, equals).trim(), trimmed.slice(equals + 1).trim());
  }
  return config;
}

export function applyParam(config: Config, key: string, value: string): void {
  const normalized = key.toLowerCase();
  switch (normalized) {
    case "host": case "hostname": config.host = value; break;
    case "server": {
      const endpoint = parseServerEndpoint(value);
      config.host = endpoint.host;
      if (endpoint.port !== undefined) config.port = endpoint.port;
      break;
    }
    case "port": {
      const port = Number(value);
      if (Number.isInteger(port) && port > 0 && port <= 65_535) config.port = port;
      break;
    }
    case "database": case "dbname": case "db": config.database = value; break;
    case "currentschema": case "current_schema": config.currentSchema = value; break;
    case "user": case "uid": case "username": config.user = value; break;
    case "password": case "pwd": config.password = value; break;
    case "authentication": {
      const mode = value.trim().toLowerCase();
      if (mode === "server") config.securityMechanism = 3;
      else if (mode === "server_encrypt") config.securityMechanism = 9;
      else if (mode === "kerberos" || mode === "gssplugin") config.securityMechanism = 11;
      else throw unsupportedAuthentication(value);
      break;
    }
    case "security":
      if (value.toLowerCase() !== "ssl") throw new DsnError("only Security=SSL is supported; use a TLS connection or omit the Security keyword");
      config.useSsl = true; break;
    case "ssl": case "use_ssl": case "usessl":
      config.useSsl = isTrue(value);
      if (config.useSsl && config.port === 50_000) config.port = 50_001;
      break;
    case "ssl_root_ca": case "ssl_ca": case "sslrootcapath": case "ssl_ca_path": case "sslservercertificate": case "ssl_server_certificate":
      config.sslRootCaPath = value; config.useSsl = true; break;
    case "ssl_client_cert_path": case "ssl_client_cert": case "sslcert": case "cert":
      config.sslClientCertPath = value; config.useSsl = true; break;
    case "ssl_client_key_path": case "ssl_client_key": case "sslkey": case "key":
      config.sslClientKeyPath = value; config.useSsl = true; break;
    case "timeout": case "connect_timeout": case "connecttimeout": {
      const duration = parseDuration(value);
      if (duration !== undefined) config.timeoutMs = duration;
      break;
    }
    case "block_size": case "blocksize": case "qryblksz": {
      const size = Number(value);
      if (Number.isInteger(size) && size >= 1_024 && size <= 1_048_576) config.blockSize = size;
      break;
    }
    case "security_mechanism": case "secmec": case "auth_mechanism": case "auth":
      config.securityMechanism = parseSecurityMechanism(value); break;
    case "spn": case "service_principal": case "server_principal": config.kerberosSpn = value; break;
    case "realm": case "krb5_realm": config.kerberosRealm = value; break;
    case "krb5_config": case "krb5_conf": config.krb5ConfigFile = value; break;
    case "krb5_keytab": case "keytab": config.krb5KeytabFile = value; break;
    case "krb5_ccache": case "ccache": config.krb5CcacheFile = value; break;
    case "gssapi_token_file": case "gssapi_token": config.gssapiTokenFile = value; break;
    case "client_applname": case "applname": case "application_name": case "appname": config.clientApplName = value; break;
    case "client_wrkstnname": case "wrkstnname": case "workstation_name": case "workstation": config.clientWrkstnName = value; break;
    case "client_userid": case "client_user": case "end_user": config.clientUserid = value; break;
    case "client_acctng": case "acctng": case "accounting": config.clientAcctng = value; break;
    case "client_corr_token": case "corr_token": case "correlation_token": config.clientCorrToken = value; break;
    default: config.params[key] = value;
  }
}

function parseServerEndpoint(value: string): { host: string; port?: number } {
  const ipv6 = /^\[([^\]]+)\](?::(\d+))?$/.exec(value);
  if (ipv6) {
    const port = ipv6[2] === undefined ? undefined : Number(ipv6[2]);
    if (port === undefined) return { host: ipv6[1]! };
    if (Number.isInteger(port) && port >= 1 && port <= 65_535) return { host: ipv6[1]!, port };
    return { host: value };
  }
  const hostPort = /^([^:]+):(\d+)$/.exec(value);
  if (!hostPort) return { host: value };
  const host = hostPort[1]!;
  const port = hostPort[2] === undefined ? undefined : Number(hostPort[2]);
  if (port === undefined || !Number.isInteger(port) || port < 1 || port > 65_535) return { host: value };
  return { host, port };
}

function isTrue(value: string): boolean {
  const normalized = value.toLowerCase();
  return normalized === "true" || normalized === "1" || normalized === "yes";
}

function stripHostBrackets(hostname: string): string {
  return hostname.startsWith("[") && hostname.endsWith("]") ? hostname.slice(1, -1) : hostname;
}

function parseSecurityMechanism(value: string): number {
  switch (value.trim().toLowerCase()) {
    case "server": case "usridpwd": case "plaintext": case "secmec3": case "3": return 3;
    case "server_encrypt": case "eusridpwd": case "dh_encrypted_password": case "secmec9": case "9": return 9;
    case "kerberos": case "gssapi": case "gssplugin": case "secmec11": case "11": return 11;
    case "0": return 0;
    default: {
      const parsed = Number(value.trim());
      if (Number.isInteger(parsed) && (parsed === 0 || parsed === 3 || parsed === 9 || parsed === 11)) return parsed;
      throw unsupportedAuthentication(value);
    }
  }
}

function unsupportedAuthentication(value: string): DsnError {
  return new DsnError(`unsupported DB2 authentication mode '${value}'; this driver supports Authentication=SERVER (3), Authentication=SERVER_ENCRYPT / security_mechanism=9 (EUSRIDPWD), and security_mechanism=11 (Kerberos/GSSAPI with gssapi_token_file)`);
}

// Matches Go's time.ParseDuration forms plus a bare integer as seconds.
function parseDuration(value: string): number | undefined {
  const input = value.trim();
  const bareSeconds = /^\d+$/.test(input) ? Number(input) * 1_000 : undefined;
  if (bareSeconds !== undefined) return Number.isSafeInteger(bareSeconds) ? bareSeconds : undefined;
  const pattern = /([+-]?\d+(?:\.\d+)?)(ns|us|µs|μs|ms|s|m|h)/gy;
  let offset = 0;
  let total = 0;
  while (offset < input.length) {
    pattern.lastIndex = offset;
    const match = pattern.exec(input);
    if (!match) return undefined;
    const magnitude = Number(match[1]);
    const unit = match[2];
    const multiplier = unit === "h" ? 3_600_000 : unit === "m" ? 60_000 : unit === "s" ? 1_000 : unit === "ms" ? 1 : (unit === "us" || unit === "µs" || unit === "μs") ? 0.001 : 0.000001;
    total += magnitude * multiplier;
    offset = pattern.lastIndex;
  }
  return Number.isSafeInteger(Math.round(total)) ? Math.round(total) : undefined;
}

export function redactDsn(input: string): string {
  return maskUrlCredential(input).replace(/(\b(?:password|pwd)\s*=\s*)[^;]*/gi, "$1******");
}

function maskUrlCredential(input: string): string {
  // Take the last '@' inside the authority and the last ':' before it, so a password
  // that itself contains '@' is masked completely instead of only up to the first one.
  const scheme = input.indexOf("://");
  if (scheme < 0) return input;
  const authorityStart = scheme + 3;
  let authorityEnd = input.length;
  for (const terminator of ["/", "?", "#"]) {
    const index = input.indexOf(terminator, authorityStart);
    if (index >= 0 && index < authorityEnd) authorityEnd = index;
  }
  const authority = input.slice(authorityStart, authorityEnd);
  const at = authority.lastIndexOf("@");
  if (at < 0) return input;
  const colon = authority.lastIndexOf(":", at);
  if (colon < 0) return input;
  return `${input.slice(0, authorityStart + colon + 1)}******${input.slice(authorityStart + at)}`;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** Trims and validates a CURRENT SCHEMA value before any network I/O.
 * Returns "" when no schema switch is wanted. Throws DsnError otherwise. */
export function normalizeCurrentSchema(raw: string | undefined): string {
  const trimmed = (raw ?? "").trim();
  if (!trimmed) return "";
  if (trimmed.length > 128) throw new DsnError(`invalid current schema ${JSON.stringify(raw)}: delimited identifier exceeds 128 characters`);
  if (trimmed.includes("\0")) throw new DsnError("invalid current schema: NUL byte");
  return trimmed;
}

export function formatConfig(config: Config): string {  return `Config{Host:${config.host}, Port:${config.port}, Database:${config.database}, User:${config.user}, Password:"******", UseSSL:${config.useSsl}, Timeout:${formatDuration(config.timeoutMs)}}`;
}

function formatDuration(milliseconds: number): string {
  if (milliseconds % 1_000 === 0) return `${milliseconds / 1_000}s`;
  return `${milliseconds}ms`;
}
