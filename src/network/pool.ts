import { performance } from "node:perf_hooks";
import { publishDb2Diagnostic } from "../diagnostics.js";
import type { Config } from "../config.js";
import { normalizeAdminCommand } from "../admin.js";
import { Db2BatchAbortError, Db2ConnectionClosedError, Db2PoolTimeoutError, Db2PoolExhaustedError, Db2ConnectTimeoutError } from "../errors.js";
import { Db2Connection, Db2ProcedureReader, type Db2Parameter, type QueryOptions, type QueryResult, type QueryStreamRow } from "./session.js";

export type Db2ConnectionFactory = (input: string | Config) => Promise<Db2Connection>;
export type Db2PoolOptions = {
  /** Maximum number of physical sessions held by the pool. Defaults to 10. */
  maxConnections?: number;
  maxWaiters?: number;
  acquireTimeoutMs?: number;
  connectTimeoutMs?: number;
  idleTimeoutMs?: number;
  maxLifetimeMs?: number;
  reapIntervalMs?: number;
  /** Connection factory override, primarily useful for tests and custom transports. */
  connectionFactory?: Db2ConnectionFactory;
};

export type Db2PoolAcquireOptions = { signal?: AbortSignal };

type Waiter = {
  resolve: (connection: Db2Connection) => void;
  reject: (error: unknown) => void;
  cancel: (error: unknown) => void;
  cleanup: () => void;
  cancelled: boolean;
  settled: boolean;
};

/** Weak registry of live pools for test/maintenance cleanup. Entries vanish with
 * their pool; close() also unregisters eagerly so abandoned registries stay small. */
const poolRegistry = new Set<WeakRef<Db2Pool>>();

/** Closes idle sessions of every live pool without stopping the pools themselves.
 * Active leases and queued waiters are untouched; pools remain usable and lazily
 * reopen sessions on demand. Intended for test teardown, not the hot path. */
export async function clearAllDb2Pools(): Promise<void> {
  const pools: Db2Pool[] = [];
  for (const ref of poolRegistry) {
    const pool = ref.deref();
    if (pool === undefined) poolRegistry.delete(ref);
    else pools.push(pool);
  }
  await Promise.all(pools.map((pool) => pool.drainIdle().catch(() => {})));
}

/** A bounded asynchronous pool. A checked out session is returned even when its operation throws. */
export class Db2Pool {
  private readonly maxConnections: number;
  private readonly connectionFactory: Db2ConnectionFactory;
  private readonly idle: Db2Connection[] = [];
  private readonly waiters: Waiter[] = [];
  private total = 0;
  private readonly limits: Required<Omit<Db2PoolOptions, "connectionFactory">>;
  private readonly timestamps = new WeakMap<Db2Connection, { born: number; idle: number }>();
  private readonly active = new Set<Db2Connection>();
  private readonly reaper: ReturnType<typeof setInterval>;
  private discarded = 0;
  private timeouts = 0;
  private exhausted = 0;
  private closed = false;
  private closePromise: Promise<void> | undefined;

  constructor(private readonly input: string | Config, options: Db2PoolOptions = {}) {
    this.maxConnections = options.maxConnections ?? 10;
    if (!Number.isSafeInteger(this.maxConnections) || this.maxConnections < 1)
      throw new RangeError("DB2 pool maxConnections must be a positive integer");
    this.limits = { maxConnections: this.maxConnections, maxWaiters: options.maxWaiters ?? 100,
      acquireTimeoutMs: options.acquireTimeoutMs ?? 30_000, connectTimeoutMs: options.connectTimeoutMs ?? 10_000,
      idleTimeoutMs: options.idleTimeoutMs ?? 300_000, maxLifetimeMs: options.maxLifetimeMs ?? 1_800_000,
      reapIntervalMs: options.reapIntervalMs ?? 30_000 };
    for (const [name, value] of Object.entries(this.limits))
      if (!Number.isSafeInteger(value) || value < 1 || (name.endsWith("Ms") && value > 2_147_483_647))
        throw new RangeError(`DB2 pool ${name} must be a positive integer within the supported range`);
    this.reaper = setInterval(() => this.reapIdle(), this.limits.reapIntervalMs);
    this.reaper.unref();
    this.connectionFactory = options.connectionFactory ?? ((dsn) => Db2Connection.connect(dsn));
    poolRegistry.add(new WeakRef(this));
  }

  get isClosed(): boolean { return this.closed; }
  get connectionCount(): number { return this.total; }
  get idleCount(): number { return this.idle.length; }
  get diagnostics() { return { connections: this.total, idle: this.idle.length, active: this.active.size,
    waiting: this.waiters.length, discarded: this.discarded, acquireTimeouts: this.timeouts, exhausted: this.exhausted }; }

  private expired(connection: Db2Connection): boolean {
    const time = this.timestamps.get(connection);
    const now = performance.now();
    return !!time && (now - time.born >= this.limits.maxLifetimeMs || now - time.idle >= this.limits.idleTimeoutMs);
  }

  private reapIdle(): void {
    for (let index = this.idle.length - 1; index >= 0; index--) {
      const connection = this.idle[index]!;
      if (connection.isClosed || this.expired(connection)) {
        this.idle.splice(index, 1); this.total--; this.closeDiscarded(connection);
      }
    }
    this.wakeWaiter();
  }

  /** Stops admission, waits for leases, then closes remaining physical sessions. */
  async shutdown(timeoutMs = 30_000): Promise<void> {
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0) throw new RangeError("invalid shutdown timeout");
    await this.close();
    const until = performance.now() + timeoutMs;
    while (this.active.size && performance.now() < until)
      await new Promise<void>(resolve => setTimeout(resolve, Math.min(20, Math.max(1, until - performance.now()))));
    await Promise.all([...this.active].map(connection => connection.close()));
  }

  /** Acquires one session; `options.signal` cancels while waiting for a pool slot. */
  async withConnection<T>(operation: (connection: Db2Connection) => Promise<T>, options: Db2PoolAcquireOptions = {}): Promise<T> {
    const connection = await this.acquire(options.signal);
    const started = performance.now();
    let result: T;
    try { result = await operation(connection); }
    catch (error) {
      // The pool still attempts its reset so server state is cleaned, but a reset
      // failure must never mask the operation error. It is attached as cleanup
      // context instead; uncertain DML is never retried automatically.
      await this.release(connection, error);
      publishDb2Diagnostic({ operation: "execute", durationMs: performance.now() - started, success: false });
      throw error;
    }
    publishDb2Diagnostic({ operation: "execute", durationMs: performance.now() - started, success: true });
    await this.release(connection);
    return result;
  }

  query(sql: string, params?: readonly unknown[], options: QueryOptions = {}): Promise<QueryResult> {
    return this.withConnection(
      (connection) => connection.query(sql, params, options),
      options.signal ? { signal: options.signal } : {},
    );
  }

  /** Starts a procedure result reader while retaining its session lease until finish/close. */
  async executeProcedureStream(sql: string, parameters: readonly Db2Parameter[] = [], options: QueryOptions = {}): Promise<Db2ProcedureReader> {
    const connection = await this.acquire(options.signal);
    try {
      const reader = await connection.executeProcedureStream(sql, parameters, options);
      reader.attachCompletionHandler(() => this.release(connection));
      return reader;
    } catch (error) {
      await this.release(connection, error);
      throw error;
    }
  }

  /** Streams rows while holding one pool lease; stopping early cancels/drains before returning it. */
  async *queryStream(sql: string, options: QueryOptions = {}): AsyncGenerator<QueryStreamRow> {
    const connection = await this.acquire(options.signal);
    let primary: unknown;
    let hasPrimary = false;
    try {
      for await (const row of connection.queryStream(sql, options)) yield row;
    } catch (error) { primary = error; hasPrimary = true; throw error; }
    finally { await this.release(connection, hasPrimary ? primary : undefined); }
  }

  async *queryStreamWithParams(sql: string, params: readonly unknown[], options: QueryOptions = {}): AsyncGenerator<QueryStreamRow> {
    const connection = await this.acquire(options.signal);
    let primary: unknown;
    let hasPrimary = false;
    try {
      for await (const row of connection.queryStreamWithParams(sql, params, options)) yield row;
    } catch (error) { primary = error; hasPrimary = true; throw error; }
    finally { await this.release(connection, hasPrimary ? primary : undefined); }
  }

  execute(sql: string, params?: readonly unknown[], options: QueryOptions = {}): Promise<number> {
    return this.withConnection((connection) => connection.execute(sql, params, options),
      options.signal ? { signal: options.signal } : {});
  }

  /** Executes one Db2 administrative command through a pooled session. */
  executeAdminCommand(command: string, options: QueryOptions = {}): Promise<number> {
    const normalized = normalizeAdminCommand(command);
    return this.withConnection((connection) => connection.executeAdminCommand(normalized, options),
      options.signal ? { signal: options.signal } : {});
  }

  async executeBatch(sql: string, parameterSets: readonly (readonly unknown[])[], options: QueryOptions = {}): Promise<number> {
    let started = false;
    try {
      return await this.withConnection(
        (connection) => {
          started = true;
          return connection.executeBatch(sql, parameterSets, options);
        },
        options.signal ? { signal: options.signal } : {},
      );
    } catch (error) {
      if (options.signal?.aborted && !started)
        throw new Db2BatchAbortError(0, undefined, 0, error);
      throw error;
    }
  }

  close(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    this.closed = true;
    clearInterval(this.reaper);
    for (const ref of poolRegistry) if (ref.deref() === this) poolRegistry.delete(ref);
    this.closePromise = Promise.resolve().then(async () => {
      const error = new Db2ConnectionClosedError("DB2 pool is closed");
      for (const waiter of this.waiters.splice(0)) waiter.reject(error);
      const idle = this.idle.splice(0);
      this.total -= idle.length;
      await Promise.all(idle.map((connection) => connection.close()));
    });
    return this.closePromise;
  }

  /** Closes idle sessions without stopping the pool; active leases are untouched.
   * The pool stays open and reopens sessions on demand. */
  async drainIdle(): Promise<void> {
    if (this.closed) return;
    const idle = this.idle.splice(0);
    this.total -= idle.length;
    for (const connection of idle) this.timestamps.delete(connection);
    await Promise.all(idle.map((connection) => connection.close().catch(() => {})));
  }

  private async acquire(signal?: AbortSignal): Promise<Db2Connection> {
    const started = performance.now();
    try {
      const connection = await this.acquireCore(signal);
      if (this.closed) { await this.release(connection); throw new Db2ConnectionClosedError("DB2 pool is closed"); }
      if (signal?.aborted) {
        await this.release(connection, signal.reason);
        throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
      }
      this.active.add(connection);
      publishDb2Diagnostic({ operation: "acquire", durationMs: performance.now() - started, success: true });
      return connection;
    } catch (error) {
      publishDb2Diagnostic({ operation: "acquire", durationMs: performance.now() - started, success: false });
      throw error;
    }
  }

  private async acquireCore(signal?: AbortSignal): Promise<Db2Connection> {
    if (this.closed) throw new Db2ConnectionClosedError("DB2 pool is closed");
    if (signal?.aborted) throw signal.reason ?? new DOMException("The operation was aborted", "AbortError");
    while (this.idle.length) {
      const connection = this.idle.pop()!;
      if (!connection.isClosed && !this.expired(connection)) {
        const idle = this.timestamps.get(connection)?.idle ?? performance.now();
        if (performance.now() - idle > 60_000) {
          try { await connection.query("VALUES 1"); }
          catch { this.total--; this.closeDiscarded(connection); continue; }
        }
        return connection;
      }
      this.total--;
      this.closeDiscarded(connection);
    }
    if (this.total < this.maxConnections) {
      this.total++;
      return this.createReservedConnection(signal);
    }
    if (this.waiters.length >= this.limits.maxWaiters) { this.exhausted++; throw new Db2PoolExhaustedError(); }
    return new Promise<Db2Connection>((resolve, reject) => {
      let deadline: ReturnType<typeof setTimeout> | undefined;
      let onAbort: (() => void) | undefined;
      const waiter: Waiter = {
        cancelled: false,
        settled: false,
        cleanup: () => {
          if (onAbort) signal?.removeEventListener("abort", onAbort);
          if (deadline) clearTimeout(deadline);
        },
        resolve: (connection) => {
          if (waiter.cancelled) {
            this.total--;
            void connection.close().catch(() => {}).finally(() => this.wakeWaiter());
            return;
          }
          if (waiter.settled) return;
          waiter.settled = true;
          waiter.cleanup();
          resolve(connection);
        },
        reject: (error) => {
          if (waiter.settled) return;
          waiter.settled = true;
          waiter.cleanup();
          reject(error instanceof Error ? error : new Error(String(error)));
        },
        cancel: (error) => {
          if (waiter.settled) return;
          waiter.cancelled = true;
          const index = this.waiters.indexOf(waiter);
          if (index >= 0) this.waiters.splice(index, 1);
          waiter.settled = true;
          waiter.cleanup();
          reject(error);
        },
      };
      if (signal) {
        onAbort = () => waiter.cancel(signal.reason ?? new DOMException("The operation was aborted", "AbortError"));
        signal.addEventListener("abort", onAbort, { once: true });
        if (signal.aborted) {
          onAbort();
          return;
        }
      }
      deadline = setTimeout(() => { this.timeouts++; waiter.cancel(new Db2PoolTimeoutError()); }, this.limits.acquireTimeoutMs);
      this.waiters.push(waiter);
    });
  }

  private async createReservedConnection(signal?: AbortSignal): Promise<Db2Connection> {
    let reservationReleased = false;
    try {
      let expired = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      let abort: (() => void) | undefined;
      const pending = this.connectionFactory(this.input).then(async connection => {
        if (expired) { await connection.close().catch(() => {}); throw new Db2ConnectTimeoutError(); }
        return connection;
      });
      let connection: Db2Connection;
      const connectStarted = performance.now();
      try {
        connection = await Promise.race([pending, new Promise<never>((_, reject) => {
          timer = setTimeout(() => { expired = true; reject(new Db2ConnectTimeoutError()); }, this.limits.connectTimeoutMs);
          if (signal) {
            abort = () => { expired = true; reject(signal.reason ?? new DOMException("The operation was aborted", "AbortError")); };
            signal.addEventListener("abort", abort, { once: true });
            if (signal.aborted) abort();
          }
        })]);
      } finally { if (timer) clearTimeout(timer); if (abort) signal?.removeEventListener("abort", abort); }
      publishDb2Diagnostic({ operation: "connect", durationMs: performance.now() - connectStarted, success: true });
      this.timestamps.set(connection, { born: performance.now(), idle: performance.now() });
      if (this.closed) {
        this.total--;
        reservationReleased = true;
        await connection.close();
        throw new Db2ConnectionClosedError("DB2 pool is closed");
      }
      // Record the server-side pool baseline before the session reaches its first
      // caller. Fake sessions in unit tests do not implement the hook and are skipped.
      if (connection instanceof Db2Connection && !connection.hasPoolBaseline) {
        try { await connection.capturePoolBaseline(); }
        catch (error) {
          this.total--;
          reservationReleased = true;
          await connection.close().catch(() => {});
          this.wakeWaiter();
          throw error;
        }
      }
      return connection;
    } catch (error) {
      if (!reservationReleased) this.total--;
      this.wakeWaiter();
      throw error;
    }
  }

  private async release(connection: Db2Connection, primaryError?: unknown): Promise<void> {
    try {
      const born = this.timestamps.get(connection)?.born ?? performance.now();
      if (this.closed || connection.isClosed || performance.now() - born >= this.limits.maxLifetimeMs) {
        this.total--;
        this.closeDiscarded(connection);
        this.wakeWaiter();
        return;
      }
      const resetStarted = performance.now();
      try { await connection.resetForPool(); publishDb2Diagnostic({ operation: "reset", durationMs: performance.now() - resetStarted, success: true }); }
      catch (resetError) {
        publishDb2Diagnostic({ operation: "reset", durationMs: performance.now() - resetStarted, success: false });
        this.total--;
        this.closeDiscarded(connection);
        this.wakeWaiter();
        // A cleanup failure discards the session but never replaces the user
        // result. When the operation itself failed, the reset error is attached
        // as additional context; uncertain DML is never retried automatically.
        if (primaryError instanceof Error && resetError instanceof Error) {
          const target = primaryError as Error & { cleanupCause?: unknown };
          if (target.cleanupCause === undefined) target.cleanupCause = resetError;
        }
        return;
      }
      // close() may have run while the asynchronous reset was in flight. A pool
      // that has been closed must never regain ownership of this session.
      if (this.closed || connection.isClosed) {
        this.total--;
        this.closeDiscarded(connection);
        this.wakeWaiter();
        return;
      }
      this.timestamps.set(connection, { born, idle: performance.now() });
      const waiter = this.waiters.shift();
      if (waiter) waiter.resolve(connection);
      else this.idle.push(connection);
    } finally { this.active.delete(connection); }
  }

  private wakeWaiter(): void {
    if (this.closed || this.total >= this.maxConnections) return;
    const waiter = this.waiters.shift();
    if (!waiter) return;
    this.total++;
    void this.createReservedConnection().then(waiter.resolve, (error: unknown) => {
      waiter.reject(error instanceof Error ? error : new Error(String(error)));
    });
  }

  /** Closes a discarded session without creating an unhandled rejection. */
  private closeDiscarded(connection: Db2Connection): void {
    this.discarded++;
    publishDb2Diagnostic({ operation: "discard", durationMs: 0, success: true });
    void connection.close().catch(() => {});
  }
}
