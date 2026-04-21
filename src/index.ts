/**
 * Bun binding for Honker — a SQLite-native task runtime.
 *
 * Uses `bun:sqlite` (built into Bun, no native deps) + the Honker
 * loadable extension (libhonker_ext.dylib / .so). Each method is
 * essentially one SQL call; no wrapper state beyond the DB handle.
 *
 * Pass 1 notes:
 *   - Async iterators (listen, stream subscribe, claimWaker) are
 *     poll-based on a short timer. A WAL-driven waker is on the
 *     roadmap.
 */

import { Database as BunDB } from "bun:sqlite";

// ---------------------------------------------------------------------
// SQLite lib shim (Bun's bundled SQLite lacks loadable-extension support)
// ---------------------------------------------------------------------

const COMMON_SQLITE_PATHS = [
  "/opt/homebrew/opt/sqlite/lib/libsqlite3.dylib",
  "/usr/local/opt/sqlite/lib/libsqlite3.dylib",
  "/usr/lib/x86_64-linux-gnu/libsqlite3.so.0",
  "/usr/lib/aarch64-linux-gnu/libsqlite3.so.0",
  "/usr/lib64/libsqlite3.so.0",
];

function locateSystemSqlite(): string | null {
  for (const p of COMMON_SQLITE_PATHS) {
    if (Bun.file(p).size > 0) return p;
  }
  return null;
}

let customSqliteConfigured = false;
function ensureCustomSqlite(override?: string): void {
  if (customSqliteConfigured) return;
  const path = override ?? locateSystemSqlite();
  if (!path) {
    throw new Error(
      "honker-bun: no extension-enabled SQLite found. Bun's bundled " +
        "SQLite is compiled without SQLITE_ENABLE_LOAD_EXTENSION. " +
        "Install SQLite (e.g. `brew install sqlite` on macOS, " +
        "`apt install libsqlite3-dev` on Linux) and pass its path as " +
        "`open(path, extPath, { sqliteLibPath: '/path/to/libsqlite3.dylib' })`.",
    );
  }
  BunDB.setCustomSQLite(path);
  customSqliteConfigured = true;
}

const DEFAULT_PRAGMAS = `
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = NORMAL;
  PRAGMA busy_timeout = 5000;
  PRAGMA foreign_keys = ON;
  PRAGMA cache_size = -32000;
  PRAGMA temp_store = MEMORY;
  PRAGMA wal_autocheckpoint = 10000;
`;

// ---------------------------------------------------------------------
// Options / types
// ---------------------------------------------------------------------

export interface QueueOptions {
  visibilityTimeoutS?: number;
  maxAttempts?: number;
}

export interface EnqueueOptions {
  delay?: number;
  runAt?: number;
  priority?: number;
  expires?: number;
  /**
   * Enqueue inside an open transaction. The row only becomes visible
   * after `tx.commit()`; a rollback drops it. Use for atomic
   * business-write + enqueue.
   */
  tx?: Transaction;
}

export interface NotifyOptions {
  tx?: Transaction;
}

export interface OpenOptions {
  sqliteLibPath?: string;
}

export interface ScheduledTask {
  name: string;
  queue: string;
  cron: string;
  payload: unknown;
  priority?: number;
  expiresS?: number | null;
}

export interface ScheduledFire {
  name: string;
  queue: string;
  fire_at: number;
  job_id: number;
}

export interface StreamEvent {
  offset: number;
  topic: string;
  key: string | null;
  payload: unknown;
  created_at: number;
}

export interface Notification {
  id: number;
  channel: string;
  payload: unknown;
}

interface RawJob {
  id: number;
  queue: string;
  payload: string;
  worker_id: string;
  attempts: number;
  claim_expires_at: number;
}

interface RawStreamEvent {
  offset: number;
  topic: string;
  key: string | null;
  payload: string;
  created_at: number;
}

// ---------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------

/**
 * Honker database handle. Wraps a single `bun:sqlite` Database with
 * the Honker loadable extension loaded and the schema bootstrapped.
 */
export class Database {
  constructor(public readonly raw: BunDB) {}

  /** Get a handle to a named queue. */
  queue(name: string, opts: QueueOptions = {}): Queue {
    return new Queue(this, name, {
      visibilityTimeoutS: opts.visibilityTimeoutS ?? 300,
      maxAttempts: opts.maxAttempts ?? 3,
    });
  }

  /** Get a handle to a named stream. */
  stream(name: string): Stream {
    return new Stream(this, name);
  }

  /** Get a scheduler facade. Cheap; no persistent state. */
  scheduler(): Scheduler {
    return new Scheduler(this);
  }

  /**
   * Fire a pg_notify-style pub/sub signal. Payload is any
   * JSON-serializable value. Returns the notification id.
   *
   * Pass `{tx}` to emit inside an open transaction; listeners see it
   * only after commit.
   */
  notify(
    channel: string,
    payload: unknown,
    opts: NotifyOptions = {},
  ): number {
    const json = JSON.stringify(payload);
    const row = (opts.tx ? opts.tx.raw : this.raw)
      .query<{ v: number }, [string, string]>("SELECT notify(?, ?) AS v")
      .get(channel, json)!;
    return row.v;
  }

  /**
   * Async iterator over notifications on `channel`. Poll-based in
   * Pass 1 (100ms interval). Only rows with id greater than the
   * max-at-attach-time are yielded; no replay.
   *
   * Cancel by breaking the `for await` or calling `.return()` on the
   * iterator. `signal` (AbortSignal) is honored.
   */
  listen(
    channel: string,
    opts: { signal?: AbortSignal; pollMs?: number } = {},
  ): AsyncIterableIterator<Notification> {
    return listenImpl(this, channel, opts.signal, opts.pollMs ?? 100);
  }

  /**
   * Begin a transaction (`BEGIN IMMEDIATE`). Call `commit()` or
   * `rollback()`. If GC'd without either, a best-effort rollback is
   * issued via FinalizationRegistry — but don't rely on it: commit or
   * rollback explicitly.
   */
  transaction(): Transaction {
    this.raw.exec("BEGIN IMMEDIATE");
    return new Transaction(this.raw);
  }

  /** Try to acquire an advisory lock. Returns a Lock or null. */
  tryLock(name: string, owner: string, ttlS: number): Lock | null {
    const row = this.raw
      .query<{ v: number }, [string, string, number]>(
        "SELECT honker_lock_acquire(?, ?, ?) AS v",
      )
      .get(name, owner, ttlS)!;
    if (row.v !== 1) return null;
    return new Lock(this, name, owner);
  }

  /** Fixed-window rate limit. True if the request fits. */
  tryRateLimit(name: string, limit: number, per: number): boolean {
    const row = this.raw
      .query<{ v: number }, [string, number, number]>(
        "SELECT honker_rate_limit_try(?, ?, ?) AS v",
      )
      .get(name, limit, per)!;
    return row.v === 1;
  }

  /** Persist a job result for later retrieval via `getResult`. */
  saveResult(jobId: number, value: string, ttlS: number): void {
    this.raw
      .query<{ v: unknown }, [number, string, number]>(
        "SELECT honker_result_save(?, ?, ?) AS v",
      )
      .get(jobId, value, ttlS);
  }

  /** Fetch a stored result, or null if missing/expired. */
  getResult(jobId: number): string | null {
    const row = this.raw
      .query<{ v: string | null }, [number]>(
        "SELECT honker_result_get(?) AS v",
      )
      .get(jobId);
    return row?.v ?? null;
  }

  /** Drop expired results. Returns rows deleted. */
  sweepResults(): number {
    const row = this.raw
      .query<{ v: number }, []>("SELECT honker_result_sweep() AS v")
      .get()!;
    return row.v;
  }

  /** Close the underlying database. */
  close(): void {
    this.raw.close();
  }
}

/** Open (or create) a SQLite DB, load the Honker extension, bootstrap. */
export function open(
  path: string,
  extensionPath: string,
  opts: OpenOptions = {},
): Database {
  ensureCustomSqlite(opts.sqliteLibPath);
  const raw = new BunDB(path, { create: true, readwrite: true });
  raw.loadExtension(extensionPath);
  raw.exec(DEFAULT_PRAGMAS);
  raw.exec("SELECT honker_bootstrap()");
  return new Database(raw);
}

// ---------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------

// Best-effort rollback on GC. Holds only the raw BunDB reference so
// the Transaction itself can be collected.
const txFinalizer = new FinalizationRegistry<{
  raw: BunDB;
  doneRef: { done: boolean };
}>((held) => {
  if (held.doneRef.done) return;
  try {
    held.raw.exec("ROLLBACK");
  } catch {
    // Finalizer may fire during teardown; swallow.
  }
});

/**
 * An open transaction. Call `commit()` or `rollback()` exactly once.
 * `tx.raw` is the underlying `bun:sqlite` Database — `*_tx` helpers
 * route through it.
 */
export class Transaction {
  private doneRef = { done: false };

  constructor(public readonly raw: BunDB) {
    txFinalizer.register(this, { raw, doneRef: this.doneRef }, this);
  }

  /** Run a statement with optional params. Returns the Statement run result. */
  execute(sql: string, params: unknown[] = []): void {
    const stmt = this.raw.query(sql);
    stmt.run(...(params as Parameters<typeof stmt.run>));
  }

  /** Run a query and return the first row, or null. */
  query<T = Record<string, unknown>>(
    sql: string,
    params: unknown[] = [],
  ): T | null {
    const stmt = this.raw.query<T, unknown[]>(sql);
    return (stmt.get(...params) as T | null) ?? null;
  }

  /** Commit. Idempotent — subsequent calls are no-ops. */
  commit(): void {
    if (this.doneRef.done) return;
    this.raw.exec("COMMIT");
    this.doneRef.done = true;
    txFinalizer.unregister(this);
  }

  /** Roll back. Idempotent — subsequent calls are no-ops. */
  rollback(): void {
    if (this.doneRef.done) return;
    this.raw.exec("ROLLBACK");
    this.doneRef.done = true;
    txFinalizer.unregister(this);
  }

  /** Has this transaction been committed or rolled back? */
  get done(): boolean {
    return this.doneRef.done;
  }
}

// ---------------------------------------------------------------------
// Queues
// ---------------------------------------------------------------------

export class Queue {
  constructor(
    private readonly db: Database,
    public readonly name: string,
    private readonly opts: Required<Omit<QueueOptions, "maxAttempts">> & {
      maxAttempts: number;
    },
  ) {}

  /** Enqueue a job. Payload is JSON-stringified. Returns the row id. */
  enqueue(payload: unknown, opts: EnqueueOptions = {}): number {
    const json = JSON.stringify(payload);
    const conn = opts.tx ? opts.tx.raw : this.db.raw;
    const row = conn
      .query<
        { v: number },
        [
          string,
          string,
          number | null,
          number | null,
          number,
          number,
          number | null,
        ]
      >("SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?) AS v")
      .get(
        this.name,
        json,
        opts.runAt ?? null,
        opts.delay ?? null,
        opts.priority ?? 0,
        this.opts.maxAttempts,
        opts.expires ?? null,
      )!;
    return row.v;
  }

  /** Atomically claim up to n jobs. */
  claimBatch(workerId: string, n: number): Job[] {
    const row = this.db.raw
      .query<{ v: string }, [string, string, number, number]>(
        "SELECT honker_claim_batch(?, ?, ?, ?) AS v",
      )
      .get(this.name, workerId, n, this.opts.visibilityTimeoutS)!;
    const raw: RawJob[] = JSON.parse(row.v);
    return raw.map((r) => new Job(this.db, r));
  }

  /** Claim a single job or return null. */
  claimOne(workerId: string): Job | null {
    return this.claimBatch(workerId, 1)[0] ?? null;
  }

  /** Ack multiple job ids in one transaction. Returns count acked. */
  ackBatch(ids: number[], workerId: string): number {
    const json = JSON.stringify(ids);
    const row = this.db.raw
      .query<{ v: number }, [string, string]>(
        "SELECT honker_ack_batch(?, ?) AS v",
      )
      .get(json, workerId)!;
    return row.v;
  }

  /** Sweep expired claim rows back to pending. Returns rows touched. */
  sweepExpired(): number {
    const row = this.db.raw
      .query<{ v: number }, [string]>("SELECT honker_sweep_expired(?) AS v")
      .get(this.name)!;
    return row.v;
  }

  /**
   * Returns a ClaimWaker that polls for jobs with a short timer.
   * Pass 1: poll-based (no WAL waker yet).
   */
  claimWaker(opts: { pollMs?: number } = {}): ClaimWaker {
    return new ClaimWaker(this, opts.pollMs ?? 100);
  }
}

/** A claimed unit of work. */
export class Job {
  readonly id: number;
  readonly queue: string;
  readonly payload: unknown;
  readonly workerId: string;
  readonly attempts: number;

  constructor(
    private readonly db: Database,
    row: RawJob,
  ) {
    this.id = row.id;
    this.queue = row.queue;
    this.payload = JSON.parse(row.payload);
    this.workerId = row.worker_id;
    this.attempts = row.attempts;
  }

  /** DELETE the row if the claim is still valid. */
  ack(): boolean {
    return (
      this.db.raw
        .query<{ v: number }, [number, string]>("SELECT honker_ack(?, ?) AS v")
        .get(this.id, this.workerId)!.v > 0
    );
  }

  /** Put the job back with a delay, or move to dead after maxAttempts. */
  retry(delaySec: number, errorMsg: string): boolean {
    return (
      this.db.raw
        .query<{ v: number }, [number, string, number, string]>(
          "SELECT honker_retry(?, ?, ?, ?) AS v",
        )
        .get(this.id, this.workerId, delaySec, errorMsg)!.v > 0
    );
  }

  /** Unconditionally move to dead. */
  fail(errorMsg: string): boolean {
    return (
      this.db.raw
        .query<{ v: number }, [number, string, string]>(
          "SELECT honker_fail(?, ?, ?) AS v",
        )
        .get(this.id, this.workerId, errorMsg)!.v > 0
    );
  }

  /** Extend the visibility timeout. */
  heartbeat(extendSec: number): boolean {
    return (
      this.db.raw
        .query<{ v: number }, [number, string, number]>(
          "SELECT honker_heartbeat(?, ?, ?) AS v",
        )
        .get(this.id, this.workerId, extendSec)!.v > 0
    );
  }
}

/**
 * Claim waker. Call `next(workerId)` to block on an incoming job;
 * poll-based (Pass 1).
 */
export class ClaimWaker {
  private stopped = false;

  constructor(
    private readonly queue: Queue,
    private readonly pollMs: number,
  ) {}

  /** Claim one job without blocking. Null if the queue is empty. */
  tryNext(workerId: string): Job | null {
    return this.queue.claimOne(workerId);
  }

  /**
   * Block until a job is claimable; returns the claimed job. Returns
   * null if stopped via `close()`. Honors `signal` (AbortSignal).
   */
  async next(
    workerId: string,
    opts: { signal?: AbortSignal } = {},
  ): Promise<Job | null> {
    while (!this.stopped && !opts.signal?.aborted) {
      const job = this.queue.claimOne(workerId);
      if (job) return job;
      await sleep(this.pollMs, opts.signal);
    }
    return null;
  }

  /** Stop any in-flight `next()` calls (they resolve to null). */
  close(): void {
    this.stopped = true;
  }
}

// ---------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------

export class Stream {
  constructor(
    private readonly db: Database,
    public readonly topic: string,
  ) {}

  /** Publish an event. Returns the assigned offset. */
  publish(payload: unknown): number {
    return this.publishImpl(null, payload, this.db.raw);
  }

  /** Publish with a partition key for per-key ordering downstream. */
  publishWithKey(key: string, payload: unknown): number {
    return this.publishImpl(key, payload, this.db.raw);
  }

  /** Publish inside an open transaction. */
  publishTx(tx: Transaction, payload: unknown, key: string | null = null): number {
    return this.publishImpl(key, payload, tx.raw);
  }

  private publishImpl(
    key: string | null,
    payload: unknown,
    conn: BunDB,
  ): number {
    const json = JSON.stringify(payload);
    const row = conn
      .query<{ v: number }, [string, string | null, string]>(
        "SELECT honker_stream_publish(?, ?, ?) AS v",
      )
      .get(this.topic, key, json)!;
    return row.v;
  }

  /** Read up to `limit` events with offset strictly greater than `offset`. */
  readSince(offset: number, limit: number): StreamEvent[] {
    const row = this.db.raw
      .query<{ v: string }, [string, number, number]>(
        "SELECT honker_stream_read_since(?, ?, ?) AS v",
      )
      .get(this.topic, offset, limit)!;
    const raw: RawStreamEvent[] = JSON.parse(row.v);
    return raw.map(toStreamEvent);
  }

  /** Read from the saved offset of `consumer`. Does not advance the offset. */
  readFromConsumer(consumer: string, limit: number): StreamEvent[] {
    return this.readSince(this.getOffset(consumer), limit);
  }

  /** Monotonic: saving a lower offset is a no-op. Returns true if saved. */
  saveOffset(consumer: string, offset: number): boolean {
    return this.saveOffsetImpl(consumer, offset, this.db.raw);
  }

  /** Save offset inside an open transaction. */
  saveOffsetTx(tx: Transaction, consumer: string, offset: number): boolean {
    return this.saveOffsetImpl(consumer, offset, tx.raw);
  }

  private saveOffsetImpl(
    consumer: string,
    offset: number,
    conn: BunDB,
  ): boolean {
    const row = conn
      .query<{ v: number }, [string, string, number]>(
        "SELECT honker_stream_save_offset(?, ?, ?) AS v",
      )
      .get(consumer, this.topic, offset)!;
    return row.v > 0;
  }

  /** Current saved offset for `consumer`, or 0 if never saved. */
  getOffset(consumer: string): number {
    const row = this.db.raw
      .query<{ v: number | null }, [string, string]>(
        "SELECT honker_stream_get_offset(?, ?) AS v",
      )
      .get(consumer, this.topic)!;
    return row.v ?? 0;
  }

  /**
   * Subscribe as a named consumer. Resumes from saved offset, polls
   * for new events, auto-saves offset every `saveEveryN` events
   * (default 1000) and on return/throw.
   *
   * Cancel with `signal` (AbortSignal) or by `break`-ing the iterator.
   */
  subscribe(
    consumer: string,
    opts: {
      saveEveryN?: number;
      pollMs?: number;
      signal?: AbortSignal;
    } = {},
  ): AsyncIterableIterator<StreamEvent> {
    return subscribeImpl(
      this,
      consumer,
      opts.saveEveryN ?? 1000,
      opts.pollMs ?? 100,
      opts.signal,
    );
  }
}

function toStreamEvent(r: RawStreamEvent): StreamEvent {
  return {
    offset: r.offset,
    topic: r.topic,
    key: r.key,
    payload: r.payload == null ? null : JSON.parse(r.payload),
    created_at: r.created_at,
  };
}

async function* subscribeImpl(
  stream: Stream,
  consumer: string,
  saveEveryN: number,
  pollMs: number,
  signal?: AbortSignal,
): AsyncIterableIterator<StreamEvent> {
  let lastOffset = stream.getOffset(consumer);
  let lastSaved = lastOffset;
  const flush = () => {
    if (saveEveryN > 0 && lastOffset > lastSaved) {
      stream.saveOffset(consumer, lastOffset);
      lastSaved = lastOffset;
    }
  };
  try {
    while (!signal?.aborted) {
      const events = stream.readSince(lastOffset, 100);
      if (events.length === 0) {
        await sleep(pollMs, signal);
        continue;
      }
      for (const ev of events) {
        yield ev;
        lastOffset = ev.offset;
        if (saveEveryN > 0 && lastOffset - lastSaved >= saveEveryN) {
          stream.saveOffset(consumer, lastOffset);
          lastSaved = lastOffset;
        }
      }
    }
  } finally {
    flush();
  }
}

// ---------------------------------------------------------------------
// Pub/sub listen (poll-based Pass 1)
// ---------------------------------------------------------------------

async function* listenImpl(
  db: Database,
  channel: string,
  signal: AbortSignal | undefined,
  pollMs: number,
): AsyncIterableIterator<Notification> {
  const startId = db.raw
    .query<{ v: number }, []>(
      "SELECT COALESCE(MAX(id), 0) AS v FROM _honker_notifications",
    )
    .get()!.v;
  let lastId = startId;
  const stmt = db.raw.query<
    { id: number; channel: string; payload: string },
    [number, string]
  >(
    "SELECT id, channel, payload FROM _honker_notifications " +
      "WHERE id > ? AND channel = ? ORDER BY id ASC LIMIT 1000",
  );
  while (!signal?.aborted) {
    const rows = stmt.all(lastId, channel);
    if (rows.length === 0) {
      await sleep(pollMs, signal);
      continue;
    }
    for (const r of rows) {
      lastId = r.id;
      yield {
        id: r.id,
        channel: r.channel,
        payload: r.payload == null ? null : JSON.parse(r.payload),
      };
    }
  }
}

// ---------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------

const SCHEDULER_LOCK = "honker-scheduler";
const SCHEDULER_LOCK_TTL_S = 60;
const SCHEDULER_HEARTBEAT_MS = 20_000;
const SCHEDULER_TICK_MS = 1_000;
const SCHEDULER_STANDBY_MS = 5_000;

export class Scheduler {
  constructor(private readonly db: Database) {}

  /** Register a cron task. Idempotent by name. */
  add(task: ScheduledTask): void {
    const payloadJson = JSON.stringify(task.payload);
    this.db.raw
      .query<
        { v: number },
        [string, string, string, string, number, number | null]
      >(
        "SELECT honker_scheduler_register(?, ?, ?, ?, ?, ?) AS v",
      )
      .get(
        task.name,
        task.queue,
        task.cron,
        payloadJson,
        task.priority ?? 0,
        task.expiresS ?? null,
      );
  }

  /** Unregister a task by name. Returns rows deleted. */
  remove(name: string): number {
    const row = this.db.raw
      .query<{ v: number }, [string]>(
        "SELECT honker_scheduler_unregister(?) AS v",
      )
      .get(name)!;
    return row.v;
  }

  /** Fire any due boundaries and return what was enqueued. */
  tick(): ScheduledFire[] {
    const now = Math.floor(Date.now() / 1000);
    const row = this.db.raw
      .query<{ v: string }, [number]>(
        "SELECT honker_scheduler_tick(?) AS v",
      )
      .get(now)!;
    return JSON.parse(row.v) as ScheduledFire[];
  }

  /** Soonest `next_fire_at` across all tasks, or 0 if none. */
  soonest(): number {
    const row = this.db.raw
      .query<{ v: number | null }, []>("SELECT honker_scheduler_soonest() AS v")
      .get()!;
    return row.v ?? 0;
  }

  /**
   * Leader-elected blocking loop. Only the process holding the
   * `honker-scheduler` lock ticks. Refreshes TTL every 20s; if the
   * refresh returns false (lock stolen), exits the leader loop and
   * re-contests. On tick error, releases the lock and throws.
   *
   * Pass `signal` (AbortSignal) to shut down cleanly.
   */
  async run(opts: { owner: string; signal: AbortSignal }): Promise<void> {
    const { owner, signal } = opts;
    while (!signal.aborted) {
      const lock = this.db.tryLock(
        SCHEDULER_LOCK,
        owner,
        SCHEDULER_LOCK_TTL_S,
      );
      if (!lock) {
        await sleep(SCHEDULER_STANDBY_MS, signal);
        continue;
      }
      try {
        await this.leaderLoop(lock, signal);
      } catch (err) {
        // release then rethrow so a standby can take over immediately
        lock.release();
        throw err;
      }
      // Normal exit (stopped or lost the lock) — release if still held.
      lock.release();
    }
  }

  private async leaderLoop(lock: Lock, signal: AbortSignal): Promise<void> {
    let lastHeartbeat = Date.now();
    while (!signal.aborted) {
      this.tick();
      const now = Date.now();
      if (now - lastHeartbeat >= SCHEDULER_HEARTBEAT_MS) {
        const stillOurs = lock.heartbeat(SCHEDULER_LOCK_TTL_S);
        if (!stillOurs) return; // lost — exit leader loop, re-contest
        lastHeartbeat = now;
      }
      await sleep(SCHEDULER_TICK_MS, signal);
    }
  }
}

// ---------------------------------------------------------------------
// Locks
// ---------------------------------------------------------------------

// Best-effort release on GC. Holds closed-over primitives + raw BunDB
// so the Lock instance itself is collectable.
const lockFinalizer = new FinalizationRegistry<{
  raw: BunDB;
  name: string;
  owner: string;
  releasedRef: { released: boolean };
}>((held) => {
  if (held.releasedRef.released) return;
  try {
    held.raw
      .query("SELECT honker_lock_release(?, ?)")
      .get(held.name, held.owner);
  } catch {
    // Finalizer may fire during interpreter teardown.
  }
});

export class Lock {
  private releasedRef = { released: false };

  constructor(
    private readonly db: Database,
    public readonly name: string,
    public readonly owner: string,
  ) {
    lockFinalizer.register(
      this,
      {
        raw: db.raw,
        name,
        owner,
        releasedRef: this.releasedRef,
      },
      this,
    );
  }

  /**
   * Release the lock. Idempotent — calling twice returns false on the
   * second call. First call returns whether we actually still held it.
   */
  release(): boolean {
    if (this.releasedRef.released) return false;
    this.releasedRef.released = true;
    lockFinalizer.unregister(this);
    const row = this.db.raw
      .query<{ v: number }, [string, string]>(
        "SELECT honker_lock_release(?, ?) AS v",
      )
      .get(this.name, this.owner)!;
    return row.v > 0;
  }

  /** True once `release()` has been called on this handle. */
  get released(): boolean {
    return this.releasedRef.released;
  }

  /**
   * Extend the TTL. Returns true if we still own it, false if it was
   * stolen (TTL elapsed and another owner acquired it).
   *
   * The extension's `honker_lock_acquire` uses `INSERT OR IGNORE`, so
   * a same-owner re-acquire doesn't actually extend the row. We UPDATE
   * directly, matching the Elixir binding.
   */
  heartbeat(ttlS: number): boolean {
    const stmt = this.db.raw.query<
      unknown,
      [number, string, string]
    >(
      "UPDATE _honker_locks SET expires_at = unixepoch() + ? " +
        "WHERE name = ? AND owner = ?",
    );
    stmt.run(ttlS, this.name, this.owner);
    return this.db.raw.query<{ c: number }, []>(
      "SELECT changes() AS c",
    ).get()!.c > 0;
  }
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) {
      resolve();
      return;
    }
    const t = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(t);
      resolve();
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
