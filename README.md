<h1 align="center">
  <img src="assets/honker-logo.png" width="120" alt="" /><br/>honker
</h1>

`honker` is a SQLite extension + language bindings that add Postgres-style `NOTIFY`/`LISTEN` semantics to SQLite, with built-in durable pub/sub, task queue, and event streams, without client polling or a daemon/broker. Any language that can `SELECT load_extension('honker')` gets the same features.

honker ships as a [Rust crate](https://crates.io/crates/honker) (`honker`, plus `honker-core`/`honker-extension`), a [SQLite loadable extension](#sqlite-extension-any-sqlite-39-client), and language packages: Python (`honker`), Node (`@honker/node`), Ruby (`honker`), Go, Bun, Elixir. The on-disk layout is defined once in Rust; every binding is a thin wrapper around the loadable extension.

`honker` works by storing queues, notifications, streams, locks, and
scheduler state in the same SQLite file as your application data. This
Bun binding is currently a thin extension wrapper; its async iterators
use short timer polling until it grows the shared update watcher used by
the Rust, Python, Node, Go, and C++ bindings.

> Experimental. API may change.

SQLite is increasingly the database for shipped projects. Those inevitably require pubsub and a task queue. The usual answer is "add Redis + Celery." That works, but it introduces a second datastore with its own backup story, a dual-write problem between your business table and the queue, and the operational overhead of running a broker.

honker takes the approach that if SQLite is the primary datastore, the queue should live in the same file. That means `INSERT INTO orders` and `queue.enqueue(...)` commit in the same transaction. Rollback drops both. The queue is just rows in a table with a partial index.

Prior art:  [`pg_notify`](https://www.postgresql.org/docs/current/sql-notify.html) (fast triggers, no retry/visibility), [Huey](https://github.com/coleifer/huey) (SQLite-backed Python), [pg-boss](https://github.com/timgit/pg-boss) and [Oban](https://github.com/sorentwo/oban) (the Postgres-side gold standards we're chasing on SQLite). If you already run Postgres, use those, as they are excellent.

## At a glance

```python
import honker

db = honker.open("app.db")
emails = db.queue("emails")

# Enqueue
emails.enqueue({"to": "alice@example.com"})

# Consume (worker process)
async for job in emails.claim("worker-1"):
    send(job.payload)
    job.ack()
```

Any enqueue can be atomic with a business write. Rollback drops both.

```python
with db.transaction() as tx:
    tx.execute("INSERT INTO orders (user_id) VALUES (?)", [42])
    emails.enqueue({"to": "alice@example.com"}, tx=tx)
```

## Features

Today:

- Notify/listen across processes on one `.db` file
- Work queues with retries, priority, delayed jobs, and a dead-letter table
- Any send can be atomic with your business write (commit together or roll back together)
- Cross-process coordination without a broker; Bun async iterators are
  poll-based today
- Handler timeouts, declarative retries with exponential backoff
- Delayed jobs, task expiration, named locks, rate-limiting
- Time-trigger scheduling with a leader-elected scheduler
- Opt-in task result storage (`enqueue` returns an id, worker persists the
  return value, caller awaits `queue.wait_result(id)`)
- Durable streams with per-consumer offsets and configurable flush interval
- SQLite loadable extension so any SQLite client can read the same tables
- Bindings: Python, Node.js, Rust, Go, Ruby, Bun, Elixir

Deliberately not built: task pipelines/chains/groups/chords, multi-writer replication, workflow orchestration with DAGs.

## Quick start 

### Python: queue (durable at-least-once work)

```bash
pip install honker
```

```python
import honker
db = honker.open("app.db")
emails = db.queue("emails")

with db.transaction() as tx:
    tx.execute("INSERT INTO orders (user_id) VALUES (?)", [42])
    emails.enqueue({"to": "alice@example.com"}, tx=tx)   # atomic with order

# Then in a worker, do: 
async for job in emails.claim("worker-1"):               # wakes on any database update
    try:
        send(job.payload); job.ack()
    except Exception as e:
        job.retry(delay_s=60, error=str(e))
```

`claim()` is an async iterator. Each iteration is one `claim_batch(worker_id, 1)`. In Bun it polls on a short interval; the shared update watcher is still future work for this binding. For batched work, call `claim_batch(worker_id, n)` explicitly and ack with `queue.ack_batch(ids, worker_id)`. Defaults: visibility 300 s.

### Python: tasks (Huey-style decorators)

If you want a function call to turn into an enqueued job without wrapping `queue.enqueue` by hand:

```python
@emails.task(retries=3, timeout_s=30)
def send_email(to: str, subject: str) -> dict:
    ...
    return {"sent_at": time.time()}

# Caller
r = send_email("alice@example.com", "Hi")   # enqueues, returns a TaskResult
print(r.get(timeout=10))                    # blocks until worker runs it
```

Worker side, either in-process or as its own process:

```bash
python -m honker worker myapp.tasks:db --queue=emails --concurrency=4
```

Auto-name is `{module}.{qualname}` (Huey/Celery convention). Explicit names with `@emails.task(name="...")` are recommended in prod so renames don't orphan pending jobs. Periodic tasks use `@emails.periodic_task(crontab("0 3 * * *"))`. Full details in [`packages/honker/examples/tasks.py`](packages/honker/examples/tasks.py).

### Python: stream (durable pub/sub)

```python
stream = db.stream("user-events")

with db.transaction() as tx:
    tx.execute("UPDATE users SET name=? WHERE id=?", [name, uid])
    stream.publish({"user_id": uid, "change": "name"}, tx=tx)

async for event in stream.subscribe(consumer="dashboard"):
    await push_to_browser(event)
```

Each named consumer tracks its own offset in the `_honker_stream_consumers` table. `subscribe` replays rows past the saved offset, then polls for newly committed rows. The iterator auto-saves offset at most every 1000 events or every 1 second (whichever first) so a high-throughput stream doesn't hammer the single-writer slot. Override with `save_every_n=` / `save_every_s=`, or set both to 0 to disable auto-save and call `stream.save_offset(consumer, offset, tx=tx)` yourself (atomic with whatever you just did in that tx). At-least-once: a crash re-delivers in-flight events up to the last flushed offset.

### Python: notify (ephemeral pub/sub)

```python
async for n in db.listen("orders"):
    print(n.channel, n.payload)

with db.transaction() as tx:
    tx.execute("INSERT INTO orders (id, total) VALUES (?, ?)", [42, 99.99])
    tx.notify("orders", {"id": 42})
```

Listeners attach at current `MAX(id)`; history is not replayed. Use `db.stream()` if you need durable replay. The notifications table is not auto-pruned. Call `db.prune_notifications(older_than_s=…, max_keep=…)` from a scheduled task. Task payloads have to be valid JSON so a Python writer and Node reader can share a channel.

### Bindings with update events

```js
const lit = require('@honker/node');
const db = lit.open('app.db');
const tx = db.transaction();
tx.execute('INSERT INTO orders (id) VALUES (?)', [42]);
tx.notify('orders', { id: 42 });
tx.commit();

const ev = db.updateEvents();
let last = 0;
while (running) {
  await ev.next();
  const rows = db.query(
    'SELECT id, payload FROM _honker_notifications WHERE id > ? ORDER BY id', [last]);
  for (const r of rows) { handle(JSON.parse(r.payload)); last = r.id; }
}
```

### SQLite extension (any SQLite 3.9+ client)

```sql
.load ./libhonker_ext
SELECT honker_bootstrap();
INSERT INTO _honker_live (queue, payload) VALUES ('emails', '{"to":"alice"}');
SELECT honker_claim_batch('emails', 'worker-1', 32, 300);    -- JSON array
SELECT honker_ack_batch('[1,2,3]', 'worker-1');              -- DELETEs; returns count
SELECT honker_sweep_expired('emails');                       -- count moved to dead
SELECT honker_lock_acquire('backup', 'me', 60);              -- 1 = got it, 0 = held
SELECT honker_lock_release('backup', 'me');                  -- 1 = released
SELECT honker_rate_limit_try('api', 10, 60);                 -- 1 = under, 0 = at limit
SELECT honker_rate_limit_sweep(3600);                        -- drop windows >1h old
SELECT honker_cron_next_after('0 3 * * *', unixepoch());     -- unix ts of next fire
SELECT honker_scheduler_register('nightly', 'backups',
  '@every 1s', '"go"', 0, NULL);                            -- register recurring schedule
SELECT honker_scheduler_tick(unixepoch());                   -- JSON: fires due
SELECT honker_scheduler_soonest();                           -- min next_fire_at
SELECT honker_scheduler_unregister('nightly');               -- 1 = deleted
SELECT honker_stream_publish('orders', 'k', '{"id":42}');    -- returns offset
SELECT honker_stream_read_since('orders', 0, 1000);          -- JSON array
SELECT honker_stream_save_offset('worker', 'orders', 42);    -- monotonic upsert
SELECT honker_stream_get_offset('worker', 'orders');         -- offset or 0
SELECT honker_result_save(42, '{"ok":true}', 3600);          -- save w/ 1h TTL
SELECT honker_result_get(42);                                -- value or NULL
SELECT honker_result_sweep();                                -- prune expired
SELECT notify('orders', '{"id":42}');
```

The extension shares `_honker_live`, `_honker_dead`, and `_honker_notifications` with the Python binding, so a Python worker can claim jobs any other language pushed via the extension. Schema compatibility is pinned by `tests/test_extension_interop.py`.

## Design

This repo includes the `honker` SQLite loadable extension and bindings for Python, Node, Rust, Go, Ruby, Bun, and Elixir. 

For most applications, [SQLite alone is sufficient](https://www.epicweb.dev/why-you-should-probably-be-using-sqlite). There are already great libraries that leverage SQLite for durable messaging. [Huey](https://github.com/coleifer/huey) is the one honker draws the most from. This project is inspired by it and seeks to do something similar across languages and frameworks by moving package logic into a SQLite extension.

For Postgres-backed apps, [`pg_notify`](https://www.postgresql.org/docs/current/sql-notify.html) + [pg-boss](https://github.com/timgit/pg-boss) or [Oban](https://hexdocs.pm/oban/) is the equivalent. This library is for apps where SQLite is the primary datastore.

The extension has three primitives that tie it together: ephemeral pub/sub (`notify()`), durable pub/sub with per-consumer offsets (`stream()`), at-least-once work queue (`queue()`). All three are INSERTs inside your transaction, which lets a task "send" be atomic with your business write, and rollback drops everything.

The explicit goal is to do `NOTIFY`/`LISTEN` semantics without constant polling, to achieve single-digit ms reaction time. If you use your app's existing SQLite file containing business logic, it will notify workers on every commit. This means that most triggers will not result in anything happening: instead, workers just read the message/queue with no result. This "overtriggering" is on purpose and is the tradeoff for push semantics and fast reaction time.

### WAL is the recommended default

The language bindings default to `journal_mode = WAL` because it gives concurrent readers with one writer and efficient fsync batching (`wal_autocheckpoint = 10000`). Other modes (DELETE, TRUNCATE, MEMORY) still work. The shared update watcher used by the Rust, Python, Node, Go, and C++ bindings polls `PRAGMA data_version`, which increments on every commit in every journal mode and is visible across processes. Bun does not use that watcher yet; its async iterators poll their target tables directly.

The library/extension is a small coordination layer built on the properties of SQLite and single-server architecture.

- One `.db` is the entire system (plus `.db-wal` / `.db-shm` sidecars if you've opted into WAL). You get every benefit of SQLite (embedded, local, durable, snapshot-able) that your app already uses.
- Claim is one `UPDATE … RETURNING` via a partial index; ack is one `DELETE`. One writer at a time no matter the journal mode; concurrent readers come with WAL.
- Rust, Python, Node, Go, and C++ poll `PRAGMA data_version` every 1 ms to detect commits from any connection in any journal mode. Bun's pass-1 implementation polls target tables on a short timer.
- SQLite has no wire protocol. Consumers must initiate reads; server-push is impossible. Wake signal = counter increment → `SELECT`.
- Transactions are cheap, so jobs, events, and notifications are rows in the caller's open `with db.transaction()` block in an "outbox"-type pattern.
- Single machine, single writer. SQLite's locking is designed for a single host. Two servers writing one `.db` over NFS will corrupt it. Shard by file, or switch to Postgres.

## Architecture

### Wake path

- Rust, Python, Node, Go, and C++ use one PRAGMA-poll update watcher per `Database`
- Bun's pass-1 async iterators poll their target table on a short timer
- Each subscriber runs `SELECT … WHERE id > last_seen` against a partial index, yields rows, returns to wait

For the bindings with an update watcher, idle cost is a single `PRAGMA data_version` query per millisecond per database. Listener count scales for free because the wake signal is a SQLite counter read instead of a polling query.

`SharedUpdateWatcher` (in `honker-core`) owns the poll thread for the Rust/Python/Node path and fans out to N subscribers via bounded `SyncSender<()>` channels keyed by subscriber id. Bun still needs a bridge to that watcher.

### Queue schema

- `_honker_live`: pending + processing rows
- Partial index: `(queue, priority DESC, run_at, id) WHERE state IN ('pending','processing')`
- Claim = one `UPDATE … RETURNING` via that index
- Ack = one `DELETE`
- Retry-exhausted → `_honker_dead` (never scanned by claim path)

Partial-index on state means the claim hot path is bounded by the *working-set* size rather than the *history* size. A queue with 100k dead rows claims as fast as a queue with zero.

### Claim iterator

- `async for job in q.claim(id)` yields one job at a time via `claim_batch(id, 1)`
- `Job.ack()` is one `DELETE` in its own transaction. Return is an honest bool: `True` iff the claim was still valid, `False` if the visibility window elapsed and another worker reclaimed.
- Bun polls for claimable work on a short interval.

For batched work, call `claim_batch(worker_id, n)` directly and ack with `queue.ack_batch(ids, worker_id)`. The library doesn't hide batching behind the iterator. The per-tx cost and the at-most-once visibility semantics are easier to reason about when the API doesn't try to be clever.

### Transactional coupling

- `notify()` is a SQL scalar function registered on the writer connection
- INSERTs into `_honker_notifications` under the caller's open tx
- `queue.enqueue(…, tx=tx)` and `stream.publish(…, tx=tx)` do the same
- Rollback drops the job/event/notification with the rest of the tx

This is the transactional outbox pattern, by default, without a library to install. Business write and side-effect enqueue commit or roll back together. There is no separate dispatch table and no separate dispatcher process: the side-effect row *is* the committed row, and any process watching the WAL picks it up within ~1 ms.

### Over-triggering quickly is better than over-triggering from polling

- A database update wakes *every* subscriber on that `Database`, not just the ones whose channel committed
- Each wasted wake = one indexed SELECT (microseconds)
- A missed wake = a silent correctness bug

The library prefers waking ten listeners that don't care over missing one that does. Channel filtering happens in the `SELECT` path instead of the trigger notification. [Many small queries are efficient in SQLite](https://www.sqlite.org/np1queryprob.html).

### Retention

- Queue jobs persist until ack; retry-exhausted rows move to `_honker_dead`
- Stream events persist; each named consumer tracks its own offset
- Notify is fire-and-forget and not auto-pruned

The caller chooses retention per primitive. `db.prune_notifications(older_than_s=…, max_keep=…)` is a tool you invoke. This keeps retention policy visible in the caller's code instead of inherited from a library default.

## Crash recovery

- Rollback drops jobs/events/notifications with your business write (SQLite ACID).
- SIGKILL mid-tx is safe. WAL rollback on next open leaves no stale state. Verified in `tests/test_crash_recovery.py` (subprocess killed pre-COMMIT, `PRAGMA integrity_check == 'ok'`, fresh notifies still flow).
- If a worker crashes mid-job, the claim expires after `visibility_timeout_s` (default 300 s) and another worker reclaims. `attempts` increments. After `max_attempts` (default 3), the row moves to `_honker_dead`.
- Listeners offline during a prune miss the pruned events. For durable replay, use `db.stream()`, which tracks per-consumer offsets.

## Wiring into your web framework

Honker ships no framework plugins. API is small and the integration is a few lines of glue:

```python
# FastAPI: enqueue in a request, run workers via lifespan.
@app.on_event("startup")
async def _start_workers():
    async def worker_loop():
        async for job in db.queue("emails").claim("worker"):
            await honker._worker.run_task(
                job, send_email, timeout=30, retries=3, backoff=2.0
            )
    app.state._worker = asyncio.create_task(worker_loop())

@app.post("/orders")
async def create_order(order: dict):
    with db.transaction() as tx:
        tx.execute("INSERT INTO orders (user_id) VALUES (?)", [order["user_id"]])
        db.queue("emails").enqueue({"to": order["email"]}, tx=tx)
    return {"ok": True}
```

SSE endpoints are ~30 lines of `async def stream(...): yield f"data: ...\n\n"` over `db.listen(channel)` or `db.stream(name).subscribe(...)`. For Django/Flask, run the worker as a dedicated CLI process (same pattern as Celery/RQ).

## Performance

Handles thousands of messages per second on a modern laptop, with cross-process wake latency bounded by the 1 ms update-watcher cadence (~1–2 ms median on M-series). Run `bench/wake_latency_bench.py` and `bench/real_bench.py` to measure on your hardware.

## Development

Layout:

```
honker-core/              # Rust rlib shared across all bindings (in-tree, published on crates.io)
honker-extension/         # SQLite loadable extension (cdylib, published on crates.io)
packages/
  honker/                 # Python package (PyO3 cdylib + Queue/Stream/Outbox/Scheduler)
  honker-node/            # napi-rs Node.js binding           [git submodule]
  honker-rs/              # ergonomic Rust wrapper            [git submodule]
  honker-go/              # Go binding                        [git submodule]
  honker-ruby/            # Ruby binding                      [git submodule]
  honker-bun/             # Bun binding                       [git submodule]
  honker-ex/              # Elixir binding                    [git submodule]
tests/                    # integration tests (cross-package)
bench/                    # benches
site/                     # honker.dev (Astro)                [git submodule]
```

Each binding repo is published independently (PyPI / npm / crates.io / Hex / RubyGems) and pinned here as a git submodule; `honker-core` + `honker-extension` live in-tree since they're the shared foundation every binding depends on. Clone with `git clone --recursive` or run `git submodule update --init --recursive` after a normal clone.

```bash
make test                   # default: rust + python + node (fast, ~10s)
make test-python-slow       # soak + real-time cron tests (~2 min)
make test-all               # everything including slow marks

make build                  # PyO3 maturin develop + loadable extension

python bench/wake_latency_bench.py --samples 500
python bench/real_bench.py --workers 4 --enqueuers 2 --seconds 15
python bench/ext_bench.py
```

### Coverage

One-time: `make install-coverage-deps` (installs `coverage.py` + `cargo-llvm-cov`).

```bash
make coverage               # both HTML reports into coverage/
make coverage-python        # honker python paths
make coverage-rust          # honker-core Rust unit tests
```

Python coverage reflects the full honker test suite (~92% of `packages/honker/`). Rust coverage reflects only `cargo test`. Many `honker_ops.rs` paths (`honker_enqueue`, `honker_claim_batch`, etc.) are only exercised via the Python test suite and won't show up in the Rust report. Combined cross-language coverage is non-trivial (LLVM profile-data merging across PyO3 boundaries) and is deferred.

## License

Apache 2.0. See [LICENSE](LICENSE).
