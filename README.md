# pg_blazingmq

A PostgreSQL extension for publishing/consuming [BlazingMQ](https://github.com/bloomberg/blazingmq)
messages directly from SQL, in the same spirit as this project's sibling
extensions `pg_zerialize` (binary row (de)serialization) and `nats_sidecar`
(content-based NATS filtering via a-tree/be-tree).

BlazingMQ's own `bmqeval` subscription-expression engine does server-side,
per-message property filtering natively (`region == 1 && active`, evaluated
by the broker against named message properties) — no external matching
engine needed, unlike NATS core subjects. `pg_blazingmq`'s job is to make
Postgres rows speak that: promote chosen columns to typed message properties
(bool/short/int32/int64/string - BlazingMQ's `MessageProperties` has no
float/double type, and no list-membership operator in its expression
grammar), and pack the full row as the message payload via `pg_zerialize`.

## Status: Phase 5 (tests)

`pg_blazingmq_link_check()` (Phase 1) constructs a real `bmqa::Session`
(without calling `start()`, so no live broker is needed) to prove the
extension's `.so` actually links and loads inside a Postgres backend
against the full BDE/NTF/bmq dependency chain.

`bmq_publish_row(queue_uri, row_data, attr_columns)` (Phase 2) publishes a
row to a BlazingMQ queue. The full row is always packed as the message
payload (msgpack, via a vendored copy of `zerialize`); `attr_columns`
selects which columns are *also* promoted to typed `bmqa::MessageProperties`
for server-side subscriber filtering via BlazingMQ's own `bmqeval`
expression language. Only `bool`/`int2`/`int4`/`int8`/text-family columns
can be attributes - naming an ineligible column (e.g. a `float8`) is a
clear error, raised before any network call. If `attr_columns` is omitted,
every eligible column is promoted automatically.

`bmq_consume(queue_uri, subscription_expr, max_messages, timeout_ms)`
(Phase 3) synchronously pulls up to `max_messages` payloads, waiting up to
`timeout_ms` total. Each received message is confirmed immediately - no
separate ack step in this first cut. `subscription_expr`, if given, is
BlazingMQ's own `bmqeval` expression, applied server-side: only matching
messages are delivered to this handle at all.

The session is held per-backend (lazily started on first use, stopped via
`on_proc_exit`). Queue handles are cached per URI for the backend's
lifetime - **one handle per (session, queue URI), always opened with
combined READ+WRITE flags**, discovered the hard way: BlazingMQ rejects
opening the same URI twice from one session even with different flags
(`ALREADY_OPENED`), so publish and consume against the same queue in the
same backend must share one handle. If a later `bmq_consume()` call asks
for a different `subscription_expr` than the cached handle currently has,
the handle is reconfigured in place (`configureQueueSync`) rather than
erroring - this is what makes "publish, then consume with a filter" work
within one session. Broker address is `blazingmq.broker_uri` (GUC,
defaults to `tcp://localhost:30114`).

**Important semantic to know**: BlazingMQ evaluates a subscription's
filter at message-*arrival* time against whatever handles/filters are
active *then* - it is not retroactive. If you reconfigure (or open) a
filtered read handle *after* messages have already been published to that
queue, those already-published messages were queued for delivery under
the *old* filter and will still arrive unfiltered. Establish the
subscription you want before publishing the messages you want filtered by
it, not after.

`bmq_subscribe(queue_uri, callback_fn, subscription_expr)` (Phase 4)
registers a dedicated background worker that stays subscribed indefinitely,
calling `callback_fn(payload bytea)` for every message received. Returns
the worker's PID, which doubles as the subscription handle for
`bmq_unsubscribe(worker_pid)`. Unlike `bmq_consume()`, delivery is
at-least-once by design: each message runs in its own transaction, and is
only confirmed *after* `callback_fn` returns successfully - if it raises
an error, that transaction rolls back, a `WARNING` is logged, and the
message is left unconfirmed for BlazingMQ to redeliver.

The worker is registered dynamically (`RegisterDynamicBackgroundWorker`)
with its one-time config (queue URI, callback OID, subscription
expression, target database/role) handed off via a pinned Dynamic Shared
Memory segment - deliberately not a `shmem_request_hook`-managed
structure, which would require `shared_preload_libraries` plus a server
restart. This needs neither: `bmq_subscribe()` works immediately after
`CREATE EXTENSION`, no restart. The worker uses the *database's* default
`blazingmq.broker_uri` (`ALTER DATABASE ... SET`), not the calling
session's - a session-local `SET` doesn't propagate to it, since the
worker is a separate OS process with its own GUC state. There's no
separate subscription registry either: every background worker already
shows up in `pg_stat_activity` with `backend_type` set to `'pg_blazingmq
subscriber'`, which is exactly what `bmq_unsubscribe()` checks before
signaling a PID, so it can't be used to terminate arbitrary processes.

`bmq_subscribe()` doesn't return until the worker has actually opened its
read queue, not merely until its OS process has started - `Wait
ForBackgroundWorkerStartup()` alone only guarantees the latter, and given
the non-retroactive filtering semantic above, a message published in the
gap between "process started" and "queue actually open" would be delivered
to no one. The worker signals readiness through an atomic flag in the same
DSM segment used for the initial config handoff; `bmq_subscribe()` polls it
(bounded, 5s) before returning. This is best-effort, not a hard guarantee -
a worker that's still slow to connect after 5s (e.g. broker unreachable)
still gets its PID back, with a `WARNING` instead of a hard failure, since
it keeps running and retrying independently either way.

```sql
CREATE EXTENSION pg_blazingmq;
SELECT pg_blazingmq_link_check('tcp://localhost:30114');
--                            pg_blazingmq_link_check
-- ------------------------------------------------------------------------------
--  pg_blazingmq link OK: brokerUri=tcp://localhost:30114 numProcessingThreads=1

CREATE TABLE trades (region int, symbol text, price float8, active bool);
CREATE TABLE received_messages (region int, symbol text, price float8, active bool);

CREATE FUNCTION handle_trade(payload bytea) RETURNS void AS $$
DECLARE j jsonb;
BEGIN
  j := msgpack_to_jsonb(payload);  -- pg_zerialize decodes the payload
  INSERT INTO received_messages (region, symbol, price, active)
  VALUES ((j->>'region')::int, j->>'symbol', (j->>'price')::float8, (j->>'active')::bool);
END;
$$ LANGUAGE plpgsql;

-- Workers use the database's broker_uri, not a session-local SET.
ALTER DATABASE mydb SET blazingmq.broker_uri = 'tcp://localhost:30114';

SELECT bmq_subscribe('bmq://bmq.test.priority/trades', 'handle_trade'::regproc) AS worker_pid;
--  worker_pid
-- ------------
--      793808

SET blazingmq.broker_uri = 'tcp://localhost:30114';  -- for this session's own publish call
INSERT INTO trades VALUES (1, 'AAPL', 150.25, true), (2, 'MSFT', 305.5, false);
SELECT bmq_publish_row('bmq://bmq.test.priority/trades', trades, ARRAY['region']) FROM trades;

-- moments later, asynchronously, with no further action from this session:
SELECT * FROM received_messages;
--  region | symbol | price  | active
-- --------+--------+--------+--------
--       1 | AAPL   | 150.25 | t
--       2 | MSFT   |  305.5 | f

SELECT bmq_unsubscribe(793808);  -- stops the worker cleanly
```

## Building

Requires a BlazingMQ checkout already built via its own
`bin/build-ubuntu.sh`, **with position-independent code enabled** - the
default build doesn't have this (BDE/NTF/bmq are normally only linked into
executables), and a Postgres extension `.so` needs every static library it
pulls in to have been compiled with `-fPIC`. Concretely, from your
BlazingMQ checkout:

```bash
# BDE, with a pic-enabled UFID
eval "$(thirdparty/bde-tools/bin/bbs_build_env -u opt_64_cpp23_pic -b build/bde -i "$(pwd)")"
cd thirdparty/bde && bbs_build configure --prefix="$(pwd)/../.." && \
  bbs_build build --prefix="$(pwd)/../.." && \
  bbs_build install --install_dir="/" --prefix="$(pwd)/../.." && cd -

# NTF, same UFID
cd thirdparty/ntf-core && ./configure --prefix "$(pwd)/../.." --output build/ntf \
  --without-warnings-as-errors --without-usage-examples --without-applications \
  --with-zlib --without-zstd --without-lz4 --ufid opt_64_pic_cpp23 && \
  make -j"$(nproc)" && make install && cd -

# bmq client group only (mqb/broker-internals aren't needed by a client)
export DIR_THIRDPARTY="$(pwd)/thirdparty" DIR_BUILD="$(pwd)/build" DIR_INSTALL="$(pwd)"
export PATH="$(pwd)/thirdparty/bde-tools/bin:$PATH"
cmake --preset ubuntu-x64 -DCMAKE_POSITION_INDEPENDENT_CODE=ON
cd build/blazingmq && ninja libbmqa.a libbmqc.a libbmqeval.a libbmqex.a \
  libbmqimp.a libbmqio.a libbmqma.a libbmqp.a libbmqpi.a libbmqscm.a \
  libbmqst.a libbmqstm.a libbmqt.a libbmqtsk.a libbmqu.a libbmqvt.a libbmq.a
```

Then:

```bash
make BMQ_ROOT=/path/to/your/blazingmq/checkout
sudo make install BMQ_ROOT=/path/to/your/blazingmq/checkout
```

`BMQ_ROOT` defaults to `$HOME/blazingmq`.

To test against a real broker, also rebuild the broker/tool executables
after the PIC reconfigure above (a `cmake --preset` reconfigure followed by
building only the `bmq` group's libraries, as shown, does not relink
anything that was already built under the old, non-PIC configuration):

```bash
cd build/blazingmq && ninja bmqbrkr.tsk bmqtool.tsk
```

## Testing

`make test` is the one-command entry point: starts a scratch single-node
broker (`test/manage_broker.sh`, reusing BlazingMQ's own
`docker/single-node/config`, patched to a local `test/.broker_scratch/`
data dir instead of `/var/local/bmq`), runs `make installcheck`, then always
stops the broker afterward - even on failure, so a failing run doesn't leak
a background broker process. Requires `pg_blazingmq` already `make
install`'d and the broker/tool executables built (see Building above).

`REGRESS = 01_link_check 02_publish_row 03_consume 04_subscribe` covers all
four phases plus `bmq_subscribe`/`bmq_unsubscribe` lifecycle management. A
few things worth knowing if you're reading or extending these:

- Each `sql/*.sql` file is its own fresh `psql` connection under
  `pg_regress`, so the per-backend session/queue-handle cache starts clean
  per file - matching what Phases 3/4 already assume.
- `04_subscribe.sql`'s subscribe/publish/verify/unsubscribe sequence runs
  inside one `DO $$ ... $$` block: `bmq_subscribe()` returns a real PID,
  which isn't reproducible across runs, so it must never leak into a query
  result `pg_regress` diffs - only into `RAISE` messages under the test's
  own control, or not at all on success.
- If a test fails *inside* that `DO` block before reaching
  `bmq_unsubscribe()`, the subscriber worker it started is left running,
  holding a connection open to `contrib_regression` - the next
  `make installcheck` run's `DROP DATABASE` will then fail with "database
  is being accessed by other users". Recover with
  `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE
  backend_type = 'pg_blazingmq subscriber';` before retrying.

## Plan

See the project's own notes for the full phased plan:

1. **Build/link line** (done) - proof-of-linkage against the full
   BDE/NTF/bmq client stack.
2. **Publish path** (done) - `bmq_publish_row(queue_uri, row_data,
   attr_columns)`: column→property mapping, zerialize-packed payload.
3. **Pull-consume** (done) - `bmq_consume(queue_uri, subscription_expr,
   max_messages, timeout_ms)`.
4. **Push-consume** (done) - `bmq_subscribe(queue_uri, callback_fn,
   subscription_expr)` / `bmq_unsubscribe(worker_pid)`: dynamic background
   worker + DSM config handoff + per-message SPI callback dispatch,
   mirroring `pgnats`'s `nats_subscribe(subject, fn_oid)`.
5. **Tests** (done) - `pg_regress` suite against a real single-node broker,
   `make test` as the one-command entry point (see Testing above).
6. **Docs** (done) - see Changelog and Maintained Documentation below.

## Changelog

Each entry corresponds to one `pg_blazingmq--X.Y.sql` version; see those
files for the exact functions each version added. This extension hasn't
reached 1.0 yet - versions below that should be considered unstable.

- **0.4** -- Added push-consume: `bmq_subscribe(queue_uri, callback_fn,
  subscription_expr)` / `bmq_unsubscribe(worker_pid)`. A dynamic
  background worker per subscription, config handed off via a pinned DSM
  segment, an atomic readiness handshake closing the race between "worker
  process started" and "worker's queue is actually open", and
  per-message SPI transaction dispatch with at-least-once delivery.
- **0.3** -- Added pull-consume: `bmq_consume(queue_uri, subscription_expr,
  max_messages, timeout_ms)`. Unified the publish/consume queue-handle
  cache into one handle per URI with combined READ+WRITE flags, after
  discovering BlazingMQ rejects opening the same URI twice from one
  session.
- **0.2** -- Added `bmq_publish_row(queue_uri, row_data, attr_columns)`:
  column-to-property promotion for server-side filtering, plus
  zerialize-packed payloads.
- **0.1** -- Initial release: `pg_blazingmq_link_check(broker_uri)`,
  proof-of-linkage against the full BDE/NTF/bmq client stack.

(Phase 5's test suite added no new SQL surface, so it didn't bump the
version.)

## Maintained Documentation

- [`QUICKSTART.md`](QUICKSTART.md): install, build, and first usage
- [`ARCHITECTURE.md`](ARCHITECTURE.md): session/queue lifecycle, the
  column-to-property/payload split, and the DSM/background-worker design
  behind push-consume
