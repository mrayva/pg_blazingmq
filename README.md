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

## Status: Phase 2 (publish path)

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

The session is held per-backend (lazily started on first use, stopped via
`on_proc_exit`); queue write-handles are cached per URI for the backend's
lifetime. Broker address is `blazingmq.broker_uri` (GUC, defaults to
`tcp://localhost:30114`).

```sql
CREATE EXTENSION pg_blazingmq;
SELECT pg_blazingmq_link_check('tcp://localhost:30114');
--                            pg_blazingmq_link_check
-- ------------------------------------------------------------------------------
--  pg_blazingmq link OK: brokerUri=tcp://localhost:30114 numProcessingThreads=1

CREATE TABLE trades (region int, symbol text, price float8, active bool);
INSERT INTO trades VALUES (1, 'AAPL', 150.25, true);

SET blazingmq.broker_uri = 'tcp://localhost:30114';
SELECT bmq_publish_row('bmq://bmq.test.priority/trades', trades, ARRAY['region'])
FROM trades;
-- delivered payload (msgpack): {region: 1, symbol: "AAPL", price: 150.25, active: true}
-- delivered properties:        region (INT32) = 1  -- subscribers can filter on "region == 1"
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

## Plan

See the project's own notes for the full phased plan:

1. **Build/link line** (done) - proof-of-linkage against the full
   BDE/NTF/bmq client stack.
2. **Publish path** (done) - `bmq_publish_row(queue_uri, row_data,
   attr_columns)`: column→property mapping, zerialize-packed payload.
3. **Pull-consume** - `bmq_consume(queue_uri, subscription_expr, ...)`.
4. **Push-consume** - background worker + SPI callback dispatch, mirroring
   `pgnats`'s `nats_subscribe(subject, fn_oid)`.
5. **Tests** - `pg_regress` suite against a real single-node broker.
6. **Docs**.
