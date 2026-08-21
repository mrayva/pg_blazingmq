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

## Status: Phase 1 (proof of linkage)

`pg_blazingmq_link_check()` is the only function so far. It constructs a
real `bmqa::Session` (without calling `start()`, so no live broker is
needed) to prove the extension's `.so` actually links and loads inside a
Postgres backend against the full BDE/NTF/bmq dependency chain. No
publish/consume functionality yet - see the phased plan below.

```sql
CREATE EXTENSION pg_blazingmq;
SELECT pg_blazingmq_link_check('tcp://localhost:30114');
--                            pg_blazingmq_link_check
-- ------------------------------------------------------------------------------
--  pg_blazingmq link OK: brokerUri=tcp://localhost:30114 numProcessingThreads=1
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

## Plan

See the project's own notes for the full phased plan:

1. **Build/link line** (this repo, in progress) - proof-of-linkage against
   the full BDE/NTF/bmq client stack.
2. **Publish path** - `bmq_publish_row(queue_uri, row, attr_columns)`:
   column→property mapping, `pg_zerialize`-packed payload.
3. **Pull-consume** - `bmq_consume(queue_uri, subscription_expr, ...)`.
4. **Push-consume** - background worker + SPI callback dispatch, mirroring
   `pgnats`'s `nats_subscribe(subject, fn_oid)`.
5. **Tests** - `pg_regress` suite against a real single-node broker.
6. **Docs**.
