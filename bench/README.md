# Benchmark

Ad hoc publish/consume throughput benchmark for `pg_blazingmq`, mirroring
[`pgnats`](https://github.com/mrayva/pgnats)'s own
`scripts/nats_publish_from_sql.py` methodology (same "publish rate" /
"receive rate" definitions - see that script's docstring) for a fair
side-by-side comparison against NATS core, one message per row, both
msgpack.

## Quick Start

Requires a running broker (`../test/manage_broker.sh start`) and
`pg_blazingmq`/`pg_zerialize` both `CREATE EXTENSION`'d, plus `psycopg` (v3):

```bash
python3 bmq_bench.py \
    --sql 'SELECT * FROM nyse_eqy_us_all_trade_20260102' \
    --limit 100000
```

`--attr-columns` (default `Sequence Number,Trade Id`) picks which columns
get promoted to filterable message properties via `bmq_publish_row`'s
`attr_columns`, matching the amount of per-row work `nats_publish_from_sql.py`
does building its NATS subject from 2 real columns - keeps the two
benchmarks doing comparable per-row work, not just comparable payloads.

## What It Measures

- **Publish rate**: pure database-side timing (encode + `bmq_publish_row()`),
  wrapped in a single `SELECT count(*) FROM (...)` so no row crosses back to
  the client - directly comparable to `nats_publish_from_sql.py`'s own
  publish-rate definition.
- **Receive rate**: wall-clock time around one `bmq_consume()` call pulling
  everything back. Note this isn't measured the same way as pgnats'
  receive rate (which polls `nats_tool`'s periodic stats log, with a ~1s
  granularity floor) - `bmq_consume()` is a synchronous pull call that
  blocks until it has `max_messages` or times out, so timing it directly
  is both the natural fit for its API shape and strictly more precise than
  polling a once-per-second log line. Keep this difference in mind when
  comparing the two numbers.
- **Verify**: republishes the same rows to a second queue, consumes them
  back, decodes via `msgpack_to_jsonb`, and compares against a
  `to_jsonb()` reference projection as an unordered multiset (same
  content-not-position philosophy as pgnats' own `--verify`).

## A Real Broker-Config Gotcha

The domain configs BlazingMQ's own `docker/single-node/config` ships (also
what `../test/manage_broker.sh`'s scratch broker uses for `make test`) cap
`bmq.test.mem.priority` at `queueLimits.messages: 1000` - sized for
`pg_regress`'s own handful of test rows, not a 100k-row throughput run.
Publishing past that limit doesn't error - the broker just silently stops
storing further messages for that queue (confirmed via the broker's own
log: `CAPACITY_STATE_FULL`), so `bmq_consume()` afterward returns fewer
rows than were "published" without ever raising an error. Either raise
`domainLimits`/`queueLimits` in a copy of that domain config for large
runs, or keep `--limit` under the configured ceiling.

## Sample Result (100,000 rows, `nyse_eqy_us_all_trade_20260102`, msgpack, single machine)

| system                          | rows    | avg bytes | publish rate | receive rate |
|----------------------------------|---------|-----------|---------------|---------------|
| pg_blazingmq (BlazingMQ, priority)  | 100,000 | 357B      | 58,319/s      | 30,653/s      |
| pg_blazingmq (BlazingMQ, broadcast) | 100,000 | 356B      | 57,755/s      | 73,294/s      |
| pgnats (NATS core)               | 100,000 | 356B      | 69,925/s      | 221,868/s     |

Both `bmq.test.mem.priority` and `bmq.test.mem.broadcast` (this domain's
own config, `docker/single-node/config/domains/bmq.test.mem.broadcast.json`
in the BlazingMQ checkout) use `inMemory` storage and `eventual`
consistency - so this is **not** a persisted-vs-non-persisted comparison;
both BlazingMQ modes here are already non-durable, same as NATS core.

Switching from priority mode to broadcast mode (fire-and-forget best-effort
fan-out, no per-consumer positional backlog tracking) left publish rate
essentially unchanged (~58k/s either way - publish-side cost is dominated
by encoding and the publish protocol itself, not consumer-side bookkeeping)
but raised receive rate ~2.4x (30,653/s -> 73,294/s). That confirms
priority mode's per-consumer positional queue bookkeeping was a real,
measurable contributor to the original gap - but a large gap to NATS
core's 221,868/s receive rate remains even under broadcast mode. The
likely remaining driver is BlazingMQ's per-message application-level
acknowledgment protocol (`session.confirmMessage()`, still required in
both queue modes for at-least-once semantics) plus general wire-protocol
and broker-side richness (subscription property evaluation via `bmqeval`,
watermark/limit accounting) that NATS core's minimal fire-and-forget
protocol simply doesn't do. That's the real tradeoff `pg_blazingmq` is
for - durability options and BlazingMQ's own server-side subscription
filtering - not raw throughput parity with a minimal core pub/sub system.

(Broadcast-mode queues only deliver to consumers already connected at
publish time, unlike priority mode's queued pull-consumption for
not-yet-connected readers - this benchmark's single-session, one-handle-
per-queue-URI design in `pg_blazingmq` already opens the queue with
combined read+write flags on the very first `bmq_publish_row()` call, so
the read side is established before any message is published and this
script needed no changes to work correctly under broadcast mode; verified
via a 50-row dry run before the full 100k run.)
