#!/usr/bin/env python3
"""Ad hoc publish/consume benchmark for pg_blazingmq, mirroring pgnats'
scripts/nats_publish_from_sql.py methodology for a fair side-by-side
comparison: "publish rate" is a pure database-side measurement (encode +
bmq_publish_row(), timed around one SQL statement, no client round trip
per row); "receive rate" times a single bmq_consume() call pulling
max_messages back, then verifies content as an unordered multiset against
a jsonb reference projection of the same source rows (via msgpack_to_jsonb).

The verify step's column normalization (see `norm` below) is written
against the nyse_eqy_us_all_trade_* fixture schema used in this repo's own
benchmarking (14 named columns, one float8) - point --sql at a different
table and adjust `norm` to match its columns before trusting --verify's
PASS/FAIL for that table. Publish/consume timing itself has no such
dependency.

Requires a target queue's BlazingMQ domain to actually hold --limit
messages: the domain configs BlazingMQ's own docker/single-node/config
ships (also what test/manage_broker.sh's scratch broker uses) cap
bmq.test.mem.priority at queueLimits.messages=1000 - a benchmark run past
that either needs a higher-capacity domain config or a smaller --limit.
See ../README.md's "Testing" section for the scratch broker itself.
"""
import argparse
import time
import psycopg
import psycopg.sql

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="host=/var/run/postgresql dbname=postgres")
    ap.add_argument("--sql", required=True, help="base SELECT (no LIMIT)")
    ap.add_argument("--limit", type=int, default=100000)
    ap.add_argument("--attr-columns", default="Sequence Number,Trade Id")
    ap.add_argument("--queue-uri", default="bmq://bmq.test.mem.priority/bmq_bench")
    ap.add_argument("--broker-uri", default="tcp://localhost:30114")
    ap.add_argument("--consume-timeout-ms", type=int, default=60000)
    args = ap.parse_args()

    attr_cols = [c.strip() for c in args.attr_columns.split(",")]
    attr_array_sql = "ARRAY[" + ",".join(f"'{c}'" for c in attr_cols) + "]"

    with psycopg.connect(args.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET blazingmq.broker_uri = {psycopg.sql.Literal(args.broker_uri).as_string(cur)}")

            # Materialize the row set ONCE, unordered (cheap - stops at the
            # first N rows of a seq scan). base_sql gets read multiple times
            # below (publish, byte-size measurement, verify reference); an
            # ORDER BY to force two separate `SELECT ... LIMIT N` executions
            # to agree would force a full sort of a 115M-row table on every
            # read (confirmed the hard way: publish rate collapsed from
            # ~29,000/s to 127/s with ORDER BY "Sequence Number" LIMIT N
            # applied at the timed publish call) - a one-time materialization
            # into a temp table is both cheap and exactly reproducible on
            # every subsequent read, without that cost.
            cur.execute(f"CREATE TEMP TABLE bmq_bench_source AS {args.sql} LIMIT {args.limit}")
            base_sql = "bmq_bench_source"

            # --- Publish: pure DB-side timing, no rows returned to client ---
            publish_sql = (
                f"SELECT count(*) FROM "
                f"(SELECT bmq_publish_row(%s, t, {attr_array_sql}) "
                f"FROM {base_sql} t) s"
            )
            t0 = time.perf_counter()
            cur.execute(publish_sql, (args.queue_uri,))
            published = cur.fetchone()[0]
            publish_secs = time.perf_counter() - t0
            print(f"published {published} row(s) in {publish_secs:.3f}s "
                  f"({published/publish_secs:,.0f}/s)")

            # avg payload bytes, measured server-side separately (doesn't
            # affect publish timing above)
            cur.execute(
                f"SELECT avg(octet_length(row_to_msgpack(t)))::float8, "
                f"sum(octet_length(row_to_msgpack(t)))::float8 FROM {base_sql} t"
            )
            avg_bytes, total_bytes = cur.fetchone()

            # --- Receive: time a single bmq_consume() pulling everything back ---
            consume_sql = "SELECT count(*) FROM bmq_consume(%s, NULL, %s, %s)"
            t0 = time.perf_counter()
            cur.execute(consume_sql, (args.queue_uri, args.limit, args.consume_timeout_ms))
            received = cur.fetchone()[0]
            receive_secs = time.perf_counter() - t0
            print(f"received {received} row(s) in {receive_secs:.3f}s "
                  f"({received/receive_secs:,.0f}/s)" if receive_secs > 0 else
                  f"received {received} row(s) instantly")

            print(f"avg payload bytes: {avg_bytes:.1f}  total: {total_bytes/1e6:.1f}MB")

            # --- Verify: re-publish+consume a small fresh sample, decode via
            # msgpack_to_jsonb, and compare as an unordered multiset against
            # to_jsonb(source row) - same content-not-position philosophy as
            # pgnats' own --verify. (Uses a separate queue so it doesn't
            # collide with the publish/receive queue above.)
            verify_uri = args.queue_uri + "_verify"
            cur.execute(
                f"SELECT bmq_publish_row(%s, t, {attr_array_sql}) FROM {base_sql} t",
                (verify_uri,),
            )
            # Normalize "Trade Price" (float8) through an explicit float8
            # cast before comparing: to_jsonb() prints the shortest
            # round-trip decimal for a double (e.g. "27.89"), while
            # msgpack_to_jsonb prints the double's full decimal expansion
            # (e.g. "27.960000000000001") - both parse back to the exact
            # same IEEE754 double, but differ as jsonb *numeric* literals,
            # so raw jsonb equality on the untouched objects spuriously
            # flags every row. All other columns here are text/bigint,
            # with no such representation ambiguity.
            norm = (
                "jsonb_build_object("
                "'Time', j->>'Time', 'Exchange', j->>'Exchange', 'Symbol', j->>'Symbol', "
                "'Sale Condition', j->>'Sale Condition', "
                "'Trade Volume', (j->>'Trade Volume')::bigint, "
                "'Trade Price', (j->>'Trade Price')::float8, "
                "'Trade Stop Stock Indicator', j->>'Trade Stop Stock Indicator', "
                "'Trade Correction Indicator', j->>'Trade Correction Indicator', "
                "'Sequence Number', (j->>'Sequence Number')::bigint, "
                "'Trade Id', (j->>'Trade Id')::bigint, "
                "'Source of Trade', j->>'Source of Trade', "
                "'Trade Reporting Facility', j->>'Trade Reporting Facility', "
                "'Participant Timestamp', j->>'Participant Timestamp', "
                "'Trade Reporting Facility TRF Timestamp', j->>'Trade Reporting Facility TRF Timestamp', "
                "'Trade Through Exempt Indicator', (j->>'Trade Through Exempt Indicator')::bigint)"
            )
            cur.execute(
                "WITH consumed AS ("
                "  SELECT msgpack_to_jsonb(bmq_consume) AS j"
                "  FROM bmq_consume(%s, NULL, %s, %s)"
                "), reference AS ("
                f"  SELECT to_jsonb(t) AS j FROM {base_sql} t"
                f"), c_grp AS (SELECT {norm} AS j, count(*) c FROM consumed GROUP BY 1),"
                f"     r_grp AS (SELECT {norm} AS j, count(*) c FROM reference GROUP BY 1)"
                "SELECT (SELECT count(*) FROM consumed), (SELECT count(*) FROM reference),"
                "       (SELECT count(*) FROM (SELECT * FROM c_grp EXCEPT SELECT * FROM r_grp) d1),"
                "       (SELECT count(*) FROM (SELECT * FROM r_grp EXCEPT SELECT * FROM c_grp) d2)",
                (verify_uri, args.limit, args.consume_timeout_ms),
            )
            c_count, r_count, d1, d2 = cur.fetchone()
            verify_ok = (c_count == r_count == args.limit) and d1 == 0 and d2 == 0
            print(f"verify: consumed={c_count} reference={r_count} "
                  f"mismatched_groups={d1+d2} -> {'PASS' if verify_ok else 'FAIL'}")

if __name__ == "__main__":
    main()
