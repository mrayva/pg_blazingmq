#!/usr/bin/env python3
"""Filtered (bmqeval subscription_expr) variant of sustained_bench.py.

Every other benchmark script in this directory calls bmq_consume() with
subscription_expr=NULL - none of them have ever exercised BlazingMQ's
native bmqeval server-side filtering under load. This script gives each
consumer its own distinct subscription_expr (e.g. "texi == 0" vs
"texi == 1"), matching the real shape a nats_sidecar-equivalent use case
would have: multiple downstream consumers each interested in a different
content subset of the same stream. Uses the exact same bisection/barrier/
backlog-curve-as-evidence methodology as sustained_bench.py - see that
file's docstring for why (undrained-backlog cost scaling, why priority
mode not broadcast, why no early-exit on an empty poll).

bmqeval does not accept quoted/spaced identifiers (confirmed empirically:
'"Trade Through Exempt Indicator" == 0' fails validation with "expression
does not use any properties" even though the *column* name itself
publishes fine as a property) - the source SQL below aliases the split
column to a plain identifier before publishing, purely so the property
name reaching bmqeval has no spaces to worry about; unrelated to the
question this script tests.

--filter-values, if given, must have exactly --consumers entries (one
per consumer, assigned by index) and makes each consumer's bmq_consume()
call use f"{filter_column} == {value}" (value written as-is, so quote
string values yourself, e.g. --filter-values "'D','Q'"). Omit
--filter-values for an unfiltered run (subscription_expr=NULL,
consumer). Both modes are supported so the same script gives a directly
comparable filtered-vs-unfiltered A/B run.
"""
import argparse
import multiprocessing as mp
import random
import time

import psycopg
import psycopg.sql


def execute_with_open_retry(cur, sql, params, max_attempts=8):
    for attempt in range(max_attempts):
        try:
            cur.execute(sql, params)
            return
        except psycopg.errors.InternalError_ as e:
            if "rc=100" in str(e) and attempt < max_attempts - 1:
                time.sleep(0.05 * (attempt + 1) + random.uniform(0, 0.05))
                continue
            raise


def producer(args, worker_id, stop_time, counter, barrier):
    dsn = args.dsn
    attr_array_sql = f"ARRAY['Sequence Number','Trade Id','{args.filter_column}']"
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET blazingmq.broker_uri = {psycopg.sql.Literal(args.broker_uri).as_string(cur)}")
            cur.execute(f"CREATE TEMP TABLE prod_src AS {args.sql} LIMIT {args.batch_rows}")
            publish_sql = (
                f"SELECT count(*) FROM "
                f"(SELECT bmq_publish_row(%s, t, {attr_array_sql}) FROM prod_src t) s"
            )
            execute_with_open_retry(
                cur,
                f"SELECT bmq_publish_row(%s, t, {attr_array_sql}) FROM prod_src t LIMIT 1",
                (args.queue_uri,),
            )
            barrier.wait()
            local_published = 0
            worker_start = time.perf_counter()
            while time.perf_counter() < stop_time:
                execute_with_open_retry(cur, publish_sql, (args.queue_uri,))
                n = cur.fetchone()[0]
                local_published += n
                with counter.get_lock():
                    counter.value += n
                if args.producer_rate_limit > 0:
                    target_elapsed = local_published / (args.producer_rate_limit / args.producers)
                    actual_elapsed = time.perf_counter() - worker_start
                    if target_elapsed > actual_elapsed:
                        time.sleep(target_elapsed - actual_elapsed)
            return local_published


def consumer(args, worker_id, hard_stop_time, counter, barrier, filter_expr, verify_counter):
    dsn = args.dsn
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET blazingmq.broker_uri = {psycopg.sql.Literal(args.broker_uri).as_string(cur)}")
            execute_with_open_retry(
                cur,
                "SELECT count(*) FROM bmq_consume(%s, %s, 1, 50, true)",
                (args.queue_uri, filter_expr),
            )
            barrier.wait()
            local_consumed = 0
            local_mismatch = 0
            while time.perf_counter() < hard_stop_time:
                if args.verify_content and filter_expr is not None:
                    cur.execute(
                        "SELECT msgpack_to_jsonb(bmq_consume)->>%s "
                        "FROM bmq_consume(%s, %s, %s, %s, true)",
                        (args.filter_column, args.queue_uri, filter_expr,
                         args.consume_batch, args.consume_timeout_ms),
                    )
                    rows = cur.fetchall()
                    n = len(rows)
                    expected = args.expected_value_by_worker[worker_id]
                    for (v,) in rows:
                        if str(v) != str(expected):
                            local_mismatch += 1
                else:
                    cur.execute(
                        "SELECT count(*) FROM bmq_consume(%s, %s, %s, %s, true)",
                        (args.queue_uri, filter_expr, args.consume_batch, args.consume_timeout_ms),
                    )
                    n = cur.fetchone()[0]
                local_consumed += n
                with counter.get_lock():
                    counter.value += n
            if local_mismatch:
                with verify_counter.get_lock():
                    verify_counter.value += local_mismatch
            return local_consumed


def monitor(published_counter, consumed_counter, stop_time, sample_interval, samples):
    while time.perf_counter() < stop_time:
        time.sleep(sample_interval)
        p = published_counter.value
        c = consumed_counter.value
        t = time.perf_counter()
        samples.append((t, p, c, p - c))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="host=/var/run/postgresql dbname=postgres")
    ap.add_argument("--sql", required=True, help="base query; must alias the split column to --filter-column")
    ap.add_argument("--filter-column", default="texi")
    ap.add_argument("--filter-values", default=None,
                     help="comma-separated, one per consumer, e.g. \"0,1\" - omit for unfiltered")
    ap.add_argument("--producers", type=int, required=True)
    ap.add_argument("--consumers", type=int, required=True)
    ap.add_argument("--batch-rows", type=int, default=2000)
    ap.add_argument("--consume-batch", type=int, default=2000)
    ap.add_argument("--consume-timeout-ms", type=int, default=200)
    ap.add_argument("--duration-secs", type=float, default=30.0)
    ap.add_argument("--ramp-up-secs", type=float, default=8.0)
    ap.add_argument("--drain-tail-secs", type=float, default=10.0)
    ap.add_argument("--sample-interval-secs", type=float, default=2.0)
    ap.add_argument("--producer-rate-limit", type=float, default=0)
    ap.add_argument("--queue-uri", default="bmq://bmq.test.mem.priority/filtered_bench")
    ap.add_argument("--broker-uri", default="tcp://localhost:30114")
    ap.add_argument("--verify-content", action="store_true")
    args = ap.parse_args()

    if args.filter_values:
        values = args.filter_values.split(",")
        assert len(values) == args.consumers, "--filter-values must have exactly --consumers entries"
        filter_exprs = [f"{args.filter_column} == {v}" for v in values]
        args.expected_value_by_worker = {i: v.strip("'\"") for i, v in enumerate(values)}
    else:
        filter_exprs = [None] * args.consumers
        args.expected_value_by_worker = {}

    n_workers = args.producers + args.consumers
    barrier = mp.Barrier(n_workers)
    published_counter = mp.Value("l", 0)
    consumed_counter = mp.Value("l", 0)
    verify_mismatch_counter = mp.Value("l", 0)
    manager = mp.Manager()
    samples = manager.list()

    t_now = time.perf_counter()
    start_time = t_now + 1.0
    stop_time = start_time + args.duration_secs
    hard_stop_time = stop_time + args.drain_tail_secs
    steady_start = start_time + args.ramp_up_secs

    procs = []
    pub_result_q = mp.Queue()
    con_result_q = mp.Queue()

    def run_producer(i):
        n = producer(args, i, stop_time, published_counter, barrier)
        pub_result_q.put(n)

    def run_consumer(i):
        n = consumer(args, i, hard_stop_time, consumed_counter, barrier, filter_exprs[i], verify_mismatch_counter)
        con_result_q.put(n)

    for i in range(args.producers):
        p = mp.Process(target=run_producer, args=(i,))
        p.start()
        procs.append(p)
    for i in range(args.consumers):
        p = mp.Process(target=run_consumer, args=(i,))
        p.start()
        procs.append(p)

    mon = mp.Process(target=monitor, args=(published_counter, consumed_counter, hard_stop_time, args.sample_interval_secs, samples))
    mon.start()

    for p in procs:
        p.join()
    mon.join()

    total_published = sum(pub_result_q.get() for _ in range(args.producers))
    total_consumed = sum(con_result_q.get() for _ in range(args.consumers))

    print(f"producers={args.producers} consumers={args.consumers} filters={filter_exprs}")
    print(f"total_published={total_published} total_consumed={total_consumed} "
          f"final_backlog={total_published-total_consumed}")
    if args.verify_content and args.filter_values:
        print(f"content_verify_mismatches={verify_mismatch_counter.value}")
    print("backlog curve (t_since_start, published_so_far, consumed_so_far, backlog):")
    for (t, p, c, b) in samples:
        print(f"  t={t-start_time:6.1f}s  published={p:>9} consumed={c:>9} backlog={b:>7}")

    in_window = [(t, p, c) for (t, p, c, b) in samples if steady_start <= t <= stop_time]
    if len(in_window) >= 2:
        t0, p0, c0 = in_window[0]
        t1, p1, c1 = in_window[-1]
        span = t1 - t0
        pub_rate = (p1 - p0) / span if span > 0 else 0
        con_rate = (c1 - c0) / span if span > 0 else 0
        max_backlog = max(p - c for (t, p, c) in in_window)
        min_backlog = min(p - c for (t, p, c) in in_window)
        print(f"\nsteady-state window: t={t0-start_time:.1f}s..{t1-start_time:.1f}s (span={span:.1f}s)")
        print(f"steady-state publish rate: {pub_rate:,.0f}/s")
        print(f"steady-state consume (sustained) rate: {con_rate:,.0f}/s")
        print(f"backlog in steady-state window: min={min_backlog} max={max_backlog}")
    else:
        print("\nnot enough samples in steady-state window")


if __name__ == "__main__":
    main()
