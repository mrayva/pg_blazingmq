#!/usr/bin/env python3
"""Multi-queue sustained-throughput benchmark for pg_blazingmq.

sustained_bench.py answers "what's the sustained rate for ONE queue with
M consumers competing on it" (answer: consumer count on a single queue
doesn't help - bench/README.md's consumer-scaling sections). This script
tests the *other* scaling axis BlazingMQ's own published benchmarks
(https://bloomberg.github.io/blazingmq/docs/performance/benchmarks/) use:
K independent queues, each with its own dedicated 1 producer + 1 consumer
pair, all running concurrently. Same backlog-bounded-is-the-evidence
methodology as sustained_bench.py, just replicated per queue instead of
per consumer.

Each producer/consumer pair gets its own queue URI
(f"{base}_{queue_idx}"), its own publish/consume counters, and the
monitor samples every queue's backlog independently so "aggregate looks
fine" can't hide one queue quietly growing unbounded while others shrink.
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


def producer(args, qidx, queue_uri, per_producer_rate, stop_time, pub_counters, barrier):
    attr_array_sql = "ARRAY['Sequence Number','Trade Id']"
    with psycopg.connect(args.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SET blazingmq.broker_uri = {psycopg.sql.Literal(args.broker_uri).as_string(cur)}"
            )
            cur.execute(
                f"CREATE TEMP TABLE prod_src AS {args.sql} LIMIT {args.batch_rows}"
            )
            publish_sql = (
                f"SELECT count(*) FROM "
                f"(SELECT bmq_publish_row(%s, t, {attr_array_sql}) FROM prod_src t) s"
            )
            execute_with_open_retry(
                cur,
                f"SELECT bmq_publish_row(%s, t, {attr_array_sql}) FROM prod_src t LIMIT 1",
                (queue_uri,),
            )
            barrier.wait()
            local_published = 0
            worker_start = time.perf_counter()
            while time.perf_counter() < stop_time:
                execute_with_open_retry(cur, publish_sql, (queue_uri,))
                n = cur.fetchone()[0]
                local_published += n
                with pub_counters[qidx].get_lock():
                    pub_counters[qidx].value += n
                if per_producer_rate > 0:
                    target_elapsed = local_published / per_producer_rate
                    actual_elapsed = time.perf_counter() - worker_start
                    if target_elapsed > actual_elapsed:
                        time.sleep(target_elapsed - actual_elapsed)
            return local_published


def consumer(args, qidx, queue_uri, hard_stop_time, con_counters, barrier):
    with psycopg.connect(args.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SET blazingmq.broker_uri = {psycopg.sql.Literal(args.broker_uri).as_string(cur)}"
            )
            execute_with_open_retry(
                cur,
                "SELECT count(*) FROM bmq_consume(%s, NULL, 1, 50, true)",
                (queue_uri,),
            )
            barrier.wait()
            local_consumed = 0
            while time.perf_counter() < hard_stop_time:
                cur.execute(
                    "SELECT count(*) FROM bmq_consume(%s, NULL, %s, %s, true)",
                    (queue_uri, args.consume_batch, args.consume_timeout_ms),
                )
                n = cur.fetchone()[0]
                local_consumed += n
                with con_counters[qidx].get_lock():
                    con_counters[qidx].value += n
            return local_consumed


def monitor(pub_counters, con_counters, k, stop_time, sample_interval, samples):
    while time.perf_counter() < stop_time:
        time.sleep(sample_interval)
        t = time.perf_counter()
        row = [(pub_counters[i].value, con_counters[i].value) for i in range(k)]
        samples.append((t, row))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="host=/var/run/postgresql dbname=postgres")
    ap.add_argument("--sql", required=True)
    ap.add_argument("--k-queues", type=int, required=True)
    ap.add_argument("--per-queue-rate", type=float, default=42000,
                     help="target rows/sec for EACH queue's single producer, 0=unthrottled")
    ap.add_argument("--batch-rows", type=int, default=2000)
    ap.add_argument("--consume-batch", type=int, default=2000)
    ap.add_argument("--consume-timeout-ms", type=int, default=200)
    ap.add_argument("--duration-secs", type=float, default=30.0)
    ap.add_argument("--ramp-up-secs", type=float, default=8.0)
    ap.add_argument("--drain-tail-secs", type=float, default=12.0)
    ap.add_argument("--sample-interval-secs", type=float, default=2.0)
    ap.add_argument("--queue-uri-base", default="bmq://bmq.test.mem.priority/mqbench")
    ap.add_argument("--broker-uri", default="tcp://localhost:30114")
    args = ap.parse_args()

    k = args.k_queues
    queue_uris = [f"{args.queue_uri_base}_{i}" for i in range(k)]
    n_workers = 2 * k
    barrier = mp.Barrier(n_workers)
    pub_counters = [mp.Value("l", 0) for _ in range(k)]
    con_counters = [mp.Value("l", 0) for _ in range(k)]
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
        n = producer(args, i, queue_uris[i], args.per_queue_rate, stop_time, pub_counters, barrier)
        pub_result_q.put((i, n))

    def run_consumer(i):
        n = consumer(args, i, queue_uris[i], hard_stop_time, con_counters, barrier)
        con_result_q.put((i, n))

    for i in range(k):
        p = mp.Process(target=run_producer, args=(i,))
        p.start()
        procs.append(p)
    for i in range(k):
        p = mp.Process(target=run_consumer, args=(i,))
        p.start()
        procs.append(p)

    mon = mp.Process(target=monitor, args=(pub_counters, con_counters, k, hard_stop_time, args.sample_interval_secs, samples))
    mon.start()

    for p in procs:
        p.join()
    mon.join()

    totals_pub = {}
    totals_con = {}
    for _ in range(k):
        i, n = pub_result_q.get()
        totals_pub[i] = n
    for _ in range(k):
        i, n = con_result_q.get()
        totals_con[i] = n

    print(f"k_queues={k} per_queue_rate={args.per_queue_rate} duration={args.duration_secs}s "
          f"ramp_up={args.ramp_up_secs}s drain_tail={args.drain_tail_secs}s")
    total_published = sum(totals_pub.values())
    total_consumed = sum(totals_con.values())
    print(f"total_published={total_published} total_consumed={total_consumed} "
          f"final_backlog={total_published - total_consumed}")

    in_window = [(t, row) for (t, row) in samples if steady_start <= t <= stop_time]
    if len(in_window) < 2:
        print("\nnot enough samples in steady-state window - increase --duration-secs")
        return

    t0, row0 = in_window[0]
    t1, row1 = in_window[-1]
    span = t1 - t0

    print(f"\nsteady-state window: t={t0-start_time:.1f}s..{t1-start_time:.1f}s (span={span:.1f}s)")
    print("per-queue steady-state rates and backlog bounds:")
    agg_pub_rate = 0.0
    agg_con_rate = 0.0
    any_growing = False
    for i in range(k):
        p0, c0 = row0[i]
        p1, c1 = row1[i]
        pub_rate = (p1 - p0) / span if span > 0 else 0
        con_rate = (c1 - c0) / span if span > 0 else 0
        backlogs = [row[i][0] - row[i][1] for (t, row) in in_window]
        min_b, max_b = min(backlogs), max(backlogs)
        first_b, last_b = backlogs[0], backlogs[-1]
        growing = last_b > first_b * 1.2 + 50  # simple growth heuristic
        if growing:
            any_growing = True
        agg_pub_rate += pub_rate
        agg_con_rate += con_rate
        print(f"  q{i}: publish={pub_rate:>9,.0f}/s consume={con_rate:>9,.0f}/s "
              f"backlog[min={min_b:>6} max={max_b:>6} first={first_b:>6} last={last_b:>6}] "
              f"{'GROWING' if growing else 'bounded'}")

    print(f"\naggregate steady-state publish rate:  {agg_pub_rate:>10,.0f}/s")
    print(f"aggregate steady-state consume rate:  {agg_con_rate:>10,.0f}/s")
    print(f"any queue's backlog growing: {any_growing}")


if __name__ == "__main__":
    main()
