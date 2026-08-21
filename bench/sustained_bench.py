#!/usr/bin/env python3
"""Concurrent publish+drain sustained-throughput benchmark for pg_blazingmq.

Every other script in this directory (bmq_bench.py, parallel_bench.py) is
publish-only: nothing ever calls bmq_consume() to drain what gets
published, so the queue's backlog grows monotonically for the whole run.
bench/README.md's "Undrained Backlog" section proved via perf that
BlazingMQ's per-message delivery-attempt cost (RootQueueEngine::
afterNewMessage()/deliverMessage()) scales with how large that undrained
backlog has grown - so a publish-only benchmark can only ever measure a
burst/small-backlog rate, never a real sustained one.

This script runs N producer processes and M consumer processes
concurrently against the same queue for a fixed wall-clock duration, with
consumers draining continuously via bmq_consume(batch_confirm=true) so
the backlog stays small instead of growing unbounded. A monitor process
samples (published - consumed) every --sample-interval-secs throughout
the run and prints it as a curve - that curve, not just "the run didn't
crash", is the evidence the backlog was actually bounded.

Priority mode, not broadcast: broadcast fans every message out to *every*
attached consumer independently (each consumer gets its own copy, and per
this session's own earlier findings the broker must retain a message
until every attached consumer has confirmed it) - adding more broadcast
consumers doesn't parallelize draining, it multiplies delivery work
without shrinking the backlog any faster. Priority mode's work-queue
routing sends each message to exactly one of the attached consumers
(round-robin among equal-priority consumers), so it's the mode that
actually lets M consumers divide up the drain work - the correct choice
for this specific benchmark, unlike every producer-only benchmark this
session, which used broadcast because there was no consumer contention to
worry about.

Producers publish in repeated small batches (not one huge single
INSERT-shaped publish) so publishing continues throughout the whole
window instead of finishing early - each producer loops
`--batch-rows`-sized bmq_publish_row() batches until --duration-secs
elapses.
"""
import argparse
import multiprocessing as mp
import random
import time

import psycopg
import psycopg.sql


def execute_with_open_retry(cur, sql, params, max_attempts=8):
    """Retries on BlazingMQ rc=100 (ALREADY_OPENED) - not the same-session
    double-open case (get_queue()'s cache already handles that fine), but
    a startup race: N sessions' first-ever open of a brand-new queue can
    still collide even when each producer/consumer primes its handle
    before a shared barrier, since per-process setup time (connect, SET,
    CREATE TEMP TABLE) varies enough that "before the barrier" isn't
    reliably "not at the same instant as some other session's first open"
    too. A short randomized backoff and retry clears it in practice.
    """
    for attempt in range(max_attempts):
        try:
            cur.execute(sql, params)
            return
        except psycopg.errors.InternalError_ as e:
            if "rc=100" in str(e) and attempt < max_attempts - 1:
                time.sleep(0.05 * (attempt + 1) + random.uniform(0, 0.05))
                continue
            raise


def producer(args, worker_id, start_time, stop_time, counter, barrier):
    dsn = args.dsn
    attr_array_sql = "ARRAY['Sequence Number','Trade Id']"
    with psycopg.connect(dsn, autocommit=True) as conn:
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
            # Prime this session's queue handle before the synchronized
            # start - without this, N producers' first-ever open of a
            # brand-new queue all land on the exact barrier-release
            # instant and race each other into ALREADY_OPENED (rc=100),
            # not the same-session double-open case that name usually
            # means; opening once, quietly, ahead of time avoids it.
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
                    # Pace this producer to a fixed target rate instead of
                    # publishing flat-out. Uncapped publish always
                    # eventually outruns any finite consumer capacity here:
                    # per-publish delivery-attempt cost itself scales with
                    # backlog size (bench/README.md's "Undrained Backlog"
                    # finding - it's not just delivery that gets expensive,
                    # publishing into a large backlog does too), so an
                    # unthrottled producer and a fixed-capacity consumer
                    # can never reach equilibrium - the gap only widens.
                    # Finding the highest rate that DOES hold in
                    # equilibrium is the actual question, not "how fast can
                    # one side outrun the other".
                    target_elapsed = local_published / (args.producer_rate_limit / args.producers)
                    actual_elapsed = time.perf_counter() - worker_start
                    if target_elapsed > actual_elapsed:
                        time.sleep(target_elapsed - actual_elapsed)
            return local_published


def consumer(args, worker_id, start_time, stop_time, hard_stop_time, counter, barrier):
    dsn = args.dsn
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SET blazingmq.broker_uri = {psycopg.sql.Literal(args.broker_uri).as_string(cur)}"
            )
            # Establish the read handle before the barrier so priority-mode
            # routing has somewhere to send messages from the very first
            # publish - bmq_consume() opens (and caches) the queue handle
            # on first call, matching every other benchmark's methodology
            # this session for "open the read side before publish begins".
            execute_with_open_retry(
                cur,
                "SELECT count(*) FROM bmq_consume(%s, NULL, 1, 50, true)",
                (args.queue_uri,),
            )
            barrier.wait()
            local_consumed = 0
            # Keep draining past stop_time up to hard_stop_time to clear
            # whatever backlog exists at the end of the producer window -
            # otherwise the last few seconds of production would be
            # reported as "backlog" that was actually just about to be
            # drained, understating how bounded steady-state really was.
            # Deliberately no early-exit on a single empty call: priority
            # mode round-robins delivery across attached consumers, so one
            # consumer seeing n==0 on a given call doesn't mean the queue
            # is actually empty (an earlier version broke on exactly that,
            # silently abandoning its share of the backlog for the rest of
            # the run the moment production paused even briefly). Loop
            # unconditionally until hard_stop_time instead.
            while time.perf_counter() < hard_stop_time:
                cur.execute(
                    "SELECT count(*) FROM bmq_consume(%s, NULL, %s, %s, true)",
                    (args.queue_uri, args.consume_batch, args.consume_timeout_ms),
                )
                n = cur.fetchone()[0]
                local_consumed += n
                with counter.get_lock():
                    counter.value += n
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
    ap.add_argument("--sql", required=True)
    ap.add_argument("--producers", type=int, required=True)
    ap.add_argument("--consumers", type=int, required=True)
    ap.add_argument("--batch-rows", type=int, default=2000,
                     help="rows per bmq_publish_row() batch, repeated by each producer")
    ap.add_argument("--consume-batch", type=int, default=2000)
    ap.add_argument("--consume-timeout-ms", type=int, default=200)
    ap.add_argument("--duration-secs", type=float, default=45.0,
                     help="producer run window (excludes drain-out tail)")
    ap.add_argument("--ramp-up-secs", type=float, default=10.0,
                     help="initial window excluded from the steady-state rate")
    ap.add_argument("--drain-tail-secs", type=float, default=15.0,
                     help="extra time consumers keep draining after producers stop")
    ap.add_argument("--sample-interval-secs", type=float, default=2.0)
    ap.add_argument("--producer-rate-limit", type=float, default=0,
                     help="total target rows/sec across all producers, 0=unthrottled")
    ap.add_argument("--queue-uri", default="bmq://bmq.test.mem.priority/sustained_bench")
    ap.add_argument("--broker-uri", default="tcp://localhost:30114")
    args = ap.parse_args()

    n_workers = args.producers + args.consumers
    barrier = mp.Barrier(n_workers)
    published_counter = mp.Value("l", 0)
    consumed_counter = mp.Value("l", 0)
    manager = mp.Manager()
    samples = manager.list()

    # Shared timeline computed once, passed to every process so they all
    # agree on when "steady state" starts/ends without needing a second
    # barrier (which would itself distort the ramp-up window).
    t_now = time.perf_counter()
    start_time = t_now + 1.0  # small fixed head start for process spin-up
    stop_time = start_time + args.duration_secs
    hard_stop_time = stop_time + args.drain_tail_secs
    steady_start = start_time + args.ramp_up_secs

    procs = []
    pub_result_q = mp.Queue()
    con_result_q = mp.Queue()

    def run_producer(i):
        n = producer(args, i, start_time, stop_time, published_counter, barrier)
        pub_result_q.put(n)

    def run_consumer(i):
        n = consumer(args, i, start_time, stop_time, hard_stop_time, consumed_counter, barrier)
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

    print(f"producers={args.producers} consumers={args.consumers} "
          f"duration={args.duration_secs}s ramp_up={args.ramp_up_secs}s drain_tail={args.drain_tail_secs}s")
    print(f"total_published={total_published} total_consumed={total_consumed} "
          f"final_backlog={total_published-total_consumed}")
    print("backlog curve (t_since_start, published_so_far, consumed_so_far, backlog):")
    for (t, p, c, b) in samples:
        print(f"  t={t-start_time:6.1f}s  published={p:>9} consumed={c:>9} backlog={b:>7}")

    # Steady-state rate: consumed-count delta across [steady_start, stop_time]
    # from the sampled curve - consumed rate is the honest "sustained
    # throughput" number (it's bounded by drain capacity, which is what
    # matters when the backlog isn't growing; published rate alone would
    # overstate things if the backlog were still quietly growing).
    in_window = [(t, p, c) for (t, p, c, b) in samples if steady_start <= t <= stop_time]
    if len(in_window) >= 2:
        t0, p0, c0 = in_window[0]
        t1, p1, c1 = in_window[-1]
        span = t1 - t0
        pub_rate = (p1 - p0) / span if span > 0 else 0
        con_rate = (c1 - c0) / span if span > 0 else 0
        max_backlog_in_window = max(p - c for (t, p, c) in in_window)
        min_backlog_in_window = min(p - c for (t, p, c) in in_window)
        print(f"\nsteady-state window: t={t0-start_time:.1f}s..{t1-start_time:.1f}s (span={span:.1f}s)")
        print(f"steady-state publish rate: {pub_rate:,.0f}/s")
        print(f"steady-state consume (sustained) rate: {con_rate:,.0f}/s")
        print(f"backlog in steady-state window: min={min_backlog_in_window} max={max_backlog_in_window}")
    else:
        print("\nnot enough samples in steady-state window - increase --duration-secs or decrease --sample-interval-secs")


if __name__ == "__main__":
    main()
