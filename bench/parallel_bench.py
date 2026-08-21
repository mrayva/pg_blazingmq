#!/usr/bin/env python3
"""Multi-backend parallel publish driver: N separate psycopg connections
(processes), each publishing its own row partition, barrier-synchronized
so timing reflects genuine concurrent overlap, not staggered starts.
"""
import argparse
import multiprocessing as mp
import time
import psycopg
import psycopg.sql

def worker(args, worker_id, offset, count, barrier, result_queue):
    dsn = args.dsn
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET blazingmq.broker_uri = {psycopg.sql.Literal(args.broker_uri).as_string(cur)}")
            cur.execute(
                f"CREATE TEMP TABLE bmq_bench_part AS "
                f"SELECT * FROM {args.sql} LIMIT {count} OFFSET {offset}"
            )
            attr_array_sql = "ARRAY['Sequence Number','Trade Id']"
            publish_sql = (
                f"SELECT count(*) FROM "
                f"(SELECT bmq_publish_row(%s, t, {attr_array_sql}) "
                f"FROM bmq_bench_part t) s"
            )
            barrier.wait()
            t0 = time.perf_counter()
            cur.execute(publish_sql, (args.queue_uri,))
            published = cur.fetchone()[0]
            elapsed = time.perf_counter() - t0
            result_queue.put((worker_id, published, elapsed, t0))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="host=/var/run/postgresql dbname=postgres")
    ap.add_argument("--sql", required=True)
    ap.add_argument("--total-rows", type=int, default=100000)
    ap.add_argument("--connections", type=int, required=True)
    ap.add_argument("--queue-uri", default="bmq://bmq.test.mem.broadcast/parallel_bench")
    ap.add_argument("--broker-uri", default="tcp://localhost:30114")
    args = ap.parse_args()

    n = args.connections
    per_worker = args.total_rows // n
    barrier = mp.Barrier(n)
    result_queue = mp.Queue()
    procs = []
    wall_t0 = time.perf_counter()
    for i in range(n):
        p = mp.Process(target=worker, args=(args, i, i * per_worker, per_worker, barrier, result_queue))
        p.start()
        procs.append(p)
    for p in procs:
        p.join()

    results = [result_queue.get() for _ in range(n)]
    total_published = sum(r[1] for r in results)
    # Wall-clock span: from the earliest t0 (post-barrier start) to the
    # latest finish (t0 + elapsed) across all workers - the true concurrent
    # window, not a sum of individual durations.
    starts = [r[3] for r in results]
    finishes = [r[3] + r[2] for r in results]
    span = max(finishes) - min(starts)
    print(f"connections={n} per_worker={per_worker} total_published={total_published} "
          f"span={span:.3f}s rate={total_published/span:,.0f}/s")

if __name__ == "__main__":
    main()
