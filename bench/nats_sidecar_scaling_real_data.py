#!/usr/bin/env python3
"""Re-run of "multiple real nats_sidecar instances under one queue group"
(bench/README.md, commits 0779c3d/9a7e32b/fbb40a8), using REAL NYSE trade
data via pgnats instead of synthetic uniform-distribution payloads, to
check whether the ~5.9-6.65x scaling at N=8 found with idealized synthetic
data holds up against real, skewed market data.

Sample: 200,000 rows materialized once into sc_bench_sample (LIMIT 200000
FROM nyse_eqy_us_all_trade_20260102, real ordering as stored - not
resampled per run, so the same 200,000 rows are used for every N).
Filter attribute: "Exchange" (character varying), a real, skewed
categorical column (Exchange 'D' alone is ~43% of this sample; 19
distinct values total). Filter value: "N" - ground truth match count
computed directly against sc_bench_sample: 21,683/200,000 (~10.8%),
chosen for a meaningful but non-degenerate match fraction, matching the
5-30% target range.

Payload: full row via row_to_msgpack(t) (pg_zerialize), published via
pgnats's nats_publish_binary - the same real-row-publish shape used
throughout this document's earlier BlazingMQ/JetStream sections, not a
single-column synthetic payload.

Same architecture/discipline as the synthetic-data runs: N separate
nats_sidecar processes under one queue group (genuine competition),
--workers 2 per instance (already-confirmed fix for worker-thread
oversubscription), fresh nats-server -js restart per N with a throwaway
JetStream store dir (lease manager needs NATS KV). Verification via
mq_bench_driver's new `subfield` mode (Exchange==N check, not the
hardcoded region==1 check `sub` mode has).
"""
import subprocess
import time
import shutil
import os
import sys

NATS_SERVER = os.path.expanduser("~/nats-server")
SIDECAR = os.path.expanduser("~/nats_sidecar/build-ci/bin/nats_sidecar")
DRIVER = os.path.expanduser("~/pg_blazingmq/bench/mq_bench_driver")
STORE_BASE = "/tmp/nsc_real_store"
PSQL = ["psql", "-h", "/var/run/postgresql", "postgres", "-q", "-c"]

EXPECTED = 21683
FILTER_FIELD = "Exchange"
FILTER_VALUE = "N"
INPUT_SUBJECT = "sc.real.in"
OUTPUT_PREFIX = "sc.real.out"


def start_server(n):
    store = f"{STORE_BASE}_{n}"
    if os.path.exists(store):
        shutil.rmtree(store)
    os.makedirs(store, exist_ok=True)
    log = open(f"/tmp/nsc_real_server_{n}.log", "w")
    p = subprocess.Popen([NATS_SERVER, "-js", "-sd", store, "-p", "4222"],
                          stdout=log, stderr=subprocess.STDOUT)
    time.sleep(1.2)
    return p, log


def start_sidecar(i, n):
    log = open(f"/tmp/nsc_real_sidecar_{n}_{i}.log", "w")
    p = subprocess.Popen([
        SIDECAR,
        "-a", "127.0.0.1", "-p", "4222",
        "-i", INPUT_SUBJECT,
        "--queue-group", "qg-real",
        "--attr", f"{FILTER_FIELD}:string",
        "--engine", "atree",
        "--output-prefix", OUTPUT_PREFIX,
        "--subscribe-subject", f"sc.real.ctrl.{i}",
        "--workers", "2",
        "--lease-bucket", f"sc-real-leases-{n}",
    ], stdout=log, stderr=subprocess.STDOUT)
    return p, log


def stop(p):
    try:
        p.terminate()
        p.wait(timeout=5)
    except Exception:
        try:
            p.kill()
            p.wait(timeout=5)
        except Exception:
            pass


def run_ctrl(i):
    r = subprocess.run([DRIVER, "ctrl", f"sc.real.ctrl.{i}",
                         f'{FILTER_FIELD} = "{FILTER_VALUE}"'],
                        capture_output=True, text=True, timeout=10)
    return r.stdout.strip()


def run_n(n):
    server, slog = start_server(n)
    sidecars = []
    try:
        for i in range(1, n + 1):
            sp, sl = start_sidecar(i, n)
            sidecars.append((sp, sl))
        time.sleep(2.5)

        for i in range(1, n + 1):
            reply = run_ctrl(i)
            if '"error"' in reply:
                print(f"  N={n} instance {i} ctrl FAILED: {reply}", file=sys.stderr)
                return None
        time.sleep(0.3)

        sub = subprocess.Popen(
            [DRIVER, "subfield", f"{OUTPUT_PREFIX}.>", str(EXPECTED), "90000",
             FILTER_FIELD, FILTER_VALUE],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        time.sleep(0.3)

        pub_sql = (
            f"SELECT nats_publish_binary('{INPUT_SUBJECT}', row_to_msgpack(t)) "
            f"FROM sc_bench_sample t;"
        )
        t0 = time.time()
        pub = subprocess.run(PSQL + [pub_sql], capture_output=True, text=True, timeout=120)
        t1 = time.time()
        if pub.returncode != 0:
            print(f"  N={n} publish FAILED: {pub.stderr}", file=sys.stderr)
            return None
        pub_secs = t1 - t0
        pub_rate = 200000 / pub_secs

        sub_out, _ = sub.communicate(timeout=100)
        sub_line = sub_out.strip().splitlines()[-1] if sub_out.strip() else "NONE"

        return pub_rate, pub_secs, sub_line
    finally:
        for sp, sl in sidecars:
            stop(sp)
            sl.close()
        stop(server)
        slog.close()


def parse(line, key):
    parts = {}
    for tok in line.split():
        if "=" in tok:
            k, v = tok.split("=", 1)
            parts[k] = v
    return parts.get(key)


if __name__ == "__main__":
    results = []
    for n in [1, 2, 4, 8]:
        print(f"=== N={n} ===", flush=True)
        r = run_n(n)
        if r is None:
            print(f"N={n} FAILED")
            continue
        pub_rate, pub_secs, sub_line = r
        print(f"  publish: {pub_rate:.0f} rows/s ({pub_secs:.2f}s)")
        print(" ", sub_line)
        tot = parse(sub_line, "total")
        wrong = parse(sub_line, "wrong")
        rate = parse(sub_line, "rate")
        results.append((n, pub_rate, tot, wrong, rate))
        time.sleep(1)

    print("\n=== SUMMARY (real NYSE data, Exchange==N, 200000 rows, expected=21683) ===")
    print("N | publish_rate(rows/s) | total | wrong | filtered_rate(matches/s)")
    for n, pr, tot, wrong, rate in results:
        print(f"{n} | {pr:.0f} | {tot}/{EXPECTED} | {wrong} | {rate}")
