#!/usr/bin/env python3
"""Control-plane variant of nats_sidecar_scaling_real_data.py: verifies the
client-assigned-subscription-ID + shared-control-subject redesign
(nats_sidecar commit adding subscription_manager::restore()-backed "id" field
handling to on_subscribe_request) against the exact same real-data fixture
and N-sweep as the original script, side by side.

Only the CONTROL plane changes here:
  - All N instances share ONE --subscribe-subject (fan-out, every instance
    gets every control message) instead of one per instance
    (sc.real.ctrl.{i}).
  - Setup issues ONE mq_bench_driver ctrl_bcast call (a single publish with
    a client-generated 64-bit ID, collecting up to N acks within a timeout)
    instead of N sequential mq_bench_driver ctrl request/reply round trips.
  - The output topic is computed deterministically from the client-assigned
    ID the instant ctrl_bcast returns its ID (before any ack arrives) -
    OUTPUT_PREFIX + "." + id - not discovered from N separate replies.

The DATA plane (--queue-group qg-real input distribution, message
publish/match/deliver path) is untouched and identical to the original
script - this is deliberate: this script exists to measure the
control-plane round-trip-count/latency difference, not to re-litigate
data-plane throughput, which should come out statistically the same as the
existing three-way table (any real difference would itself be worth
reporting, not assumed away).

Same fixture/ground truth as nats_sidecar_scaling_real_data.py: 200,000 real
NYSE rows in sc_bench_sample, Exchange="N" filter, 21,683 expected matches.
"""
import subprocess
import time
import shutil
import os
import sys

NATS_SERVER = os.path.expanduser("~/nats-server")
SIDECAR = os.path.expanduser("~/nats_sidecar/build-ci/bin/nats_sidecar")
DRIVER = os.path.expanduser("~/pg_blazingmq/bench/mq_bench_driver")
STORE_BASE = "/tmp/nsc_bcast_store"
PSQL = ["psql", "-h", "/var/run/postgresql", "postgres", "-q", "-c"]

EXPECTED = 21683
FILTER_FIELD = "Exchange"
FILTER_VALUE = "N"
INPUT_SUBJECT = "sc.real.in"
OUTPUT_PREFIX = "sc.real.out"
CTRL_SUBJECT = "sc.real.ctrl.shared"  # ONE subject, shared by every instance


def start_server(n):
    store = f"{STORE_BASE}_{n}"
    if os.path.exists(store):
        shutil.rmtree(store)
    os.makedirs(store, exist_ok=True)
    log = open(f"/tmp/nsc_bcast_server_{n}.log", "w")
    p = subprocess.Popen([NATS_SERVER, "-js", "-sd", store, "-p", "4222"],
                          stdout=log, stderr=subprocess.STDOUT)
    time.sleep(1.2)
    return p, log


def start_sidecar(i, n):
    log = open(f"/tmp/nsc_bcast_sidecar_{n}_{i}.log", "w")
    p = subprocess.Popen([
        SIDECAR,
        "-a", "127.0.0.1", "-p", "4222",
        "-i", INPUT_SUBJECT,
        "--queue-group", "qg-real",
        "--attr", f"{FILTER_FIELD}:string",
        "--engine", "atree",
        "--output-prefix", OUTPUT_PREFIX,
        "--subscribe-subject", CTRL_SUBJECT,
        "--workers", "2",
        "--lease-bucket", f"sc-bcast-leases-{n}",
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


def run_ctrl_bcast(n):
    """ONE publish, up to n acks. Returns (sub_id, output_topic, acks, errors,
    driver_latency_ms, wall_secs) or None on failure."""
    t0 = time.time()
    r = subprocess.run(
        [DRIVER, "ctrl_bcast", CTRL_SUBJECT, f'{FILTER_FIELD} = "{FILTER_VALUE}"',
         str(n), "5000"],
        capture_output=True, text=True, timeout=15)
    wall_secs = time.time() - t0
    lines = r.stdout.strip().splitlines()
    sub_id = None
    acks = errors = expected = None
    latency_ms = None
    for line in lines:
        if line.startswith("CTRL_BCAST_ID "):
            sub_id = int(line.split()[1])
        elif line.startswith("CTRL_BCAST_ACKS "):
            parts = line.split()
            acks = int(parts[1])
            for p in parts[2:]:
                k, v = p.split("=", 1)
                if k == "errors":
                    errors = int(v)
                elif k == "expected":
                    expected = int(v)
                elif k == "latency_ms":
                    latency_ms = float(v)
    if sub_id is None:
        print(f"  ctrl_bcast FAILED, stdout={r.stdout!r} stderr={r.stderr!r}", file=sys.stderr)
        return None
    return sub_id, f"{OUTPUT_PREFIX}.{sub_id}", acks, errors, latency_ms, wall_secs


def run_n(n):
    server, slog = start_server(n)
    sidecars = []
    try:
        for i in range(1, n + 1):
            sp, sl = start_sidecar(i, n)
            sidecars.append((sp, sl))
        time.sleep(2.5)

        result = run_ctrl_bcast(n)
        if result is None:
            print(f"  N={n} ctrl_bcast FAILED", file=sys.stderr)
            return None
        sub_id, output_topic, acks, errors, ctrl_latency_ms, ctrl_wall_secs = result
        if errors:
            print(f"  N={n} ctrl_bcast reported {errors} error ack(s) (expected 0)", file=sys.stderr)
        if acks != n:
            print(f"  N={n} WARNING: only {acks}/{n} instances acked within timeout", file=sys.stderr)
        time.sleep(0.3)

        sub = subprocess.Popen(
            [DRIVER, "subfield", output_topic, str(EXPECTED), "90000",
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

        return pub_rate, pub_secs, sub_line, acks, ctrl_latency_ms, ctrl_wall_secs
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
    for n in [1, 2, 4, 8, 16, 32]:
        print(f"=== N={n} (shared control subject, 1 ctrl_bcast call) ===", flush=True)
        r = run_n(n)
        if r is None:
            print(f"N={n} FAILED")
            continue
        pub_rate, pub_secs, sub_line, acks, ctrl_latency_ms, ctrl_wall_secs = r
        print(f"  publish: {pub_rate:.0f} rows/s ({pub_secs:.2f}s)")
        print(f"  ctrl_bcast: acks={acks}/{n} driver_latency_ms={ctrl_latency_ms:.2f} "
              f"wall_secs={ctrl_wall_secs:.3f} (vs {n} sequential round trips in the original design)")
        print(" ", sub_line)
        tot = parse(sub_line, "total")
        wrong = parse(sub_line, "wrong")
        rate = parse(sub_line, "rate")
        results.append((n, pub_rate, tot, wrong, rate, acks, ctrl_latency_ms, ctrl_wall_secs))
        time.sleep(1)

    print("\n=== SUMMARY (real NYSE data, Exchange==N, 200000 rows, expected=21683, shared control subject) ===")
    print("N | publish_rate(rows/s) | total | wrong | filtered_rate(matches/s) | ctrl_acks | ctrl_latency_ms | ctrl_wall_secs | ctrl_round_trips")
    for n, pr, tot, wrong, rate, acks, lat, wall in results:
        print(f"{n} | {pr:.0f} | {tot}/{EXPECTED} | {wrong} | {rate} | {acks}/{n} | {lat:.2f} | {wall:.3f} | 1 (was {n})")
