#!/usr/bin/env python3
"""Isolates and measures ONLY the real wall-clock cost of the control-plane
setup phase for both nats_sidecar designs, using real running instances, so
the "N round trips vs 1" comparison is backed by a real measurement on both
sides rather than an estimate on one. Skips the data-plane publish/subscribe
entirely - nats_sidecar_scaling_real_data.py / _bcast.py already cover that
and it's unrelated to this specific cost.
"""
import subprocess
import time
import shutil
import os
import sys

NATS_SERVER = os.path.expanduser("~/nats-server")
SIDECAR = os.path.expanduser("~/nats_sidecar/build-ci/bin/nats_sidecar")
DRIVER = os.path.expanduser("~/pg_blazingmq/bench/mq_bench_driver")
STORE_BASE = "/tmp/nsc_ctrlonly_store"

FILTER_FIELD = "Exchange"
FILTER_VALUE = "N"
INPUT_SUBJECT = "sc.real.in"
OUTPUT_PREFIX = "sc.real.out"


def start_server(tag):
    store = f"{STORE_BASE}_{tag}"
    if os.path.exists(store):
        shutil.rmtree(store)
    os.makedirs(store, exist_ok=True)
    log = open(f"/tmp/nsc_ctrlonly_server_{tag}.log", "w")
    p = subprocess.Popen([NATS_SERVER, "-js", "-sd", store, "-p", "4222"],
                          stdout=log, stderr=subprocess.STDOUT)
    time.sleep(1.2)
    return p, log


def start_sidecar_unique_subject(i, tag):
    log = open(f"/tmp/nsc_ctrlonly_sidecar_{tag}_{i}.log", "w")
    p = subprocess.Popen([
        SIDECAR, "-a", "127.0.0.1", "-p", "4222", "-i", INPUT_SUBJECT,
        "--queue-group", "qg-real", "--attr", f"{FILTER_FIELD}:string",
        "--engine", "atree", "--output-prefix", OUTPUT_PREFIX,
        "--subscribe-subject", f"sc.real.ctrl.{i}",
        "--workers", "2", "--lease-bucket", f"sc-ctrlonly-old-leases-{tag}-{i}",
    ], stdout=log, stderr=subprocess.STDOUT)
    return p, log


def start_sidecar_shared_subject(i, tag):
    log = open(f"/tmp/nsc_ctrlonly_sidecar_new_{tag}_{i}.log", "w")
    p = subprocess.Popen([
        SIDECAR, "-a", "127.0.0.1", "-p", "4222", "-i", INPUT_SUBJECT,
        "--queue-group", "qg-real", "--attr", f"{FILTER_FIELD}:string",
        "--engine", "atree", "--output-prefix", OUTPUT_PREFIX,
        "--subscribe-subject", "sc.real.ctrl.shared",
        "--workers", "2", "--lease-bucket", f"sc-ctrlonly-new-leases-{tag}-{i}",
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


def run_ctrl_old(i):
    r = subprocess.run([DRIVER, "ctrl", f"sc.real.ctrl.{i}",
                         f'{FILTER_FIELD} = "{FILTER_VALUE}"'],
                        capture_output=True, text=True, timeout=10)
    return r.stdout.strip()


def measure_old(n):
    server, slog = start_server(f"old{n}")
    sidecars = []
    try:
        for i in range(1, n + 1):
            sp, sl = start_sidecar_unique_subject(i, n)
            sidecars.append((sp, sl))
        time.sleep(2.0 + 0.1 * n)

        t0 = time.time()
        for i in range(1, n + 1):
            reply = run_ctrl_old(i)
            if '"error"' in reply:
                print(f"  OLD N={n} instance {i} ctrl FAILED: {reply}", file=sys.stderr)
                return None
        t1 = time.time()
        return (t1 - t0) * 1000.0
    finally:
        for sp, sl in sidecars:
            stop(sp)
            sl.close()
        stop(server)
        slog.close()


def measure_new(n):
    server, slog = start_server(f"new{n}")
    sidecars = []
    try:
        for i in range(1, n + 1):
            sp, sl = start_sidecar_shared_subject(i, n)
            sidecars.append((sp, sl))
        time.sleep(2.0 + 0.1 * n)

        t0 = time.time()
        r = subprocess.run(
            [DRIVER, "ctrl_bcast", "sc.real.ctrl.shared",
             f'{FILTER_FIELD} = "{FILTER_VALUE}"', str(n), "5000"],
            capture_output=True, text=True, timeout=15)
        t1 = time.time()
        if "CTRL_BCAST_ID" not in r.stdout:
            print(f"  NEW N={n} ctrl_bcast FAILED: {r.stdout!r} {r.stderr!r}", file=sys.stderr)
            return None
        acks = None
        for line in r.stdout.splitlines():
            if line.startswith("CTRL_BCAST_ACKS "):
                acks = int(line.split()[1])
        if acks != n:
            print(f"  NEW N={n} WARNING only {acks}/{n} acked", file=sys.stderr)
        return (t1 - t0) * 1000.0
    finally:
        for sp, sl in sidecars:
            stop(sp)
            sl.close()
        stop(server)
        slog.close()


if __name__ == "__main__":
    print("N | old_design_ms (N sequential ctrl round trips) | new_design_ms (1 ctrl_bcast call)")
    for n in [1, 2, 4, 8, 16, 32]:
        old_ms = measure_old(n)
        time.sleep(0.5)
        new_ms = measure_new(n)
        time.sleep(0.5)
        print(f"{n} | {old_ms:.1f} | {new_ms:.1f}" if old_ms is not None and new_ms is not None
              else f"{n} | FAILED old={old_ms} new={new_ms}")
