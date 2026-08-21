#!/usr/bin/env python3
"""Re-run of the "multiple real nats_sidecar instances under one queue
group" filtered-throughput test (see bench/README.md, commit 0779c3d),
this time using the repo's usual C++ tooling (mq_bench_driver, a small
nats_asio-based publisher/subscriber/control-plane client) instead of the
original nats-py harness, to get an absolute number not potentially
limited by the Python harness itself.

Same parameters as the original: input subject sc.test.in, queue group
qg-cpp, one-attribute schema region:integer, atree engine, output prefix
sc.test.out, filter "region = 1" (a-tree uses single '=', confirmed by
trial), 100,000 published messages with region round-robin over 8 values
(exactly 12,500 should match), --workers 2 per instance (avoids the
worker-thread-oversubscription confound the original run found and fixed).
Fresh nats-server -js restart per N (own throwaway JetStream store dir -
nats_sidecar's lease manager needs NATS KV).

Process management uses subprocess.Popen/.terminate()/.kill() directly,
never pkill/os.system - pkill was found to misbehave in this environment
by an earlier fork's teardown logic.
"""
import subprocess
import time
import shutil
import os
import sys

NATS_SERVER = os.path.expanduser("~/nats-server")
SIDECAR = os.path.expanduser("~/nats_sidecar/build-ci/bin/nats_sidecar")
DRIVER = os.path.expanduser("~/pg_blazingmq/bench/mq_bench_driver")
STORE_BASE = "/tmp/nsc_cpp_store"

MSG_COUNT = 100_000
EXPECTED = MSG_COUNT // 8  # region round-robin 1..8


def start_server(n):
    store = f"{STORE_BASE}_{n}"
    if os.path.exists(store):
        shutil.rmtree(store)
    os.makedirs(store, exist_ok=True)
    log = open(f"/tmp/nsc_cpp_server_{n}.log", "w")
    p = subprocess.Popen([NATS_SERVER, "-js", "-sd", store, "-p", "4222"],
                          stdout=log, stderr=subprocess.STDOUT)
    time.sleep(1.2)
    return p, log


def start_sidecar(i, n):
    log = open(f"/tmp/nsc_cpp_sidecar_{n}_{i}.log", "w")
    p = subprocess.Popen([
        SIDECAR,
        "-a", "127.0.0.1", "-p", "4222",
        "-i", "sc.test.in",
        "--queue-group", "qg-cpp",
        "--attr", "region:integer",
        "--engine", "atree",
        "--output-prefix", "sc.test.out",
        "--subscribe-subject", f"sc.ctrl.{i}",
        "--workers", "2",
        "--lease-bucket", f"sc-cpp-leases-{n}",
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
    r = subprocess.run([DRIVER, "ctrl", f"sc.ctrl.{i}", "region = 1"],
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

        # consumer first (background), then publisher
        sub = subprocess.Popen(
            [DRIVER, "sub", "sc.test.out.>", str(EXPECTED), "60000"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        time.sleep(0.3)

        pub = subprocess.run([DRIVER, "pub", "sc.test.in", str(MSG_COUNT)],
                              capture_output=True, text=True, timeout=60)
        pub_line = pub.stdout.strip()

        sub_out, _ = sub.communicate(timeout=70)
        sub_line = sub_out.strip().splitlines()[-1] if sub_out.strip() else "NONE"

        return pub_line, sub_line
    finally:
        for sp, sl in sidecars:
            stop(sp)
            sl.close()
        stop(server)
        slog.close()


def parse(line, prefix, key):
    # e.g. "PUB_DONE count=100000 secs=0.67 rate=148880"
    parts = {}
    for tok in line.split():
        if "=" in tok:
            k, v = tok.split("=", 1)
            parts[k] = v
    return parts.get(key)


if __name__ == "__main__":
    results = []
    for n in [1, 2, 4, 8]:
        print(f"=== N={n} ===")
        r = run_n(n)
        if r is None:
            print(f"N={n} FAILED")
            continue
        pub_line, sub_line = r
        print(" ", pub_line)
        print(" ", sub_line)
        pub_rate = parse(pub_line, "PUB_DONE", "rate")
        tot = parse(sub_line, "SUB_DONE", "total")
        wrong = parse(sub_line, "SUB_DONE", "wrong")
        rate = parse(sub_line, "SUB_DONE", "rate")
        results.append((n, pub_rate, tot, wrong, rate))
        time.sleep(1)

    print("\n=== SUMMARY ===")
    print("N | publish_rate | total | wrong | filtered_rate")
    for n, pr, tot, wrong, rate in results:
        print(f"{n} | {pr} | {tot}/{EXPECTED} | {wrong} | {rate}")
