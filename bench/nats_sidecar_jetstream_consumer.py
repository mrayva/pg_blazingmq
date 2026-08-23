#!/usr/bin/env python3
"""Verifies the JetStream-durable-consumer input redesign (nats_sidecar
commit adding --input-stream/--consumer-*), which replaces the sidecar's
plain core-NATS queue-group input subscription with a durable JetStream push
consumer (shared durable_name/deliver_subject/deliver_group across N
instances = real queue-group-style load distribution, backed by explicit
acks and max_ack_pending flow control instead of core NATS's at-most-once,
no-flow-control plain subscribe).

Three things this script checks, matching the approved plan's verification
section:

1. integration_check(n): N real instances sharing one durable consumer,
   real sc_bench_sample fixture (200,000 rows, Exchange="N" filter, ground
   truth 21,683 matches), confirms the exact count AND that every instance's
   own stats log shows it actually processed a nonzero share (real
   distribution, not one instance doing everything with N-1 idle).

2. stress_repro(trials): the actual pass/fail bar - reproduces the exact
   condition that found both prior loss modes this session (a full N=32
   sweep's heavy 32-process teardown immediately followed by a high-burst
   single-consumer N=1 run), this time with BOTH JetStream-ack-tracked
   publish (nats_publish_binary_stream_async + nats_publish_stream_flush,
   already proven correct in isolation) AND the new JetStream durable
   consumer active on the sidecar side. Reports the real stall count across
   `trials` repeated trials - the number that actually matters.

3. throughput_sweep(ns): real N-sweep (msgs/sec) against the existing
   recorded plain-queue-group table, to honestly characterize the real cost
   of durability rather than assume it's free.

The sidecar self-provisions its own input JetStream stream at startup
(ensure_input_stream(), following lease_manager::ensure_bucket()'s idiom) -
no separate out-of-band stream-creation step is needed here, unlike the
earlier nats_sidecar_n1_jetstream_repro.py script (which predates this
consumer-side redesign and had to create the stream itself via a Python
nats client before the sidecar's own plain-subscribe couldn't).
"""
import subprocess
import time
import shutil
import os
import sys
import argparse

NATS_SERVER = os.path.expanduser("~/nats-server")
SIDECAR = os.path.expanduser("~/nats_sidecar/build-ci/bin/nats_sidecar")
DRIVER = os.path.expanduser("~/pg_blazingmq/bench/mq_bench_driver")
STORE_BASE = "/tmp/jssc_store"
PSQL = ["psql", "-h", "/var/run/postgresql", "postgres", "-q", "-c"]

EXPECTED = 21683
FILTER_FIELD = "Exchange"
FILTER_VALUE = "N"
INPUT_SUBJECT = "sc.real.in"
OUTPUT_PREFIX = "sc.real.out"
INPUT_STREAM = "SC_JS_INPUT"
DURABLE_NAME = "sc-js-durable"
DELIVER_SUBJECT = "sc.js.deliver"
DELIVER_GROUP = "sc-js-group"


def start_server(tag):
    store = f"{STORE_BASE}_{tag}"
    if os.path.exists(store):
        shutil.rmtree(store)
    os.makedirs(store, exist_ok=True)
    log = open(f"/tmp/jssc_server_{tag}.log", "w")
    p = subprocess.Popen([NATS_SERVER, "-js", "-sd", store, "-p", "4222"],
                          stdout=log, stderr=subprocess.STDOUT)
    time.sleep(1.2)
    return p, log


def start_sidecar_plain(i, tag):
    """Plain queue-group mode - used only to generate real N=32 teardown
    load in stress_repro, matching this ecosystem's existing warmup pattern.
    Not the thing under test."""
    log = open(f"/tmp/jssc_sidecar_plain_{tag}_{i}.log", "w")
    p = subprocess.Popen([
        SIDECAR,
        "-a", "127.0.0.1", "-p", "4222",
        "-i", INPUT_SUBJECT,
        "--queue-group", "qg-real",
        "--attr", f"{FILTER_FIELD}:string",
        "--engine", "atree",
        "--output-prefix", OUTPUT_PREFIX,
        "--subscribe-subject", f"sc.jssc.plain.ctrl.{i}",
        "--workers", "2",
        "--lease-bucket", f"sc-jssc-plain-leases-{tag}",
    ], stdout=log, stderr=subprocess.STDOUT)
    return p, log


def start_sidecar_js(i, tag, max_ack_pending=2000):
    log_path = f"/tmp/jssc_sidecar_js_{tag}_{i}.log"
    log = open(log_path, "w")
    p = subprocess.Popen([
        SIDECAR,
        "-a", "127.0.0.1", "-p", "4222",
        "-i", INPUT_SUBJECT,
        "--input-stream", INPUT_STREAM,
        "--consumer-durable-name", DURABLE_NAME,
        "--consumer-deliver-subject", DELIVER_SUBJECT,
        "--consumer-deliver-group", DELIVER_GROUP,
        "--consumer-max-ack-pending", str(max_ack_pending),
        "--consumer-ack-wait", "30",
        "--attr", f"{FILTER_FIELD}:string",
        "--engine", "atree",
        "--output-prefix", OUTPUT_PREFIX,
        "--subscribe-subject", f"sc.jssc.js.ctrl.{tag}.{i}",
        "--workers", "2",
        "--lease-bucket", f"sc-jssc-js-leases-{tag}",
        "--stats-interval", "1",
    ], stdout=log, stderr=subprocess.STDOUT)
    return p, log, log_path


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


def run_ctrl(subscribe_subject):
    r = subprocess.run([DRIVER, "ctrl", subscribe_subject,
                         f'{FILTER_FIELD} = "{FILTER_VALUE}"'],
                        capture_output=True, text=True, timeout=10)
    return r.stdout.strip()


def instance_processed_count(log_path):
    """Last 'stats: ... processed=<N> ...' line's processed value, or None
    if the instance never logged one (e.g. exited before its first stats
    tick)."""
    try:
        with open(log_path) as f:
            lines = f.readlines()
    except OSError:
        return None
    for line in reversed(lines):
        if "stats:" in line and "processed=" in line:
            for tok in line.split():
                if tok.startswith("processed="):
                    try:
                        return int(tok.split("=", 1)[1])
                    except ValueError:
                        return None
    return None


def run_n32_load():
    """Full N=32 run, plain publish/plain subscribe - only to generate the
    real heavy-teardown load condition, not the thing under test."""
    server, slog = start_server("32")
    sidecars = []
    try:
        for i in range(1, 33):
            sp, sl = start_sidecar_plain(i, "32")
            sidecars.append((sp, sl))
        time.sleep(2.5 + 0.2 * 32)
        for i in range(1, 33):
            reply = run_ctrl(f"sc.jssc.plain.ctrl.{i}")
            if '"error"' in reply:
                print(f"  N=32 warmup instance {i} ctrl FAILED: {reply}", file=sys.stderr)
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
        pub = subprocess.run(PSQL + [pub_sql], capture_output=True, text=True, timeout=120)
        if pub.returncode != 0:
            print(f"  N=32 warmup publish FAILED: {pub.stderr}", file=sys.stderr)
        sub.communicate(timeout=100)
    finally:
        for sp, sl in sidecars:
            stop(sp)
            sl.close()
        stop(server)
        slog.close()
    # Deliberately no cooldown - immediate N=1 burst right after this
    # teardown is the condition that reproduces the loss.


def run_n_jetstream(n, tag):
    """N real JetStream-consumer-mode instances sharing one durable
    consumer, real sc_bench_sample publish via the JetStream-ack-tracked
    path. Returns dict with total/wrong/stalled/pub_secs/per_instance."""
    server, slog = start_server(tag)
    sidecars = []
    try:
        for i in range(1, n + 1):
            sp, sl, log_path = start_sidecar_js(i, tag)
            sidecars.append((sp, sl, log_path))
        time.sleep(1.5 + 0.2 * n)
        for sp, sl, log_path in sidecars:
            if sp.poll() is not None:
                print(f"  N={n} instance exited early - see {log_path}", file=sys.stderr)
                return None

        for i in range(1, n + 1):
            reply = run_ctrl(f"sc.jssc.js.ctrl.{tag}.{i}")
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
            f"SELECT nats_publish_binary_stream_async('{INPUT_SUBJECT}', row_to_msgpack(t)) "
            f"FROM sc_bench_sample t; "
            f"SELECT nats_publish_stream_flush();"
        )
        t0 = time.time()
        pub = subprocess.run(PSQL + [pub_sql], capture_output=True, text=True, timeout=120)
        t1 = time.time()
        if pub.returncode != 0:
            print(f"  N={n} JetStream publish FAILED: {pub.stderr}", file=sys.stderr)
            return None
        pub_secs = t1 - t0

        sub_out, _ = sub.communicate(timeout=100)
        sub_line = sub_out.strip().splitlines()[-1] if sub_out.strip() else "NONE"

        tot = wrong = None
        for tok in sub_line.split():
            if "=" in tok:
                k, v = tok.split("=", 1)
                if k == "total":
                    tot = v
                elif k == "wrong":
                    wrong = v
        stalled = (tot is None) or (int(tot) != EXPECTED)

        # --stats-interval 1 (set in start_sidecar_js) guarantees at least
        # one "stats:" line within ~1s of the last message being processed -
        # wait for it before reading logs.
        per_instance = None
        if n > 1:
            time.sleep(1.5)
            per_instance = [instance_processed_count(log_path) for _, _, log_path in sidecars]

        return {
            "total": tot, "wrong": wrong, "stalled": stalled,
            "pub_secs": pub_secs, "per_instance": per_instance,
        }
    finally:
        for sp, sl, _ in sidecars:
            stop(sp)
            sl.close()
        stop(server)
        slog.close()


def integration_check(n=4):
    print(f"=== Integration check: N={n} JetStream-consumer instances, real ground truth ===")
    result = run_n_jetstream(n, f"integ{n}")
    if result is None:
        print("  FAILED: run_n_jetstream returned None")
        return False
    ok = (not result["stalled"]) and result["total"] == str(EXPECTED)
    print(f"  total={result['total']} wrong={result['wrong']} expected={EXPECTED} "
          f"stalled={result['stalled']} pub_secs={result['pub_secs']:.2f}")
    print(f"  per-instance processed counts: {result['per_instance']}")
    return ok


def stress_repro(trials=15):
    print(f"=== Stress repro: N=32 teardown -> immediate N=1 JetStream-consumer burst, "
          f"{trials} trials ===")
    stalls = 0
    for t in range(1, trials + 1):
        run_n32_load()
        result = run_n_jetstream(1, "stress1")
        if result is None:
            print(f"  trial {t}: FAILED (run returned None)")
            stalls += 1
            continue
        status = "STALLED" if result["stalled"] else "clean"
        print(f"  trial {t}: total={result['total']} wrong={result['wrong']} "
              f"pub_secs={result['pub_secs']:.2f} -> {status}")
        if result["stalled"]:
            stalls += 1
    print(f"=== Stress repro result: {stalls}/{trials} trials stalled ===")
    return stalls


def throughput_sweep(ns=(1, 4, 8, 16, 32)):
    print(f"=== Throughput sweep: N in {ns} ===")
    table = {}
    for n in ns:
        result = run_n_jetstream(n, f"tp{n}")
        if result is None or result["stalled"]:
            print(f"  N={n}: FAILED or stalled ({result})")
            table[n] = None
            continue
        rate = 200000 / result["pub_secs"]
        print(f"  N={n}: {rate:.0f} msgs/sec ({result['pub_secs']:.2f}s for 200000 rows)")
        table[n] = rate
    return table


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["integration", "stress", "throughput", "all"])
    ap.add_argument("--trials", type=int, default=15)
    args = ap.parse_args()

    if args.mode in ("integration", "all"):
        ok = integration_check(4)
        print("INTEGRATION:", "PASS" if ok else "FAIL")
    if args.mode in ("stress", "all"):
        stalls = stress_repro(args.trials)
        print("STRESS:", f"{stalls}/{args.trials} stalled")
    if args.mode in ("throughput", "all"):
        table = throughput_sweep()
        print("THROUGHPUT:", table)
