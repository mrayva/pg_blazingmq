#!/usr/bin/env python3
"""Verifies whether switching the PUBLISHER side of the real nats_sidecar N=1
stress repro from plain nats_publish_binary() to JetStream-backed
nats_publish_binary_stream_async()+nats_publish_stream_flush() achieves zero
message loss under the exact real condition that's been reproducibly losing
~0.1-0.2% of a 200,000-row real NYSE burst (sc_bench_sample, Exchange="N",
21,683 expected matches): a full N=32 nats_sidecar sweep's heavy 32-process
teardown immediately followed by a high-burst single-consumer N=1
publish+match run.

Each trial: start N=32 (plain publish, just to generate the real teardown
load - not the thing under test), tear down, immediately start N=1 and
publish via the JetStream path under test, check for exact 21,683 matches
with no timeout/stall. Repeated for --trials trials.

A JetStream stream (SCJS, subjects sc.real.in.>) is created fresh per N=1
attempt (server is restarted fresh per attempt in this repro, matching the
existing scripts' own per-N fresh-server discipline).
"""
import subprocess
import time
import shutil
import os
import sys

NATS_SERVER = os.path.expanduser("~/nats-server")
SIDECAR = os.path.expanduser("~/nats_sidecar/build-ci/bin/nats_sidecar")
DRIVER = os.path.expanduser("~/pg_blazingmq/bench/mq_bench_driver")
VENV_PY = os.path.expanduser("~/duckdb-nats-jetstream/.venv/bin/python3")
STORE_BASE = "/tmp/n1js_store"
PSQL = ["psql", "-h", "/var/run/postgresql", "postgres", "-q", "-c"]

EXPECTED = 21683
FILTER_FIELD = "Exchange"
FILTER_VALUE = "N"
INPUT_SUBJECT = "sc.real.in"
OUTPUT_PREFIX = "sc.real.out"
STREAM_NAME = "SCJS"

CREATE_STREAM_PY = f"""
import asyncio, nats
async def main():
    nc = await nats.connect("nats://127.0.0.1:4222")
    js = nc.jetstream()
    try:
        await js.add_stream(name="{STREAM_NAME}", subjects=["{INPUT_SUBJECT}.>", "{INPUT_SUBJECT}"])
    except Exception as e:
        print("stream create:", e)
    await nc.close()
asyncio.run(main())
"""


def start_server(tag):
    store = f"{STORE_BASE}_{tag}"
    if os.path.exists(store):
        shutil.rmtree(store)
    os.makedirs(store, exist_ok=True)
    log = open(f"/tmp/n1js_server_{tag}.log", "w")
    p = subprocess.Popen([NATS_SERVER, "-js", "-sd", store, "-p", "4222"],
                          stdout=log, stderr=subprocess.STDOUT)
    time.sleep(1.2)
    return p, log


def start_sidecar(i, tag, subscribe_subject):
    log = open(f"/tmp/n1js_sidecar_{tag}_{i}.log", "w")
    p = subprocess.Popen([
        SIDECAR,
        "-a", "127.0.0.1", "-p", "4222",
        "-i", INPUT_SUBJECT,
        "--queue-group", "qg-real",
        "--attr", f"{FILTER_FIELD}:string",
        "--engine", "atree",
        "--output-prefix", OUTPUT_PREFIX,
        "--subscribe-subject", subscribe_subject,
        "--workers", "2",
        "--lease-bucket", f"sc-n1js-leases-{tag}",
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


def run_ctrl(subscribe_subject):
    r = subprocess.run([DRIVER, "ctrl", subscribe_subject,
                         f'{FILTER_FIELD} = "{FILTER_VALUE}"'],
                        capture_output=True, text=True, timeout=10)
    return r.stdout.strip()


def run_n32_load():
    """Full N=32 run, plain publish - just to generate the real
    32-process-teardown load condition, not the thing under test."""
    server, slog = start_server("32")
    sidecars = []
    try:
        for i in range(1, 33):
            sp, sl = start_sidecar(i, "32", f"sc.n1js.ctrl.{i}")
            sidecars.append((sp, sl))
        time.sleep(2.5 + 0.2 * 32)
        for i in range(1, 33):
            reply = run_ctrl(f"sc.n1js.ctrl.{i}")
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
    # Deliberately NO cooldown sleep here - immediate N=1 burst right after
    # the 32-process teardown is the condition that reproduces the loss.


def run_n1_jetstream():
    """N=1, publisher uses nats_publish_binary_stream_async + flush (the
    path under test). Returns (total, wrong, stalled_bool, pub_secs)."""
    server, slog = start_server("1")
    sidecars = []
    try:
        sp, sl = start_sidecar(1, "1", "sc.n1js.ctrl.solo")
        sidecars.append((sp, sl))
        time.sleep(1.5)

        # Create the JetStream stream covering the input subject, fresh
        # server per attempt (matches this ecosystem's existing per-N
        # fresh-server discipline).
        r = subprocess.run([VENV_PY, "-c", CREATE_STREAM_PY],
                            capture_output=True, text=True, timeout=15)
        if r.returncode != 0:
            print(f"  stream create FAILED: {r.stdout} {r.stderr}", file=sys.stderr)
            return None

        reply = run_ctrl("sc.n1js.ctrl.solo")
        if '"error"' in reply:
            print(f"  N=1 ctrl FAILED: {reply}", file=sys.stderr)
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
            print(f"  N=1 JetStream publish FAILED: {pub.stderr}", file=sys.stderr)
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
        return tot, wrong, stalled, pub_secs, pub.stdout
    finally:
        for sp, sl in sidecars:
            stop(sp)
            sl.close()
        stop(server)
        slog.close()


if __name__ == "__main__":
    trials = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    stalls = 0
    results = []
    for t in range(1, trials + 1):
        print(f"=== trial {t}/{trials}: N=32 warmup+teardown ===", flush=True)
        run_n32_load()
        print(f"=== trial {t}/{trials}: immediate N=1 (JetStream publish) ===", flush=True)
        r = run_n1_jetstream()
        if r is None:
            print(f"  trial {t}: HARD FAILURE (setup error)")
            stalls += 1
            results.append((t, None, None, True, None))
            continue
        tot, wrong, stalled, pub_secs, flush_out = r
        marker = "STALL" if stalled else "clean"
        print(f"  trial {t}: total={tot}/{EXPECTED} wrong={wrong} pub_secs={pub_secs:.3f} [{marker}]")
        if stalled:
            stalls += 1
        results.append((t, tot, wrong, stalled, pub_secs))

    print(f"\n=== SUMMARY: {stalls}/{trials} trials stalled (JetStream stream_async+flush publish path) ===")
    for t, tot, wrong, stalled, pub_secs in results:
        print(f"  trial {t}: total={tot} wrong={wrong} stalled={stalled} pub_secs={pub_secs}")
