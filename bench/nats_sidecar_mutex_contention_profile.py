#!/usr/bin/env python3
"""Captures nats-server's real Go block/mutex-contention profile (runtime
block profiler, via the prof_block_rate config key - confirmed present in
nats-server v2.14.5's real source, github.com/nats-io/nats-server
server/server.go:2986 setBlockProfileRate()/opts.go:1342 "prof_block_rate" -
no patched binary needed) during a real N=16 JetStream+file nats_sidecar
run, to check whether RWMutex contention causes real goroutine blocking
beyond what today's earlier on-CPU `perf record` profile could see.

Reuses nats_sidecar_jetstream_storage_isolation.py's exact conventions
(same fixture, same sidecar launch flags) for the JetStream+file case only.
runtime.SetMutexProfileFraction is never called anywhere in nats-server's
source (checked directly) - true mutex-profile (as opposed to block-profile)
data is genuinely unavailable without patching the binary. Go's block
profile DOES capture sync.Mutex/RWMutex contention wait time (not just
channel blocking), confirmed by a smoke test showing sync.(*RWMutex).RLock
in a real captured stack - so this is the real, available substitute for
"mutex contention profile" using nats-server's own built-in instrumentation.
"""
import subprocess
import time
import shutil
import os
import sys
import urllib.request

NATS_SERVER = os.path.expanduser("~/nats-server")
SIDECAR = os.path.expanduser("~/nats_sidecar/build-ci/bin/nats_sidecar")
DRIVER = os.path.expanduser("~/pg_blazingmq/bench/mq_bench_driver")
PSQL = ["psql", "-h", "/var/run/postgresql", "postgres", "-q", "-c"]

EXPECTED = 21683
FILTER_FIELD = "Exchange"
FILTER_VALUE = "N"
INPUT_SUBJECT = "sc.real.in"
OUTPUT_PREFIX = "sc.real.out"
INPUT_STREAM = "SC_MUTEXPROF_INPUT"
DURABLE_NAME = "sc-mutexprof-durable"
DELIVER_SUBJECT = "sc.mutexprof.deliver"
DELIVER_GROUP = "sc-mutexprof-group"
PROF_PORT = 6061
N = 16


def start_server():
    store = "/tmp/nsc_mutexprof_store"
    if os.path.exists(store):
        shutil.rmtree(store)
    os.makedirs(store, exist_ok=True)
    conf_path = "/tmp/nsc_mutexprof_server.conf"
    with open(conf_path, "w") as f:
        f.write(f'port: 4222\njetstream {{ store_dir: "{store}" }}\n'
                f'prof_port: {PROF_PORT}\nprof_block_rate: 1\n')
    log = open("/tmp/nsc_mutexprof_server.log", "w")
    p = subprocess.Popen([NATS_SERVER, "-c", conf_path], stdout=log, stderr=subprocess.STDOUT)
    time.sleep(1.2)
    return p, log


def start_sidecar(i):
    log = open(f"/tmp/nsc_mutexprof_sidecar_{i}.log", "w")
    p = subprocess.Popen([
        SIDECAR, "-a", "127.0.0.1", "-p", "4222", "-i", INPUT_SUBJECT,
        "--input-stream", INPUT_STREAM, "--input-stream-storage", "file",
        "--consumer-durable-name", DURABLE_NAME,
        "--consumer-deliver-subject", DELIVER_SUBJECT,
        "--consumer-deliver-group", DELIVER_GROUP,
        "--consumer-max-ack-pending", "2000", "--consumer-ack-wait", "30",
        "--attr", f"{FILTER_FIELD}:string", "--engine", "atree",
        "--output-prefix", OUTPUT_PREFIX,
        "--subscribe-subject", f"sc.mutexprof.ctrl.{i}",
        "--workers", "2", "--lease-bucket", f"sc-mutexprof-leases-{i}",
    ], stdout=log, stderr=subprocess.STDOUT)
    return p, log


def stop(p):
    try:
        p.terminate(); p.wait(timeout=5)
    except Exception:
        try: p.kill(); p.wait(timeout=5)
        except Exception: pass


def run_ctrl(subscribe_subject):
    r = subprocess.run([DRIVER, "ctrl", subscribe_subject,
                         f'{FILTER_FIELD} = "{FILTER_VALUE}"'],
                        capture_output=True, text=True, timeout=10)
    return r.stdout.strip()


def fetch_block_profile(out_path):
    with urllib.request.urlopen(f"http://localhost:{PROF_PORT}/debug/pprof/block?debug=1", timeout=10) as r:
        data = r.read()
    with open(out_path, "wb") as f:
        f.write(data)
    return data


def main():
    server, slog = start_server()
    sidecars = []
    try:
        # Baseline block profile right after startup (idle housekeeping only).
        fetch_block_profile("/tmp/nsc_mutexprof_baseline.txt")

        for i in range(1, N + 1):
            sp, sl = start_sidecar(i)
            sidecars.append((sp, sl))
        time.sleep(1.5 + 0.2 * N)
        for sp, sl in sidecars:
            if sp.poll() is not None:
                print(f"N={N} instance exited early", file=sys.stderr)
                return 1

        for i in range(1, N + 1):
            reply = run_ctrl(f"sc.mutexprof.ctrl.{i}")
            if '"error"' in reply:
                print(f"N={N} ctrl FAILED: {reply}", file=sys.stderr)
                return 1
        time.sleep(0.3)

        sub = subprocess.Popen(
            [DRIVER, "subfield", f"{OUTPUT_PREFIX}.>", str(EXPECTED), "90000",
             FILTER_FIELD, FILTER_VALUE],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        time.sleep(0.3)

        pub_sql = (
            f"SELECT nats_publish_binary_stream_async('{INPUT_SUBJECT}', row_to_msgpack(t)) "
            f"FROM sc_bench_sample t; SELECT nats_publish_stream_flush();"
        )
        t0 = time.time()
        pub = subprocess.run(PSQL + [pub_sql], capture_output=True, text=True, timeout=120)
        t1 = time.time()
        if pub.returncode != 0:
            print(f"publish FAILED: {pub.stderr}", file=sys.stderr)
            return 1

        sub_out, _ = sub.communicate(timeout=100)
        t2 = time.time()

        # Capture the block profile immediately after the run completes,
        # while accumulated contention data is still fresh/relevant to this
        # specific run (block profile is cumulative since prof_block_rate
        # was set at startup, so baseline above lets us see what's new).
        fetch_block_profile("/tmp/nsc_mutexprof_after_run.txt")

        sub_line = sub_out.strip().splitlines()[-1] if sub_out.strip() else "NONE"
        tot = wrong = None
        for tok in sub_line.split():
            if "=" in tok:
                k, v = tok.split("=", 1)
                if k == "total": tot = v
                elif k == "wrong": wrong = v
        ok = (tot == str(EXPECTED)) and (wrong == "0")
        print(f"pub_secs={t1-t0:.2f} e2e_secs={t2-t0:.2f} total={tot} wrong={wrong} ok={ok}")
        return 0 if ok else 1
    finally:
        for sp, sl in sidecars:
            stop(sp); sl.close()
        stop(server); slog.close()


if __name__ == "__main__":
    sys.exit(main())
