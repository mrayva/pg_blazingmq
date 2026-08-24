#!/usr/bin/env python3
"""Differential perf profile of nats-server itself during a real nats_sidecar
run, plain vs JetStream+file vs JetStream+memory, to root-cause whether the
~2.6-2.9x JetStream-durable-consumer throughput cost (found in
nats_sidecar_jetstream_storage_isolation.py) traces to the same
(*client).flushOutbound + sync.RWMutex mechanism already found for plain
NATS's raw connection-scaling ceiling, or a distinct JetStream-specific code
path (ack processing, consumer/stream tracking, etc.).

Reuses nats_sidecar_throughput_clean.py's/nats_sidecar_jetstream_storage_
isolation.py's exact server/sidecar/publish/ground-truth machinery - this
script only adds `perf record -p <nats-server-pid>` around the publish-to-
confirmed-match window and a `perf report` summary afterward.
"""
import subprocess
import time
import shutil
import os
import sys

NATS_SERVER = os.path.expanduser("~/nats-server")
SIDECAR = os.path.expanduser("~/nats_sidecar/build-ci/bin/nats_sidecar")
DRIVER = os.path.expanduser("~/pg_blazingmq/bench/mq_bench_driver")
PSQL = ["psql", "-h", "/var/run/postgresql", "postgres", "-q", "-c"]

EXPECTED = 21683
FILTER_FIELD = "Exchange"
FILTER_VALUE = "N"
INPUT_SUBJECT = "sc.real.in"
OUTPUT_PREFIX = "sc.real.out"

INPUT_STREAM = "SC_PERF_INPUT"
DURABLE_NAME = "sc-perf-durable"
DELIVER_SUBJECT = "sc.perf.deliver"
DELIVER_GROUP = "sc-perf-group"

N = 16


def start_server(tag):
    store = f"/tmp/nsc_perf_store_{tag}"
    if os.path.exists(store):
        shutil.rmtree(store)
    os.makedirs(store, exist_ok=True)
    log = open(f"/tmp/nsc_perf_server_{tag}.log", "w")
    p = subprocess.Popen([NATS_SERVER, "-js", "-sd", store, "-p", "4222"],
                          stdout=log, stderr=subprocess.STDOUT)
    time.sleep(1.2)
    return p, log


def start_sidecar_plain(i, tag):
    log = open(f"/tmp/nsc_perf_sidecar_{tag}_{i}.log", "w")
    p = subprocess.Popen([
        SIDECAR, "-a", "127.0.0.1", "-p", "4222",
        "-i", INPUT_SUBJECT, "--queue-group", "qg-perf",
        "--attr", f"{FILTER_FIELD}:string", "--engine", "atree",
        "--output-prefix", OUTPUT_PREFIX,
        "--subscribe-subject", f"sc.perf.plain.ctrl.{i}",
        "--workers", "2", "--lease-bucket", f"sc-perf-plain-leases-{tag}",
    ], stdout=log, stderr=subprocess.STDOUT)
    return p, log


def start_sidecar_js(i, tag, storage):
    log = open(f"/tmp/nsc_perf_sidecar_{tag}_{i}.log", "w")
    p = subprocess.Popen([
        SIDECAR, "-a", "127.0.0.1", "-p", "4222",
        "-i", INPUT_SUBJECT,
        "--input-stream", INPUT_STREAM,
        "--input-stream-storage", storage,
        "--consumer-durable-name", DURABLE_NAME,
        "--consumer-deliver-subject", DELIVER_SUBJECT,
        "--consumer-deliver-group", DELIVER_GROUP,
        "--consumer-max-ack-pending", "2000",
        "--consumer-ack-wait", "30",
        "--attr", f"{FILTER_FIELD}:string", "--engine", "atree",
        "--output-prefix", OUTPUT_PREFIX,
        "--subscribe-subject", f"sc.perf.js.ctrl.{tag}.{i}",
        "--workers", "2", "--lease-bucket", f"sc-perf-js-leases-{tag}",
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


def run_profiled(mode, tag, perf_out):
    """mode: 'plain' or 'js-file' or 'js-memory'."""
    server, slog = start_server(tag)
    sidecars = []
    perf_proc = None
    try:
        for i in range(1, N + 1):
            if mode == "plain":
                sp, sl = start_sidecar_plain(i, tag)
                ctrl_subject = f"sc.perf.plain.ctrl.{i}"
            else:
                storage = "file" if mode == "js-file" else "memory"
                sp, sl = start_sidecar_js(i, tag, storage)
                ctrl_subject = f"sc.perf.js.ctrl.{tag}.{i}"
            sidecars.append((sp, sl, ctrl_subject))
        time.sleep(1.5 + 0.2 * N)
        for sp, sl, _ in sidecars:
            if sp.poll() is not None:
                print(f"  [{mode}] instance exited early", file=sys.stderr)
                return None

        for _, _, ctrl_subject in sidecars:
            reply = run_ctrl(ctrl_subject)
            if '"error"' in reply:
                print(f"  [{mode}] ctrl FAILED: {reply}", file=sys.stderr)
                return None
        time.sleep(0.3)

        sub = subprocess.Popen(
            [DRIVER, "subfield", f"{OUTPUT_PREFIX}.>", str(EXPECTED), "90000",
             FILTER_FIELD, FILTER_VALUE],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        time.sleep(0.3)

        if mode == "plain":
            pub_sql = (
                f"SELECT nats_publish_binary('{INPUT_SUBJECT}', row_to_msgpack(t)) "
                f"FROM sc_bench_sample t;"
            )
        else:
            pub_sql = (
                f"SELECT nats_publish_binary_stream_async('{INPUT_SUBJECT}', row_to_msgpack(t)) "
                f"FROM sc_bench_sample t; "
                f"SELECT nats_publish_stream_flush();"
            )

        # Start perf record attached to the real nats-server PID right before
        # publish begins, covering the whole publish-to-confirmed-match window.
        perf_log = open(f"/tmp/nsc_perf_{mode}.log", "w")
        perf_proc = subprocess.Popen(
            ["perf", "record", "-p", str(server.pid), "-g", "--call-graph", "fp",
             "-o", perf_out, "--"],
            stdout=perf_log, stderr=subprocess.STDOUT)
        # perf record with a trailing "--" and no command just profiles the
        # -p target until signaled; give it a moment to attach.
        time.sleep(0.3)

        t_start = time.time()
        pub = subprocess.run(PSQL + [pub_sql], capture_output=True, text=True, timeout=120)
        if pub.returncode != 0:
            print(f"  [{mode}] publish FAILED: {pub.stderr}", file=sys.stderr)
            return None

        sub_out, _ = sub.communicate(timeout=100)
        t_e2e_done = time.time()
        e2e_secs = t_e2e_done - t_start
        e2e_rate = 200000 / e2e_secs if e2e_secs > 0 else float("nan")

        # Stop perf cleanly so it flushes perf.data.
        perf_proc.send_signal(2)  # SIGINT
        try:
            perf_proc.wait(timeout=10)
        except Exception:
            perf_proc.terminate()
            perf_proc.wait(timeout=5)
        perf_log.close()

        sub_line = sub_out.strip().splitlines()[-1] if sub_out.strip() else "NONE"
        tot = wrong = None
        for tok in sub_line.split():
            if "=" in tok:
                k, v = tok.split("=", 1)
                if k == "total":
                    tot = v
                elif k == "wrong":
                    wrong = v
        ok = (tot == str(EXPECTED)) and (wrong == "0")

        print(f"  [{mode}] e2e_rate={e2e_rate:.0f}/s total={tot} wrong={wrong} ok={ok}")
        return {"e2e_rate": e2e_rate, "total": tot, "wrong": wrong, "ok": ok}
    finally:
        if perf_proc is not None and perf_proc.poll() is None:
            perf_proc.terminate()
        for sp, sl, _ in sidecars:
            stop(sp)
            sl.close()
        stop(server)
        slog.close()


def main():
    modes = [("plain", "/tmp/perf_plain.data"),
             ("js-file", "/tmp/perf_jsfile.data"),
             ("js-memory", "/tmp/perf_jsmemory.data")]
    for mode, perf_out in modes:
        print(f"=== profiling mode={mode} N={N} ===")
        res = run_profiled(mode, mode, perf_out)
        if res is None or not res["ok"]:
            print(f"  [{mode}] FAILED/incorrect: {res}", file=sys.stderr)


if __name__ == "__main__":
    main()
