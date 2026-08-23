#!/usr/bin/env python3
"""Clean, same-day, back-to-back N=1,4,8,16,32 throughput comparison:
old plain-`--queue-group` nats_sidecar design vs new JetStream-durable-
consumer design (commit 1331434), run on the identical system in the same
session to settle whether the two designs' throughput numbers are really
comparable, and to capture a true end-to-end rate (publish-start until the
subscriber confirms all 21,683 expected matches) alongside each design's
existing publish-statement-only rate for continuity with prior scripts.

Reuses the exact same real fixture/ground-truth/CLI patterns as
nats_sidecar_scaling_real_data.py (old design) and
nats_sidecar_jetstream_consumer.py (new design) - not a new benchmark
methodology, just both designs measured back-to-back with an added
end-to-end timer neither existing script currently has.
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

INPUT_STREAM = "SC_CLEAN_INPUT"
DURABLE_NAME = "sc-clean-durable"
DELIVER_SUBJECT = "sc.clean.deliver"
DELIVER_GROUP = "sc-clean-group"


def start_server(tag):
    store = f"/tmp/nsc_clean_store_{tag}"
    if os.path.exists(store):
        shutil.rmtree(store)
    os.makedirs(store, exist_ok=True)
    log = open(f"/tmp/nsc_clean_server_{tag}.log", "w")
    p = subprocess.Popen([NATS_SERVER, "-js", "-sd", store, "-p", "4222"],
                          stdout=log, stderr=subprocess.STDOUT)
    time.sleep(1.2)
    return p, log


def start_sidecar_plain(i, tag):
    log = open(f"/tmp/nsc_clean_sidecar_plain_{tag}_{i}.log", "w")
    p = subprocess.Popen([
        SIDECAR,
        "-a", "127.0.0.1", "-p", "4222",
        "-i", INPUT_SUBJECT,
        "--queue-group", "qg-clean",
        "--attr", f"{FILTER_FIELD}:string",
        "--engine", "atree",
        "--output-prefix", OUTPUT_PREFIX,
        "--subscribe-subject", f"sc.clean.plain.ctrl.{i}",
        "--workers", "2",
        "--lease-bucket", f"sc-clean-plain-leases-{tag}",
    ], stdout=log, stderr=subprocess.STDOUT)
    return p, log


def start_sidecar_js(i, tag, max_ack_pending=2000):
    log = open(f"/tmp/nsc_clean_sidecar_js_{tag}_{i}.log", "w")
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
        "--subscribe-subject", f"sc.clean.js.ctrl.{tag}.{i}",
        "--workers", "2",
        "--lease-bucket", f"sc-clean-js-leases-{tag}",
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


def run_one(n, tag, mode):
    """mode: 'plain' or 'js'. Returns dict or None on failure."""
    server, slog = start_server(tag)
    sidecars = []
    try:
        for i in range(1, n + 1):
            if mode == "plain":
                sp, sl = start_sidecar_plain(i, tag)
                ctrl_subject = f"sc.clean.plain.ctrl.{i}"
            else:
                sp, sl = start_sidecar_js(i, tag)
                ctrl_subject = f"sc.clean.js.ctrl.{tag}.{i}"
            sidecars.append((sp, sl, ctrl_subject))
        time.sleep(1.5 + 0.2 * n)
        for sp, sl, _ in sidecars:
            if sp.poll() is not None:
                print(f"  [{mode}] N={n} instance exited early", file=sys.stderr)
                return None

        for _, _, ctrl_subject in sidecars:
            reply = run_ctrl(ctrl_subject)
            if '"error"' in reply:
                print(f"  [{mode}] N={n} ctrl FAILED: {reply}", file=sys.stderr)
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

        t_start = time.time()
        pub = subprocess.run(PSQL + [pub_sql], capture_output=True, text=True, timeout=120)
        t_pub_done = time.time()
        if pub.returncode != 0:
            print(f"  [{mode}] N={n} publish FAILED: {pub.stderr}", file=sys.stderr)
            return None
        pub_secs = t_pub_done - t_start
        pub_rate = 200000 / pub_secs

        sub_out, _ = sub.communicate(timeout=100)
        t_e2e_done = time.time()
        e2e_secs = t_e2e_done - t_start
        e2e_rate = 200000 / e2e_secs if e2e_secs > 0 else float("nan")

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

        return {
            "pub_secs": pub_secs, "pub_rate": pub_rate,
            "e2e_secs": e2e_secs, "e2e_rate": e2e_rate,
            "total": tot, "wrong": wrong, "ok": ok,
        }
    finally:
        for sp, sl, _ in sidecars:
            stop(sp)
            sl.close()
        stop(server)
        slog.close()


def run_n_avg(n, mode, runs=2):
    results = []
    for r in range(runs):
        res = run_one(n, f"{mode}{n}_{r}", mode)
        if res is None or not res["ok"]:
            print(f"  [{mode}] N={n} run {r}: FAILED/incorrect ({res})", file=sys.stderr)
            continue
        results.append(res)
        print(f"  [{mode}] N={n} run {r}: pub_rate={res['pub_rate']:.0f}/s "
              f"e2e_rate={res['e2e_rate']:.0f}/s total={res['total']} wrong={res['wrong']}")
    return results


def main():
    ns = (1, 4, 8, 16, 32)
    table = {}
    for mode in ("plain", "js"):
        print(f"=== mode={mode} ===")
        for n in ns:
            results = run_n_avg(n, mode, runs=2)
            table[(mode, n)] = results

    print("\n=== SUMMARY ===")
    print(f"{'N':>3}  {'plain pub/s':>12}  {'plain e2e/s':>12}  {'js pub/s':>12}  {'js e2e/s':>12}")
    for n in ns:
        def fmt(mode):
            rs = table[(mode, n)]
            if not rs:
                return "FAIL", "FAIL"
            pub = sum(r["pub_rate"] for r in rs) / len(rs)
            e2e = sum(r["e2e_rate"] for r in rs) / len(rs)
            return f"{pub:.0f}", f"{e2e:.0f}"
        pp, pe = fmt("plain")
        jp, je = fmt("js")
        print(f"{n:>3}  {pp:>12}  {pe:>12}  {jp:>12}  {je:>12}")


if __name__ == "__main__":
    main()
