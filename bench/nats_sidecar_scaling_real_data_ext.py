#!/usr/bin/env python3
"""Extension of nats_sidecar_scaling_real_data.py (bench/README.md commit
41d73be) to N=16 and N=32 instances, to see whether the ~6.49x scaling
found at N=8 with real NYSE data keeps climbing, plateaus, or declines -
and whether any such change is a genuine algorithmic effect or just this
24-hardware-thread machine running out of real cores to give each
instance dedicated capacity.

Reuses the exact same fixture (sc_bench_sample, 200,000 real rows),
filter (Exchange == "N", ground truth 21,683), and architecture as the
1/2/4/8 run. The one deliberate difference: --workers is reduced per N
to stay within the real 24-thread budget (2 per instance is what N<=8
used; 16 instances x 2 = 32 threads already exceeds budget, so N=16 uses
--workers 1; N=32 has no way to avoid oversubscription on this hardware
at all - tested anyway as an explicit "what happens past the core
budget" data point, with mpstat -P ALL run concurrently to prove/disprove
hardware saturation rather than assume it either way.
"""
import subprocess
import time
import shutil
import os
import sys
import threading

NATS_SERVER = os.path.expanduser("~/nats-server")
SIDECAR = os.path.expanduser("~/nats_sidecar/build-ci/bin/nats_sidecar")
DRIVER = os.path.expanduser("~/pg_blazingmq/bench/mq_bench_driver")
STORE_BASE = "/tmp/nsc_real_store_ext"
PSQL = ["psql", "-h", "/var/run/postgresql", "postgres", "-q", "-c"]

EXPECTED = 21683
FILTER_FIELD = "Exchange"
FILTER_VALUE = "N"
INPUT_SUBJECT = "sc.real.in"
OUTPUT_PREFIX = "sc.real.out"

WORKERS_FOR_N = {16: "1", 32: "1"}


def start_server(n):
    store = f"{STORE_BASE}_{n}"
    if os.path.exists(store):
        shutil.rmtree(store)
    os.makedirs(store, exist_ok=True)
    log = open(f"/tmp/nsc_real_server_ext_{n}.log", "w")
    p = subprocess.Popen([NATS_SERVER, "-js", "-sd", store, "-p", "4222"],
                          stdout=log, stderr=subprocess.STDOUT)
    time.sleep(1.2)
    return p, log


def start_sidecar(i, n, workers):
    log = open(f"/tmp/nsc_real_sidecar_ext_{n}_{i}.log", "w")
    p = subprocess.Popen([
        SIDECAR,
        "-a", "127.0.0.1", "-p", "4222",
        "-i", INPUT_SUBJECT,
        "--queue-group", "qg-real",
        "--attr", f"{FILTER_FIELD}:string",
        "--engine", "atree",
        "--output-prefix", OUTPUT_PREFIX,
        "--subscribe-subject", f"sc.real.ctrl.{i}",
        "--workers", workers,
        "--lease-bucket", f"sc-real-leases-ext-{n}",
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


class MpstatCapture:
    def __init__(self, path):
        self.path = path
        self.proc = None

    def start(self):
        f = open(self.path, "w")
        self._f = f
        self.proc = subprocess.Popen(["mpstat", "-P", "ALL", "1"], stdout=f, stderr=subprocess.STDOUT)

    def stop(self):
        if self.proc:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=3)
            except Exception:
                self.proc.kill()
        self._f.close()

    def summarize(self):
        # Report the max "all" row %idle (lower = busier) seen, and avg over samples.
        try:
            with open(self.path) as f:
                lines = f.readlines()
        except FileNotFoundError:
            return "no mpstat data"
        idles = []
        for ln in lines:
            parts = ln.split()
            if len(parts) >= 3 and parts[1] == "all":
                try:
                    idles.append(float(parts[-1]))
                except ValueError:
                    pass
        if not idles:
            return "no mpstat samples parsed"
        return f"all-core %idle samples: min={min(idles):.1f} avg={sum(idles)/len(idles):.1f} max={max(idles):.1f} n={len(idles)}"


def run_n(n):
    workers = WORKERS_FOR_N.get(n, "2")
    server, slog = start_server(n)
    sidecars = []
    mp = MpstatCapture(f"/tmp/nsc_mpstat_{n}.log")
    try:
        for i in range(1, n + 1):
            sp, sl = start_sidecar(i, n, workers)
            sidecars.append((sp, sl))
        time.sleep(3.0 if n >= 16 else 2.5)

        for i in range(1, n + 1):
            reply = run_ctrl(i)
            if '"error"' in reply:
                print(f"  N={n} instance {i} ctrl FAILED: {reply}", file=sys.stderr)
                return None
        time.sleep(0.3)

        sub = subprocess.Popen(
            [DRIVER, "subfield", f"{OUTPUT_PREFIX}.>", str(EXPECTED), "120000",
             FILTER_FIELD, FILTER_VALUE],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        time.sleep(0.3)

        # Start mpstat right before the actual publish/consume hot phase
        # (not before ctrl registration, whose subprocess-spawn overhead
        # would dilute the window with idle time and starve mpstat's 1s
        # minimum sampling interval of real hot-phase coverage - confirmed
        # empirically on the previous attempt). This version of mpstat
        # (sysstat 12.7.7) has no sub-second interval option, so the hot
        # phase itself (well under 1s) still can't be cleanly isolated -
        # holding the window open ~2.5s past completion trades some
        # dilution-by-idle-tail for at least 2-3 real samples instead of 1.
        mp.start()

        pub_sql = (
            f"SELECT nats_publish_binary('{INPUT_SUBJECT}', row_to_msgpack(t)) "
            f"FROM sc_bench_sample t;"
        )
        t0 = time.time()
        pub = subprocess.run(PSQL + [pub_sql], capture_output=True, text=True, timeout=180)
        t1 = time.time()
        if pub.returncode != 0:
            print(f"  N={n} publish FAILED: {pub.stderr}", file=sys.stderr)
            return None
        pub_secs = t1 - t0
        pub_rate = 200000 / pub_secs

        sub_out, _ = sub.communicate(timeout=130)
        time.sleep(2.5)  # let mpstat collect a couple more 1s samples past completion
        mp.stop()
        sub_line = sub_out.strip().splitlines()[-1] if sub_out.strip() else "NONE"

        return pub_rate, pub_secs, sub_line, mp.summarize(), workers
    finally:
        try:
            mp.stop()
        except Exception:
            pass
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
    for n in [16, 32]:
        print(f"=== N={n} (workers={WORKERS_FOR_N.get(n,'2')}) ===", flush=True)
        r = run_n(n)
        if r is None:
            print(f"N={n} FAILED")
            continue
        pub_rate, pub_secs, sub_line, mp_summary, workers = r
        print(f"  publish: {pub_rate:.0f} rows/s ({pub_secs:.2f}s)")
        print(" ", sub_line)
        print("  mpstat:", mp_summary)
        tot = parse(sub_line, "total")
        wrong = parse(sub_line, "wrong")
        rate = parse(sub_line, "rate")
        results.append((n, pub_rate, tot, wrong, rate, mp_summary, workers))
        time.sleep(2)

    print("\n=== SUMMARY (N=16/32 extension, real NYSE data, Exchange==N, 200000 rows, expected=21683) ===")
    print("N | workers | publish_rate(rows/s) | total | wrong | filtered_rate(matches/s) | mpstat")
    for n, pr, tot, wrong, rate, mp_summary, workers in results:
        print(f"{n} | {workers} | {pr:.0f} | {tot}/{EXPECTED} | {wrong} | {rate} | {mp_summary}")
