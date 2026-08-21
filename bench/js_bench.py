#!/usr/bin/env python3
"""JetStream publish-then-drain benchmark, matching pg_blazingmq's
bmq_bench.py methodology: publish N rows to a persistent stream with NO
consumer attached, confirm they actually persisted, THEN start a pull
consumer and time how long it takes to drain the backlog - polling the
consumer's --dump file line count at high frequency rather than trusting
nats_tool's whole-second stats log (see bench/README.md's "NATS receive
rate was never real" section for why that matters).

Does NOT use nats_publish_from_sql.py, deliberately: that script's
delete_js_stream() cleanup runs unconditionally in a `finally` block
right after publish, which raced ahead of any attempt to drain the
stream separately - not a bug, just not the right tool for a
publish-then-drain-separately measurement. This script keeps the stream
alive between the publish and drain phases on purpose.

Stream storage is "memory", not "file", to match BlazingMQ's own
in-memory scratch domain config used throughout this benchmark suite -
both sides non-durable-across-restart, for a fair comparison.
"""
import argparse
import json
import re
import subprocess
import time
import uuid

import psycopg
import psycopg.sql


def js_req(nats_tool, subject, payload_obj, timeout=15):
    proc = subprocess.run(
        [nats_tool, "req", "--topic", subject, "--data", json.dumps(payload_obj)],
        capture_output=True, text=True, timeout=timeout,
    )
    for line in (proc.stdout or "").splitlines():
        if re.match(r"^\[\d{4}-\d{2}-\d{2}", line):
            continue
        _, bracket, body = line.partition("] ")
        if not bracket:
            continue
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            continue
    raise RuntimeError(f"no parseable reply for {subject!r}: {proc.stdout[:300]!r}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="host=/var/run/postgresql dbname=postgres")
    ap.add_argument("--sql", required=True)
    ap.add_argument("--limit", type=int, default=100000)
    ap.add_argument("--subject", default=None)
    ap.add_argument("--stream", default=None)
    ap.add_argument("--nats-tool", default="/home/mrayva/nats_asio/build/bin/nats_tool")
    ap.add_argument("--drain-timeout-secs", type=int, default=60)
    args = ap.parse_args()

    tag = uuid.uuid4().hex[:8]
    subject = args.subject or f"jsbench.{tag}"
    stream = args.stream or f"JSBENCH_{tag.upper()}"

    nt = args.nats_tool

    print(f"== creating stream {stream!r} (memory storage) for subject {subject!r} ==")
    resp = js_req(nt, f"$JS.API.STREAM.CREATE.{stream}", {
        "name": stream, "subjects": [subject], "retention": "limits",
        "storage": "memory", "max_msgs": -1, "max_bytes": -1, "max_age": 0,
        "max_msg_size": -1, "discard": "old", "num_replicas": 1,
    })
    if "error" in resp:
        raise RuntimeError(f"stream create failed: {resp['error']}")
    print("  stream ready")

    # Reference jsonb is projected from the *same* query execution/tuple as
    # the publish call, not a second separate SELECT afterward - a bare
    # unordered LIMIT against a 115M-row table is not guaranteed to return
    # the same row set twice (confirmed directly: a naive two-query version
    # of this script produced a 100%-mismatched reference at 100k rows,
    # despite passing cleanly at 500). Matches bmq_bench.py's/
    # nats_publish_from_sql.py's own "compute the payload once, reuse it"
    # principle.
    reference = []
    with psycopg.connect(args.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            publish_sql = psycopg.sql.SQL(
                "SELECT nats_publish_binary_stream_async({subject}, row_to_msgpack(t.*)), "
                "octet_length(row_to_msgpack(t.*)), to_jsonb(t.*) "
                "FROM ({user_sql} LIMIT {limit}) t"
            ).format(subject=psycopg.sql.Literal(subject),
                      user_sql=psycopg.sql.SQL(args.sql),
                      limit=psycopg.sql.Literal(args.limit))

            print(f"== publishing up to {args.limit} rows ==")
            t0 = time.monotonic()
            cur.execute(publish_sql)
            total_bytes = 0
            row_count = 0
            for _ignored, nbytes, ref in cur:
                total_bytes += nbytes
                reference.append(ref)
                row_count += 1
            cur.execute("SELECT nats_publish_stream_flush()")
            flushed = cur.fetchone()[0]
            publish_secs = time.monotonic() - t0
            avg_bytes = (total_bytes / row_count) if row_count else 0
            print(f"  published {row_count} row(s), flushed {flushed} ack(s) "
                  f"(remainder past the 3000-message auto-drain window) "
                  f"in {publish_secs:.3f}s ({row_count/publish_secs:,.0f}/s, "
                  f"avg {avg_bytes:.0f}B)")

    # Confirm real persistence before draining - the whole point of this
    # script vs. the earlier invalidated attempt.
    info = js_req(nt, f"$JS.API.STREAM.INFO.{stream}", {})
    persisted = info.get("state", {}).get("messages", -1)
    print(f"== stream state after publish: {persisted} message(s) persisted ==")
    if persisted != row_count:
        raise RuntimeError(f"persisted count {persisted} != published count {row_count} - "
                            f"not a valid backlog to drain, aborting")

    # Drain phase: pull consumer, dump to file, poll the dump file's line
    # count at high frequency instead of nats_tool's whole-second stats log.
    dump_path = f"/tmp/js_bench_dump_{tag}.jsonl"
    consumer = f"drain_{tag}"
    log_path = f"/tmp/js_bench_grub_{tag}.log"
    print(f"== draining {row_count} messages via js_grub pull consumer ==")
    proc = subprocess.Popen(
        [nt, "--mode", "js_grub", "--stream", stream, "--consumer", consumer,
         "--topic", subject, "--auto_ack", "--max_msgs", str(row_count),
         "--dump", dump_path, "--format", "msgpack", "--json"],
        stdout=open(log_path, "w"), stderr=subprocess.STDOUT,
    )

    t0 = time.monotonic()
    deadline = t0 + args.drain_timeout_secs
    last_count = 0
    while time.monotonic() < deadline:
        try:
            with open(dump_path, "rb") as f:
                last_count = f.read().count(b"\n")
        except FileNotFoundError:
            last_count = 0
        if last_count >= row_count:
            break
        time.sleep(0.01)
    drain_secs = time.monotonic() - t0
    # js_grub doesn't self-exit on --max_msgs the way plain grub does -
    # terminate explicitly once the poll loop confirms full delivery.
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)

    print(f"  drained {last_count}/{row_count} in {drain_secs:.4f}s "
          f"({last_count/drain_secs:,.0f}/s)" if drain_secs > 0 else "  instant")

    # Content-verify: compare dumped payloads (decoded) against the jsonb
    # reference collected during the publish pass above, as an unordered
    # multiset.
    with open(dump_path) as f:
        dumped = [json.loads(line) for line in f if line.strip()]

    def canon(v):
        # nats_tool's JSON rendering of a decoded msgpack float64 keeps a
        # trailing ".0" for whole-numbered values (e.g. 11.0); Postgres's
        # to_jsonb renders the same numeric value as a bare integer (11) -
        # both are the same number, just different JSON text for a
        # whole-valued float. Canonicalize so the comparison is numeric,
        # not textual.
        if isinstance(v, float) and v.is_integer():
            return int(v)
        if isinstance(v, dict):
            return {k: canon(x) for k, x in v.items()}
        if isinstance(v, list):
            return [canon(x) for x in v]
        return v

    def norm(d):
        return json.dumps({k: canon(d[k]) for k in sorted(d.keys())}, sort_keys=True, default=str)

    dumped_set = sorted(norm(d["payload"]) for d in dumped)
    ref_set = sorted(norm(r) for r in reference)
    verify = "PASS" if dumped_set == ref_set else f"FAIL (mismatched_groups={sum(a!=b for a,b in zip(dumped_set, ref_set))})"
    print(f"  content verify: {verify}")
    if verify != "PASS":
        shown = 0
        from collections import Counter
        dc, rc = Counter(dumped_set), Counter(ref_set)
        only_dumped = list((dc - rc).elements())
        only_ref = list((rc - dc).elements())
        for a, b in zip(sorted(only_dumped)[:3], sorted(only_ref)[:3]):
            print("  DUMPED-ONLY:", a)
            print("  REF-ONLY:   ", b)

    # Cleanup
    js_req(nt, f"$JS.API.STREAM.DELETE.{stream}", {})
    import os
    for p in (dump_path, log_path):
        try:
            os.remove(p)
        except FileNotFoundError:
            pass

    print()
    print("=== SUMMARY ===")
    print(f"rows: {row_count}  avg_bytes: {avg_bytes:.0f}")
    print(f"publish_rate: {row_count/publish_secs:,.0f}/s")
    print(f"drain_rate: {last_count/drain_secs:,.0f}/s" if drain_secs > 0 else "drain_rate: instant")
    print(f"verify: {verify}")


if __name__ == "__main__":
    main()
