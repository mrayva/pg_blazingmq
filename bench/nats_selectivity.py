#!/usr/bin/env python3
"""Does NATS core's server-side subject-matching cost depend on a single
subscriber's pattern *selectivity* - broad wildcard vs a narrow,
fully-qualified subject? Never tested elsewhere in this repo's bench/ suite:
every other NATS benchmark here used one fixed subject/pattern.

Publishes a fixed workload (same count, same payload size, same subject
distribution) across a real trades.<region>.<symbol> hierarchy
(REGIONS x SYMBOLS concrete subjects) under four subscriber configurations:
none (baseline), broad (trades.>, matches everything), medium
(trades.<region0>.>, matches 1/REGIONS of traffic), narrow
(trades.<region0>.<symbol0>, matches 1/(REGIONS*SYMBOLS) of traffic).

Publisher and subscriber run as *separate OS processes*, each with its own
NATS connection - not one process publishing and receiving on the same
asyncio event loop. An earlier version of this script did that and measured
a confound, not server-side matching cost: with a broad pattern, the
publisher process was also processing 100,000 of its own inbound callback
invocations on the same single-threaded event loop it was timing the
publish loop on, so "broad" looked far more expensive than "narrow" purely
from client-side message-handling load scaling with match count, not
anything server-side. Separate processes remove that: the publisher's
wall-clock timing reflects only its own publish loop, on a connection that
never receives anything back.

Timing is direct wall-clock around the publish loop (t1-t0, with a final
nc.flush() to make sure every publish has actually left the client before
stopping the clock) - not nats_tool's periodic stats-log timer, which this
repo's own bench/README.md already found to be a whole-second-granularity
artifact, not a real measurement.

Caveat, stated plainly: this uses nats-py (pure Python asyncio), not the
repo's usual C++ nats_tool - so absolute rates here are Python-client-bound,
not comparable to nats_tool's own ~90k-8M/s numbers elsewhere in this
document. The controlled variable is subscriber selectivity with every
other part of the methodology (client, payload, count, machine) held fixed
across the sweep, so relative differences between selectivity levels are
the meaningful signal, not the absolute rate.

Usage: this script IS both the publisher and the subscriber, selected via
--role. The orchestrating shell/caller is responsible for restarting
nats-server fresh between configurations (this repo's established
discipline - back-to-back runs on a warm process were found to skew
BlazingMQ numbers ~2x) and for starting the subscriber process before the
publisher process for filtered configurations.
"""
import argparse
import asyncio
import json
import time

import nats

REGIONS = 8
SYMBOLS = 200
TOTAL_SUBJECTS = REGIONS * SYMBOLS


def subject_for(i: int) -> str:
    region = i % REGIONS
    symbol = (i // REGIONS) % SYMBOLS
    return f"trades.r{region}.s{symbol}"


def expected_matches(count: int, pattern: str) -> int:
    if pattern == "trades.>":
        return count
    if pattern == "trades.r0.>":
        return len(range(0, count, REGIONS))
    if pattern == "trades.r0.s0":
        return len(range(0, count, TOTAL_SUBJECTS))
    raise ValueError(f"no expected-match formula for pattern {pattern!r}")


async def run_publisher(server_url: str, count: int, payload_size: int):
    nc = await nats.connect(server_url)
    payload = b"x" * payload_size

    t0 = time.monotonic()
    for i in range(count):
        await nc.publish(subject_for(i), payload)
    await nc.flush()
    t1 = time.monotonic()
    await nc.close()

    publish_secs = t1 - t0
    publish_rate = count / publish_secs if publish_secs > 0 else float("inf")
    print(json.dumps({
        "role": "publisher",
        "count": count,
        "publish_secs": round(publish_secs, 4),
        "publish_rate": round(publish_rate, 1),
        "publish_end_monotonic": t1,
    }))


async def run_subscriber(server_url: str, count: int, pattern: str, ready_file: str,
                          settle_timeout_s: float):
    nc = await nats.connect(server_url)

    received = 0
    last_recv_monotonic = None

    async def handler(msg):
        nonlocal received, last_recv_monotonic
        received += 1
        last_recv_monotonic = time.monotonic()

    await nc.subscribe(pattern, cb=handler)
    await nc.flush()  # make sure the SUB has actually round-tripped to the server

    # Signal the orchestrator this process is ready for the publisher to start.
    with open(ready_file, "w") as f:
        f.write("ready\n")

    expected = expected_matches(count, pattern)
    deadline = time.monotonic() + settle_timeout_s
    while received < expected and time.monotonic() < deadline:
        await asyncio.sleep(0.01)

    await nc.close()
    print(json.dumps({
        "role": "subscriber",
        "pattern": pattern,
        "expected_matches": expected,
        "received": received,
        "match_ok": received == expected,
        "last_recv_monotonic": last_recv_monotonic,
    }))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default="nats://127.0.0.1:4222")
    ap.add_argument("--count", type=int, default=100000)
    ap.add_argument("--payload-size", type=int, default=128)
    ap.add_argument("--role", choices=["publisher", "subscriber"], required=True)
    ap.add_argument("--pattern", default=None, help="Required for --role subscriber")
    ap.add_argument("--ready-file", default=None, help="Required for --role subscriber")
    ap.add_argument("--settle-timeout", type=float, default=15.0)
    args = ap.parse_args()

    if args.role == "publisher":
        asyncio.run(run_publisher(args.server, args.count, args.payload_size))
    else:
        assert args.pattern and args.ready_file
        asyncio.run(run_subscriber(args.server, args.count, args.pattern, args.ready_file,
                                    args.settle_timeout))


if __name__ == "__main__":
    main()
