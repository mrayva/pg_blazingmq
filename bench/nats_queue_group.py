#!/usr/bin/env python3
"""Does a NATS core queue group actually deliver "horizontal scaling" as
consumer count grows, or does it hit a collapse-under-live-contention wall
the way BlazingMQ's directly-analogous "priority mode" did when rigorously
tested elsewhere in this repo (see bench/README.md - 1/2/4/8/16/32
consumers swept, dispatcher pools matched to session count each time,
every count landing on the same ~42-48k/s ceiling, with 2 and 4 consumers
actively *collapsing* under live-publish contention to ~3,999/s and
~32,000/s respectively)?

A "queue group" is NATS core's own round-robin distribution mechanism:
N consumers subscribe to the same subject under the same queue-group name,
and the server delivers each message to exactly one member, chosen
round-robin - structurally the same idea as BlazingMQ priority mode's
per-message consumer selection, just without any persistence/ack/backlog
machinery underneath it.

Publisher and every consumer run as *separate OS processes*, each with its
own NATS connection - the same fix already applied in nats_selectivity.py
after an earlier version of that script conflated publisher-side and
subscriber-side cost by running both on one event loop.

Correctness, not just speed, is checked: every published message carries
a sequence id; each consumer records the ids it actually received; the
orchestrator verifies the union across all consumers is exactly
{0..count-1} with no duplicates and no drops before trusting any rate.

Usage: this script IS both the publisher and every consumer, selected via
--role. The orchestrating shell driving a full sweep is responsible for
restarting nats-server fresh between N values (this repo's established
discipline - back-to-back runs on a warm process skew numbers) and for
starting every consumer process before the publisher process (queue-group
membership, like any NATS core subscription, only affects messages
published after the subscribe call - there is no backlog for a late
joiner).
"""
import argparse
import asyncio
import json
import struct
import sys
import time

import nats

SUBJECT = "bench.qg.data"
STOP_SUBJECT = "bench.qg.stop"
GROUP = "bench-qg"


async def run_publisher(server_url: str, count: int, payload_size: int, n_consumers: int):
    # Readiness is confirmed by the orchestrating shell via marker files
    # BEFORE this process is even launched, not via a NATS message - a
    # "ready" NATS message has the same non-retroactive-subscription race
    # as the benchmarked subscription itself (a message published before
    # the reader's subscribe() call lands is simply never delivered), so
    # coordinating readiness over the very channel being benchmarked would
    # risk silently proving nothing. n_consumers is accepted for the
    # result payload only.
    nc = await nats.connect(server_url)
    pad = b"x" * max(0, payload_size - 8)
    t0 = time.monotonic()
    for i in range(count):
        await nc.publish(SUBJECT, struct.pack(">Q", i) + pad)
    await nc.flush()
    t1 = time.monotonic()

    # Broadcast STOP (not queue-grouped - every consumer sees it) so
    # consumers know publishing is done and can start their drain timeout.
    await nc.publish(STOP_SUBJECT, b"")
    await nc.flush()
    await nc.close()

    publish_secs = t1 - t0
    publish_rate = count / publish_secs if publish_secs > 0 else float("inf")
    print(json.dumps({
        "role": "publisher", "count": count, "n_consumers": n_consumers,
        "publish_secs": publish_secs, "publish_rate": publish_rate,
    }))


async def run_consumer(server_url: str, result_path: str, ready_path: str, drain_grace_secs: float):
    nc = await nats.connect(server_url)
    received_ids = []
    first_recv = None
    last_recv = None

    async def on_msg(msg):
        nonlocal first_recv, last_recv
        now = time.monotonic()
        if first_recv is None:
            first_recv = now
        last_recv = now
        (seq,) = struct.unpack(">Q", msg.data[:8])
        received_ids.append(seq)

    await nc.subscribe(SUBJECT, queue=GROUP, cb=on_msg)

    stopped = asyncio.Event()

    async def on_stop(msg):
        stopped.set()

    stop_sub = await nc.subscribe(STOP_SUBJECT, cb=on_stop)
    await nc.flush()

    # Readiness signaled via a filesystem marker, not a NATS message - see
    # run_publisher()'s comment for why. Written only after both
    # subscriptions are confirmed live (the flush() above waits for the
    # server's ack of both SUB commands).
    with open(ready_path, "w") as f:
        f.write("ready")

    try:
        await asyncio.wait_for(stopped.wait(), timeout=120)
    except asyncio.TimeoutError:
        pass
    # Grace period to catch any messages still in flight when STOP arrived.
    await asyncio.sleep(drain_grace_secs)
    await stop_sub.unsubscribe()
    await nc.close()

    with open(result_path, "w") as f:
        json.dump({
            "count": len(received_ids),
            "ids": received_ids,
            "first_recv": first_recv,
            "last_recv": last_recv,
        }, f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--role", required=True, choices=["publisher", "consumer"])
    ap.add_argument("--server", default="nats://127.0.0.1:4222")
    ap.add_argument("--count", type=int, default=100000)
    ap.add_argument("--payload-size", type=int, default=64)
    ap.add_argument("--n-consumers", type=int, default=1)
    ap.add_argument("--result-path")
    ap.add_argument("--ready-path")
    ap.add_argument("--drain-grace-secs", type=float, default=1.0)
    args = ap.parse_args()

    if args.role == "publisher":
        asyncio.run(run_publisher(args.server, args.count, args.payload_size, args.n_consumers))
    else:
        if not args.result_path or not args.ready_path:
            print("consumer requires --result-path and --ready-path", file=sys.stderr)
            sys.exit(1)
        asyncio.run(run_consumer(args.server, args.result_path, args.ready_path, args.drain_grace_secs))


if __name__ == "__main__":
    main()
