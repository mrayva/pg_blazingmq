# Benchmark

Ad hoc publish/consume throughput benchmark for `pg_blazingmq`, mirroring
[`pgnats`](https://github.com/mrayva/pgnats)'s own
`scripts/nats_publish_from_sql.py` methodology (same "publish rate" /
"receive rate" definitions - see that script's docstring) for a fair
side-by-side comparison against NATS core, one message per row, both
msgpack.

## Quick Start

Requires a running broker (`../test/manage_broker.sh start`) and
`pg_blazingmq`/`pg_zerialize` both `CREATE EXTENSION`'d, plus `psycopg` (v3):

```bash
python3 bmq_bench.py \
    --sql 'SELECT * FROM nyse_eqy_us_all_trade_20260102' \
    --limit 100000
```

`--attr-columns` (default `Sequence Number,Trade Id`) picks which columns
get promoted to filterable message properties via `bmq_publish_row`'s
`attr_columns`, matching the amount of per-row work `nats_publish_from_sql.py`
does building its NATS subject from 2 real columns - keeps the two
benchmarks doing comparable per-row work, not just comparable payloads.

## What It Measures

- **Publish rate**: pure database-side timing (encode + `bmq_publish_row()`),
  wrapped in a single `SELECT count(*) FROM (...)` so no row crosses back to
  the client - directly comparable to `nats_publish_from_sql.py`'s own
  publish-rate definition.
- **Receive rate**: wall-clock time around one `bmq_consume()` call pulling
  everything back. Note this isn't measured the same way as pgnats'
  receive rate (which polls `nats_tool`'s periodic stats log, with a ~1s
  granularity floor) - `bmq_consume()` is a synchronous pull call that
  blocks until it has `max_messages` or times out, so timing it directly
  is both the natural fit for its API shape and strictly more precise than
  polling a once-per-second log line. Keep this difference in mind when
  comparing the two numbers.
- **Verify**: republishes the same rows to a second queue, consumes them
  back, decodes via `msgpack_to_jsonb`, and compares against a
  `to_jsonb()` reference projection as an unordered multiset (same
  content-not-position philosophy as pgnats' own `--verify`).

## A Real Broker-Config Gotcha

The domain configs BlazingMQ's own `docker/single-node/config` ships (also
what `../test/manage_broker.sh`'s scratch broker uses for `make test`) cap
`bmq.test.mem.priority` at `queueLimits.messages: 1000` - sized for
`pg_regress`'s own handful of test rows, not a 100k-row throughput run.
Publishing past that limit doesn't error - the broker just silently stops
storing further messages for that queue (confirmed via the broker's own
log: `CAPACITY_STATE_FULL`), so `bmq_consume()` afterward returns fewer
rows than were "published" without ever raising an error. Either raise
`domainLimits`/`queueLimits` in a copy of that domain config for large
runs, or keep `--limit` under the configured ceiling.

## Sample Result (100,000 rows, `nyse_eqy_us_all_trade_20260102`, msgpack, single machine)

| system                          | rows    | avg bytes | publish rate | receive rate |
|----------------------------------|---------|-----------|---------------|---------------|
| pg_blazingmq (BlazingMQ, priority, immediate confirm)  | 100,000 | 357B      | 58,319/s      | 30,653/s      |
| pg_blazingmq (BlazingMQ, priority, batch_confirm=true) | 100,000 | 355B      | 60,741/s      | 53,428/s      |
| pg_blazingmq (BlazingMQ, broadcast, immediate confirm) | 100,000 | 356B      | 57,755/s      | 73,294/s      |
| pgnats (NATS core)               | 100,000 | 356B      | 69,925/s      | 221,868/s     |

Both `bmq.test.mem.priority` and `bmq.test.mem.broadcast` (this domain's
own config, `docker/single-node/config/domains/bmq.test.mem.broadcast.json`
in the BlazingMQ checkout) use `inMemory` storage and `eventual`
consistency - so this is **not** a persisted-vs-non-persisted comparison;
both BlazingMQ modes here are already non-durable, same as NATS core.

Switching from priority mode to broadcast mode (fire-and-forget best-effort
fan-out, no per-consumer positional backlog tracking) left publish rate
essentially unchanged (~58k/s either way - publish-side cost is dominated
by encoding and the publish protocol itself, not consumer-side bookkeeping)
but raised receive rate ~2.4x (30,653/s -> 73,294/s). That confirms
priority mode's per-consumer positional queue bookkeeping was a real,
measurable contributor to the original gap.

Confirming per message is *not* actually mandatory - `confirmMessage()` is
documented as asynchronous, and BlazingMQ has a real batch API
(`bmqa::ConfirmEventBuilder` / `session.confirmMessages()`) built exactly
for this. `bmq_consume(..., batch_confirm => true)` (0.5+) uses it, and on
the same priority-mode queue it very nearly closed the broadcast-mode gap
without changing queue mode at all: 30,653/s -> 53,428/s, a ~1.75x
improvement from batching alone. That isolates immediate-per-message
confirmation as the single largest driver of the original priority-mode
gap - bigger than the queue-mode bookkeeping difference above.

**A real gotcha this uncovered**: naively deferring *every* confirm to one
flush at the very end of a large pull doesn't just underperform, it
deadlocks. BlazingMQ's broker enforces a default per-consumer flow-control
window (`bmqt::QueueOptions::k_DEFAULT_MAX_UNCONFIRMED_MESSAGES = 1000`) -
once that many messages are outstanding unconfirmed, it stops sending more
until some are confirmed. A first implementation that confirmed only after
the whole receive loop finished stalled permanently at exactly 1000
messages on a `max_messages` pull larger than that, because nothing ever
confirmed anything to reopen the window. Fixed by flushing the batch
periodically (every `kBatchConfirmFlushThreshold = 500` messages, in
`pg_blazingmq.cpp`) - comfortably under the default window - not just at
the end.

Even with batching, a real gap to NATS core's 221,868/s receive rate
remains. The likely remaining driver is general wire-protocol and
broker-side richness (subscription property evaluation via `bmqeval`,
watermark/limit accounting, the CONFIRM protocol messages themselves -
still sent, just batched) that NATS core's minimal fire-and-forget
protocol simply doesn't do. That's the real tradeoff `pg_blazingmq` is
for - durability options and BlazingMQ's own server-side subscription
filtering - not raw throughput parity with a minimal core pub/sub system.

(Broadcast-mode queues only deliver to consumers already connected at
publish time, unlike priority mode's queued pull-consumption for
not-yet-connected readers - this benchmark's single-session, one-handle-
per-queue-URI design in `pg_blazingmq` already opens the queue with
combined read+write flags on the very first `bmq_publish_row()` call, so
the read side is established before any message is published and this
script needed no changes to work correctly under broadcast mode; verified
via a 50-row dry run before the full 100k run.)

## Broadcast + batch_confirm combined, and a properties-free isolation

Broadcast mode and `batch_confirm` are independent wins (queue-mode
bookkeeping vs. confirm-protocol overhead) and hadn't been tested
together. On a **freshly restarted broker**, combining them reaches
**81,623/s receive** - the best number measured so far, though still well
below NATS core's 221,868/s.

The next hypothesis was that `bmq_publish_row`'s per-message
`MessageProperties` (`--attr-columns`) were themselves adding encoding/
decoding overhead independent of queue mode or confirmation, since NATS
core has no equivalent concept at all. `--attr-columns ""` (added to
`bmq_bench.py` - an *empty* array, not `NULL`: passing `NULL`/omitting
the argument means "auto-promote every eligible column", not "no
properties" - `bmq_publish_row`'s `attrs_explicit` flag only suppresses
promotion given an *explicit* empty array) tests this directly.

**Result: the opposite of the hypothesis.** On matched fresh-broker runs:

| variant | publish rate | receive rate |
|---|---|---|
| broadcast + batch_confirm, **with** properties | 58,063/s | **81,623/s** |
| broadcast + batch_confirm, **without** properties | 99,133/s | 46,121/s |

Removing properties nearly doubles *publish* rate (less to encode, as
expected) but *receive* rate drops by nearly half instead of improving.
Property-encoding is not the remaining bottleneck on the receive side -
if anything, its presence correlates with faster delivery, plausibly
because it changes how the broker batches messages into `bmqa::Event`s
per `nextEvent()` call (unconfirmed - would need broker-side
instrumentation to prove, out of scope here).

**A separate, important methodology finding surfaced while chasing this
down**: repeated benchmark runs against the *same* broker process
degrade significantly - a first run against a freshly started broker
consistently lands around 81-99k/s, while a second run immediately after
(different queue URI, same broker, same variant) drops to 35-46k/s,
regardless of which variant is tested. This is a real confound for *any*
future pg_blazingmq benchmarking in this repo: comparisons across
variants are only trustworthy with a **fresh broker restart between each
one** - back-to-back runs in one broker session are not a fair
comparison, since broker-session state accumulation dominates the
variance far more than the variable actually being tested. All numbers
in this section used a fresh restart per variant; the numbers earlier in
this file (broadcast-mode section, batch_confirm section) were not
controlled this way and may be understating both variants somewhat
consistently, though the *relative* comparisons in each of those
sections held up under an immediate same-broker re-test.

## Final, Fully-Controlled Comparison (Supersedes All Numbers Above)

Every number above was measured under a mix of conditions - some with a
fresh broker restart immediately before, some not, and the NATS/pgnats
reference number was never re-verified against a freshly restarted
`nats-server` at all. This section re-runs **every** variant, including
the NATS reference, with a full fresh process restart (BlazingMQ broker
or `nats-server`, whichever applies) immediately before each individual
run, all on the same machine in the same sitting, same 100,000-row
`nyse_eqy_us_all_trade_20260102` fixture, msgpack, content-verified PASS
on every run.

| variant | publish rate | receive rate |
|---|---|---|
| pg_blazingmq, priority, immediate confirm | 60,851/s | 30,767/s |
| pg_blazingmq, broadcast, immediate confirm | 60,004/s | 72,744/s |
| pg_blazingmq, priority, batch_confirm=true | 61,735/s | 54,369/s |
| pg_blazingmq, broadcast + batch_confirm, with properties | 58,604-59,111/s | 71,981-75,485/s (2 samples) |
| pg_blazingmq, broadcast + batch_confirm, no properties | 91,144/s | 46,777/s |
| pgnats / NATS core (fresh `nats-server`) | 93,445-96,415/s | 110,913-110,946/s (2 samples) |

**Two things this controlled re-run corrects, not just confirms**:

1. **Broadcast + batch_confirm together does not measurably beat broadcast
   alone.** The earlier single-sample 81,623/s for this combination does
   not reproduce - two fresh-broker samples here land at 71,981/s and
   75,485/s, both essentially indistinguishable from broadcast-mode alone
   (72,744/s). `batch_confirm`'s real, reproducible win is specific to
   *priority* mode (30,653-30,767/s -> 53,428-54,369/s, ~1.75x) - it
   doesn't compound with broadcast mode, which already removes most of
   the per-consumer bookkeeping `batch_confirm` is compensating for in
   priority mode. Treat the earlier 81,623/s figure as sampling noise,
   not a real effect.

2. **The NATS/pgnats reference number was itself not warmup-controlled,
   and the effect is not small.** A freshly restarted `nats-server`
   reproducibly measures **110,913-110,946/s** receive - almost exactly
   *half* the previously reported 221,868/s. This means the true,
   controlled gap between pg_blazingmq's best config (broadcast mode,
   ~72-75k/s) and NATS core is roughly **1.5x**, not the ~2.7-3x implied
   by comparing against the old, uncontrolled NATS number. Whether this
   halving is a genuine broker/server warmup effect (symmetric with what
   was found on the BlazingMQ side) or an artifact of
   `nats_publish_from_sql.py --verify`'s receive-timing method (it polls
   `nats_tool`'s periodic stats log at ~1s granularity - both fresh runs
   here measured "received in 0.901-0.902s", suspiciously close to a
   single poll interval, which could itself bias the rate calculation)
   was not isolated here and is worth a follow-up if the exact multiplier
   matters for a real decision.

The properties-free finding **does** reproduce under this rigor:
no-properties receive rate (46,777/s) remains well below the matched
with-properties broadcast+batch_confirm samples (71,981-75,485/s),
consistent with the original counter-hypothesis result - property
presence correlates with faster delivery here, not slower, for reasons
not root-caused (see prior section).

**Bottom line**: pg_blazingmq's best confirmed, reproducible throughput is
**broadcast mode, ~72-75k/s receive** (batch_confirm adds nothing further
in broadcast mode; it matters only in priority mode).

## The NATS "receive rate" was never real - it's a measurement artifact, not a warmup effect

The 1.5x-gap conclusion above is itself superseded. The suspicion at the end
of the previous section (that `nats_publish_from_sql.py --verify`'s
receive-timing might be a polling artifact, not a warmup effect) was
isolated directly and confirmed: **it's entirely an artifact, and the "true"
NATS receive rate isn't a comparable number at all.**

Root cause: `nats_tool`'s `--stats_interval` is a whole-integer-seconds-only
timer (`worker.hpp`'s `m_stats_interval` is an `int`, driving
`timer.expires_after(std::chrono::seconds(...))`), hardcoded to `1` by
`nats_publish_from_sql.py`. It cannot emit its first "Stats: N events/sec"
line before a full second has elapsed, regardless of how fast messages
actually arrived - so `wait_for_received_count()`'s reported "receive_secs"
is really just "time until the next 1-second stats tick fires and gets
observed," not real delivery time.

To measure the real thing, a one-off diagnostic script drove
`nats_publish_from_sql.py --verify --keep-dump` as a subprocess and, from
the exact same t0 reference point the script itself uses ("Waiting for
delivery," i.e. right after publish finishes), polled the `--dump` file's
line count directly at ~10ms resolution instead of parsing the 1s-granular
stats log (the dump file flushes every 100 messages, not on a timer - see
`message_output.hpp`'s `dump_file_writer`). Result, reproduced across 3
fresh-`nats-server` runs: **the dump file already contained all 100,000
lines within ~17ms of that reference point, every time** (17.0ms, 17.6ms,
17.5ms). The consumer was subscribed before publishing began, so messages
were arriving essentially as fast as they were published (100,000 rows
published in ~1.05-1.07s, ~93-96k/s) - by the time the script started
"waiting," receipt was already ~100% complete. The 0.75-0.9s / 110-133k/s
figures the script reports are not measuring anything about NATS's true
delivery speed; they're measuring how long until the next second-boundary
stats tick happens to land.

**This also means the whole "receive rate" comparison against pg_blazingmq
was comparing two different things, not just using two different clocks.**
`bmq_consume()` synchronously *pulls* a backlog of already-published,
persisted messages after the fact - a real, meaningful "how fast can this
drain a queue" number. NATS core here is *push*-delivered to an
already-subscribed, concurrently-running consumer - there is no backlog to
drain, so "receive rate" isn't really a rate NATS core has in this
scenario; delivery lag relative to publish is near-zero. The fairer
NATS-side number for this workload shape is its **publish rate**
(~93-96k/s, comparable to pg_blazingmq's own ~58-91k/s publish numbers
depending on variant), not a "receive rate" that was never really
measuring delivery throughput at all.

**Bottom line, superseding every earlier "gap to NATS" framing in this
document**: pg_blazingmq's best confirmed, reproducible pull-consume rate
is **broadcast mode, ~72-75k/s**. There is no reliable, comparable NATS
core "receive rate" figure to set it against - the push/pull architectural
difference between the two systems means that specific comparison doesn't
have a single well-defined answer. If a future benchmark wants a genuinely
fair NATS-side number, measure end-to-end per-message publish-to-delivery
*latency* (not a rate derived from a coarse periodic counter) rather than
trying to force NATS's push model into a "receive rate" shape built for
pg_blazingmq's pull model.

## Release-Mode Rebuild (Debug-Build Tax Quantified)

`PROFILING.md`'s `perf` profile of `bmqbrkr.tsk` surfaced that the broker's
`bmq`/`mqb` CMake group had been building as `CMAKE_BUILD_TYPE=Debug` this
entire session - unset in `CMakePresets.json`'s `ubuntu-x64` preset, so it
silently fell back to CMake's own default, while BDE/NTF (built separately
via `bde-tools`) were already genuinely optimized (`-O2 -DNDEBUG`, verified
in `build/bde/build.ninja`). Reconfigured with
`-DCMAKE_BUILD_TYPE=RelWithDebInfo` (same `-O2`/`-DNDEBUG` as `Release`,
keeps debug symbols for future `perf` work) and rebuilt the same minimal
target set (`README.md`'s Building section). All 5 benchmark variants
re-run with the same fresh-broker-restart-per-variant discipline as the
prior section, 100,000 rows, `nyse_eqy_us_all_trade_20260102`:

| variant | Debug receive rate | Release receive rate |
|---|---|---|
| priority, immediate confirm | 30,767/s | 31,038/s |
| broadcast, immediate confirm | 72,744/s | 87,531/s |
| priority, `batch_confirm=true` | 54,369/s | 64,090/s |
| broadcast + `batch_confirm`, with properties | 71,981-75,485/s | 86,395/s |
| broadcast + `batch_confirm`, no properties | 46,777/s | 84,329/s |

Two real findings, not just "Release is faster":

- **The Debug-build tax was real but partial** (~0-20% depending on
  variant) - it does not remotely explain the earlier ~1.5x gap to NATS
  core's publish rate (a comparison this document no longer makes, see
  above, but worth noting the magnitude here for context).
- **The earlier "removing properties makes receive rate worse" finding
  (46,777/s vs 71,981-75,485/s in the Debug build) does not reproduce in
  Release** - with and without properties are statistically
  indistinguishable (84,329/s vs 86,395/s). That earlier result was itself
  a Debug-build artifact, not a real property-encoding effect.

**Recommendation**: use `-DCMAKE_BUILD_TYPE=RelWithDebInfo` (or `Release`)
when building BlazingMQ's `bmq` group for anything performance-sensitive -
`README.md`'s Building section has been updated accordingly.

## The Real Headline Finding: `allocatorType`, Not Build Type

Re-profiling the Release build with `perf record -g` (see `PROFILING.md`)
found `_Unwind_Find_FDE` *still* the single largest symbol - 7.26% self
time, actually **higher** than the Debug build's 2.80%, not lower. The
call graph traces it precisely: `BloombergLP::mqbs::InMemoryStorage::put`
/ `mqba::ClientSession::onPutEvent` → `bsl::vector::reserve` →
`BloombergLP::balst::StackTraceTestAllocator::allocate()` →
`bsls::StackAddressUtil::getStackAddresses()` → `__backtrace` →
`_Unwind_Backtrace` → `_Unwind_Find_FDE`. This has nothing to do with
`CMAKE_BUILD_TYPE` - it's `bmqbrkrcfg.json`'s `taskConfig.allocatorType`,
a broker-config field with three options (`mqbcfg.xsd`'s `AllocatorType`
enum: `NEWDELETE`, `COUNTING`, `STACKTRACETEST`). The scratch broker's
config - copied verbatim from BlazingMQ's own
`docker/single-node/config/bmqbrkrcfg.json`, used by **every single
benchmark run in this entire document** - sets `STACKTRACETEST`: an
allocator that captures a full stack trace on every allocation, clearly a
debugging aid, not a production/benchmarking default.

Tested all three options directly (broadcast + `batch_confirm`, with
properties, Release build, fresh broker per run, 100,000 rows):

| `allocatorType` | receive rate |
|---|---|
| `STACKTRACETEST` (the shipped default, used everywhere above) | 86,395/s |
| `NEWDELETE` | 40,107/s, 43,388/s (2 runs) |
| `COUNTING` | 45,958/s |

**Counterintuitive and reproduced twice**: `STACKTRACETEST` is not just
"not the bottleneck it looked like" - it's roughly **2x faster** than
either alternative, despite doing far more work per allocation (capturing
and resolving a full stack trace). The stack-unwind cost visible in the
`perf` profile is real, but whatever pooling/arena strategy
`StackTraceTestAllocator` uses underneath evidently outweighs it by a wide
margin for BlazingMQ's actual allocation pattern here - plain `NEWDELETE`
(routing every allocation through glibc `malloc`/`free`) and `COUNTING`
(lighter bookkeeping, no stack capture) are both markedly *worse*, not
better. Not fully root-caused why (would need to profile the `NEWDELETE`
run directly to see what's actually slow there instead of assuming); worth
revisiting if this ever becomes a decision that matters for a real
deployment, but for benchmarking purposes: **leave `allocatorType` at its
shipped `STACKTRACETEST` default** - every number in this document already
does, and that turns out to be the right call, not a confound to fix.

## NATS JetStream Comparison (Resolved - The Earlier "Bug" Was A False Alarm)

The core-NATS comparison above was invalidated (see "The NATS receive
rate was never real") because the consumer was already caught up
concurrently with publish - no real backlog to drain, unlike
`bmq_consume()`'s genuine "pull an already-persisted backlog" shape.
JetStream is the correct fix for that mismatch, since it persists
messages regardless of consumer timing.

**A prior attempt at this reported the JetStream stream "never actually
exists" after publish, diagnosed as a possible bug in pgnats's
`nats_publish_stream_flush()`.** That diagnosis was wrong. Verified
directly (minimal repro: create a stream, publish 20 rows via
`nats_publish_binary_stream_async`/`nats_publish_stream_flush`, check
`jsz` *before* anything else touches the stream - all 20 messages
genuinely persisted) - **`nats_publish_binary_stream_async`/
`nats_publish_stream_flush` work correctly, no bug in pgnats.** The
earlier finding was checking stream existence only *after* the whole
`nats_publish_from_sql.py` process had exited - which is exactly when
that script's own documented cleanup (`delete_js_stream()`, called
unconditionally in a `finally` block specifically so a leftover stream
never pollutes a later run - see `js_stream_name()`'s own docstring) had
already removed it. The stream and its messages existed the entire time
the script was running; they just don't exist afterward, on purpose.

**Correct methodology**: `nats_publish_from_sql.py` isn't the right tool
for a publish-then-drain-*separately* measurement, since its cleanup
races ahead of any external attempt to drain the stream after the fact.
`bench/js_bench.py` (new) sidesteps this by not using that script at all:
it creates a memory-backed stream (matching BlazingMQ's own in-memory
scratch domain, for a fair non-durable-vs-non-durable comparison),
publishes via pgnats's own SQL functions directly, confirms real
persistence via `$JS.API.STREAM.INFO` before draining anything, *then*
drains via a pull consumer (`nats_tool --mode js_grub`), timing the drain
by polling the consumer's `--dump` file line count at 10ms resolution
(same technique that correctly measured pg_blazingmq's drain rate and
exposed the core-NATS stats-timer artifact), and only deletes the stream
at the very end.

**Results** (100,000 rows, `nyse_eqy_us_all_trade_20260102`, msgpack,
fresh `nats-server` restart, content-verified PASS, reproduced twice):

| variant | publish rate | drain rate |
|---|---|---|
| pg_blazingmq (Release, broadcast + batch_confirm) | 146,877/s | 90,522/s |
| NATS JetStream (memory storage, pull-consumer drain) | 150,502-152,534/s | 78,353-78,958/s |

**pg_blazingmq comes out ahead on drain rate** (~90.5k/s vs ~78.4-79.0k/s,
roughly 15-18% faster) once compared against a comparably-durable system
instead of bare NATS core - confirming the prediction made when this
comparison was proposed: NATS core's earlier apparent advantage was
entirely a function of it doing structurally less work (no persistence,
no ack protocol), not a language or implementation-quality gap. JetStream
publish rate is slightly ahead of pg_blazingmq's (~151k/s vs ~147k/s), so
the two systems are much closer to parity on the write side than the
read/drain side once both are actually paying for durability.

One benign methodology gotcha hit while building `js_bench.py`, worth
noting for anyone extending it: `nats_tool`'s JSON rendering of a decoded
msgpack `float64` keeps a trailing `.0` for whole-numbered values (e.g.
`11.0`), while Postgres's `to_jsonb` renders the same value as a bare
integer (`11`) - both are the same number, just different JSON text.
Content-verification must canonicalize floats before comparing (`v.0 ->
int(v)` when `v.is_integer()`) or every whole-numbered `double precision`
column produces a spurious mismatch - this cost ~1% of rows a false
"FAIL" before being normalized away; it was never a real data-loss issue,
confirmed independently via an in-process `row_to_msgpack`/
`msgpack_to_jsonb` vs `to_jsonb` comparison (zero mismatches) before the
NATS round-trip was even involved.

## Raw Client, Out-of-Postgres, Parallel Publish

Every number above went through Postgres SQL on both sides (`bmq_publish_row`/
`nats_publish_binary`-style functions). This section removes Postgres
entirely and tests each system's own native client tool directly -
`bmqtool` (BlazingMQ's official CLI, `--mode auto`) and `nats_tool`'s
purpose-built `bench` mode - both to isolate any Postgres-layer overhead
and to test genuine multi-connection parallelism, which nothing above did.
Synthetic 355-byte payloads (matching the real fixture's average msgpack
row size), fresh broker/server restart before this session, 1,000,000
messages per run unless noted. Timed by wall-clock bracketing the whole
run (`date +%s.%N` around the process, or the tool's own single
start/end `steady_clock` measurement for `nats_tool bench` - not a
periodic stats-log tick, avoiding the exact artifact class found and
fixed twice earlier in this document).

**BlazingMQ** (`bmqtool --mode auto`, broadcast queue, `WRITE`-only, no
ack requested - the least-overhead config, matching NATS core's own
unacked default):

| connections (separate processes) | publish rate |
|---|---|
| 1 | 84,584/s |
| 4 | 94,094/s |
| 16 | 93,627/s |

Scales modestly from 1→4 connections (+11%), then flatlines - the
bottleneck past 4 concurrent producers is server-side (likely
single-threaded domain/queue processing), not client-side. **Genuinely
surprising result, not assumed**: this raw-client number is *lower* than
the Postgres-driven `bmq_publish_row` publish rate (~147k/s, broadcast +
`batch_confirm`, Release build, earlier in this document) - Postgres was
never the bottleneck on the publish side; if anything `bmqtool`'s own
posting loop is less efficient here than pg_blazingmq's
`MessageEventBuilder`-based batching. Not fully root-caused - `bmqtool`'s
`--postrate`/`--postinterval`/`--eventsize` scheduling model may simply
not be tuned for unthrottled max-speed posting the way a tight pipelined
loop is.

**NATS core** (`nats_tool bench`, no JetStream, no ack - core's normal
fire-and-forget mode):

| connections | publish rate |
|---|---|
| 1 | 5,263,158/s |
| 4 | 8,333,333/s |
| 16 | 7,633,588/s |

Already far beyond anything either system does with durability enabled at
N=1 - consistent with everything else this document has found about NATS
core doing structurally less per-message work than either BlazingMQ or
JetStream. This is not a fair comparison point for pg_blazingmq (see "The
NATS core comparison was never real" section above) - included here only
for completeness/scale, not as a rate to close a gap against.

**NATS JetStream** (`nats_tool bench --js --create_stream`, acked, the
fair comparison point):

| connections | count | publish rate |
|---|---|---|
| 1 | 100,000 | 153,610/s |
| 4 | 100,000 | 138,504/s |
| 16 | 100,000 | 126,743/s |

Matches the Postgres-driven JetStream publish rate (~151k/s) almost
exactly at N=1 - Postgres wasn't a bottleneck on this side either.
Throughput *decreases* as connections increase (opposite of BlazingMQ's
own modest 1->4 scaling above) - all connections publish to the same
single stream, so this looks like server-side write-serialization
contention on that stream growing with concurrent producers, not a
client-side limit.

**The `nats_tool bench --js` counting bug flagged in an earlier version of
this section has been root-caused and fixed, in `nats_asio` itself**
(`samples/modes/benchmarker.hpp`, uncommitted local fix pending a
maintainer decision - see below): `worker::m_counter` is `exchange(0,
...)`'d every `stats_interval` seconds by the periodic `Stats:` timer -
correct for that line's own per-interval delta, but `benchmarker::run()`'s
final summary read `total_msgs` from that *same* being-reset counter
instead of keeping its own running total. Any run spanning more than one
stats tick (i.e. anything slower than ~1 stats_interval - core-NATS mode
looked "correct" purely because it always finishes before the first
reset) had almost its entire count zeroed out before the final read,
leaving only whatever accumulated in the last partial interval - not a
publish failure or early exit, a pure reporting bug. Fixed by adding a
separate `std::atomic<std::size_t> m_total_counter` that every `run_*()`
loop increments alongside `m_counter`, read at the end instead. Verified:
`--count 1000000` (fresh `nats-server` restart) now reports "Messages:
1000000 in 8.87s, Throughput: 112689 msgs/sec" - matching the sustained
Stats-line rate, not a truncated fraction of it. The N=1/4/16 numbers
above were obtained with this fix in place. `nats_asio` doesn't have the
same standing push authorization established elsewhere this session for
`pg_blazingmq`/`nats_sidecar` - the fix is verified locally
(`samples/modes/benchmarker.hpp`) but intentionally left uncommitted/
unpushed there pending a maintainer decision.

**Bottom line**: going around Postgres didn't change the ceiling on
either system's durable/comparable path (JetStream) - Postgres was never
the bottleneck there. On BlazingMQ's side, the raw client was actually
*slower* than the Postgres-driven number, a genuinely counterintuitive
result pointing at `bmqtool`'s own load-generation code rather than
anything about BlazingMQ, Postgres, or pg_blazingmq.

## Multiple Postgres Backends, and the Real Broker-Side Ceiling

Every `bmq_bench.py` run above used one psycopg connection - one Postgres
backend, one `bmqa::Session` (`pg_blazingmq.cpp`'s `get_session()` is a
per-backend singleton; Postgres's own process-per-connection model means
this already gives one independent session per concurrent client, with no
code change needed). Two things had never actually been tested: whether
concurrently-publishing backends help throughput, and whether
`bmqa::Session`'s client-side I/O thread count (`SessionOptions::
setNumProcessingThreads()`, never called by `get_session()` - every
session runs the BlazingMQ default) matters.

**`numProcessingThreads` doesn't matter.** Tested 1 (default), 4, and 24
on a single session/backend, same broadcast + `batch_confirm` config as
the established ~86-90k/s baseline: 94,121/s, 85,884/s, 87,077/s -
statistically the same. Rules out client-side event-dispatch threading as
a factor; not landed as a GUC since it's a no-op knob here.

**Multiple concurrent backends scale, but not monotonically with the
broker's default config.** N separate psycopg connections (multiprocessing,
each its own backend/session), same 100,000-row split across workers,
fresh broker restart per run, `bmq.test.mem.broadcast`:

| connections | publish rate |
|---|---|
| 1 | 166,559/s |
| 4 | 336,147/s |
| 16 | 70,109/s (worse than N=1) |

Ruled out a test artifact first: re-ran N=16 at 4x the row count (25,000
rows/worker, matching N=4's per-worker share) to rule out fixed
per-worker session-startup cost dominating at a small per-worker row
count - result was unchanged (68,114/s), confirming a genuine
degradation, not a fixed-cost artifact of the test itself.

**Root cause, found via `perf record -g` on `bmqbrkr.tsk` during an N=4
run** (53,056 samples): the broker's own dispatcher runs on named,
fixed-size thread pools - `bmqDispSession` (46.84% of all samples) and
`bmqDispQueue` (29.56%) - not one thread per client connection. Their
size is a real, documented, configurable broker setting:
`appConfig.dispatcherConfig.sessions.numProcessors` (default **4** in
BlazingMQ's own `docker/single-node/config/bmqbrkrcfg.json`, which
`test/manage_broker.sh`'s scratch broker uses unmodified) and
`.queues.numProcessors` (default 8). N=4 landed right at the sessions
pool's default size - the sweet spot, not a coincidence. N=16 meant 16
concurrent sessions contending for 4 dispatcher threads.

**Confirmed by fixing it**: raised `dispatcherConfig.sessions.
numProcessors` from 4 to 16 (matching the client connection count) in the
broker config, fresh restart, re-ran all three connection counts:

| connections | numProcessors=4 (default) | numProcessors=16 |
|---|---|---|
| 1 | 166,559/s | 165,438/s (unchanged, as expected) |
| 4 | 336,147/s | 302,558/s (unchanged within noise) |
| 16 | 70,109/s | **157,900/s** (>2x, degradation gone) |

**This directly answers the "sub-100k ceiling on modern hardware"
question from earlier in this investigation: the ceiling was never
fundamental to BlazingMQ.** pg_blazingmq already supports multi-session
parallelism for free (concurrent Postgres backends); getting real
throughput out of it in production just requires setting
`dispatcherConfig.sessions.numProcessors` (and likely `.queues.
numProcessors`) to match expected concurrent client/producer count,
the same way you'd size any thread pool to expected concurrency. This is
a **broker deployment config**, not a `pg_blazingmq` code change - no
GUC was added; the fix is "set this in your `bmqbrkrcfg.json`," which
belongs in deployment docs, not the extension itself. The change was
**not** kept in BlazingMQ's own shared `docker/single-node/config/`
template (reverted cleanly, `git status` clean in `~/blazingmq`) since
that's shared infrastructure beyond this session's authorization for
unilateral changes - only documented here.

## Past 500k/s: Mapping The Full Curve, And Proving It's Not Hardware-Bound

Pushed further to see whether the N=4 (336,147/s) / N=16-tuned (157,900/s)
result above was near a real ceiling or just two points on a curve nobody
had mapped. It was the latter. Same setup (broadcast, `batch_confirm`,
Release build, `dispatcherConfig.sessions.numProcessors` and
`.queues.numProcessors` and `networkInterfaces.tcpInterface.ioThreads` all
set equal to the connection count N, fresh broker restart per point,
100,000 rows split evenly across N psycopg connections/backends, wall-clock
span measured from a `multiprocessing.Barrier`-synchronized start so all
workers begin concurrently):

| N (connections = pools) | publish rate |
|---|---|
| 4 | 304,286/s |
| 6 | 447,822/s |
| 8 | 490,596 - 515,677/s (reproduced 3x) |
| **9** | **549,407/s** |
| 10 | 468,528/s |
| 12 | 362,107 - 369,456/s |
| 16 | 196,541 - 219,975/s |
| 20 | 127,062/s |
| 24 | 77,723/s |

**Target met: 549,407/s, comfortably past 500k/s, at N=9 with all three
pools matched to 9.** N=8 reproduces reliably in the 490-515k range across
three separate runs. The curve peaks sharply around N=8-9 and degrades on
both sides - not a step function at the dispatcher pool size the way the
earlier N=4-vs-N=16 comparison suggested, but a real peak with a falloff
past it, for reasons not further isolated here (plausibly per-connection
BlazingMQ session/queue-open overhead outweighing added parallelism past
~9 concurrent sessions, but this is not confirmed - see below).

**Confirmed via direct CPU measurement that this is not a hardware
ceiling.** `mpstat -P ALL 1` during a longer run (1,000,000 rows, N=9,
~2.2s span at 100k scale, ~2s+ at 1M) never exceeded ~65% combined
usr+sys across all 24 hardware threads at its single busiest 1-second
sample, averaging noticeably lower - real, measurable idle headroom
remained on every core throughout. Storage is tmpfs-backed (RAM), so I/O
wait was negligible throughout (`%iowait` at or near 0.00 in every
sample). This machine (AMD Ryzen 9 7900X, 24 threads, 61GB RAM) was not
pushed anywhere near its ceiling even at the best-performing
configuration - the ~500-550k/s peak is a **software** ceiling (something
in the connection-count-vs-throughput curve past N~9, not yet
root-caused further), not a CPU or I/O limit. A longer, larger run (1M
rows, N=9) actually measured *lower* throughput (449,448/s) than the
100k-row run at the same N - worth noting as a real, unexplained
discrepancy for anyone pushing this further, not smoothed over.

**Operational finding worth flagging**: `bmqbrkr.tsk`'s stop/start cycle
has a real, intermittent race on its control pipe (`balb_pipecontrolchannel.cpp`:
`Named pipe './/bmqbrkr.ctl' is already in use by another process`,
`PANIC [STARTUP]`) - `test/manage_broker.sh stop` waits for the old PID
to disappear, but that doesn't guarantee the pipe resource is released
before the very next `start()` tries to recreate it. A 1.5-3s sleep
between `stop` and `start` was NOT always sufficient in this sweep; some
data points needed a retry loop (stop, sleep, try start, retry on
failure) to get a clean broker. Anyone scripting repeated broker
restarts for benchmarking should build in a retry, not just a fixed
sleep - `manage_broker.sh` itself wasn't patched (out of scope here, and
its own single-run start/stop already has its own 10s "started
successfully" poll-wait, which is correct for a single restart; this
only bites a tight loop of many restarts in quick succession).

Config changes (`sessions`/`queues`/`ioThreads` = N per data point) were
made and reverted in the same scratch-broker config path as every prior
run this session; `git status --short` in `~/blazingmq` confirmed clean
before finishing.

## The Real Explanation For "Sustained Rate Looks Lower": An Undrained Backlog, Not Duration

Chased two open threads: what causes the falloff past N~9, and why a
longer (1M-row) N=9 run measured *lower* throughput (449,448/s) than the
100k-row run that found the 549,407/s peak.

**A previously-untested third dispatcher pool, ruled out.**
`appConfig.dispatcherConfig` actually has *three* `numProcessors` pools,
not two - `sessions` (default 4) and `queues` (default 8), both already
tuned in the prior section, plus `clusters` (default 4), never touched.
Hypothesis: leaving `clusters` at 4 while `sessions`/`queues` scaled past
4 could make it the new bottleneck at higher N. Tested directly - raised
all three (`sessions`/`queues`/`clusters`) to 16 together: N=16 still
landed at 214,497/s, matching the already-reported 196,541-219,975/s
range almost exactly. `clusters.numProcessors` isn't a factor here (a
single-node scratch broker likely never exercises this pool much
regardless of client connection count - it's for inter-node cluster
traffic, not client sessions/queues).

**The real finding, and it's bigger than the N-vs-N falloff question:
throughput is not duration/connection-count-dependent, it's dependent on
how large the queue's *undrained* backlog has grown.** `parallel_bench.py`
is publish-only - nothing ever calls `bmq_consume()` to drain what gets
published, so every row published during one run adds to a
monotonically-growing, never-emptied queue for that run's duration. Ran
the same N=16 config (this time with all three pools already matched)
at increasing row counts on a fresh broker each time:

| total rows | rate |
|---|---|
| 100,000 | 214,497/s |
| 300,000 (with `perf record` attached) | 28,555/s |
| 300,000 (no profiler, re-run to rule out profiling overhead) | 5,598/s |

Not profiler overhead - the *unprofiled* re-run was slower than the
profiled one, both catastrophically below the 100k-row rate for only 3x
the volume. This is superlinear degradation, not a fixed per-call cost:
tripling total messages published in one run cut throughput by roughly
40-90x, depending on run.

**Root cause, confirmed via `perf record -g` on `bmqbrkr.tsk` during the
degraded run, not inferred:** `BloombergLP::mqbblp::RootQueueEngine::
afterNewMessage()` -> `QueueEngineUtil_AppsDeliveryContext::
deliverMessage()` -> `QueueHandle::deliverMessageImpl()` dominates the
profile (~25%+ of all samples nested under this one call path). This
delivery-attempt logic runs on *every* single publish, and its cost
scales with the size of the queue's retained/undelivered message state -
which, in a publish-only test with no consumer ever confirming/draining
anything, grows without bound for the whole run. Early in a run the
backlog is small and cheap to process; by the back half of a 300k-row
run it's large, and per-message delivery-attempt cost has grown with it -
producing exactly the superlinear, average-throughput-craters-as-volume-
grows pattern measured above.

**This is the real explanation for the earlier N=9 1M-row discrepancy
too** (449,448/s vs the 549,407/s 100k-row peak, same N) - not a
duration-based "sustained state" effect, an undrained-backlog-size
effect. A 1M-row publish-only run accumulates a much larger undelivered
backlog than a 100k-row one by its back half, so its *average* rate is
pulled down by the same mechanism, independent of how many concurrent
connections are involved.

**What this means for a genuinely trustworthy sustained-rate number: a
publish-only benchmark cannot produce one.** Any number from
`parallel_bench.py`/`bmq_bench.py` as currently written reflects
publishing into a growing, undrained queue - not steady-state throughput
under realistic concurrent publish+consume load, where a consumer
draining messages would keep the backlog (and therefore the per-message
delivery-attempt cost) small and roughly constant. The 549,407/s peak and
every other number in this document should be read as **burst/small-
backlog publish rates**, not sustained rates - producing a true sustained
number requires a benchmark that runs `bmq_consume()` (or `bmq_subscribe()`)
concurrently with publish, keeping the backlog bounded, which no
benchmark in this repo does yet. This is flagged here rather than solved
- building that concurrent publish+drain benchmark is real, additional
scope beyond this investigation.

**Practical implication for anyone deploying pg_blazingmq**: this is a
strong argument for consuming promptly (a live `bmq_subscribe()` worker
or a tight `bmq_consume()` poll loop) rather than letting messages
accumulate unread - not just for the obvious reasons (memory/storage
retention limits), but because the broker's own per-message delivery-
attempt cost appears to grow with backlog size, making a large undrained
backlog progressively more expensive to publish *into*, not just to
eventually drain.

Broker config reverted, `git status --short` clean in `~/blazingmq`,
broker stopped, no stray processes.

## A Real Sustained-Rate Benchmark (`sustained_bench.py`)

Built the concurrent publish+drain benchmark the section above said didn't
exist yet: `bench/sustained_bench.py` runs N producer processes and M
consumer processes against the same queue simultaneously, for a fixed
wall-clock window, and samples `(published - consumed)` every couple of
seconds throughout the run - printed as a curve, not asserted - so a
reader can see for themselves whether the backlog stayed bounded or grew.

**Priority mode, not broadcast, and it matters why.** Broadcast fans every
message out to *every* attached consumer independently - per this
session's own earlier findings, each consumer gets its own copy and the
broker must retain a message until every attached consumer has confirmed
it. Adding more broadcast consumers doesn't parallelize draining, it
multiplies delivery work without shrinking the backlog any faster.
Priority mode's work-queue routing sends each message to exactly one
attached consumer (round-robin among equal-priority consumers), so it's
the only mode where adding consumers can actually help drain throughput.

**Two real bugs in the first cut of this benchmark, found and fixed
before trusting any number from it - not smoothed over:**

1. Concurrent producers all doing their *first-ever* open of a brand-new
   queue at the exact instant a `multiprocessing.Barrier` releases them
   collide into `rc=100` (`ALREADY_OPENED`) - not the same-session
   double-open case that error usually means (`get_queue()`'s cache
   already handles repeated calls from one session fine), but a genuine
   startup race between *different* sessions' first opens. Primed each
   producer's queue handle with one row published before the barrier -
   this reduced but didn't eliminate the race (per-connection setup time,
   `SET`/`CREATE TEMP TABLE`/psycopg connect, varies enough that "before
   the barrier" isn't reliably "not colliding with another session's
   first open" either) - closed the remaining gap with a short randomized
   retry-on-`rc=100` wrapper around every open-sensitive call.
2. A consumer loop that broke out early the first time a single
   `bmq_consume()` call returned zero rows after the producer window
   ended - looked reasonable, but priority mode's round-robin delivery
   means one consumer seeing `n==0` on a given call says nothing about
   whether the *queue* is actually empty (another attached consumer could
   have gotten that round's messages instead). This silently abandoned a
   consumer's share of the backlog for the rest of the run the moment
   production paused even briefly - confirmed by seeing `consumed` freeze
   completely mid-`drain_tail` despite a large nonzero backlog still
   sitting in the queue. Fixed by looping unconditionally until the drain
   deadline instead of trying to detect "empty" from one call.

**The core, load-bearing finding: an uncapped producer can never reach a
sustained equilibrium with any finite consumer capacity, because
publishing itself gets more expensive as the backlog grows** - not just
delivery. This follows directly from the section above:
`RootQueueEngine::afterNewMessage()`/`deliverMessage()` runs on *every*
publish and its cost scales with retained backlog size. So a producer
publishing flat-out and a consumer trying to keep pace aren't just racing
at fixed speeds - the producer's own per-message cost rises as the gap
between them widens, which only widens the gap further. Confirmed
directly: 1 producer (unthrottled) vs 1 consumer in priority mode grows
an ever-larger backlog no matter how the consumer side is tuned (tried
1-4 consumers, larger/smaller `bmq_consume()` batch sizes, shorter/longer
timeouts - backlog always grew without bound while the producer ran flat
out).

**The right experiment is therefore "what's the highest *fixed* publish
rate at which consumption genuinely keeps the backlog flat", not "how
fast can producers outrun consumers before falling behind".** Added
`--producer-rate-limit` (paces each producer to a target total rows/sec
via a per-batch sleep) and bisected it for a single producer + single
consumer, priority mode, `batch_confirm=true` both sides, fresh broker
per run, 12s window (3s ramp-up excluded, 8-10s drain tail to confirm
full drain at the end):

| target rate | backlog over the steady-state window | bounded? |
|---|---|---|
| 5,000/s | flat at ~3,400 | yes, comfortably |
| 30,000/s | flat at ~15,000 | yes, comfortably |
| **42,000/s** | **21,929 -> 19,088 (shrinking)** | **yes** |
| 48,000/s | 25,113 -> 177,613 (growing) | no |
| 60,000/s | 76,900 -> 649,900 (growing fast) | no |

**Sustained throughput for pg_blazingmq (1 producer : 1 consumer,
priority mode, Release build): ~42,000-45,000/s** - this is the number to
actually cite for capacity planning, not the 549,407/s burst peak two
sections up. Every run at or below 42k/s fully drained to zero backlog
during the drain tail; every run at or above 48k/s did not (48k/s did
eventually drain given enough tail time in this specific test, but its
steady-state window was clearly growing, not flat - not a rate that
holds indefinitely).

**The 2-consumer regression, now root-caused: it's not multi-consumer
drain capacity, it's live-publish interaction.** A follow-up isolated the
two things that were tangled together in the result above: consumer
*drain capacity in isolation* (a fixed pre-published backlog, no producer
running at all) versus consumer performance *while a producer is
concurrently publishing*, using a standalone diagnostic
(`bmqbrkr.tsk` fresh each time, 300-400k row backlogs, `perf record -g`
attached to the broker during the drain):

| scenario | consumers | rate |
|---|---|---|
| isolated drain (no live producer), pre-published backlog | 1 | 223,339/s |
| isolated drain (no live producer), pre-published backlog | 2 | 58,221-77,104/s |
| isolated drain (no live producer), pre-published backlog | 4 | 49,327/s |
| **live** publish+drain, target 42,000/s (1 producer's own sustainable rate) | 2 | **3,999/s** |
| **live** publish+drain, target 50,000/s | 2 | 10,773/s (jumps to full drain speed the instant the producer stops) |

Isolated 2-consumer drain is genuinely fine - slower than 1 consumer
per-consumer (consistent with round-robin coordination overhead scaling
with consumer count, confirmed via `perf`: no single dominant lock/
contention symbol, just broadly higher confirm/delivery/stats-tracking
cost spread across many functions, all under 5% individually), but still
a healthy 58-77k/s in absolute terms. The catastrophic collapse only
happens with a **live, concurrently-publishing producer in the picture**:
at the exact rate (42,000/s) that 1 consumer sustains cleanly with a
shrinking backlog, adding a second consumer instead grows the backlog to
440k+ and drains at a crawl even during the drain-tail (after the
producer has stopped) - nothing like the isolated test's 58-77k/s. The
50,000/s run shows the mechanism directly: consume rate is throttled to
~10,773/s the entire time the producer is live, then jumps to full speed
the moment publishing stops. Combined with the already-established
finding that publish cost itself scales with backlog size
(`RootQueueEngine::afterNewMessage()`/`deliverMessage()`), this points to
a shared-dispatcher-thread-pool contention loop: a live producer's own
per-message dispatch work competes with confirm-handling dispatch work on
the *same* thread pools, round-robin's per-message consumer-selection
adds cost specifically during live interleaved delivery (not needed for
a bulk pre-published backlog), and any backlog growth from that
contention makes the producer's own next publish more expensive too -
self-reinforcing, matching the pattern already documented above.

**Practical implication, and the actual answer to "can consumers help":
no, not by adding more of them.** Isolated drain capacity being healthy
doesn't matter if it can't be reached under live production load, and
this test found the opposite of what more consumers is supposed to buy
you - the live 2-consumer number was consistently *worse* than the clean
1-consumer number at the same target rate, not better. **1 consumer,
priority mode, ~42,000-45,000/s remains the best-known, most trustworthy
sustained rate for this configuration** - not because multi-consumer
drain is inherently slow (it isn't, in isolation), but because the
interaction between live publish and multi-consumer round-robin delivery
is actively harmful, not helpful, and isn't something client-side
consumer count or `bmq_consume()` batch/timeout tuning can fix on its
own. Real remaining scope for whoever picks this up: profile the *live*
2-consumer case directly (not the isolated drain) to find the specific
contention point, and check whether `channelHighWatermark`/dispatcher
pool sizing tuned specifically for the producer+consumer mix (not just
total session count) changes this - neither was reached here.

New reusable script: `bench/sustained_bench.py`. Domain/broker config
changes made and reverted the same way as every other run in this
document; `git status --short` in `~/blazingmq` confirmed clean, broker
stopped, no leftover `pg_blazingmq subscriber` connections in
`pg_stat_activity` after finishing.

## Consumer Count 4/8/16/32: Dispatcher-Pool Sizing Was Part Of It, But Not All Of It

The 2-consumer regression above was tested with the broker's *default*
`dispatcherConfig` (`sessions`/`queues`/`clusters.numProcessors` all at
their stock 4/8/4) - 1 producer + 2 consumers is only 3 concurrent
sessions, comfortably under a pool of 4, so pool sizing wasn't actually
the missing piece for that specific case. But it was never verified at
higher consumer counts, where total session count (producer + consumers)
does exceed the default pools. Retested at 4/8/16/32 consumers with all
three pools *and* `tcpInterface.ioThreads` explicitly set to match total
session count each time (5/9/17/33), fresh broker restart before every
rate probe (a methodology slip mid-run - reusing one broker across three
rate probes in a row produced an impossible "consumed > published" result
from a prior run's undrained leftover backlog contaminating the next
probe's numbers - caught before trusting it, redone with a fresh restart
per probe from then on):

| consumers | ceiling (backlog stays bounded) | first rate that grows unbounded |
|---|---|---|
| 1 (baseline, from above) | ~42,000-45,000/s | 48,000/s |
| 4 | ~32,000/s (38,000/s starts clean, destabilizes ~13s in) | 38,000/s |
| 8 | ~40,000-42,000/s | 47,000/s |
| 16 | ~40,000-45,000/s | 47,000/s |
| 32 | ~40,000/s (noisier: backlog oscillates 37k-105k vs 8's tighter 9k-24k band, but no growth trend) | 45,000/s |

**The honest answer: dispatcher-pool sizing explains why 8/16/32
consumers no longer collapse catastrophically the way the under-pooled
2-consumer test did (all three now land in the same 40-45k/s band as 1
consumer, not the 2-consumer test's 3,999/s), but it does not explain why
more consumers never beats 1 consumer.** N=4 is actually the worst of
the properly-pooled configurations (~32k/s, below the 1-consumer
ceiling), and N=8/16/32 all converge on essentially the *same* ~40-42k/s
ceiling as N=1 - not a step up, just a plateau. Every configuration
eventually hits the identical wall: a rate around 47-48k/s where the
backlog stops staying flat and starts climbing, regardless of how many
consumers are pulling from it. This is consistent with (though not
independently reconfirmed by new profiling here - a `perf` capture taken
during a *working* 45,000/s/16-consumer run showed only the
already-documented `StackTraceTestAllocator`/protocol-parse overhead, no
new contention symbol) the standing explanation from the section above:
publish-side cost itself scales with backlog size
(`RootQueueEngine::afterNewMessage()`/`deliverMessage()`), so the ceiling
is set by how fast the *producer* can keep publishing before its own
cost curve turns upward - not by how much drain capacity is available on
the consumer side. Adding consumers can't fix a producer-side cost
mechanism.

**Revised practical answer**: 1 consumer, priority mode,
`batch_confirm=true`, ~42,000-45,000/s remains both the simplest and the
best configuration found. More consumers (properly pooled) don't hurt the
way an under-pooled setup does, but they also don't help - use exactly
one consumer per queue for this workload shape unless there's a reason
besides raw throughput (e.g. consumer-process fault tolerance) to run
more.

Config changes reverted the same way as every prior section
(`git status --short` clean in `~/blazingmq`), broker stopped, no
leftover `pg_blazingmq subscriber` connections.

## Multi-Queue Scaling: Testing BlazingMQ's Own Published Scaling Strategy — It Doesn't Help Here

BlazingMQ's official benchmarks (https://bloomberg.github.io/blazingmq/docs/performance/benchmarks/)
scale priority-mode throughput via **queue count**, not consumers per
queue: 1Q/1P/1C → 60,000/s, 10Q/10P/10C → 120,000/s aggregate, 50Q/50P/50C
→ 100,000/s, 100Q/100P/100C → 100,000/s (their hardware: 6-node cluster,
strong consistency, 1KB messages — very different from this scratch
single-node setup, but the *scaling strategy* — many independent queues,
each with its own dedicated 1P/1C pair — is what's being tested here,
not an exact number match). This is a genuinely different axis from
everything tested in the sections above, which all scaled *consumers on
one shared queue* and found it doesn't help. Multiple independent queues
had never been tested.

New script: `bench/multi_queue_bench.py`, same conventions and
backlog-bounded-is-the-evidence discipline as `sustained_bench.py`, just
replicated per queue — each of K queues gets its own dedicated producer
and consumer process, its own publish/consume counters, and its own
backlog trend, so one queue's healthy result can't mask another's
quietly growing one.

**Result: it doesn't scale here — aggregate capacity across K queues is
*lower* than one queue alone, not higher.** Bisected per-queue rate
against a bounded-backlog outcome, fresh broker restart per data point,
dispatcher pools (`sessions`/`queues`/`clusters.numProcessors`,
`tcpInterface.ioThreads`) matched to total session count (2×K) each time:

| K queues | per-queue rate tested | aggregate | bounded? |
|---|---|---|---|
| 1 (reference, prior section) | — | ~42,000-45,000/s | yes |
| 2 | 5,000/s | 10,000/s | yes |
| 2 | 8,000/s | 16,000/s | **yes (K=2's real ceiling, ~here)** |
| 2 | 12,500/s | 25,000/s | no — consume collapses to ~4,000/s aggregate |
| 2 | 20,000/s | 40,000/s | no — consume collapses to ~4,000/s aggregate |
| 4 | 4,000/s | 16,000/s | yes — same aggregate ceiling as K=2 |

K=2 and K=4 land on the **same aggregate ceiling, ~16,000-20,000/s**,
each well below what one queue alone sustains (~42,000-45,000/s). Not a
step-down-then-flat curve like the earlier consumer-scaling sweep — a
consistent aggregate cap regardless of how the work is split across
queues, and *lower* than a single queue gets on its own. Splitting
publish+consume load across more queues doesn't parallelize it here; it
divides a smaller shared budget among them. Once per-queue rate is
pushed past that shared ceiling, consume throughput collapses to a
roughly fixed low value (~2,000/s per queue regardless of target rate)
rather than degrading gracefully — the same collapse-under-live-contention
shape found in the earlier 2-consumer-on-one-queue investigation, now
appearing across queues instead of within one.

**This doesn't reproduce Bloomberg's published scaling shape**, and the
gap is worth being explicit about rather than assumed away: their
benchmark runs on a real multi-node cluster with strong-consistency
replication, dedicated hardware, and (implicitly) broker-side resource
allocation this single-node scratch broker doesn't have. The most
plausible explanation, consistent with every other finding in this
document, is that this scratch broker's shared dispatcher/confirm-path
resources (already implicated in both the single-queue backlog-scaling
ceiling and the live multi-consumer collapse) are a broker-wide bottleneck
that queue count alone doesn't route around — not a `pg_blazingmq` client
limitation, and not proof BlazingMQ itself can't scale via queue count
under different (real, multi-node, properly-provisioned) deployment
conditions. Not further root-caused here (a `perf` diff between the K=2
bounded and K=2 collapsed configurations would be the natural next step,
not done — this section prioritized mapping the shape over explaining
the mechanism, given remaining budget).

**Revised practical answer, updated**: for this single-node scratch
broker, ~42,000-45,000/s (one queue, one producer, one consumer, priority
mode, `batch_confirm=true`) remains the best sustained throughput found
across every scaling strategy tested this session — consumer count,
connection count, and now queue count. None of them beat it. Whether a
real multi-node BlazingMQ cluster (matching Bloomberg's own published
setup) would show different scaling behavior is a real, open question
this investigation's scratch-broker environment cannot answer.

Config changes reverted the same way as every prior section
(`git status --short` clean in `~/blazingmq`), broker stopped, no
leftover `pg_blazingmq subscriber` connections.

## Filtered Consumption (`bmqeval`) Under Load

Every benchmark above calls `bmq_consume()` with `subscription_expr=NULL`
(verified via `grep bmq_consume( bench/*.py` before starting this
section) - none of them ever exercised BlazingMQ's native `bmqeval`
server-side filtering under real load. This matters directly: the
argument that pg_blazingmq doesn't need a `nats_sidecar`-equivalent
bolt-on (`bmqeval` already provides native, broker-evaluated content
filtering) had only ever been made from architecture/docs, never
measured. `PROFILING.md`'s "zero `bmqeval`/`SimpleEvaluator` occurrences"
finding is not evidence filtering is cheap - it's simply that filtering
was never invoked in any prior test.

New script: `bench/filtered_bench.py`, same bisection/backlog-curve
methodology as `sustained_bench.py`, but each consumer gets its own
`subscription_expr` instead of `NULL`. Fixture: `nyse_eqy_us_all_trade_
20260102`'s `Trade Through Exempt Indicator` column (bigint, eligible as
an `int8` property, real ~68%/32% split between values 0 and 1) aliased
to a plain identifier `texi` in the source query - `bmqeval` rejects
quoted/spaced identifiers outright (`'"Trade Through Exempt Indicator"
== 0'` fails validation with "expression does not use any properties",
even though the column publishes fine as a property under its real
name) - unrelated to the actual question, just a syntax fact worth
knowing before writing a `subscription_expr`.

**Single filtered consumer, isolating pure evaluation cost**: producer
publishes only `texi=0` rows (100% match rate, so the comparison isolates
"cost of having an active subscription filter", not "consumer receives
fewer messages"), one consumer with `subscription_expr = "texi == 0"`.
Bisected the same way as every other sustained number in this document:

| producer rate | backlog trend | bounded? |
|---|---|---|
| 2,200/s | flat (~1,000-2,900) | yes |
| 6,500/s | flat (~1,100-3,000), drains to near-zero after | **yes - the real ceiling** |
| 9,000/s | grows during steady state, recovers only in drain tail | no |
| 15,000/s | grows steadily, never recovers | no |
| 43,000/s (matching the unfiltered baseline) | runs away to 1.8M+ | no |

**Real single-consumer filtered ceiling: ~6,500/s, vs ~42,000-45,000/s
unfiltered - roughly a 6.5x cost for evaluating one simple `==`
expression against every published message.** This is a genuine,
substantial per-message cost, not noise - the 43,000/s attempt (same
rate as the unfiltered baseline) collapsed by more than an order of
magnitude, and even at 15,000/s (barely a third of the unfiltered rate)
the backlog never stabilized.

**Two consumers, distinct filters, realistic mixed traffic** (the actual
`nats_sidecar`-equivalent shape: multiple downstream consumers each
interested in a different content subset of the same stream) - producer
publishes a real mix of both `texi` values, consumer 0 filters
`texi == 0`, consumer 1 filters `texi == 1`:

| producer rate | backlog trend | bounded? |
|---|---|---|
| 4,000/s | flat (~1,600-3,300), drains after | yes |
| 6,000/s | grows steadily (4,611→10,531 in steady window) | no |

**Real 2-consumer filtered aggregate ceiling: ~4,000-5,000/s.** Content
verified throughout (`--verify-content`: each consumer's received
payloads checked against its own expected filter value; 0 mismatches
across every run) - the filtering is working correctly, this isn't a
routing bug inflating or deflating the numbers.

**The genuinely interesting comparison**: this ~4,000-5,000/s filtered
2-consumer number is *close to, not dramatically worse than*, the
already-established ~3,999/s unfiltered 2-consumer collapse (`README.md`'s
consumer-scaling section, commit `1336b78` - live-publish contention
between round-robin consumers on one queue). Filtering and multi-consumer
contention don't compound multiplicatively here; both roughly land on
the same order-of-magnitude ceiling independently. That's consistent
with a shared root cause rather than two independent, additive costs -
plausibly the same broker-side dispatcher/evaluation path handles both
"deciding which of several round-robin consumers gets a message" and
"deciding whether a subscribed consumer's filter matches a message",
though this session didn't `perf`-profile the filtered case to confirm
that directly (real, additional scope, not done here).

**Revised answer to "do we need a `bmq_sidecar`"**: bmqeval filtering is
real, correct, and does exactly what a `nats_sidecar`-equivalent would
need to do - but it is not free. At ~6,500/s (1 filtered consumer) to
~4,000-5,000/s (2 filtered consumers with distinct expressions) vs
~42,000-45,000/s unfiltered, native filtering costs roughly 6-10x
throughput on this scratch broker. Whether that's an acceptable price
depends entirely on the actual required rate - for any workload under a
few thousand messages/sec, native `bmqeval` filtering is still clearly
the right choice over an external sidecar (avoids an extra process, an
extra network hop, and the deserialize/re-evaluate/republish overhead a
sidecar would add on top of *its own* consume path). For a workload that
genuinely needs tens of thousands of filtered messages/sec, this number
says native filtering alone may not get there on this single-node broker
shape - though a sidecar wouldn't obviously do better, since it would
still have to consume the *unfiltered* stream at whatever the base
unfiltered ceiling is (already found to collapse under multi-consumer
contention too) before it could even begin filtering downstream.

Config changes reverted the same way as every prior section
(`git status --short` clean in `~/blazingmq`), broker stopped, no
leftover `pg_blazingmq subscriber` connections.
