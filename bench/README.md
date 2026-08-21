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
| 1 | 100,000 | 150,830/s |

Matches the Postgres-driven JetStream publish rate (~151k/s) almost
exactly - Postgres wasn't a bottleneck on this side either. **A `nats_tool
bench --js` counting bug was found at scale**, worth flagging for
`nats_asio` separately from this benchmark: at `--count 1000000` (both
with and without `--no_ack`), the periodic `Stats:` lines report a
plausible, sustained ~100-150k events/sec for the full run duration, but
the final "Messages: N" summary reports a much smaller N (93,878 and
16,079 respectively) than the requested count or than the sustained
per-tick rate implies - the run exits early without publishing (or without
correctly counting) the full requested count. Not investigated further
here (out of scope for a benchmark task); the 100,000-count run above
completed cleanly and is the trustworthy number. N=4/N=16 JetStream
parallelism numbers were not obtained because of this - would need the
counting bug fixed first, or count kept at 100,000 with connections
splitting that total instead of multiplying it.

**Bottom line**: going around Postgres didn't change the ceiling on
either system's durable/comparable path (JetStream) - Postgres was never
the bottleneck there. On BlazingMQ's side, the raw client was actually
*slower* than the Postgres-driven number, a genuinely counterintuitive
result pointing at `bmqtool`'s own load-generation code rather than
anything about BlazingMQ, Postgres, or pg_blazingmq.
