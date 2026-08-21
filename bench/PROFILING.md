# Profiling bmqbrkr with perf

`perf record -g -F 999 -p <bmqbrkr pid>` during a 1,000,000-row
publish+consume run (`bmq_bench.py`, broadcast mode, `--batch-confirm`,
`nyse_eqy_us_all_trade_20260102`, content-verified PASS), 166,776 samples,
full debug symbols (`bmqbrkr.tsk` built `with debug_info, not stripped`).

## Headline finding: this broker binary is a Debug build

`~/blazingmq/build/blazingmq/CMakeCache.txt`: `CMAKE_BUILD_TYPE:STRING=Debug`.
Every pg_blazingmq/BlazingMQ number measured this session - including every
comparison against pgnats/NATS core in this same `bench/README.md` - was
against an **unoptimized Debug build** of `bmqbrkr.tsk`, while the NATS side
used an official pre-built release binary (`nats-server-v2.14.3-linux-amd64`
from GitHub Releases). This is a real, likely substantial confound on top of
everything else already found in this file (broker-warmup effects, the
NATS-side timing artifact) - a Release-mode rebuild and re-run is the
single highest-value next step for this benchmark thread, well above any
further micro-optimization hunting in this profile.

## Self-time breakdown (grouped)

Individual symbols mostly land in the 0.3-2.8% range (a flat profile, no
single dominant hot function) but group into clear categories:

- **Atomics / lock-free synchronization** (~6.4% combined): `bsls::
  AtomicOperations_ALL_ALL_GCCIntrinsics::{addInt64Nv,testAndSwapInt64,
  getInt64Acquire,addIntNvRelaxed,addIntNv,getInt64,getIntRelaxed,
  getIntAcquire,addInt64NvAcqRel}`, `AtomicInt::addRelaxed`. Traced (via
  `-g` call graph) substantially to `mqbstat::QueueStatsDomain::onEvent<>`
  and `bmqst::StatValue::{adjustValue,updateMinMax}` - BlazingMQ's own
  built-in stats system (`mqbstat`) uses atomic counters updated on every
  message, and that bookkeeping is itself a measurable cost.
- **Stack-unwind/backtrace machinery** (~4.7% combined): `_Unwind_Find_FDE`
  (2.80% - the single largest individual symbol in the whole profile),
  `__sframe_find_fre`, `_Unwind_Backtrace`, `backtrace_helper`,
  `balst::StackTraceTestAllocator::allocate`. **Not genuine C++ exception
  throwing** - no `__cxa_throw`/`__cxa_begin_catch` anywhere in the profile,
  and the call graph traces this to normal-path functions
  (`mqbblp::QueueHandle::postMessage`, `mqba::Dispatcher::dispatchEvent`,
  `OrderedHashMapWithHistory::insert`, `mqbstat::QueueStatsDomain::onEvent`),
  not error handling. The presence of `StackTraceTestAllocator` alongside
  the Debug build strongly suggests a stack-trace-capturing allocator is
  active on this build config, walking the stack on ordinary allocations -
  plausibly disabled or far cheaper in a Release build.
- **shared_ptr/ManagedPtr reference counting** (~2.2%): `bslma::
  SharedPtrRep::releaseRef()`, `bslma::ManagedPtr_Members::pointer()`,
  `bslstl::Function_Rep::invoker()`.
- **Blob/buffer manipulation** (~2.9%): `bdlbb::Blob::{numDataBuffers,
  length}`, `bmqu::BlobPosition::{buffer,BlobPosition,byte}`,
  `bdlbb::BlobUtil::{append,findOffset}` - real payload-copying cost,
  structurally necessary.
- **Protocol parsing/dispatch** (~3.2%): `bmqp::PutMessageIterator::next()`,
  `mqba::Dispatcher::EventCallback::operator()`, `mqba::ClientSession::
  {onPutEvent,onDispatcherEvent}`, `mqbblp::LocalQueue::postMessage`,
  `bsls::ByteOrderUtil::swapBytes32`, `bdlb::BigEndianUint32::operator
  unsigned int()` - the last two are wire-protocol endianness conversion,
  a real structural cost of BlazingMQ's richer binary protocol.
- **Networking/event loop** (~1.2%): `ntco::Epoll::run()`, `ntcr::
  StreamSocket::receive()`, `bmqio::NtcChannel::
  processReadQueueLowWatermark()`.
- **Clock/timing queries** (~1.5%): `__vdso_clock_gettime`,
  `clock_gettime`, `bmqu::Time::highResolutionTimer()` - plausibly tied to
  the same `mqbstat` timing/latency instrumentation as the atomics above.
- **Hash-map/bucket lookups** (~0.9%): `bmqc::OrderedHashMap_Bucket`
  related, `bslalg::HashTableAnchor::bucketArrayAddress()`.

## bmqeval: zero occurrences

`bmqeval`/`SimpleEvaluator` do not appear anywhere in the profile - this
decisively refutes the earlier `bmqeval` subscription-property-evaluation
hypothesis, at least for this specific workload (broadcast mode,
`bmq_consume(..., NULL, ...)` - no `subscription_expr`, so no expression
matching is ever invoked). The bookkeeping/synchronization/backtrace
categories above dominate instead.

## What this changes about the earlier "gap" story

The atomics + stack-unwind + refcounting categories combined (~13.3% of
self-time) noticeably outweigh the "useful work" categories - blob
manipulation + protocol parsing + networking (~7.3%). A meaningful chunk of
that 13.3%, particularly the stack-unwind machinery, is plausibly a
Debug-build-only cost. Re-running this same profile against a Release
build of `bmqbrkr.tsk` would be the natural next step to separate "real,
structural BlazingMQ overhead" from "overhead specific to this session's
unoptimized broker build."
