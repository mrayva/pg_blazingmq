# Architecture

## Overview

`pg_blazingmq` is a single-file PostgreSQL C++ extension (PGXS-based,
mirroring `pg_zerialize`'s build conventions) that links the real BlazingMQ
client stack (`bmqa`/`bmqt`/`bdlbb`, from BDE/NTF/bmq's `bmq` group only -
no `mqb` broker-internals) directly into the Postgres backend.

Two things happen on publish, and they're independent of each other:

1. The full row is packed as the message **payload** (msgpack, via
   `zerialize::dyn::Value` + `zerialize::serialize<MsgPack>`).
2. Selected columns are additionally promoted to typed
   `bmqa::MessageProperties`, which BlazingMQ's own `bmqeval` expression
   engine evaluates **server-side** at message-arrival time
   (`region == 1 && active`) - there is no external matching engine to run
   or keep in sync, unlike NATS core subjects.

## Session And Queue Lifecycle

- One `bmqa::Session` per backend process, created lazily on first use
  (`get_session()`) and stopped via `on_proc_exit`.
- Queue handles are cached per URI in a single
  `unordered_map<string, QueueEntry>`, always opened with combined
  `e_READ | e_WRITE` flags. This was forced by an empirical discovery, not
  a design preference: BlazingMQ rejects opening the same queue URI twice
  from one session, even with different flags (`ALREADY_OPENED`) - so
  `bmq_publish_row()` and `bmq_consume()` against the same queue in the
  same backend must share one handle.
- If a call asks for a `subscription_expr` different from the cached
  handle's current one, the handle is reconfigured in place
  (`session.configureQueueSync()`) rather than erroring - this is what
  makes "publish, then consume with a filter" work within a single
  session.
- **Filtering is not retroactive.** BlazingMQ evaluates a subscription's
  filter at message-*arrival* time against whichever filter is active
  *then*. Reconfiguring or opening a filtered handle after messages have
  already been published to that queue does not retroactively filter
  those already-queued messages - they were queued under the filter that
  was active when they arrived.

## Column → Property / Payload Split (`bmq_publish_row`)

`heap_deform_tuple` walks the row's columns in `TupleDesc` order, once,
building both structures in the same pass:

- **Properties** (`set_message_property`): only `bool`/`int2`/`int4`/
  `int8`/text-family columns are eligible. `attr_columns`, if given,
  names exactly which eligible columns become properties - naming an
  ineligible column (e.g. `float8`) is a clear error raised before any
  network call. If `attr_columns` is omitted, every eligible column is
  promoted automatically.
- **Payload** (`datum_to_dyn_value`): every column, eligible or not, is
  always included - this mapping is permissive (numeric types handled
  directly, everything else falls back to `OidOutputFunctionCall` +
  string), since the payload has no BlazingMQ-imposed type restrictions.

The asymmetry matters: a `float8` column can never be a filter attribute,
but it always appears in the payload.

## Push-Consume: Background Worker + DSM Handoff

`bmq_subscribe()` registers a dynamic background worker
(`RegisterDynamicBackgroundWorker`) rather than using a
`shmem_request_hook`-managed structure, specifically to avoid the latter's
requirement of `shared_preload_libraries` plus a server restart -
`bmq_subscribe()` needs to work immediately after `CREATE EXTENSION`.

**Config handoff.** A fixed-size `SubscriberConfig` struct (queue URI,
subscription expression, callback OID, target database/role) is written
into a Dynamic Shared Memory segment, pinned (`dsm_pin_segment`) so it
outlives the creating backend, and handed to the worker via
`bgw_main_arg` (the segment's `dsm_handle`, which fits a `Datum`).

**Readiness handshake.** `WaitForBackgroundWorkerStartup()` only confirms
the worker's OS process has started - not that it has finished
`BackgroundWorkerInitializeConnectionByOid` and opened its read queue (a
real network round trip). Combined with the non-retroactive filtering
semantic above, a message published immediately after `bmq_subscribe()`
returns could be delivered to no one if the queue isn't open yet. A
`pg_atomic_uint32 ready` field in the same DSM segment closes this: the
worker flips it to 1 right after `get_queue()` succeeds (keeping the
segment attached until then, unlike the queue URI/expression/callback
fields, which are copied to local variables immediately and don't need
the segment again); `bmq_subscribe()` polls it, bounded to 5 seconds,
before returning. This is best-effort, not a hard guarantee - a worker
still slow to connect after 5s (broker unreachable, etc.) still gets its
PID back, with a `WARNING` instead of a hard failure, since it keeps
running and retrying independently either way.

**No separate registry.** The worker's PID doubles as the subscription
handle. Every background worker already appears in `pg_stat_activity`
with `backend_type` set to `bgw_type` (`'pg_blazingmq subscriber'` here),
so no custom shared-memory registry table is needed - `bmq_unsubscribe()`
checks `pg_stat_activity` for a matching `(pid, backend_type)` pair before
signaling, which is also what stops it from being used to terminate
arbitrary processes.

**Per-message dispatch.** Each message runs in its own transaction:
`StartTransactionCommand` / `SPI_connect` / `PushActiveSnapshot`, then
`OidFunctionCall1(callback_fn, ...)` inside `PG_TRY`/`PG_CATCH`. On
success, the transaction commits and *then* the message is confirmed
(`session.confirmMessage`). On failure, the transaction is aborted, a
`WARNING` is logged, and the message is left unconfirmed - BlazingMQ
redelivers it. This is deliberately at-least-once, unlike `bmq_consume()`,
which confirms unconditionally as soon as a message is added to its
result set.

**GUC scoping.** `blazingmq.broker_uri` is a per-process GUC. The worker
is a separate OS process with its own GUC state, so a session-local `SET`
in the calling backend never reaches it - the worker picks up the
*database's* default instead (`ALTER DATABASE ... SET
blazingmq.broker_uri = ...`), which is why `bmq_subscribe()`'s usage
examples set it that way rather than with a plain `SET`.

## Type Mapping Summary

| Postgres type | Message property (`attr_columns`) | Payload |
|---|---|---|
| `bool` | `setPropertyAsBool` | included |
| `int2` | `setPropertyAsShort` | included |
| `int4` | `setPropertyAsInt32` | included |
| `int8` | `setPropertyAsInt64` | included |
| `text`/`varchar`/`bpchar` | `setPropertyAsString` | included |
| `float4`/`float8` | not eligible (error if named) | included |
| everything else | not eligible (error if named) | included, via `OidOutputFunctionCall` text fallback |

`bmqa::MessageProperties` has no float/double type, and BlazingMQ's
`bmqeval` grammar has no list-membership operator - both are hard limits
of the underlying property/expression system, not something this
extension works around.

## Build

Client-only `bmq` group (16 ninja targets: `libbmqa.a` through `libbmq.a`
- no `mqb/`, which is broker-internal). Requires BDE/NTF/bmq built with
`-fPIC` enabled, since none of BlazingMQ's own build scripts do this by
default (they only ever link static libraries into executables). See
README.md's "Building" section for the exact reconfigure commands.
