\echo Use "CREATE EXTENSION pg_blazingmq" to load this file. \quit

-- Phase 1 proof-of-linkage only: touches real bmqt/bmqa symbols across the
-- full BDE/NTF/bmq dependency chain without needing a live broker, to prove
-- the extension .so actually links and loads into a Postgres backend.
CREATE FUNCTION pg_blazingmq_link_check(broker_uri text DEFAULT 'tcp://localhost:30114')
RETURNS text
AS 'MODULE_PATHNAME', 'pg_blazingmq_link_check'
LANGUAGE C STRICT;

-- Phase 2: publish a row to a BlazingMQ queue.
--
-- attr_columns, if given, names exactly which columns become typed
-- bmqa::MessageProperties (server-side filterable via BlazingMQ's own
-- subscription expressions, e.g. "region == 1 && active") - only
-- bool/int2/int4/int8/text-family columns are eligible; naming an
-- ineligible column is an error. If attr_columns is NULL (the default),
-- every eligible column is promoted automatically and ineligible ones are
-- silently left out of the attribute set (but still appear in the
-- payload).
--
-- The full row - every column, not just the attribute set - is always
-- packed as the message payload (msgpack via zerialize).
CREATE FUNCTION bmq_publish_row(
    queue_uri text,
    row_data record,
    attr_columns text[] DEFAULT NULL
)
RETURNS void
AS 'MODULE_PATHNAME', 'bmq_publish_row'
LANGUAGE C;

-- Phase 3: synchronously pull up to max_messages payloads from a queue,
-- waiting up to timeout_ms total (not per message) for them to arrive.
--
-- subscription_expr, if given, is BlazingMQ's own bmqeval expression
-- (e.g. "region == 1 && active") applied server-side: only messages
-- matching it are delivered to this queue handle at all. It's fixed for
-- the lifetime of the queue's cached handle in this backend - calling
-- bmq_consume() again on the same queue_uri with a different
-- subscription_expr from the same session is an error (see README).
--
-- batch_confirm (0.5+, default false) trades a wider at-least-once
-- redelivery window for fewer, larger CONFIRM wire messages: with the
-- default false, each received message is confirmed individually,
-- immediately after being added to the result set, so a mid-call error
-- only leaves the message(s) not yet added unconfirmed. With true,
-- confirmations are accumulated in one bmqa::ConfirmEventBuilder and sent
-- as a batch (via session.confirmMessages()) only once, after the whole
-- call's receive loop finishes - if the backend dies before that flush,
-- every message received so far in this call is redelivered, not just
-- the last one. See README's Testing/benchmarking notes for measured
-- throughput impact.
--
-- Returns raw payload bytes only (msgpack, if produced by
-- bmq_publish_row) - deserialize with pg_zerialize's own
-- msgpack_to_jsonb() or msgpack_populate_record() downstream.
CREATE FUNCTION bmq_consume(
    queue_uri text,
    subscription_expr text DEFAULT NULL,
    max_messages int DEFAULT 1,
    timeout_ms int DEFAULT 1000,
    batch_confirm boolean DEFAULT false
)
RETURNS SETOF bytea
AS 'MODULE_PATHNAME', 'bmq_consume'
LANGUAGE C;

-- Phase 4: push-consume via a background worker.
--
-- Registers a dedicated background worker that stays subscribed to
-- queue_uri indefinitely, calling callback_fn(payload bytea) for every
-- message received (subscription_expr, if given, filters server-side same
-- as bmq_consume - and has the same "must be established before the
-- messages you want filtered are published" semantic, see README).
--
-- callback_fn must take exactly one bytea argument; its return value is
-- ignored. It runs in its own transaction per message. If it raises an
-- error, that transaction is rolled back, a WARNING is logged, and the
-- message is left *unconfirmed* - BlazingMQ will redeliver it. Messages
-- are only confirmed after the callback succeeds (at-least-once
-- delivery), unlike bmq_consume() which confirms unconditionally.
--
-- Returns the worker's PID, which doubles as the subscription handle for
-- bmq_unsubscribe(). The worker uses the database's default
-- blazingmq.broker_uri - a session-local SET does not propagate to it.
CREATE FUNCTION bmq_subscribe(
    queue_uri text,
    callback_fn regproc,
    subscription_expr text DEFAULT NULL
)
RETURNS int
AS 'MODULE_PATHNAME', 'bmq_subscribe'
LANGUAGE C;

-- Stops a subscriber worker started by bmq_subscribe(). Validates
-- worker_pid is an active pg_blazingmq subscriber (via pg_stat_activity)
-- before signaling it, so this can't be used to terminate arbitrary
-- processes.
CREATE FUNCTION bmq_unsubscribe(worker_pid int)
RETURNS boolean
AS 'MODULE_PATHNAME', 'bmq_unsubscribe'
LANGUAGE C;
