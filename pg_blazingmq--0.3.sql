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
-- Each received message is confirmed (acknowledged) immediately after
-- being added to the result set - there is no separate ack step in this
-- first cut.
--
-- subscription_expr, if given, is BlazingMQ's own bmqeval expression
-- (e.g. "region == 1 && active") applied server-side: only messages
-- matching it are delivered to this queue handle at all. It's fixed for
-- the lifetime of the queue's cached handle in this backend - calling
-- bmq_consume() again on the same queue_uri with a different
-- subscription_expr from the same session is an error (see README).
--
-- Returns raw payload bytes only (msgpack, if produced by
-- bmq_publish_row) - deserialize with pg_zerialize's own
-- msgpack_to_jsonb() or msgpack_populate_record() downstream.
CREATE FUNCTION bmq_consume(
    queue_uri text,
    subscription_expr text DEFAULT NULL,
    max_messages int DEFAULT 1,
    timeout_ms int DEFAULT 1000
)
RETURNS SETOF bytea
AS 'MODULE_PATHNAME', 'bmq_consume'
LANGUAGE C;
