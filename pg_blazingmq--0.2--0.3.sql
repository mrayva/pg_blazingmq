\echo Use "ALTER EXTENSION pg_blazingmq UPDATE TO '0.3'" to load this file. \quit

CREATE FUNCTION bmq_consume(
    queue_uri text,
    subscription_expr text DEFAULT NULL,
    max_messages int DEFAULT 1,
    timeout_ms int DEFAULT 1000
)
RETURNS SETOF bytea
AS 'MODULE_PATHNAME', 'bmq_consume'
LANGUAGE C;
