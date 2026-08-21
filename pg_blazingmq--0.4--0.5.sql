\echo Use "ALTER EXTENSION pg_blazingmq UPDATE TO '0.5'" to load this file. \quit

-- 0.5: batch_confirm on bmq_consume(). See pg_blazingmq--0.5.sql for the
-- full doc comment on the new parameter's tradeoff.
CREATE OR REPLACE FUNCTION bmq_consume(
    queue_uri text,
    subscription_expr text DEFAULT NULL,
    max_messages int DEFAULT 1,
    timeout_ms int DEFAULT 1000,
    batch_confirm boolean DEFAULT false
)
RETURNS SETOF bytea
AS 'MODULE_PATHNAME', 'bmq_consume'
LANGUAGE C;
