\echo Use "ALTER EXTENSION pg_blazingmq UPDATE TO '0.5'" to load this file. \quit

-- 0.5: batch_confirm on bmq_consume(). See pg_blazingmq--0.5.sql for the
-- full doc comment on the new parameter's tradeoff.
--
-- Adding a parameter changes bmq_consume()'s signature, so CREATE OR
-- REPLACE below would define a second, separate 5-arg overload alongside
-- the existing 4-arg one rather than replacing it - drop the old one
-- first so an upgraded install ends up in the exact same state (one
-- bmq_consume, 5 args) as a fresh CREATE EXTENSION at 0.5.
DROP FUNCTION bmq_consume(text, text, int, int);

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
