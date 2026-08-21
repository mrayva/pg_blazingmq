\echo Use "ALTER EXTENSION pg_blazingmq UPDATE TO '0.4'" to load this file. \quit

CREATE FUNCTION bmq_subscribe(
    queue_uri text,
    callback_fn regproc,
    subscription_expr text DEFAULT NULL
)
RETURNS int
AS 'MODULE_PATHNAME', 'bmq_subscribe'
LANGUAGE C;

CREATE FUNCTION bmq_unsubscribe(worker_pid int)
RETURNS boolean
AS 'MODULE_PATHNAME', 'bmq_unsubscribe'
LANGUAGE C;
