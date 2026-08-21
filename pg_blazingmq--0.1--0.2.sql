\echo Use "ALTER EXTENSION pg_blazingmq UPDATE TO '0.2'" to load this file. \quit

CREATE FUNCTION bmq_publish_row(
    queue_uri text,
    row_data record,
    attr_columns text[] DEFAULT NULL
)
RETURNS void
AS 'MODULE_PATHNAME', 'bmq_publish_row'
LANGUAGE C;
