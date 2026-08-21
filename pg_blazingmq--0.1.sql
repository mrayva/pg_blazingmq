\echo Use "CREATE EXTENSION pg_blazingmq" to load this file. \quit

-- Phase 1 proof-of-linkage only: touches real bmqt/bmqa symbols across the
-- full BDE/NTF/bmq dependency chain without needing a live broker, to prove
-- the extension .so actually links and loads into a Postgres backend.
CREATE FUNCTION pg_blazingmq_link_check(broker_uri text DEFAULT 'tcp://localhost:30114')
RETURNS text
AS 'MODULE_PATHNAME', 'pg_blazingmq_link_check'
LANGUAGE C STRICT;
