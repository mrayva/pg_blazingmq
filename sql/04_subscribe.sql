-- Phase 4: bmq_subscribe() / bmq_unsubscribe(). Needs a live broker
-- (manage_broker.sh). The subscribe/publish/verify/unsubscribe sequence
-- runs inside one DO block: bmq_subscribe() returns a real PID, which is
-- not reproducible across runs, so it must never appear directly in a
-- query result pg_regress would diff - only in RAISE messages we control,
-- or not at all on success.
--
-- Workers are separate OS processes with their own GUC state, so they use
-- the *database's* blazingmq.broker_uri, not a session-local SET (see
-- README) - hence ALTER DATABASE here instead of SET. \gset suppresses
-- display, so this doesn't leak the (test-run-dependent) database name
-- into the diffed output.
SELECT current_database() AS dbname \gset
ALTER DATABASE :"dbname" SET blazingmq.broker_uri = 'tcp://localhost:30114';
SET blazingmq.broker_uri = 'tcp://localhost:30114';

CREATE TABLE pgregress_subscribe_sink (region int, symbol text, price float8, active bool);
CREATE TABLE pgregress_subscribe_trades (region int, symbol text, price float8, active bool);
INSERT INTO pgregress_subscribe_trades VALUES (1, 'AAPL', 150.25, true);

CREATE FUNCTION pgregress_handle_trade(payload bytea) RETURNS void AS $$
DECLARE j jsonb;
BEGIN
  j := msgpack_to_jsonb(payload);
  INSERT INTO pgregress_subscribe_sink (region, symbol, price, active)
  VALUES ((j->>'region')::int, j->>'symbol', (j->>'price')::float8, (j->>'active')::bool);
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
  wpid int;
  i int;
BEGIN
  wpid := bmq_subscribe('bmq://bmq.test.mem.priority/pgregress_subscribe', 'pgregress_handle_trade'::regproc);

  PERFORM bmq_publish_row('bmq://bmq.test.mem.priority/pgregress_subscribe', pgregress_subscribe_trades, ARRAY['region'])
  FROM pgregress_subscribe_trades;

  FOR i IN 1..50 LOOP
    IF (SELECT count(*) FROM pgregress_subscribe_sink) > 0 THEN
      EXIT;
    END IF;
    PERFORM pg_sleep(0.1);
  END LOOP;

  IF NOT EXISTS (SELECT 1 FROM pgregress_subscribe_sink) THEN
    RAISE EXCEPTION 'push-consume: no message arrived within timeout';
  END IF;

  IF NOT bmq_unsubscribe(wpid) THEN
    RAISE EXCEPTION 'bmq_unsubscribe returned false for a worker pid we just started';
  END IF;
END;
$$;

-- The row content itself IS deterministic and worth checking directly.
SELECT region, symbol, price, active FROM pgregress_subscribe_sink;

-- Negative path: an arbitrary pid must be rejected, not signaled.
SELECT bmq_unsubscribe(1);
