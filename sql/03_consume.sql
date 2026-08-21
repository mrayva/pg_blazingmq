-- Phase 3: bmq_consume(). Needs a live broker (manage_broker.sh) and
-- pg_zerialize (for msgpack_to_jsonb, to check payload content legibly).
SET blazingmq.broker_uri = 'tcp://localhost:30114';
CREATE EXTENSION IF NOT EXISTS pg_zerialize;

CREATE TABLE pgregress_consume_trades (region int, symbol text, price float8, active bool);
INSERT INTO pgregress_consume_trades VALUES (1, 'AAPL', 150.25, true), (2, 'MSFT', 305.5, false);

-- Basic round trip: publish, then pull both back, decode, and check
-- content - not just row counts.
SELECT bmq_publish_row('bmq://bmq.test.mem.priority/pgregress_consume', pgregress_consume_trades, ARRAY['region'])
FROM pgregress_consume_trades;

SELECT msgpack_to_jsonb(bmq_consume) AS payload
FROM bmq_consume('bmq://bmq.test.mem.priority/pgregress_consume', NULL, 5, 3000)
ORDER BY (msgpack_to_jsonb(bmq_consume)->>'region')::int;

-- Server-side filtering: the filter must be established *before* the
-- messages it should apply to are published (see README) - establishing
-- it first, then publishing, then consuming should return only the
-- matching row.
SELECT count(*) AS pre_publish_count
FROM bmq_consume('bmq://bmq.test.mem.priority/pgregress_consume_filtered', 'region == 1', 5, 500);

SELECT bmq_publish_row('bmq://bmq.test.mem.priority/pgregress_consume_filtered', pgregress_consume_trades, ARRAY['region'])
FROM pgregress_consume_trades;

SELECT msgpack_to_jsonb(bmq_consume) AS payload
FROM bmq_consume('bmq://bmq.test.mem.priority/pgregress_consume_filtered', 'region == 1', 5, 3000);

-- Negative path: max_messages/timeout_ms validation.
SELECT * FROM bmq_consume('bmq://bmq.test.mem.priority/pgregress_consume_bad', NULL, 0, 1000);
SELECT * FROM bmq_consume('bmq://bmq.test.mem.priority/pgregress_consume_bad', NULL, 1, -1);
