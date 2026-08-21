-- Phase 2: bmq_publish_row(). Needs a live broker (manage_broker.sh).
SET blazingmq.broker_uri = 'tcp://localhost:30114';

CREATE TABLE pgregress_trades (region int, symbol text, price float8, active bool);
INSERT INTO pgregress_trades VALUES (1, 'AAPL', 150.25, true), (2, 'MSFT', 305.5, false);

-- Happy path: explicit attr_columns.
SELECT bmq_publish_row('bmq://bmq.test.mem.priority/pgregress_publish', pgregress_trades, ARRAY['region'])
FROM pgregress_trades;

-- Happy path: attr_columns omitted - every eligible column is promoted
-- automatically (region/active here; symbol is text and also eligible).
SELECT bmq_publish_row('bmq://bmq.test.mem.priority/pgregress_publish_default', pgregress_trades)
FROM pgregress_trades;

-- Negative path: a float8 column can't be a message attribute - must
-- error clearly, before any network call.
SELECT bmq_publish_row('bmq://bmq.test.mem.priority/pgregress_publish_bad', pgregress_trades, ARRAY['price'])
FROM pgregress_trades LIMIT 1;

-- Negative path: null queue_uri / row.
SELECT bmq_publish_row(NULL, pgregress_trades) FROM pgregress_trades LIMIT 1;
