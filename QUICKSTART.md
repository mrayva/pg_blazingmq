# Quick Start

Assumes a BlazingMQ checkout already built with position-independent code
enabled - see README.md's "Building" section for the exact commands
(BDE/NTF/bmq each need a PIC-enabled build; BlazingMQ's own
`bin/build-ubuntu.sh` doesn't produce one, since it only ever links static
libraries into executables).

## Build And Enable

```bash
make BMQ_ROOT=/path/to/your/blazingmq/checkout
sudo make install BMQ_ROOT=/path/to/your/blazingmq/checkout
psql -d postgres -c 'CREATE EXTENSION pg_blazingmq'
```

`BMQ_ROOT` defaults to `$HOME/blazingmq`.

## Start A Broker

For local testing, `test/manage_broker.sh` starts/stops a scratch
single-node broker on `tcp://localhost:30114`, reusing BlazingMQ's own
`docker/single-node/config`:

```bash
test/manage_broker.sh start
```

(`make test` does this automatically around `make installcheck` - see
README.md's "Testing" section. Also requires `bmqbrkr.tsk` built - see
README.md's "Building" section.)

## Publish And Pull-Consume

```sql
SET blazingmq.broker_uri = 'tcp://localhost:30114';

CREATE TABLE trades (region int, symbol text, price float8, active bool);
INSERT INTO trades VALUES (1, 'AAPL', 150.25, true), (2, 'MSFT', 305.5, false);

-- attr_columns picks which columns become server-side-filterable message
-- properties; the full row is always packed as the payload regardless.
SELECT bmq_publish_row('bmq://bmq.test.priority/trades', trades, ARRAY['region'])
FROM trades;

-- pull up to 5 messages, waiting up to 3s total
SELECT msgpack_to_jsonb(bmq_consume) AS payload
FROM bmq_consume('bmq://bmq.test.priority/trades', NULL, 5, 3000);
```

`msgpack_to_jsonb` is `pg_zerialize`'s decoder - `pg_blazingmq` packs
payloads as msgpack via `zerialize` but doesn't ship its own decoder, so
`pg_zerialize` (`CREATE EXTENSION pg_zerialize`) needs to be installed
alongside it to read payloads back in SQL.

## Push-Consume

```sql
CREATE TABLE received_messages (region int, symbol text, price float8, active bool);

CREATE FUNCTION handle_trade(payload bytea) RETURNS void AS $$
DECLARE j jsonb;
BEGIN
  j := msgpack_to_jsonb(payload);
  INSERT INTO received_messages (region, symbol, price, active)
  VALUES ((j->>'region')::int, j->>'symbol', (j->>'price')::float8, (j->>'active')::bool);
END;
$$ LANGUAGE plpgsql;

-- Workers use the database's broker_uri, not a session-local SET.
ALTER DATABASE mydb SET blazingmq.broker_uri = 'tcp://localhost:30114';

SELECT bmq_subscribe('bmq://bmq.test.priority/trades', 'handle_trade'::regproc) AS worker_pid;

-- ... messages arrive asynchronously, with no further action from this session ...

SELECT bmq_unsubscribe(<worker_pid>);  -- stops the worker cleanly
```

See README.md for the full function reference, filtering semantics, and
[ARCHITECTURE.md](ARCHITECTURE.md) for the internal design.
