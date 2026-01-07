-- sql/20_rollups.sql
-- Calendar-aligned (UTC) continuous aggregates from ohlcv_1m

BEGIN;

-- 5-minute OHLCV
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_5m
WITH (timescaledb.continuous) AS
SELECT
  venue_id,
  time_bucket(INTERVAL '5 minutes', ts) AS bucket,
  first(open, ts)  AS open,
  max(high)        AS high,
  min(low)         AS low,
  last(close, ts)  AS close,
  sum(volume_base) AS volume_base,
  sum(volume_quote) AS volume_quote,
  sum(trades)      AS trades
FROM ohlcv_1m
GROUP BY venue_id, bucket
WITH NO DATA;

-- 1-hour OHLCV
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_1h
WITH (timescaledb.continuous) AS
SELECT
  venue_id,
  time_bucket(INTERVAL '1 hour', ts) AS bucket,
  first(open, ts)  AS open,
  max(high)        AS high,
  min(low)         AS low,
  last(close, ts)  AS close,
  sum(volume_base) AS volume_base,
  sum(volume_quote) AS volume_quote,
  sum(trades)      AS trades
FROM ohlcv_1m
GROUP BY venue_id, bucket
WITH NO DATA;

-- 4-hour OHLCV
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_4h
WITH (timescaledb.continuous) AS
SELECT
  venue_id,
  time_bucket(INTERVAL '4 hours', ts) AS bucket,
  first(open, ts)  AS open,
  max(high)        AS high,
  min(low)         AS low,
  last(close, ts)  AS close,
  sum(volume_base) AS volume_base,
  sum(volume_quote) AS volume_quote,
  sum(trades)      AS trades
FROM ohlcv_1m
GROUP BY venue_id, bucket
WITH NO DATA;

-- 1-day OHLCV (UTC calendar days)
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_1d
WITH (timescaledb.continuous) AS
SELECT
  venue_id,
  time_bucket(INTERVAL '1 day', ts) AS bucket,
  first(open, ts)  AS open,
  max(high)        AS high,
  min(low)         AS low,
  last(close, ts)  AS close,
  sum(volume_base) AS volume_base,
  sum(volume_quote) AS volume_quote,
  sum(trades)      AS trades
FROM ohlcv_1m
GROUP BY venue_id, bucket
WITH NO DATA;

-- 1-week OHLCV (calendar-aligned weeks; for timestamptz this is UTC-based bucketing)
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_1w
WITH (timescaledb.continuous) AS
SELECT
  venue_id,
  time_bucket(INTERVAL '1 week', ts) AS bucket,
  first(open, ts)  AS open,
  max(high)        AS high,
  min(low)         AS low,
  last(close, ts)  AS close,
  sum(volume_base) AS volume_base,
  sum(volume_quote) AS volume_quote,
  sum(trades)      AS trades
FROM ohlcv_1m
GROUP BY venue_id, bucket
WITH NO DATA;

-- Indexes (speed up range scans)
CREATE INDEX IF NOT EXISTS ohlcv_5m_idx ON ohlcv_5m (venue_id, bucket DESC);
CREATE INDEX IF NOT EXISTS ohlcv_1h_idx ON ohlcv_1h (venue_id, bucket DESC);
CREATE INDEX IF NOT EXISTS ohlcv_4h_idx ON ohlcv_4h (venue_id, bucket DESC);
CREATE INDEX IF NOT EXISTS ohlcv_1d_idx ON ohlcv_1d (venue_id, bucket DESC);
CREATE INDEX IF NOT EXISTS ohlcv_1w_idx ON ohlcv_1w (venue_id, bucket DESC);

-- Refresh policies
-- These keep aggregates updated as new 1m candles arrive.
-- end_offset keeps the “currently-forming” bucket out of refresh to avoid partials.
SELECT add_continuous_aggregate_policy('ohlcv_5m',
  start_offset => INTERVAL '30 days',
  end_offset   => INTERVAL '10 minutes',
  schedule_interval => INTERVAL '5 minutes'
);

SELECT add_continuous_aggregate_policy('ohlcv_1h',
  start_offset => INTERVAL '180 days',
  end_offset   => INTERVAL '2 hours',
  schedule_interval => INTERVAL '15 minutes'
);

SELECT add_continuous_aggregate_policy('ohlcv_4h',
  start_offset => INTERVAL '365 days',
  end_offset   => INTERVAL '6 hours',
  schedule_interval => INTERVAL '1 hour'
);

SELECT add_continuous_aggregate_policy('ohlcv_1d',
  start_offset => INTERVAL '10 years',
  end_offset   => INTERVAL '2 days',
  schedule_interval => INTERVAL '6 hours'
);

SELECT add_continuous_aggregate_policy('ohlcv_1w',
  start_offset => INTERVAL '20 years',
  end_offset   => INTERVAL '2 weeks',
  schedule_interval => INTERVAL '1 day'
);

COMMIT;
