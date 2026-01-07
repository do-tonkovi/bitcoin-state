-- sql/20_30m_rollup.sql
-- Calendar-aligned (UTC) continuous aggregates from ohlcv_1m

BEGIN;
-- 30-minute OHLCV
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlcv_30m
WITH (timescaledb.continuous) AS
SELECT
  venue_id,
  time_bucket(INTERVAL '30 minutes', ts) AS bucket,
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
CREATE INDEX IF NOT EXISTS ohlcv_30m_idx ON ohlcv_30m (venue_id, bucket DESC);

-- Refresh policies
-- These keep aggregates updated as new 1m candles arrive.
-- end_offset keeps the “currently-forming” bucket out of refresh to avoid partials.
SELECT add_continuous_aggregate_policy('ohlcv_30m',
  start_offset => INTERVAL '90 days',
  end_offset   => INTERVAL '1 hour',
  schedule_interval => INTERVAL '10 minutes'
);


COMMIT;
