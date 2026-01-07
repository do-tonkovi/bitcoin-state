-- sql/02_raw.sql
-- Raw canonical time-series (perp only, v1)

BEGIN;

-- 1-minute OHLCV (canonical grain)
-- Store as NUMERIC for safety; you can optimize later if needed.
CREATE TABLE IF NOT EXISTS ohlcv_1m (
  venue_id SMALLINT NOT NULL REFERENCES venue(venue_id),
  ts TIMESTAMPTZ NOT NULL,
  open  NUMERIC(18,2) NOT NULL,
  high  NUMERIC(18,2) NOT NULL,
  low   NUMERIC(18,2) NOT NULL,
  close NUMERIC(18,2) NOT NULL,
  volume_base  NUMERIC(28,8) NOT NULL,  -- BTC volume
  volume_quote NUMERIC(28,2),           -- USDT volume if available
  trades BIGINT,
  -- Binance klines are naturally upserted per (venue_id, ts)
  PRIMARY KEY (venue_id, ts)
);

-- Make it a hypertable (TimescaleDB)
SELECT create_hypertable('ohlcv_1m', 'ts',
  chunk_time_interval => INTERVAL '1 day',
  if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS ohlcv_1m_ts_desc_idx
  ON ohlcv_1m (venue_id, ts DESC);


-- Open interest 1-minute snapshots (perp venues only)
CREATE TABLE IF NOT EXISTS open_interest_1m (
  venue_id SMALLINT NOT NULL REFERENCES venue(venue_id),
  ts TIMESTAMPTZ NOT NULL,
  oi_contracts NUMERIC(28,8),          -- if exchange reports contracts
  oi_base      NUMERIC(28,8),          -- BTC
  oi_quote     NUMERIC(28,2),          -- USDT
  PRIMARY KEY (venue_id, ts)
);

SELECT create_hypertable('open_interest_1m', 'ts',
  chunk_time_interval => INTERVAL '1 day',
  if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS open_interest_1m_ts_desc_idx
  ON open_interest_1m (venue_id, ts DESC);

COMMIT;
