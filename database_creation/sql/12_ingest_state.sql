-- sql/12_ingest_state.sql
-- Track ingestion/backfill progress per dataset + venue

BEGIN;

CREATE TABLE IF NOT EXISTS ingest_cursor (
  dataset TEXT NOT NULL,              -- e.g. 'binance_um_klines_1m'
  venue_id SMALLINT NOT NULL REFERENCES venue(venue_id),
  next_ts TIMESTAMPTZ NOT NULL,       -- next candle open time to fetch
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (dataset, venue_id)
);

COMMIT;
