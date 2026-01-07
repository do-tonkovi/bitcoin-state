-- sql/30_market_state.sql
-- Store market-state labels per timeframe (versioned via label_set_id)

BEGIN;

-- Enum-ish constraint via CHECK keeps it simple; we can expand later.
CREATE TABLE IF NOT EXISTS market_state (
  venue_id SMALLINT NOT NULL REFERENCES venue(venue_id),
  bucket_ts TIMESTAMPTZ NOT NULL,              -- start time of the bar/bucket
  timeframe_seconds INTEGER NOT NULL CHECK (timeframe_seconds IN (300, 1800, 3600, 14400, 86400, 604800)),
  label_set_id SMALLINT NOT NULL REFERENCES label_set(label_set_id),

  -- Your classification (keep minimal now; expand later)
  state TEXT NOT NULL CHECK (state IN ('range', 'trend', 'transition', 'unknown')),

  -- Minimal supporting metrics (so you can backtest without recomputing everything)
  atr_pct NUMERIC(10,6),          -- ATR / price (or similar)
  slope NUMERIC(18,10),           -- regression slope or EMA slope proxy
  efficiency NUMERIC(10,6),       -- e.g. net move / total movement
  notes JSONB NOT NULL DEFAULT '{}'::jsonb,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (venue_id, timeframe_seconds, label_set_id, bucket_ts)
);

-- Helpful index for “give me state for a window”
CREATE INDEX IF NOT EXISTS market_state_lookup_idx
  ON market_state (venue_id, timeframe_seconds, bucket_ts DESC, label_set_id);

COMMIT;
