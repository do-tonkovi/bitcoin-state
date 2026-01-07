-- sql/01_dimensions.sql
-- Core dimensions + metadata tables (minimal, future-proof)

BEGIN;

-- Exchanges (Binance now, others later)
CREATE TABLE IF NOT EXISTS exchange (
  exchange_id SMALLSERIAL PRIMARY KEY,
  code TEXT NOT NULL UNIQUE,          -- e.g. 'binance'
  name TEXT NOT NULL
);

-- Trading venues (spot/perp/futures + symbol mapping)
-- For v1 you’ll only insert perp venues, but the model supports spot later.
CREATE TABLE IF NOT EXISTS venue (
  venue_id SMALLSERIAL PRIMARY KEY,
  exchange_id SMALLINT NOT NULL REFERENCES exchange(exchange_id),
  market_type TEXT NOT NULL CHECK (market_type IN ('spot','perp','futures')),
  symbol TEXT NOT NULL,               -- e.g. 'BTCUSDT' for Binance perp
  base TEXT NOT NULL DEFAULT 'BTC',
  quote TEXT NOT NULL,                -- 'USDT'
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  UNIQUE (exchange_id, market_type, symbol)
);

-- Feature set versioning (so you can recompute without overwriting history)
CREATE TABLE IF NOT EXISTS feature_set (
  feature_set_id SMALLSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,          -- e.g. 'vwap_v1', 'profile_v1'
  description TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  params JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Label set versioning (range/trend rules, regime definitions, etc.)
CREATE TABLE IF NOT EXISTS label_set (
  label_set_id SMALLSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,          -- e.g. 'day_type_v1', 'regime_v1'
  description TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  params JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Seed: Binance exchange
INSERT INTO exchange (code, name)
VALUES ('binance', 'Binance')
ON CONFLICT (code) DO NOTHING;

COMMIT;
