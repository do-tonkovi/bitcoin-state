-- sql/10_seed_venues.sql
-- Seed initial venues (perp only v1)

BEGIN;

-- Ensure Binance exists (harmless if already inserted)
INSERT INTO exchange (code, name)
VALUES ('binance', 'Binance')
ON CONFLICT (code) DO NOTHING;

-- Insert Binance Perp BTCUSDT
INSERT INTO venue (exchange_id, market_type, symbol, base, quote, is_active)
SELECT e.exchange_id, 'perp', 'BTCUSDT', 'BTC', 'USDT', TRUE
FROM exchange e
WHERE e.code = 'binance'
ON CONFLICT (exchange_id, market_type, symbol) DO NOTHING;

COMMIT;
