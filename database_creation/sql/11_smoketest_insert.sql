-- sql/11_smoketest_insert.sql
-- Insert one fake 1m candle to validate pipeline (delete later)

INSERT INTO ohlcv_1m
(venue_id, ts, open, high, low, close, volume_base, volume_quote, trades)
VALUES
(1, '2026-01-01 00:00:00+00', 42000, 42100, 41950, 42050, 12.34567890, 518000.00, 12345)
ON CONFLICT (venue_id, ts) DO UPDATE
SET open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume_base = EXCLUDED.volume_base,
    volume_quote = EXCLUDED.volume_quote,
    trades = EXCLUDED.trades;
