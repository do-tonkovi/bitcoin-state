#!/usr/bin/env python3
"""
Full backfill Binance BTCUSDT 1m candles into Postgres/TimescaleDB.

Modes:
- Default: resume from MAX(ts)+1m already in DB.
- Force full backfill: start from EXACT_START and walk forward, skipping duplicates via ON CONFLICT DO NOTHING.

Requirements:
  pip install requests psycopg2-binary
"""

import time
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone, timedelta

# -------------------- CONFIG --------------------
DB_DSN = "dbname=bitcoin_state user=postgres password=YOUR_PASSWORD host=localhost port=5432"

SYMBOL = "BTCUSDT"
INTERVAL = "1m"
TIMEFRAME_LABEL = "1m"
LIMIT = 1000
SLEEP_SECONDS = 0.15  # can be 0.1–0.3 safely

# Set to True to force a full walk from EXACT_START, even if DB has recent data
FORCE_FULL_BACKFILL = True

# ~6 years-ish anchor. Adjust if you want earlier/later.
EXACT_START = datetime(2019, 1, 1, tzinfo=timezone.utc)

BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"

INSERT_SQL = """
INSERT INTO btc_ohlcv (ts, timeframe, open, high, low, close, volume)
VALUES %s
ON CONFLICT (ts, timeframe) DO NOTHING;
"""
# ------------------------------------------------


def to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def fetch_klines(start_ms: int) -> list:
    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "limit": LIMIT,
        "startTime": start_ms,
    }
    r = requests.get(BINANCE_KLINES_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def get_start_ms(cur) -> int:
    """Return startTime in ms based on mode."""
    if FORCE_FULL_BACKFILL:
        return to_ms(EXACT_START)

    cur.execute(
        "SELECT MAX(ts) FROM btc_ohlcv WHERE timeframe = %s;",
        (TIMEFRAME_LABEL,),
    )
    max_ts = cur.fetchone()[0]
    if max_ts is None:
        return to_ms(EXACT_START)

    next_dt = (max_ts + timedelta(minutes=1)).replace(second=0, microsecond=0)
    return to_ms(next_dt)


def main():
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False
    cur = conn.cursor()

    start_ms = get_start_ms(cur)
    print(f"Starting from: {from_ms(start_ms).isoformat()} (UTC)")
    print("Mode:", "FORCE_FULL_BACKFILL" if FORCE_FULL_BACKFILL else "RESUME")

    pages = 0
    total_fetched = 0

    try:
        while True:
            # retry transient failures
            data = None
            for attempt in range(6):
                try:
                    data = fetch_klines(start_ms)
                    break
                except requests.RequestException as e:
                    wait = min(30.0, 1.8 ** attempt)
                    print(f"Fetch error (attempt {attempt+1}/6): {e} | sleeping {wait:.1f}s")
                    time.sleep(wait)

            if not data:
                print("No data returned. Done.")
                break

            rows = []
            for k in data:
                open_time_ms = int(k[0])
                ts = from_ms(open_time_ms)
                rows.append((
                    ts,
                    TIMEFRAME_LABEL,
                    float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])
                ))

            execute_values(cur, INSERT_SQL, rows, page_size=1000)
            conn.commit()

            pages += 1
            total_fetched += len(rows)
            print(
                f"Page {pages}: fetched={len(rows)} | "
                f"{rows[0][0].isoformat()} → {rows[-1][0].isoformat()} | total_fetched={total_fetched}"
            )

            # advance to next minute after last candle
            start_ms = int(data[-1][0]) + 60_000

            # stop if we're at the end
            if len(data) < LIMIT:
                print("Reached the most recent candles (final page < limit).")
                break

            time.sleep(SLEEP_SECONDS)

    finally:
        cur.close()
        conn.close()

    print("Backfill complete.")


if __name__ == "__main__":
    main()
