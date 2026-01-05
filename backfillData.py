import time
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

DB_DSN = "dbname=bitcoin_state user=postgres password=YOUR_PASSWORD host=localhost port=5432"

KLINES_URL = "https://api.binance.com/api/v3/klines"
SYMBOL = "BTCUSDT"
INTERVAL = "1m"
LIMIT = 1000

INSERT_SQL = """
INSERT INTO btc_ohlcv (ts, timeframe, open, high, low, close, volume)
VALUES %s
ON CONFLICT (ts, timeframe) DO NOTHING;
"""

def fetch_klines(start_ms):
    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "limit": LIMIT,
        "startTime": start_ms
    }
    r = requests.get(KLINES_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()

    # start from most recent 1000 candles first (simple test)
    now_ms = int(time.time() * 1000)
    data = fetch_klines(now_ms - LIMIT * 60_000)

    rows = []
    for k in data:
        ts = datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc)
        rows.append((
            ts,
            "1m",
            float(k[1]),
            float(k[2]),
            float(k[3]),
            float(k[4]),
            float(k[5])
        ))

    execute_values(cur, INSERT_SQL, rows)
    conn.commit()

    cur.close()
    conn.close()

    print(f"Inserted {len(rows)} candles")

if __name__ == "__main__":
    main()
