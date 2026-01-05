import psycopg2
import numpy as np
from datetime import datetime, timezone, timedelta

DB_DSN = "dbname=bitcoin_state user=postgres password=YOUR_PASSWORD host=localhost port=5432"

INSERT_SQL = """
INSERT INTO btc_market_state (
  ts, timeframe,
  volatility, volume_zscore,
  oi_delta
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (ts, timeframe) DO UPDATE SET
  volatility=EXCLUDED.volatility,
  volume_zscore=EXCLUDED.volume_zscore,
  oi_delta=EXCLUDED.oi_delta;
"""

def main():
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False

    # Compute for the latest completed minute
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    ts = now - timedelta(minutes=1)

    with conn.cursor() as cur:
        # Pull last 120 minutes of 1m candles (enough for stable stats)
        cur.execute("""
          SELECT ts, close, volume
          FROM btc_ohlcv
          WHERE timeframe='1m' AND ts >= %s AND ts <= %s
          ORDER BY ts ASC
        """, (ts - timedelta(minutes=120), ts))
        rows = cur.fetchall()

        if len(rows) < 70:
            print("Not enough candle data yet to compute features.")
            return

        closes = np.array([r[1] for r in rows], dtype=float)
        vols = np.array([r[2] for r in rows], dtype=float)

        # 1m log returns
        rets = np.diff(np.log(closes))
        # volatility: rolling std over last 60 returns
        vol_window = rets[-60:]
        volatility = float(np.std(vol_window)) if len(vol_window) > 10 else None

        # volume zscore over last 60 mins
        vol_hist = vols[-60:]
        v_mean = float(np.mean(vol_hist))
        v_std = float(np.std(vol_hist))
        v_now = float(vols[-1])
        volume_z = float((v_now - v_mean) / v_std) if v_std > 0 else 0.0

        # Open interest delta over 5 mins
        cur.execute("SELECT open_interest FROM btc_open_interest WHERE ts=%s", (ts,))
        oi_now_row = cur.fetchone()
        cur.execute("SELECT open_interest FROM btc_open_interest WHERE ts=%s", (ts - timedelta(minutes=5),))
        oi_prev_row = cur.fetchone()

        oi_delta = None
        if oi_now_row and oi_prev_row:
            oi_delta = float(oi_now_row[0]) - float(oi_prev_row[0])

        cur.execute(INSERT_SQL, (ts, "1m", volatility, volume_z, oi_delta))
        conn.commit()

    conn.close()
    print(f"Inserted market_state for {ts.isoformat()}")

if __name__ == "__main__":
    main()
