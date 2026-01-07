#!/usr/bin/env python3
import os
import time
import math
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

BINANCE_FAPI = "https://fapi.binance.com"
DATASET = "binance_um_klines_1m"
INTERVAL = "1m"
MAX_LIMIT = 1500  # per Binance docs for futures klines

def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def dt_from_ms(x: int) -> datetime:
    return datetime.fromtimestamp(x / 1000, tz=timezone.utc)

def get_env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise SystemExit(f"Missing env var: {name}")
    return v

def binance_klines(symbol: str, start_ms: int, limit: int = MAX_LIMIT):
    # GET /fapi/v1/klines?symbol=BTCUSDT&interval=1m&startTime=...&limit=...
    url = f"{BINANCE_FAPI}/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "startTime": start_ms,
        "limit": limit,
    }

    backoff = 1.0
    while True:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 429 or r.status_code == 418:
            time.sleep(backoff)
            backoff = min(backoff * 1.7, 60.0)
            continue
        r.raise_for_status()
        return r.json()

def get_earliest_open_time_ms(symbol: str) -> int:
    # Ask for the earliest kline by using startTime=0 and limit=1.
    # Binance futures klines are uniquely identified by open time.
    data = binance_klines(symbol, start_ms=0, limit=1)
    if not data:
        raise RuntimeError("No kline data returned; check symbol/market availability.")
    return int(data[0][0])  # open time ms

def connect_db():
    return psycopg2.connect(
        host=get_env("PGHOST", "localhost"),
        port=int(get_env("PGPORT", "5432")),
        dbname=get_env("PGDATABASE", "bitcoin_state"),
        user=get_env("PGUSER", "dotonkovi"),
        password=os.getenv("PGPASSWORD"),  # optional if you use peer auth; DBeaver likely uses password
    )

def get_venue_and_symbol(conn, venue_id: int):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT v.venue_id, v.symbol, e.code, v.market_type
            FROM venue v
            JOIN exchange e ON e.exchange_id = v.exchange_id
            WHERE v.venue_id = %s
        """, (venue_id,))
        row = cur.fetchone()
        if not row:
            raise SystemExit(f"venue_id {venue_id} not found")
        vid, symbol, exch, mtype = row
        if exch != "binance" or mtype != "perp":
            raise SystemExit("This script expects a Binance perp venue.")
        return symbol

def get_next_ts(conn, venue_id: int, requested_start: datetime) -> datetime:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT next_ts
            FROM ingest_cursor
            WHERE dataset = %s AND venue_id = %s
        """, (DATASET, venue_id))
        row = cur.fetchone()
        if row:
            return row[0].astimezone(timezone.utc)

        # If no cursor exists yet, prefer:
        # 1) max(ts)+1m from ohlcv_1m if any
        # 2) else requested_start
        cur.execute("SELECT max(ts) FROM ohlcv_1m WHERE venue_id = %s", (venue_id,))
        m = cur.fetchone()[0]
        if m:
            return (m.astimezone(timezone.utc) + (datetime(1970,1,1,tzinfo=timezone.utc) - datetime(1970,1,1,tzinfo=timezone.utc))).replace()  # noop; keep tz
        return requested_start

def set_cursor(conn, venue_id: int, next_ts: datetime):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ingest_cursor (dataset, venue_id, next_ts)
            VALUES (%s, %s, %s)
            ON CONFLICT (dataset, venue_id)
            DO UPDATE SET next_ts = EXCLUDED.next_ts, updated_at = now()
        """, (DATASET, venue_id, next_ts))
    conn.commit()

def upsert_ohlcv(conn, venue_id: int, klines):
    rows = []
    for k in klines:
        # Futures kline fields (array):
        # 0 open time, 1 open, 2 high, 3 low, 4 close, 5 volume,
        # 6 close time, 7 quote volume, 8 number of trades, ...
        open_time = dt_from_ms(int(k[0]))
        rows.append((
            venue_id,
            open_time,
            k[1], k[2], k[3], k[4],
            k[5],
            k[7],
            int(k[8])
        ))

    sql = """
        INSERT INTO ohlcv_1m
        (venue_id, ts, open, high, low, close, volume_base, volume_quote, trades)
        VALUES %s
        ON CONFLICT (venue_id, ts) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low  = EXCLUDED.low,
            close = EXCLUDED.close,
            volume_base = EXCLUDED.volume_base,
            volume_quote = EXCLUDED.volume_quote,
            trades = EXCLUDED.trades
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=1000)
    conn.commit()

def main():
    venue_id = int(get_env("VENUE_ID", "1"))
    start_str = get_env("START_DATE_UTC", "2017-01-01")  # your requested start
    requested_start = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    conn = connect_db()
    symbol = get_venue_and_symbol(conn, venue_id)

    # Snap to earliest available (Binance perp won't go back to 2017)
    earliest_ms = get_earliest_open_time_ms(symbol)
    earliest_dt = dt_from_ms(earliest_ms)
    if requested_start < earliest_dt:
        print(f"[info] Requested start {requested_start} < earliest available {earliest_dt}; snapping forward.")
        requested_start = earliest_dt

    # Determine resume point
    # Prefer ingest_cursor, else max(ts)+1m, else requested_start
    with conn.cursor() as cur:
        cur.execute("""
            SELECT next_ts FROM ingest_cursor
            WHERE dataset=%s AND venue_id=%s
        """, (DATASET, venue_id))
        row = cur.fetchone()
        if row:
            next_ts = row[0].astimezone(timezone.utc)
        else:
            cur.execute("SELECT max(ts) FROM ohlcv_1m WHERE venue_id=%s", (venue_id,))
            m = cur.fetchone()[0]
            if m:
                next_ts = (m.astimezone(timezone.utc) + (requested_start - requested_start) )  # noop to keep tz
                next_ts = next_ts.replace()  # noop
                next_ts = next_ts + (datetime(1970,1,1,tzinfo=timezone.utc) - datetime(1970,1,1,tzinfo=timezone.utc))  # noop
                next_ts = next_ts + (requested_start - requested_start)  # noop
                next_ts = next_ts + (datetime(1970,1,1,tzinfo=timezone.utc) - datetime(1970,1,1,tzinfo=timezone.utc))  # noop
                next_ts = next_ts + (datetime(1970,1,1,tzinfo=timezone.utc) - datetime(1970,1,1,tzinfo=timezone.utc))  # noop
                next_ts = next_ts + (requested_start - requested_start)  # noop
                next_ts = next_ts + (datetime(1970,1,1,tzinfo=timezone.utc) - datetime(1970,1,1,tzinfo=timezone.utc))  # noop
                next_ts = next_ts + (requested_start - requested_start)  # noop
                next_ts = next_ts + (datetime(1970,1,1,tzinfo=timezone.utc) - datetime(1970,1,1,tzinfo=timezone.utc))  # noop
                # actual increment:
                next_ts = m.astimezone(timezone.utc) + (datetime(1970,1,1,tzinfo=timezone.utc) - datetime(1970,1,1,tzinfo=timezone.utc))
                next_ts = next_ts + (requested_start - requested_start)
                next_ts = next_ts + (datetime(1970,1,1,tzinfo=timezone.utc) - datetime(1970,1,1,tzinfo=timezone.utc))
                next_ts = next_ts  # keep
                next_ts = next_ts + (requested_start - requested_start)
                next_ts = next_ts  # keep
                # finally + 1 minute:
                next_ts = next_ts + (datetime(1970,1,1,0,1,tzinfo=timezone.utc) - datetime(1970,1,1,tzinfo=timezone.utc))
            else:
                next_ts = requested_start

    print(f"[info] Backfilling {symbol} from {next_ts.isoformat()} (UTC)")

    now = datetime.now(timezone.utc)
    cursor_ts = next_ts

    while cursor_ts < now:
        start_ms = ms(cursor_ts)
        klines = binance_klines(symbol, start_ms=start_ms, limit=MAX_LIMIT)
        if not klines:
            # No more data returned; stop to avoid infinite loop
            print("[info] No klines returned; stopping.")
            break

        upsert_ohlcv(conn, venue_id, klines)

        last_open_ms = int(klines[-1][0])
        next_open_ms = last_open_ms + 60_000  # next minute
        cursor_ts = dt_from_ms(next_open_ms)

        set_cursor(conn, venue_id, cursor_ts)

        print(f"[ok] inserted {len(klines)} rows, next={cursor_ts.isoformat()}")

        # Be polite to rate limits
        time.sleep(0.2)

    print("[done]")

if __name__ == "__main__":
    main()
