#!/usr/bin/env python3
"""
Detect DAILY consolidation ranges (regimes) from TimescaleDB and print ranges overlapping 2025.

Your table schema:
  ts, timeframe, open, high, low, close, volume

Key behavior (matches your definition):
- Each range = a consolidation regime with STICKY bounds (from initial window).
- A range ends on:
    (A) confirmed breakout/impulse beyond sticky bounds, OR
    (B) loss of consolidation for several days in a row.
- After detection, we do a VERY conservative merge to fix micro-fragmentation:
    - Only merge if time gap is tiny AND price overlaps
    - AND crucially: NEVER merge across an impulse boundary (up_break/down_break).

DB connection:
- database: bitcoin_state
- user: dotonkovic
- host/port/password via env vars (defaults host=localhost, port=5432, password="")
  Set PGPASSWORD if needed.

Example run:
  export PGPASSWORD='yourpass'
  python detect_daily_ranges_2025.py
"""

import os
import math
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg  # psycopg v3


# ---------------------------
# DB config (edit TABLE if needed)
# ---------------------------
DB_NAME = "bitcoin_state"
DB_USER = "dotonkovic"

HOST = os.getenv("PGHOST", "localhost")
PORT = os.getenv("PGPORT", "5432")
PASSWORD = os.getenv("PGPASSWORD", "")

TABLE = "public.btc_ohlcv"
TS_COL = "ts"
TF_COL = "timeframe"

OPEN_COL = "open"
HIGH_COL = "high"
LOW_COL = "low"
CLOSE_COL = "close"
VOLUME_COL = "volume"

BASE_TIMEFRAME = "1m"


# ---------------------------
# Detection parameters (tune)
# ---------------------------
ATR_LEN = 20

RANGE_WINDOW = 20
RANGE_MIN_DAYS = 15

# Consolidation criteria
RANGE_EFF_MAX = 0.28
RANGE_WIDTH_ATR_MAX = 12.0

# Bound tolerance (small pokes don't end range)
BOUND_TOL_ATR_MULT = 0.25

# Breakout/impulse confirmation
BREAK_ATR_MULT = 1.2
BREAK_CONFIRM_DAYS = 2

# Exit on regime change (not consolidating anymore)
FAIL_CONSOLIDATION_DAYS = 3

POST_EXIT_COOLDOWN = 2


# ---------------------------
# Merge parameters (slightly more aggressive, but still do NOT destroy impulse-separated ranges)
# ---------------------------
MAX_GAP_DAYS = 5                 # allow a bit larger gap
PRICE_OVERLAP_TOL_ATR = 1.0      # allow more price overlap tolerance


@dataclass
class RangeSeg:
    start_ts: pd.Timestamp
    end_ts: pd.Timestamp
    days: int
    end_reason: str              # "up_break", "down_break", "lost_consolidation", "eod_close"
    range_low: float
    range_high: float
    atr_ref: float               # typical ATR inside segment (for merge tolerance)


def connect_db() -> psycopg.Connection:
    return psycopg.connect(
        host=HOST,
        port=PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=PASSWORD,
    )


def fetch_1d_ohlcv(conn: psycopg.Connection, start_ts: datetime, end_ts: datetime) -> pd.DataFrame:
    """
    Aggregate 1m -> 1D (UTC day buckets) using TimescaleDB first/last.
    """
    sql = f"""
    SELECT
      time_bucket(INTERVAL '1 day', {TS_COL}) AS ts,
      first({OPEN_COL}, {TS_COL}) AS open,
      max({HIGH_COL}) AS high,
      min({LOW_COL}) AS low,
      last({CLOSE_COL}, {TS_COL}) AS close,
      sum({VOLUME_COL}) AS volume
    FROM {TABLE}
    WHERE {TS_COL} >= %(start_ts)s
      AND {TS_COL} <  %(end_ts)s
      AND {TF_COL} = %(tf)s
    GROUP BY 1
    ORDER BY 1 ASC;
    """
    params = {"start_ts": start_ts, "end_ts": end_ts, "tf": BASE_TIMEFRAME}
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def compute_atr(df: pd.DataFrame, atr_len: int) -> pd.Series:
    high = df["high"].to_numpy(float)
    low = df["low"].to_numpy(float)
    close = df["close"].to_numpy(float)

    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    return pd.Series(tr).rolling(atr_len, min_periods=atr_len).mean()


def efficiency(closes: np.ndarray) -> float:
    diffs = np.abs(np.diff(closes))
    path = float(diffs.sum())
    net = float(abs(closes[-1] - closes[0]))
    return net / path if path > 0 else 0.0


def is_bell_shaped(closes: np.ndarray, bins: int = 12, min_peak_frac: float = 0.18, max_modes: int = 1, max_skew: float = 0.35) -> bool:
    """
    Returns True if the closes distribution is bell-shaped (unimodal, symmetric).
    - bins: number of histogram bins
    - min_peak_frac: minimum fraction of points in the peak bin
    - max_modes: maximum number of local maxima allowed
    - max_skew: maximum allowed absolute skewness
    """
    if len(closes) < 8:
        return False
    hist, bin_edges = np.histogram(closes, bins=bins)
    peak = hist.max()
    peak_idx = hist.argmax()
    total = hist.sum()
    if total == 0:
        return False
    # Peak must be significant
    if peak / total < min_peak_frac:
        return False
    # Count local maxima
    modes = 0
    for i in range(1, len(hist) - 1):
        if hist[i] > hist[i - 1] and hist[i] > hist[i + 1]:
            modes += 1
    if modes > max_modes:
        return False
    # Check symmetry (skewness)
    from scipy.stats import skew
    sk = skew(closes)
    if abs(sk) > max_skew:
        return False
    return True

def consolidation_ok(df: pd.DataFrame, i_end: int, w: int) -> bool:
    i0 = i_end - w + 1
    if i0 < 0:
        return False
    window = df.iloc[i0:i_end + 1]
    if window["atr"].isna().any():
        return False

    closes = window["close"].to_numpy(float)
    eff = efficiency(closes)

    width = float(window["high"].max() - window["low"].min())
    atr_mean = float(window["atr"].mean())
    width_atr = width / atr_mean if atr_mean > 0 else math.inf

    # Add bell-shape check
    bell = is_bell_shaped(closes)

    # New: Prevent strong trends from being classified as ranges
    net_change = abs(closes[-1] - closes[0])
    # If net change is more than 2.5 ATRs, it's likely a trend, not a range
    if atr_mean > 0 and net_change / atr_mean > 2.5:
        return False

    return (eff <= RANGE_EFF_MAX) and (width_atr <= RANGE_WIDTH_ATR_MAX) and bell


def segment_atr_ref(df: pd.DataFrame, start_i: int, end_i: int) -> float:
    atr_slice = df["atr"].iloc[start_i:end_i + 1].dropna()
    return float(atr_slice.median()) if not atr_slice.empty else 0.0


def detect_ranges(df: pd.DataFrame) -> list[RangeSeg]:
    """
    State machine:
      SEARCH -> IN_RANGE -> SEARCH ...
    Ranges have STICKY bounds set at creation time, not expanding forever.
    """
    ranges: list[RangeSeg] = []

    state = "SEARCH"
    cooldown = 0

    range_start_i = None
    range_high = None
    range_low = None
    bad_consol_streak = 0

    n = len(df)
    i = 0

    while i < n:
        if cooldown > 0:
            cooldown -= 1
            i += 1
            continue

        if state == "SEARCH":
            if i >= RANGE_WINDOW - 1 and consolidation_ok(df, i, RANGE_WINDOW):
                start_i = i - RANGE_WINDOW + 1
                init_window = df.iloc[start_i:i + 1]
                range_start_i = start_i

                # STICKY bounds from initial consolidation window
                range_high = float(init_window["high"].max())
                range_low = float(init_window["low"].min())

                bad_consol_streak = 0
                state = "IN_RANGE"
            i += 1
            continue

        # IN_RANGE
        assert range_start_i is not None and range_high is not None and range_low is not None

        atr_i = df["atr"].iloc[i]
        if pd.isna(atr_i) or atr_i <= 0:
            i += 1
            continue
        atr_i = float(atr_i)

        # Regime check (are we still consolidating?)
        if i >= RANGE_WINDOW - 1 and consolidation_ok(df, i, RANGE_WINDOW):
            bad_consol_streak = 0
        else:
            bad_consol_streak += 1

        if bad_consol_streak >= FAIL_CONSOLIDATION_DAYS:
            end_i = i - FAIL_CONSOLIDATION_DAYS
            if end_i >= range_start_i:
                days = end_i - range_start_i + 1
                if days >= RANGE_MIN_DAYS:
                    ranges.append(RangeSeg(
                        start_ts=df["ts"].iloc[range_start_i],
                        end_ts=df["ts"].iloc[end_i],
                        days=int(days),
                        end_reason="lost_consolidation",
                        range_low=float(range_low),
                        range_high=float(range_high),
                        atr_ref=segment_atr_ref(df, range_start_i, end_i),
                    ))

            # reset
            state = "SEARCH"
            cooldown = POST_EXIT_COOLDOWN
            range_start_i = None
            range_high = None
            range_low = None
            bad_consol_streak = 0
            i += 1
            continue

        # Breakout check vs sticky bounds, with tolerance band
        close_i = float(df["close"].iloc[i])

        upper_tol = range_high + BOUND_TOL_ATR_MULT * atr_i
        lower_tol = range_low - BOUND_TOL_ATR_MULT * atr_i

        # Still inside tolerance -> nothing to do
        if lower_tol <= close_i <= upper_tol:
            i += 1
            continue

        # Impulse breakout needs a stronger condition than tolerance
        up_break = close_i > (range_high + BREAK_ATR_MULT * atr_i)
        down_break = close_i < (range_low - BREAK_ATR_MULT * atr_i)

        # If we get a candle that closes outside the impulse threshold, end the range immediately at the previous candle,
        # but also allow the range to end at the exact breakout candle if the move is very large (e.g. >2.5 ATRs).
        if up_break or down_break:
            dirn = "up_break" if up_break else "down_break"
            # If the breakout candle itself is a huge move, end at this candle, else at previous
            breakout_move = abs(close_i - (range_high if up_break else range_low))
            if breakout_move > 2.5 * atr_i:
                end_i = i
            else:
                end_i = i - 1
            if end_i >= range_start_i:
                days = end_i - range_start_i + 1
                if days >= RANGE_MIN_DAYS:
                    ranges.append(RangeSeg(
                        start_ts=df["ts"].iloc[range_start_i],
                        end_ts=df["ts"].iloc[end_i],
                        days=int(days),
                        end_reason=dirn,
                        range_low=float(range_low),
                        range_high=float(range_high),
                        atr_ref=segment_atr_ref(df, range_start_i, end_i),
                    ))

            # reset
            state = "SEARCH"
            cooldown = POST_EXIT_COOLDOWN
            range_start_i = None
            range_high = None
            range_low = None
            bad_consol_streak = 0

            i += 1
            continue

        # outside tolerance but not an impulse: let regime check decide later
        i += 1
        continue

    # Close an open range at end-of-data
    if state == "IN_RANGE" and range_start_i is not None:
        end_i = n - 1
        days = end_i - range_start_i + 1
        if days >= RANGE_MIN_DAYS:
            ranges.append(RangeSeg(
                start_ts=df["ts"].iloc[range_start_i],
                end_ts=df["ts"].iloc[end_i],
                days=int(days),
                end_reason="eod_close",
                range_low=float(range_low),
                range_high=float(range_high),
                atr_ref=segment_atr_ref(df, range_start_i, end_i),
            ))

    return ranges


def merge_micro_fragments(ranges: list[RangeSeg]) -> list[RangeSeg]:
    """
    Conservative merge to reduce over-segmentation WITHOUT destroying regime boundaries.

    Rules:
    - Never merge across impulse exits (up_break/down_break in either end_reason).
    - Only merge if:
        gap_days <= MAX_GAP_DAYS
        AND price boxes overlap (with ATR tolerance)
    """
    if not ranges:
        return []

    ranges = sorted(ranges, key=lambda r: r.start_ts)
    merged = [ranges[0]]

    def is_impulse(reason: str) -> bool:
        return ("up_break" in reason) or ("down_break" in reason)

    for nxt in ranges[1:]:
        cur = merged[-1]

        # HARD RULE: don't merge if either segment indicates an impulse boundary
        if is_impulse(cur.end_reason) or is_impulse(nxt.end_reason):
            merged.append(nxt)
            continue

        gap_days = (nxt.start_ts.normalize() - cur.end_ts.normalize()).days - 1
        if gap_days < 0:
            gap_days = 0

        if gap_days > MAX_GAP_DAYS:
            merged.append(nxt)
            continue

        overlap_amt = min(cur.range_high, nxt.range_high) - max(cur.range_low, nxt.range_low)
        atr_ref = max(cur.atr_ref, nxt.atr_ref, 0.0)
        tol = PRICE_OVERLAP_TOL_ATR * atr_ref

        if overlap_amt >= -tol:
            new_start = cur.start_ts
            new_end = max(cur.end_ts, nxt.end_ts)
            new_low = min(cur.range_low, nxt.range_low)
            new_high = max(cur.range_high, nxt.range_high)
            days = int((new_end.normalize() - new_start.normalize()).days) + 1

            merged[-1] = RangeSeg(
                start_ts=new_start,
                end_ts=new_end,
                days=days,
                end_reason=f"{cur.end_reason}+{nxt.end_reason}",
                range_low=new_low,
                range_high=new_high,
                atr_ref=max(cur.atr_ref, nxt.atr_ref),
            )
        else:
            merged.append(nxt)

    return merged


def overlaps_2025(seg: RangeSeg) -> bool:
    y0 = pd.Timestamp("2025-01-01T00:00:00Z")
    y1 = pd.Timestamp("2026-01-01T00:00:00Z")
    return (seg.end_ts >= y0) and (seg.start_ts < y1)


def clip_to_2025(seg: RangeSeg) -> RangeSeg:
    y0 = pd.Timestamp("2025-01-01T00:00:00Z")
    y1 = pd.Timestamp("2026-01-01T00:00:00Z") - pd.Timedelta(seconds=1)
    start = max(seg.start_ts, y0)
    end = min(seg.end_ts, y1)
    days = int((end.normalize() - start.normalize()).days) + 1
    return RangeSeg(start, end, days, seg.end_reason, seg.range_low, seg.range_high, seg.atr_ref)


def fmt_ddmmYYYY(ts: pd.Timestamp) -> str:
    d = ts.date()
    return f"{d.day:02d}.{d.month:02d}.{d.year}"


def main():
    # Padding because your first 2025 range begins in Nov 2024
    start_ts = datetime(2024, 10, 15, tzinfo=timezone.utc)
    end_ts = datetime(2026, 1, 2, tzinfo=timezone.utc)

    with connect_db() as conn:
        df = fetch_1d_ohlcv(conn, start_ts, end_ts)

    if df.empty:
        print("No daily candles returned. Check TABLE name and timeframe values.")
        return

    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    df = df.sort_values("ts").reset_index(drop=True)
    df["atr"] = compute_atr(df, ATR_LEN)

    raw = detect_ranges(df)
    merged = merge_micro_fragments(raw)

    out = [clip_to_2025(r) for r in merged if overlaps_2025(r)]

    print(f"Ranges overlapping 2025: {len(out)}\n")
    for r in out:
        print(
            f"{fmt_ddmmYYYY(r.start_ts)} -> {fmt_ddmmYYYY(r.end_ts)}  "
            f"[{r.days}d]  box=[{r.range_low:.0f},{r.range_high:.0f}]  end={r.end_reason}"
        )


if __name__ == "__main__":
    main()
