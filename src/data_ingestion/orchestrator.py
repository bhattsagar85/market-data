from datetime import datetime, timedelta, date, time
import pandas as pd
import pytz
import logging
import yaml

from data_ingestion.timeframe_mapper import TIMEFRAMES
from scheduler.job_registry import get_job_config
from scheduler.guards import is_market_open

from data_ingestion.fetcher import fetch_candles
from data_ingestion.db_reader import get_last_candle_ts
from data_ingestion.writer import write_candles
from data_ingestion.gap_detector import detect_gaps
from data_ingestion.db import get_db_connection

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

MAX_BACKFILL_DAYS = 90

def parse_date_local(value) -> datetime:
    """
    Parse start/end values from YAML.
    Supports:
      - str: "2020-01-01"
      - date: datetime.date
      - datetime: datetime.datetime
    Returns timezone-aware datetime in IST.
    """
    if isinstance(value, datetime):
        if value.tzinfo:
            return value.astimezone(IST)
        return IST.localize(value)

    if isinstance(value, date):
        return IST.localize(
            datetime.combine(value, time(0, 0))
        )

    if isinstance(value, str):
        return IST.localize(
            datetime.strptime(value, "%Y-%m-%d")
        )

    raise ValueError(f"Invalid date value: {value}")


DEFAULT_INTRADAY_LOOKBACK_MINUTES = {
    "15M": 7 * 24 * 60,
    "5M": 3 * 24 * 60,
    "1M": 24 * 60,
}

# ─────────────────────────────────────────────
# Timeframe helpers
# ─────────────────────────────────────────────

def _timeframe_delta(timeframe: str) -> timedelta:
    if timeframe == "1D":
        return timedelta(days=1)
    return timedelta(minutes=TIMEFRAMES[timeframe]["minutes"])


def align_to_timeframe(ts: datetime, timeframe: str) -> datetime:
    ts = ts.astimezone(IST)

    if timeframe == "1D":
        return ts.replace(hour=0, minute=0, second=0, microsecond=0)

    minutes = TIMEFRAMES[timeframe]["minutes"]
    aligned_minute = (ts.minute // minutes) * minutes
    return ts.replace(minute=aligned_minute, second=0, microsecond=0)

# ─────────────────────────────────────────────
# Incremental ingestion helpers
# ─────────────────────────────────────────────

def resolve_start_ts(conn, symbol: str, timeframe: str) -> datetime:
    last_ts = get_last_candle_ts(conn, symbol, timeframe)
    now = datetime.now(IST)

    if last_ts:
        return last_ts + _timeframe_delta(timeframe)

    if timeframe == "1D":
        return now - timedelta(days=365 * 10)

    lookback = DEFAULT_INTRADAY_LOOKBACK_MINUTES[timeframe]
    return now - timedelta(minutes=lookback)

# ─────────────────────────────────────────────
# Scheduler ingestion (unchanged)
# ─────────────────────────────────────────────

def ingest_symbol(conn, symbol: str, timeframe: str):
    start = resolve_start_ts(conn, symbol, timeframe)
    now = datetime.now(IST)

    def safe_now(tf: str, now_: datetime) -> datetime:
        if tf == "1D":
            return now_ - timedelta(days=3)
        buffer_minutes = max(TIMEFRAMES[tf]["minutes"] * 3, 5)
        return now_ - timedelta(minutes=buffer_minutes)

    start = align_to_timeframe(start, timeframe)
    end = align_to_timeframe(safe_now(timeframe, now), timeframe)

    if start >= end:
        logger.info(f"{symbol} | {timeframe} | no new candles")
        return

    df = fetch_candles(symbol, timeframe, start, end)
    if df.empty:
        return

    if timeframe != "1D":
        gaps = detect_gaps(df, timeframe)
        for g_start, g_end in gaps:
            gap_df = fetch_candles(symbol, timeframe, g_start, g_end)
            if not gap_df.empty:
                df = pd.concat([df, gap_df], ignore_index=True)
        df = df.drop_duplicates(subset=["ts"]).sort_values("ts")

    write_candles(conn, symbol, timeframe, df)
    logger.info(f"{symbol} | {timeframe} | inserted {len(df)} candles")


def run_ingestion_job(job_name: str, symbols: list[str]):
    job = get_job_config(job_name)
    timeframe = job["timeframe"]

    if job["run_type"] == "INTRADAY" and not is_market_open():
        logger.info("Market closed — skipping job")
        return

    conn = get_db_connection()
    try:
        for s in symbols:
            ingest_symbol(conn, s, timeframe)
    finally:
        conn.close()

# ─────────────────────────────────────────────
# Backfill (single unit – unchanged)
# ─────────────────────────────────────────────

def run_backfill(symbol: str, timeframe: str, start: datetime, end: datetime):
    logger.info(f"🚀 Backfill start | {symbol} | {timeframe} | {start} → {end}")

    start = align_to_timeframe(start, timeframe)
    end = align_to_timeframe(end, timeframe)

    conn = get_db_connection()
    try:
        last_ts = get_last_candle_ts(conn, symbol, timeframe)
        chunk_start = last_ts + _timeframe_delta(timeframe) if last_ts else start

        while chunk_start < end:
            chunk_end = min(chunk_start + timedelta(days=MAX_BACKFILL_DAYS), end)
            df = fetch_candles(symbol, timeframe, chunk_start, chunk_end)
            if not df.empty:
                write_candles(conn, symbol, timeframe, df)
            chunk_start = chunk_end
    finally:
        conn.close()

    logger.info(f"🎉 Backfill completed | {symbol} | {timeframe}")

# ─────────────────────────────────────────────
# 🔥 MULTI-SYMBOL + MULTI-TIMEFRAME DISPATCHER
# ─────────────────────────────────────────────

def run_multi_backfill(cfg: dict):
    # symbols
    if "symbols" in cfg:
        symbols = cfg["symbols"]
    else:
        symbols = [cfg["symbol"]]

    # timeframes
    if "timeframes" in cfg:
        timeframes = cfg["timeframes"]
    else:
        timeframes = [cfg["timeframe"]]

    start = parse_date_local(cfg["start"])
    end = parse_date_local(cfg["end"])


    logger.info(
        f"🚀 MULTI BACKFILL | symbols={symbols} | "
        f"timeframes={timeframes} | {start.date()} → {end.date()}"
    )

    for s in symbols:
        for tf in timeframes:
            run_backfill(s, tf, start, end)

# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    if cfg.get("mode") == "backfill":
        run_multi_backfill(cfg)
    else:
        raise ValueError("Only backfill mode supported here")
