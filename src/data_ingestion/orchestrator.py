# src/data_ingestion/orchestrator.py

from datetime import datetime, timedelta, date
import pandas as pd
import pytz
import logging

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

# Default backfill windows (minutes) for first-ever intraday runs
DEFAULT_INTRADAY_LOOKBACK_MINUTES = {
    "15M": 7 * 24 * 60,
    "5M": 3 * 24 * 60,
    "1M": 24 * 60,
}

# ─────────────────────────────────────────────
# Timeframe helpers
# ─────────────────────────────────────────────

# def _timeframe_delta(timeframe: str) -> timedelta:
#     if timeframe == "1D":
#         return timedelta(days=1)
#     if timeframe == "15M":
#         return timedelta(minutes=15)
#     if timeframe == "5M":
#         return timedelta(minutes=5)
#     if timeframe == "1M":
#         return timedelta(minutes=1)
#     raise ValueError(f"Unsupported timeframe: {timeframe}")

def _timeframe_delta(timeframe: str) -> timedelta:
    try:
        minutes = TIMEFRAMES[timeframe]["minutes"]
    except KeyError:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    if timeframe == "1D":
        return timedelta(days=1)

    return timedelta(minutes=minutes)


def align_to_timeframe(ts: datetime, timeframe: str) -> datetime:
    ts = ts.astimezone(IST)

    if timeframe == "1D":
        return ts.replace(hour=0, minute=0, second=0, microsecond=0)

    try:
        minutes = TIMEFRAMES[timeframe]["minutes"]
    except KeyError:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    aligned_minute = (ts.minute // minutes) * minutes

    return ts.replace(
        minute=aligned_minute,
        second=0,
        microsecond=0,
    )


# def align_to_timeframe(ts: datetime, timeframe: str) -> datetime:
#     ts = ts.astimezone(IST)

#     if timeframe == "15M":
#         minute = (ts.minute // 15) * 15
#         return ts.replace(minute=minute, second=0, microsecond=0)
    
#     if timeframe == "10M":
#         minute = (ts.minute // 10) * 10
#         return ts.replace(minute=minute, second=0, microsecond=0)

#     if timeframe == "5M":
#         minute = (ts.minute // 5) * 5
#         return ts.replace(minute=minute, second=0, microsecond=0)

#     if timeframe == "1M":
#         return ts.replace(second=0, microsecond=0)

#     # Daily
#     return ts.replace(hour=0, minute=0, second=0, microsecond=0)


# ─────────────────────────────────────────────
# Incremental start resolution
# ─────────────────────────────────────────────

def resolve_start_ts(conn, symbol: str, timeframe: str) -> datetime:
    last_ts = get_last_candle_ts(conn, symbol, timeframe)
    now = datetime.now(IST)

    if last_ts:
        return last_ts + _timeframe_delta(timeframe)

    if timeframe == "1D":
        return now - timedelta(days=365 * 10)

    lookback_minutes = DEFAULT_INTRADAY_LOOKBACK_MINUTES[timeframe]
    return now - timedelta(minutes=lookback_minutes)


# ─────────────────────────────────────────────
# Incremental ingestion (scheduler jobs)
# ─────────────────────────────────────────────

def ingest_symbol(conn, symbol: str, timeframe: str):
    start = resolve_start_ts(conn, symbol, timeframe)
    now = datetime.now(IST)

    def get_safe_now(timeframe: str, now: datetime) -> datetime:
        if timeframe == "1D":
            return now - timedelta(days=3)
        minutes = TIMEFRAMES[timeframe]["minutes"]
        # 3 candle buffer, minimum 5 minutes
        buffer_minutes = max(minutes * 3, 5)
        return now - timedelta(minutes=buffer_minutes)

    safe_now = get_safe_now(timeframe, now)



    # # Safety lag
    # if timeframe == "15M":
    #     safe_now = now - timedelta(minutes=45)
    # elif timeframe == "10M":
    #     safe_now = now - timedelta(minutes=30)
    # elif timeframe == "5M":
    #     safe_now = now - timedelta(minutes=15)
    # elif timeframe == "1M":
    #     safe_now = now - timedelta(minutes=5)
    # elif timeframe == "1D":
    #     safe_now = now - timedelta(days=3)  # weekend/holiday safe
    # else:
    #     safe_now = now

    start = align_to_timeframe(start, timeframe)
    end = align_to_timeframe(safe_now, timeframe)

    if start >= end:
        logger.info(f"{symbol} | {timeframe} | no new candles")
        return

    df = fetch_candles(
        symbol=symbol,
        timeframe=timeframe,
        start=start,
        end=end,
    )

    if df.empty:
        logger.info(f"{symbol} | {timeframe} | fetch returned empty")
        return

    if timeframe != "1D":
        gaps = detect_gaps(df, timeframe)
        for gap_start, gap_end in gaps:
            logger.warning(
                f"GAP {symbol} {timeframe}: {gap_start} → {gap_end}"
            )
            gap_df = fetch_candles(
                symbol=symbol,
                timeframe=timeframe,
                start=gap_start,
                end=gap_end,
            )
            if not gap_df.empty:
                df = pd.concat([df, gap_df], ignore_index=True)

        df = df.drop_duplicates(subset=["ts"]).sort_values("ts")

    write_candles(conn, symbol, timeframe, df)

    logger.info(
        f"{symbol} | {timeframe} | inserted {len(df)} candles"
    )


def run_ingestion_job(job_name: str, symbols: list[str]):
    job = get_job_config(job_name)
    timeframe = job["timeframe"]
    run_type = job["run_type"]

    if run_type == "INTRADAY" and not is_market_open():
        logger.info(f"Market closed — skipping {job_name}")
        return

    conn = get_db_connection()
    try:
        for symbol in symbols:
            try:
                ingest_symbol(conn, symbol, timeframe)
            except Exception:
                logger.exception(f"FAILED {symbol} {timeframe}")
    finally:
        conn.close()


# ─────────────────────────────────────────────
# Pure backfill runner
# ─────────────────────────────────────────────

def run_backfill(
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
):
    logger.info(
        f"🚀 Backfill start | {symbol} | {timeframe} | {start} → {end}"
    )

    start = align_to_timeframe(start, timeframe)
    end = align_to_timeframe(end, timeframe)

    if start >= end:
        logger.warning("Backfill start >= end — nothing to do")
        return

    df = fetch_candles(
        symbol=symbol,
        timeframe=timeframe,
        start=start,
        end=end,
    )

    if df.empty:
        logger.warning(
            f"Backfill empty | {symbol} | {timeframe}"
        )
        return

    conn = get_db_connection()
    try:
        write_candles(conn, symbol, timeframe, df)
    finally:
        conn.close()

    logger.info(
        f"✅ Backfill complete | {symbol} | {timeframe} | {len(df)} candles"
    )


# ─────────────────────────────────────────────
# CLI + YAML entry point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import yaml

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Market data ingestion orchestrator"
    )

    parser.add_argument("--config", help="Path to YAML config file")
    parser.add_argument("--mode", choices=["job", "backfill"])

    # Job mode
    parser.add_argument("--job-name")
    parser.add_argument("--symbols")

    # Backfill mode
    parser.add_argument("--symbol")
    parser.add_argument("--tf")
    parser.add_argument("--start")
    parser.add_argument("--end")

    args = parser.parse_args()

    # Load YAML config
    config = {}
    if args.config:
        logger.info(f"📄 Loading config file: {args.config}")
        with open(args.config, "r") as f:
            config = yaml.safe_load(f) or {}

    def get_value(name):
        if hasattr(args, name):
            val = getattr(args, name)
            if val is not None:
                return val
        return config.get(name)

    def parse_date(value):
        """
        Accepts:
          - str (YYYY-MM-DD)
          - datetime.date
          - datetime.datetime
        Returns timezone-aware datetime (IST).
        """
        if isinstance(value, datetime):
            return value.astimezone(IST)
        if isinstance(value, date):
            return IST.localize(datetime(value.year, value.month, value.day))
        if isinstance(value, str):
            return IST.localize(datetime.strptime(value, "%Y-%m-%d"))
        raise ValueError(f"Unsupported date value: {value} ({type(value)})")

    mode = get_value("mode")

    # ─── JOB MODE ───
    if mode == "job":
        job_name = get_value("job_name")
        symbols_raw = get_value("symbols")

        if not job_name or not symbols_raw:
            parser.error("--job-name and --symbols required for job mode")

        symbols = [s.strip() for s in symbols_raw.split(",")]

        run_ingestion_job(
            job_name=job_name,
            symbols=symbols,
        )

    # ─── BACKFILL MODE ───
    elif mode == "backfill":
        symbol = get_value("symbol")
        timeframe = get_value("timeframe") or get_value("tf")
        start_raw = get_value("start")
        end_raw = get_value("end")

        missing = [
            k for k, v in {
                "symbol": symbol,
                "timeframe": timeframe,
                "start": start_raw,
                "end": end_raw,
            }.items() if v is None
        ]

        if missing:
            parser.error(
                f"Missing required args for backfill: {', '.join(missing)}"
            )

        start_dt = parse_date(start_raw)
        end_dt = parse_date(end_raw)

        run_backfill(
            symbol=symbol,
            timeframe=timeframe,
            start=start_dt,
            end=end_dt,
        )

    else:
        parser.error("mode must be specified (job or backfill)")
