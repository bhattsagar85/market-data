from datetime import datetime, timedelta, date, time
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

MAX_BACKFILL_DAYS = 90

# Default backfill windows (minutes) for first-ever intraday runs
DEFAULT_INTRADAY_LOOKBACK_MINUTES = {
    "15M": 7 * 24 * 60,
    "5M": 3 * 24 * 60,
    "1M": 24 * 60,
}

# ─────────────────────────────────────────────
# Timeframe helpers
# ─────────────────────────────────────────────

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
        buffer_minutes = max(minutes * 3, 5)
        return now - timedelta(minutes=buffer_minutes)

    safe_now = get_safe_now(timeframe, now)

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
# Backfill logic (single unit)
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

    conn = get_db_connection()

    try:
        last_ts = get_last_candle_ts(conn, symbol, timeframe)

        if last_ts and start <= last_ts < end:
            chunk_start = last_ts + _timeframe_delta(timeframe)
            logger.info(
                f"🔄 Resuming backfill from last candle | {chunk_start}"
            )
        else:
            chunk_start = start

        while chunk_start < end:
            chunk_end = min(
                chunk_start + timedelta(days=MAX_BACKFILL_DAYS),
                end,
            )

            logger.info(
                f"📥 Fetching chunk | {symbol} | {timeframe} | "
                f"{chunk_start} → {chunk_end}"
            )

            df = fetch_candles(
                symbol=symbol,
                timeframe=timeframe,
                start=chunk_start,
                end=chunk_end,
            )

            if not df.empty:
                write_candles(conn, symbol, timeframe, df)
                logger.info(
                    f"✅ Inserted {len(df)} candles | "
                    f"{chunk_start} → {chunk_end}"
                )
            else:
                logger.warning(
                    f"⚠️ Empty chunk | {chunk_start} → {chunk_end}"
                )

            chunk_start = chunk_end

    finally:
        conn.close()

    logger.info(
        f"🎉 Backfill completed | {symbol} | {timeframe}"
    )

# ─────────────────────────────────────────────
# 🔥 MULTI-SYMBOL DISPATCHER (NEW)
# ─────────────────────────────────────────────

def run_multi_symbol_backfill(cfg: dict):
    """
    Backward compatible:
    - supports symbol OR symbols[]
    """

    symbols = cfg.get("symbols")

    if symbols is None:
        symbols = [cfg["symbol"]]

    elif isinstance(symbols, list):
    # flatten in case of accidental nesting
        if len(symbols) == 1 and isinstance(symbols[0], list):
            symbols = symbols[0]
            
    elif isinstance(symbols, str):
        symbols = [symbols]

    timeframe = cfg["timeframe"]
    start_raw = cfg.get("start")
    end_raw = cfg.get("end")

    if not start_raw or not end_raw:
        raise ValueError("Backfill requires start and end dates")

    def parse_date_local(value):
        if isinstance(value, datetime):
            return value.astimezone(IST)
        if isinstance(value, date):
            return IST.localize(datetime(value.year, value.month, value.day))
        if isinstance(value, str):
            return IST.localize(datetime.strptime(value, "%Y-%m-%d"))
        raise ValueError(f"Invalid date: {value}")

    start_dt = parse_date_local(start_raw)
    end_dt = parse_date_local(end_raw)

    logger.info(
        f"🚀 MULTI-SYMBOL BACKFILL | symbols={symbols} | "
        f"timeframe={timeframe} | {start_dt.date()} → {end_dt.date()}"
    )

    for symbol in symbols:
        try:
            run_backfill(
                symbol=symbol,
                timeframe=timeframe,
                start=start_dt,
                end=end_dt,
            )
        except Exception:
            logger.exception(
                f"❌ Backfill failed | {symbol} | {timeframe}"
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

    args = parser.parse_args()

    config = {}
    if args.config:
        logger.info(f"📄 Loading config file: {args.config}")
        with open(args.config, "r") as f:
            config = yaml.safe_load(f) or {}

    mode = config.get("mode") or args.mode

    if mode == "job":
        job_name = config.get("job_name")
        symbols_raw = config.get("symbols")

        if not job_name or not symbols_raw:
            parser.error("--job-name and --symbols required for job mode")

        if isinstance(symbols_raw, str):
            symbols = [s.strip() for s in symbols_raw.split(",")]
        else:
            symbols = symbols_raw

        run_ingestion_job(job_name=job_name, symbols=symbols)

    elif mode == "backfill":
        run_multi_symbol_backfill(config)

    else:
        parser.error("mode must be specified (job or backfill)")
