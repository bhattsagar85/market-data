from datetime import datetime, time
import pytz
import logging

from data_ingestion.fetcher import fetch_candles
from data_ingestion.writer import write_candles
from data_ingestion.fetcher import get_api
from data_ingestion.db import get_db_connection

from agents.data_quality.data_completeness_agent import DataCompletenessAgent

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

INTRADAY_TIMEFRAMES = ["15M", "5M", "1M"]


def trading_day_window(now_ist: datetime):
    """
    Returns (start, end) datetime for today's trading session.
    """
    start = now_ist.replace(
        hour=9, minute=15, second=0, microsecond=0
    )
    end = now_ist.replace(
        hour=15, minute=30, second=0, microsecond=0
    )
    return start, end


def run_eod_reconciliation(symbols: list[str]):
    """
    End-of-day reconciliation for intraday candles.
    Also triggers data governance & auto-backfill AFTER reconciliation.
    """

    now = datetime.now(IST)

    # Safety guard — never run before market close
    if now.time() < time(16, 0):
        logger.info("EOD reconciliation skipped — market not fully closed")
        return

    start, end = trading_day_window(now)

    logger.info(
        f"EOD RECON START | window {start} → {end}"
    )

    api = get_api()
    conn = get_db_connection()

    try:
        # ─────────────────────────────────────────
        # 1️⃣ INTRADAY RECONCILIATION
        # ─────────────────────────────────────────
        for symbol in symbols:
            for timeframe in INTRADAY_TIMEFRAMES:
                try:
                    df = fetch_candles(
                        api=api,
                        symbol=symbol,
                        timeframe=timeframe,
                        start=start,
                        end=end,
                    )

                    if df.empty:
                        continue

                    write_candles(conn, symbol, timeframe, df)

                    logger.info(
                        f"EOD FIX | {symbol} | {timeframe} | fetched {len(df)} candles"
                    )

                except Exception:
                    logger.exception(
                        f"EOD FIX FAILED | {symbol} | {timeframe}"
                    )

    finally:
        conn.close()

    logger.info("EOD RECON END")

    # ─────────────────────────────────────────
    # 2️⃣ DATA GOVERNANCE (AFTER RECON)
    # ─────────────────────────────────────────
    logger.info("DATA GOVERNANCE START")

    gov_conn = get_db_connection()
    try:
        DataCompletenessAgent().run(gov_conn)
        logger.info("DATA GOVERNANCE END")
    except Exception:
        logger.exception("DATA GOVERNANCE FAILED")
    finally:
        gov_conn.close()
