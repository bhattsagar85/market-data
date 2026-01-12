from datetime import datetime
import pytz
import logging

from data_ingestion.fetcher import fetch_candles
from data_ingestion.db import get_db_connection
from data_ingestion.writer import write_candles

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

# CONFIG
SYMBOL = "TCS"
TIMEFRAME = "1D"   # must match orchestrator conventions

def run_backfill():
    logger.info(f"🚀 Starting Kite Backfill | {SYMBOL} | {TIMEFRAME}")

    # ─── DEFINE YOUR RANGE HERE ───
    start_dt = IST.localize(datetime(2024, 12, 1))
    end_dt   = IST.localize(datetime(2024, 12, 31))

    logger.info(f"📅 Start: {start_dt}")
    logger.info(f"📅 End:   {end_dt}")

    df = fetch_candles(
        symbol=SYMBOL,
        timeframe=TIMEFRAME,
        start=start_dt,
        end=end_dt,
    )

    if df.empty:
        logger.warning("❌ No data returned for this range")
        return

    conn = get_db_connection()
    write_candles(conn, SYMBOL, TIMEFRAME, df)
    conn.close()

    logger.info(f"✅ Inserted {len(df)} candles")

if __name__ == "__main__":
    run_backfill()