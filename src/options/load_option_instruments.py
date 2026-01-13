import logging
import os
from kiteconnect import KiteConnect
from datetime import date
from psycopg2.extras import execute_batch

from auth.zerodha_auth import load_access_token
from data_ingestion.db import get_db_connection

logger = logging.getLogger(__name__)

SUPPORTED_UNDERLYINGS = {
    "NIFTY": "NIFTY",
    "BANKNIFTY": "BANKNIFTY",
}


def load_option_instruments():
    kite = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
    kite.set_access_token(load_access_token())

    logger.info("📥 Fetching NFO instruments from Zerodha")
    instruments = kite.instruments("NFO")

    rows = []

    for inst in instruments:
        # Filter only index options
        if inst.get("instrument_type") not in ("CE", "PE"):
            continue

        name = inst.get("name")
        if name not in SUPPORTED_UNDERLYINGS:
            continue

        rows.append(
            (
                inst["instrument_token"],
                name,
                inst["expiry"],
                inst["strike"],
                inst["instrument_type"],
                inst["exchange"],
                inst["tradingsymbol"],
            )
        )

    logger.info(f"✅ Found {len(rows)} option instruments")

    if not rows:
        logger.warning("No option instruments found")
        return

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
          cur.executemany(
            """
            INSERT INTO option_instruments (
                instrument_token,
                underlying,
                expiry,
                strike,
                option_type,
                exchange,
                tradingsymbol
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (instrument_token) DO NOTHING
            """,
            rows,
          )
        conn.commit()
        logger.info("🎉 Option instruments loaded into DB")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    load_option_instruments()
