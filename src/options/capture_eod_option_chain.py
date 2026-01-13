import logging
import os
from datetime import datetime

from kiteconnect import KiteConnect

from auth.zerodha_auth import load_access_token
from data_ingestion.db import get_db_connection

logger = logging.getLogger(__name__)

UNDERLYINGS = ("NIFTY", "BANKNIFTY")


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def get_nearest_expiry(conn, underlying: str):
    """
    Returns nearest (earliest) non-expired expiry.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MIN(expiry) AS expiry
            FROM option_instruments
            WHERE underlying = %s
              AND expiry >= CURRENT_DATE
            """,
            (underlying,),
        )
        row = cur.fetchone()
        return row["expiry"] if row else None


def get_all_strikes(conn, underlying: str, expiry):
    """
    Returns all option instruments for underlying + expiry.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                instrument_token,
                tradingsymbol,
                strike,
                option_type
            FROM option_instruments
            WHERE underlying = %s
              AND expiry = %s
            ORDER BY strike, option_type
            """,
            (underlying, expiry),
        )
        return cur.fetchall()


def get_spot_price(kite, underlying: str) -> float:
    """
    Fetch live index spot price.
    """
    if underlying == "NIFTY":
        symbol = "NSE:NIFTY 50"
    elif underlying == "BANKNIFTY":
        symbol = "NSE:NIFTY BANK"
    else:
        raise ValueError(f"Unsupported underlying: {underlying}")

    quote = kite.ltp([symbol])
    return quote[symbol]["last_price"]


def insert_option_chain(conn, rows):
    """
    Insert EOD option chain snapshot.
    Idempotent via primary key.
    """
    if not rows:
        return

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO option_chain_eod (
                ts,
                underlying,
                expiry,
                strike,
                option_type,
                ltp,
                oi,
                volume,
                spot_price
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ts, underlying, expiry, strike, option_type)
            DO NOTHING
            """,
            rows,
        )
    conn.commit()


# ─────────────────────────────────────────────
# MAIN JOB
# ─────────────────────────────────────────────

def capture_eod_chain():
    kite = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
    kite.set_access_token(load_access_token())

    conn = get_db_connection()

    ts = datetime.now()

    for underlying in UNDERLYINGS:
        expiry = get_nearest_expiry(conn, underlying)
        if not expiry:
            logger.warning(f"No active expiry for {underlying}")
            continue

        instruments = get_all_strikes(conn, underlying, expiry)
        if not instruments:
            logger.warning(f"No strikes found for {underlying} {expiry}")
            continue

        symbols = [
            f"NFO:{inst['tradingsymbol']}"
            for inst in instruments
        ]

        logger.info(
            f"📊 Capturing EOD chain | {underlying} | {expiry} | "
            f"contracts={len(symbols)}"
        )

        quotes = kite.ltp(symbols)
        spot = get_spot_price(kite, underlying)

        rows = []
        for inst in instruments:
            key = f"NFO:{inst['tradingsymbol']}"
            q = quotes.get(key)
            if not q:
                continue

            rows.append(
                (
                    ts,
                    underlying,
                    expiry,
                    inst["strike"],
                    inst["option_type"],
                    q["last_price"],
                    q.get("oi"),
                    q.get("volume"),
                    spot,
                )
            )

        insert_option_chain(conn, rows)

        logger.info(
            f"✅ EOD chain saved | {underlying} | {expiry} | "
            f"rows={len(rows)}"
        )

    conn.close()


# ─────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    capture_eod_chain()
