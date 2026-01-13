import logging
import os
from datetime import datetime, time as dtime

import pytz
from kiteconnect import KiteConnect

from auth.zerodha_auth import load_access_token
from data_ingestion.db import get_db_connection

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

INTERVAL_MINUTES = 5
UNDERLYINGS = ("NIFTY", "BANKNIFTY")

IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.UTC

MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def round_to_5m(ts: datetime) -> datetime:
    minute = (ts.minute // 5) * 5
    return ts.replace(minute=minute, second=0, microsecond=0)


def is_market_open(now_ist: datetime) -> bool:
    return MARKET_OPEN <= now_ist.time() <= MARKET_CLOSE


def get_nearest_expiry(conn, underlying):
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


def get_all_strikes(conn, underlying, expiry):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
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


def get_spot_price(kite, underlying):
    if underlying == "NIFTY":
        symbol = "NSE:NIFTY 50"
    elif underlying == "BANKNIFTY":
        symbol = "NSE:NIFTY BANK"
    else:
        raise ValueError(underlying)

    return kite.ltp([symbol])[symbol]["last_price"]


def insert_intraday_chain(conn, rows):
    if not rows:
        return

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO option_chain_intraday (
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

def capture_intraday_option_chain():
    kite = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
    kite.set_access_token(load_access_token())

    conn = get_db_connection()

    now_ist = datetime.now(IST)
    if not is_market_open(now_ist):
        logger.info("⏭️ Market closed — skipping intraday chain")
        return

    ts = round_to_5m(now_ist).astimezone(UTC)

    for underlying in UNDERLYINGS:
        expiry = get_nearest_expiry(conn, underlying)
        if not expiry:
            continue

        instruments = get_all_strikes(conn, underlying, expiry)
        if not instruments:
            continue

        symbols = [
            f"NFO:{inst['tradingsymbol']}"
            for inst in instruments
        ]

        logger.info(
            f"🕔 Capturing 5M chain | {underlying} | {expiry} | "
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

        insert_intraday_chain(conn, rows)

        logger.info(
            f"✅ 5M chain saved | {underlying} | {expiry} | rows={len(rows)}"
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
    capture_intraday_option_chain()
