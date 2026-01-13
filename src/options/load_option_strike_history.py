import time
import logging
import os
from datetime import datetime, timedelta
import pytz

from kiteconnect import KiteConnect

from auth.zerodha_auth import load_access_token
from data_ingestion.db import get_db_connection

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

INTERVAL = "day"          # start safe with 1D
SLEEP_SEC = 0.4           # Zerodha rate-safe
MAX_STRIKES = 10          # ATM ± 10
UNDERLYINGS = ("NIFTY", "BANKNIFTY")


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def interval_delta():
    """Return timedelta for the configured interval."""
    return timedelta(days=1)


def get_nearest_expiry(conn, underlying: str):
    """
    Returns nearest (earliest) non-expired expiry
    for the given underlying.
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
        if not row:
            return None
        return row["expiry"]


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


def get_atm_strikes(conn, underlying, expiry, spot):
    """
    Returns ATM ± MAX_STRIKES option instruments.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                instrument_token,
                underlying,
                expiry,
                strike,
                option_type,
                tradingsymbol,
                created_at
            FROM option_instruments
            WHERE underlying = %s
              AND expiry = %s
            ORDER BY ABS(strike - %s)
            LIMIT %s
            """,
            (underlying, expiry, spot, MAX_STRIKES * 2),
        )
        return cur.fetchall()


def get_last_strike_ts(conn, instrument_token):
    """
    Returns last stored candle timestamp for a strike.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(ts) AS ts
            FROM option_strike_candles
            WHERE instrument_token = %s
            """,
            (instrument_token,),
        )
        row = cur.fetchone()
        if not row:
            return None
        return row["ts"]


def insert_strike_candles(conn, inst, candles):
    """
    Insert historical candles for one option strike.
    Idempotent via PK (instrument_token, ts).
    """
    if not candles:
        return

    rows = []
    for c in candles:
        rows.append(
            (
                inst["instrument_token"],
                inst["underlying"],
                inst["expiry"],
                inst["strike"],
                inst["option_type"],
                c["date"],
                c["open"],
                c["high"],
                c["low"],
                c["close"],
                c.get("volume"),
                c.get("oi"),
            )
        )

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO option_strike_candles (
                instrument_token,
                underlying,
                expiry,
                strike,
                option_type,
                ts,
                open,
                high,
                low,
                close,
                volume,
                oi
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (instrument_token, ts)
            DO NOTHING
            """,
            rows,
        )

    conn.commit()


# ─────────────────────────────────────────────
# MAIN LOADER (RESUME-SAFE)
# ─────────────────────────────────────────────

def load_strike_history():
    kite = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
    kite.set_access_token(load_access_token())

    conn = get_db_connection()
    now = datetime.now(pytz.UTC)

    for underlying in UNDERLYINGS:
        expiry = get_nearest_expiry(conn, underlying)
        if not expiry:
            logger.warning(f"No active expiry for {underlying}")
            continue

        spot = get_spot_price(kite, underlying)
        instruments = get_atm_strikes(conn, underlying, expiry, spot)

        logger.info(
            f"📥 {underlying} | expiry={expiry} | strikes={len(instruments)}"
        )

        for inst in instruments:
            last_ts = get_last_strike_ts(
                conn, inst["instrument_token"]
            )

            if last_ts:
                from_date = last_ts + interval_delta()
            else:
                from_date = inst["created_at"].date()

            if isinstance(from_date, datetime):
                pass
            else:
            # date → datetime at start of day UTC
              from_date = datetime.combine(from_date, datetime.min.time(), tzinfo=pytz.UTC)    

            if from_date >= now:
                logger.info(
                    f"⏭️ Up-to-date | {inst['tradingsymbol']}"
                )
                continue

            logger.info(
                f"📥 Strike history | {inst['tradingsymbol']} | "
                f"from {from_date.date()}"
            )

            try:
                candles = kite.historical_data(
                    instrument_token=inst["instrument_token"],
                    from_date=from_date,
                    to_date=now,
                    interval=INTERVAL,
                    oi=True,
                )
            except Exception:
                logger.exception(
                    f"Failed fetching history for {inst['tradingsymbol']}"
                )
                continue

            insert_strike_candles(conn, inst, candles)
            time.sleep(SLEEP_SEC)

    conn.close()


# ─────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    load_strike_history()
