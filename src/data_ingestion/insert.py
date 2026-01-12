import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

def insert_ohlcv_batch(conn, candles: List[Dict]):
    """
    Insert OHLCV candles into TimescaleDB.
    Safe for:
    - backfill
    - scheduler
    - retries
    """

    if not candles:
        return

    query = """
    INSERT INTO ohlcv (
        symbol,
        timeframe,
        candle_ts,
        open,
        high,
        low,
        close,
        volume
    )
    VALUES (
        %(symbol)s,
        %(timeframe)s,
        %(candle_ts)s,
        %(open)s,
        %(high)s,
        %(low)s,
        %(close)s,
        %(volume)s
    )
    ON CONFLICT (symbol, timeframe, candle_ts)
    DO NOTHING;
    """

    with conn.cursor() as cur:
        cur.executemany(query, candles)

    conn.commit()

    logger.info(f"Inserted {len(candles)} candles (deduplicated)")
