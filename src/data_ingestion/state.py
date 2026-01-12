# src/data_ingestion/state.py

import pytz

IST = pytz.timezone("Asia/Kolkata")

def get_last_candle_ts(conn, symbol: str, timeframe: str):
    """
    Returns last candle timestamp in IST, or None if no data exists.
    Compatible with tuple or dict cursors.
    """
    query = """
        SELECT MAX(candle_ts) AS last_ts
        FROM ohlcv
        WHERE symbol = %s
          AND timeframe = %s;
    """

    with conn.cursor() as cur:
        cur.execute(query, (symbol, timeframe))
        row = cur.fetchone()

    if not row:
        return None

    # Handle both tuple-based and dict-based cursors
    last_ts = (
        row["last_ts"] if isinstance(row, dict)
        else row[0]
    )

    if not last_ts:
        return None

    return last_ts.astimezone(IST)
