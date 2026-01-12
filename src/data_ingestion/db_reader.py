# src/data_ingestion/db_reader.py

def get_last_candle_ts(conn, symbol, timeframe):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT MAX(ts) AS last_ts
        FROM candles
        WHERE symbol=%s AND timeframe=%s
        """,
        (symbol, timeframe)
    )
    row = cur.fetchone()

    if not row:
        return None

    # Works for tuple cursor OR RealDictCursor
    if isinstance(row, dict):
        return row.get("last_ts")

    return row[0]
