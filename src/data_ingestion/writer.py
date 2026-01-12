# src/data_ingestion/writer.py

import psycopg2.extras


def write_candles(conn, symbol, timeframe, df):
    if df.empty:
        return

    rows = [
        (
            symbol,
            timeframe,
            row.ts,
            row.open,
            row.high,
            row.low,
            row.close,
            int(row.volume),
        )
        for row in df.itertuples()
    ]

    query = """
    INSERT INTO candles
    (symbol, timeframe, ts, open, high, low, close, volume)
    VALUES %s
    ON CONFLICT DO NOTHING
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, query, rows)
        conn.commit()
