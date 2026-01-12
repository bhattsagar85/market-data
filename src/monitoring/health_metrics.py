from datetime import datetime, timezone

from data_ingestion.db import get_db_connection


# Allowed lag (in minutes) per timeframe
ALLOWED_LAG_MINUTES = {
    "1m": 2,
    "5m": 7,
    "15m": 20,
    "1D": 1440  # daily
}


def get_last_candle(conn, symbol: str, timeframe: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(ts) AS last_ts
            FROM candles
            WHERE symbol = %s AND timeframe = %s
            """,
            (symbol, timeframe)
        )
        row = cur.fetchone()
        return row["last_ts"]



def check_health(symbols, timeframes):
    conn = get_db_connection()

    now = datetime.now(timezone.utc)

    print("\nSYMBOL | TF  | LAST_TS | LAG(min) | STATUS")
    print("-" * 55)

    try:
        for symbol in symbols:
            for tf in timeframes:
                last_ts = get_last_candle(conn, symbol, tf)

                if last_ts is None:
                    print(f"{symbol:6} | {tf:3} | NONE | N/A | ❌ NO DATA")
                    continue

                lag_min = int((now - last_ts).total_seconds() / 60)
                allowed = ALLOWED_LAG_MINUTES.get(tf, 60)

                status = "OK" if lag_min <= allowed else "WARN"

                print(
                    f"{symbol:6} | {tf:3} | {last_ts} | {lag_min:8} | {status}"
                )

    finally:
        conn.close()
