# src/scripts/sanity_check_ohlcv.py

from datetime import datetime, timedelta, time
import pytz

from data_ingestion.db import get_db_connection
from data_ingestion.timeframe_mapper import TIMEFRAMES

IST = pytz.timezone("Asia/Kolkata")

# ─────────────────────────────────────────────
# CONFIG (EDIT WHEN RUNNING)
# ─────────────────────────────────────────────

SYMBOL = "INFY"
TIMEFRAME = "15m"  # "1m", "5m", "15m"
START_DATE = "2025-12-01"
END_DATE   = "2025-12-31"

# NSE trading hours
MARKET_OPEN  = time(9, 15)
MARKET_CLOSE = time(15, 30)

TIMEFRAME_MINUTES = {
    tf: meta["minutes"]
    for tf, meta in TIMEFRAMES.items()
}

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def generate_expected_candles(trade_date, timeframe):
    """
    Generate expected candle timestamps (IST) for a trading day.
    """
    step = timedelta(minutes=TIMEFRAME_MINUTES[timeframe])

    candles = []
    current = datetime.combine(trade_date, MARKET_OPEN)
    current = IST.localize(current)

    market_close = datetime.combine(trade_date, MARKET_CLOSE)
    market_close = IST.localize(market_close)

    while current < market_close:
        candles.append(current)
        current += step

    return candles


def fetch_actual_candles(conn, symbol, timeframe, start_dt, end_dt):
    """
    Fetch candle timestamps from DB (converted to IST).
    """
    query = """
        SELECT candle_ts
        FROM ohlcv
        WHERE symbol = %s
          AND timeframe = %s
          AND candle_ts BETWEEN %s AND %s
        ORDER BY candle_ts;
    """

    with conn.cursor() as cur:
        cur.execute(query, (symbol, timeframe, start_dt, end_dt))
        rows = cur.fetchall()

    return [row[0].astimezone(IST) for row in rows]


# ─────────────────────────────────────────────
# SANITY CHECK
# ─────────────────────────────────────────────

def run_sanity_check():
    start_date = datetime.strptime(START_DATE, "%Y-%m-%d").date()
    end_date   = datetime.strptime(END_DATE, "%Y-%m-%d").date()

    print("\n🔍 SANITY CHECK STARTED")
    print(f"Symbol    : {SYMBOL}")
    print(f"Timeframe : {TIMEFRAME}")
    print(f"Date range: {START_DATE} → {END_DATE}\n")

    conn = get_db_connection()

    overall_missing = []

    current = start_date
    while current <= end_date:
        expected = generate_expected_candles(current, TIMEFRAME)

        if not expected:
            current += timedelta(days=1)
            continue

        day_start = expected[0]
        day_end   = expected[-1] + timedelta(minutes=TIMEFRAME_MINUTES[TIMEFRAME])

        actual = fetch_actual_candles(
            conn,
            SYMBOL,
            TIMEFRAME,
            day_start.astimezone(pytz.UTC),
            day_end.astimezone(pytz.UTC),
        )

        expected_set = set(expected)
        actual_set   = set(actual)

        missing = sorted(expected_set - actual_set)

        if missing:
            print(f"❌ {current} — Missing {len(missing)} candles")
            for ts in missing[:5]:
                print(f"   ⛔ {ts}")
            if len(missing) > 5:
                print("   ...")
            overall_missing.extend(missing)
        else:
            print(f"✅ {current} — All candles present ({len(expected)})")

        current += timedelta(days=1)

    conn.close()

    print("\n📊 SANITY CHECK SUMMARY")
    if overall_missing:
        print(f"❌ FAILED — Total missing candles: {len(overall_missing)}")
    else:
        print("🎉 PASSED — No missing candles detected")

    print("🔍 SANITY CHECK COMPLETED\n")


if __name__ == "__main__":
    run_sanity_check()
