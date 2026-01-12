from datetime import datetime
import pytz

IST = pytz.timezone("Asia/Kolkata")

def normalize_shoonya_candles(
    raw_candles,
    symbol: str,
    timeframe: str,
    base_date=None,
):
    """
    Normalize Shoonya OHLCV candles into DB-ready format.

    Handles:
    - 'DD-MM-YYYY HH:MM:SS'
    - 'HH:MM:SS' (requires base_date)
    """

    records = []

    for c in raw_candles:
        try:
            time_str = c.get("time")
            if not time_str:
                continue

            # ─────────────────────────────
            # Timestamp parsing
            # ─────────────────────────────
            if "-" in time_str:
                ts = datetime.strptime(
                    time_str, "%d-%m-%Y %H:%M:%S"
                )
            else:
                if base_date is None:
                    raise ValueError("base_date required for intraday candle")
                t = datetime.strptime(time_str, "%H:%M:%S").time()
                ts = datetime.combine(base_date, t)

            ts = IST.localize(ts)

            records.append({
                "symbol": symbol,
                "timeframe": timeframe,
                "candle_ts": ts,
                "open": float(c["into"]),
                "high": float(c["inth"]),
                "low": float(c["intl"]),
                "close": float(c["intc"]),
                "volume": int(c.get("intv", 0)),
            })

        except Exception as e:
            print("⚠️ Skipping bad candle:", c, "| error:", e)

    return records
