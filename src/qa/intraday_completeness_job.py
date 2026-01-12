import logging
import yaml
from datetime import datetime, timedelta, time, date
import pytz

from data_ingestion.db import get_db_connection
from agents.calendar.market_holiday_agent import MarketHolidayAgent
from data_ingestion.timeframe_mapper import TIMEFRAMES

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

# ─────────────────────────────────────────────
# Timeframe helpers
# ─────────────────────────────────────────────

TIMEFRAME_MINUTES = {
    tf: meta["minutes"]
    for tf, meta in TIMEFRAMES.items()
}

# ─────────────────────────────────────────────
# Config helpers
# ─────────────────────────────────────────────

def normalize_symbols(cfg) -> list[str]:
    """
    Normalize symbol(s) from config.

    Supports:
      - symbol: "TCS"
      - symbols: ["TCS", "INFY"]
    """
    if "symbols" in cfg:
        symbols = cfg["symbols"]
        if not isinstance(symbols, list):
            raise ValueError("'symbols' must be a list")
        return symbols

    if "symbol" in cfg:
        return [cfg["symbol"]]

    raise ValueError(
        "Config must define either 'symbol' or 'symbols'"
    )


def parse_date(value):
    """
    Parse date from YAML (str | date | datetime)
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return datetime.strptime(value, "%Y-%m-%d").date()
    raise ValueError(f"Invalid date value: {value}")

# ─────────────────────────────────────────────
# Candle helpers
# ─────────────────────────────────────────────

def expected_intraday_candles(trade_date, timeframe, market_open, market_close):
    """
    Generate expected intraday candle timestamps for one trading day.
    """
    if timeframe == "1D":
        raise ValueError("1D timeframe is not intraday")

    step = timedelta(minutes=TIMEFRAME_MINUTES[timeframe])

    start = IST.localize(datetime.combine(trade_date, market_open))
    end = IST.localize(datetime.combine(trade_date, market_close))

    candles = []
    current = start
    while current < end:
        candles.append(current)
        current += step

    return candles


def fetch_actual_candles(conn, symbol, timeframe, start_ts, end_ts):
    """
    Fetch actual candles from DB.
    """
    query = """
        SELECT ts
        FROM candles
        WHERE symbol = %s
          AND timeframe = %s
          AND ts >= %s
          AND ts < %s
        ORDER BY ts
    """

    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                symbol,
                timeframe,
                start_ts.astimezone(pytz.UTC),
                end_ts.astimezone(pytz.UTC),
            ),
        )
        rows = cur.fetchall()

    return [row["ts"].astimezone(IST) for row in rows]

# ─────────────────────────────────────────────
# DAILY QA (1D)
# ─────────────────────────────────────────────

def run_daily_completeness_check(cfg) -> bool:
    """
    Daily candle QA:
    - Exactly one candle per trading day
    - Timestamp aligned to 00:00 IST
    """

    symbols = normalize_symbols(cfg)
    timeframe = cfg["timeframe"]

    start_raw = cfg.get("start_date") or cfg.get("start")
    end_raw = cfg.get("end_date") or cfg.get("end")

    start_date = parse_date(start_raw)
    end_date = parse_date(end_raw)

    holiday_agent = MarketHolidayAgent()
    conn = get_db_connection()

    results = {}
    overall_success = True

    for symbol in symbols:
        logger.info(
            f"🔍 DAILY QA START | {symbol} | {start_date} → {end_date}"
        )

        current = start_date
        missing_days = 0

        while current <= end_date:

            if not holiday_agent.is_trading_day(current):
                logger.info(f"⏭️ {current} — holiday/weekend")
                current += timedelta(days=1)
                continue

            expected_ts = IST.localize(
                datetime.combine(current, time(0, 0))
            )

            actual = fetch_actual_candles(
                conn,
                symbol,
                timeframe,
                expected_ts,
                expected_ts + timedelta(days=1),
            )

            if not actual:
                logger.error(
                    f"❌ {symbol} | {current} — missing daily candle"
                )
                missing_days += 1
            else:
                logger.info(
                    f"✅ {symbol} | {current} — OK"
                )

            current += timedelta(days=1)

        if missing_days > 0:
            logger.error(
                f"❌ DAILY QA FAILED | {symbol} | "
                f"missing days = {missing_days}"
            )
            results[symbol] = (False, f"missing {missing_days} days")
            overall_success = False
        else:
            logger.info(
                f"🎉 DAILY QA PASSED | {symbol}"
            )
            results[symbol] = (True, "OK")

    conn.close()

    _print_qa_summary(results, overall_success, timeframe="1D")
    return overall_success

# ─────────────────────────────────────────────
# INTRADAY QA
# ─────────────────────────────────────────────

def run_intraday_completeness_check(config_path: str) -> bool:
    """
    Automated QA job:
    - Validates intraday candle completeness
    - Supports multi-symbol configs
    - Routes 1D to daily QA
    """

    logger.info(f"📄 Loading QA config: {config_path}")

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f) or {}

    symbols = normalize_symbols(cfg)
    timeframe = cfg["timeframe"]

    # 🔀 Route DAILY timeframe
    if timeframe == "1D":
        logger.info("⏭️ Detected 1D timeframe — running daily QA")
        return run_daily_completeness_check(cfg)

    checks = cfg.get("checks", {})
    if not checks.get("intraday_completeness", True):
        logger.info("⏭️ Intraday completeness check disabled via config")
        return True

    start_raw = cfg.get("start_date") or cfg.get("start")
    end_raw = cfg.get("end_date") or cfg.get("end")

    if not start_raw or not end_raw:
        raise ValueError(
            "Config must define start/end or start_date/end_date"
        )

    start_date = parse_date(start_raw)
    end_date = parse_date(end_raw)

    market_open = time.fromisoformat(cfg["market"]["open"])
    market_close = time.fromisoformat(cfg["market"]["close"])

    holiday_agent = MarketHolidayAgent()
    conn = get_db_connection()

    results = {}
    overall_success = True
    allow_missing_days = checks.get("allow_missing_days", False)

    for symbol in symbols:
        logger.info(
            f"🔍 QA START | {symbol} | {timeframe} | "
            f"{start_date} → {end_date}"
        )

        total_missing = 0
        current = start_date

        while current <= end_date:

            if not holiday_agent.is_trading_day(current):
                logger.info(f"⏭️ {current} — holiday/weekend")
                current += timedelta(days=1)
                continue

            expected = expected_intraday_candles(
                current, timeframe, market_open, market_close
            )

            actual = fetch_actual_candles(
                conn,
                symbol,
                timeframe,
                expected[0],
                IST.localize(
                    datetime.combine(current, market_close)
                ),
            )

            missing = sorted(set(expected) - set(actual))

            if missing:
                if allow_missing_days and len(missing) == len(expected):
                    logger.warning(
                        f"⏭️ {symbol} | {current} — "
                        "full-day missing (treated as holiday)"
                    )
                else:
                    logger.error(
                        f"❌ {symbol} | {current} — "
                        f"missing {len(missing)} candles"
                    )
                    for ts in missing[:3]:
                        logger.error(f"   ⛔ {ts}")
                    total_missing += len(missing)
            else:
                logger.info(
                    f"✅ {symbol} | {current} — "
                    f"OK ({len(expected)} candles)"
                )

            current += timedelta(days=1)

        if total_missing > 0:
            logger.error(
                f"❌ QA FAILED | {symbol} | "
                f"total missing candles = {total_missing}"
            )
            results[symbol] = (False, f"missing {total_missing} candles")
            overall_success = False
        else:
            logger.info(
                f"🎉 QA PASSED | {symbol}"
            )
            results[symbol] = (True, "OK")

    conn.close()

    _print_qa_summary(results, overall_success, timeframe)
    return overall_success

# ─────────────────────────────────────────────
# QA SUMMARY PRINTER
# ─────────────────────────────────────────────

def _print_qa_summary(results: dict, overall_success: bool, timeframe: str):
    logger.info(f"📊 QA SUMMARY ({timeframe})")
    logger.info("-" * 40)

    for symbol, (passed, detail) in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(f"{symbol:<12} : {status} ({detail})")

    logger.info("-" * 40)
    final_status = "✅ PASS" if overall_success else "❌ FAIL"
    logger.info(f"OVERALL RESULT: {final_status}")
