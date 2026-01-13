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
# Timeframe metadata
# ─────────────────────────────────────────────

TIMEFRAME_MINUTES = {
    tf: meta["minutes"]
    for tf, meta in TIMEFRAMES.items()
}

# ─────────────────────────────────────────────
# Config normalizers
# ─────────────────────────────────────────────

def normalize_symbols(cfg) -> list[str]:
    if "symbols" in cfg:
        if not isinstance(cfg["symbols"], list):
            raise ValueError("'symbols' must be a list")
        return cfg["symbols"]

    if "symbol" in cfg:
        return [cfg["symbol"]]

    raise ValueError("Config must define 'symbol' or 'symbols'")


def normalize_timeframes(cfg) -> list[str]:
    if "timeframes" in cfg:
        if not isinstance(cfg["timeframes"], list):
            raise ValueError("'timeframes' must be a list")
        return cfg["timeframes"]

    if "timeframe" in cfg:
        return [cfg["timeframe"]]

    raise ValueError("Config must define 'timeframe' or 'timeframes'")


def parse_date(value) -> date:
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

def expected_intraday_candles(
    trade_date: date,
    timeframe: str,
    market_open: time,
    market_close: time,
):
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


def fetch_actual_candles(
    conn,
    symbol: str,
    timeframe: str,
    start_ts: datetime,
    end_ts: datetime,
):
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

def run_daily_completeness_check(cfg, results) -> bool:
    symbols = normalize_symbols(cfg)
    timeframe = "1D"

    start_date = parse_date(cfg.get("start"))
    end_date = parse_date(cfg.get("end"))

    holiday_agent = MarketHolidayAgent()
    conn = get_db_connection()

    success = True

    for symbol in symbols:
        missing_days = 0
        current = start_date

        while current <= end_date:
            if not holiday_agent.is_trading_day(current):
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
                missing_days += 1

            current += timedelta(days=1)

        key = (symbol, timeframe)

        if missing_days > 0:
            results[key] = (False, f"missing {missing_days} days")
            logger.error(
                f"❌ DAILY QA FAILED | {symbol} | missing {missing_days} days"
            )
            success = False
        else:
            results[key] = (True, "OK")
            logger.info(f"🎉 DAILY QA PASSED | {symbol}")

    conn.close()
    return success

# ─────────────────────────────────────────────
# INTRADAY QA (multi-symbol × multi-timeframe)
# ─────────────────────────────────────────────

def run_intraday_completeness_check(config_path: str) -> bool:
    logger.info(f"📄 Loading QA config: {config_path}")

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f) or {}

    symbols = normalize_symbols(cfg)
    timeframes = normalize_timeframes(cfg)

    start_date = parse_date(cfg.get("start"))
    end_date = parse_date(cfg.get("end"))

    market_open = time.fromisoformat(cfg["market"]["open"])
    market_close = time.fromisoformat(cfg["market"]["close"])

    checks = cfg.get("checks", {})
    allow_missing_days = checks.get("allow_missing_days", False)

    holiday_agent = MarketHolidayAgent()
    conn = get_db_connection()

    results = {}  # (symbol, timeframe) -> (pass, details)
    overall_success = True

    for timeframe in timeframes:

        if timeframe == "1D":
            if not run_daily_completeness_check(cfg, results):
                overall_success = False
            continue

        for symbol in symbols:
            logger.info(
                f"🔍 QA START | {symbol} | {timeframe} | "
                f"{start_date} → {end_date}"
            )

            total_missing = 0
            current = start_date

            while current <= end_date:

                if not holiday_agent.is_trading_day(current):
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

                missing = set(expected) - set(actual)

                if missing:
                    if allow_missing_days and len(missing) == len(expected):
                        logger.warning(
                            f"⏭️ {symbol} | {current} — "
                            "full-day missing (treated as holiday)"
                        )
                    else:
                        total_missing += len(missing)

                current += timedelta(days=1)

            key = (symbol, timeframe)

            if total_missing > 0:
                logger.error(
                    f"❌ QA FAILED | {symbol} | {timeframe} | "
                    f"missing {total_missing} candles"
                )
                results[key] = (False, f"missing {total_missing} candles")
                overall_success = False
            else:
                logger.info(
                    f"🎉 QA PASSED | {symbol} | {timeframe}"
                )
                results[key] = (True, "OK")

    conn.close()

    _print_combined_summary(results, overall_success)
    return overall_success

# ─────────────────────────────────────────────
# COMBINED QA SUMMARY
# ─────────────────────────────────────────────

def _print_combined_summary(results, overall_success: bool):
    logger.info("📊 QA SUMMARY (symbol × timeframe)")
    logger.info("-" * 55)

    for (symbol, timeframe), (passed, detail) in sorted(results.items()):
        status = "✅ PASS" if passed else "❌ FAIL"
        logger.info(
            f"{symbol:<12} | {timeframe:<5} | {status:<6} | {detail}"
        )

    logger.info("-" * 55)
    final = "✅ PASS" if overall_success else "❌ FAIL"
    logger.info(f"OVERALL RESULT: {final}")
