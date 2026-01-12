import logging
import yaml
from datetime import datetime, timedelta, time, date
import pytz

from data_ingestion.db import get_db_connection
from agents.calendar.market_holiday_agent import MarketHolidayAgent
from data_ingestion.timeframe_mapper import TIMEFRAMES

logger = logging.getLogger(__name__)
IST = pytz.timezone("Asia/Kolkata")

TIMEFRAME_MINUTES = {
    tf: meta["minutes"]
    for tf, meta in TIMEFRAMES.items()
}

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

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


def expected_intraday_candles(trade_date, timeframe, market_open, market_close):
    """
    Generate expected candle timestamps for one trading day.
    """
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
# QA Job
# ─────────────────────────────────────────────

def run_intraday_completeness_check(config_path: str) -> bool:
    """
    Automated QA job:
    - Sync holidays for required years
    - Validate intraday candle completeness
    """

    logger.info(f"📄 Loading QA config: {config_path}")

    # ─── Load config ───
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f) or {}

    # ─── Respect checks flag ───
    checks = cfg.get("checks", {})
    if not checks.get("intraday_completeness", True):
        logger.info("⏭️ Intraday completeness check disabled via config")
        return True

    symbol = cfg["symbol"]
    timeframe = cfg["timeframe"]

    # ─── Extract date range (backfill-aware) ───
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

    logger.info(
        f"🔍 QA START | {symbol} | {timeframe} | {start_date} → {end_date}"
    )

    # ─── Holiday sync (range-based, NOT current year) ───
    holiday_agent = MarketHolidayAgent()

    conn = get_db_connection()
    total_missing = 0
    current = start_date

    allow_missing_days = checks.get("allow_missing_days", False)

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
            IST.localize(datetime.combine(current, market_close)),
        )

        missing = sorted(set(expected) - set(actual))

        if missing:
            # Full-day missing (likely holiday / exchange closed)
            if allow_missing_days and len(missing) == len(expected):
                logger.warning(
                    f"⏭️ {current} — full-day missing (treated as holiday)"
                )
            else:
                logger.error(
                    f"❌ {current} — missing {len(missing)} candles"
                )
                for ts in missing[:3]:
                    logger.error(f"   ⛔ {ts}")
                total_missing += len(missing)
        else:
            logger.info(
                f"✅ {current} — OK ({len(expected)} candles)"
            )

        current += timedelta(days=1)

    conn.close()

    if total_missing > 0:
        logger.error(
            f"❌ QA FAILED | total missing candles = {total_missing}"
        )
        return False

    logger.info("🎉 QA PASSED — all intraday candles present")
    return True
