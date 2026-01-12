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
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return datetime.strptime(value, "%Y-%m-%d").date()
    raise ValueError(f"Invalid date: {value}")


def expected_intraday_candles(trade_date, timeframe, market_open, market_close):
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
    logger.info(f"📄 Loading QA config: {config_path}")

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    symbol = cfg["symbol"]
    timeframe = cfg["timeframe"]

    start_date = parse_date(cfg["start_date"])
    end_date = parse_date(cfg["end_date"])

    market_open = time.fromisoformat(cfg["market"]["open"])
    market_close = time.fromisoformat(cfg["market"]["close"])

    logger.info(
        f"🔍 QA START | {symbol} | {timeframe} | {start_date} → {end_date}"
    )

    holiday_agent = MarketHolidayAgent()
    conn = get_db_connection()

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
            IST.localize(datetime.combine(current, market_close)),
        )

        missing = sorted(set(expected) - set(actual))

        if missing:
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

    logger.info("🎉 QA PASSED — all candles present")
    return True
