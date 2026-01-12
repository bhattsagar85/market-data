from datetime import datetime, timedelta, date
import time
import pytz
import logging

from data_ingestion.db import get_db_connection
from data_ingestion.insert import insert_ohlcv_batch
from agents.calendar.market_holiday_agent import MarketHolidayAgent
from data_ingestion.normalize import normalize_shoonya_candles
from data_ingestion.timeframe_mapper import TIMEFRAMES

logger = logging.getLogger(__name__)

IST = pytz.timezone("Asia/Kolkata")

TIMEFRAME_MINUTES = {
    tf: meta["minutes"]
    for tf, meta in TIMEFRAMES.items()
}

MAX_LOOKBACK_DAYS = 3
MAX_CANDLES_PER_RUN = 200
API_SLEEP_SECONDS = 1.5


class IntradayBackfillAgent:
    """
    Safely heals missing intraday candles.
    """

    def __init__(self, exchange: str = "NSE"):
        self.exchange = exchange
        self.holiday_agent = MarketHolidayAgent(exchange)
       

    # ─────────────────────────────────────────────
    # ENTRY POINT
    # ─────────────────────────────────────────────
    def backfill_missing_candles(
        self,
        symbol: str,
        timeframe: str,
        missing_candles: list[datetime],
    ) -> dict:
        """
        Attempts to backfill missing intraday candles.
        Returns summary dict.
        """

        if timeframe not in TIMEFRAME_MINUTES:
            return {"status": "SKIPPED", "reason": "unsupported timeframe"}

        if not missing_candles:
            return {"status": "NOOP"}

        # Filter only recent trading days
        recent = self._filter_recent_trading_days(missing_candles)

        if not recent:
            return {"status": "SKIPPED", "reason": "too_old"}

        # Hard safety cap
        recent = recent[:MAX_CANDLES_PER_RUN]

        logger.info(
            f"Intraday backfill | {symbol} {timeframe} | "
            f"{len(recent)} candles"
        )

        self.client.login()  # MFA once per run

        inserted = 0
        attempted = 0

        for candle_ts in recent:
            attempted += 1

            start = candle_ts
            end = candle_ts + timedelta(
                minutes=TIMEFRAME_MINUTES[timeframe]
            )

            raw = self.client.get_historical(
                symbol=symbol,
                timeframe=timeframe.lower(),
                start=start,
                end=end,
            )

            if raw:
                records = normalize_shoonya_candles(
                    raw,
                    symbol,
                    timeframe,
                    base_date=start.date(),
                )

                if records:
                    conn = get_db_connection()
                    insert_ohlcv_batch(conn, records)
                    conn.close()
                    inserted += len(records)

            time.sleep(API_SLEEP_SECONDS)

        return {
            "status": "COMPLETE" if inserted else "PARTIAL",
            "attempted": attempted,
            "inserted": inserted,
            "skipped": len(missing_candles) - len(recent),
        }

    # ─────────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────────
    def _filter_recent_trading_days(
        self, candles: list[datetime]
    ) -> list[datetime]:
        today = datetime.now(IST).date()
        cutoff = today - timedelta(days=MAX_LOOKBACK_DAYS)

        filtered = []
        for ts in candles:
            d = ts.date()
            if d < cutoff:
                continue
            if not self.holiday_agent.is_trading_day(d):
                continue
            filtered.append(ts)

        return sorted(filtered)
