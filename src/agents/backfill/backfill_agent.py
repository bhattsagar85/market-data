from datetime import date, datetime, timedelta, time
from typing import List

from agents.calendar.market_holiday_agent import MarketHolidayAgent
from data_ingestion.orchestrator import ingest_symbol


class BackfillAgent:
    """
    Handles throttled auto-backfill for DAILY candles.
    """

    # 🔒 THROTTLE CONFIG (SAFE DEFAULT)
    MAX_DAYS_PER_RUN = 5

    def get_missing_trading_days(
        self,
        conn,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> List[date]:

        cur = conn.cursor()

        cur.execute("""
            SELECT DISTINCT ts::date AS d
            FROM candles
            WHERE symbol = %s
              AND timeframe = '1D'
        """, (symbol,))

        existing_days = {row["d"] for row in cur.fetchall()}

        missing_days = []
        d = start_date

        while d <= end_date:
            if MarketHolidayAgent.is_trading_day(d):
                if d not in existing_days:
                    missing_days.append(d)
            d += timedelta(days=1)

        return missing_days

    def backfill_daily(
        self,
        symbol: str,
        missing_days: List[date]
    ) -> List[date]:
        """
        Throttled DAILY backfill.
        Returns the days actually backfilled.
        """

        # 🧠 THROTTLING APPLIED HERE
        days_to_heal = missing_days[: self.MAX_DAYS_PER_RUN]

        for d in days_to_heal:
            start = datetime.combine(d, time(9, 15))
            end = datetime.combine(d, time(15, 30))

            ingest_symbol(
                symbol=symbol,
                timeframe="1D",
                start=start,
                end=end
            )

        return days_to_heal
