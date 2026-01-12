import logging
from datetime import date
from data_ingestion.db import get_db_connection

logger = logging.getLogger(__name__)


class MarketHolidayAgent:
    """
    Deterministic NSE trading calendar.
    DB-backed. No external dependencies.
    """

    DEFAULT_EXCHANGE = "NSE"

    def __init__(self, exchange: str = DEFAULT_EXCHANGE):
        self.exchange = exchange

    def is_trading_day(self, d: date) -> bool:
        """
        True if market was open on given date.
        """

        # Weekend
        if d.weekday() >= 5:
            return False

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT 1
                    FROM market_holidays
                    WHERE exchange = %s
                      AND holiday_date = %s
                    """,
                    (self.exchange, d),
                )
                return cur.fetchone() is None
        finally:
            conn.close()

    def get_holidays_for_year(self, year: int) -> list[date]:
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT holiday_date
                    FROM market_holidays
                    WHERE exchange = %s
                      AND year = %s
                    ORDER BY holiday_date
                    """,
                    (self.exchange, year),
                )
                return [row["holiday_date"] for row in cur.fetchall()]
        finally:
            conn.close()
