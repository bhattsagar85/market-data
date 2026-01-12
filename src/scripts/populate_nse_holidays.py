import requests
import logging
from datetime import datetime

from data_ingestion.db import get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NSE_BASE_URL = "https://www.nseindia.com"
NSE_HOLIDAY_API = "https://www.nseindia.com/api/holiday-master?type=trading"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}


def fetch_nse_holidays():
    """
    Fetch NSE trading holidays using a warmed session.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    # Step 1: Warm up cookies
    session.get(NSE_BASE_URL, timeout=10)

    # Step 2: Call holiday API
    resp = session.get(NSE_HOLIDAY_API, timeout=10)
    resp.raise_for_status()

    data = resp.json()

    holidays = []
    for h in data.get("CBM", []):
        holiday_date = datetime.strptime(
            h["tradingDate"], "%d-%b-%Y"
        ).date()

        holidays.append({
            "date": holiday_date,
            "description": h.get("description", ""),
            "year": holiday_date.year,
        })

    return holidays


def cache_holidays(holidays):
    """
    Insert holidays into DB (idempotent).
    """
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            for h in holidays:
                cur.execute(
                    """
                    INSERT INTO market_holidays
                    (exchange, holiday_date, description, year)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        "NSE",
                        h["date"],
                        h["description"],
                        h["year"],
                    )
                )
        conn.commit()
        logger.info(f"Cached {len(holidays)} NSE holidays")
    finally:
        conn.close()


def main():
    logger.info("Fetching NSE holidays...")
    holidays = fetch_nse_holidays()

    if not holidays:
        raise RuntimeError("No holidays returned from NSE API")

    cache_holidays(holidays)
    logger.info("✅ NSE holidays populated successfully")


if __name__ == "__main__":
    main()
