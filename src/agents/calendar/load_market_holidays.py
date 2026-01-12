import csv
from datetime import datetime
from data_ingestion.db import get_db_connection

CSV_PATH = "data/HolidaycalenderData.csv"   # <-- adjust if needed


def clean(value):
    """Safely clean CSV cell values."""
    if value is None:
        return ""
    return value.strip().strip('"')


def load_holidays():
    conn = get_db_connection()
    inserted = 0
    skipped = 0

    with open(CSV_PATH, newline="", encoding="utf-8") as f, conn.cursor() as cur:
        reader = csv.reader(f)

        headers = next(reader)
        headers = [clean(h) for h in headers]

        # Build index map
        try:
            idx_date = headers.index("Date")
            idx_occasion = headers.index("Occasion")
        except ValueError as e:
            raise RuntimeError(
                f"CSV headers not found. Parsed headers: {headers}"
            ) from e

        for row in reader:
            # Skip empty / malformed rows
            if not row or len(row) <= max(idx_date, idx_occasion):
                skipped += 1
                continue

            raw_date = clean(row[idx_date])
            occasion = clean(row[idx_occasion])

            # Skip footer or junk lines
            if not raw_date or raw_date.startswith("*"):
                skipped += 1
                continue

            try:
                holiday_date = datetime.strptime(
                    raw_date, "%d/%m/%Y"
                ).date()
            except ValueError:
                skipped += 1
                continue

            cur.execute(
                """
                INSERT INTO market_holidays (
                    exchange,
                    holiday_date,
                    year,
                    description
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    "NSE",
                    holiday_date,
                    holiday_date.year,
                    occasion,
                ),
            )

            inserted += cur.rowcount

    conn.commit()
    conn.close()

    print(f"✅ Holidays inserted: {inserted}")
    print(f"⏭️ Rows skipped: {skipped}")


if __name__ == "__main__":
    load_holidays()
