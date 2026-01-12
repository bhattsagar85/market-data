from data_ingestion.db import get_db_connection

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS market_holidays (
    exchange TEXT NOT NULL,
    holiday_date DATE NOT NULL,
    description TEXT,
    year INT NOT NULL,
    PRIMARY KEY (exchange, holiday_date)
);
"""

def main():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        print("✅ market_holidays table created successfully")
    except Exception as e:
        conn.rollback()
        print("❌ Failed to create market_holidays table")
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    main()
