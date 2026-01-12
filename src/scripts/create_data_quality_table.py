from data_ingestion.db import get_db_connection

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS data_quality_reports (
    id BIGSERIAL PRIMARY KEY,
    run_ts TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    check_type TEXT NOT NULL,
    status TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""

def main():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()
        print("✅ data_quality_reports table created")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
