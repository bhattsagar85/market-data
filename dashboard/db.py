from data_ingestion.db import get_db_connection

def get_db():
    conn = get_db_connection()
    try:
        yield conn
    finally:
        conn.close()
