from fastapi import APIRouter, Depends
from dashboard.db import get_db
from dashboard.schemas import SymbolHealth

router = APIRouter(prefix="/health", tags=["Health"])


@router.get("/symbols", response_model=list[SymbolHealth])
def get_symbol_health(conn=Depends(get_db)):
    cur = conn.cursor()

    cur.execute("""
        WITH latest AS (
            SELECT DISTINCT ON (symbol, timeframe, check_type)
                symbol,
                timeframe,
                check_type,
                status
            FROM data_quality_reports
            ORDER BY symbol, timeframe, check_type, run_ts DESC
        )
        SELECT * FROM latest
    """)

    health = {}

    for row in cur.fetchall():
        symbol = row["symbol"]
        health.setdefault(symbol, {
            "symbol": symbol,
            "daily_coverage": None,
            "auto_backfill": None,
            "freshness": {}
        })

        if row["check_type"] == "daily_coverage":
            health[symbol]["daily_coverage"] = row["status"]

        elif row["check_type"] == "auto_backfill":
            health[symbol]["auto_backfill"] = row["status"]

        elif row["check_type"] == "freshness":
            health[symbol]["freshness"][row["timeframe"]] = row["status"]

    return list(health.values())
