from fastapi import APIRouter, Depends
from dashboard.db import get_db
from dashboard.schemas import QualityEvent

router = APIRouter(prefix="/symbols", tags=["Symbols"])


@router.get("/{symbol}/history", response_model=list[QualityEvent])
def get_symbol_history(symbol: str, limit: int = 50, conn=Depends(get_db)):
    cur = conn.cursor()

    cur.execute("""
        SELECT
            run_ts,
            check_type,
            status,
            details
        FROM data_quality_reports
        WHERE symbol = %s
        ORDER BY run_ts DESC
        LIMIT %s
    """, (symbol, limit))

    return cur.fetchall()
