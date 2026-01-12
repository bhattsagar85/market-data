from typing import Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel


class Alert(BaseModel):
    symbol: str
    raised_at: datetime
    consecutive_partial_days: int
    threshold: int
    message: str


class SymbolHealth(BaseModel):
    symbol: str
    daily_coverage: Optional[str]
    auto_backfill: Optional[str]
    freshness: Dict[str, str]


class QualityEvent(BaseModel):
    run_ts: datetime
    check_type: str
    status: str
    details: Dict[str, Any]
