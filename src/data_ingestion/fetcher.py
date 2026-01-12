import logging
import pandas as pd
from datetime import datetime

from data_ingestion.clients.kite_client import KiteClient

logger = logging.getLogger(__name__)

_kite_client = None


def _get_kite_client() -> KiteClient:
    global _kite_client
    if _kite_client is None:
        logger.info("Initializing Kite client")
        _kite_client = KiteClient()
    return _kite_client


def fetch_candles(
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """
    Fetch historical candles from Zerodha Kite.

    Returns DataFrame with columns:
    ts, open, high, low, close, volume
    """

    client = _get_kite_client()

    return client.fetch_candles(
        symbol=symbol,
        timeframe=timeframe,
        start=start,
        end=end,
    )
