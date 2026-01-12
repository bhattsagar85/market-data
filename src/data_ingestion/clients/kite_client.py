import time
import logging
from datetime import datetime, timedelta
import pandas as pd
import os

from kiteconnect import KiteConnect
from kiteconnect.exceptions import KiteException

from auth.zerodha_auth import load_access_token
from data_ingestion.symbol_resolver import resolve_symbol
from data_ingestion.timeframe_mapper import TIMEFRAME_MAP, TIMEFRAMES

logger = logging.getLogger(__name__)

MAX_KITE_DAYS = 2000


class KiteClient:
    """
    Zerodha Kite Connect client for historical candle fetching.
    Rate-safe and production-ready.
    """

    # Zerodha allows ~3 requests / second.
    _MIN_CALL_INTERVAL_SEC = 0.4

    def __init__(self):
        self.kite = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
        self.kite.set_access_token(load_access_token())
        self._last_call_ts = 0.0

    # ─────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────

    def _rate_limit(self):
        elapsed = time.time() - self._last_call_ts
        if elapsed < self._MIN_CALL_INTERVAL_SEC:
            time.sleep(self._MIN_CALL_INTERVAL_SEC - elapsed)
        self._last_call_ts = time.time()

    # ─────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────

    def fetch_candles(
        self,
        symbol: str,
        timeframe: str,
        start: datetime,
        end: datetime,
        exchange: str = "NSE",
    ) -> pd.DataFrame:
        """
        Fetch historical candles for a symbol.
        Automatically chunks requests to satisfy Kite limits.
        """

        instrument_token = resolve_symbol(symbol, exchange)
        interval = get_kite_interval(timeframe)

        if not interval:
            raise ValueError(f"Unsupported timeframe for Kite: {timeframe}")

        all_dfs = []
        current_start = start

        while current_start < end:
            current_end = min(
                current_start + timedelta(days=MAX_KITE_DAYS),
                end,
            )

            self._rate_limit()

            try:
                data = self.kite.historical_data(
                    instrument_token=instrument_token,
                    from_date=current_start,
                    to_date=current_end,
                    interval=interval,
                )
            except KiteException:
                logger.exception(
                    f"Kite API failure: {symbol} {timeframe} "
                    f"{current_start} → {current_end}"
                )
                raise

            if data:
                df = pd.DataFrame(data)
                all_dfs.append(df)

            current_start = current_end

        if not all_dfs:
            return pd.DataFrame()

        df = pd.concat(all_dfs, ignore_index=True)

        # Normalize schema
        df.rename(columns={"date": "ts"}, inplace=True)
        df["ts"] = (
            pd.to_datetime(df["ts"], utc=True)
            .dt.tz_convert("Asia/Kolkata")
        )

        df = df[["ts", "open", "high", "low", "close", "volume"]]

        return df
    

def get_kite_interval(timeframe: str) -> str:
    try:
        return TIMEFRAMES[timeframe]["kite"]
    except KeyError:
        raise ValueError(
            f"Unsupported timeframe '{timeframe}'. "
            f"Supported: {list(TIMEFRAMES.keys())}"
        )

