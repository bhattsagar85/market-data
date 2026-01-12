import pandas as pd
from functools import lru_cache
from data_ingestion.instruments import load_instruments


# We cache instruments in memory to avoid repeated parquet reads
@lru_cache(maxsize=1)
def _get_instruments_df() -> pd.DataFrame:
    df = load_instruments()

    # Normalize for safety
    df["tradingsymbol"] = df["tradingsymbol"].astype(str)
    df["exchange"] = df["exchange"].astype(str)

    return df


def resolve_symbol(
    symbol: str,
    exchange: str = "NSE",
) -> int:
    """
    Resolve tradingsymbol to Zerodha instrument_token.

    Example:
        INFY → 1594
    """
    df = _get_instruments_df()

    match = df[
        (df["tradingsymbol"] == symbol)
        & (df["exchange"] == exchange)
    ]

    if match.empty:
        raise ValueError(
            f"Instrument not found for symbol={symbol}, exchange={exchange}"
        )

    if len(match) > 1:
        # This should NEVER happen for NSE equities
        raise RuntimeError(
            f"Multiple instruments found for {symbol} on {exchange}"
        )

    return int(match.iloc[0]["instrument_token"])
