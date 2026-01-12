# src/data_ingestion/timeframe_mapper.py

TIMEFRAMES = {
    "1M": {
        "minutes": 1,
        "db": "1minute",
        "kite": "minute",      # Zerodha uses "minute"
    },
    "5M": {
        "minutes": 5,
        "db": "5minute",
        "kite": "5minute",
    },
    "10M": {
        "minutes": 10,
        "db": "10minute",
        "kite": "10minute",
    },
    "15M": {
        "minutes": 15,
        "db": "15minute",
        "kite": "15minute",
    },
    "1D": {
        "minutes": 1440,
        "db": "day",
        "kite": "day",
    },
}

# 👇 BACKWARD-COMPAT EXPORT FOR KiteClient
TIMEFRAME_MAP = {
    tf: meta["kite"]
    for tf, meta in TIMEFRAMES.items()
}
