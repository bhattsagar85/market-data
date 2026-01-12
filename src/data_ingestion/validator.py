# src/data_ingestion/validator.py

def validate_candles(df):
    if df.empty:
        return df

    # No duplicates
    df = df.drop_duplicates(subset=["ts"])

    # Monotonic time
    df = df.sort_values("ts")

    return df
