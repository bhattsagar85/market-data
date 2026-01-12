import pandas as pd
from kiteconnect import KiteConnect
import os
from auth.zerodha_auth import load_access_token


INSTRUMENTS_FILE = "data/instruments_kite.parquet"


def download_instruments():
    kite = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
    kite.set_access_token(load_access_token())

    data = kite.instruments()
    df = pd.DataFrame(data)

    # 🔧 FIX: Normalize expiry column
    if "expiry" in df.columns:
        df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce")

    # 🔧 Optional but recommended: enforce dtypes
    df["instrument_token"] = df["instrument_token"].astype("int64")
    df["exchange_token"] = df["exchange_token"].astype("int64")

    # 🔧 Ensure output directory exists
    os.makedirs(os.path.dirname(INSTRUMENTS_FILE), exist_ok=True)

    df.to_parquet(INSTRUMENTS_FILE, index=False)

    print(f"✅ Instruments downloaded: {len(df)} rows")
    return df


def load_instruments():
    if not os.path.exists(INSTRUMENTS_FILE):
        raise RuntimeError("Instrument file not found. Run download_instruments().")

    return pd.read_parquet(INSTRUMENTS_FILE)
