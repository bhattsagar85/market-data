import os
import numpy as np
import pandas as pd
import sqlalchemy as sa
from pathlib import Path
from dotenv import load_dotenv


# ─────────────────────────────────────────────
# ENV & DB CONFIG
# ─────────────────────────────────────────────

# Load .env file (project root)
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "marketdata")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not DB_USER:
    raise RuntimeError("❌ DB_USER is not set in environment or .env")

if DB_PASSWORD is None:
    raise RuntimeError("❌ DB_PASSWORD is not set in environment or .env")

DB_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# SQL file path
SQL_PATH = Path(__file__).resolve().parents[1] / "sql" / "feature_base_15m.sql"


# ─────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()

    rs = avg_gain / avg_loss
    return 1 - (1 / (1 + rs))  # normalized 0–1


def macd_hist(series: pd.Series) -> pd.Series:
    ema12 = series.ewm(span=12, adjust=False).mean()
    ema26 = series.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd - signal


# ─────────────────────────────────────────────
# FEATURE BUILDER
# ─────────────────────────────────────────────

def build_feature_vectors():
    """
    Build 15M market-state vectors for FAISS / LLM usage.

    Returns:
        List[Tuple[np.ndarray, dict]]
    """

    # 1️⃣ Create SQLAlchemy engine (CORRECT way)
    engine = sa.create_engine(
        DB_URL,
        pool_pre_ping=True,
    )

    # 2️⃣ Load SQL
    sql = SQL_PATH.read_text()
    df = pd.read_sql(sql, engine)

    if df.empty:
        print("⚠️ No rows returned from SQL")
        return []

    # 3️⃣ Force numeric dtypes (defensive, mandatory)
    numeric_cols = [
        "close",
        "ret_15m",
        "ret_1h",
        "vol_30m",
        "vol_zscore",
        "vwap_dist",
        "close_pos",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    vectors = []

    # 4️⃣ Feature computation per symbol
    for symbol, g in df.groupby("symbol"):
        g = g.sort_values("ts")

        # Indicators
        g["rsi_14"] = rsi(g["close"])
        g["macd_hist"] = macd_hist(g["close"])

        # Minimal, stable feature set (Phase-1)
        required_cols = [
            "ret_15m",
            "ret_1h",
            "vol_30m",
            "rsi_14",
            "macd_hist",
        ]

        g = g.dropna(subset=required_cols)

        if g.empty:
            continue

        for _, r in g.iterrows():
            vector = np.array(
                [
                    r.ret_15m,
                    r.ret_1h,
                    r.vol_30m,
                    np.clip(r.vol_zscore, -3, 3),
                    r.vwap_dist,
                    r.rsi_14,
                    r.macd_hist,
                    r.close_pos,
                ],
                dtype="float32",
            )

            meta = {
                "symbol": symbol,
                "timeframe": "15M",
                "ts": r.ts.isoformat(),
            }

            vectors.append((vector, meta))

    return vectors


# ─────────────────────────────────────────────
# CLI ENTRYPOINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    data = build_feature_vectors()

    print(f"✅ Total vectors built: {len(data)}")

    if data:
        print("Sample vector:", data[0][0])
        print("Sample metadata:", data[0][1])
