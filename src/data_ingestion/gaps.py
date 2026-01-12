from datetime import timedelta
from venv import logger

EXPECTED_DELTAS = {
    "15M": timedelta(minutes=15),
    "5M": timedelta(minutes=5),
    "1M": timedelta(minutes=1),
}

def detect_and_refetch_gaps(api, conn, symbol, timeframe, df):
    expected = EXPECTED_DELTAS.get(timeframe)
    if not expected:
        return

    df = df.sort_values("ts")

    for prev, curr in zip(df["ts"], df["ts"][1:]):
        if curr - prev > expected * 1.5:
            logger.warning(
                f"GAP DETECTED {symbol} {timeframe}: {prev} → {curr}"
            )
