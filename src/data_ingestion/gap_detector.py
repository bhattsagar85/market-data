# src/data_ingestion/gap_detector.py

from datetime import timedelta, time
import pytz


TF_DELTA = {
    "15M": timedelta(minutes=15),
    "5M": timedelta(minutes=5),
    "1M": timedelta(minutes=1),
}


IST = pytz.timezone("Asia/Kolkata")

def is_market_close_gap(prev_ts, curr_ts):
    """
    Returns True if the gap is due to market close (EOD → next day open).
    """
    prev_t = prev_ts.astimezone(IST).time()
    curr_t = curr_ts.astimezone(IST).time()

    return prev_t >= time(15, 30) and curr_t <= time(9, 15)


def detect_gaps(df, timeframe):
    """
    Detect gaps inside an intraday dataframe.

    Returns:
        List of (gap_start, gap_end)
    """
    timeframe = timeframe.upper()

    if df.empty or timeframe not in TF_DELTA:
        return []

    expected_delta = TF_DELTA[timeframe]

    df = df.sort_values("ts")
    timestamps = list(df["ts"])

    gaps = []
    prev_ts = None

    for ts in timestamps:
        if prev_ts and ts - prev_ts > expected_delta * 1.5:
            gaps.append(
                (prev_ts + expected_delta, ts - expected_delta)
            )
        prev_ts = ts

    return gaps


