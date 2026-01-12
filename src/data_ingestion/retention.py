from datetime import timedelta

RETENTION_POLICY = {
    "1D": 3650,
    "15m": 180,
    "5m": 90,
    "1m": 30,
}


def retention_cutoff(timeframe: str, now):
    tf = timeframe.upper()

    if tf not in RETENTION_POLICY:
        raise ValueError(f"Unknown timeframe for retention: {timeframe}")

    days = RETENTION_POLICY[tf]
    return now - timedelta(days=days)
