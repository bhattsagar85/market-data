# src/scheduler/job_registry.py

JOB_REGISTRY = {
    "daily_eod": {
        "timeframe": "1d",
        "tier": 1,
        "retention_days": None,
        "run_type": "EOD"
    },
    "intraday_15m": {
        "timeframe": "15m",
        "tier": 2,
        "retention_days": 365,
        "run_type": "INTRADAY"
    },
    "intraday_5m": {
        "timeframe": "5m",
        "tier": 2,
        "retention_days": 180,
        "run_type": "INTRADAY"
    },
    "intraday_1m": {
        "timeframe": "1m",
        "tier": 3,
        "retention_days": 60,
        "run_type": "INTRADAY"
    }
}


def get_job_config(job_name: str) -> dict:
    job = JOB_REGISTRY[job_name].copy()
    job["timeframe"] = job["timeframe"].upper()
    return job
