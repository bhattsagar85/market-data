from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from scheduler.job_runner import job_wrapper
from data_ingestion.eod_reconciliation import run_eod_reconciliation
from agents.data_quality.data_completeness_agent import DataCompletenessAgent
from data_ingestion.db import get_db_connection


scheduler = BlockingScheduler(timezone="Asia/Kolkata")

SYMBOLS = ["RELIANCE", "INFY", "TCS"]


def start():
    print("✅ Scheduler started. Waiting for jobs...")

    # ─────────────────────────────────────────────
    # Daily EOD (low priority, once per day)
    # ─────────────────────────────────────────────
    scheduler.add_job(
        job_wrapper,
        CronTrigger(hour=18, minute=0),
        args=["daily_eod", SYMBOLS],
        id="daily_eod",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
        replace_existing=True,
    )

    # ─────────────────────────────────────────────
    # 15-minute intraday
    # ─────────────────────────────────────────────
    scheduler.add_job(
        job_wrapper,
        CronTrigger(minute="*/15"),
        args=["intraday_15m", SYMBOLS],
        id="intraday_15m",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
        replace_existing=True,
    )

    # ─────────────────────────────────────────────
    # 5-minute intraday
    # ─────────────────────────────────────────────
    scheduler.add_job(
        job_wrapper,
        CronTrigger(minute="*/5"),
        args=["intraday_5m", SYMBOLS],
        id="intraday_5m",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=180,
        replace_existing=True,
    )

    # ─────────────────────────────────────────────
    # 1-minute intraday (HIGH CARE)
    # ─────────────────────────────────────────────
    scheduler.add_job(
        job_wrapper,
        CronTrigger(minute="*/1"),
        args=["intraday_1m", SYMBOLS],
        id="intraday_1m",
        max_instances=1,          # ❗ no overlap
        coalesce=True,            # ❗ collapse missed runs
        misfire_grace_time=30,    # ❗ tolerate short delays
        replace_existing=True,
    )

    # ─────────────────────────────────────────────
    # End-of-day intraday reconciliation
    # ─────────────────────────────────────────────
    scheduler.add_job(
        run_eod_reconciliation,
        CronTrigger(hour=18, minute=0),
        args=[SYMBOLS],
        id="eod_reconciliation",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
        replace_existing=True,
    )


    scheduler.start()




if __name__ == "__main__":
    start()
