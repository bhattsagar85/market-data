import time
import logging
from typing import List

from data_ingestion.orchestrator import run_ingestion_job

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Batching configuration
# ─────────────────────────────────────────────
BATCH_SIZE = 3

# Adaptive throttling params
MIN_SLEEP = 0.5      # seconds
MAX_SLEEP = 10.0     # seconds
SLEEP_STEP = 1.0     # incremental backoff
RECOVERY_STEP = 0.5  # speed-up on success


def chunked(lst: List[str], size: int):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def job_wrapper(job_name: str, symbols: List[str]):
    """
    Scheduler entry point with:
    - symbol batching
    - soft sleep
    - adaptive throttling
    """

    logger.info(
        f"JOB START | {job_name} | symbols={len(symbols)}"
    )

    current_sleep = MIN_SLEEP
    error_streak = 0

    for batch_no, batch in enumerate(chunked(symbols, BATCH_SIZE), start=1):
        logger.info(
            f"JOB {job_name} | batch {batch_no} | sleep={current_sleep:.1f}s | {batch}"
        )

        try:
            run_ingestion_job(job_name, batch)

            # ✅ Success → recover slowly
            error_streak = 0
            current_sleep = max(
                MIN_SLEEP,
                current_sleep - RECOVERY_STEP
            )

        except Exception as e:
            error_streak += 1

            logger.exception(
                f"JOB ERROR | {job_name} | batch={batch} | streak={error_streak}"
            )

            # 🔴 Backoff on error
            current_sleep = min(
                MAX_SLEEP,
                current_sleep + SLEEP_STEP * error_streak
            )

        # Soft sleep between batches
        if batch_no * BATCH_SIZE < len(symbols):
            time.sleep(current_sleep)

    logger.info(
        f"JOB END | {job_name}"
    )
