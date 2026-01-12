import argparse
import logging
from qa.intraday_completeness_job import run_intraday_completeness_check

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run market data QA checks")
    parser.add_argument(
        "--config",
        required=True,
        help="Path to QA YAML config",
    )

    args = parser.parse_args()

    success = run_intraday_completeness_check(args.config)

    if not success:
        exit(1)
