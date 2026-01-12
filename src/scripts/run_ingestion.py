# src/scripts/run_ingestion.py

import argparse
import logging

from data_ingestion.orchestrator import run_ingestion_job


def get_symbols():
    return ["INFY", "TCS", "RELIANCE"]   # adjust if your symbol source differs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def main():
    parser = argparse.ArgumentParser(
        description="Run market data ingestion job"
    )
    parser.add_argument(
        "--job",
        required=True,
        help="Job name (e.g. intraday_15m, daily_eod)",
    )

    args = parser.parse_args()

    symbols = get_symbols()
    run_ingestion_job(args.job, symbols)


if __name__ == "__main__":
    main()
