# probmatch_main.py

from credentials import get_credentials
from logging_handlers import setup_logger
from queries import run_matching_query
import sys


def main():
    logger = setup_logger()
    logger.info("Starting Probabilistic Match Script")

    try:
        creds = get_credentials()
        logger.debug("Credentials loaded successfully")

        result = run_matching_query(creds, logger)
        logger.info(f"Matching complete. {len(result)} records returned.")

    except Exception as e:
        logger.exception("An error occurred during processing")
        sys.exit(1)


if __name__ == "__main__":
    main()
