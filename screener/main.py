# main.py
import logging
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from screener.option_screener import OptionScreener

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

def main():
    """
    Main entry point for the Daily Option Screener.
    Orchestrates the data fetch and S3 upload.
    """
    start_time = datetime.now()
    logging.info("--- Starting Daily Option Screener Job ---")

    try:
        logging.info("Initializing Screener...")
        screener = OptionScreener()

        screener.export_data()

        duration = datetime.now() - start_time
        logging.info(f"--- Job Completed Successfully in {duration} ---")

    except Exception as e:
        logging.critical(f"Job Failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
