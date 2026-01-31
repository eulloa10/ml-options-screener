import logging
import sys
import os
import yfinance as yf
from datetime import datetime, date

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from screener.option_screener import OptionScreener

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

def is_market_open():
    """
    Checks if the US Equity market is open today by fetching the latest
    data for a benchmark ticker (SPY).
    """
    try:
        spy = yf.download("SPY", period="1d", progress=False, ignore_tz=True)
        
        if spy.empty:
            logging.warning("Market Check Failed: No data returned for SPY.")
            return False

        # Get the date of the latest available data
        latest_date = spy.index[-1].date()
        today_date = date.today()

        if latest_date < today_date:
            logging.info(f"Market Closed. Latest data is from {latest_date}, but today is {today_date}.")
            return False
            
        return True

    except Exception as e:
        logging.warning(f"Market check failed due to API error: {e}. Assuming open.")
        # Fail open: If Yahoo is glitchy, we try running anyway rather than skipping a valid day.
        return True

def main():
    """
    Main entry point for the Daily Option Screener.
    Orchestrates the data fetch and S3 upload.
    """
    start_time = datetime.now()
    logging.info("--- Starting Daily Option Screener Job ---")

    if not is_market_open():
        logging.info("--- Job Skipped (Market Closed) ---")
        # Exit with 0 (Success) so GitHub Actions doesn't send a "Failure" email
        sys.exit(0)    

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
