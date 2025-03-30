import yfinance as yf
import logging
import time
import os
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'raw')

def fetch_currency_volatility(date, max_retries=3, retry_delay=1):
    attempts = 0
    while attempts < max_retries:
        try:
            end = date.strftime('%Y-%m-%d')
            start = (date - timedelta(days=29)).strftime('%Y-%m-%d')
            data = yf.download('DX=F', start=start, end=end)
            if data.empty:
                logging.error("No data available")
                attempts += 1
                time.sleep(retry_delay)
                continue
            file_path = os.path.join(DATA_DIR, f'dollar_volatility_{date.strftime("%Y-%m-%d")}.csv')
            data.to_csv(file_path)
            logging.info(f"Dollar volatility data downloaded successfully as of {date.strftime('%Y-%m-%d')}")
            return file_path
        except Exception as e:
            logging.error(f"Error downloading dollar volatility data: {e}")
            attempts += 1
            time.sleep(retry_delay)
    else:
        logging.error(f"Failed to download dollar volatility data after {max_retries} attempts")
        return None