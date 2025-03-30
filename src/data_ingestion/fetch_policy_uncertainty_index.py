import logging
import requests
import time
import pandas as pd
import os
from datetime import datetime, timedelta
import io

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_policy_uncertainty_index(date, max_retries=3, retry_delay=1):
    POLICY_UNCERTAINTY_API = "https://www.policyuncertainty.com/media/All_Daily_Updates.csv"
    params = {}

    attempts = 0
    while attempts < max_retries:
        try:
            response = requests.get(POLICY_UNCERTAINTY_API, params=params, timeout=10)  # Set a timeout of 10 seconds
            response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
            logging.debug(f"Received response from {POLICY_UNCERTAINTY_API}: {response.status_code}")
            break
        except requests.exceptions.HTTPError as errh:
            logging.error(f"HTTP Error: {errh}")
            attempts += 1
            time.sleep(retry_delay)
        except requests.exceptions.ConnectionError as errc:
            logging.error(f"Error Connecting: {errc}")
            attempts += 1
            time.sleep(retry_delay)
        except requests.exceptions.Timeout as errt:
            logging.error(f"Timeout Error: {errt}")
            attempts += 1
            time.sleep(retry_delay)
        except requests.exceptions.RequestException as err:
            logging.error(f"Something went wrong: {err}")
            attempts += 1
            time.sleep(retry_delay)
    else:
        logging.error(f"Failed to download data from {POLICY_UNCERTAINTY_API} after {max_retries} attempts")
        return None

    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data', 'raw')
    os.makedirs(data_dir, exist_ok=True)

    try:
        data = pd.read_csv(io.BytesIO(response.content))
        if data.empty:
            logging.error("No data available")
            return None

        data['Date'] = pd.to_datetime(data[['year','month', 'day']])

        start_date = date - timedelta(days=29)
        end_date = date

        data = data[(data['Date'] >= start_date) & (data['Date'] <= end_date)]

        file_path = os.path.join(data_dir, f"us_policy_uncertainty_index_{date.strftime('%Y-%m-%d')}.csv")

        data.to_csv(file_path, index=False)
        logging.info(f"Data saved to {file_path}")
        return file_path
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        return None