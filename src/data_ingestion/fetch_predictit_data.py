from datetime import timedelta, datetime
import logging
import requests
import pandas as pd
import os
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the data directory
DATA_DIR = '/home/jesse-russell/PycharmProjects/VolatilityProject/data/raw'
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_predictit_data(date):
    try:
        # Fetch PredictIt data for the past month
        end_date = date
        start_date = date - timedelta(days=30)
        date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

        predictit_data_dir = DATA_DIR
        predictit_data_files = []

        for file_date in date_range:
            # Fetch PredictIt data for each day and save it to a file
            url = 'https://www.predictit.org/api/marketdata/all/'
            max_retries = 5
            retries = 0
            while retries < max_retries:
                try:
                    response = requests.get(url)
                    response.raise_for_status()
                    break
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 429:
                        logging.warning(f"Too many requests to PredictIt API. Waiting 10 seconds and retrying...")
                        time.sleep(10)
                        retries += 1
                    else:
                        logging.error(f"Failed to fetch PredictIt data: {e}")
                        return None
                except requests.exceptions.RequestException as e:
                    logging.error(f"Failed to fetch PredictIt data: {e}")
                    return None

            data = response.json()

            if not isinstance(data, dict):
                logging.error(f"Invalid PredictIt data: {data}")
                return None

            if'markets' not in data:
                logging.error(f"Invalid PredictIt data: {data}")
                return None

            markets = []
            for market in data['markets']:
                for contract in market['contracts']:
                    markets.append({
                        'Date': file_date.strftime("%Y-%m-%d"),
                        'Market': market['name'],
                        'Contract': contract['name'],
                        'LastTradePrice': contract['lastTradePrice']
                    })

            df = pd.DataFrame(markets)
            file_name = f'predictit_data_daily_{file_date.strftime("%Y-%m-%d")}.csv'
            file_path = os.path.join(predictit_data_dir, file_name)
            df.to_csv(file_path, index=False)

            predictit_data_files.append(file_path)

            # Wait for 60 seconds before fetching the data for the next day
            time.sleep(60)

        # Concatenate the files
        predictit_data = pd.concat([pd.read_csv(file) for file in predictit_data_files])

        # Save the concatenated data to a single file
        output_file_path = os.path.join(predictit_data_dir, f'predictit_data_{date.strftime("%Y-%m-%d")}.csv')
        predictit_data.to_csv(output_file_path, index=False)

        # Remove the individual files
        for file in os.listdir(predictit_data_dir):
            if 'daily' in file:
                os.remove(os.path.join(predictit_data_dir, file))

        return output_file_path
    except Exception as e:
        logging.error(f"Error fetching PredictIt data: {e}")
        return None