import sys
import os
import logging
from src.data_ingestion.fetch_predictit_data import fetch_predictit_data
from src.data_ingestion.fetch_policy_uncertainty_index import fetch_policy_uncertainty_index
from src.data_ingestion.fetch_currency_volatility import fetch_currency_volatility
from src.feature_engineering.clean_data import clean_data
from datetime import datetime, timedelta

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        logging.info("Program started")

        date = datetime.today() - timedelta(days=1)
        date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        logging.info(f"Date set to {date}")

        data_dir = '/home/jesse-russell/PycharmProjects/VolatilityProject/data/raw'
        predictit_data_file_path = os.path.join(data_dir, f'predictit_data_{date.strftime("%Y-%m-%d")}.csv')
        policy_uncertainty_index_file_path = os.path.join(data_dir, f'policy_uncertainty_index_{date.strftime("%Y-%m-%d")}.csv')
        dollar_volatility_file_path = os.path.join(data_dir, f'dollar_volatility_{date.strftime("%Y-%m-%d")}.csv')

        logging.info(f"Checking if data for {date.strftime('%Y-%m-%d')} is already downloaded")

        if os.path.exists(predictit_data_file_path) and os.path.exists(policy_uncertainty_index_file_path) and os.path.exists(dollar_volatility_file_path):
            logging.info(f"Data for {date.strftime('%Y-%m-%d')} is already downloaded")
        else:
            logging.info(f"Fetching data for {date.strftime('%Y-%m-%d')}")

            if not os.path.exists(predictit_data_file_path):
                predictit_data_file_path = fetch_predictit_data(date)
            if not os.path.exists(policy_uncertainty_index_file_path):
                policy_uncertainty_index_file_path = fetch_policy_uncertainty_index(date)
            if not os.path.exists(dollar_volatility_file_path):
                dollar_volatility_file_path = fetch_dollar_volatility(date)

            logging.info(f"PredictIt data saved to {predictit_data_file_path}")
            logging.info(f"Policy uncertainty index saved to {policy_uncertainty_index_file_path}")
            logging.info(f"Dollar volatility saved to {dollar_volatility_file_path}")

        logging.info(f"Cleaning data for {date.strftime('%Y-%m-%d')}")

        cleaned_data = clean_data(predictit_data_file_path, policy_uncertainty_index_file_path, dollar_volatility_file_path, date)

        if cleaned_data is None:
            logging.error(f"Data cleaning failed for {date.strftime('%Y-%m-%d')}")
        else:
            logging.info(f"Data cleaned successfully for {date.strftime('%Y-%m-%d')}")
            logging.info(f"Cleaned data shape: {cleaned_data.shape}")
            logging.info(f"Cleaned data columns: {cleaned_data.columns}")

            cleaned_data_dir = '/home/jesse-russell/PycharmProjects/VolatilityProject/data/cleaned'
            os.makedirs(cleaned_data_dir, exist_ok=True)
            cleaned_data_file_path = os.path.join(cleaned_data_dir, f'cleaned_data_{date.strftime("%Y-%m-%d")}.csv')
            logging.info(f"Saving cleaned data to {cleaned_data_file_path}")

            try:
                cleaned_data.to_csv(cleaned_data_file_path, index=False)
                logging.info(f"Cleaned data saved to {cleaned_data_file_path}")
            except Exception as e:
                logging.error(f"Error saving cleaned data to {cleaned_data_file_path}: {e}")

    except Exception as e:
        logging.error(f"Error occurred: {e}")

if __name__ == "__main__":
    main()