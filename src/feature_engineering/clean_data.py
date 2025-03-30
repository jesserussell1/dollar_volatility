import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional

def read_data(file_path: str) -> Optional[pd.DataFrame]:
    """Reads data from a file and handles exceptions."""
    try:
        data = pd.read_csv(file_path)
        return data
    except Exception as e:
        logging.error(f"Error reading data from {file_path}: {e}")
        return None

def clean_data(predictit_data_file_path, policy_uncertainty_index_file_path, dollar_volatility_file_path, date):
    try:
        logging.info("Cleaning data")

        # Read the data
        dollar_data = pd.read_csv(dollar_volatility_file_path, skiprows=3,
                                  names=['Date', 'Close', 'High', 'Low', 'Open', 'Volume'])
        dollar_data['Date'] = pd.to_datetime(dollar_data['Date']).dt.date
        logging.info(f"Dollar data shape: {dollar_data.shape}")
        logging.info(f"Dollar data columns: {dollar_data.columns}")

        predictit_data = read_data(predictit_data_file_path)
        if predictit_data is None:
            return None

        predictit_data['Date'] = pd.to_datetime(predictit_data['Date']).dt.date
        logging.info(f"PredictIt data shape: {predictit_data.shape}")
        logging.info(f"PredictIt data columns: {predictit_data.columns}")

        policy_uncertainty_index = read_data(policy_uncertainty_index_file_path)
        if policy_uncertainty_index is None:
            return None

        # Select the last 30 columns
        last_30_columns = policy_uncertainty_index.columns[-30:]

        # Pivot the data into a format with one row per day
        policy_uncertainty_index = policy_uncertainty_index[['day','month', 'year'] + list(last_30_columns)].melt(id_vars=['day','month', 'year'], var_name='column_name', value_name='Policy_Uncertainty_Index')

        # Create a date column
        policy_uncertainty_index['Date'] = pd.to_datetime(policy_uncertainty_index.apply(lambda row: datetime(int(row['year']), int(row['month']), int(row['day'])), axis=1)).dt.date

        # Select only the date and policy uncertainty index columns
        policy_uncertainty_index = policy_uncertainty_index[['Date', 'Policy_Uncertainty_Index']]

        # Aggregate PredictIt data to a single row per day
        active_markets = predictit_data.groupby('Date')['Market'].count().reset_index()
        active_markets = active_markets.rename(columns={'Market': 'Active_Markets'})

        average_odds = predictit_data.groupby('Date')['LastTradePrice'].mean().reset_index()
        average_odds = average_odds.rename(columns={'LastTradePrice': 'Average_Odds'})

        odds_std_dev = predictit_data.groupby('Date')['LastTradePrice'].std().reset_index()
        odds_std_dev = odds_std_dev.rename(columns={'LastTradePrice': 'Odds_Std_Dev'})

        predictit_data = pd.merge(active_markets, average_odds, on='Date', how='inner')
        predictit_data = pd.merge(predictit_data, odds_std_dev, on='Date', how='inner')

        # Merge the policy uncertainty index data with the dollar data
        merged_data = pd.merge(dollar_data, policy_uncertainty_index, on='Date', how='inner')

        # Merge the merged data with the PredictIt data
        merged_data = pd.merge(merged_data, predictit_data, on='Date', how='inner')

        # Filter the merged data to only include the last 30 days
        merged_data = merged_data.sort_values('Date').tail(30)

        # Check for missing values
        if merged_data.isnull().values.any():
            logging.warning("Missing values found in merged data")
            print(merged_data.isnull().sum())

        logging.info("Data merged successfully")
        logging.info(merged_data.head())

        return merged_data

    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return None