import pandas as pd
import requests
import logging
import io
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_policy_uncertainty_index():
    POLICY_UNCERTAINTY_API = "https://www.policyuncertainty.com/media/All_Daily_Updates.csv"
    response = requests.get(POLICY_UNCERTAINTY_API)
    data = pd.read_csv(io.BytesIO(response.content))
    data['date'] = pd.to_datetime(data[['year','month', 'day']])
    end_date = datetime.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=30)
    data = data[(data['date'] >= start_date) & (data['date'] <= end_date)]
    return data

data = fetch_policy_uncertainty_index()
print(data.head())
print(data.tail())