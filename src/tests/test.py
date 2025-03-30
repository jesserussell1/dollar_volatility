# tests.py

import unittest
import os
from src.data_ingestion.fetch_predictit_data import fetch_predictit_data
from src.data_ingestion.fetch_policy_uncertainty_index import fetch_policy_uncertainty_index
from src.data_ingestion.fetch_currency_volatility import fetch_currency_volatility
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TestDataIngestion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.date = datetime.today() - timedelta(days=1)
        cls.date = cls.date.replace(hour=0, minute=0, second=0, microsecond=0)
        #cls.predictit_data_file_path = fetch_predictit_data(cls.date)
        cls.policy_uncertainty_index_file_path = fetch_policy_uncertainty_index(cls.date)
        cls.currency_volatility_file_path = fetch_currency_volatility(cls.date)

    def test_fetch_predictit_data(self):
        self.assertIsNotNone(self.predictit_data_file_path)
        self.assertTrue(os.path.exists(self.predictit_data_file_path))
        self.assertGreater(os.path.getsize(self.predictit_data_file_path), 0)

    def test_predictit_data_file_contents(self):
        with open(self.predictit_data_file_path, 'r') as file:
            contents = file.read()
        self.assertIn('Market', contents)
        self.assertIn('Date', contents)

    def test_fetch_policy_uncertainty_index(self):
        self.assertIsNotNone(self.policy_uncertainty_index_file_path)
        self.assertTrue(os.path.exists(self.policy_uncertainty_index_file_path))

    def test_fetch_policy_uncertainty_index_file_size(self):
        self.assertGreater(os.path.getsize(self.policy_uncertainty_index_file_path), 0)

    def test_fetch_currency_volatility(self):
        self.assertIsNotNone(self.currency_volatility_file_path)
        self.assertTrue(os.path.exists(self.currency_volatility_file_path))

    def test_fetch_currency_volatility_file_size(self):
        self.assertGreater(os.path.getsize(self.currency_volatility_file_path), 0)

if __name__ == '__main__':
    unittest.main()