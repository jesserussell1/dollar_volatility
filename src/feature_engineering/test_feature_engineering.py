# test_feature_engineering.py
import pandas as pd
import numpy as np
from feature_creation import create_features

def test_create_features():
    # Create a sample DataFrame
    np.random.seed(0)
    data = pd.DataFrame({
        'Open': np.random.uniform(90, 110, 100),
        'High': np.random.uniform(95, 115, 100),
        'Low': np.random.uniform(85, 105, 100),
        'Close': np.random.uniform(95, 115, 100)
    })

    # Create features
    data_with_features = create_features(data)

    # Check if the features are created correctly
    assert 'volatility' in data_with_features.columns
    assert'ma_7' in data_with_features.columns
    assert'momentum_7' in data_with_features.columns
    assert 'upper_bb' in data_with_features.columns

    # Check if the features have the correct data type
    assert data_with_features['volatility'].dtype == np.float64
    assert data_with_features['ma_7'].dtype == np.float64
    assert data_with_features['momentum_7'].dtype == np.float64
    assert data_with_features['upper_bb'].dtype == np.float64

    # Check if the features are calculated correctly
    assert np.allclose(data_with_features['volatility'].iloc[-1], data_with_features['atr'].iloc[-1] / data_with_features['Close'].iloc[-1])
    assert np.allclose(data_with_features['ma_7'].iloc[-1], data_with_features['Close'].iloc[-7:].mean())
    assert np.allclose(data_with_features['momentum_7'].iloc[-1], data_with_features['Close'].iloc[-1] - data_with_features['Close'].iloc[-8])
    assert np.allclose(data_with_features['upper_bb'].iloc[-1], data_with_features['ma_20'].iloc[-1] + 2 * data_with_features['std_20'].iloc[-1])

def main():
    test_create_features()
    print("All tests passed.")

if __name__ == "__main__":
    main()