import pandas as pd

def create_volatility_feature(data):
    """
    Creates a feature for the volatility of the dollar value.

    Parameters:
    data (pd.DataFrame): DataFrame containing the dollar value data.

    Returns:
    pd.DataFrame: DataFrame with the added volatility feature.
    """
    # Calculate the daily high-low range
    data['high_low_range'] = data['High'] - data['Low']

    # Calculate the daily open-close range
    data['open_close_range'] = data['Close'] - data['Open']

    # Calculate the daily average true range (ATR)
    data['high_low_range_shift'] = data['high_low_range'].shift(1)
    data['high_low_range_shift'] = data['high_low_range_shift'].fillna(0)
    data['high_low_range_max'] = data[['high_low_range', 'high_low_range_shift']].max(axis=1)
    data['atr'] = data['high_low_range_max'].rolling(window=14).mean()

    # Calculate the daily volatility using the ATR
    data['volatility'] = data['atr'] / data['Close']

    return data

def create_moving_average_feature(data):
    """
    Creates a feature for the moving average of the dollar value.

    Parameters:
    data (pd.DataFrame): DataFrame containing the dollar value data.

    Returns:
    pd.DataFrame: DataFrame with the added moving average feature.
    """
    # Calculate the 7-day moving average
    data['ma_7'] = data['Close'].rolling(window=7).mean()

    # Calculate the 14-day moving average
    data['ma_14'] = data['Close'].rolling(window=14).mean()

    # Calculate the 30-day moving average
    data['ma_30'] = data['Close'].rolling(window=30).mean()

    return data

def create_momentum_feature(data):
    """
    Creates a feature for the momentum of the dollar value.

    Parameters:
    data (pd.DataFrame): DataFrame containing the dollar value data.

    Returns:
    pd.DataFrame: DataFrame with the added momentum feature.
    """
    # Calculate the 7-day momentum
    data['momentum_7'] = data['Close'] - data['Close'].shift(7)

    # Calculate the 14-day momentum
    data['momentum_14'] = data['Close'] - data['Close'].shift(14)

    # Calculate the 30-day momentum
    data['momentum_30'] = data['Close'] - data['Close'].shift(30)

    return data

def create_bollinger_bands_feature(data):
    """
    Creates a feature for the Bollinger Bands of the dollar value.

    Parameters:
    data (pd.DataFrame): DataFrame containing the dollar value data.

    Returns:
    pd.DataFrame: DataFrame with the added Bollinger Bands feature.
    """
    # Calculate the 20-day moving average
    data['ma_20'] = data['Close'].rolling(window=20).mean()

    # Calculate the 20-day standard deviation
    data['std_20'] = data['Close'].rolling(window=20).std()

    # Calculate the upper Bollinger Band
    data['upper_bb'] = data['ma_20'] + 2 * data['std_20']

    # Calculate the lower Bollinger Band
    data['lower_bb'] = data['ma_20'] - 2 * data['std_20']

    return data

def create_features(data):
    try:
        if not isinstance(data, pd.DataFrame):
            raise ValueError("Input data must be a pandas DataFrame")
        data = create_volatility_feature(data)
        data = create_moving_average_feature(data)
        data = create_momentum_feature(data)
        data = create_bollinger_bands_feature(data)
        return data
    except Exception as e:
        print(f"Error creating features: {e}")
        return None