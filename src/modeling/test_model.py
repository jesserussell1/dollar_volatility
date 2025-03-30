# test_model.py
import pandas as pd
from modeling.model import BayesianTimeSeriesModel

# Load test data
test_data = pd.read_csv('test_data.csv')

# Create an instance of the model
model = BayesianTimeSeriesModel(test_data)

# Define the test model
model.define_model()

# Fit the test model
model.fit_model()

# Evaluate the test model
summary = model.evaluate_model()

# Print the results
print(summary)