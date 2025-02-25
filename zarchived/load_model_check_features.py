import joblib

# Load the trained model
model = joblib.load("random_forest_model.pkl")

# Check expected features
print("Model was trained with features:", model.feature_names_in_)
