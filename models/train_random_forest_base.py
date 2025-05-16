import sys
import os
import pandas as pd 
import numpy as np 

# Dynamically add the parent directory (neil/) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import joblib
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report
from config import NFLConfig
from utils import training_data_filter

import sqlite3

config = NFLConfig()
def prepare_features_and_target(df, target_column, conn): 
    X = df.drop(columns = ['home_team','away_team','outcome'])
    y = df[target_column]
    X.to_sql("X_training",conn, if_exists = "replace", index = False)
    return X,y 

def train_random_forest_model(X_train, y_train):
    """Train a Random Forest classifier."""
    model = RandomForestClassifier(random_state=42)
    model.fit(X_train, y_train)
    return model

def evaluate_model(model, X_test, y_test):
    """Make predictions and evaluate the model."""
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_pred)
    print(f'Accuracy: {accuracy:.2f}')
    print(f'ROC-AUC Score: {roc_auc:.2f}')
    print(classification_report(y_test, y_pred))
    return accuracy, roc_auc

def cross_validate_model(model, X, y):
    """Perform cross-validation to check model stability."""
    imputer = SimpleImputer(strategy='mean')
    X_imputed = pd.DataFrame(imputer.fit_transform(X), columns=X.columns)
    scores = cross_val_score(model, X_imputed, y, cv=5, scoring='accuracy')
    print(f"Cross-Validation Accuracy: {scores.mean():.2f}")
    return scores.mean()

def feature_importance(model, feature_names):
    """Display feature importance from the model."""
    importances = pd.DataFrame({
        'Feature': feature_names,
        'Importance': model.feature_importances_
    }).sort_values(by='Importance', ascending=False)
    print("Top 10 Important Features:")
    print(importances.head(10))
    return importances

def run_classification_pipeline(df, config,conn):
    target_column = 'outcome'
    # Step 3: Prepare features and target variable
    X, y = prepare_features_and_target(df, target_column,conn)

    # Step 4: Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    # Step 6: Train model
    model = train_random_forest_model(X_train, y_train)

    # Step 7: Evaluate model
    evaluate_model(model, X_test, y_test)

    # Step 8: Cross-validation
    cross_validate_model(model, X, y)

    # Step 9: Feature importance
    feature_importance(model, X.columns)

    return model

if __name__ == "__main__":


    # Load data from SQLite
    with sqlite3.connect(config.DB_PATH) as conn:
        query = f"SELECT * FROM rf_training_data WHERE {training_data_filter(config)}"
        merged_data = pd.read_sql_query(query, conn)

    # Run the full model training pipeline
    model = run_classification_pipeline(merged_data, config,conn)

    # Save trained model
    model_filename = "random_forest_model.pkl"
    joblib.dump(model, model_filename)
    print(f"Model saved as {model_filename}")
