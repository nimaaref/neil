import pandas as pd
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report

def check_missing_values(df):
    """Check and display columns and rows with missing values."""
    nan_counts = df.isnull().sum()
    columns_with_nulls = nan_counts[nan_counts > 0].index
    print("Columns with NaN values and their counts:")
    print(nan_counts[nan_counts > 0])
    return columns_with_nulls

def show_missing_records(df, columns_with_nulls, conn):
    """Display the records (rows) with missing values and save them to SQLite."""
    if len(columns_with_nulls) == 0:
        print("No missing values in any column.")
        return

    missing_rows = df[df[columns_with_nulls].isnull().any(axis=1)]


    # Save missing rows to SQLite
    missing_rows.to_sql("missing_records", conn, if_exists="replace", index=False)
    print(f"Missing records saved to 'missing_records' table in the SQLite database.")
    return missing_rows


def prepare_features_and_target(df, columns_to_drop, target_column):
    """Prepare the features and target for the model."""
    X = df.drop(columns=columns_to_drop)
    y = df[target_column]

    # Convert season_type from categorical to numerical
    if 'season_type' in X.columns:
        X['season_type'] = X['season_type'].map({'REG': 0, 'POST': 1})  # Label Encoding

    print("Model feature columns:")
    print(', '.join(X.columns))
    return X, y


def impute_missing_values(X_train, X_test, columns_with_nulls):
    """Impute missing values using mean imputation."""
    existing_columns = [col for col in columns_with_nulls if col in X_train.columns]

    if len(existing_columns) == 0:
        print("No columns need imputation.")
        return X_train, X_test

    imputer = SimpleImputer(strategy='mean')
    X_train[existing_columns] = imputer.fit_transform(X_train[existing_columns])
    X_test[existing_columns] = imputer.transform(X_test[existing_columns])

    return X_train, X_test

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
    cross_val_scores = cross_val_score(model, X_imputed, y, cv=5, scoring='accuracy')
    print(f"Cross-Validation Accuracy: {cross_val_scores.mean():.2f}")
    return cross_val_scores.mean()

def feature_importance(model, feature_names):
    """Display feature importance from the model."""
    importances = pd.DataFrame({
        'Feature': feature_names,
        'Importance': model.feature_importances_
    }).sort_values(by='Importance', ascending=False)
    print("Top 10 Important Features:")
    print(importances.head(10))
    return importances

def run_classification_pipeline(df):
    """Main function to run the classification model pipeline."""
    columns_to_drop = ['game_id', 'home_team', 'away_team', 'outcome', 'score_diff', 'game_total_points']
    target_column = 'outcome'

    # Step 1: Check for missing values
    columns_with_nulls = check_missing_values(df)

    # Step 2: Show and save records (rows) with missing values
    conn = sqlite3.connect("nfl_data.db")
    show_missing_records(df, columns_with_nulls, conn)
    conn.close()

    # Step 3: Prepare features and target variable
    X, y = prepare_features_and_target(df, columns_to_drop, target_column)

    # Step 4: Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    # Step 5: Impute missing values
    X_train, X_test = impute_missing_values(X_train, X_test, columns_with_nulls)

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
    # Connect to SQLite and load data for training
    import sqlite3
    import joblib

    conn = sqlite3.connect("nfl_data.db")
    merged_data = pd.read_sql_query("SELECT * FROM weekly_game_data", conn)
    conn.close()

    # Run the full model training and evaluation pipeline
    model = run_classification_pipeline(merged_data)

    # Save the trained model to a file
    joblib.dump(model, "random_forest_model.pkl")
    print("Model saved as random_forest_model.pkl")


