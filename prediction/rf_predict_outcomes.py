import sys
import os
# Dynamically add the parent directory (neil/) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import sqlite3
import pandas as pd
import joblib
from config import NFLConfig
from utils import (load_existing_nfl_data, prediction_week_filter,
                   training_data_filter, build_season_week_filter)

# Initialize config
config = NFLConfig()

def predict_outcomes(model, X):
    """Predicts the outcomes for the given DataFrame."""
    return model.predict(X)

def save_predictions_to_sqlite(predictions_df, conn, table_name="rf_game_predictions"):
    """Saves predictions to an SQLite table."""
    predictions_df.to_sql(table_name, conn, if_exists='append', index=False)
    print(f"Predictions saved to '{table_name}' table.")

def run_prediction_pipeline(conn, model, config, prediction_sql):
    """
    Runs the prediction pipeline using a fully rendered SQL query.
    
    prediction_sql: a full SQL string with rolling window filters already applied.
    """
    print(f"🔍 Executing dynamic prediction SQL for Week {config.TARGET_WEEK}")
    
    try:
        prediction_df = pd.read_sql_query(prediction_sql, conn)
        print(f"✅ Loaded {len(prediction_df)} prediction records for Week {config.TARGET_WEEK}")
    except Exception as e:
        print(f"❌ Error executing prediction SQL: {e}")
        return

    if prediction_df.empty:
        print("⚠️ No prediction data found; skipping prediction.")
        return

    # Prepare prediction features
    X_pred = prediction_df.drop(
        columns=['game_id', 'home_team', 'away_team', 'OUTCOME', 'score_diff', 'game_total_points'],
        errors='ignore'
    )
    y_true = prediction_df.get("OUTCOME")

    # Run prediction
    y_pred = predict_outcomes(model, X_pred)

    # Save predictions with metadata
    predictions_to_save = pd.DataFrame({
        "game_id": prediction_df["game_id"],
        "home_team": prediction_df["home_team"],
        "away_team": prediction_df["away_team"],
        "predicted_outcome": y_pred,
        "week": config.TARGET_WEEK
    })

    save_predictions_to_sqlite(predictions_to_save, conn)
    print("📦 Predictions:")
    print(predictions_to_save.head())


if __name__ == "__main__":
    # Reinitialize config (this may be overridden externally when looping through weeks)
    config = NFLConfig()
    conn = sqlite3.connect(config.DB_PATH)
    
    # Load the trained model (make sure that model file corresponds to the week being predicted)
    model = joblib.load("random_forest_model.pkl")
    
    # Run the prediction pipeline using dynamic rolling window input
    run_prediction_pipeline(conn, model, config)
    
    conn.close()
