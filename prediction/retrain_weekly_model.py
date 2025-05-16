import sys
import os

# Dynamically add the parent directory (neil/) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import sqlite3
import pandas as pd
import joblib
from config import NFLConfig
from models.train_model import run_classification_pipeline
from predict_outcomes import run_prediction_pipeline
from utils import load_existing_nfl_data

# SQLite connection
conn = sqlite3.connect(NFLConfig().DB_PATH)

# Create or ensure the predictions table exists
drop_table_query = "DROP TABLE IF EXISTS game_predictions;"
create_table_query = """
CREATE TABLE game_predictions (
    game_id TEXT,
    home_team TEXT,
    away_team TEXT,
    predicted_outcome INTEGER
);
"""
conn.execute(drop_table_query)
conn.execute(create_table_query)
conn.commit()

# Loop through all regular season and playoff weeks
for week in range(1, 23):  # 18 Regular Season + 4 Playoff Weeks
    print(f"\n--- Processing Week {week} ---\n")
    
    # Dynamically create config with correct week settings
    config = NFLConfig(target_week=week, training_cutoff_week=week - 1)
    
    # Load training data
    if week == 1:
        query = f"SELECT * FROM {config.RF_TRAINING_DATA} WHERE season == {config.CURRENT_SEASON} - 1 AND week >= 1"
    else:
        query = f"SELECT * FROM {config.RF_TRAINING_DATA} WHERE season < {config.CURRENT_SEASON} OR (season == {config.CURRENT_SEASON} AND week <= {config.TRAINING_CUTOFF_WEEK})"
    training_data = pd.read_sql(query, conn)
    
    if training_data.empty:
        print(f"Skipping Week {week} - Not enough data to train.")
        continue
    
    # Retrain the model
    model = run_classification_pipeline(training_data, config, conn)
    model_path = f"random_forest_model_week_{week}.pkl"
    joblib.dump(model, model_path)
    print(f"Model for Week {week} saved as {model_path}")
    
    # Load base data again to ensure it's up to date
    base_model_data = pd.read_sql(
    f"SELECT * FROM {config.RF_TRAINING_DATA} WHERE season <= {config.CURRENT_SEASON} AND week <= {week-1}",
    conn
)
    
    # Run predictions
    prediction_week_data = base_model_data[base_model_data["week"] <= week]
    run_prediction_pipeline(conn, prediction_week_data, model, config)

    print(f"Predictions for Week {week} saved to SQLite database")

conn.close()
print("\nCompleted backtesting for all regular season and playoff weeks!")
