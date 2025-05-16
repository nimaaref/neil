import sys
import os
import sqlite3
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier

# Add parent directory for local imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import NFLConfig
from models.train_random_forest_base import run_classification_pipeline
from utils import (
    training_data_filter,
    build_season_week_filter
)

def render_prediction_sql(config, sql_file_path=None):
    """
    Reads the SQL prediction table template and substitutes dynamic placeholders.
    """
    if sql_file_path is None:
        sql_file_path = os.path.join(os.path.dirname(__file__), "rf_prediction_table.sql")

    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_template = f.read()

    season_week_filter = build_season_week_filter(config)
    query = sql_template.format(
        season=config.CURRENT_SEASON,
        target_week=config.TARGET_WEEK,
        season_week_filter=season_week_filter
    )
    return query

with sqlite3.connect(NFLConfig().DB_PATH) as conn:
    conn.execute("DROP TABLE IF EXISTS rf_game_predictions_2024")
    print("🧹 Dropped existing rf_game_predictions_2024 table.")


# --- Main loop: retrain and predict for each week in 2024 season
for week in range(1, 23):
    print(f"\n--- Processing Week {week} ---\n")

    # Set dynamic config for this week's training & prediction
    config = NFLConfig(target_week=week, training_cutoff_week=week - 1)
    conn = sqlite3.connect(config.DB_PATH)

    # Step 1: Load training data
    if week == 1:
        train_query = f"SELECT * FROM rf_training_data WHERE season = {config.CURRENT_SEASON - 1}"
    else:
        train_query = f"SELECT * FROM rf_training_data WHERE {training_data_filter(config)}"

    training_data = pd.read_sql(train_query, conn)

    if training_data.empty:
        print(f"⚠️ Skipping Week {week} - Training data empty.")
        conn.close()
        continue

    # Step 2: Train the model
    model = run_classification_pipeline(training_data, config, conn)
    model_path = f"random_forest_model_week_{week}.pkl"
    joblib.dump(model, model_path)
    print(f"✅ Model for Week {week} saved as {model_path}")

    # Step 3: Build prediction SQL and load input
    prediction_sql = render_prediction_sql(config)
    try:
        prediction_week_data = pd.read_sql_query(prediction_sql, conn)
    except Exception as e:
        print(f"❌ Error loading prediction data for Week {week}: {e}")
        conn.close()
        continue

    if prediction_week_data.empty:
        print(f"⚠️ Skipping Week {week} - No prediction input data.")
        conn.close()
        continue

    print(f"📊 Prediction input loaded: {len(prediction_week_data)} records")

    # ✅ Save prediction input data to week-specific SQL table
    pred_input_table = f"rf_prediction_inputs_week_{week}"
    prediction_week_data.to_sql(pred_input_table, conn, if_exists="replace", index=False)
    print(f"📁 Saved input features to table: {pred_input_table}")

    # Step 4: Predict and save results
    X_pred = prediction_week_data[config.TRAINING_COLUMNS]
    y_pred = model.predict(X_pred)

    predictions_df = pd.DataFrame({
        "game_id": prediction_week_data["game_id"],
        "home_team": prediction_week_data["home_team"],
        "away_team": prediction_week_data["away_team"],
        "predicted_outcome": y_pred,
        "week": week
    })
    prediction_output_table = "rf_game_predictions_2024"
    predictions_df.to_sql(prediction_output_table, conn, if_exists="append", index=False)
    print(f"📁 Appended predictions to table: {prediction_output_table}")


    conn.close()

print("\n🏁 Completed backtesting for all regular season and playoff weeks!")
