import sys
import os
import pandas as pd 
import numpy as np 

# Dynamically add the parent directory (neil/) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import joblib
from config import NFLConfig
from utils import load_existing_nfl_data , prediction_week_filter, training_data_filter

import sqlite3

config = NFLConfig()

def compute_team_stats(base_model, training_cutoff_week, home_agg_dict, away_agg_dict):
    """Computes team stats up to the current week."""
    data_up_to_current_week = base_model[base_model['week'] <= training_cutoff_week]

    # Aggregate home and away stats
    team_stats_home = data_up_to_current_week.groupby('home_team').agg(home_agg_dict).reset_index().rename(columns={'home_team': 'team'})
    team_stats_away = data_up_to_current_week.groupby('away_team').agg(away_agg_dict).reset_index().rename(columns={'away_team': 'team'})

    # Remove '_home' and '_away' suffixes from column names
    team_stats_home.columns = [col.replace('_home', '') for col in team_stats_home.columns]
    team_stats_away.columns = [col.replace('_away', '') for col in team_stats_away.columns]

    return team_stats_home, team_stats_away

def merge_team_stats_with_schedule(schedule, team_stats_home, team_stats_away):
    """Merges historical stats with the schedule for the prediction week."""
    schedule = pd.merge(schedule, team_stats_home, how='left', left_on='home_team', right_on='team')
    schedule = pd.merge(schedule, team_stats_away, how='left', left_on='away_team', right_on='team', suffixes=('_home', '_away'))
    schedule = schedule.loc[:, ~schedule.columns.duplicated()]  # Remove duplicate columns if needed
    return schedule

def prepare_features_for_prediction(merged_data, expected_columns):
    """Reindexes and fills missing columns with 0 for prediction features."""
    X = merged_data.reindex(columns=expected_columns, fill_value=0)

    # Convert season_type from categorical to numerical
    if 'season_type' in X.columns:
        X['season_type'] = X['season_type'].map({'REG': 0, 'POST': 1})
    return pd.DataFrame(X, columns=expected_columns)

def predict_outcomes(model, X):
    """Predicts the outcomes for the given DataFrame."""
    return model.predict(X)

def save_predictions_to_sqlite(predictions_df, conn, table_name="game_predictions"):
    """Saves predictions to an SQLite table."""
    predictions_df.to_sql(table_name, conn, if_exists='append', index=False)
    print(f"Predictions saved to '{table_name}' table.")

def run_prediction_pipeline(conn, merged_data, model, config):
    training_cutoff_week = config.TRAINING_CUTOFF_WEEK
    target_week = config.TARGET_WEEK

    """Main function to run the prediction pipeline."""
    # Load schedules for the prediction week from SQLite
    schedules = load_existing_nfl_data(conn, config.SCHEDULES_TABLE, '2024')
    schedules = schedules.query(prediction_week_filter(config))

    # Define aggregation dictionaries
    home_aggregation = {
        'passing_yards_home': 'mean', 'rushing_yards_home': 'mean', 'sacks_home': 'mean',
        'total_score_home': 'mean', 'carries_home': 'mean', 'receiving_yards_home': 'mean',
        'targets_home': 'median', 'receptions_home': 'median',
        'passing_tds_home': 'sum', 'rushing_tds_home': 'sum', 'receiving_tds_home': 'sum',
        'turnovers_offense_home': 'sum', 'interceptions_home': 'sum', 'rushing_fumbles_home': 'sum',
        'special_teams_tds_home': 'sum', 'total_yards_offense_home': 'mean'
    }
    away_aggregation = {
        'passing_yards_away': 'mean', 'rushing_yards_away': 'mean', 'sacks_away': 'mean',
        'total_score_away': 'mean', 'carries_away': 'mean', 'receiving_yards_away': 'mean',
        'targets_away': 'median', 'receptions_away': 'median',
        'passing_tds_away': 'sum', 'rushing_tds_away': 'sum', 'receiving_tds_away': 'sum',
        'turnovers_offense_away': 'sum', 'interceptions_away': 'sum', 'rushing_fumbles_away': 'sum',
        'special_teams_tds_away': 'sum', 'total_yards_offense_away': 'mean'
    }

    # Compute team stats up to the current week
    team_stats_home, team_stats_away = compute_team_stats(merged_data, training_cutoff_week, home_aggregation, away_aggregation)

    # Merge team stats with the prediction week schedule
    prediction_week_merged = merge_team_stats_with_schedule(schedules, team_stats_home, team_stats_away)

    # Prepare features for prediction
    expected_columns = merged_data.drop(columns=['game_id', 'home_team', 'away_team', 'outcome', 'score_diff', 'game_total_points']).columns.tolist()
    X_2024_prediction_week = prepare_features_for_prediction(prediction_week_merged, expected_columns)

    # Predict outcomes
    y_pred = predict_outcomes(model, X_2024_prediction_week)

    # Save predictions to SQLite
    predictions_df = pd.DataFrame({
        'game_id': prediction_week_merged['game_id'],
        'home_team': prediction_week_merged['home_team'],
        'away_team': prediction_week_merged['away_team'],
        'predicted_outcome': y_pred  # 1 for home win, 0 for away win
    })
    save_predictions_to_sqlite(predictions_df, conn)

    print(predictions_df)

if __name__ == "__main__":
    config = NFLConfig()
    # Connect to SQLite and load merged data
    conn = sqlite3.connect("nfl_data.db")
    base_model =  load_existing_nfl_data(conn,config.BASE_MODEL_TABLE, training_data_filter(config) )
    # Load the trained model
    model = joblib.load("random_forest_model.pkl")

    # Run the prediction pipeline
    run_prediction_pipeline(conn, base_model, model, config)

    conn.close()
