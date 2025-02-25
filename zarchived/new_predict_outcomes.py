import pandas as pd
import sqlite3
import joblib
from sklearn.impute import SimpleImputer
from config import NFLConfig

def load_schedules_from_sqlite(conn, config):
    """Loads the schedules data for the prediction week from SQLite."""
    query = f"SELECT * FROM {config.SCHEDULES_TABLE} WHERE {config.schedule_filter}"
    schedules = pd.read_sql_query(query, conn)
    return schedules[config.SCHEDULE_COLUMNS]

def filter_prediction_week(schedules, config):
    """Filters the schedule to only include games for the prediction week."""
    return schedules.query(config.prediction_week_filter)

def compute_team_stats(weekly_game_data, config, agg_dict):
    """Computes team stats up to the current week."""
    data_up_to_current_week = weekly_game_data.query(config.training_data_filter)
    team_stats = data_up_to_current_week.groupby('recent_team').agg(agg_dict).reset_index()
    return team_stats

def merge_team_stats_with_schedule(schedule, team_stats):
    """Merges historical stats with the schedule for the prediction week."""
    schedule = schedule.merge(team_stats, how='left', left_on='home_team', right_on='recent_team', suffixes=('_home', ''))
    schedule = schedule.merge(team_stats, how='left', left_on='away_team', right_on='recent_team', suffixes=('', '_away'))
    schedule.drop(columns=['recent_team'], errors='ignore', inplace=True)
    return schedule

def prepare_features_for_prediction(merged_data, expected_columns):
    """Ensures features match expected format, handling missing values."""
    X = merged_data.reindex(columns=expected_columns, fill_value=0)
    imputer = SimpleImputer(strategy='mean')
    X = imputer.fit_transform(X)
    return pd.DataFrame(X, columns=expected_columns)

def predict_outcomes(model, X):
    """Predicts the outcomes for the given DataFrame."""
    print(f"Model expects {model.n_features_in_} features, input has {X.shape[1]} features")
    if hasattr(model, 'feature_names_in_'):
        print("Expected feature names:", model.feature_names_in_)
    return model.predict(X)

def save_predictions_to_sqlite(predictions_df, conn, table_name="game_predictions"):
    """Saves predictions to an SQLite table."""
    predictions_df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Predictions saved to '{table_name}' table.")

def run_prediction_pipeline():
    """Main function to run the prediction pipeline."""
    config = NFLConfig()
    conn = sqlite3.connect(config.DB_PATH)
    
    schedules = load_schedules_from_sqlite(conn, config)
    schedules = filter_prediction_week(schedules, config)
    weekly_game_data = pd.read_sql_query(f"SELECT * FROM {config.WEEKLY_STATS_TABLE}", conn)
    
    agg_dict = {key: 'mean' for key in config.WEEKLY_STATS_COLUMNS if key not in ['recent_team', 'season', 'season_type', 'week']}
    team_stats = compute_team_stats(weekly_game_data, config, agg_dict)
    schedules = merge_team_stats_with_schedule(schedules, team_stats)
    
    expected_columns = [col for col in config.WEEKLY_STATS_COLUMNS if col not in ['recent_team', 'season', 'season_type', 'week']]
    X = prepare_features_for_prediction(schedules, expected_columns)
    
    model = joblib.load("random_forest_model.pkl")
    y_pred = predict_outcomes(model, X)
    
    predictions_df = pd.DataFrame({
        'game_id': schedules['game_id'],
        'home_team': schedules['home_team'],
        'away_team': schedules['away_team'],
        'predicted_outcome': y_pred
    })
    
    save_predictions_to_sqlite(predictions_df, conn)
    print(predictions_df)
    conn.close()

if __name__ == "__main__":
    run_prediction_pipeline()
