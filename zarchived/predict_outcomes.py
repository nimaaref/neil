import pandas as pd
from sklearn.impute import SimpleImputer
import sqlite3
import joblib

def load_schedules_from_sqlite(conn):
    """Loads the schedules data for the prediction week from SQLite."""
    schedules = pd.read_sql_query("SELECT * FROM schedules WHERE season = 2024", conn)
    schedules = schedules[['game_id', 'season', 'week', 'home_team', 'away_team', 'home_score', 'away_score',
                           'away_rest', 'home_rest', 'away_moneyline', 'home_moneyline', 'spread_line',
                           'away_spread_odds', 'home_spread_odds', 'total_line', 'under_odds', 'over_odds', 'div_game']]
    schedules['home_score'].fillna(0, inplace=True)
    schedules['away_score'].fillna(0, inplace=True)
    return schedules

def filter_prediction_week(schedules, prediction_week):
    """Filters the schedule to only include games for the prediction week."""
    return schedules[schedules['week'] == prediction_week]

def compute_team_stats(weekly_game_data, current_week, home_agg_dict, away_agg_dict):
    """Computes team stats up to the current week."""
    data_up_to_current_week = weekly_game_data[weekly_game_data['week'] < current_week]

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
    return pd.DataFrame(X, columns=expected_columns)

def predict_outcomes(model, X):
    """Predicts the outcomes for the given DataFrame."""
    return model.predict(X)

def save_predictions_to_sqlite(predictions_df, conn, table_name="game_predictions"):
    """Saves predictions to an SQLite table."""
    predictions_df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Predictions saved to '{table_name}' table.")

def run_prediction_pipeline(conn, merged_data, model, current_week, prediction_week):
    """Main function to run the prediction pipeline."""
    # Load schedules for the prediction week from SQLite
    schedules_2024 = load_schedules_from_sqlite(conn)
    schedules_2024_prediction_week = filter_prediction_week(schedules_2024, prediction_week)

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
    team_stats_home, team_stats_away = compute_team_stats(merged_data, current_week, home_aggregation, away_aggregation)

    # Merge team stats with the prediction week schedule
    schedules_2024_prediction_week = merge_team_stats_with_schedule(schedules_2024_prediction_week, team_stats_home, team_stats_away)

    # Prepare features for prediction
    expected_columns = merged_data.drop(columns=['game_id', 'home_team', 'away_team', 'outcome', 'score_diff', 'game_total_points']).columns.tolist()
    X_2024_prediction_week = prepare_features_for_prediction(schedules_2024_prediction_week, expected_columns)

    # Predict outcomes
    y_pred = predict_outcomes(model, X_2024_prediction_week)

    # Save predictions to SQLite
    predictions_df = pd.DataFrame({
        'game_id': schedules_2024_prediction_week['game_id'],
        'home_team': schedules_2024_prediction_week['home_team'],
        'away_team': schedules_2024_prediction_week['away_team'],
        'predicted_outcome': y_pred  # 1 for home win, 0 for away win
    })
    save_predictions_to_sqlite(predictions_df, conn)

    print(predictions_df)

if __name__ == "__main__":
    # Connect to SQLite and load merged data
    conn = sqlite3.connect("nfl_data.db")
    merged_data = pd.read_sql_query("SELECT * FROM weekly_game_data", conn)

    # Load the trained model
    model = joblib.load("random_forest_model.pkl")

    # Set current week and prediction week
    current_week = 15
    prediction_week = 16

    # Run the prediction pipeline
    run_prediction_pipeline(conn, merged_data, model, current_week, prediction_week)

    conn.close()
