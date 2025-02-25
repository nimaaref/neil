import pandas as pd
import sqlite3
import joblib
from sklearn.impute import SimpleImputer
from config import NFLConfig

def load_schedules_from_sqlite(conn, config):
    """Loads the schedules data for the prediction week from SQLite."""
    query = f"SELECT * FROM {config.SCHEDULES_TABLE} WHERE {config.schedule_filter} AND {config.prediction_week_filter}"
    schedules = pd.read_sql_query(query, conn)
    schedules = schedules[config.SCHEDULE_COLUMNS]
    print(schedules.head())
    return schedules

def compute_team_stats(weekly_game_data, config, home_agg_dict, away_agg_dict):
    """Computes team stats up to the current week."""
    data_up_to_current_week = weekly_game_data.query(config.training_data_filter)
    
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
    print("Available columns:", sorted(merged_data.columns.tolist()))
    print("Expected columns:", sorted(expected_columns))
    print("\nMissing columns:", sorted(set(expected_columns) - set(merged_data.columns)))
    
    X = merged_data.reindex(columns=expected_columns, fill_value=0)
    return pd.DataFrame(X, columns=expected_columns)

def predict_outcomes(model, X):
    """Predicts the outcomes for the given DataFrame."""
    return model.predict(X)

def save_predictions_to_sqlite(predictions_df, conn, table_name='predictions'):
    """
    Saves the predictions DataFrame to SQLite database.
    
    Args:
        predictions_df (pd.DataFrame): DataFrame containing predictions
        conn (sqlite3.Connection): SQLite database connection
        table_name (str): Name of the table to save predictions to
    """
    try:
        # Create table if it doesn't exist
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS predictions (
            game_id TEXT PRIMARY KEY,
            home_team TEXT,
            away_team TEXT,
            predicted_outcome INTEGER,
            prediction_week INTEGER,
            prediction_timestamp TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        '''
        conn.execute(create_table_query)
        
        # Insert or replace predictions
        predictions_df.to_sql(table_name, conn, if_exists='append', index=False)
        conn.commit()
        print(f"Successfully saved {len(predictions_df)} predictions to database")
        
    except Exception as e:
        conn.rollback()
        print(f"Error saving predictions to database: {str(e)}")
        raise

def run_prediction_pipeline(conn, merged_data, model, current_week, prediction_week):
    """Main function to run the prediction pipeline."""
    config = NFLConfig()
    # Set both target week and training cutoff week
    prediction_week = config.TARGET_WEEK 
    
    current_week = config.TRAINING_CUTOFF_WEEK 
    
    # Load schedules for the prediction week from SQLite
    schedules = load_schedules_from_sqlite(conn, config)
    

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

    try:
        # Pass config object to compute_team_stats instead of current_week
        team_stats_home, team_stats_away = compute_team_stats(merged_data, config, home_aggregation, away_aggregation)

        # Merge team stats with the prediction week schedule
        schedules = merge_team_stats_with_schedule(schedules, team_stats_home, team_stats_away)

        # Prepare features for prediction using config's training columns
        X_prediction_week = prepare_features_for_prediction(schedules, config.TRAINING_COLUMNS)

        # Verify we have data to predict
        if X_prediction_week.empty:
            raise ValueError(f"No games found for prediction week {prediction_week}")

        # Predict outcomes
        y_pred = predict_outcomes(model, X_prediction_week)

        # Save predictions to SQLite
        predictions_df = pd.DataFrame({
            'game_id': schedules['game_id'],
            'home_team': schedules['home_team'],
            'away_team': schedules['away_team'],
            'predicted_outcome': y_pred,  # 1 for home win, 0 for away win
            'prediction_week': prediction_week,  # Added prediction week for tracking
            'prediction_timestamp': pd.Timestamp.now()  # Added timestamp
        })
        
        save_predictions_to_sqlite(predictions_df, conn)
        return predictions_df

    except Exception as e:
        print(f"Error in prediction pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    # Connect to SQLite and load merged data
    conn = sqlite3.connect("nfl_data.db")
    merged_data = pd.read_sql_query("SELECT * FROM weekly_game_data", conn)

    # Load the trained model
    model = joblib.load("random_forest_model.pkl")

    config = NFLConfig()

    prediction_week = config.TARGET_WEEK 
    
    current_week = config.TRAINING_CUTOFF_WEEK 

    # Run the prediction pipeline
    run_prediction_pipeline(conn, merged_data, model, current_week, prediction_week)

    conn.close()