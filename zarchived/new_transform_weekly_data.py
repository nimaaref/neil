import pandas as pd
import sqlite3
from config import NFLConfig
from utils import training_data_filter
from utils import schedule_filter

def load_data_from_sqlite(config):
    """Loads data from team_stats and schedules tables in the SQLite database."""
    conn = sqlite3.connect(config.DB_PATH)
    
    # Load tables
    team_stats = pd.read_sql_query(f"SELECT * FROM {config.WEEKLY_STATS_TABLE}", conn)
    schedules = pd.read_sql_query(f"SELECT * FROM {config.SCHEDULES_TABLE}", conn)
    
    conn.close()
    
    # Apply filters
    team_stats = team_stats.query(training_data_filter(team_stats))
    schedules = schedules.query(schedule_filter(schedules))
    
    return team_stats, schedules

def merge_team_stats_with_schedules(schedules, team_stats):
    """
    Merges home and away team stats with schedules to create a unified game data DataFrame.
    """
    # Merge home team stats
    merged_data = pd.merge(
        schedules, team_stats, 
        how='left',
        left_on=['home_team', 'season', 'week', 'season_type'],
        right_on=['recent_team', 'season', 'week', 'season_type']
    )

    # Merge away team stats with a suffix to distinguish columns
    merged_data = pd.merge(
        merged_data, team_stats, 
        how='left',
        left_on=['away_team', 'season', 'week', 'season_type'],
        right_on=['recent_team', 'season', 'week', 'season_type'],
        suffixes=('_home', '_away')
    )

    return merged_data

def calculate_game_related_metrics(merged_data):
    """Calculates additional game-related fields and cleans up the DataFrame."""
    # Calculate game outcome (1 if home team wins, 0 otherwise)
    merged_data['outcome'] = (merged_data['home_score'] > merged_data['away_score']).astype(int)
    
    # Total points in the game
    merged_data['game_total_points'] = merged_data['home_score'] + merged_data['away_score']
    
    # Score difference
    merged_data['score_diff'] = merged_data['home_score'] - merged_data['away_score']
    
    # Drop unnecessary columns
    merged_data.drop(columns=['recent_team_home', 'recent_team_away'], inplace=True, errors='ignore')

    return merged_data

def save_to_sqlite(data, config, table_name="weekly_game_data"):
    """Saves the transformed data to a new table in SQLite."""
    with sqlite3.connect(config.DB_PATH) as conn:
        data.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Data successfully saved to {table_name} table.")

if __name__ == "__main__":
    config = NFLConfig()  # Load config

    # Step 1: Load data from SQLite
    team_stats, schedules = load_data_from_sqlite(config)

    # Step 2: Merge data
    merged_data = merge_team_stats_with_schedules(schedules, team_stats)

    # Step 3: Calculate game-related metrics
    weekly_game_data = calculate_game_related_metrics(merged_data)

    # Step 4: Save the final data to a new SQLite table
    save_to_sqlite(weekly_game_data, config)

    # Display a preview of the final DataFrame
    print(weekly_game_data.head())
