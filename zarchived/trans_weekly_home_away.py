# transform_weekly_data.py
import pandas as pd
import sqlite3

def load_data_from_sqlite(db_path):
    """Loads data from team_stats and schedules tables in the SQLite database."""
    conn = sqlite3.connect(db_path)
    # Load team_stats and schedules tables into DataFrames
    team_stats = pd.read_sql_query("SELECT * FROM team_stats", conn)
    schedules = pd.read_sql_query("SELECT * FROM schedules", conn)
    conn.close()
    return team_stats, schedules

def merge_team_stats_with_schedules(schedules, team_stats):
    """
    Merges home and away team stats with schedules to create a unified game data DataFrame.
    """
    # Merge home team stats
    merged_data = pd.merge(schedules, team_stats, how='left',
                           left_on=['home_team', 'season', 'week','season_type'],
                           right_on=['recent_team', 'season', 'week','season_type'])

    # Merge away team stats with a suffix to distinguish columns
    merged_data = pd.merge(merged_data, team_stats, how='left',
                           left_on=['away_team', 'season', 'week','season_type'],
                           right_on=['recent_team', 'season', 'week','season_type'],
                           suffixes=('_home', '_away'))

    return merged_data

def calculate_game_related_metrics(merged_data):
    """Calculates additional game-related fields and cleans up the DataFrame."""
    # Calculate game outcome and total points
    merged_data['outcome'] = (merged_data['home_score'] > merged_data['away_score']).astype(int)
    merged_data['game_total_points'] = merged_data['home_score'] + merged_data['away_score']

    # Calculate score difference
    merged_data['score_diff'] = merged_data['home_score'] - merged_data['away_score']

    # Drop unnecessary columns
    merged_data.drop(columns=['recent_team_home', 'recent_team_away'], inplace=True)

    return merged_data

def save_to_sqlite(data, db_path, table_name="weekly_game_data"):
    """Saves the transformed data to a new table in SQLite."""
    conn = sqlite3.connect(db_path)
    data.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    print(f"Data successfully saved to {table_name} table.")

if __name__ == "__main__":
    db_path = "nfl_data.db"  # Path to your SQLite database

    # Step 1: Load data from SQLite
    team_stats, schedules = load_data_from_sqlite(db_path)

    # Step 2: Merge data
    merged_data = merge_team_stats_with_schedules(schedules, team_stats)

    # Step 3: Calculate game-related metrics
    weekly_game_data = calculate_game_related_metrics(merged_data)

    # Step 4: Save the final data to a new SQLite table
    save_to_sqlite(weekly_game_data, db_path, table_name="weekly_game_data")

    # Optionally display the final DataFrame
    print(weekly_game_data.head())


