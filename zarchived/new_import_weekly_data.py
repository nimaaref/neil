import pandas as pd
import sqlite3
from datetime import datetime
import nfl_data_py as nfl
from config import NFLConfig
from utils import calculate_current_week
from utils import import_nfl_data
from utils import load_existing_nfl_data
from utils import load_to_sqlite

config = NFLConfig()

def select_and_clean_columns(data, columns):
    """Selects relevant columns and fills NaN values with 0."""
    return data[columns].fillna(0)

def aggregate_team_stats(data):
    """Groups by team, season, and week, and sums values for each group."""
    return data.groupby(['recent_team', 'season', 'season_type', 'week']).sum().reset_index()

def calculate_derived_metrics(stats):
    """Calculates touchdowns, 2pt conversions, total score, total yards offense, and turnovers."""
    stats['touchdowns'] = 6 * (stats['passing_tds'] + stats['rushing_tds'] + stats['special_teams_tds'])
    stats['2pt_conversions'] = 2 * (stats['passing_2pt_conversions'] + stats['rushing_2pt_conversions'])
    stats['total_score'] = stats['touchdowns'] + stats['2pt_conversions']
    stats['total_yards_offense'] = stats['passing_yards'] + stats['rushing_yards']
    stats['turnovers_offense'] = stats['interceptions'] + stats['rushing_fumbles']

    return stats

def import_weekly_data(conn):
    """
    Main function to import, transform, and load weekly NFL data into SQLite.
    """
    current_week = calculate_current_week()

    # Step 1: Ensure the table exists before reading
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{config.STAGING_WEEKLY_STATS_TABLE}'")
    table_exists = cursor.fetchone() is not None

    if not table_exists:
        print(f"Table {config.WEEKLY_STATS_TABLE} does not exist. Creating it now...")
        weekly_data = import_nfl_data(config.SEASONS, conn, config.STAGING_WEEKLY_STATS_TABLE)  # Import all available data
    else:
        # Step 2: Load existing data
        weekly_data = load_existing_nfl_data(conn, config.STAGING_WEEKLY_STATS_TABLE)
        if weekly_data.empty:
            print("No existing data found. Importing new data...")
            weekly_data = import_nfl_data(config.SEASONS, conn, config.STAGING_WEEKLY_STATS_TABLE)

    # Step 3: Select relevant columns
    score_data = select_and_clean_columns(weekly_data, config.WEEKLY_STATS_COLUMNS)

    # Step 4: Aggregate data by team, season, and week
    team_stats = aggregate_team_stats(score_data)

    # Step 5: Calculate derived metrics
    team_stats = calculate_derived_metrics(team_stats)

    # Step 6: Load into SQLite
    load_to_sqlite(team_stats, conn, config.WEEKLY_STATS_TABLE)

    print(f"Weekly data up to week {current_week} imported successfully.")
    return team_stats

if __name__ == "__main__":
    conn = sqlite3.connect(config.DB_PATH)

    # Run the data import
    team_stats = import_weekly_data(conn)

    conn.close()
