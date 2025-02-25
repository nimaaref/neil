import sys
import os

# Dynamically add the parent directory (neil/) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import NFLConfig
from utils import load_existing_nfl_data, load_to_sqlite,calculate_current_week

import sqlite3


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

def transform_weekly_scores(conn):
    """
    Main function to import, transform, and load weekly NFL data into SQLite.
    """
    current_week = calculate_current_week()
    
    weekly_score_data = load_existing_nfl_data(conn, config.STAGING_WEEKLY_STATS_TABLE)
    # Step 3: Select relevant columns
    weekly_score_data = select_and_clean_columns(weekly_score_data, config.WEEKLY_STATS_COLUMNS)

    # Step 4: Aggregate data by team, season, and week
    weekly_score_data = aggregate_team_stats(weekly_score_data)

    # Step 5: Calculate derived metrics
    weekly_score_data = calculate_derived_metrics(weekly_score_data)

    # Step 6: Load into SQLite
    load_to_sqlite(weekly_score_data, conn, config.WEEKLY_SCORES_TABLE)

    
    print(f"Weekly scores transformed up to and uploaded to {config.WEEKLY_SCORES_TABLE}.")
    
    return weekly_score_data

if __name__ == "__main__":
    conn = sqlite3.connect(config.DB_PATH)

    weekly_score_data = transform_weekly_scores(conn)

    conn.close()