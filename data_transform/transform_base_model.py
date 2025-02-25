import sys
import os
import pandas as pd 
import numpy as np 

# Dynamically add the parent directory (neil/) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import NFLConfig
from utils import load_existing_nfl_data, load_to_sqlite,calculate_current_week

import sqlite3
config = NFLConfig()


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

    print(f"Base model data caculated succesfully. Data loaded into {config.BASE_MODEL_TABLE}.")

    return merged_data

if __name__ == "__main__":
    conn = sqlite3.connect(config.DB_PATH)
    config = NFLConfig()  # Load config

    # Step 1: Load data from SQLite
    schedules = load_existing_nfl_data(conn, config.SCHEDULES_TABLE)   

    team_stats  = load_existing_nfl_data(conn, config.WEEKLY_SCORES_TABLE)
    # Step 2: Merge data
    base_data = merge_team_stats_with_schedules(schedules, team_stats)

    # Step 3: Calculate game-related metrics
    base_data = calculate_game_related_metrics(base_data)

    # Step 4: Save the final data to a new SQLite table
    load_to_sqlite(base_data, conn,config.BASE_MODEL_TABLE )

    conn.close()