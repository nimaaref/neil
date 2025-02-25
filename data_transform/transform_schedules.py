import sys
import os
import pandas as pd 
import numpy as np 

# Dynamically add the parent directory (neil/) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import NFLConfig
from utils import load_existing_nfl_data, schedule_filter, load_to_sqlite

import sqlite3

config = NFLConfig()


def select_filter_schedules(schedules, config):
    """Selects and filters relevant schedule data based on season and week."""
    try:
        # Apply season and week filters using pandas query
        schedules = schedules.query(schedule_filter(schedules))
        
        # Map game types to season types
        schedules['season_type'] = schedules['game_type'].map(config.GAME_TYPE_MAPPING)
        
        # Select relevant columns from config
        schedules = schedules[config.SCHEDULE_COLUMNS]
        
        return schedules
    except Exception as e:
        print(f"Error in filtering schedules: {str(e)}")
        print(f"Filter being applied: {schedule_filter(schedules)}")
        raise

def calculate_game_metrics(schedules):
    """Calculates additional game metrics."""
    schedules['outcome'] = (schedules['home_score'] > schedules['away_score']).astype(int)
    schedules['game_total_points'] = schedules['home_score'] + schedules['away_score']
    return schedules

def transform_schedules(conn):
    schedule_data = load_existing_nfl_data( conn, config.STAGING_SCHEDULES_TABLE)

    schedule_data = select_filter_schedules(schedule_data, config)
    schedule_data = calculate_game_metrics(schedule_data)
    load_to_sqlite(schedule_data,conn,config.SCHEDULES_TABLE)
    print(f"Schedule data transformed sucessfuly and loaded into {config.SCHEDULES_TABLE}.")
    return schedule_data

if __name__ == "__main__":
    conn = sqlite3.connect(config.DB_PATH)

    schedule_data = transform_schedules(conn)

    conn.close()