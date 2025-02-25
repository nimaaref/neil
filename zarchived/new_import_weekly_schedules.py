# import_weekly_schedules.py
import pandas as pd
import numpy as np
import sqlite3
import nfl_data_py as nfl
from config import NFLConfig
from utils import import_nfl_data
from utils import load_existing_nfl_data
from utils import calculate_current_week
from utils import schedule_filter
from utils import load_to_sqlite

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


def import_schedules(conn):
    """Main function to import, transform, and load NFL schedule data."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type = 'table' AND name = '{config.STAGING_SCHEDULES_TABLE}'") 
    table_exists = cursor.fetchone() is not None

    if not table_exists:
        print(f"Table {config.STAGING_SCHEDULES_TABLE} does not exist. Creating it now...")
        schedule_data = import_nfl_data(config.SEASONS, config.SCHEDULE_ENDPOINT, conn, config.STAGING_SCHEDULES_TABLE)
    else:
        schedule_data = load_existing_nfl_data(conn, config.STAGING_SCHEDULES_TABLE)
        if schedule_data.empty:
            print("No existing data found. Importing new data...")
            schedule_data = import_nfl_data(config.SEASONS, conn, config.STAGING_SCHEDULES_TABLE)
    
    schedule_data = select_filter_schedules(schedule_data, config)
    schedule_data = calculate_game_metrics(schedule_data)
    load_to_sqlite(schedule_data, conn, config.SCHEDULES_TABLE)
    print(f"Schedule data imported successfully up to week {calculate_current_week()} of {config.CURRENT_SEASON}")
    return schedule_data



if __name__ == "__main__":
    conn = sqlite3.connect(config.DB_PATH)

    schedule_data = import_schedules(conn)

    conn.close()