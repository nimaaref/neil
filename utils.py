# data_utils.py
import pandas as pd
import nfl_data_py as nfl
import sqlite3
from datetime import datetime
from config import NFLConfig

config = NFLConfig()

def import_nfl_data(seasons, endpoint, conn, table_name):
    """
    Imports weekly NFL data for the specified endpoint and saves it to an SQLite table.
    
    Args:
        seasons (list): List of NFL seasons to import.
        endpoint (function): NFL API function to call (e.g., nfl.import_weekly_data).
        conn (sqlite3.Connection): SQLite connection object.
        table_name (str): Name of the SQLite table to store data.
        
    Returns:
        pd.DataFrame: The DataFrame of imported NFL data.
    """
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    table_exists = cursor.fetchone() is not None

    if table_exists:
        existing_data = pd.read_sql(f"SELECT DISTINCT season, week FROM {table_name}", conn)
        new_data = endpoint(seasons)
        new_data = new_data.merge(existing_data, on=['season', 'week'], how='left', indicator=True)
        new_data = new_data[new_data['_merge'] == 'left_only'].drop(columns=['_merge'])
    else:
        new_data = endpoint(seasons)

    if not new_data.empty:
        new_data.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Added {len(new_data)} new records to {table_name}.")
    else:
        print("No new data to add.")

    return new_data

def calculate_current_week():
    """Calculates the current NFL week based on the season start date."""
    today = datetime.now()
    days_difference = (today - config.SEASON_START_DATE).days
    return max(1, (days_difference // 7) + (1 if days_difference % 7 else 0))

def load_to_sqlite(data, conn, table_name):
    """Loads the DataFrame into an SQLite table."""
    data.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Data successfully loaded into {table_name} table.")


def load_existing_nfl_data(conn, table_name, where_clause=None):
    """
    Loads existing NFL weekly data from SQLite with an optional WHERE clause.

    Args:
        conn (sqlite3.Connection): SQLite connection object.
        table_name (str): Name of the SQLite table to read from.
        where_clause (str, optional): Optional WHERE clause (e.g., "season = 2024").

    Returns:
        pd.DataFrame: The DataFrame of existing NFL weekly data.
    """
    try:
        query = f"SELECT * FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        return pd.read_sql(query, conn)
    except Exception as e:
        print(f"Error loading data from {table_name}: {e}")
        return pd.DataFrame()

    
def training_data_filter(config):
    """Pandas query string for training data.
        Includes all seasons before the current season. 
        Includes the current season up to the TRAINING_CUTOFF_WEEK.
        Excludes games beyond the training cutoff week in the current_season."""
    return f"(season < {config.CURRENT_SEASON}) | ((season == {config.CURRENT_SEASON}) & (week <= {config.TRAINING_CUTOFF_WEEK}))"
    
def prediction_week_filter(config):
    """Pandas query string for prediction week.
        Filters only games for the target week in the current season. """
    return f"season == {config.CURRENT_SEASON} & week == {config.TARGET_WEEK}"   

def schedule_filter(config):
    """Pandas query string for schedule data.
        Includes all past seasons and filters only up to the current week in the current season. 
        Excludes future weeks. """
    return f"season <= {config.CURRENT_SEASON} & week <= {calculate_current_week()}"


def import_data(conn, table_name, endpoint=None, final_table=None):
    """
    General function to import NFL data and save it into SQLite.
    
    Args:
        conn (sqlite3.Connection): SQLite connection object.
        table_name (str): Name of the staging table.
        endpoint (function, optional): API function to call, e.g., nfl.import_schedules.
        final_table (str, optional): Name of the final table to store data (if different from table_name).
    
    Returns:
        pd.DataFrame: The DataFrame of imported NFL data.
    """
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    table_exists = cursor.fetchone() is not None

    if not table_exists:
        print(f"Table {table_name} does not exist. Creating it now...")
        data = import_nfl_data(config.SEASONS, endpoint, conn, table_name) if endpoint else import_nfl_data(config.SEASONS, conn, table_name)
    else:
        data = load_existing_nfl_data(conn, table_name)
        if data.empty:
            print(f"No existing data found for {table_name}. Importing new data...")
            data = import_nfl_data(config.SEASONS, endpoint, conn, table_name) if endpoint else import_nfl_data(config.SEASONS, conn, table_name)

    load_to_sqlite(data, conn, final_table or table_name)

    if final_table:
        print(f"Data imported successfully into {final_table} (up to week {calculate_current_week()} of {config.CURRENT_SEASON})")
    
    return data
