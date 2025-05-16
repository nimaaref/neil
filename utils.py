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


def run_sql_file_and_save_to_table(conn, sql_file_path, output_table_name, if_exists="replace", verbose=True):
    """
    Reads a SQL query from a file, executes it, and saves the result to a table.
    
    Args:
        conn: SQLite connection.
        sql_file_path: Path to the .sql file.
        output_table_name: Name for the output table.
        if_exists: What to do if the table exists ("replace", "append", or "fail").
        verbose: If True, prints status messages.
    """
    # Read SQL file
    with open(sql_file_path, "r", encoding="utf-8") as file:
        query = file.read().strip()
    
    if verbose:
        print("Executing SQL from:", sql_file_path)
    
    # Execute query and load result into DataFrame
    df = pd.read_sql_query(query, conn)
    
    # Save the result to the target table
    df.to_sql(output_table_name, conn, if_exists=if_exists, index=False)
    
    if verbose:
        print(f"Table '{output_table_name}' created with {len(df)} rows.")
        print(df.head())
    
    return df


def build_season_week_filter(config, lag_window=3):
    """
    Builds a SQL-compatible WHERE clause for weeks leading up to the target week.
    Handles cross-season logic for early weeks.
    """
    current_season = config.CURRENT_SEASON
    target_week = config.TARGET_WEEK

    if target_week == 1:
        return "(season = {prev_season} AND week IN (16, 17, 18))".format(prev_season=current_season - 1)
    elif target_week == 2:
        return ("(season = {prev_season} AND week IN (17, 18)) "
                "OR (season = {current_season} AND week = 1)").format(
                    prev_season=current_season - 1, current_season=current_season)
    elif target_week == 3:
        return ("(season = {prev_season} AND week = 18) "
                "OR (season = {current_season} AND week IN (1, 2))").format(
                    prev_season=current_season - 1, current_season=current_season)
    else:
        lag_weeks = list(range(target_week - lag_window, target_week))
        lag_week_str = ", ".join(str(w) for w in lag_weeks)
        return f"(season = {current_season} AND week IN ({lag_week_str}))"
