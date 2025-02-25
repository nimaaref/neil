import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import nfl_data_py as nfl  # Assuming nfl_data_py is the library used to pull data

def import_schedules_data(seasons):
    """Imports weekly NFL schedules for the specified seasons."""
    return nfl.import_schedules(seasons)

def calculate_current_week(start_date):
    """Calculates the current NFL week based on the season start date."""
    today = datetime.now()
    days_difference = (today - start_date).days
    current_week = max(1, (days_difference // 7) + (1 if days_difference % 7 else 0))
    return current_week

def select_filter_schedules(schedules, current_week):
    """Selects and filters relevant schedule data based on season and week."""
    schedules = schedules[(schedules['season'] <= 2024) & (schedules['week'] <= current_week)]
    schedules['season_type'] = np.where(schedules['game_type'] != 'REG', 'POST', 'REG')
    schedules = schedules[['game_id', 'season','season_type', 'week', 'home_team', 'away_team', 'home_score', 'away_score',
                           'away_rest', 'home_rest', 'away_moneyline', 'home_moneyline', 'spread_line',
                           'away_spread_odds', 'home_spread_odds', 'total_line', 'under_odds', 'over_odds', 'div_game']]

    return schedules

def load_to_sqlite(data, conn, table_name="schedules"):
    """Loads the DataFrame into an SQLite table."""
    data.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Data successfully loaded into {table_name} table.")

def import_schedules(conn):
    """
    Main function to import, transform, and load NFL schedule data into SQLite.
    """
    nfl_start_date = datetime(2024, 9, 5)
    current_week = calculate_current_week(nfl_start_date)

    # Import schedules data
    schedules_data = import_schedules_data([2024, 2023, 2022, 2021, 2020, 2019, 2018])

    # Select and filter schedule data
    schedules_data = select_filter_schedules(schedules_data, current_week)

    # Calculate additional metrics
    schedules_data['outcome'] = (schedules_data['home_score'] > schedules_data['away_score']).astype(int)
    schedules_data['game_total_points'] = schedules_data['home_score'] + schedules_data['away_score']

    # Load data into SQLite
    load_to_sqlite(schedules_data, conn)

    print(f"Schedule data for team stats up to week {current_week} imported successfully.")

if __name__ == "__main__":
    # Create or connect to the SQLite database
    conn = sqlite3.connect("nfl_data.db")

    # Run the import and processing pipeline
    import_schedules(conn)

    # Close the database connection
    conn.close()
