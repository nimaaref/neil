# import_weekly_data.py
import pandas as pd
import sqlite3
from datetime import datetime
import nfl_data_py as nfl  # Assuming nfl_data_py is the library used to pull data


def calculate_current_week(start_date):
    """Calculates the current NFL week based on the season start date."""
    today = datetime.now()
    days_difference = (today - start_date).days
    return max(1, (days_difference // 7) + (1 if days_difference % 7 else 0))

def import_nfl_data(seasons):
    """Imports weekly NFL data for the specified seasons."""
    return nfl.import_weekly_data(seasons)

def select_and_clean_columns(data, columns):
    """Selects relevant columns and fills NaN values with 0."""
    return data[columns].fillna(0)

def aggregate_team_stats(data):
    """Groups by team, season, and week, and sums values for each group."""
    return data.groupby(['recent_team', 'season','season_type', 'week']).sum().reset_index()

def calculate_derived_metrics(stats):
    """Calculates touchdowns, 2pt conversions, total score, total yards offense, and turnovers."""
    stats['touchdowns'] = 6 * (stats['passing_tds'] + stats['rushing_tds'] + stats['special_teams_tds'])
    stats['2pt_conversions'] = 2 * (stats['passing_2pt_conversions'] + stats['rushing_2pt_conversions'])
    stats['total_score'] = stats['touchdowns'] + stats['2pt_conversions']
    stats['total_yards_offense'] = stats['passing_yards'] + stats['rushing_yards']
    stats['turnovers_offense'] = stats['interceptions'] + stats['rushing_fumbles']

    # Drop unnecessary columns if they are not needed anymore
    return stats.drop(columns=['passing_2pt_conversions', 'rushing_2pt_conversions', 'touchdowns', '2pt_conversions'])

def load_to_sqlite(data, conn, table_name="team_stats"):
    """Loads the DataFrame into an SQLite table."""
    data.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Data successfully loaded into {table_name} table.")

def import_weekly_data(conn):
    """
    Main function to import, transform, and load weekly NFL data into SQLite.
    """
    nfl_start_date = datetime(2024, 9, 5)
    current_week = calculate_current_week(nfl_start_date)

    # Import weekly data
    weekly_data = import_nfl_data([2024, 2023, 2022, 2021, 2020, 2019, 2018])

    # Select relevant columns for calculating team scores
    relevant_columns = [
        'recent_team', 'season','season_type', 'week', 'passing_yards', 'rushing_yards',
        'passing_tds', 'rushing_tds', 'interceptions', 'sacks', 'rushing_fumbles',
        'receiving_tds', 'special_teams_tds', 'carries', 'targets', 'receptions',
        'receiving_yards', 'passing_2pt_conversions', 'rushing_2pt_conversions'
    ]
    score_data = select_and_clean_columns(weekly_data, relevant_columns)

    # Aggregate data by team, season, and week
    team_stats = aggregate_team_stats(score_data)

    # Calculate derived metrics
    team_stats = calculate_derived_metrics(team_stats)

    # Load the transformed data into SQLite
    load_to_sqlite(team_stats, conn)

    print(f"Weekly data for team stats up to week {current_week} imported successfully.")
    return team_stats  # Optionally, return team_stats for further use in the program


if __name__ == "__main__":
    # Create or connect to the SQLite database
    conn = sqlite3.connect("nfl_data.db")

    # Run the import and processing pipeline
    team_stats = import_weekly_data(conn)

    # Close the database connection
    conn.close()
