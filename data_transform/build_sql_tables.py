import sys
import os
import pandas as pd 
import numpy as np 

# Dynamically add the parent directory (neil/) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


import sqlite3
from config import NFLConfig
from utils import run_sql_file_and_save_to_table



config = NFLConfig()

with sqlite3.connect(config.DB_PATH) as conn:
    run_sql_file_and_save_to_table(conn, "data_transform/int_schedules.sql", "int_schedules")
    run_sql_file_and_save_to_table(conn, "data_transform/int_weekly_scores.sql", "int_weekly_score")
    run_sql_file_and_save_to_table(conn, "data_transform/int_team_stats.sql", "int_team_stats")
    run_sql_file_and_save_to_table(conn, "data_transform/rf_training_data.sql", "rf_training_data")
    run_sql_file_and_save_to_table(conn, "data_transform/team_stats.sql", "team_stats")

