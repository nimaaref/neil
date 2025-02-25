import sys
import os
import pandas as pd 
import numpy as np 

# Dynamically add the parent directory (neil/) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import sqlite3
import nfl_data_py as nfl
from config import NFLConfig
from utils import import_data
config = NFLConfig()


if __name__ == "__main__":
    conn = sqlite3.connect(config.DB_PATH)

    schedule_data = import_data(conn, config.STAGING_SCHEDULES_TABLE, config.SCHEDULE_ENDPOINT,config.SCHEDULES_TABLE)

    conn.close()