import sqlite3
from config import NFLConfig
from utils import import_data
config = NFLConfig()


if __name__ == "__main__":
    conn = sqlite3.connect(config.DB_PATH)

    schedule_data = import_data(conn, config.STAGING_WEEKLY_STATS_TABLE, config.WEEKLY_DATA_ENPOINT,config.WEEKLY_SCORES_TABLE)

    conn.close()