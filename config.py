from datetime import datetime
import nfl_data_py as nfl

class NFLConfig:
    def __init__(self):
        # Season configuration
        self.SEASONS = [2024, 2023, 2022, 2021, 2020, 2019, 2018]  # All seasons to pull
        self.CURRENT_SEASON = 2024
        self.SEASON_START_DATE = datetime(2024, 9, 5)
        
        # Week configuration
        self.TARGET_WEEK = 17      # Week to predict
        self.TRAINING_CUTOFF_WEEK = 16  # Use data up to this week for training
        
        # Database configuration
        self.DB_PATH = "nfl_data.db"
        self.WEEKLY_SCORES_TABLE = "weekly_scores"
        self.SCHEDULES_TABLE = "schedules"
        self.BASE_MODEL_TABLE = 'base_model'
        self.STAGING_WEEKLY_STATS_TABLE = "stg_weekly_scores"
        self.STAGING_SCHEDULES_TABLE = "stg_schedules"

        # Endpoint configurations
        self.SCHEDULE_ENDPOINT = nfl.import_schedules
        self.WEEKLY_DATA_ENPOINT = nfl.import_weekly_data
        
        # Column configurations
        self.WEEKLY_STATS_COLUMNS = [
            'recent_team', 'season', 'season_type', 'week', 
            'passing_yards', 'rushing_yards', 'passing_tds', 
            'rushing_tds', 'interceptions', 'sacks', 
            'rushing_fumbles', 'receiving_tds', 'special_teams_tds',
            'carries', 'targets', 'receptions', 'receiving_yards', 
            'passing_2pt_conversions', 'rushing_2pt_conversions'
        ]
        
        self.SCHEDULE_COLUMNS = [
            'game_id', 'season', 'season_type', 'week', 
            'home_team', 'away_team', 'home_score', 'away_score',
            'away_rest', 'home_rest', 'away_moneyline', 
            'home_moneyline', 'spread_line', 'away_spread_odds',
            'home_spread_odds', 'total_line', 'under_odds',
            'over_odds', 'div_game'
        ]

        self.TRAINING_COLUMNS = ['home_score', 'away_score', 'away_rest', 'home_rest', 'away_moneyline', 'home_moneyline', 'spread_line',
                                  'away_spread_odds', 'home_spread_odds', 'total_line', 'under_odds', 'over_odds', 'div_game', 'passing_yards_home', 'rushing_yards_home', 'sacks_home', 'carries_home', 'targets_home',
                                    'receptions_home', 'passing_tds_home', 'rushing_tds_home', 'receiving_tds_home', 'interceptions_home', 'rushing_fumbles_home', 'special_teams_tds_home', 'receiving_yards_home', 'passing_yards_away', 
                                    'rushing_yards_away', 'sacks_away', 'carries_away', 'targets_away', 'receptions_away', 'passing_tds_away', 'rushing_tds_away', 'receiving_tds_away', 'interceptions_away', 'rushing_fumbles_away', 
                                    'special_teams_tds_away', 'receiving_yards_away','total_score_away','total_score_home','total_yards_offense_away','total_yards_offense_home', 'turnovers_offense_away','turnovers_offense_home']
        
        # Game type mapping
        self.GAME_TYPE_MAPPING = {
            'REG': 'REG',
            'POST': 'POST',
            'WC': 'POST',
            'DIV': 'POST',
            'CON': 'POST',
            'SB': 'POST'
        }
    
  