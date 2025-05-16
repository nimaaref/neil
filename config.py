from datetime import datetime
import nfl_data_py as nfl

class NFLConfig:
    def __init__(self, target_week=None, training_cutoff_week = None):
        # Season configuration
        self.SEASONS = [2024, 2023, 2022, 2021, 2020, 2019, 2018]  # All seasons to pull
        self.CURRENT_SEASON = 2024
        self.SEASON_START_DATE = datetime(2024, 9, 5)
        
        # Week configuration (use passed values if available)
        self.TARGET_WEEK = target_week if target_week is not None else 13
        self.TRAINING_CUTOFF_WEEK = training_cutoff_week if training_cutoff_week is not None else 12

        
        # Database configuration
        self.DB_PATH = "nfl_data.db"
        self.WEEKLY_SCORES_TABLE = "weekly_scores"
        self.SCHEDULES_TABLE = "schedules"
        self.BASE_MODEL_TABLE = 'base_model'
        self.STAGING_WEEKLY_STATS_TABLE = "stg_weekly_scores"
        self.STAGING_SCHEDULES_TABLE = "stg_schedules"
        self.RF_TRAINING_DATA = 'rf_training_data'
        self.RF_PREDICTION_DATA = 'rf_prediction_table'

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

        self.TRAINING_COLUMNS = ['season', 'week','game_type_num', 'home_rest', 'away_rest', 'home_moneyline', 'away_moneyline', 'spread_line', 'home_spread_odds', 'away_spread_odds', 'total_line', 'under_odds', 'over_odds', 'div_game', 'passing_yards_home', 'rushing_yards_home', 'receiving_yards_home', 'sacks_home', 'rushing_fumbles_home', 'receiving_fumbles_home', 'sack_fumbles_home', 'interceptions_home', 'targets_home', 'carries_home', 'receptions_home',  'passing_yards_away', 'rushing_yards_away', 'receiving_yards_away', 'interceptions_away',  'sacks_away', 'rushing_fumbles_away', 'receiving_fumbles_away', 'sack_fumbles_away',  'carries_away', 'targets_away', 'receptions_away']


 #['season', 'week','game_type_num', 'home_rest', 'away_rest', 'home_moneyline', 'away_moneyline', 'spread_line', 'home_spread_odds', 'away_spread_odds', 'total_line', 'under_odds', 'over_odds', 'div_game', 'passing_yards_home', 'rushing_yards_home', 'receiving_yards_home', 'sacks_home', 'special_teams_tds_home', 'rushing_fumbles_home', 'receiving_fumbles_home', 'sack_fumbles_home', 'interceptions_home', 'passing_tds_home', 'rushing_tds_home', 'receiving_tds_home', 'targets_home', 'carries_home', 'receptions_home', 'passing_2pt_conversions_home', 'rushing_2pt_conversions_home', 'touchdown_points_home', 'kicking_points_home', 'passing_yards_away', 'rushing_yards_away', 'receiving_yards_away', 'passing_tds_away', 'rushing_tds_away', 'interceptions_away', 'special_teams_tds_away', 'sacks_away', 'rushing_fumbles_away', 'receiving_fumbles_away', 'sack_fumbles_away', 'receiving_tds_away', 'carries_away', 'targets_away', 'receptions_away', 'passing_2pt_conversions_away', 'rushing_2pt_conversions_away', 'touchdown_points_away', 'kicking_points_away']

        # Game type mapping
        self.GAME_TYPE_MAPPING = {
            'REG': 'REG',
            'POST': 'POST',
            'WC': 'POST',
            'DIV': 'POST',
            'CON': 'POST',
            'SB': 'POST'
        }
    
  