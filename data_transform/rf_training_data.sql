--predictors are removed from this query: home/away score, outcome, total

WITH score_data AS (
    SELECT
        s.season, 
        s.week,
        s.home_team,
        s.away_team, 
        SUM(s.home_score) AS home_score, 
        SUM(s.away_score) AS away_score, 
        IFNULL(SUM(home_ws.touchdown_points), 0) AS home_touchdown_points, 
        IFNULL(SUM(away_ws.touchdown_points), 0) AS away_touchdown_points,
        SUM(s.home_score) - IFNULL(SUM(home_ws.touchdown_points), 0) AS home_kicking_points, 
        SUM(s.away_score) - IFNULL(SUM(away_ws.touchdown_points), 0) AS away_kicking_points
    FROM int_schedules AS s
    LEFT JOIN int_team_stats AS home_ws 
        ON home_ws.recent_team = s.home_team
        AND home_ws.week = s.week 
        AND home_ws.season = s.season
        AND home_ws.season_type = s.game_type 
    LEFT JOIN int_team_stats AS away_ws
        ON away_ws.recent_team = s.away_team
        AND away_ws.week = s.week 
        AND away_ws.season = s.season
        AND away_ws.season_type = s.game_type 
    GROUP BY
        s.season, 
        s.week,
        s.home_team,
        s.away_team
)

, kicking_stats AS (
    SELECT 
        season,
        week,
    	team,
    	kicking_points
    FROM (
        SELECT season, week, home_team AS team, home_kicking_points AS kicking_points
        FROM score_data
        UNION ALL
        SELECT season, week, away_team AS team, away_kicking_points AS kicking_points
        FROM score_data
    )
) 

SELECT 
    s.season, 
    s.week, 
    s.home_team, 
    s.away_team,
    s.outcome,
    s.GAME_TYPE_NUM as game_type_num,
    s.home_rest, 
    s.away_rest, 
    s.home_moneyline, 
    s.away_moneyline, 
    s.spread_line, 
    s.home_spread_odds, 
    s.away_spread_odds, 
    s.total_line, 
    s.under_odds, 
    s.over_odds, 
    s.div_game,
    home_stats.passing_yards as passing_yards_home, 
    home_stats.rushing_yards as rushing_yards_home, 
    home_stats.receiving_yards as receiving_yards_home,
    home_stats.sacks as sacks_home, 
   -- home_stats.special_teams_tds as special_teams_tds_home, 
    home_stats.rushing_fumbles as rushing_fumbles_home,
    home_stats.receiving_fumbles as receiving_fumbles_home,
    home_stats.sack_fumbles as sack_fumbles_home,
    home_stats.interceptions as interceptions_home,
    /**
    home_stats.passing_tds as passing_tds_home, 
    home_stats.rushing_tds as rushing_tds_home, 
    home_stats.receiving_tds as receiving_tds_home,
    **/ 
    home_stats.targets as targets_home,
    home_stats.carries as carries_home, 
    home_stats.receptions as receptions_home, 
    /**
    home_stats.passing_2pt_conversions as passing_2pt_conversions_home, 
    home_stats.rushing_2pt_conversions as rushing_2pt_conversions_home, 
    home_stats.touchdown_points as touchdown_points_home,
    hks.kicking_points as kicking_points_home,
    **/
    away_stats.passing_yards as passing_yards_away, 
    away_stats.rushing_yards as rushing_yards_away, 
    away_stats.receiving_yards as receiving_yards_away, 
    /**
    away_stats.passing_tds as passing_tds_away, 
    away_stats.rushing_tds as rushing_tds_away,
    **/ 
    away_stats.interceptions as interceptions_away, 
   -- away_stats.special_teams_tds as special_teams_tds_away, 
    away_stats.sacks as sacks_away, 
    away_stats.rushing_fumbles as rushing_fumbles_away, 
    away_stats.receiving_fumbles as receiving_fumbles_away,
    away_stats.sack_fumbles as sack_fumbles_away,
    --away_stats.receiving_tds as receiving_tds_away, 
    away_stats.carries as carries_away, 
    away_stats.targets as targets_away, 
    away_stats.receptions as receptions_away
    --away_stats.passing_2pt_conversions as passing_2pt_conversions_away, 
    --away_stats.rushing_2pt_conversions as rushing_2pt_conversions_away, 
    --away_stats.touchdown_points as touchdown_points_away, 
    --aks.kicking_points as kicking_points_away
FROM int_schedules s
LEFT JOIN int_team_stats AS home_stats
    ON s.home_team = home_stats.recent_team
    AND s.season = home_stats.season
    AND s.week = home_stats.week
LEFT JOIN int_team_stats AS away_stats
    ON s.away_team = away_stats.recent_team
    AND s.season = away_stats.season
    AND s.week = away_stats.week
 LEFT JOIN 
 	kicking_stats as hks 
 ON 
 	 home_stats.recent_team = hks.team
 	AND s.season = hks.season
 	AND s.week = hks.week 
 LEFT JOIN 
 	kicking_stats as aks
 ON 
 	away_stats.recent_team = aks.team 
 	AND s.season = aks.season
 	AND s.week = aks.week;
