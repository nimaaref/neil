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
, kicking_avg AS (
    SELECT 
        team,
        AVG(kicking_points) AS avg_kicking_points
    FROM (
        SELECT home_team AS team, home_kicking_points AS kicking_points
        FROM score_data
        WHERE {season_week_filter}

        UNION ALL

        SELECT away_team AS team, away_kicking_points AS kicking_points
        FROM score_data
        WHERE {season_week_filter}
    )
    GROUP BY team
) 
, schedule_data AS (
    SELECT 
        game_id,
        home_team, 
        away_team, 
        season, 
        week, 
        GAME_TYPE_NUM, 
        div_game, 
        home_rest, 
        away_rest, 
        home_moneyline, 
        away_moneyline, 
        away_spread_odds, 
        home_spread_odds, 
        over_odds, 
        under_odds,
        spread_line, 
        total_line
    FROM int_schedules
    WHERE season = {season} AND week = {target_week}
),

weekly_stats AS (
    SELECT 
    	recent_team,
        AVG(passing_yards) AS passing_yards_avg, 
        AVG(rushing_yards) AS rushing_yards_avg, 
        AVG(receiving_yards) AS receiving_yards_avg,
        SUM(passing_tds) AS passing_tds, 
        SUM(rushing_tds) AS rushing_tds, 
        SUM(special_teams_tds) AS special_teams_tds, 
        SUM(interceptions) AS interceptions, 
        SUM(sacks) AS sacks, 
        SUM(rushing_fumbles)  as rushing_fumbles,
        SUM(receiving_fumbles) as receiving_fumbles, 
        SUM(sack_fumbles) AS sack_fumbles, 
        AVG(receptions) AS avg_receptions, 
        AVG(targets) AS avg_targets, 
        AVG(carries) AS avg_carries,
        SUM(passing_2pt_conversions) AS passing_2pt_conversions,
        SUM(rushing_2pt_conversions) AS rushing_2pt_conversions,
        AVG(touchdown_points) as touchdown_points,
        AVG(receiving_tds) as receiving_tds
    FROM int_team_stats
    WHERE {season_week_filter}
    GROUP BY recent_team
)

SELECT
     	sd.game_id,
        sd.season, 
        sd.week, 
        sd.home_team, 
        sd.away_team, 
        sd.GAME_TYPE_NUM AS game_type_num,
        sd.home_rest, 
        sd.away_rest,
        sd.home_moneyline, 
        sd.away_moneyline,
        sd.spread_line, 
        sd.home_spread_odds, 
        sd.away_spread_odds, 
        sd.total_line, 
        sd.under_odds,
        sd.over_odds, 
        sd.div_game,     
       	hs.passing_yards_avg as passing_yards_home, 
       	hs.rushing_yards_avg as rushing_yards_home, 
       	hs.receiving_yards_avg as receiving_yards_home ,
      	hs.sacks as sacks_home, 
        --hs.special_teams_tds as special_teams_tds_home, 
      	hs.rushing_fumbles as rushing_fumbles_home, 
        hs.receiving_fumbles as receiving_fumbles_home,
        hs.sack_fumbles as sack_fumbles_home,

        hs.interceptions as interceptions_home, 
       	/**
        hs.passing_tds as passing_tds_home, 
       	hs.rushing_tds as rushing_tds_home, 
        hs.receiving_tds as receiving_tds_home,
        **/
        hs.avg_targets as targets_home, 
        hs.avg_carries as carries_home,
        hs.avg_receptions as receptions_home, 
        --hs.passing_2pt_conversions as passing_2pt_conversions_home,
        --hs.rushing_2pt_conversions as rushing_2pt_conversions_home,
        --hs.touchdown_points as touchdown_points_home,
        --hka.avg_kicking_points as kicking_points_home, 
        aws.passing_yards_avg as passing_yards_away, 
       	aws.rushing_yards_avg as rushing_yards_away, 
       	aws.receiving_yards_avg as receiving_yards_away,
       	--aws.passing_tds as passing_tds_away, 
       	--aws.rushing_tds as rushing_tds_away, 
       	aws.interceptions as interceptions_away, 
       --	aws.special_teams_tds as special_teams_tds_away, 
      	aws.sacks as sacks_away, 
      	aws.rushing_fumbles as rushing_fumbles_away, 
        aws.receiving_fumbles as receiving_fumbles_away,
        aws.sack_fumbles as sack_fumbles_away, 
        --aws.receiving_tds as receiving_tds_away,
        aws.avg_carries as carries_away,
        aws.avg_targets as targets_away, 
        aws.avg_receptions as receptions_away
        --aws.passing_2pt_conversions as passing_2pt_conversions_away,
        --aws.rushing_2pt_conversions as rushing_2pt_conversions_away,
        --aws.touchdown_points as touchdown_points_away,
        --aka.avg_kicking_points as kicking_points_away
        
FROM 
	schedule_data AS sd
LEFT JOIN
	weekly_stats as hs
ON 
	hs.recent_team = sd.home_team
LEFT JOIN
	weekly_stats as aws
ON 
	aws.recent_team = sd.away_team
LEFT JOIN 
	kicking_avg AS hka 
ON
	hka.team = sd.home_team
LEFT JOIN
	kicking_avg AS aka
ON 
	aka.team = sd.away_team;
