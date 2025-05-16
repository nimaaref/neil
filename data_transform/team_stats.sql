with schedules as(  
SELECT 
    season,
    week,
    home_team AS recent_team,
    winner
  FROM int_schedules

  UNION ALL

  SELECT 
    season,
    week,
    away_team AS recent_team,
    winner
  FROM int_schedules
)


select 
    t.recent_team,
    case when s.winner = t.recent_team then "win" else "loss" end as result ,
    t.season,
    t.season_type,
    t.week,
    t.touchdown_points,
    t.touchdowns_scored,
    t.passing_yards,
    t.rushing_yards,
    t.receiving_yards,
    t.passing_tds,
    t.rushing_tds,
    t.receiving_tds,
    t.special_teams_tds,
    t.interceptions,
    t.sacks,
    t.rushing_fumbles,
    t.receiving_fumbles,
    t.rushing_first_downs,
    t.receiving_first_downs,
    t.carries,
    t.targets,
    t.receptions,
    t.passing_2pt_conversions,
    t.rushing_2pt_conversions,
    t.receiving_2pt_conversions,
    t.completions,
    t.attempts,
    t.sack_yards,
    t.sack_fumbles,
    t.sack_fumbles_lost,
    t.passing_air_yards,
    t.passing_yards_after_catch,
    t.passing_first_downs,
    t.rushing_fumbles_lost,
    t.receiving_fumbles_lost,
    t.receiving_air_yards,
    t.receiving_yards_after_catch
from int_team_stats as t
inner join schedules as s
on s.recent_team = t.recent_team 
and s.season = t.season
and s.week = t.week ;