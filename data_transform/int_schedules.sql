select 
    game_id, 
    season, 
    game_type, 
    case
        when game_type = "reg" then "reg"
        else "post" 
    end as game_type_group, 
    case
        when game_type = "reg" then 0
        else 1 
    end as game_type_num,
    week, 
    gameday, 
    weekday, 
    gametime, 
    case 
        when away_team is null or away_team = 'OAK' then 'LV' 
        else away_team 
    end as away_team, 
    away_score,
    case 
        when home_team is null or home_team = 'OAK' then 'LV' 
        else home_team 
    end as home_team,  
    home_score, 
    case
        when home_score > away_score then 1 
        else 0 
    end as outcome,
    case
        when home_score > away_score then home_team
        else away_team 
    end as winner, 
    location, 
    total, 
    overtime, 
    away_rest, 
    home_rest, 
    away_moneyline, 
    home_moneyline, 
    spread_line, 
    away_spread_odds, 
    home_spread_odds, 
    total_line, 
    under_odds, 
    over_odds, 
    div_game, 
    roof, 
    surface,
    temp,
    wind, 
    away_qb_id, 
    home_qb_id, 
    away_qb_name, 
    home_qb_name, 
    away_coach, 
    home_coach, 
    referee, 
    stadium_id, 
    stadium
from
    stg_schedules;






