{{ config(materialized='table') }}

with home_performance as (
    select
        home_team_id,
        home_team,
        count(match_id) as home_matches,
        sum(home_team_score) as goals_scored_home,
        sum(away_team_score) as goals_conceded_home,
        sum(case when winner = 'HOME_TEAM' then 1 else 0 end) as home_wins,
        sum(case when winner = 'AWAY_TEAM' then 1 else 0 end) as home_losses,
        sum(case when winner = 'DRAW' then 1 else 0 end) as home_draws
    from {{ source('silver', 'transformed_matches_tbl') }}
    group by home_team_id, home_team
),

away_performance as (
    select
        away_team_id,
        away_team,
        count(match_id) as away_matches,
        sum(home_team_score) as goals_conceded_away,
        sum(away_team_score) as goals_scored_away,
        sum(case when winner = 'HOME_TEAM' then 1 else 0 end) as away_losses,
        sum(case when winner = 'AWAY_TEAM' then 1 else 0 end) as away_wins,
        sum(case when winner = 'DRAW' then 1 else 0 end) as away_draws
    from {{ source('silver', 'transformed_matches_tbl') }}
    group by away_team_id, away_team
),

team_performance as (
    select *
    from home_performance h
    full outer join away_performance a
        on h.home_team_id = a.away_team_id
)

select
    coalesce(home_team_id, away_team_id) as team_id,
    coalesce(home_team, away_team) as team,
    coalesce(home_matches, 0) + coalesce(away_matches, 0) as total_matches,
    coalesce(goals_scored_home, 0) + coalesce(goals_scored_away, 0) as total_goals_scored,
    coalesce(goals_scored_home, 0) as goals_scored_home,
    coalesce(goals_scored_away, 0) as goals_scored_away,
    coalesce(goals_conceded_home, 0) + coalesce(goals_conceded_away, 0) as total_goals_conceded,
    coalesce(goals_conceded_home, 0) as goals_conceded_home,
    coalesce(goals_conceded_away, 0) as goals_conceded_away,
    coalesce(home_wins, 0) + coalesce(away_wins, 0) as total_wins,
    coalesce(home_wins, 0) as home_wins,
    coalesce(away_wins, 0) as away_wins,
    coalesce(home_losses, 0) + coalesce(away_losses, 0) as total_losses,
    coalesce(home_losses, 0) as home_losses,
    coalesce(away_losses, 0) as away_losses,
    coalesce(home_draws, 0) + coalesce(away_draws, 0) as total_draws,
    coalesce(home_draws, 0) as home_draws,
    coalesce(away_draws, 0) as away_draws
from team_performance
