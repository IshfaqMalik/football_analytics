{{ config(materialized='table') }}

with cl_team as (
    select
        team_id,
        team_name,
        coach,
        contract_start,
        contract_end,
        last_updated,
        squad.name as squad
    from {{ source('silver', 'transformed_teams_tbl') }}
    where competition_id = 2001
)

select * from cl_team