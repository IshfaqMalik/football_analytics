{{ config(materialized='table') }}

with deduplicated as (

    select
        *,
        row_number() over (partition by team_id, team_name order by competition) as rn




    from (
        select  competition,
                team_id,
                team_name,
             coach, contract_start, contract_end, last_updated, squad.name as squad
             from
                     {{ source ('silver', 'transformed_teams_tbl') }}))

select 
            competition,
            team_id,
            team_name,
             coach, contract_start, contract_end, last_updated, squad
    from deduplicated
    where rn = 1
 