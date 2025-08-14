{{ config(materialized='table')}}

with base as (
  select
    season,
    fg_pct::float as fg_pct,
    tov::int as turnovers
  from {{ source('silver', 'spurs_games') }}
)

select
  season,
   round(avg(fg_pct)::numeric, 3) as avg_fg_pct,
  round(avg(turnovers), 2) as avg_turnovers
from base
group by season
having round(avg(fg_pct)::numeric, 3) < 0.45 or round(avg(turnovers), 2) > 14
order by season
