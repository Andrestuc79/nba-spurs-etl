{{ config(materialized='table')}}

with base as (
  select
    season,
    case
      when matchup like '%@%' then 'Away'
      else 'Home'
    end as location,
    wl,
    pts::int as pts
  from {{ source('silver', 'spurs_games') }}
)

select
  season,
  location,
  count(*) as games,
  sum(case when wl = 'W' then 1 else 0 end) as wins,
  sum(case when wl = 'L' then 1 else 0 end) as losses,
  round(avg(pts), 2) as avg_points
from base
group by season, location
order by season, location