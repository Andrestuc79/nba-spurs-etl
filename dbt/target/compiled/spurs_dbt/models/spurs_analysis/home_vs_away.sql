
WITH nba_games AS (
    SELECT
        t1.*,t2.full_name AS team_name2
    FROM "nba"."silver"."games" AS t1
    INNER JOIN "nba"."silver"."teams" AS t2
        ON t1.team_id = t2.id
),


base as (
  select
    case when season like '2024' then '2024-25'
         else season end as season,
    team_name2 as team_name,     
    case
      when matchup like '%@%' then 'Away'
      else 'Home'
    end as location,
    wl,
    pts::int as pts
  from nba_games
)

select
  season,
  team_name,
  location,
  count(*) as games,
  sum(case when wl = 'W' then 1 else 0 end) as wins,
  sum(case when wl = 'L' then 1 else 0 end) as losses,
  round(avg(pts), 2) as avg_points
from base
group by season,team_name,location
order by season, location,wins desc