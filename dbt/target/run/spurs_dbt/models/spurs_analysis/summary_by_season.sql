
  
    

  create  table "nba"."gold"."summary_by_season__dbt_tmp"
  
  
    as
  
  (
    

WITH nba_games AS (
    SELECT
        t1.*
    FROM "nba"."silver"."games" AS t1
    INNER JOIN "nba"."silver"."teams" AS t2
        ON t1.team_id = t2.id
),

base as (
  select
    case when season like '2024' then '2024-25'
        else season end as season,
    team_name,
    wl,
    pts::int as pts
  from nba_games
  
),

sumariza as (select
  season,
  team_name,
  count(*) as total_games,
  sum(case when wl = 'W' then 1 else 0 end) as wins,
  sum(case when wl = 'L' then 1 else 0 end) as losses,
  round(avg(pts), 2) as avg_points
from base
group by season,team_name
order by season,wins desc)

 
  SELECT
    t1.season,
    t1.team_name,
    t1.wins,
    t1.losses,
    t1.total_games,
    t1.avg_points,
    DENSE_RANK() OVER (PARTITION BY t1.season ORDER BY t1.wins DESC, t1.losses ASC,t1.avg_points desc) AS team_ranking
from sumariza t1
  );
  