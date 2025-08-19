
WITH nba_games AS (
    SELECT
         case when t1.season like '2024' then '2024-25'
            else t1.season end as season2, t1.*
    FROM "nba"."silver"."games" AS t1
    INNER JOIN "nba"."silver"."teams" AS t2
        ON t1.team_id = t2.id
),


spurs_stats AS (
    SELECT
        season2,
        AVG(fg_pct::numeric) AS avg_fg_pct,
        AVG(fg3_pct::numeric) AS avg_fg3_pct,
        AVG(tov::numeric) AS avg_turnovers,
        AVG(reb::numeric) AS avg_rebounds,
        AVG(blk::numeric) AS avg_blocks,
        AVG(stl::numeric) AS avg_steals,
        AVG(plus_minus::numeric) AS avg_plus_minus
    FROM nba_games
    WHERE team_id = '1610612759'  -- Team ID for San Antonio Spurs
    GROUP BY season2
),

team_averages AS (
    SELECT
        season2,
        AVG(fg_pct::numeric) AS avg_league_fg_pct,
        AVG(fg3_pct::numeric) AS avg_league_fg3_pct,
        AVG(tov::numeric) AS avg_league_turnovers,
        AVG(reb::numeric) AS avg_league_rebounds,
        AVG(blk::numeric) AS avg_league_blocks,
        AVG(stl::numeric) AS avg_league_steals,
        AVG(plus_minus::numeric) AS avg_league_plus_minus
    FROM nba_games
    GROUP BY season2
),

all_teams_stats AS (
    SELECT
        season2,
        team_id,
        AVG(fg_pct::numeric) AS avg_fg_pct,
        AVG(fg3_pct::numeric) AS avg_fg3_pct,
        AVG(tov::numeric) AS avg_turnovers,
        AVG(reb::numeric) AS avg_rebounds,
        AVG(blk::numeric) AS avg_blocks,
        AVG(stl::numeric) AS avg_steals,
        AVG(plus_minus::numeric) AS avg_plus_minus
    FROM nba_games
    GROUP BY season2, team_id
),

best_team_stats AS (
    SELECT
        season2,
        avg_fg_pct AS best_team_avg_fg_pct,
        avg_fg3_pct AS best_team_avg_fg3_pct,
        avg_turnovers AS best_team_avg_turnovers,
        avg_rebounds AS best_team_avg_rebounds,
        avg_blocks AS best_team_avg_blocks,
        avg_steals AS best_team_avg_steals,
        avg_plus_minus AS best_team_avg_plus_minus
    FROM (
        SELECT
            season2,
            avg_fg_pct,
            avg_fg3_pct,
            avg_turnovers,
            avg_rebounds,
            avg_blocks,
            avg_steals,
            avg_plus_minus,
            ROW_NUMBER() OVER (PARTITION BY season2 ORDER BY avg_plus_minus DESC) AS rn
        FROM all_teams_stats
    ) X
    WHERE X.rn = 1
)

SELECT
    ss.season2,
    ss.avg_fg_pct,
    ta.avg_league_fg_pct,
    bts.best_team_avg_fg_pct,
    ss.avg_fg3_pct,
    ta.avg_league_fg3_pct,
    bts.best_team_avg_fg3_pct,
    ss.avg_turnovers,
    ta.avg_league_turnovers,
    bts.best_team_avg_turnovers,
    ss.avg_rebounds,
    ta.avg_league_rebounds,
    bts.best_team_avg_rebounds,
    ss.avg_blocks,
    ta.avg_league_blocks,
    bts.best_team_avg_blocks,
    ss.avg_steals,
    ta.avg_league_steals,
    bts.best_team_avg_steals,
    ss.avg_plus_minus,
    ta.avg_league_plus_minus,
    bts.best_team_avg_plus_minus,
    CASE WHEN ss.avg_fg_pct < ta.avg_league_fg_pct THEN 'Debilidad' ELSE 'Fortaleza' END AS fg_pct_rating_vs_league,
    CASE WHEN ss.avg_fg_pct < bts.best_team_avg_fg_pct THEN 'Debilidad' ELSE 'Fortaleza' END AS fg_pct_rating_vs_best_team,
    CASE WHEN ss.avg_fg3_pct < ta.avg_league_fg3_pct THEN 'Debilidad' ELSE 'Fortaleza' END AS fg3_pct_rating_vs_league,
    CASE WHEN ss.avg_fg3_pct < bts.best_team_avg_fg3_pct THEN 'Debilidad' ELSE 'Fortaleza' END AS fg3_pct_rating_vs_best_team,
    CASE WHEN ss.avg_turnovers > ta.avg_league_turnovers THEN 'Debilidad' ELSE 'Fortaleza' END AS turnovers_rating_vs_league,
    CASE WHEN ss.avg_turnovers > bts.best_team_avg_turnovers THEN 'Debilidad' ELSE 'Fortaleza' END AS turnovers_rating_vs_best_team,
    CASE WHEN ss.avg_rebounds < ta.avg_league_rebounds THEN 'Debilidad' ELSE 'Fortaleza' END AS rebounds_rating_vs_league,
    CASE WHEN ss.avg_rebounds < bts.best_team_avg_rebounds THEN 'Debilidad' ELSE 'Fortaleza' END AS rebounds_rating_vs_best_team,
    CASE WHEN ss.avg_blocks < ta.avg_league_blocks THEN 'Debilidad' ELSE 'Fortaleza' END AS blocks_rating_vs_league,
    CASE WHEN ss.avg_blocks < bts.best_team_avg_blocks THEN 'Debilidad' ELSE 'Fortaleza' END AS blocks_rating_vs_best_team,
    CASE WHEN ss.avg_steals < ta.avg_league_steals THEN 'Debilidad' ELSE 'Fortaleza' END AS steals_rating_vs_league,
    CASE WHEN ss.avg_steals < bts.best_team_avg_steals THEN 'Debilidad' ELSE 'Fortaleza' END AS steals_rating_vs_best_team,
    CASE WHEN ss.avg_plus_minus < ta.avg_league_plus_minus THEN 'Debilidad' ELSE 'Fortaleza' END AS plus_minus_rating_vs_league,
    CASE WHEN ss.avg_plus_minus < bts.best_team_avg_plus_minus THEN 'Debilidad' ELSE 'Fortaleza' END AS plus_minus_rating_vs_best_team
FROM spurs_stats ss
JOIN team_averages ta ON ss.season2 = ta.season2
JOIN best_team_stats bts ON ss.season2 = bts.season2