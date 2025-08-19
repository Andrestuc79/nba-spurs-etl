
  
    

  create  table "nba"."gold"."team_weaknesses_unpivoted__dbt_tmp"
  
  
    as
  
  (
    

WITH spurs_stats AS (
    SELECT
        case when season like '2024' then '2024-25'
            else season end as season2,
        AVG(fg_pct::numeric) AS avg_fg_pct,
        AVG(fg3_pct::numeric) AS avg_fg3_pct,
        AVG(tov::numeric) AS avg_turnovers,
        AVG(reb::numeric) AS avg_rebounds,
        AVG(blk::numeric) AS avg_blocks,
        AVG(stl::numeric) AS avg_steals,
        AVG(plus_minus::numeric) AS avg_plus_minus
    FROM "nba"."silver"."games"
    WHERE team_name = 'San Antonio Spurs'
    GROUP BY season2
),

team_averages AS (
    SELECT
        case when t1.season like '2024' then '2024-25'
            else t1.season end as season2,
        AVG(t1.fg_pct::numeric) AS avg_league_fg_pct,
        AVG(t1.fg3_pct::numeric) AS avg_league_fg3_pct,
        AVG(t1.tov::numeric) AS avg_league_turnovers,
        AVG(t1.reb::numeric) AS avg_league_rebounds,
        AVG(t1.blk::numeric) AS avg_league_blocks,
        AVG(t1.stl::numeric) AS avg_league_steals,
        AVG(t1.plus_minus::numeric) AS avg_league_plus_minus
    FROM "nba"."silver"."games" t1
    INNER JOIN "nba"."silver"."teams" AS t2
        ON t1.team_id = t2.id
    GROUP BY season2
),

best_team_stats AS (
    SELECT
        season2 AS season2,
        MAX(avg_fg_pct) AS best_team_avg_fg_pct,
        MAX(avg_fg3_pct) AS best_team_avg_fg3_pct,
        MIN(avg_turnovers) AS best_team_avg_turnovers, -- Menos pérdidas es mejor
        MAX(avg_reb) AS best_team_avg_rebounds,
        MAX(avg_blk) AS best_team_avg_blocks,
        MAX(avg_stl) AS best_team_avg_steals,
        MAX(avg_plus_minus) AS best_team_avg_plus_minus
    FROM (
        SELECT
            case when t1.season like '2024' then '2024-25'
            else t1.season end as season2,
            t1.team_name,
            AVG(t1.fg_pct::numeric) AS avg_fg_pct,
            AVG(t1.fg3_pct::numeric) AS avg_fg3_pct,
            AVG(t1.tov::numeric) AS avg_turnovers,
            AVG(t1.reb::numeric) AS avg_reb,
            AVG(t1.blk::numeric) AS avg_blk,
            AVG(t1.stl::numeric) AS avg_stl,
            AVG(t1.plus_minus::numeric) AS avg_plus_minus
        FROM "nba"."silver"."games" t1
        INNER JOIN "nba"."silver"."teams" AS t2
            ON t1.team_id = t2.id
        GROUP BY season2, team_name
    ) AS all_teams
    GROUP BY season2
),

unpivoted_weaknesses AS (
    SELECT
        ss.season2,
        'Porcentaje de tiro de campo' AS weakness_type,
        ss.avg_fg_pct AS valor_equipo,
        ta.avg_league_fg_pct AS valor_liga,
        bts.best_team_avg_fg_pct AS valor_mejor_equipo,
        case when ss.avg_fg_pct < ta.avg_league_fg_pct then 'Debilidad' else 'Fortaleza' end AS Resultado
    FROM spurs_stats ss
    JOIN team_averages ta ON ss.season2 = ta.season2
    JOIN best_team_stats bts ON ss.season2 = bts.season2
    
    UNION ALL

    SELECT
        ss.season2,
        'Porcentaje de tres' AS weakness_type,
        ss.avg_fg3_pct,
        ta.avg_league_fg3_pct,
        bts.best_team_avg_fg3_pct,
        case when ss.avg_fg3_pct < ta.avg_league_fg3_pct then 'Debilidad' else 'Fortaleza' end AS Resultado
    FROM spurs_stats ss
    JOIN team_averages ta ON ss.season2 = ta.season2
    JOIN best_team_stats bts ON ss.season2 = bts.season2

    UNION ALL

    SELECT
        ss.season2,
        'Pérdidas de balón' AS weakness_type,
        ss.avg_turnovers,
        ta.avg_league_turnovers,
        bts.best_team_avg_turnovers,
        case when ss.avg_turnovers > ta.avg_league_turnovers then 'Debilidad' else 'Fortaleza' end AS Resultado
    FROM spurs_stats ss
    JOIN team_averages ta ON ss.season2 = ta.season2
    JOIN best_team_stats bts ON ss.season2 = bts.season2

    UNION ALL

    SELECT
        ss.season2,
        'Rebotes' AS weakness_type,
        ss.avg_rebounds,
        ta.avg_league_rebounds,
        bts.best_team_avg_rebounds,
        case when ss.avg_rebounds < ta.avg_league_rebounds then 'Debilidad' else 'Fortaleza' end AS Resultado
    FROM spurs_stats ss
    JOIN team_averages ta ON ss.season2 = ta.season2
    JOIN best_team_stats bts ON ss.season2 = bts.season2

    UNION ALL

    SELECT
        ss.season2,
        'Robos' AS weakness_type,
        ss.avg_steals,
        ta.avg_league_steals,
        bts.best_team_avg_steals,
        case when ss.avg_steals < ta.avg_league_steals then 'Debilidad' else 'Fortaleza' end AS Resultado
    FROM spurs_stats ss
    JOIN team_averages ta ON ss.season2 = ta.season2
    JOIN best_team_stats bts ON ss.season2 = bts.season2

    UNION ALL

    SELECT
        ss.season2,
        'Bloqueos' AS weakness_type,
        ss.avg_blocks,
        ta.avg_league_blocks,
        bts.best_team_avg_blocks,
        case when ss.avg_blocks < ta.avg_league_blocks then 'Debilidad' else 'Fortaleza' end AS Resultado
    FROM spurs_stats ss
    JOIN team_averages ta ON ss.season2 = ta.season2
    JOIN best_team_stats bts ON ss.season2 = bts.season2

    UNION ALL

    SELECT
        ss.season2,
        'Diferencial Puntos' AS weakness_type,
        ss.avg_plus_minus,
        ta.avg_league_plus_minus,
        bts.best_team_avg_plus_minus,
        case when ss.avg_plus_minus < ta.avg_league_plus_minus then 'Debilidad' else 'Fortaleza' end AS Resultado
    FROM spurs_stats ss
    JOIN team_averages ta ON ss.season2 = ta.season2
    JOIN best_team_stats bts ON ss.season2 = bts.season2
)

SELECT
    season2,
    weakness_type,
    valor_equipo,
    valor_liga,
    valor_mejor_equipo,
    resultado
FROM unpivoted_weaknesses
ORDER BY season2, weakness_type
  );
  