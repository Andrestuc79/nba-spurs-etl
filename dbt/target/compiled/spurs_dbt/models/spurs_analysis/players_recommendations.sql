

WITH spurs_weaknesses_unpivoted AS (
    SELECT
        season2,
        weakness_type
    FROM "nba"."gold"."team_weaknesses_unpivoted"
    WHERE Resultado = 'Debilidad'
),

ranked_free_agents AS (
  SELECT
        p.player_id,
        p.player AS player_name,
        p.position,
        CASE WHEN fa.player_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_free_agent,
        CASE WHEN i.player_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_injured,
        s.salary_usd::numeric AS salary,
        pgs.avg_fg_pct,
        pgs.avg_fg3_pct,
        pgs.avg_reb,
        pgs.avg_tov,
        pgs.avg_plus_minus,
        pgs.avg_stl,
        pgs.avg_blk,
        ROW_NUMBER() OVER (ORDER BY pgs.avg_fg_pct DESC) AS rank_fg,
        ROW_NUMBER() OVER (ORDER BY pgs.avg_fg3_pct DESC) AS rank_fg3,
        ROW_NUMBER() OVER (ORDER BY pgs.avg_reb DESC) AS rank_reb,
        ROW_NUMBER() OVER (ORDER BY pgs.avg_tov ASC) AS rank_tov,
        ROW_NUMBER() OVER (ORDER BY pgs.avg_stl DESC) AS rank_stl,
        ROW_NUMBER() OVER (ORDER BY pgs.avg_blk DESC) AS rank_blk,
        ROW_NUMBER() OVER (ORDER BY pgs.avg_plus_minus DESC) AS rank_plus_minus
    FROM (select distinct player_id,player,position from "nba"."silver"."players") AS p
    JOIN (SELECT
    player_id,
    player_name,
    AVG(fg_pct::numeric) AS avg_fg_pct,  
    AVG(fg3_pct::numeric) AS avg_fg3_pct,
    AVG(reb::numeric) AS avg_reb,
    AVG(tov::numeric) AS avg_tov,
    AVG(stl::numeric) AS avg_stl,
    AVG(blk::numeric) AS avg_blk,
    AVG(plus_minus::numeric) AS avg_plus_minus
    FROM "nba"."silver"."player_stats" 
	group by player_id, player_name
    ) AS pgs
    ON p.player_id = pgs.player_id
    LEFT JOIN "nba"."silver"."free_agents"  AS fa ON p.player_id = fa.player_id
    LEFT JOIN (Select player_id from "nba"."silver"."injuries"  group by player_id) 
	AS i ON p.player_id = i.player_id
    LEFT JOIN (select player_id,max(salary_usd) as salary_usd from "nba"."silver"."salaries" group by player_id) AS s ON p.player_id = s.player_id
),

top_targets AS (
    SELECT
        'Porcentaje de tiro de campo' AS weakness_type,
        player_id,
        is_free_agent,
        is_injured,
        player_name,
        avg_fg_pct AS metric_value,
        position,
        salary,
        'Contratar un tirador de élite para mejorar la eficiencia del tiro.' AS reason
    FROM ranked_free_agents
    WHERE rank_fg <= 5 AND position IN ('G', 'F')
    
    UNION ALL
    SELECT
        'Porcentaje de tres' AS weakness_type,
        player_id,
        is_free_agent,
        is_injured,
        player_name,
        avg_fg3_pct AS metric_value,
        position,
        salary,
        'Contratar un tirador de élite para abrir el campo.' AS reason
    FROM ranked_free_agents
    WHERE rank_fg3 <= 5 AND position IN ('G', 'G-F', 'F')
    UNION ALL
    SELECT
        'Rebotes' AS weakness_type,
        player_id,
        is_free_agent,
        is_injured,
        player_name,
        avg_reb AS metric_value,
        position,
        salary,
        'Adquirir un rebotador consistente para controlar los tableros.' AS reason
    FROM ranked_free_agents
    WHERE rank_reb <= 5 AND position IN ('F', 'F-C', 'C')
    UNION ALL
    SELECT
        'Pérdidas de balón' AS weakness_type,
        player_id,
        is_free_agent,
        is_injured,
        player_name,
        avg_tov AS metric_value,
        position,
        salary,
        'Incorporar un base que reduzca las pérdidas de balón.' AS reason
    FROM ranked_free_agents
    WHERE rank_tov <= 5 AND position IN ('G')
    UNION ALL
    SELECT
        'Robos' AS weakness_type,
        player_id,
        is_free_agent,
        is_injured,
        player_name,
        avg_stl AS metric_value,
        position,
        salary,
        'Firmar un defensor perimetral para mejorar la defensa en el robo de balones.' AS reason
    FROM ranked_free_agents
    WHERE rank_stl <= 5 AND position IN ('G', 'F')
    UNION ALL
    SELECT
        'Bloqueos' AS weakness_type,
        player_id,
        is_free_agent,
        is_injured,
        player_name,
        avg_blk AS metric_value,
        position,
        salary,
        'Contratar un defensor interior para proteger el aro y aumentar los bloqueos.' AS reason
    FROM ranked_free_agents
    WHERE rank_blk <= 5 AND position IN ('F-C', 'C')
    UNION ALL
    SELECT
        'Diferencial Puntos' AS weakness_type,
        player_id,
        is_free_agent,
        is_injured,
        player_name,
        avg_plus_minus AS metric_value,
        position,
        salary,
        'Contratar a un jugador con impacto positivo en el diferencial de puntos.' AS reason
    FROM ranked_free_agents
    WHERE rank_plus_minus <= 5
     
)

SELECT
    swu.season2,
    tt.weakness_type,
    tt.player_name AS recommended_player,
    tt.position,
    tt.metric_value,
    tt.salary,
    tt.reason,
    tt.player_id,
    tt.is_free_agent,
    tt.is_injured
FROM spurs_weaknesses_unpivoted swu
JOIN top_targets tt ON swu.weakness_type = tt.weakness_type
ORDER BY swu.season2, tt.weakness_type, tt.metric_value DESC