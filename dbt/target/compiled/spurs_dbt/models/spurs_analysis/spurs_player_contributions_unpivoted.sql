

WITH source_data AS (

    -- Usamos `source` si tienes la tabla definida en tu `sources.yml`,
    -- o la referenciamos directamente si es un modelo intermedio.
    -- Reemplaza `your_source_name` y `your_table_name` con los nombres correctos.
    SELECT
    player_id,
    player_name,
    team_abbreviation,
    AVG(fg_pct::numeric) AS avg_fg_pct,
    AVG(fg3_pct::numeric) AS avg_fg3_pct,
    AVG(reb::numeric) AS avg_reb,
    AVG(tov::numeric) AS avg_tov,
    AVG(stl::numeric) AS avg_stl,
    AVG(blk::numeric) AS avg_blk,
    AVG(plus_minus::numeric) AS avg_plus_minus
from 
silver.player_stats
where team_abbreviation = 'SAS'
GROUP BY player_id, player_name, team_abbreviation
),


unpivoted_contribucion AS (
    SELECT
        ss.player_id,
        ss.player_name,
        'Porcentaje de tiro de campo' AS rubro,
        ss.avg_fg_pct AS valor
    FROM source_data ss
    
    UNION ALL

    SELECT
        ss.player_id,
        ss.player_name,
        'Porcentaje de tres' AS rubro,
        ss.avg_fg3_pct as valor
    FROM source_data ss
  
    UNION ALL

    SELECT
        ss.player_id,
        ss.player_name,
        'Pérdidas de balón' AS rubro,
        ss.avg_tov as valor
    FROM source_data ss
   
    UNION ALL

    SELECT
        ss.player_id,
        ss.player_name,
        'Rebotes' AS rubro,
        ss.avg_reb as valor
    FROM source_data ss
  
    UNION ALL

    SELECT
        ss.player_id,
        ss.player_name,
        'Robos' AS rubro,
        ss.avg_stl as valor
    FROM source_data ss
   
    UNION ALL

    SELECT
        ss.player_id,
        ss.player_name,
        'Bloqueos' AS rubro,
        ss.avg_blk as valor
    FROM source_data ss
  
    UNION ALL

    SELECT
        ss.player_id,
        ss.player_name,
        'Diferencial Puntos' AS rubro,
        ss.avg_plus_minus as valor
    FROM source_data ss
    
)

SELECT * FROM unpivoted_contribucion