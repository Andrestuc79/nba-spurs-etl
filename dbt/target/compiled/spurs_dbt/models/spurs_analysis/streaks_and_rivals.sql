

WITH spurs_games AS (
    SELECT
        case when season like '2024' then '2024-25'
         else season end as season,
        game_date,
        matchup,
        plus_minus,
        CASE 
            WHEN wl = 'W' THEN 1 
            ELSE 0 
        END AS is_win,
        CASE 
            WHEN wl = 'L' THEN 1 
            ELSE 0 
        END AS is_loss
    FROM "nba"."silver"."games"
    WHERE team_abbreviation = 'SAS'
),

winning_streaks AS (
    SELECT
        *,
        SUM(is_loss) OVER (ORDER BY game_date) AS loss_group
    FROM spurs_games
),

losing_streaks AS (
    SELECT
        *,
        SUM(is_win) OVER (ORDER BY game_date) AS win_group
    FROM spurs_games
),

best_winning_streak AS (
    SELECT
        season,
        COUNT(*) AS streak_length
    FROM winning_streaks
    WHERE is_win = 1
    GROUP BY season, loss_group
    ORDER BY streak_length DESC
    LIMIT 1
),

worst_losing_streak AS (
    SELECT
        season,
        COUNT(*) AS streak_length
    FROM losing_streaks
    WHERE is_loss = 1
    GROUP BY season, win_group
    ORDER BY streak_length DESC
    LIMIT 1
),

biggest_win AS (
    SELECT
        season,
        -- Extrae el nombre del oponente de la columna 'matchup'
        CASE
            WHEN matchup LIKE '%vs.%' THEN SPLIT_PART(matchup, 'vs. ', 2)
            WHEN matchup LIKE '%@%' THEN SPLIT_PART(matchup, '@ ', 2)
            ELSE matchup
        END AS opponent,
        plus_minus AS point_differential
    FROM spurs_games
    WHERE plus_minus = (SELECT MAX(plus_minus) FROM spurs_games)
    ORDER BY point_differential DESC
    LIMIT 1
),

biggest_loss AS (
    SELECT
        season,
        -- Extrae el nombre del oponente de la columna 'matchup'
        CASE
            WHEN matchup LIKE '%vs.%' THEN SPLIT_PART(matchup, 'vs. ', 2)
            WHEN matchup LIKE '%@%' THEN SPLIT_PART(matchup, '@ ', 2)
            ELSE matchup
        END AS opponent,
        plus_minus AS point_differential
    FROM spurs_games
    WHERE plus_minus = (SELECT MIN(plus_minus) FROM spurs_games)
    ORDER BY point_differential ASC
    LIMIT 1
)

-- Consulta final que une todas las métricas en una sola fila
SELECT
    (SELECT season FROM best_winning_streak) AS best_winning_streak_season,
    (SELECT streak_length FROM best_winning_streak) AS best_winning_streak_length,
    (SELECT season FROM worst_losing_streak) AS worst_losing_streak_season,
    (SELECT streak_length FROM worst_losing_streak) AS worst_losing_streak_length,
    (SELECT season FROM biggest_win) AS biggest_win_season,
    (SELECT opponent FROM biggest_win) AS team_beat_by_most,
    (SELECT point_differential FROM biggest_win) AS biggest_win_margin,
    (SELECT season FROM biggest_loss) AS biggest_loss_season,
    (SELECT opponent FROM biggest_loss) AS team_lost_to_by_most,
    (SELECT point_differential FROM biggest_loss) AS biggest_loss_margin