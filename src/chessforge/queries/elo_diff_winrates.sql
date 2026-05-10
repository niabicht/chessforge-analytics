WITH rating_diff AS (
    SELECT 
        (white_elo - black_elo) AS diff,
        result
    FROM games
),
buckets AS (
    SELECT
        CASE 
            WHEN diff < -200 THEN '< -200'
            WHEN diff BETWEEN -200 AND -50 THEN '-200 to -50'
            WHEN diff BETWEEN -50 AND 50 THEN '-50 to 50'
            WHEN diff BETWEEN 50 AND 200 THEN '50 to 200'
            ELSE '> 200'
        END AS bucket,
        result
    FROM rating_diff
)
SELECT
    bucket,
    COUNT(*) AS total_games,
    ROUND(
        SUM(CASE WHEN result = 2 THEN 1 ELSE 0 END)::numeric / COUNT(*),
        3
    ) AS white_win_rate
FROM buckets
GROUP BY bucket
ORDER BY bucket;