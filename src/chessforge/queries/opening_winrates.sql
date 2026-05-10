SELECT 
    opening,
    COUNT(*) AS total_games,
    SUM(CASE WHEN result = 2 THEN 1 ELSE 0 END) AS white_wins,
    SUM(CASE WHEN result = 0 THEN 1 ELSE 0 END) AS black_wins,
    SUM(CASE WHEN result = 1 THEN 1 ELSE 0 END) AS draws,
    ROUND(
        SUM(CASE WHEN result = 2 THEN 1 ELSE 0 END)::numeric / COUNT(*),
        3
    ) AS white_win_rate
FROM games
GROUP BY opening
HAVING COUNT(*) > 100
ORDER BY white_win_rate DESC
LIMIT 10;