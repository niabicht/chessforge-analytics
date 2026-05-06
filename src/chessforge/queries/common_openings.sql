SELECT opening, COUNT(*) AS games
FROM games
GROUP BY opening
ORDER BY games DESC
LIMIT 10;