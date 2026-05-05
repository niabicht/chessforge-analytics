SELECT
    ROUND(AVG(white_elo)::numeric, 2) AS avg_white_elo,
    ROUND(AVG(black_elo)::numeric, 2) AS avg_black_elo,
    MIN(LEAST(white_elo, black_elo)) AS min_elo, -- could just be from one color as no player will every play only one color but lets be thorough
    MAX(GREATEST(white_elo, black_elo)) AS max_elo
FROM games;