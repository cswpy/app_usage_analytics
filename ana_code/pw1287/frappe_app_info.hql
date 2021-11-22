USE pw1287;

SELECT category, COUNT(DISTINCT(name)) FROM meta_cleaned GROUP BY category;

SELECT developer, COUNT(DISTINCT(name)) AS app_cnt FROM meta_cleaned GROUP BY developer ORDER BY app_cnt DESC LIMIT 5;

