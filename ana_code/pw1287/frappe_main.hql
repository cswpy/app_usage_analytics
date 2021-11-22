USE pw1287;

WITH f AS (SELECT app_id, COUNT(*) AS cnt FROM frappe f GROUP BY app_id)
SELECT m.*, f.cnt FROM f LEFT JOIN meta_cleaned m ON f.app_id = m.app_id ORDER BY f.cnt DESC;

WITH f AS (SELECT app_id, COUNT(*) AS cnt FROM frappe f GROUP BY app_id)
 SELECT m.category, SUM(f.cnt) AS cat_cnt FROM f LEFT JOIN meta_cleaned m ON f.app_id = m.app_id GROUP BY m.category ORDER BY cat_cnt DESC;

SELECT category, AVG(rating) AS cat_avg_rating FROM meta_cleaned m GROUP BY category ORDER BY cat_avg_rating DESC;

SELECT AVG(t.app_cnt) FROM (SELECT user_id, COUNT(DISTINCT(app_id)) AS app_cnt FROM frappe GROUP BY user_id) AS t;

