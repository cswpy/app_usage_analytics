SELECT COUNT(DISTINCT(user_id)) FROM frappe;

SELECT COUNT(DISTINCT(app_id)) FROM frappe;

SELECT COUNT(DISTINCT(country)) FROM frappe;

SELECT country, COUNT(*) AS cnt FROM frappe GROUP BY country ORDER BY cnt DESC;

WITH f AS (SELECT app_id, COUNT(*) AS cnt FROM frappe f GROUP BY app_id)
SELECT m.*, f.cnt FROM f LEFT JOIN meta_cleaned m ON f.app_id = m.app_id ORDER BY f.cnt DESC;

WITH f AS (SELECT app_id, COUNT(*) AS cnt FROM frappe f GROUP BY app_id)
 SELECT m.category, SUM(f.cnt) AS cat_cnt FROM f LEFT JOIN meta_cleaned m ON f.app_id = m.app_id GROUP BY m.category ORDER BY cat_cnt DESC;

SELECT category, AVG(rating) AS cat_avg_rating FROM meta_cleaned m GROUP BY category ORDER BY cat_avg_rating DESC;

