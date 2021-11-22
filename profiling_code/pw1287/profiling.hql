SELECT COUNT(DISTINCT(user_id)) FROM frappe;

SELECT COUNT(DISTINCT(app_id)) FROM frappe;

SELECT COUNT(DISTINCT(country)) FROM frappe;

SELECT country, COUNT(*) AS cnt FROM frappe GROUP BY country ORDER BY cnt DESC;