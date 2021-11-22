USE pw1287;

 SELECT f.*, m.name, m.category FROM frequent_app_by_country f LEFT JOIN meta_cleaned m ON f.app_id = m.app_id ORDER BY f.country ASC;