--- data profiling queries for table 1:
SELECT COUNT(username) FROM appusage1;
SELECT COUNT(DISTINCT(app)) FROM appusage1;
SELECT COUNT(DISTINCT(username)) FROM appusage1;

SELECT COUNT(user_id) FROM unique_appusage;
