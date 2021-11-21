CREATE TABLE frappe (userid INT, info STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION '/user/pw1287/project/cleaning/clean_output/';

CREATE TABLE frappe_splitted(user_id INT, app_id INT, count INT, country STRING, city INT);

INSERT INTO TABLE frappe_splitted
SELECT f.userid as user_id, f.sp[0] AS app_id, f.sp[1] AS count, f.sp[2] AS country, f.sp[3] AS city
FROM (SELECT userid, split(infp, ',') AS sp FROM frappe)f;
