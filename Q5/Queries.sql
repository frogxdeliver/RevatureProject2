CREATE DATABASE IF NOT EXISTS GENDERSTATS_DB;

USE GENDERSTATS_DB;

DROP TABLE if exists fertility_rate_stats;
DROP TABLE if exists fertility_rate_stats_3;

CREATE TABLE fertility_rate_stats
AS
SELECT * FROM GENDER_STATS
WHERE INDICATOR_CODE='SP.ADO.TFRT';

create table fertility_rate_stats_3
AS
SELECT country_name, year_1960, year_2015, ((year_1960-year_2015)/1000)*100 AS Percent_Difference
FROM fertility_rate_stats;




CREATE DATABASE IF NOT EXISTS GENDERSTATS_DB;

USE GENDERSTATS_DB;

DROP TABLE IF EXISTS GENDERSTATS_DB.fertility_rate_stats;

CREATE TABLE GENDERSTATS_DB.fertility_rate_stats
AS
SELECT * FROM GENDER_STATS
WHERE INDICATOR_CODE='SP.ADO.TFRT';