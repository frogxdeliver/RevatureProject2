USE GENDERSTATS_DB;

LOAD DATA LOCAL INPATH "/home/cloudera/Downloads/Gender_StatsData.csv"
INTO TABLE GENDER_STATS;