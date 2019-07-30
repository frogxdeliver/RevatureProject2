\ - multi line command




sqoop export \
--connect jdbc:mysql://localhost/STUDENT_DB \
--username STUDENT_ADMIN \
--password p4ssw0rd \
--table STUDENT \
--input-fields-terminated-by ',' \
--export-dir 'sqoop/' \
-m 1


line 12: 'HiveTables/' 
^ holds the students.csv value that we need. It was changed a few times




sqoop import \
--connect jdbc:mysql://localhost/STUDENT_DB \
--username STUDENT_ADMIN \
--password p4ssw0rd \
--table STUDENT_SUMMARY \
--target-dir 'output/sqoop-all' \
-m 1



removed from line 27 V
# --input-fields-terminated-by '|' \




sqoop import \
--connect jdbc:mysql://localhost/STUDENT_DB \
--username STUDENT_ADMIN \
--password p4ssw0rd \
--table STUDENT_SUMMARY \
--where "STATE='Texas'" \
--target-dir 'output/sqoop-texas' \
--fields-terminated-by  '|' \
-m 1






sqoop job --create student_job \
-- import \
--connect jdbc:mysql://localhost/STUDENT_DB \
--username STUDENT_ADMIN \
--password p4ssw0rd \
--table STUDENT_NUMERIC \
--target-dir 'output/sqoop-append' \
--fields-terminated-by  '|' \
--incremental append \
--check-column SSN_NUMERIC \
-m 1

#to view the list
sqoop job --list

#to delete
sqoop job --delete student_job

#to run job
sqoop job --exec student_job

#to copy to local, the sqoop file we made (not necessary)
hdfs dfs -copyToLocal output/sqoop-append/part-m-00001
