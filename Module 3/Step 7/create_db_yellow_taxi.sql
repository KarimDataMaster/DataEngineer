--создать БД на HDFS в папке с данными такси
create database yellow_taxi location 's3a://step5/';

--назначить движок исполнения запросов
set hive.execution.engine=tez;