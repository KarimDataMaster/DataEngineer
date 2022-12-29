--создать таблицу-витрину
create external table yellow_taxi.taxi_mart
(
pt string,
dt date,
avg_ta double,
total_pc int
)
stored as parquet
location 's3a://step5/taxi_mart/';