--  создать таблицу, из которой будут считываться данные
create external table yellow_taxi.taxi_data
(
vendor_id string,
tpep_pickup_datetime timestamp,
tpep_dropoff_datetime timestamp,
passenger_count int,
trip_distance double,
ratecode_id int,
store_and_fwd_flag string,
pulocation_id int,
dolocation_id int,
payment_type int,
fare_amount double,
extra double,
mta_tax double,
tip_amount double,
tolls_amount double,
improvement_surcharge double,
total_amount double,
congestion_surcharge double,
airport_fee double
)
row format delimited
fields terminated by ','
lines terminated by '\n'
location 's3a://step5/taxi_data'
TBLPROPERTIES ("skip.header.line.count"="1");