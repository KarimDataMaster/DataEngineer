--выполнить перед исполнением
set hive.exec.dynamic.partition.mode=nonstrict;

--увеличить количество партиций
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

--загрузка значений в таблицу-фактов
insert overwrite table yellow_taxi.trip partition(dt)
select
vendor_id
,tpep_pickup_datetime
,tpep_dropoff_datetime
,passenger_count
,trip_distance
,ratecode_id
,store_and_fwd_flag string
,pulocation_id
,dolocation_id
,payment_type
,fare_amount
,extra
,mta_tax
,tip_amount
,tolls_amount
,improvement_surcharge
,total_amount
,congestion_surcharge
,airport_fee
,to_date(tpep_pickup_datetime) as dt
from yellow_taxi.taxi_data
where extract(year from tpep_pickup_datetime)=2020;