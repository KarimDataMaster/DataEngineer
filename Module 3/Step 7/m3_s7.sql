--создать БД на HDFS в папке с данными такси
create database yellow_taxi location 's3a://step5/';

--назначить движок исполнения запросов
set hive.execution.engine=tez;

use yellow_taxi;

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

-- таблица-факт поездок такси
create external table yellow_taxi.trip
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
payment_type_id int,
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
partitioned by (dt date)
stored as parquet
location 's3a://step5/trip/';

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


-- таблица-справочник вендоров такси
create external table yellow_taxi.dim_vendor
(
    id int,
    name string
)
stored as parquet
location 's3a://step5/dim_vendor/';

-- загрузка значений в справочник
insert into yellow_taxi.dim_vendor
select 1, 'Creative Mobile Technologies'
union all
select 2, 'VeriFone Inc';

-- таблица-справочник рейтинга поездки
create external table yellow_taxi.rates
(
    id int,
    name string
)
stored as parquet
location 's3a://step5/rates/';

--загрузка значений в справочник
insert into yellow_taxi.rates
select 1, 'Standard rate'
union all
select 2, 'JFK'
union all
select 3, 'Newark'
union all
select 4, 'Nassau or Westchester'
union all
select 5, 'Negotiated fare'
union all
select 6, 'Group ride';

-- таблица-справочник типа оплаты поездки
create external table yellow_taxi.payment
(
    id int,
    name string
)
stored as parquet
location 's3a://step5/payment/';

--загрузка значений в справочник
with t as (select 1, 'Credit card'
union all
select 2, 'Cash'
union all
select 3, 'No charge'
union all
select 4, 'Dispute'
union all
select 5, 'Unknown'
union all
select 6, 'Voided trip')
insert into yellow_taxi.payment select * from t;


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

--создать представление для вычисления витрины
create view taxi_mart_view as select
    p.name,
    t.dt,
    round(avg(t.tip_amount), 2) as avg_ta,
    sum(t.passenger_count) as total_pc
from yellow_taxi.trip t join yellow_taxi.payment p on t.payment_type_id = p.id
group by p.name, t.dt;


--загрузить в витрину из представления
insert into yellow_taxi.taxi_mart
select * from taxi_mart_view;

------------------------------------------------
--создать таблицу-витрину для практики 2 урока 5
create external table yellow_taxi.taxi_mart_5_2
(
pt string,
dt date,
avg_ta double,
)
stored as parquet
location 's3a://step5/taxi_mart_5_2/';

create view v_taxi_mart_5_2 as
select
    p.name,
    t.dt,
    round(avg(t.tip_amount), 2) as avg_ta
from yellow_taxi.trip t join yellow_taxi.payment p on t.payment_type_id = p.id
where t.tip_amount >= 0
group by p.name, t.dt;

insert into yellow_taxi.taxi_mart_5_2
select * from v_taxi_mart_5_2;


