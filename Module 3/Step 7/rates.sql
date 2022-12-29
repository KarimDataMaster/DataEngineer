-- таблица-справочник рейтинга поездки
create external table yellow_taxi.rates
(
    id int,
    name string
)
stored as parquet
location 's3a://step5/rates/';