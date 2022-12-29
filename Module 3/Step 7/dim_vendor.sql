-- таблица-справочник вендоров такси
create external table yellow_taxi.dim_vendor
(
    id int,
    name string
)
stored as parquet
location 's3a://step5/dim_vendor/';