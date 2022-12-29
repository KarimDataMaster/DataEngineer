-- таблица-справочник типа оплаты поездки
create external table yellow_taxi.payment
(
    id int,
    name string
)
stored as parquet
location 's3a://step5/payment/';