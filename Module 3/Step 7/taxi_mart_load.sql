--загрузить в витрину из представления
insert into yellow_taxi.taxi_mart
select * from taxi_mart_view;