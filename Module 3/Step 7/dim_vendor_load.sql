-- загрузка значений в справочник
insert into yellow_taxi.dim_vendor
select 1, 'Creative Mobile Technologies'
union all
select 2, 'VeriFone Inc';