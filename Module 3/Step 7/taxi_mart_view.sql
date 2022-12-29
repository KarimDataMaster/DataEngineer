--создать представление для вычисления витрины
create view taxi_mart_view as select
    p.name,
    t.dt,
    round(avg(t.tip_amount), 2) as avg_ta,
    sum(t.passenger_count) as total_pc
from yellow_taxi.trip t join yellow_taxi.payment p on t.payment_type_id = p.id
group by p.name, t.dt;