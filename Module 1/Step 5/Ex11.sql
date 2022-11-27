explain analyze
select * from tpch1.customer_2 c
inner join tpch1.orders_2 o 
on c.c_custkey = o.o_custkey;