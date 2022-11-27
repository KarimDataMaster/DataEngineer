explain analyze
select * from tpch1.customer c
inner join tpch1.orders o 
on c.c_custkey = o.o_custkey;

--select count(*) from tpch1.customer;