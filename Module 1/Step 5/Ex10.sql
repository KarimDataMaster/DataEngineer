explain analyze
select * from tpch1.nation_ext ne 
inner join tpch1.region_ext re
on ne.n_regionkey = re.r_regionkey;