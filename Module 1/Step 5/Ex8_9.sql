select gp_segment_id, count(*) from gp_dist_random('tpch1.customer') group by gp_segment_id;
select gp_segment_id, count(*) from gp_dist_random('tpch1.customer_1') group by gp_segment_id;
select gp_segment_id, count(*) from gp_dist_random('tpch1.customer_2') group by gp_segment_id;
select gp_segment_id, count(*) from gp_dist_random('tpch1.orders_2') group by gp_segment_id;