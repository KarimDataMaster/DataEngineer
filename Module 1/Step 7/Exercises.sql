/*> ������� 1
� ���� karpovcourses � ������� public.buowl_habitat ��������� ������ �� ����� �������� ����� ��������.

��� ������ ������� st_area (geometry) ������� ��������� ������� �������������� ������ 
����������� ������ 5 ������� (�� habitat_id) �� ����������� �������. 
� ������ ����� ������������ ������� habitat_id.*/

select bh.*,  st_area(geom) area from public.buowl_habitat bh;

select habitat_id, st_area(geom) area
from public.buowl_habitat
order by habitat_id
limit 5;

/*> ������� 2
� ������� public.linear_projects ��������� ������ � �������������.

��������� ������� st_intersects(geometry,geometry) , 
���������� ������� ����������� ���� ����� ��������� ���� �������.*/

select * from public.linear_projects;

select count(*) as ���������� from (
	select lp1.gid, lp2.gid
	from public.linear_projects as lp1
	cross join public.linear_projects as lp2
	where lp1.gid < lp2.gid and st_intersects(lp1.geom, lp2.geom) = True
	group by lp1.gid, lp2.gid
	order by lp1.gid, lp2.gid
	) t1

/*> ������� 3
� ������� public.articles ���������� ��������� ������.

��� ������ ���������� to_tsquery � to_tsvector 
(https://greenplum.docs.pivotal.io/6-17/admin_guide/textsearch/tables-indexes.html) 
���������� �� �������� ������� ������ (body) ����������� ������������ Ryanair.*/

select * from public.articles;

SELECT body
FROM public.articles
WHERE to_tsvector('english', body) @@ to_tsquery('Ryanair');

/*> ������� 4
��� ������ ������� ts_rank(tsvector,tsquery) ������������ ������ �� ���� ������ ��� ����� airline. 
����� ������ ��������� ������ (� ����� ���� ������ ����)?*/
select * from public.articles;

select heading, ts_rank(to_tsvector('english', body), to_tsquery('airline')) as rank
from public.articles
order by rank desc;

/*��� ������ ������� ts_headline (text,tsquery) 
���������� � ������� ���������� ������������ Ryanair �/��� Wizz.

� ����� ������ ����������� ���������� ����� ������������?*/

select * from public.articles;

SELECT heading, ts_headline('english', body,  to_tsquery('Ryanair & Wizz'))
from public.articles;


