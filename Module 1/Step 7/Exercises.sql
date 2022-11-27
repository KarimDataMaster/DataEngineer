/*> Задание 1
В базе karpovcourses в таблице public.buowl_habitat находятся данные по зонам обитания диких животных.

При помощи функции st_area (geometry) которая вычисляет площадь геометрической фигуры 
упорядочите первые 5 записей (по habitat_id) по возрастанию площади. 
В ответе нужен получившийся порядок habitat_id.*/

select bh.*,  st_area(geom) area from public.buowl_habitat bh;

select habitat_id, st_area(geom) area
from public.buowl_habitat
order by habitat_id
limit 5;

/*> Задание 2
В таблице public.linear_projects находятся данные о коммуникациях.

Используя функцию st_intersects(geometry,geometry) , 
определите сколько пересечений есть между объектами этой таблицы.*/

select * from public.linear_projects;

select count(*) as Количество from (
	select lp1.gid, lp2.gid
	from public.linear_projects as lp1
	cross join public.linear_projects as lp2
	where lp1.gid < lp2.gid and st_intersects(lp1.geom, lp2.geom) = True
	group by lp1.gid, lp2.gid
	order by lp1.gid, lp2.gid
	) t1

/*> Задание 3
В таблице public.articles содержатся несколько статей.

При помощи операторов to_tsquery и to_tsvector 
(https://greenplum.docs.pivotal.io/6-17/admin_guide/textsearch/tables-indexes.html) 
определите во скольких текстах статей (body) упоминается авиакомпания Ryanair.*/

select * from public.articles;

SELECT body
FROM public.articles
WHERE to_tsvector('english', body) @@ to_tsquery('Ryanair');

/*> Задание 4
При помощи функции ts_rank(tsvector,tsquery) отранжируйте статьи по телу статьи для слова airline. 
Какая статья оказалась первой (у какой ранг больше всех)?*/
select * from public.articles;

select heading, ts_rank(to_tsvector('english', body), to_tsquery('airline')) as rank
from public.articles
order by rank desc;

/*При помощи функции ts_headline (text,tsquery) 
подсветите в статьях упоминания авиакомпаний Ryanair и/или Wizz.

В какой статье встретилось упоминание обеих авиакомпаний?*/

select * from public.articles;

SELECT heading, ts_headline('english', body,  to_tsquery('Ryanair & Wizz'))
from public.articles;


