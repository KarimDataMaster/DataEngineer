CREATE TABLE customer (                                        
 c_custkey     integer                
 ,c_name        character varying(25)  
 ,c_address     character varying(40)  
 ,c_nationkey   integer                
 ,c_phone       character(15)          
 ,c_acctbal     numeric(15,2)          
 ,c_mktsegment  character(10)          
 ,c_comment     character varying(117) 
 ,n_emp         character(2)           
);
 
CREATE TABLE nation (
n_nationkey integer                
,n_name      character(25)          
,n_regionkey integer                
,n_comment   character varying(152)
,n_emp       character(2)           
);

CREATE TABLE region (
r_regionkey  integer                
,r_name      character(25)          
,r_comment    character varying(152)
,n_emp        character(2)          
);

CREATE TABLE part (
p_partkey      integer               
,p_name         character varying(55) 
,p_mfgr         character(25)         
,p_brand        character(10)         
,p_type         character varying(25) 
,p_size         integer               
,p_container    character(10)         
,p_retailprice  numeric(15,2)         
,p_comment      character varying(23) 
,n_emp          character(2)          
);

CREATE TABLE partsupp (
ps_partkey     integer                
,ps_suppkey     integer                
,ps_availqty    integer                
,ps_supplycost  numeric(15,2)          
,ps_comment     character varying(199) 
,n_emp          character(2)           
);

CREATE TABLE supplier (
s_suppkey    integer                
,s_name       character(25)         
,s_address    character varying(40) 
,s_nationkey  integer               
,s_phone      character(15)         
,s_acctbal    numeric(15,2)         
,s_comment    character varying(101)
,n_emp        character(2)          
);


SELECT * FROM customer;
SELECT * FROM region;
SELECT * FROM nation;

--Используя таблицы customer, nation, region необходимо понять, сколько клиентов в базе из Азии?

SELECT r.r_name, COUNT(c.c_name) FROM customer c 
INNER JOIN nation n 
ON c.c_nationkey = n.n_nationkey 
INNER JOIN region r 
ON n.n_regionkey = r.r_regionkey 
WHERE r.r_name = 'ASIA'
GROUP BY r.r_name;

/*Необходимо создать вью v1 в которой будут отображаться схемы и имена всех вью в текущей базе данных. 
 * Для получения этой информации нужно использовать pg_class и pg_namespace.
Сколько всего строк будет выдавать эта вью?*/



--ссылка на служебные таблицы https://postgrespro.ru/docs/postgrespro/10/view-pg-views

/*В таблице pg_class хранится информация обо всех объектах. 
Для просмотра содержимого нужно просто заселектить все из таблицы: */

SELECT * 
FROM pg_catalog.pg_class

/*
В таблице `pg_namespace` содержится информация обо всех схемах.
Например, попробуем достать информацию о таблицах из конкретной схемы:
*/

SELECT * 
FROM pg_class pc 
JOIN pg_namespace pn 
ON pn.oid=pc.relnamespace 
WHERE nspname='public'
ORDER BY relname;

/*В результатах можно увидеть индексы таблиц. В поле `relstorage` показано как хранятся наши таблицы:

- h - обычная таблица
- x - внешняя таблица
- с - оптимизированная для добавления таблица*/

--создать представление
CREATE VIEW v1 AS (
SELECT schemaname, viewname
FROM pg_catalog.pg_views
);

SELECT COUNT(*) FROM v1;

select count(*), avg(c_acctbal) from customer;
select * from customer order by c_custkey desc limit 5;
select count(*), avg(n_regionkey) from nation;
select count(*), avg(p_retailprice) from part;
select count(*), avg(ps_supplycost) from partsupp;
select count(*), max(s_acctbal), min(s_acctbal) from supplier;

select avg(ps_supplycost) from partsupp;

select * from partsupp;

