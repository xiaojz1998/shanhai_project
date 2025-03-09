-- 以下为学习测试
SELECT * FROM json_each('{"name": "Alice", "age": 25}'::json);
select current_timestamp;
--
select
	to_timestamp('1726991155'),
	'2024-09-21 23:45:55.623'::date;

select
    to_timestamp(created_at)::date as d_date,
    to_date('2023-02-11','YYYY-MM-DD') as d_date
from app_user_track_log;

select
	event ,
	count(*) pv,
	count(distinct uid) uv
from (select
	 uid as uid ,
	 created_at as created_at,
	 event as event ,
	 to_timestamp(created_at::bigint)::date as d_date
from app_user_track_log
where to_timestamp(created_at::bigint)::date = '2024-09-18'
union all
select
	uid as uid,
	created_at as created_at,
	event as event ,
	to_timestamp(created_at::bigint)::date as d_date
from user_log ul
where to_timestamp(created_at::bigint)::date = '2024-09-18' ) t1
group by event ;

select *
from dw.ads_api_abtest_event_df
