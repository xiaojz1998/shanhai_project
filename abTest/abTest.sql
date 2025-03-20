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
----------------------------------------------------------------------
-- 找相关埋点 ab_tag = '' 现在先不上线
----------------------------------------------------------------------

select
    *
from app_user_track_log
where event_name = 'profile_topup' and to_timestamp(created_at::bigint)::date = '2025-03-18' and get_json_object(ext_body,'$.ab_tag') = ''
limit 10