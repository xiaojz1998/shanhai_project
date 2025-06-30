---------------------------------------------
-- File: 2025-06-21取数.sql
-- Time: 2025/6/21 14:18
-- User: xiaoj
-- Description:  
---------------------------------------------

set timezone ='UTC-0';
-- 需要排除的自然流数据
with excluded_data as (
    select
        t1.v_date,
        t1.d_date :: date,
        t1.uid ::bigint,
        t1.user_source
    from dwd_user_info t1
    left join  (
    -- 3月以后 每个用户每天观看时间
        SELECT
            to_timestamp(created_at)::date as d_date,
            uid,
            sum(case when event=2 then watch_time else 0 end) as watch_duration_sec
        FROM public.app_user_track_log a
        WHERE a.event = 2 AND a.vid > 0 AND a.eid > 0 AND to_timestamp(a.created_at)::date >= '2025-05-01'
        GROUP BY to_timestamp(a.created_at):: date,a.uid
    ) t2
        on t1.d_date::date = t2.d_date::date and t1.uid::bigint = t2.uid::bigint
    where t1.d_date>= '2025-05-01' and( watch_duration_sec is null or watch_duration_sec < 3) and user_source = '自然流'
    group by t1.d_date,t1.uid
),
new_reg_users as (
	select
	     t.v_date as created_date
	    , t.d_date::date as d_date
	    , t.uid::int8 as uid
	    , t.country_name
	    , t.lang_name
	    , t.user_source
	    , t.ad_channel
	    , case when ed.uid is null then 1 else 0 end as "是否有效"
	from public.dwd_user_info t
	left join excluded_data ed on t.uid::bigint = ed.uid and t.d_date::date = ed.d_date
	-- where ed.uid is null -- 剔除这部分新用户
),
tmp_excluded_users as (
    select
        d_date
        , count(distinct uid) as excluded_uv
    from excluded_data
    group by d_date
),
tmp_1 as (
    select
        d_date
        , count(distinct uid) as "新用户数"
        , count(distinct case when user_source = '自然流' then uid else null end) as "自然流新用户"
        , count(distinct case when user_source = '自然流' and "是否有效" = 0 then uid else null end) as "自然流新用户无效数"
    from new_reg_users
    group by d_date
),
tmp_unacitive_users as (
    select
        v_date,
        d_date,
        count(distinct case when stay_active is null then uid else null end) as stay_unactive_uv
    from(
        select
            t.v_date,
            t.d_date,
            t.uid,
            max(t0.uid) as stay_active
        from excluded_data t
        left join public.dwd_user_active t0 on t.d_date + 1 <= t0.d_date  and t0.d_date <= t.d_date + 7 and t0.uid = t.uid
        group by t.v_date, t.d_date, t.uid
    ) t
    group by t.v_date, t.d_date
)
select
    a.d_date as "日期"
    , "新用户数"
    , "自然流新用户"
    , "自然流新用户无效数"
    , stay_unactive_uv as 后七天不活跃用户数
from tmp_1 a
left join tmp_unacitive_users b  on a.d_date = b.d_date
where a.d_date >= '2025-05-01'

-- select
--     t.d_date as 日期,
--     t.country_name as 国家,
--     t.lang_name as 语言,
--     t.ad_channel as 渠道,
--     t.user_source as 是否是推广流,
--     excluded_uv as 排除用户数,
--     stay_unactive_uv as 后七天不活跃用户数
-- from tmp_excluded_users t
-- left join tmp_unacitive_users t0
--     on t.d_date = t0.d_date and t.user_source = t0.user_source and t.country_name = t0.country_name and t.lang_name=t0.lang_name and t.ad_channel = t0.ad_channel