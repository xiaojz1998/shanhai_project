---------------------------------------------
-- File: 测试.sql
-- Time: 2025/5/26 10:05
-- User: xiaoj
-- Description:  
---------------------------------------------
--  public.dw_push_view 数据量 115254
-- select count(*) from public.dw_push_view;

-- 查看user_layered_configs
select
    push_id,
    string_agg(layered_name ,';' order by layered_name)
from (select
    id::text as push_id,
    user_layered_configs ,
       json_array_elements((REGEXP_MATCH(replace(user_layered_configs, E'\\', ''),'\[.*?\]'))[1]::json) ->> 'id' as layered_id,
       json_array_elements((REGEXP_MATCH(replace(user_layered_configs, E'\\', ''),'\[.*?\]'))[1]::json) ->> 'name' as layered_name
from public."oversea-api_osd_pushed" ) t
group by push_id;

select
    id::text as push_id,
    user_layered_configs ,
       json_array_elements((REGEXP_MATCH(replace(user_layered_configs, E'\\', ''),'\[.*?\]'))[1]::json) as layered_id,
       json_array_elements((REGEXP_MATCH(replace(user_layered_configs, E'\\', ''),'\[.*?\]'))[1]::json) as layered_name
from public."oversea-api_osd_pushed";

select
    count(distinct  id)
from public."oversea-api_osd_pushed"

select
    count(*)
from public."oversea-api_osd_personalized_push_configs"

-- 12514
-- 2101
select
    push_time
    , to_timestamp(updated_at) at time zone 'UTC-8' as push_time_c
from public."oversea-api_osd_personalize_push_statistic" where push_id = 8455

-- 查看个性化推送会不会在同一天推送两次
-- 首先确认了 created_at utc-8 对应了 push_time
-- 不会出现同一天推送了两次的情况
select
    push_id,
    to_timestamp(created_at) at time zone 'UTC-0',
    count(*)
from public."oversea-api_osd_personalize_push_statistic"
group by push_id, created_at
having count(*) = 1

select
    *
from dw_push_view
where push_id like '%person%'
limit 100


select
    *
from public."oversea-api_osd_personalize_push_statistic" where id

select
    *
from

select
    *
from public."oversea-api_osd_auto_push_logs"

select
    push_time
from public."oversea-api_osd_personalize_push_statistic"

select
    push_time
from public."oversea-api_osd_auto_push_logs"

-- 2523
select
    *
from public."oversea-api_osd_personalized_push_configs"
where vid = 4355

-- 2w
select
    *
from public."oversea-api_osd_personalize_push_statistic"
where push_id = 9078


select current_timestamp

with push_view as(
    select
        concat('person_',t.vid::text) as push_id
        ,t2.push_id as push
        , to_timestamp(t2.created_at) at time zone 'UTC-8' as push_time
--         , '未知'::text as layered_name
        , concat('{',case when t1.name = '中文简体' then '简体中文' when t1.name = '印尼语' then '印度尼西亚语' else t1.name end ,'}') as 语言
        , title as 标题
        , content as 内容
        , t2.sent_count as 推送人数
        , t2.delivered_count as 触达人数
        , t2.click_count as 点击人数
    from public."oversea-api_osd_personalized_push_configs" t
    left join public."oversea-api_osd_videos" t0 on t.vid = t0.id
    left join public."oversea-api_osd_categories" t1 on t0.category_id = t1.id
    left join public."oversea-api_osd_personalize_push_statistic" t2 on t.id = t2.push_id
    where to_timestamp(t2.created_at) between '2025-05-21' and '2025-05-28'
)
,tmp_push_log as (
    select
        push_id
        , d_date
        , sum(watch_duration) as 看剧时长                                 -- 看剧时长(分)
    from(
       select
            to_timestamp(created_at)::date as d_date    -- 数据日期
            , push_id                                   -- push_id
            , uid                                       -- 用户id
            , count(case when event=58 then uid else null end) as popup_pv
            , count(case when event in(1,2,13,14) and vid>0 and eid>0 then uid else null end) as watch_pv
            , round(sum(case when event=2 and vid>0 and eid>0 then watch_time else 0 end)/60.0,2) as watch_duration
            , count(distinct case when event in(1,2,13,14) and vid>0 and eid>0 then eid else null end) as watch_eid
       from public.app_user_track_log a
       where 1=1
            and event in (1,2,13,14)
            and push_id<>''
            and to_timestamp(created_at) between '2025-05-21' and '2025-05-28'
          -- and (push_id='3640' or push_id='3049' or push_id='3923')
          -- 58 进入充值弹窗就上报
       group by
            to_timestamp(created_at)::date
            , push_id
            , uid
    )t0
    group by
       push_id
        , d_date
)
,order_view as(
          select push_id
               , to_timestamp(created_at)::date as d_date
               ,sum(money) * 0.01 as 充值金额
          from public.all_order_log o
          where 1=1
              and o.environment = 1
              and o.status = 1
              and to_timestamp(created_at) between '2025-05-21' and '2025-05-28'
          group by push_id, to_timestamp(created_at)::date
)
-- 基础维度表
, tmp_primary as(
	select push_id ,d_date
	from(
		select distinct push_id ,d_date from tmp_push_log
		union all
		select distinct push_id ,d_date from order_view
		union all
		select distinct push_id ,(push_time at time zone 'UTC-8' at time zone 'UTC-0')::date from push_view
	)a
	where push_id is not null and push_id <>'' and d_date is not null
	group by push_id ,d_date
)
select t.push_id,t.d_date,t1.*,coalesce(t2.看剧时长,0) as 看剧时长,coalesce(t3.充值金额,0) as 充值金额
from tmp_primary t
left join push_view t1 on t1.push_id = t.push_id and (t1.push_time at time zone 'UTC-8' at time zone 'UTC-0')::date = t.d_date
left join tmp_push_log t2 on t.push_id = t2.push_id and t2.d_date = t.d_date
left join order_view t3 on t.push_id = t3.push_id and t3.d_date = t.d_date
where t.push_id like '%person%'




