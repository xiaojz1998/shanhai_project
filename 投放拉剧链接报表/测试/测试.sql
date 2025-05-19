------------------------------------------
-- file: 测试.sql
-- author: xiaoj
-- time: 2025/5/14 17:43
-- description:
------------------------------------------

------------------------------------------
-- 性能埋点分析
------------------------------------------
select
    *
from "app_performance_event_log"
where event = 22
limit 100;

-- 各个埋点最早日期
-- 22 2024-12-04
-- 23 2025-01-02
-- 28 2021-08-23
-- 29 2025-03-16
-- 30 2025-03-17
select
    min(to_timestamp(created_at)::date)
from "app_performance_event_log"
where event = 30


-- 各个埋点的触发数量
-- 确定漏斗条件
-- 22 179653
-- 28 309795
-- 29 228677
-- 30 134974
-- 23 121621
select
    count(*)
from "app_performance_event_log"
where event = 29
      -- 限制type 为deeplink和onelink
      and (type = '0' or type = '1')
      and to_timestamp(created_at)::date = '2025-05-12';

------------------------------------------
-- ad_cost_data_log 数据量 45697568
------------------------------------------
select
    count(*)
from ad_cost_data_log;

-- 210838
select
    campaign_id,
    count(*)
from (select
    -- count(distinct campaign_id) -- 210829
    campaign_id,ad_channel
from ad_cost_data_log
group by campaign_id,ad_channel) t
group by campaign_id
having count(*) >1

select
    -- count(distinct campaign_id) -- 210829
    campaign_id,ad_channel
from ad_cost_data_log
group by campaign_id,ad_channel
having campaign_id = '93974'

------------------------------------------
-- 测试 null
------------------------------------------
with tmp_event_log as (
    select
        to_timestamp(created_at):: date as d_date
        ,event
        ,country_code
        ,vid
        ,uid
        ,request_url
        ,campaign_id
        ,campaign_name
    from "app_performance_event_log"
    where (event = 22 or event = 23 or event = 28 or event = 29 or event = 30)
        -- 增量更新 一天
        and to_timestamp(created_at):: date = '2025-05-12'::date
)select
     count(*)
from tmp_event_log
where campaign_id is null


select
    count(*)
from "app_performance_event_log"
where country_code is null


-- 测试type
select
    type
from "app_performance_event_log"
where to_timestamp(created_at)::date = '2025-05-12'::date
group by type


-------------------------------------------------------------
-- 测试sql
-------------------------------------------------------------
with dim_tb as
    (
        select
            device_id as device_id
             ,lang_name
             ,d_date::date as imp_date
             ,campaign_id
        from
            dwd_user_info
        where d_date::date>='20250501'
    )
   ,success_tb as
    (
        select
            device_id  as device_id
             ,campaign_id
             ,to_timestamp(created_at)::date as act_date
             ,event
        from
            app_performance_event_log
        where event in (22,23,27,28,29,30,247)
          and to_timestamp(created_at)::date>='20250501'  and ad_source_type = 1 or ad_source_type = 0
          AND CASE
                  WHEN split_part(app_version, '.', 2) ~ '^\d+$' THEN split_part(app_version, '.', 2)::bigint
                  ELSE 0
                  END >= 18
    )
select
    imp_date as 日期

     ,count(distinct a.device_id) as 新增用户量
     ,count(distinct case when a.campaign_id <> '0' and a.campaign_id <> ''  then a.device_id end ) as 推广量
     ,count(distinct case when 1=1
    and event=22
                              then b.device_id end ) as 获取量22
     ,count(distinct case when 1=1
    and event=29
                              then b.device_id end ) as 解析量29
     ,count(distinct case when 1=1
    and event=30
                              then b.device_id end ) as 获取集信息量30



from
    dim_tb a
        left join success_tb b
                  on a.device_id=b.device_id  and  a.imp_date=b.act_date

group by imp_date
order by imp_date;