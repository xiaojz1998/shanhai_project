------------------------------------------
-- file: 检验.sql
-- author: xiaoj
-- time: 2025/5/13 16:37
-- description:
------------------------------------------
set timezone ='UTC-0';
with tmp_user_ad_log as (
    -- 埋点表和性能埋点表 取到广告相关条
    select
        uid,
        created_at,
        to_timestamp(created_at)::date as d_date,
        country_code,
        event,
        event_name,
        case when event in (39,36) then '签到广告'
            when event = 262 then '开屏/插屏广告'
            when event = 191 then '剧集解锁广告'
            when event = 41 then '阶梯广告'
            when event = 266 then 'firefly广告'
            when event = 269 then 'adcloud广告' end as ad_type
    from app_user_track_log
    where to_timestamp(created_at)::date >= '2024-09-01'    -- 2024/9/14 有广告数据
         and (event in (262,266,269,39,36) or (event = 41 and GET_JSON_OBJECT(ext_body,'$.task') = 'Watch Ads' ) or (event = 191 and ext_body::json->>'type' = '1'))
         -- 全量更新
         and to_timestamp(created_at)::date >= '2025-03-01'
         -- 增量更新
         -- and to_timestamp(created_at)::date >= (current_date+interval '-2 day')::date

    union all

     select
        uid,
        created_at,
        to_timestamp(created_at)::date as d_date,
        country_code,
        event,
        event_name,
        case when type = '0' then '阶梯广告'
            when type = '1' or type = '2' then '签到广告'
            when type = '3' then '剧集解锁广告' end as ad_type
    from app_performance_event_log
    where to_timestamp(created_at)::date >= '2024-10-15'    -- 2024/10/15 有广告数据
        and event in (6,7,8,9) and type in ('0','1','2','3')
        -- 全量更新
        and to_timestamp(created_at)::date >= '2025-03-01'
        -- 增量更新
        -- and to_timestamp(created_at)::date >= (current_date+interval '-2 day')::date
) select d_date ,count(distinct case when event = 191 then uid else null end ) from tmp_user_ad_log
  group by d_date
  order by d_date desc