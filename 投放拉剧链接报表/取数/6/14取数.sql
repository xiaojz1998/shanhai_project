---------------------------------------------
-- File: 14取数.sql
-- Time: 2025/6/14 13:22
-- User: xiaoj
-- Description:  
---------------------------------------------
with tmp_new_camp_user as (
    select
        d_date::date        -- 注册日期
        , device_id         -- 设备id
        , a.country_name    -- 国家
        , b.country_grade   -- T级国家
        , a.area            -- 区域
        , ad_channel        -- 渠道
        , lang_name         -- 语言
        , campaign_id       -- 广告Id
        , campaign_name     -- 广告名
        , vid               -- 剧id
        , case when lower((regexp_split_to_array(campaign_name, '_'))[3]::text) like '%w2a%' then 'W2A' else '直投' end as type-- 投放链路
        , ad_source_type    -- 归因通道 统一用用户表的，因为埋点表的不准确
    from public.dwd_user_info a
    left join v_dim_country_area b on a.country_code = b.country_code           -- 获得国家级别
    where user_source = '推广流'                                                 -- 判断推广流用户
         and d_date::date >= '2025-05-01'
         -- and d_date::date = '${dt}'::date                                    -- 限制时间
)
-- 下面单独取出4个埋点
, tmp_event_22_log as (
    select
        to_timestamp(created_at):: date as d_date       -- 日期
        -- , event                                         -- 时间
        , device_id                                     -- 设备id
        , case when vid is not null then vid::text
               when substring(request_url FROM 'playletId=([0-9]+)') is not null then substring(request_url FROM 'playletId=([0-9]+)')
               when substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)') is not null then substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)')
               else '未知' end as vid                     -- 取到vid 2中url提取 , 且处理null为未知
        -- , case when type = '0' then 'W2A'
        --        when type = '1' then '直投'
        --        else '未知' end as type                    -- 投放链路 用用户表的投放链路
        -- , case when ad_source_type = 0 then 1 else ad_source_type end as ad_source_type      -- 归因通道 统一用用户表
    from "app_performance_event_log"
    where (event = 22 or event = 23 or event = 29 or event = 30)                                -- 限制event
         -- and ad_source_type != 2                                                             -- 过滤掉自归因的数据
          and to_timestamp(created_at):: date >= '2025-05-01'
         -- and to_timestamp(created_at):: date = '${dt}'::date                               -- 限制时间
    group by to_timestamp(created_at):: date                                                  -- 去重
        ,  device_id
        , case when vid is not null then vid::text
               when substring(request_url FROM 'playletId=([0-9]+)') is not null then substring(request_url FROM 'playletId=([0-9]+)')
               when substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)') is not null then substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)')
               else '未知' end
)
, tmp_event_29_log as (
    select
        to_timestamp(created_at):: date as d_date       -- 日期
        -- , event                                         -- 时间
        , device_id                                     -- 设备id
        , case when vid is not null then vid::text
               when substring(request_url FROM 'playletId=([0-9]+)') is not null then substring(request_url FROM 'playletId=([0-9]+)')
               when substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)') is not null then substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)')
               else '未知' end as vid                     -- 取到vid 2中url提取 , 且处理null为未知
        -- , case when type = '0' then 'W2A'
        --        when type = '1' then '直投'
        --        else '未知' end as type                    -- 投放链路 用用户表的投放链路
        -- , case when ad_source_type = 0 then 1 else ad_source_type end as ad_source_type      -- 归因通道 统一用用户表
    from "app_performance_event_log"
    where (event = 23 or event = 29 or event = 30)                                -- 限制event
         -- and ad_source_type != 2                                                             -- 过滤掉自归因的数据
          and to_timestamp(created_at):: date >= '2025-05-01'
         -- and to_timestamp(created_at):: date = '${dt}'::date                               -- 限制时间
    group by to_timestamp(created_at):: date                                                  -- 去重
        ,  device_id
        , case when vid is not null then vid::text
               when substring(request_url FROM 'playletId=([0-9]+)') is not null then substring(request_url FROM 'playletId=([0-9]+)')
               when substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)') is not null then substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)')
               else '未知' end
)
, tmp_event_30_log as (
    select
        to_timestamp(created_at):: date as d_date       -- 日期
        -- , event                                         -- 时间
        , device_id                                     -- 设备id
        , case when vid is not null then vid::text
               when substring(request_url FROM 'playletId=([0-9]+)') is not null then substring(request_url FROM 'playletId=([0-9]+)')
               when substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)') is not null then substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)')
               else '未知' end as vid                     -- 取到vid 2中url提取 , 且处理null为未知
        -- , case when type = '0' then 'W2A'
        --        when type = '1' then '直投'
        --        else '未知' end as type                    -- 投放链路 用用户表的投放链路
        -- , case when ad_source_type = 0 then 1 else ad_source_type end as ad_source_type      -- 归因通道 统一用用户表
    from "app_performance_event_log"
    where (event = 23  or event = 30)                                -- 限制event
         -- and ad_source_type != 2                                                             -- 过滤掉自归因的数据
          and to_timestamp(created_at):: date >= '2025-05-01'
         -- and to_timestamp(created_at):: date = '${dt}'::date                               -- 限制时间
    group by to_timestamp(created_at):: date                                                  -- 去重
        ,  device_id
        , case when vid is not null then vid::text
               when substring(request_url FROM 'playletId=([0-9]+)') is not null then substring(request_url FROM 'playletId=([0-9]+)')
               when substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)') is not null then substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)')
               else '未知' end
)
, tmp_event_23_log as (
    select
        to_timestamp(created_at):: date as d_date       -- 日期
        -- , event                                         -- 时间
        , device_id                                     -- 设备id
        , case when vid is not null then vid::text
               when substring(request_url FROM 'playletId=([0-9]+)') is not null then substring(request_url FROM 'playletId=([0-9]+)')
               when substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)') is not null then substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)')
               else '未知' end as vid                     -- 取到vid 2中url提取 , 且处理null为未知
        -- , case when type = '0' then 'W2A'
        --        when type = '1' then '直投'
        --        else '未知' end as type                    -- 投放链路 用用户表的投放链路
        -- , case when ad_source_type = 0 then 1 else ad_source_type end as ad_source_type      -- 归因通道 统一用用户表
    from "app_performance_event_log"
    where (event = 23)                                -- 限制event
         -- and ad_source_type != 2                                                             -- 过滤掉自归因的数据
          and to_timestamp(created_at):: date >= '2025-05-01'
         -- and to_timestamp(created_at):: date = '${dt}'::date                               -- 限制时间
    group by to_timestamp(created_at):: date                                                  -- 去重
        ,  device_id
        , case when vid is not null then vid::text
               when substring(request_url FROM 'playletId=([0-9]+)') is not null then substring(request_url FROM 'playletId=([0-9]+)')
               when substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)') is not null then substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)')
               else '未知' end
)
select
    t.d_date as 日期
    , t22_0.device_id as device_id
    , country_name as 国家
    , area as 区域
    , country_grade as T级国家
    , lang_name as 语言
    , t.vid as 剧id
    , tv.name as 剧名
    , campaign_id as 广告系列id
    , campaign_name as 广告系列名称
    , ad_channel as 渠道
    , t.type as 投放链路
    , t.ad_source_type as 归因通道
from tmp_new_camp_user t
left join "oversea-api_osd_videos" tv on t.vid = tv.id::text                    -- 补充剧名
left join (select d_date,device_id from tmp_event_22_log group by d_date, device_id) t22_0 on t.d_date = t22_0.d_date and t.device_id = t22_0.device_id  -- 不带vid链接的
left join (select d_date,device_id from tmp_event_29_log group by d_date, device_id) t29_0 on t.d_date = t29_0.d_date and t.device_id = t29_0.device_id
where t22_0.device_id is not null and t29_0.device_id is null and t.d_date = '2025-06-11' and t.ad_source_type = '2' and t.type = 'W2A'