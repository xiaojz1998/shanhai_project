---------------------------------------------
-- File: 投放拉剧链接报表v4.sql
-- Time: 2025/6/3 10:45
-- User: xiaoj
-- Description:  
---------------------------------------------
------------------------------------------
-- 修改任务：
--  后发生的埋点，需要对前置丢失的埋点进行补充上报。
--  报表新增深度链接获取用户数（不做vid匹配），用于判断受不匹配的影响的加载
-- 参考顺序： 22 29 30 23
------------------------------------------
set timezone ='UTC-0';
------------------------------------------
-- 建表
------------------------------------------
drop table if exists public.ads_campaign_link_statistics;
create table if not exists public.ads_campaign_link_statistics (
    -- 分组维度
    日期  date
    , 国家  text
    , 区域  text
    , T级国家  text
    , 语言  text
    , 剧id  text
    , 剧名  text
    , 广告系列id  text
    , 广告系列名称  text
    , 渠道  text
    , 投放链路  text
    , 归因通道  text
    --  计算字段
    , 新增推广用户数 BIGINT
    , "深度链获取新增用户数(不关联vid)" BIGINT
    , "深度链获取新增用户数(关联vid)" BIGINT
    , 深度链解析新增用户数 BIGINT
    , 剧集信息获取新增用户数 BIGINT
    , 视频加载新增用户数 BIGINT
);
------------------------------------------
-- 更新
------------------------------------------
drop table if exists tmp.tmp_ads_campaign_link_statistics;
create table tmp.tmp_ads_campaign_link_statistics as
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
-- select
--     日期
--     , sum("新增推广用户数")
--     , sum("深度链获取新增用户数(关联vid)")
--     , sum("深度链解析新增用户数")
--     , sum("剧集信息获取新增用户数")
--     , sum("视频加载新增用户数")
--     , 1.0*sum("深度链获取新增用户数(关联vid)")/sum("新增推广用户数")
--     , 1.0*sum("深度链解析新增用户数")/sum("深度链获取新增用户数(关联vid)")
--     , 1.0*sum("剧集信息获取新增用户数")/sum("深度链解析新增用户数")
--     , 1.0*sum("视频加载新增用户数")/sum("剧集信息获取新增用户数")
-- from (
select
    t.d_date as 日期
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
    , count(distinct t.device_id) as "新增推广用户数"
    , count(distinct t0.device_id) as "深度链获取新增用户数(不关联vid)"
    , count(distinct t1.device_id) as "深度链获取新增用户数(关联vid)"
    , count(distinct t2.device_id) as "深度链解析新增用户数"
    , count(distinct t3.device_id) as "剧集信息获取新增用户数"
    , count(distinct t4.device_id) as "视频加载新增用户数"
from tmp_new_camp_user t
left join "oversea-api_osd_videos" tv on t.vid = tv.id::text                    -- 补充剧名
left join (select d_date,device_id from tmp_event_22_log group by d_date, device_id) t0 on t.d_date = t0.d_date and t.device_id = t0.device_id  -- 不带vid链接的
left join tmp_event_22_log t1 on t.d_date = t1.d_date and t.device_id = t1.device_id and t.vid = t1.vid
left join tmp_event_29_log t2 on t.d_date = t2.d_date and t.device_id = t2.device_id and t.vid = t2.vid
left join tmp_event_30_log t3 on t.d_date = t3.d_date and t.device_id = t3.device_id and t.vid = t3.vid
left join tmp_event_23_log t4 on t.d_date = t4.d_date and t.device_id = t4.device_id and t.vid = t4.vid
group by t.d_date
        , country_name
        , area
        , country_grade
        , lang_name
        , t.vid
        , tv.name
        , campaign_id
        , campaign_name
        , ad_channel
        , t.type
        , t.ad_source_type
-- ) t
-- where 投放链路 = 'W2A' and 归因通道='1'
-- group by t.日期
truncate table public.ads_campaign_link_statistics;
insert into public.ads_campaign_link_statistics
select * from tmp.tmp_ads_campaign_link_statistics;