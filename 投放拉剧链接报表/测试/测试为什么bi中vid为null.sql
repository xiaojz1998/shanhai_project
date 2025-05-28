---------------------------------------------
-- File: 测试为什么bi中vid为null.sql
-- Time: 2025/5/24 22:53
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
    left join v_dim_country_area b on a.country_code = b.country_code       -- 获得国家级别
    where not ((media_source in ('unkown','organic','') or media_source  is null) and (campaign_id is null or  campaign_id='' or campaign_id='0'))-- 判断推广流用户
         and d_date::date >= '2025-05-01'::date                             -- 限制时间
)
, tmp_event_log as (
    select
        to_timestamp(created_at):: date as d_date       -- 日期
        , event                                         -- 时间
        , device_id                                     -- 设备id
        , case when vid is not null then vid::text
               when substring(request_url FROM 'playletId=([0-9]+)') is not null then substring(request_url FROM 'playletId=([0-9]+)')
               when substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)') is not null then substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)')
               else '未知' end as vid                     -- 取到vid 2中url提取 , 且处理null为未知
        , case when type = '0' then 'W2A'
               when type = '1' then '直投'
               else '未知' end as type                    -- 投放链路
        -- , case when ad_source_type = 0 then 1 else ad_source_type end as ad_source_type      -- 归因通道 统一用用户表
    from "app_performance_event_log"
    where (event = 22 or event = 23 or event = 29 or event = 30)                                -- 限制event
         and ad_source_type != 2                                                                -- 过滤掉自归因的数据
         and to_timestamp(created_at):: date >= '2025-05-01'::date                               -- 限制时间
) select count(*) from tmp_new_camp_user where vid = ''