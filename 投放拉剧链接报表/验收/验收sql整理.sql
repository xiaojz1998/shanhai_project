---------------------------------------------
-- File: 验收sql整理.sql
-- Time: 2025/5/27 11:00
-- User: xiaoj
-- Description:  
---------------------------------------------

-- 找到新注册推广流用户
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
         and d_date::date = '2025-05-20'::date                             -- 限制时间
)
-- 埋点相关信息
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
         and to_timestamp(created_at):: date = '2025-05-20'::date                               -- 限制时间
)
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
    , count(distinct case when event = 22 then t0.device_id else null end) as "深度链获取新增用户数"
    , count(distinct case when event = 29 then t0.device_id else null end) as "深度链解析新增用户数"
    , count(distinct case when event = 30 then t0.device_id else null end) as "剧集信息获取新增用户数"
    , count(distinct case when event = 23 then t0.device_id else null end) as "视频加载新增用户数"
from tmp_new_camp_user t
left join "oversea-api_osd_videos" tv on t.vid = tv.id::text                    -- 补充剧名
left join tmp_event_log t0
    on t.d_date = t0.d_date and t.device_id = t0.device_id  and t.vid = t0.vid  -- 用日期 设备id vid 关联
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
        , t.ad_source_type;