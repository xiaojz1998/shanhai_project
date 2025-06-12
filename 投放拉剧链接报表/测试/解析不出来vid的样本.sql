---------------------------------------------
-- File: 解析不出来vid的样本.sql
-- Time: 2025/6/5 10:19
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
        , app_version
        , request_url
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
        , app_version
        , request_url
        , case when vid is not null then vid::text
               when substring(request_url FROM 'playletId=([0-9]+)') is not null then substring(request_url FROM 'playletId=([0-9]+)')
               when substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)') is not null then substring(request_url from 'playletId[%][0-9A-Fa-f]+[Dd]([0-9]+)')
               else '未知' end
)
select
    distinct t22_1.request_url
    , app_version
from tmp_new_camp_user t
left join "oversea-api_osd_videos" tv on t.vid = tv.id::text                    -- 补充剧名
left join tmp_event_22_log t22_1 on t.d_date = t22_1.d_date and t.device_id = t22_1.device_id and t22_1.vid = '未知'
where t.d_date = '2025-06-02' and t.ad_source_type  =  '1' and t.type = 'W2A'