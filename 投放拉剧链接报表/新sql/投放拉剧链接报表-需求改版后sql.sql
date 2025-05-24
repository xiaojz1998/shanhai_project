------------------------------------------
-- file: 投放拉剧链接报表-需求改版后sql.sql
-- author: xiaoj
-- time: 2025/5/21 18:07
-- description:
------------------------------------------
set timezone ='UTC-0';
------------------------------------------
-- 建表
------------------------------------------
-- drop table if exists public.ads_campaign_link_statistics;
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
    , 深度链获取新增用户数 BIGINT
    , 深度链解析新增用户数 BIGINT
    , 剧集信息获取新增用户数 BIGINT
    , 视频加载新增用户数 BIGINT
);


------------------------------------------
-- 更新
------------------------------------------
drop table if exists tmp.ads_campaign_link_statistics_tmp;
create table tmp.ads_campaign_link_statistics_tmp as
with tmp_new_camp_user as (
    select
        d_date::date              -- 注册日期
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
        , ad_source_type    -- 归因通道
    from public.dwd_user_info a
    left join v_dim_country_area b on a.country_code = b.country_code       -- 获得国家级别
    where not (media_source in ('organic','unknown','') and (campaign_id is null or campaign_id = '' or campaign_id='0'))-- 判断推广流用户
         and d_date::date = '2025-05-20'::date                   -- 限制时间
)
, tmp_event_log as (
    select
        to_timestamp(created_at):: date as d_date   -- 日期
        , event                                     -- 时间
        , device_id                                 -- 设备id
        , case when vid is not null then vid::text
              else substring(request_url FROM 'playletId=([0-9]+)') end as vid -- 取到vid
        , case when type = '0' then 'W2A'
               when type = '1' then '直投'
               else '未知' end as type               -- 投放链路
        -- , case when ad_source_type = 0 then 1 else ad_source_type end as ad_source_type    -- 归因通道
    from "app_performance_event_log"
    where (event = 22 or event = 23 or event = 29 or event = 30) -- 限制event
        and to_timestamp(created_at):: date = '2025-05-20'::date  -- 限制时间
)
-- 按照维度 求出新增推广用户数
, tmp_new_camp_user_uv as (
    select
        d_date
        , country_name
        , area
        , country_grade
        , lang_name
        , vid
        , campaign_id
        , campaign_name
        , ad_channel
        , type
        , ad_source_type
        , count(distinct device_id) as "新增推广用户数"
    from tmp_new_camp_user
    group by d_date, country_name, area, country_grade, lang_name, vid, campaign_id, campaign_name, ad_channel, type, ad_source_type
)
-- 按照维度 求出埋点相关字段 新增用户数
, tmp_event_log_uv as (
    select
        -- 分组维度
        t.d_date
        , country_name
        , area
        , country_grade
        , lang_name
        , t.vid
        , campaign_id
        , campaign_name
        , ad_channel
        , t.type
        , t0.ad_source_type
        -- 计算字段
        , count(distinct case when event = 22 then t.device_id else null end) as "深度链获取新增用户数"
        , count(distinct case when event = 29 then t.device_id else null end) as "深度链解析新增用户数"
        , count(distinct case when event = 30 then t.device_id else null end) as "剧集信息获取新增用户数"
        , count(distinct case when event = 23 then t.device_id else null end) as "视频加载新增用户数"
    from tmp_event_log t
    inner join tmp_new_camp_user t0                                                 -- 筛选出真实新增用户
        on t.device_id = t0.device_id and t.vid = t0.vid and t.d_date = t0.d_date
    group by t.d_date, country_name, area, country_grade, lang_name, t.vid, campaign_id, campaign_name, ad_channel, t.type, t0.ad_source_type
)
-- 合并两个计算的基础维度
-- 防止有维度被漏掉
, tmp_primary as (
    select
        d_date,country_name,area,country_grade,lang_name,vid,name,campaign_id,campaign_name,ad_channel,t.type,ad_source_type
    from (select
        d_date,country_name,area,country_grade,lang_name,vid,campaign_id,campaign_name,ad_channel,type,ad_source_type
    from tmp_new_camp_user_uv
    union all
    select
        d_date,country_name,area,country_grade,lang_name,vid,campaign_id,campaign_name,ad_channel,type,ad_source_type
    from tmp_event_log_uv ) t left join "oversea-api_osd_videos" t0 on t.vid = t0.id::text          -- 补充剧名
    group by d_date,country_name,area,country_grade,lang_name,vid,name,campaign_id,campaign_name,ad_channel,t.type,ad_source_type
)
select
    -- 分组维度
    t.d_date as 日期
    , t.country_name as 国家
    , t.area as 区域
    , t.country_grade as T级国家
    , t.lang_name as 语言
    , t.vid as 剧id
    , t.name as 剧名
    , t.campaign_id as 广告系列id
    , t.campaign_name as 广告系列名称
    , t.ad_channel as 渠道
    , t.type as 投放链路
    , t.ad_source_type as 归因通道
    -- 计算字段
    , t0."新增推广用户数"
    , t1."深度链获取新增用户数"
    , t1."深度链解析新增用户数"
    , t1."剧集信息获取新增用户数"
    , t1."视频加载新增用户数"
from tmp_primary t
left join tmp_new_camp_user_uv t0
    on t.d_date = t0.d_date
           and t.country_name = t0.country_name
           and t.area = t0.area
           and t.country_grade = t0.country_grade
           and t.lang_name = t0.lang_name
           and t.vid = t0.vid
           and t.campaign_id = t0.campaign_id
           and t.campaign_name = t0.campaign_name
           and t.ad_channel = t0.ad_channel
           and t.type = t0.type
           and t.ad_source_type = t0.ad_source_type
left join tmp_event_log_uv t1
    on t.d_date = t1.d_date
           and t.country_name = t1.country_name
           and t.area = t1.area
           and t.country_grade = t1.country_grade
           and t.lang_name = t1.lang_name
           and t.vid = t1.vid
           and t.campaign_id = t1.campaign_id
           and t.campaign_name = t1.campaign_name
           and t.ad_channel = t1.ad_channel
           and t.type = t1.type
           and t.ad_source_type = t1.ad_source_type ;

-- 更新
delete from public.ads_campaign_link_statistics where 日期::date = '2025-05-20'::date ;
insert into public.ads_campaign_link_statistics
select * from tmp.ads_campaign_link_statistics_tmp;

