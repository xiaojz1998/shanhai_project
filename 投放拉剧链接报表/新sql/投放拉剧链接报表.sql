------------------------------------------
-- file: 投放拉剧链接报表.sql
-- author: xiaoj
-- time: 2025/5/14 17:34
-- description:
------------------------------------------
set timezone ='UTC-0';
------------------------------------------
--  建表
------------------------------------------
-- drop table if exists public.ads_campaign_link_statistics;
create table if not exists public.ads_campaign_link_statistics (
    -- 分组维度
    日期 date,
    区域 text,
    国家 text,
    T级国家 text,
    渠道 text,
    语言 text,
    剧id text,
    剧名 text,
    投放组 text,
    投放人员 text,
    广告系列id text,
    广告系列名称 text,
    -- 计算字段
    "获取到深度链用户数" bigint,
    "获取到deeplink用户数" bigint,
    "获取到onelink用户数" bigint,
    "设备登陆成功用户数" bigint,
    "deeplink设备登陆成功用户数" bigint,
    "onelink设备登陆成功用户数" bigint,
    "解析深度链成功用户数" bigint,
    "解析deeplink成功用户数" bigint,
    "获取onelink成功用户数" bigint,
    "获取剧集信息成功用户数" bigint,
    "deeplink获取剧集信息成功用户数" bigint,
    "onelink获取剧集信息成功用户数" bigint,
    "视频加载成功用户数" bigint,
    "deeplink视频加载成功用户数" bigint,
    "onelink视频加载成功用户数" bigint
);

-- 当天22、23、28、29、30埋点数据
drop table if exists tmp.ads_campaign_link_statistics_tmp;
create table tmp.ads_campaign_link_statistics_tmp as
with tmp_event_log as (
    select
        to_timestamp(created_at):: date as d_date
        ,event
        ,country_code
        ,cast( split_part(campaign_name, '_', 5) as text) as vid
        ,uid
        ,type
        ,campaign_id
        ,campaign_name
    from "app_performance_event_log"
    where (event = 22 or event = 23 or event = 28 or event = 29 or event = 30)
        -- 限制type 为deeplink和onelink
        and (type = '0' or type = '1')
        -- 过滤脏数据
        and campaign_id != '0' and campaign_name is not null
        -- 增量更新 一天
        and to_timestamp(created_at):: date = '2025-05-12'::date
)
-- 补全区域、国家名、t型国家
, tmp_country_info as (
    select
        country_code
        ,country_name
        ,area
        ,country_grade
    from v_dim_country_area
)
-- 补全语言
, tmp_ad_campaign_info as (
    select
        campaign_id
        ,lang_name
    from v_dim_ad_campaign_info
    group by campaign_id, lang_name
)
-- 补全渠道
, tmp_campaign_channel_info as (
    select
        campaign_id
        , ad_channel
    from ad_cost_data_log
    where
        -- 过滤掉脏数据
        campaign_id != 'None' and campaign_id != '93974' and campaign_id != '93975'
    group by campaign_id, ad_channel
)
select
    -- 分组维度
    d_date as "日期" -- 日期
    , area  as "区域"-- 区域
    , country_name  as "国家"-- 国家
    , country_grade  as "T级国家"-- 国家等级
    , ad_channel as "渠道"-- 渠道
    , lang_name  as "语言"-- 语言
    , vid   as "剧id"     -- 剧id
    , name  as "剧名"     -- 剧名
    , case
        when upper(split_part(campaign_name, '_', 8)) = 'G1' then '广州一组'
        when upper(split_part(campaign_name, '_', 8)) = 'G2' then '广州二组'
        when upper(split_part(campaign_name, '_', 8)) = 'G3' then '广州三组'
        when upper(split_part(campaign_name, '_', 8)) = 'G4' then '广州四组'
        when upper(split_part(campaign_name, '_', 8)) = 'H1' then '杭州一组'
        when upper(split_part(campaign_name, '_', 8)) = 'Z1' then '深圳一组'
        when upper(split_part(campaign_name, '_', 8)) = 'Z2' then '深圳二组'
        else '其他'
      end as "投放组"
    , split_part(campaign_name, '_', 9) as "投放人员"
    , a.campaign_id as "广告系列id"
    , campaign_name as "广告系列名称"
    -- 计算字段
    -- 22
    , count(distinct case when event = 22 then uid else null end) as "获取到深度链用户数"
    , count(distinct case when event = 22 and a.type = '1' then uid else null end) as "获取到deeplink用户数"
    , count(distinct case when event = 22 and a.type = '0' then uid else null end) as "获取到onelink用户数"
    -- 28
    , count(distinct case when event = 28 then uid else null end) as "设备登陆成功用户数"
    , count(distinct case when event = 28 and a.type = '1' then uid else null end) as "deeplink设备登陆成功用户数"
    , count(distinct case when event = 28 and a.type = '0' then uid else null end) as "onelink设备登陆成功用户数"
    -- 29
    , count(distinct case when event = 29 then uid else null end) as "解析深度链成功用户数"
    , count(distinct case when event = 29 and a.type = '1' then uid else null end) as "解析deeplink成功用户数"
    , count(distinct case when event = 29 and a.type = '0' then uid else null end) as "解析onelink成功用户数"
    -- 30
    , count(distinct case when event = 30 then uid else null end) as "获取剧集信息成功用户数"
    , count(distinct case when event = 30 and a.type = '1' then uid else null end) as "deeplink获取剧集信息成功用户数"
    , count(distinct case when event = 30 and a.type = '0' then uid else null end) as "onelink获取剧集信息成功用户数"
    -- 23
    , count(distinct case when event = 23 then uid else null end) as "视频加载成功用户数"
    , count(distinct case when event = 23 and a.type = '1' then uid else null end) as "deeplink视频加载成功用户数"
    , count(distinct case when event = 23 and a.type = '0' then uid else null end) as "onelink视频加载成功用户数"
from tmp_event_log a
left join "oversea-api_osd_videos" b on a.vid = b.id::text
left join tmp_country_info c on a.country_code = c.country_code
left join tmp_ad_campaign_info d on a.campaign_id = d.campaign_id
left join tmp_campaign_channel_info e on a.campaign_id = e.campaign_id
group by d_date, area, country_name, country_grade, ad_channel, lang_name, vid, name, campaign_name, a.campaign_id;


-- 增量更新
delete from public.ads_campaign_link_statistics where 日期 = '2025-05-12'::date;
insert into public.ads_campaign_link_statistics
select * from tmp.ads_campaign_link_statistics_tmp;

