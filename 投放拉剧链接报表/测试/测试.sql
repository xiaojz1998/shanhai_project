------------------------------------------
-- file: 测试.sql
-- author: xiaoj
-- time: 2025/5/22 14:18
-- description:
------------------------------------------

-- 测试 剧集表
select count(distinct id) from "oversea-api_osd_videos"

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
) select sum("新增推广用户数") from tmp_new_camp_user_uv