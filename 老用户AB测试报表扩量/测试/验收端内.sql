---------------------------------------------
-- File: 验收端内.sql
-- Time: 2025/6/20 15:29
-- User: xiaoj
-- Description:  
---------------------------------------------

with user_type_derivation as (
    SELECT DISTINCT
        a.uid
        , CASE
            WHEN a.uid % 100 >= 10 and a.uid %100 <= 59 THEN '对照组'         -- 扩量后
            WHEN (a.uid % 100 >= 0 and a.uid %100 <= 9) or (a.uid %100 >= 60 and a.uid %100 <= 99) THEN '实验组'   -- 扩量后
            ELSE NULL END AS user_group
        , to_timestamp(a.created_at)::date as 日期  -- 日期
        , '老用户' as user_type
        , b.area
        , b.country_name
        , b.ad_channel
        , b.lang_name
        , case when EXTRACT(epoch from  min(to_timestamp(a.created_at)) - b.d_time::timestamp)/3600 > 24 and EXTRACT(epoch from  min(to_timestamp(a.created_at)) - b.d_time::timestamp)/3600<= 48 then '注册1天'
             when EXTRACT(epoch from  min(to_timestamp(a.created_at)) - b.d_time::timestamp)/3600 > 48 and EXTRACT(epoch from  min(to_timestamp(a.created_at)) - b.d_time::timestamp)/3600<= 96 then '注册2-3天'
             when EXTRACT(epoch from  min(to_timestamp(a.created_at)) - b.d_time::timestamp)/3600 > 96 and EXTRACT(epoch from  min(to_timestamp(a.created_at)) - b.d_time::timestamp)/3600<= 192 then '注册4-7天'
             when EXTRACT(epoch from  min(to_timestamp(a.created_at)) - b.d_time::timestamp)/3600 > 192 then '注册7天以上'
             else '未知' END AS registration_period
    FROM "app_user_cover_show_log"  a
    left join public.dwd_user_info b on a.uid::text = b.uid                     -- 关联用户表  限制老用户
    left join "oversea-api_osd_recommend" c on a.model_id::int = c.id           -- 关联推荐位  限制推荐位
    WHERE 1=1
        and to_timestamp(a.created_at)::date  BETWEEN '2025-06-19'::date-7 and  '2025-06-19'::date --从5月26日开始
        AND event = 111                                         -- 曝光event
        AND CAST(ext_body::json ->> 'page' AS int) = 1          -- 首页
        and c.user_newuser_alg_status = 1                       -- 应用新用户推荐策略的推荐位
        -- and c.chinese_name not like '%泰语%'                  -- 扩量不扩泰语
    group by a.uid , to_timestamp(a.created_at)::date,b.d_time,b.area, b.country_name, b.ad_channel, b.lang_name -- 按照uid 日期 推荐位 分组
    having min(to_timestamp(a.created_at)) > b.d_time::timestamp + INTERVAL '24 hours' -- 限制老用户
)
select count(distinct uid) from user_type_derivation where 日期 = '2025-06-19' and user_group = '对照组'