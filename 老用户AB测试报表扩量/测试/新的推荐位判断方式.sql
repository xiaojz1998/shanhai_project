---------------------------------------------
-- File: 新的推荐位判断方式.sql
-- Time: 2025/6/19 15:08
-- User: xiaoj
-- Description:  
---------------------------------------------



-- 如何判断推荐位有效

-- 获取注册时间戳
-- 维度：uid
with tmp_user_info as (
    select
        uid::bigint
        , d_time::timestamp as register_timestamp
        , t.country_name
        , t0.country_grade  as country_grade
        , t.area
        , lang_name
        , case when uid::bigint % 100 < 50 and uid::bigint % 100 >= 0 then '对照组'
               when uid::bigint % 100 < 100 and uid::bigint % 100 >= 50 then '实验组'
           end as user_group            -- 对uid进行分组
        , ad_channel
    from dwd_user_info t
    left join v_dim_country_area t0 on t.country_code = t0.country_code
)
-- 获取用户曝光行为
-- 维度：uid 日期
, tmp_user_exposure as (
    select
        a.uid
        , to_timestamp(a.created_at)::date as d_date -- 日志日期
        , model_id as recommmend_id
        , min(to_timestamp(a.created_at)) as min_exposure_time  -- 每天最小曝光时间戳
        , b.register_timestamp
        , b.country_name
        , b.country_grade
        , b.area
        , b.lang_name
        , b.ad_channel
        , b.user_group
    from public.app_user_cover_show_log a
    left join tmp_user_info b on a.uid = b.uid                          -- 关联用户信息
    where event = 111                                                   -- 曝光埋点
        and to_timestamp(a.created_at)::date >= '2025-05-12'::date      -- 限制时间
        and CAST(ext_body::json ->> 'page' AS int) = 1                  -- 页数
    group by a.uid
           , to_timestamp(a.created_at)::date
           , model_id
           , b.register_timestamp
           , b.country_name
           , b.country_grade
           , b.area
           , b.lang_name
           , b.ad_channel
           , b.user_group
)
select
    *
from tmp_user_exposure a
left join "oversea-api_osd_recommend" b on a.recommmend_id::int = b.id  -- 过滤掉不启用的推荐位
where b.user_newuser_alg_status = 1