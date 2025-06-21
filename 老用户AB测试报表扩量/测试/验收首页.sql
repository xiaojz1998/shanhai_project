---------------------------------------------
-- File: 验收首页.sql
-- Time: 2025/6/20 11:53
-- User: xiaoj
-- Description:  
---------------------------------------------
with tmp_user_exposure as (
    select
        a.uid
        , to_timestamp(a.created_at)::date as d_date                    -- 日志日期
        , a.model_id::text as recommend_id
        , c.chinese_name as recommend_name
        , min(to_timestamp(a.created_at)) as min_exposure_time          -- 用户当天推荐位最小曝光时间戳
        , b.d_time::timestamp as register_timestamp
        , b.country_name
        , b.area
        , b.lang_name
        , b.ad_channel
        , CASE
            WHEN a.uid::bigint % 100 >= 10 and a.uid::bigint %100 <= 59 THEN '对照组'
            WHEN (a.uid::bigint % 100 >= 0 and a.uid::bigint %100 <= 9) or (a.uid::bigint %100 >= 60 and a.uid::bigint %100 <= 99) THEN '实验组'
            ELSE NULL
            END AS user_group            -- 对uid进行分组
    from public.app_user_cover_show_log a
    left join dwd_user_info b on a.uid = b.uid::bigint                          -- 关联用户信息
    inner join "oversea-api_osd_recommend" c on a.model_id::int = c.id   -- 关联推荐位表 用于过滤推荐位
    where 1=1
        and event = 111                                                 -- 曝光埋点
        and to_timestamp(a.created_at)::date = '2025-06-19'::date            -- 限制时间
        and CAST(ext_body::json ->> 'page' AS int) = 1                  -- 页数
        and c.user_newuser_alg_status = 1                               -- 应用新用户推荐策略的推荐位
        -- and c.chinese_name not like '%泰语%'                          -- 泰语不扩量
    group by a.uid
           , to_timestamp(a.created_at)::date
           , a.model_id
           , c.chinese_name
           , b.d_time::timestamp
           , b.country_name
           , b.area
           , b.lang_name
           , b.ad_channel
)
-- 获取用户曝光行为、用户信息
-- 维度：uid 日期
-- 要处理null的维度
, tmp_user_exposure_info as (
    select
        uid
        , d_date
        , recommend_id
        , recommend_name
        , CASE WHEN register_timestamp IS NULL THEN '未知'
            WHEN min_exposure_time <= (register_timestamp + INTERVAL '24 hours') THEN '新用户'
            ELSE '老用户' END AS user_type
        , coalesce(area,  '未知') as area
        , coalesce(country_name, '未知') as country_name
        , coalesce(ad_channel, '未知') as ad_channel
        , coalesce(user_group, '未知') as user_group
        , coalesce(lang_name, '未知') as lang_name
        , case when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 24 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 48 then '注册1天'
            when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 48 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 96 then '注册2-3天'
            when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 96 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 192 then '注册4-7天'
            when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 192 then '注册7天以上'
            else '未知' END AS registration_period
    from tmp_user_exposure
    where register_timestamp IS not NULL and min_exposure_time > (register_timestamp + INTERVAL '24 hours') -- 限制老用户
)
, tmp_exposure_data as (
    select
        to_timestamp(a.created_at)::date as d_date
        , a.uid
        , b.user_type
        , b.area
        , b.country_name
        , b.ad_channel
        , b.user_group
        , b.recommend_id
        , b.recommend_name
        , b.lang_name
        , b.registration_period
    from public.app_user_cover_show_log a
    inner join tmp_user_exposure_info b on a.uid = b.uid and to_timestamp(a.created_at)::date = b.d_date and a.model_id::text = b.recommend_id-- 限制当天有曝光的
    where event = 111                                                   -- 曝光埋点
        and user_type = '老用户'                                         -- 限制老用户
        and to_timestamp(a.created_at)::date = '2025-06-19'::date
        and CAST(ext_body::json ->> 'page' AS int) = 1                  -- 页数
)
, tmp_click_data as (
    select
        to_timestamp(a.created_at)::date as d_date
        , a.uid
        , event
        , case when order_id like '%SH%' then order_id else CONCAT('SH', order_id) end as order_id  -- 用于匹配支付
        , watch_time
        -- , money::numeric(20,2) as money                 -- 用于计算预估广告收入
        , vid                   -- k币
        , eid                   -- k币
        , recommend_id
        , recommend_name
        --, CAST(a.ext_body::json ->> 'type' AS text) as type         -- 用于看广告解锁
        , b.user_type
        , b.area
        , b.country_name
        , b.ad_channel
        , b.user_group
        , b.lang_name
        , b.registration_period
    from public.app_user_track_log a
    inner join tmp_user_exposure_info b on a.uid = b.uid and to_timestamp(a.created_at)::date = b.d_date and NULLIF(a.column1, '')::text = b.recommend_id
    where event in (112, 1, 192, 191, 2, 13, 14)                    -- 取的事件
        and to_timestamp(a.created_at)::date = '2025-06-19'::date       -- 限制时间
        and CAST(ext_body::json ->> 'page' AS int) = 1                  -- 页数
        and (NULLIF(a.column1, '') ~ '^\d+$')
)
-- 支付信息
, tmp_payment_data as (
    select
        to_timestamp(a.created_at)::date as d_date
        , a.uid
        , order_num
        , money * 1.0 / 100 AS total_payment_amount
    from public.all_order_log a
    where status = 1                                                    -- 支付成功
        and environment = 1                                             -- 生产环境
        and to_timestamp(a.created_at)::date = '2025-06-19'::date       -- 限制时间
)
-- k币消耗信息
, tmp_k_consume_data as (
    select
        t.d_date
        , t.uid
        , goods_id
        , goods_sku_id
        , money
    from (select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_00"
    where type = 0
        and to_timestamp(created_at)::date = '2025-06-19'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_01"
    where type = 0
        and to_timestamp(created_at)::date = '2025-06-19'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_02"
    where type = 0
        and to_timestamp(created_at)::date = '2025-06-19'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_03"
    where type = 0
        and to_timestamp(created_at)::date = '2025-06-19'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_04"
    where type = 0
        and to_timestamp(created_at)::date = '2025-06-19'::date       -- 限制时间
    ) t
)
-- 对k币消耗信息进行聚合
-- 要用event = 191 uid和剧id关联
, tmp_aggregated_k_consume as (
    select
        a.d_date
        , a.user_type
        , a.area
        , a.country_name
        , a.ad_channel
        , a.user_group
        , a.recommend_id
        , a.recommend_name
        , a.lang_name
        , a.registration_period
        , sum(b.money) as k_consume_amount
    from tmp_click_data a
    left join tmp_k_consume_data b on a.vid = b.goods_id and a.eid = b.goods_sku_id and a.d_date = b.d_date and a.uid = b.uid
    and a.event = 191
    group by a.d_date, a.user_type, a.area,a.country_name, a.ad_channel, a.user_group,a.recommend_id,a.recommend_name, a.lang_name,a.registration_period
)
-- 对曝光信息进行聚合
, tmp_aggregated_exposure as (
    select
        d_date
        , user_type
        , recommend_id
        , recommend_name
        , area
        , country_name
        , ad_channel
        , user_group
        , lang_name
        , registration_period
        , COUNT(DISTINCT uid) AS exposure_uv
        , COUNT(*) AS exposure_pv
    from tmp_exposure_data
    group by d_date, user_type, recommend_id, recommend_name, area, country_name, ad_channel, user_group, lang_name, registration_period
)
-- 对点击信息进行聚合
, tmp_aggregated_click as (
    select
        d_date
        , user_type
        , recommend_id
        , recommend_name
        , area
        , country_name
        , ad_channel
        , user_group
        , lang_name
        , registration_period
        , COUNT(DISTINCT CASE WHEN event = 112 THEN uid END) AS click_uv                -- 点击人数
        , COUNT(DISTINCT CASE WHEN event IN (1, 2, 13, 14) THEN uid END) AS play_uv     -- 播放人数
        , COUNT(DISTINCT CASE WHEN event = 192 THEN uid END) AS recharge_submission_uv  -- 充值提交人数
        , COUNT(DISTINCT CASE WHEN event = 191 THEN uid END) AS episode_unlock_uv       -- 解锁剧集人数
        , COUNT(CASE WHEN event = 112 THEN uid END) AS click_pv                         -- 点击次数
        , COUNT(CASE WHEN event IN (1, 2, 13, 14) THEN uid END) AS episode_play-- 播放集数
        , COUNT(CASE WHEN event = 192 THEN uid END) AS recharge_submission_pv           -- 充值提交次数
        , COUNT(CASE WHEN event = 191 THEN uid END) AS episode_unlocks         -- 解锁剧集集数
        -- , count(distinct CASE WHEN event = 191 and type = '1' THEN concat(uid,'-',eid) END) as ad_episode_unlocks   --  广告解锁剧集集数
        -- , count(distinct CASE WHEN event = 191 and type != '1' THEN concat(uid,'-',eid) END) as non_ad_episode_unlocks -- 非广告解锁剧集集数
        -- , COALESCE(SUM(CASE WHEN event = 263 THEN money ELSE 0 END), 0) as estimated_ad_income  -- 预估广告收入
        , ROUND(COALESCE(SUM(CASE WHEN event = 2 THEN watch_time ELSE 0 END) / 60.0, 0)) AS watch_duration_minutes  -- 观看时长（分钟）
    from tmp_click_data
    group by d_date, user_type, recommend_id , recommend_name, area, country_name, ad_channel, user_group, lang_name , registration_period
)
-- 对支付信息进行聚合
, tmp_aggregated_payment as (
    SELECT
        a.d_date
        , a.user_type
        , a.area
        , a.country_name
        , a.ad_channel
        , a.user_group
        , a.recommend_id
        , a.recommend_name
        , a.lang_name
        , a.registration_period
        , COALESCE(COUNT(DISTINCT b.uid), 0) AS successful_payment_uv
        , COALESCE(COUNT(DISTINCT b.order_num), 0) AS successful_payment_pv
        , COALESCE(SUM(b.total_payment_amount), 0) AS total_payment_amount
    FROM tmp_click_data a
    left join tmp_payment_data b ON a.uid = b.uid AND a.d_date = b.d_date and a.event = 192 and a.order_id=b.order_num
    GROUP BY a.d_date, a.user_type,a.area , a.country_name, a.ad_channel, a.user_group, a.recommend_id,a.recommend_name,a.lang_name,a.registration_period
)
select
    sum(exposure_uv)
from (select
    -- 分组维度
    a.d_date
    , a.user_type
    , a.recommend_id
    , a.recommend_name
    , a.area
    , a.country_name
    , a.ad_channel
    , a.user_group
    , a.lang_name
    , a.registration_period
    -- 统计指标
    , COALESCE(a.exposure_uv, 0) AS exposure_uv                             -- 曝光人数
    , COALESCE(b.click_uv, 0) AS click_uv                                -- 点击人数
    , COALESCE(b.play_uv, 0) AS play_uv                                  -- 播放人数
    , COALESCE(b.recharge_submission_uv, 0) AS recharge_submission_uv       -- 充值提交人数
    , COALESCE(b.episode_unlock_uv, 0) AS episode_unlock_uv                 -- 剧集解锁人数
    , COALESCE(a.exposure_pv, 0) AS exposure_pv
    , COALESCE(b.click_pv, 0) AS click_pv                               -- 点击次数
    , COALESCE(b.episode_play, 0) AS episode_play                       -- 播放集数
    , COALESCE(b.recharge_submission_pv, 0) AS recharge_submission_pv   -- 充值提交次数
    , COALESCE(b.episode_unlocks, 0) AS episode_unlocks                 -- 解锁剧集集数
    -- , COALESCE(b.ad_episode_unlocks, 0) AS ad_episode_unlocks           -- 广告解锁剧集集数
    -- , COALESCE(b.non_ad_episode_unlocks, 0) AS non_ad_episode_unlocks   -- 非广告解锁剧集集数
    , cast(COALESCE(b.watch_duration_minutes, 0) as bigint)AS watch_duration_minutes
    , COALESCE(c.successful_payment_uv, 0) AS successful_payment_uv     --  成功支付人数
    , COALESCE(c.successful_payment_pv, 0) AS successful_payment_pv     -- 成功支付次数
    , cast(COALESCE(c.total_payment_amount, 0) as numeric(20,2)) AS total_payment_amount -- 总支付金额
    -- , cast(coalesce(b.estimated_ad_income,0) as numeric(20,2))as estimated_ad_income -- 预估广告收入
    , cast(coalesce(d.k_consume_amount,0) as numeric(20,2))as k_consume_amount  -- K币消耗
from tmp_aggregated_exposure a
left join tmp_aggregated_click b on a.d_date = b.d_date and a.user_type = b.user_type and a.recommend_id = b.recommend_id and a.recommend_name = b.recommend_name and a.area = b.area and a.country_name = b.country_name  and a.ad_channel = b.ad_channel and a.user_group = b.user_group and a.lang_name = b.lang_name and a.registration_period = b.registration_period
left join tmp_aggregated_payment c on a.d_date = c.d_date and a.user_type = c.user_type and a.recommend_id = b.recommend_id and a.recommend_name = c.recommend_name and a.area = c.area and a.country_name = c.country_name  and a.ad_channel = c.ad_channel and a.user_group = c.user_group and a.lang_name = c.lang_name and a.registration_period = c.registration_period
left join tmp_aggregated_k_consume d on a.d_date = d.d_date and a.user_type = d.user_type and a.recommend_id = b.recommend_id and a.recommend_name = d.recommend_name and a.area = d.area and a.country_name = d.country_name  and a.ad_channel = d.ad_channel and a.user_group = d.user_group and a.lang_name = d.lang_name and a.registration_period = d.registration_period
where a.user_group is not null ) t
where recommend_id = '427'




