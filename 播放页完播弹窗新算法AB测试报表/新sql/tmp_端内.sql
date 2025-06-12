---------------------------------------------
-- File: tmp_端内.sql
-- Time: 2025/5/29 01:27
-- User: xiaoj
-- Description:  
---------------------------------------------

-- 播放页完播弹窗- 端内
-- drop table if exists tmp.tmp_dw_recommend_playfinish_app;
-- create table tmp.tmp_dw_recommend_playfinish_app as
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
        , case when uid::bigint % 2 = 0 then '对照组'
               when uid::bigint % 2 = 1 then '实验组'
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
        -- , CAST(a.ext_body::json ->> 'show_title' AS text) as recommend_name
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
        and to_timestamp(a.created_at)::date = '2025-05-10'::date       -- 限制时间
        and CAST(ext_body::json ->> 'page' AS int) = 3                  -- 页数
        and ext_body::json ->> 'show_title' = 'playFinish'              -- 播放页完播弹窗
    group by a.uid
           , to_timestamp(a.created_at)::date
           -- , CAST(a.ext_body::json ->> 'show_title' AS text)
           , b.register_timestamp
           , b.country_name
           , b.country_grade
           , b.area
           , b.lang_name
           , b.ad_channel
           , b.user_group
)
-- 获取用户曝光行为、用户信息
-- 维度：uid 日期
-- 要处理null的维度
, tmp_user_exposure_info as (
    select
        uid
        , d_date
        -- , recommend_name
        , CASE WHEN register_timestamp IS NULL THEN '未知'
            WHEN min_exposure_time <= (register_timestamp + INTERVAL '24 hours') THEN 'newuser'
            ELSE 'olduser' END AS user_type
        , coalesce(area,  '未知') as area
        , coalesce(country_name, '未知') as country_name
        , coalesce(country_grade, '未知') as country_grade
        , coalesce(ad_channel, '未知') as ad_channel
        , coalesce(user_group, '未知') as user_group
        , coalesce(lang_name, '未知') as lang_name
        , case when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 24 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 48 then '注册1天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 48 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 96 then '注册2-3天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 96 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 192 then '注册4-7天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 192 then '注册7天以上'
             else '未知' END AS registration_period
    from tmp_user_exposure
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
        and to_timestamp(created_at)::date = '2025-05-10'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_01"
    where type = 0
        and to_timestamp(created_at)::date = '2025-05-10'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_02"
    where type = 0
        and to_timestamp(created_at)::date = '2025-05-10'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_03"
    where type = 0
        and to_timestamp(created_at)::date = '2025-05-10'::date       -- 限制时间
    union all
    select
        to_timestamp(created_at) :: date as d_date
        , uid
        , goods_id
        , goods_sku_id
        , money
    from "middle_user_consume_record_04"
    where type = 0
        and to_timestamp(created_at)::date = '2025-05-10'::date       -- 限制时间
    ) t
)
-- 点击信息
, tmp_click_data as (
    select
        to_timestamp(a.created_at)::date as d_date
        , a.uid
        , event
        , case when order_id like '%SH%' then order_id else CONCAT('SH', order_id) end as order_id  -- 用于匹配支付
        , watch_time
        , money::numeric(20,2) as money                 -- 用于计算预估广告收入
        , vid                   -- k币
        , eid                   -- k币
        , CAST(a.ext_body::json ->> 'type' AS text) as type         -- 用于看广告解锁
        , b.user_type
        , b.area
        , b.country_name
        , b.country_grade
        , b.ad_channel
        , b.user_group
        , b.lang_name
        , b.registration_period
    from public.app_user_track_log a
    inner join tmp_user_exposure_info b on a.uid = b.uid and to_timestamp(a.created_at)::date = b.d_date    -- 找到当天有曝光的用户的点击行为
    where event in (112, 1, 192, 191, 2, 13, 14,263)                    -- 取的事件
        and to_timestamp(a.created_at)::date = '2025-05-10'::date       -- 限制时间
)
-- 支付信息
, tmp_payment_data as (
    select
        to_timestamp(a.created_at)::date as d_date
        , a.uid
        , order_num
        , money * 1.0 / 100 AS total_payment_amount
        , b.user_type
        , b.area
        , b.country_name
        , b.country_grade
        , b.ad_channel
        , b.user_group
        , b.lang_name
        , b.registration_period
    from public.all_order_log a
    inner join tmp_user_exposure_info b on a.uid = b.uid and to_timestamp(a.created_at)::date = b.d_date -- 过滤出当天有曝光的用户
    where status = 1                                                    -- 支付成功
        and environment = 1                                             -- 生产环境
        and to_timestamp(a.created_at)::date = '2025-05-10'::date       -- 限制时间
)
-- 对曝光信息进行聚合
, tmp_aggregated_exposure as (
    select
        d_date
        , user_type
        , area
        , country_name
        , country_grade
        , ad_channel
        , user_group
        , lang_name
        , registration_period
        , count(distinct uid) as exposure_uv            -- 曝光人数
    from tmp_user_exposure_info
    group by d_date, user_type, area, country_name,country_grade , ad_channel, user_group, lang_name, registration_period
)
-- 对点击信息进行聚合
, tmp_aggregated_click as (
    select
        d_date
        , user_type
        , area
        , country_name
        , country_grade
        , ad_channel
        , user_group
        , lang_name
        , registration_period
        , COUNT(DISTINCT CASE WHEN event IN (1, 2, 13, 14) THEN uid END) AS play_uv     -- 播放人数
        , COUNT(DISTINCT CASE WHEN event = 192 THEN uid END) AS recharge_submission_uv  -- 充值提交人数
        , COUNT(DISTINCT CASE WHEN event = 191 THEN uid END) AS episode_unlock_uv       -- 解锁剧集人数
        , COUNT(distinct CASE WHEN event IN (1, 2, 13, 14) THEN eid END) AS episode_play-- 播放集数
        , COUNT(CASE WHEN event = 192 THEN uid END) AS recharge_submission_pv           -- 充值提交次数
        , COUNT(distinct CASE WHEN event = 191 THEN eid END) AS episode_unlocks         -- 解锁剧集集数
        , count(distinct CASE WHEN event = 191 and type = '1' THEN eid END) as ad_episode_unlocks   --  广告解锁剧集集数
        , count(distinct CASE WHEN event = 191 and type = '2' THEN eid END) as non_ad_episode_unlocks -- 非广告解锁剧集集数
        , ROUND(COALESCE(SUM(CASE WHEN event = 2 THEN watch_time ELSE 0 END) / 60.0, 0)) AS watch_duration_minutes  -- 观看时长（分钟）
    from tmp_click_data
    group by d_date, user_type, area, country_name, country_grade, ad_channel, user_group, lang_name, registration_period
)
-- 对支付信息进行聚合
, tmp_aggregated_payment as (
    SELECT
        a.d_date
        , a.user_type
        , a.area
        , a.country_name
        , a.country_grade
        , a.ad_channel
        , a.user_group
        , a.lang_name
        , a.registration_period
        , COALESCE(COUNT(DISTINCT a.uid), 0) AS successful_payment_uv
        , COALESCE(COUNT(DISTINCT a.order_num), 0) AS successful_payment_pv
        , COALESCE(SUM(a.total_payment_amount), 0) AS total_payment_amount
    FROM tmp_payment_data a
    GROUP BY a.d_date, a.user_type,a.area , a.country_name, a.country_grade, a.ad_channel, a.user_group,a.lang_name, a.registration_period
)
-- 对k币信息进行聚合
, tmp_aggregated_k_consume as (
    select
        a.d_date
        , a.user_type
        , a.area
        , a.country_name
        , a.country_grade
        , a.ad_channel
        , a.user_group
        , a.lang_name
        , a.registration_period
        , sum(b.money) as k_consume_amount
    from tmp_click_data a           -- 曝光过滤过后的
    left join tmp_k_consume_data b on a.vid = b.goods_id and a.eid = b.goods_sku_id and a.d_date = b.d_date and a.uid = b.uid
    and a.event = 191
    group by a.d_date, a.user_type, a.area,a.country_name, a.country_grade, a.ad_channel, a.user_group, a.lang_name, a.registration_period
)
, tmp_aggregated_retention as (
    select
        a.d_date
        , a.user_type
        , a.area
        , a.country_name
        , a.country_grade
        , a.ad_channel
        , a.user_group
        , a.lang_name
        , a.registration_period
        , count(distinct b.uid) as retention_1_days
        , count(distinct c.uid) as retention_3_days
        , count(distinct d.uid) as retention_7_days
    from tmp_user_exposure_info a
    left join (select uid from dwd_user_active where d_date = '2025-05-10'::date + 1 group by  uid) b on a.uid = b.uid
    left join (select uid from dwd_user_active where d_date = '2025-05-10'::date + 3 group by  uid) c on a.uid = c.uid
    left join (select uid from dwd_user_active where d_date = '2025-05-10'::date + 7 group by  uid) d on a.uid = d.uid
    group by a.d_date, a.user_type, a.area,a.country_name, a.country_grade, a.ad_channel, a.user_group, a.lang_name, a.registration_period
)
select
    -- 分组维度
    a.d_date
    , a.user_type
    , a.user_group
    , a.area
    , a.country_name
    , a.country_grade
    , a.ad_channel
    , a.lang_name
    , a.registration_period
    -- 统计指标
    , exposure_uv
    , play_uv
    , recharge_submission_uv  -- 提交充值人数
    , episode_unlock_uv    -- 剧集解锁人数
    , episode_play  -- 播放集数
    , recharge_submission_pv  -- 提交充值次数
    , episode_unlocks  -- 解锁集数
    , ad_episode_unlocks       -- 广告解锁集数
    , non_ad_episode_unlocks   -- 非广告解锁集数
    , watch_duration_minutes   -- 观看时长
    , successful_payment_uv  -- 支付成功人数
    , successful_payment_pv  -- 支付成功次数
    , total_payment_amount  -- 总支付金额
    , k_consume_amount    -- 消耗k币数
    , retention_1_days         -- 1日留存
    , retention_3_days         -- 3日留存
    , retention_7_days         -- 7日留存
from tmp_aggregated_exposure a
left join tmp_aggregated_click b on a.d_date = b.d_date and a.user_type = b.user_type  and a.area = b.area and a.country_name = b.country_name and a.country_grade = b.country_grade and a.ad_channel = b.ad_channel and a.user_group = b.user_group and a.lang_name = b.lang_name and a.registration_period = b.registration_period
left join tmp_aggregated_payment c on a.d_date = c.d_date and a.user_type = c.user_type and a.area = c.area and a.country_name = c.country_name and a.country_grade = c.country_grade and a.ad_channel = c.ad_channel and a.user_group = c.user_group and a.lang_name = c.lang_name and a.registration_period = c.registration_period
left join tmp_aggregated_k_consume d on a.d_date = d.d_date and a.user_type = d.user_type  and a.area = d.area and a.country_name = d.country_name and a.country_grade = d.country_grade and a.ad_channel = d.ad_channel and a.user_group = d.user_group and a.lang_name = d.lang_name and a.registration_period = d.registration_period
left join tmp_aggregated_retention e on a.d_date = e.d_date and a.user_type = e.user_type  and a.area = e.area and a.country_name = e.country_name and a.country_grade = e.country_grade and a.ad_channel = e.ad_channel and a.user_group = e.user_group and a.lang_name = e.lang_name and a.registration_period = e.registration_period
where a.user_group is not null;
