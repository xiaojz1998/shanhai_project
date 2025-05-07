------------------------------------------
-- file: 测试首页.sql
-- author: xiaoj
-- time: 2025/4/25 17:07
-- description:
------------------------------------------
----------------------------------------------------
-- dim_homepage_user 正确
----------------------------------------------------
-- 对照组 1443
-- 实验组 1366
-- select
--     id,
--     CASE
--         WHEN RIGHT(CAST(uid AS VARCHAR), 2) in ('10','11','12','13','14','15','16','17','18','19') THEN '对照组'
--         WHEN RIGHT(CAST(uid AS VARCHAR), 2) in ('00','01','02','03','04','05','06','07','08','09') THEN '实验组'
--         ELSE NULL
--     END AS user_group,
--     count(distinct uid) as c
-- from public.dim_homepage_user
-- where d_date = '2025-04-21' and user_type='olduser' and id = 211
-- group by CASE
--             WHEN RIGHT(CAST(uid AS VARCHAR), 2) in ('10','11','12','13','14','15','16','17','18','19') THEN '对照组'
--             WHEN RIGHT(CAST(uid AS VARCHAR), 2) in ('00','01','02','03','04','05','06','07','08','09') THEN '实验组'
--             ELSE NULL
--         END,id

----------------------------------------------------
-- 测试我写的曝光人数
----------------------------------------------------
WITH user_registration AS (
    SELECT
        d_date AS register_date,
        uid::int8 AS uid,
        country_name,
        ad_channel,
        lang_name,              -- 新加入语言名称
        CASE
            WHEN RIGHT(CAST(uid AS VARCHAR), 2) in ('10','11','12','13','14','15','16','17','18','19') THEN '对照组'
            WHEN RIGHT(CAST(uid AS VARCHAR), 2) in ('00','01','02','03','04','05','06','07','08','09') THEN '实验组'
            ELSE NULL
        END AS user_group
    FROM dwd_user_info
),
-- 每一日用户曝光的基本信息维表
-- 维度： 日期 uid
-- 用来补全信息  排除土耳其语和越南语用户 判断新老用户
user_type_info AS (
    SELECT
        d.uid,
        d.d_date AS 日期,
        COALESCE(d.user_type, 'Unknown') AS user_type,
        d.id,                   -- 推荐位id
        d.chinese_name,
        ur.country_name,
        ur.ad_channel,
        ur.user_group,
        ur.lang_name,
        case when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 24 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 48 then '注册1天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 48 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 96 then '注册2-3天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 96 and EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600<= 192 then '注册4-7天'
             when EXTRACT(epoch from  min_exposure_time - register_timestamp)/3600 > 192 then '注册7天以上'
             else '未知' END AS registration_period
    FROM public.dim_homepage_user d
    LEFT JOIN user_registration ur ON d.uid = ur.uid
    -- 增量更新
    -- WHERE d.d_date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更细
    WHERE d.d_date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    -- 限定用户
    and d.user_type='olduser'
),
-- 曝光信息
exposure_data AS (
    SELECT
        TO_TIMESTAMP(ed.created_at)::date AS date,
        ed.uid,
        uti.user_type,
        uti.country_name,
        uti.ad_channel,
        uti.user_group,
        uti.id,
        uti.chinese_name,
        uti.lang_name,
        uti.registration_period
    FROM public.app_user_cover_show_log ed
    inner JOIN user_type_info uti ON ed.uid = uti.uid AND TO_TIMESTAMP(ed.created_at)::date = uti.日期
    and NULLIF(ed.model_id, '')::bigint=uti.id
    WHERE event = 111
    -- 增量更新
    -- AND TO_TIMESTAMP(ed.created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更新
    AND TO_TIMESTAMP(ed.created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    AND CAST(ext_body::json ->> 'page' AS int) = 1
),
-- 点击信息
click_data AS (
    SELECT
        TO_TIMESTAMP(cd.created_at)::date AS date,
        cd.uid,
        event,
        watch_time,
        case when order_id like '%SH%' then order_id else CONCAT('SH', order_id) end AS order_id,
        vid,                            -- 剧id用于关联k币消耗
        eid,                            -- 集id用于关联k币消耗
        uti.user_type,
        uti.country_name,
        uti.ad_channel,
        uti.user_group,
        uti.id,
        uti.chinese_name,
        uti.lang_name,
        uti.registration_period
    FROM public.app_user_track_log cd
    inner  JOIN user_type_info uti ON cd.uid = uti.uid AND TO_TIMESTAMP(cd.created_at)::date = uti.日期
    and NULLIF(cd.column1, '')::bigint=uti.id
    WHERE event IN (112, 1, 192, 191, 2, 13, 14)
    -- 增量更新
    -- AND TO_TIMESTAMP(cd.created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更新
    AND TO_TIMESTAMP(cd.created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    AND CAST(ext_body::json ->> 'page' AS int) = 1          -- 应该是首页的意思
    and (NULLIF(cd.column1, '') ~ '^\d+$')
),
-- 支付信息
payment_data AS (
    SELECT
        TO_TIMESTAMP(pd.created_at)::date AS date,
        pd.uid,
        -- SUBSTRING(order_num FROM 3)::bigint AS order_id,
        order_num,
        money * 1.0 / 100 AS total_payment_amount
    FROM public.all_order_log pd
    WHERE status = 1 AND environment = 1
    -- 增量更新
    -- AND TO_TIMESTAMP(pd.created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更细
    AND TO_TIMESTAMP(pd.created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
),
-- k币消耗信息
k_consume_data as (
    select
        t.date,
        t.uid,
        goods_id,                       -- vid
        goods_sku_id,                   -- eid
        money
    from (select
        to_timestamp(created_at) :: date as date,
        uid,
        goods_id,                       -- vid
        goods_sku_id,                   -- eid
        money
    from "middle_user_consume_record_00"
    where type = 0
    -- 增量更新
    -- and TO_TIMESTAMP(created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更新
    and  to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    union all
    select
        to_timestamp(created_at) :: date as date,
        uid,
        goods_id,                       -- vid
        goods_sku_id,                   -- eid
        money
    from "middle_user_consume_record_01"
    where type = 0
    -- 增量更新
    -- and TO_TIMESTAMP(created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更新
    and  to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    union all
    select
        to_timestamp(created_at) :: date as date,
        uid,
        goods_id,                       -- vid
        goods_sku_id,                   -- eid
        money
    from "middle_user_consume_record_02"
    where type = 0
    -- 增量更新
    -- and TO_TIMESTAMP(created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更新
    and  to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    union all
    select
        to_timestamp(created_at) :: date as date,
        uid,
        goods_id,                       -- vid
        goods_sku_id,                   -- eid
        money
    from "middle_user_consume_record_03"
    where type = 0
    -- 增量更新
    -- and TO_TIMESTAMP(created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更新
    and  to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    union all
    select
        to_timestamp(created_at) :: date as date,
        uid,
        goods_id,                       -- vid
        goods_sku_id,                   -- eid
        money
    from "middle_user_consume_record_04"
    where type = 0
    -- 增量更新
    -- and TO_TIMESTAMP(created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    -- 全量更新
    and to_timestamp(created_at)::date BETWEEN '2025-04-01' AND (current_date+interval '-1 day') ) t
),
-- 对k币消耗信息进行聚合
-- 要用event = 191 uid和剧id关联
aggregated_k_consume as (
    select
        cd.date,
        cd.user_type,
        cd.country_name,
        cd.ad_channel,
        cd.user_group,
        cd.id,
        cd.chinese_name,
        cd.lang_name,
        cd.registration_period,
        sum(money) as k_consume_amount
    from click_data cd
    left join k_consume_data on cd.vid = k_consume_data.goods_id and cd.eid= k_consume_data.goods_sku_id and k_consume_data.date = cd.date and k_consume_data.uid = cd.uid
    and cd.event = 191
    group by cd.date, cd.user_type, cd.country_name, cd.ad_channel, cd.user_group, cd.id, cd.chinese_name, cd.lang_name, cd.registration_period
),
-- 对曝光信息进行聚合
aggregated_exposure AS (
    SELECT
        date,
        user_type,
        country_name,
        ad_channel,
        user_group,
        id,
        chinese_name,
        lang_name,
        registration_period,
        COUNT(DISTINCT uid) AS exposure_users,
        COUNT(*) AS exposure_times
    FROM exposure_data
    GROUP BY date, user_type, country_name, ad_channel, user_group, id, chinese_name,lang_name, registration_period
),
-- 对点击信息进行聚合
aggregated_click AS (
    SELECT
        date,
        user_type,
        country_name,
        ad_channel,
        user_group,
        id,
        chinese_name,
        lang_name,
        registration_period,
        COUNT(DISTINCT CASE WHEN event = 112 THEN uid END) AS click_users,
        COUNT(DISTINCT CASE WHEN event IN (1, 2, 13, 14) THEN uid END) AS play_users,
        COUNT(DISTINCT CASE WHEN event = 192 THEN uid END) AS recharge_submission_users,
        COUNT(DISTINCT CASE WHEN event = 191 THEN uid END) AS episode_unlock_users,
        COUNT(CASE WHEN event = 112 THEN uid END) AS click_times,
        COUNT(CASE WHEN event IN (1, 2, 13, 14) THEN uid END) AS play_times,
        COUNT(CASE WHEN event = 192 THEN uid END) AS recharge_submission_times,
        COUNT(CASE WHEN event = 191 THEN uid END) AS episode_unlocks,
        ROUND(COALESCE(SUM(CASE WHEN event = 2 THEN watch_time ELSE 0 END) / 60.0, 0)) AS watch_duration_minutes
    FROM click_data
    GROUP BY date, user_type,  country_name, ad_channel, user_group, id, chinese_name,lang_name, registration_period
),
-- 对支付信息进行聚合
aggregated_payment AS (
    SELECT
        pd.date,
        pd.user_type,
        pd.country_name,
        pd.ad_channel,
        pd.user_group,
        pd.id,
        pd.chinese_name,
        pd.lang_name,
        pd.registration_period,
        COALESCE(COUNT(DISTINCT cd.uid), 0) AS successful_payment_users,
        COALESCE(COUNT(DISTINCT cd.order_num), 0) AS successful_payment_times,
        COALESCE(SUM(cd.total_payment_amount), 0) AS total_payment_amount
    FROM click_data pd
    LEFT JOIN payment_data cd ON pd.uid = cd.uid AND pd.date = cd.date AND pd.event = 192
    and pd.order_id=cd.order_num
    GROUP BY pd.date, pd.user_type, pd.country_name, pd.ad_channel, pd.user_group, pd.id, pd.chinese_name,pd.lang_name, pd.registration_period
)
select
    id ,
    user_group,
    sum(exposure_users) as exposure_users,
    sum(click_users) as click_users,
    sum(play_users) as play_users,
    sum(recharge_submission_users) as recharge_submission_users,
    sum(successful_payment_users) as successful_payment_users,
    sum(episode_unlock_users) as episode_unlock_users,
    sum(total_payment_amount) as total_payment_amount,
    sum(k_consume_amount) as k_consume_amount,
    sum(exposure_times) as exposure_times,
    sum(click_times) as click_times,
    sum(play_times) as play_times,
    sum(recharge_submission_times) as recharge_submission_times,
    sum(successful_payment_times)as successful_payment_times,
    sum(episode_unlocks) as episode_unlocks,
    sum(watch_duration_minutes) as watch_duration_minutes
from (SELECT
    -- 维度
    ae.date,
    cast(ae.user_type as text ) as user_type,
    ae.country_name,
    ae.ad_channel,
    ae.user_group,
    ae.id:: text as id,
    ae.chinese_name,
    ae.lang_name,
    ae.registration_period,
    -- 计算字段
    COALESCE(ae.exposure_users, 0) AS exposure_users,
    COALESCE(ac.click_users, 0) AS click_users,
    COALESCE(ac.play_users, 0) AS play_users,
    COALESCE(ac.recharge_submission_users, 0) AS recharge_submission_users,
    COALESCE(ac.episode_unlock_users, 0) AS episode_unlock_users,
    COALESCE(ae.exposure_times, 0) AS exposure_times,
    COALESCE(ac.click_times, 0) AS click_times,
    COALESCE(ac.play_times, 0) AS play_times,
    COALESCE(ac.recharge_submission_times, 0) AS recharge_submission_times,
    COALESCE(ac.episode_unlocks, 0) AS episode_unlocks,
    cast (COALESCE(ac.watch_duration_minutes, 0) as bigint) AS watch_duration_minutes,
    COALESCE(ap.successful_payment_users, 0) AS successful_payment_users,
    COALESCE(ap.successful_payment_times, 0) AS successful_payment_times,
    cast(COALESCE(ap.total_payment_amount, 0) as numeric(20,2) ) AS total_payment_amount, --,
    cast(coalesce(akc.k_consume_amount,0) as numeric(20,2)) as k_consume_amount
-- 聚合曝光表为主表
FROM aggregated_exposure ae
LEFT JOIN aggregated_click ac ON ae.date = ac.date
    AND ae.user_type = ac.user_type
    AND ae.country_name = ac.country_name
    AND ae.ad_channel = ac.ad_channel
    AND ae.user_group = ac.user_group
    AND ae.id = ac.id
    AND ae.chinese_name = ac.chinese_name
    AND ae.lang_name = ac.lang_name
    AND ae.registration_period = ac.registration_period
LEFT JOIN aggregated_payment ap ON ae.date = ap.date
    AND ae.user_type = ap.user_type
    AND ae.country_name = ap.country_name
    AND ae.ad_channel = ap.ad_channel
    AND ae.user_group = ap.user_group
    AND ae.id = ap.id
    AND ae.chinese_name = ap.chinese_name
    AND ae.lang_name = ap.lang_name
    AND ae.registration_period = ap.registration_period
left join aggregated_k_consume  akc on ae.date = akc.date
    and ae.user_type = akc.user_type
    and ae.country_name = akc.country_name
    and ae.ad_channel = akc.ad_channel
    and ae.user_group = akc.user_group
    and ae.id = akc.id
    and ae.chinese_name = akc.chinese_name
    and ae.lang_name = akc.lang_name
    and ae.registration_period = akc.registration_period
WHERE ae.user_group IS NOT null
and ae.id IS NOT null
ORDER BY ae.date, ae.user_type, ae.country_name, ae.ad_channel, ae.user_group, ae.id, ae.chinese_name, ae.lang_name, ae.registration_period) t
    where t.date = '2025-04-21' and id  = '211'
group by id,user_group

-- 测试bi数字不一致的来源，源自 v_dim_country_area
select
    user_group,
    sum(exposure_users) as exposure_users,
    sum(click_users) as click_users,
    sum(play_users) as play_users,
    sum(recharge_submission_users) as recharge_submission_users,
    sum(successful_payment_users) as successful_payment_users,
    sum(episode_unlock_users) as episode_unlock_users,
    sum(total_payment_amount) as total_payment_amount,
    sum(k_consume_amount) as k_consume_amount,
    sum(exposure_times) as exposure_times,
    sum(click_times) as click_times,
    sum(play_times) as play_times,
    sum(recharge_submission_times) as recharge_submission_times,
    sum(successful_payment_times)as successful_payment_times,
    sum(episode_unlocks) as episode_unlocks,
    sum(watch_duration_minutes) as watch_duration_minutes
from public.dw_recommend_home_olduser t1
left join v_dim_country_area t2 on t1.country_name = t2.country_name
where date  = '2025-04-21' and t1.id  = '211'
group by user_group

-- 得知country_name 不唯一，country_code 唯一
select count(distinct country_code ) from v_dim_country_area