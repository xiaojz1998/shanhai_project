------------------------------------------
-- file: 推荐位报表_首页新用户扩量0401_增量两天_h.新sql_v1
-- author: xiaoj
-- time: 2025/4/23 19:09
-- description:
------------------------------------------


set timezone ='UTC-0';

-- truncate table tmp.dw_recommend_home0401_tmp01;
-- INSERT INTO tmp.dw_recommend_home0401_tmp01
delete from tmp.dw_recommend_home0401_tmp01 where
date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day');
-- date ='2025-03-26';
INSERT INTO tmp.dw_recommend_home0401_tmp01
-- 获取用户注册信息
-- 并对用户分组
WITH user_registration AS (
    SELECT
        d_date AS register_date,
        uid::int8 AS uid,
        country_name,
        ad_channel,
        CASE
            WHEN RIGHT(CAST(uid AS VARCHAR), 1) in ('1','2','3') THEN '对照组'
            WHEN RIGHT(CAST(uid AS VARCHAR), 1) in ('5','6','7') THEN '实验组'
            ELSE NULL
        END AS user_group
    FROM dwd_user_info
),
-- dim_homepage_user 是啥不知道
-- 依旧用来补全信息
user_type_info AS (
    SELECT
        d.uid,
        d.d_date AS 日期,
        COALESCE(d.user_type, 'Unknown') AS user_type,
        d.id,
        d.chinese_name,
        ur.country_name,
        ur.ad_channel,
        ur.user_group
    FROM public.dim_homepage_user d
    LEFT JOIN user_registration ur ON d.uid = ur.uid
   WHERE d.d_date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
--    WHERE d.d_date ='2025-03-26'
    -- WHERE d.d_date BETWEEN '2025-03-10' AND (current_date+interval '-1 day')
    and d.user_type='newuser'
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
        uti.chinese_name
    FROM public.app_user_cover_show_log ed
    LEFT JOIN user_registration ur ON ed.uid = ur.uid
    LEFT JOIN user_type_info uti ON ed.uid = uti.uid AND TO_TIMESTAMP(ed.created_at)::date = uti.日期
    and NULLIF(ed.model_id, '')::bigint=uti.id
    WHERE event = 111
     AND TO_TIMESTAMP(ed.created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    --  AND TO_TIMESTAMP(ed.created_at)::date ='2025-03-26'
    --  AND TO_TIMESTAMP(ed.created_at)::date BETWEEN '2025-03-10' AND (current_date+interval '-1 day')
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
        uti.user_type,
        uti.country_name,
        uti.ad_channel,
        uti.user_group,
        uti.id,
        uti.chinese_name
    FROM public.app_user_track_log cd
    LEFT JOIN user_registration ur ON cd.uid = ur.uid
    LEFT JOIN user_type_info uti ON cd.uid = uti.uid AND TO_TIMESTAMP(cd.created_at)::date = uti.日期
    and NULLIF(cd.column1, '')::bigint=uti.id
    WHERE event IN (112, 1, 192, 191, 2, 13, 14)
     AND TO_TIMESTAMP(cd.created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    --  AND TO_TIMESTAMP(cd.created_at)::date ='2025-03-26'
    --   AND TO_TIMESTAMP(cd.created_at)::date BETWEEN '2025-03-10' AND (current_date+interval '-1 day')
      AND CAST(ext_body::json ->> 'page' AS int) = 1
      and (NULLIF(cd.column1, '') ~ '^\d+$')
),
-- 支付信息
payment_data AS (
    SELECT
        TO_TIMESTAMP(pd.created_at)::date AS date,
        pd.uid,
        -- SUBSTRING(order_num FROM 3)::bigint AS order_id,
        order_num,
        money * 1.0 / 100 AS total_payment_amount,
        uti.user_type,
        uti.country_name,
        uti.ad_channel,
        uti.user_group,
        uti.id,
        uti.chinese_name
    FROM public.all_order_log pd
    LEFT JOIN user_type_info uti ON pd.uid = uti.uid AND TO_TIMESTAMP(pd.created_at)::date = uti.日期
    WHERE status = 1
      AND environment = 1
     AND TO_TIMESTAMP(pd.created_at)::date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day')
    --  AND TO_TIMESTAMP(pd.created_at)::date ='2025-03-26'
    --   AND TO_TIMESTAMP(pd.created_at)::date BETWEEN '2025-03-10' AND (current_date+interval '-1 day')
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
        COUNT(DISTINCT uid) AS exposure_users,
        COUNT(*) AS exposure_times
    FROM exposure_data
    GROUP BY date, user_type, country_name, ad_channel, user_group, id, chinese_name
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
    GROUP BY date, user_type,  country_name, ad_channel, user_group, id, chinese_name
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
        COALESCE(COUNT(DISTINCT cd.uid), 0) AS successful_payment_users,
        COALESCE(COUNT(DISTINCT cd.order_num), 0) AS successful_payment_times,
        COALESCE(SUM(cd.total_payment_amount), 0) AS total_payment_amount
    FROM click_data pd
    LEFT JOIN payment_data cd ON pd.uid = cd.uid AND pd.date = cd.date AND pd.event = 192
    and pd.order_id=cd.order_num
    GROUP BY pd.date, pd.user_type, pd.country_name, pd.ad_channel, pd.user_group, pd.id, pd.chinese_name
)
SELECT
    -- 维度
    ae.date,
    ae.user_type,
    ae.country_name,
    ae.ad_channel,
    ae.user_group,
    ae.id,
    ae.chinese_name,
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
    COALESCE(ac.watch_duration_minutes, 0) AS watch_duration_minutes,
    COALESCE(ap.successful_payment_users, 0) AS successful_payment_users,
    COALESCE(ap.successful_payment_times, 0) AS successful_payment_times,
    COALESCE(ap.total_payment_amount, 0) AS total_payment_amount
FROM aggregated_exposure ae
LEFT JOIN aggregated_click ac ON ae.date = ac.date
    AND ae.user_type = ac.user_type
    AND ae.country_name = ac.country_name
    AND ae.ad_channel = ac.ad_channel
    AND ae.user_group = ac.user_group
    AND ae.id = ac.id
    AND ae.chinese_name = ac.chinese_name
LEFT JOIN aggregated_payment ap ON ae.date = ap.date
    AND ae.user_type = ap.user_type
    AND ae.country_name = ap.country_name
    AND ae.ad_channel = ap.ad_channel
    AND ae.user_group = ap.user_group
    AND ae.id = ap.id
    AND ae.chinese_name = ap.chinese_name
WHERE ae.user_group IS NOT null
and ae.id IS NOT null
ORDER BY ae.date, ae.user_type, ae.country_name, ae.ad_channel, ae.user_group, ae.id, ae.chinese_name;


-- truncate table public.dw_recommend_home0401;
--         insert into public.dw_recommend_home0401 select * from tmp.dw_recommend_home0401_tmp01;
delete from public.dw_recommend_home0401 where date
between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day');
-- ='2025-03-26';
        insert into public.dw_recommend_home0401 select * from tmp.dw_recommend_home0401_tmp01
        where date between (CURRENT_DATE + INTERVAL '-2 day') and (current_date+interval '-1 day');
        -- where date ='2025-03-26';



---端内
-- set timezone ='UTC-0';
-- truncate table tmp.dw_recommend_home0401_app_tmp01;
-- INSERT INTO tmp.dw_recommend_home0401_app_tmp01
delete from tmp.dw_recommend_home0401_app_tmp01 where date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day');
INSERT INTO tmp.dw_recommend_home0401_app_tmp01
-- 用户注册表
-- 维度：uid
-- 用于补全信息
WITH user_registration AS (
    SELECT
        d_date AS register_date,
        uid::int8 AS uid,
        country_name,
        ad_channel,
        CASE
            WHEN RIGHT(CAST(uid AS VARCHAR), 1) in ('1','2','3') THEN '对照组'
            WHEN RIGHT(CAST(uid AS VARCHAR), 1) in ('5','6','7') THEN '实验组'
            ELSE NULL
        END AS user_group
    FROM dwd_user_info
),
-- 每一日用户曝光的基本信息维表
-- 维度： 日期 uid
-- 用来补全信息  排除土耳其语和越南语用户 判断新老用户
user_type_derivation AS (
        SELECT
        d.uid,
        d.d_date AS 日期,
        COALESCE(d.user_type, 'Unknown') AS user_type,
        d.id,
        d.chinese_name,
        ur.country_name,
        ur.ad_channel,
        ur.user_group
    FROM public.dim_homepage_user d
    LEFT JOIN user_registration ur ON d.uid = ur.uid
   WHERE d.d_date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
    -- WHERE d.d_date BETWEEN '2025-03-10' AND (current_date+interval '-1 day')
    and d.user_type='newuser'
),
pv_counts AS (
    SELECT
        uid,
        to_timestamp(created_at)::date as 日期,
        COUNT(1) AS pv
    FROM "app_user_track_log"
    WHERE
        to_timestamp(created_at)::date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
        -- to_timestamp(created_at)::date BETWEEN '2025-03-10' AND (current_date+interval '-1 day')
        AND event  in(1,2,13,14)
    GROUP BY uid, 日期
),
recharge_counts AS (
    SELECT
        uid,
        to_timestamp(created_at)::date as 日期,
        COUNT(1) AS recharge_times
    FROM "app_user_track_log"
    WHERE
        to_timestamp(created_at)::date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
        -- to_timestamp(created_at)::date BETWEEN '2025-03-10' AND (current_date+interval '-1 day')
        AND event_name = 'submitRecharge'
    GROUP BY uid, 日期
),
unlock_episodes_counts AS (
    SELECT
        uid,
        to_timestamp(created_at)::date as 日期,
        COUNT(1) AS unlock_episodes_times
    FROM "app_user_track_log"
    WHERE
        to_timestamp(created_at)::date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
        -- to_timestamp(created_at)::date BETWEEN '2025-03-10' AND (current_date+interval '-1 day')
        AND event_name = 'unlockEpisodes'
    GROUP BY uid, 日期
),
watch_time_counts AS (
    SELECT
        uid,
        to_timestamp(created_at)::date as 日期,
        SUM(watch_time) / 60 AS watch_time_minutes
    FROM "app_user_track_log"
    WHERE
        to_timestamp(created_at)::date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
        -- to_timestamp(created_at)::date BETWEEN '2025-03-10' AND (current_date+interval '-1 day')
        AND event_name = 'drama_watch_time'
    GROUP BY uid, 日期
),
successful_recharges AS (
    SELECT
        a.uid,
        to_timestamp(created_at)::date as 日期,
        COUNT(DISTINCT a.uid) AS successful_recharge_times,
        SUM(a.money)/100.0  AS total_recharge_amount,
        SUBSTRING(order_num FROM 3)::bigint AS order_id
    from public.all_order_log a
    WHERE
        to_timestamp(created_at)::date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
        -- to_timestamp(created_at)::date BETWEEN '2025-03-10' AND (current_date+interval '-1 day')
        AND status = 1
        AND environment = 1
    GROUP BY a.uid, 日期, SUBSTRING(order_num FROM 3)::bigint
),
active_users AS (
    SELECT
        uid,
        d_date AS active_date
    FROM public.dwd_user_active
    WHERE
        d_date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day')
        -- d_date BETWEEN '2025-03-10' AND (current_date+interval '-1 day')
    GROUP BY uid,d_date
),
active_day_2 AS (
    SELECT DISTINCT
        d_date AS 活跃日期,
        uid
    FROM public.dwd_user_active
    WHERE
        d_date >=  '2025-03-11'
),
active_day_4 AS (
    SELECT DISTINCT
        d_date AS 活跃日期,
        uid
    FROM public.dwd_user_active
    WHERE
        d_date >=  '2025-03-13'
),
active_day_8 AS (
    SELECT DISTINCT
        d_date AS 活跃日期,
        uid
    FROM public.dwd_user_active
    WHERE
        d_date >=  '2025-03-17'
)

SELECT
    us.日期 as date,
    us.user_type,
    us.user_group as group_type,
    us.country_name AS country,
    us.ad_channel,
    COUNT(DISTINCT us.uid) AS exposure_users,
    COUNT(DISTINCT CASE WHEN pv_counts.pv IS NOT NULL THEN us.uid END) AS play_users,
    COUNT(DISTINCT CASE WHEN recharge_counts.recharge_times IS NOT NULL THEN us.uid END) AS submit_recharge_users,
    COALESCE(SUM(recharge_counts.recharge_times), 0) AS submit_recharge_times,
    COUNT(DISTINCT CASE WHEN unlock_episodes_counts.unlock_episodes_times IS NOT NULL THEN us.uid END) AS unlock_episodes_users,
    COALESCE(SUM(unlock_episodes_counts.unlock_episodes_times), 0) AS unlock_episodes_times,
    COALESCE(SUM(watch_time_counts.watch_time_minutes), 0) AS total_watch_time_minutes,
    COUNT(DISTINCT CASE WHEN sr.uid IS NOT NULL THEN us.uid END) AS successful_recharge_users,
--    COUNT(DISTINCT sr.uid) AS 充值成功人数,
    COUNT(sr.order_id)  AS successful_recharge_times,
    COALESCE(SUM(sr.total_recharge_amount),  0) as total_recharge_amount ,
    COUNT(DISTINCT au.uid) as daily_active_users,
    COUNT(DISTINCT ad2.uid) as retention_next_day,
    COUNT(DISTINCT ad4.uid)  as retention_3_days,
    COUNT(DISTINCT ad8.uid)  as retention_7_days
FROM user_type_derivation us
LEFT JOIN pv_counts ON us.uid = pv_counts.uid AND us.日期 = pv_counts.日期
LEFT JOIN recharge_counts ON us.uid = recharge_counts.uid AND us.日期 = recharge_counts.日期
LEFT JOIN unlock_episodes_counts ON us.uid = unlock_episodes_counts.uid AND us.日期 = unlock_episodes_counts.日期
LEFT JOIN watch_time_counts ON us.uid = watch_time_counts.uid AND us.日期 = watch_time_counts.日期
LEFT JOIN successful_recharges sr ON us.uid = sr.uid AND us.日期 = sr.日期
LEFT JOIN active_users au ON us.uid = au.uid AND au.active_date = us.日期
LEFT JOIN active_day_2 ad2 ON us.uid = ad2.uid AND ad2.活跃日期 = us.日期 + INTERVAL '1 day'
LEFT JOIN active_day_4 ad4 ON us.uid = ad4.uid AND ad4.活跃日期 = us.日期 + INTERVAL '3 day'
LEFT JOIN active_day_8 ad8 ON us.uid = ad8.uid AND ad8.活跃日期 = us.日期 + INTERVAL '7 day'
where us.user_group is not null
GROUP BY us.日期, us.user_type
,us.user_group
    ,us.country_name
    ,us.ad_channel
ORDER BY us.日期, us.user_type;


-- truncate table public.dw_recommend_home0401_app;
-- insert into public.dw_recommend_home0401_app select * from tmp.dw_recommend_home0401_app_tmp01;
delete from public.dw_recommend_home0401_app where date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day');
 insert into public.dw_recommend_home0401_app select * from tmp.dw_recommend_home0401_app_tmp01 where date between (CURRENT_DATE + INTERVAL '-4 day') and (current_date+interval '-1 day');



--  set timezone ='UTC-0';

truncate table tmp.dw_recommend_home0401_index_tmp01;
INSERT INTO  tmp.dw_recommend_home0401_index_tmp01
WITH
user_selection AS (
    SELECT DISTINCT
           uid,
           CASE
            WHEN RIGHT(CAST(uid AS VARCHAR), 1) in ('1','2','3') THEN '对照组'
            WHEN RIGHT(CAST(uid AS VARCHAR), 1) in ('5','6','7') THEN '实验组'
            ELSE NULL
        END AS type,
           to_timestamp(created_at)::date as 日期
    FROM "app_user_cover_show_log"
     WHERE
--    to_timestamp(created_at)::date >='2025-03-18'
    to_timestamp(created_at)::date between '2025-04-01' and  (CURRENT_DATE - INTERVAL '1 day')
    --  to_timestamp(created_at)::date >= '2025-03-25'
      AND event_name = 'drama_cover_show'
      AND CAST(ext_body::json ->> 'page' AS int) = 1
--      AND ext_body::json ->> 'show_title' = 'playRetain'

),
first_exposure AS (
    SELECT
           uid
           ,type,
           MIN(日期) as first_exposure_date
    FROM user_selection
    GROUP BY uid
    ,type
),
successful_recharges AS (
  SELECT
         a.uid,
         to_timestamp(created_at)::date as 日期,
         SUM(a.money) AS total_recharge_amount
  FROM public.all_order_log a
  JOIN first_exposure fe ON a.uid = fe.uid
  WHERE to_timestamp(created_at)::date >= fe.first_exposure_date
    --   AND to_timestamp(created_at)::date >='2025-03-18'
     and to_timestamp(created_at)::date between '2025-04-01' and (CURRENT_DATE - INTERVAL '1 day')
--   and to_timestamp(created_at)::date >= '2025-03-25'
      AND status = 1
      AND environment = 1
  GROUP BY a.uid
  , 日期
),
user_active_days AS (
  SELECT
         a.uid,
         COUNT(DISTINCT to_timestamp(a.created_at)::date) AS user_active_days
  FROM "app_user_track_log" a
  JOIN first_exposure fe ON a.uid = fe.uid
  WHERE event IN (1, 16)
    AND to_timestamp(created_at)::date >= fe.first_exposure_date
    -- AND to_timestamp(created_at)::date >='2025-03-18'
   and to_timestamp(created_at)::date between '2025-04-01' and (CURRENT_DATE - INTERVAL '1 day')
    --   and to_timestamp(created_at)::date >= '2025-03-25'
   GROUP BY a.uid
),

combined_data AS (
  SELECT
         us.uid,
         us.type,
         COALESCE(SUM(sr.total_recharge_amount), 0) AS total_recharge_amount,
         uad.user_active_days
  FROM first_exposure us
  LEFT JOIN successful_recharges sr ON us.uid = sr.uid
  LEFT JOIN user_active_days uad ON us.uid = uad.uid
  GROUP BY us.uid, us.type
  , uad.user_active_days
),
user_type_info AS (
    SELECT
        d.uid,
        d.d_date AS 日期,
        COALESCE(d.user_type, 'Unknown') AS user_type,
        ur.type
    FROM public.dim_playretain_user d
    LEFT JOIN user_selection ur ON d.uid = ur.uid
    WHERE d.d_date BETWEEN '2025-04-01' AND (current_date+interval '-1 day')
    -- where d_date >= '2025-03-25'
    and d.user_type='newuser'
)
SELECT
       cd.type as 实验分组,
       COUNT(DISTINCT cd.uid) AS 观察日期内曝光人数,
       SUM(cd.total_recharge_amount)/100.0 AS 观察日期内充值金额,
       SUM(cd.user_active_days) AS 观察日期内活跃天数
FROM combined_data cd
JOIN user_type_info uti ON cd.uid = uti.uid
where cd.type is not null
GROUP BY cd.type
ORDER BY cd.type;


        truncate table public.dw_recommend_home0401_index;
        insert into public.dw_recommend_home0401_index select * from tmp.dw_recommend_home0401_index_tmp01;