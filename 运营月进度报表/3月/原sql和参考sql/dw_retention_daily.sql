set timezone ='UTC-0';
truncate table public.dw_retention_daily;
INSERT INTO public.dw_retention_daily
-- 新增用户
WITH t1 AS (
        SELECT
            d_date::date as  register_date
           , b.country_name AS 国家
           , a.area
           ,CASE WHEN os = 'ios' THEN 'IOS' ELSE 'Android' END AS 系统
           , campaign_id
           , uid::int8 as uid
        FROM public.dwd_user_info a LEFT JOIN v_dim_country_area b ON UPPER(a.country_code) = b.country_code
),
-- 活跃用户表
t2 AS (
    SELECT d_date as active_date, uid::int8 as uid
    FROM public.dwd_user_active
),
t3 AS (
    SELECT active_date,
        COALESCE(register_date, '2024-07-01') register_date,
        COALESCE(area, '未知') area,
        COALESCE(国家, '未知') 国家,
        COALESCE(系统, 'Android') 系统,
        COALESCE(campaign_id, '0') campaign_id,
        t2.uid
    FROM t2 LEFT JOIN t1 ON t1.uid = t2.uid
)
SELECT a.active_date,
    a.area AS 区域,
    a.国家,
    a.系统,
    COUNT(DISTINCT a.uid) DAU,
    COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 1 THEN b.uid ELSE NULL END) 总次日留存,
    COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 3 THEN b.uid ELSE NULL END) 总3日留存,
    COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 7 THEN b.uid ELSE NULL END) 总7日留存,
    COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 14 THEN b.uid ELSE NULL END) 总14日留存,
    COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 30 THEN b.uid ELSE NULL END) 总30日留存,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date THEN a.uid ELSE NULL END) 新用户数,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 1 THEN b.uid ELSE NULL END ) 新用户次日留存,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 3 THEN b.uid ELSE NULL END ) 新用户3日留存,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 7 THEN b.uid ELSE NULL END ) 新用户7日留存,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 14 THEN b.uid ELSE NULL END ) 新用户14日留存,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 30 THEN b.uid ELSE NULL END ) 新用户30日留存,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.campaign_id NOT IN ('0', '') THEN a.uid ELSE NULL END) 新推广用户,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.campaign_id NOT IN ('0', '') AND b.active_date - a.active_date = 1 THEN b.uid ELSE NULL END ) 新推广用户次日留存,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.campaign_id NOT IN ('0', '') AND b.active_date - a.active_date = 3 THEN b.uid ELSE NULL END) 新推广用户3日留存,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.campaign_id NOT IN ('0', '') AND b.active_date - a.active_date = 7 THEN b.uid ELSE NULL END) 新推广用户7日留存,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.campaign_id NOT IN ('0', '') AND b.active_date - a.active_date = 14 THEN b.uid ELSE NULL END) 新推广用户14日留存,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.campaign_id NOT IN ('0', '') AND b.active_date - a.active_date = 30 THEN b.uid ELSE NULL END) 新推广用户30日留存,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.campaign_id IN ('0', '') THEN a.uid ELSE NULL END) 新自然用户数,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.campaign_id IN ('0', '') AND b.active_date - a.active_date = 1 THEN b.uid ELSE NULL END) 新自然用户次日留存,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.campaign_id IN ('0', '') AND b.active_date - a.active_date = 3 THEN b.uid ELSE NULL END) 新自然用户3日留存,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.campaign_id IN ('0', '') AND b.active_date - a.active_date = 7 THEN b.uid ELSE NULL END) 新自然用户7日留存,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.campaign_id IN ('0', '') AND b.active_date - a.active_date = 14 THEN b.uid ELSE NULL END) 新自然用户14日留存,
    COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.campaign_id IN ('0', '') AND b.active_date - a.active_date = 30 THEN b.uid ELSE NULL END) 新自然用户30日留存,
    COUNT(DISTINCT CASE WHEN a.active_date <> a.register_date THEN a.uid ELSE NULL END) 老用户数,
    COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 1 AND a.active_date <> a.register_date THEN b.uid ELSE NULL END ) 老用户次日留存,
    COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 3 AND a.active_date <> a.register_date THEN b.uid ELSE NULL END ) 老用户3日留存,
    COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 7 AND a.active_date <> a.register_date THEN b.uid ELSE NULL END ) 老用户7日留存,
    COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 14 AND a.active_date <> a.register_date THEN b.uid ELSE NULL END ) 老用户14日留存,
    COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 30 AND a.active_date <> a.register_date THEN b.uid ELSE NULL END ) 老用户30日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 60 THEN b.uid ELSE NULL END) 总60日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 120 THEN b.uid ELSE NULL END) 总120日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 60 THEN b.uid ELSE NULL END ) 新用户60日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 120 THEN b.uid ELSE NULL END ) 新用户120日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.campaign_id NOT IN ('0', '') AND b.active_date - a.active_date = 60 THEN b.uid ELSE NULL END) 新推广用户60日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.campaign_id NOT IN ('0', '') AND b.active_date - a.active_date = 120 THEN b.uid ELSE NULL END) 新推广用户120日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.campaign_id IN ('0', '') AND b.active_date - a.active_date = 60 THEN b.uid ELSE NULL END) 新自然用户60日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND a.campaign_id IN ('0', '') AND b.active_date - a.active_date = 120 THEN b.uid ELSE NULL END) 新自然用户120日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 60 AND a.active_date <> a.register_date THEN b.uid ELSE NULL END ) 老用户60日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 120 AND a.active_date <> a.register_date THEN b.uid ELSE NULL END ) 老用户120日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 90 THEN b.uid ELSE NULL END) 总90日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 180 THEN b.uid ELSE NULL END) 总180日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 360 THEN b.uid ELSE NULL END) 总360日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 90 THEN b.uid ELSE NULL END ) 新用户90日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 180 THEN b.uid ELSE NULL END ) 新用户180日留存
    ,COUNT(DISTINCT CASE WHEN a.active_date = a.register_date AND b.active_date - a.active_date = 360 THEN b.uid ELSE NULL END ) 新用户360日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 90 AND a.active_date <> a.register_date THEN b.uid ELSE NULL END ) 老用户90日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 180 AND a.active_date <> a.register_date THEN b.uid ELSE NULL END ) 老用户180日留存
    ,COUNT(DISTINCT CASE WHEN b.active_date - a.active_date = 360 AND a.active_date <> a.register_date THEN b.uid ELSE NULL END ) 老用户360日留存
FROM t3 a LEFT JOIN t3 b ON a.uid = b.uid AND b.active_date > a.active_date
WHERE a.active_date IS NOT NULL
and  a.active_date between '2024-07-01' and (current_date+interval '-1 day')
GROUP BY a.active_date, a.area, a.系统, a.国家;




truncate table public.dw_retention_week;
INSERT INTO public.dw_retention_week
WITH t1 AS (
    SELECT *
    FROM (
        SELECT
            TO_CHAR(to_timestamp(created_at), 'IYYY"-"IW') AS register_week
            --, concat((date_trunc('week', to_timestamp(created_at)) )::date,'~',(date_trunc('week', to_timestamp(created_at)) + interval '6 days')::date) as week_day
            --, (date_trunc('week', to_timestamp(created_at)) )::date week_start
            --, (date_trunc('week', to_timestamp(created_at)) + interval '6 days')::date week_end
           , TO_TIMESTAMP(created_at) :: date register_date
            --, case when area = 'id' then '印尼'
            --     when area = 'kr' then '韩国'
            --     when area = 'jp' then '日本'
            --     when area = 'es' then '西班牙'
            --     when area = 'cn' then '中国(简体)'
            --   else '英语国家' end as 区域
           , b.country_name 国家
           , b.area AS 区域
           , CASE WHEN os = 'ios' THEN 'IOS' ELSE 'Android' END AS 系统
           , campaign_id
           , uid
           , event
           , row_number() OVER (PARTITION BY uid ORDER BY created_at) rk  -- 可能存在多次归因，以第一次为准
        FROM "user_log"  a LEFT JOIN v_dim_country_area b ON UPPER(a.area) = b.country_code
        WHERE 1 = 1
            AND event = 1
            AND created_date >= 20240701
    ) a
    WHERE rk = 1
),
t2 AS (
    SELECT
        TO_CHAR(to_timestamp(created_at), 'IYYY"-"IW') AS active_week,
        (date_trunc('week', to_timestamp(created_at)) )::date week_start,
        (date_trunc('week', to_timestamp(created_at)) + interval '6 days')::date week_end,
        TO_TIMESTAMP(created_at) :: date active_date,
        uid  -- 活跃表
    FROM app_user_track_log
    WHERE true
        AND event IN (1, 16)
        AND created_date >= 20240904
    GROUP BY TO_CHAR(to_timestamp(created_at), 'IYYY"-"IW'),
        (date_trunc('week', to_timestamp(created_at)) )::date,
        (date_trunc('week', to_timestamp(created_at)) + interval '6 days')::date,
        TO_TIMESTAMP(created_at) :: date,
        uid

    UNION ALL

    SELECT
        TO_CHAR(to_timestamp(created_at), 'IYYY"-"IW') AS active_week,
        (date_trunc('week', to_timestamp(created_at)) )::date week_start,
        (date_trunc('week', to_timestamp(created_at)) + interval '6 days')::date week_end,
        TO_TIMESTAMP(created_at) :: date active_date,
        uid
    FROM user_log
    WHERE true
        AND created_date >= 20240801
    GROUP BY TO_CHAR(to_timestamp(created_at), 'IYYY"-"IW'),
        (date_trunc('week', to_timestamp(created_at)) )::date,
        (date_trunc('week', to_timestamp(created_at)) + interval '6 days')::date,
        TO_TIMESTAMP(created_at) :: date,
        uid
),
t3 AS (
    SELECT
        active_week,
        COALESCE(register_date, '2024-07-01') register_date,
        active_date,
        concat(week_start, '~', week_end) week_day,
        register_week,
        week_start,
        week_end,
        COALESCE(区域, '未知') 区域,
        COALESCE(国家, '未知') 国家,
        COALESCE(系统, 'Android') 系统,
        COALESCE(campaign_id, '0') campaign_id,
        t2.uid
    FROM t2 LEFT JOIN t1 ON t1.uid = t2.uid
)
SELECT
    week_day,
    active_week,
    区域,
    国家,
    系统,
    COUNT(DISTINCT a.uid) WAU,
    COUNT(DISTINCT CASE WHEN next_active - active_date BETWEEN 7 AND 13 THEN next_week_uid  ELSE NULL END) 总次周留存,
    COUNT(DISTINCT CASE WHEN next_active - active_date BETWEEN 14 AND 20 THEN next_week_uid ELSE NULL END) 总2周留存,
    COUNT(DISTINCT CASE WHEN next_active - active_date BETWEEN 21 AND 27 THEN next_week_uid ELSE NULL END) 总3周留存,
    COUNT(DISTINCT CASE WHEN next_active - active_date BETWEEN 28 AND 34 THEN next_week_uid ELSE NULL END) 总4周留存,
    COUNT(DISTINCT CASE WHEN a.active_week = a.register_week THEN a.uid ELSE NULL END) 新用户数,
    COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND next_active - active_date BETWEEN 7 AND 13 THEN next_week_uid ELSE NULL END) 新用户次周留存,
    COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND next_active - active_date BETWEEN 14 AND 20 THEN next_week_uid ELSE NULL END) 新用户2周留存,
    COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND next_active - active_date BETWEEN 21 AND 27 THEN next_week_uid ELSE NULL END) 新用户3周留存,
    COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND next_active - active_date BETWEEN 28 AND 34 THEN next_week_uid ELSE NULL END) 新用户4周留存,
    COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND a.campaign_id NOT IN ('0', '')  THEN a.uid ELSE NULL END) 新推广用户数,
    COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND next_active - active_date BETWEEN 7 AND 13  AND a.campaign_id NOT IN ('0', '') THEN next_week_uid ELSE NULL END) 新推广用户次周留存,
    COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND next_active - active_date BETWEEN 14 AND 20 AND a.campaign_id NOT IN ('0', '') THEN next_week_uid ELSE NULL END) 新推广用户2周留存,
    COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND next_active - active_date BETWEEN 21 AND 27 AND a.campaign_id NOT IN ('0', '') THEN next_week_uid ELSE NULL END) 新推广用户3周留存,
    COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND next_active - active_date BETWEEN 28 AND 34 AND a.campaign_id NOT IN ('0', '') THEN next_week_uid ELSE NULL END) 新推广用户4周留存,
    COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND a.campaign_id IN ('0', '')  THEN a.uid ELSE NULL END) 新自然用户数,
    COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND next_active - active_date BETWEEN 7 AND 13  AND a.campaign_id IN ('0', '') THEN next_week_uid ELSE NULL END) 新自然用户次周留存,
    COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND next_active - active_date BETWEEN 14 AND 20 AND a.campaign_id IN ('0', '') THEN next_week_uid ELSE NULL END) 新自然用户2周留存,
    COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND next_active - active_date BETWEEN 21 AND 27 AND a.campaign_id IN ('0', '') THEN next_week_uid ELSE NULL END) 新自然用户3周留存,
    COUNT(DISTINCT CASE WHEN a.active_week = a.register_week AND next_active - active_date BETWEEN 28 AND 34 AND a.campaign_id IN ('0', '') THEN next_week_uid ELSE NULL END) 新自然用户4周留存,
    COUNT(DISTINCT CASE WHEN a.active_week <> a.register_week THEN a.uid ELSE NULL END) 老用户数,
    COUNT(DISTINCT CASE WHEN a.active_week <> a.register_week AND next_active - active_date BETWEEN 7 AND 13 THEN next_week_uid ELSE NULL END) 老用户次周留存,
    COUNT(DISTINCT CASE WHEN a.active_week <> a.register_week AND next_active - active_date BETWEEN 14 AND 20  THEN next_week_uid ELSE NULL END) 老用户2周留存,
    COUNT(DISTINCT CASE WHEN a.active_week <> a.register_week AND next_active - active_date BETWEEN 21 AND 27 THEN next_week_uid ELSE NULL END) 老用户3周留存,
    COUNT(DISTINCT CASE WHEN a.active_week <> a.register_week AND next_active - active_date BETWEEN 28 AND 34 THEN next_week_uid ELSE NULL END) 老用户4周留存
FROM (
    SELECT
        a.active_week,
        a.week_day,
        a.register_week,
        b.active_week next_week,
        a.区域,
        a.国家,
        a.系统,
        a.uid,
        b.uid AS next_week_uid,
        a.campaign_id,
        a.active_date,
        b.active_date AS next_active
        --, DATE_PART('isoyear', b.week_end) * 52 + DATE_PART('week', b.week_end) - (DATE_PART('isoyear', a.week_end) * 52 + DATE_PART('week',a.week_end)) AS week_difference
    FROM t3 a LEFT JOIN t3 b ON a.uid = b.uid AND b.week_end > a.week_end
) a
GROUP BY week_day,
    active_week,
    区域,
    国家,
    系统;



truncate table public.dw_retention_month;
INSERT INTO public.dw_retention_month
WITH t1 AS (
    SELECT *
    FROM (
        SELECT
            TO_CHAR(to_timestamp(created_at), 'YYYY-MM') AS register_month,
            TO_TIMESTAMP(created_at) :: date register_date,
            --, case when area = 'id' then '印尼'
            --     when area = 'kr' then '韩国'
            --     when area = 'jp' then '日本'
            --     when area = 'es' then '西班牙'
            --     when area = 'cn' then '中国(简体)'
            --   else '英语国家' end as 区域
          b.area AS 区域,
          b.country_name AS 国家,
          CASE WHEN os = 'ios' THEN 'IOS' ELSE 'Android' END AS 系统,
           campaign_id,
           uid,
           event,
           row_number() OVER (PARTITION BY uid ORDER BY created_at) rk  -- 可能存在多次归因，以第一次为准
        FROM "user_log"  a LEFT JOIN v_dim_country_area b ON UPPER(a.area) = b.country_code
        WHERE 1 = 1
            AND event = 1
            AND created_date >= 20240701
    ) a
    WHERE rk = 1
),
t2 AS (
    SELECT
        TO_CHAR(to_timestamp(created_at), 'YYYY-MM') active_month,
        uid,
        TO_TIMESTAMP(created_at) :: date active_date  -- 活跃表
    FROM app_user_track_log
    WHERE true
        AND event IN (1, 16)
        AND created_date >= 20240904
    GROUP BY TO_CHAR(to_timestamp(created_at), 'YYYY-MM'),
        uid,
        TO_TIMESTAMP(created_at) :: date

    UNION

    SELECT
        TO_CHAR(to_timestamp(created_at), 'YYYY-MM') active_month,
        uid,
        TO_TIMESTAMP(created_at) :: date active_date
    FROM user_log
    WHERE true
        AND created_date >= 20240801
    GROUP BY TO_CHAR(to_timestamp(created_at), 'YYYY-MM'),
        uid,
        TO_TIMESTAMP(created_at) :: date
),
t3 AS (
    SELECT
        active_month,
        register_month,
        COALESCE(register_date, '2024-07-01') register_date,
        active_date,
        COALESCE(区域, '未知') 区域,
        COALESCE(国家, '未知') 国家,
        COALESCE(系统, 'Android') 系统,
        COALESCE(campaign_id, '0') campaign_id,
        t2.uid
    FROM t2 LEFT JOIN t1 ON t1.uid = t2.uid
)
SELECT
    a.active_month,
    a.区域,
    a.系统,
    a.国家,
    COUNT(DISTINCT a.uid) AS MAU,
    COUNT(DISTINCT CASE WHEN next_active - active_date BETWEEN 30 AND 59 THEN next_month_uid ELSE NULL END) 总次月留存,
    COUNT(DISTINCT CASE WHEN next_active - active_date BETWEEN 60 AND 89 THEN next_month_uid ELSE NULL END) 总2月留存,
    COUNT(DISTINCT CASE WHEN next_active - active_date BETWEEN 90 AND 119  THEN next_month_uid ELSE NULL END) 总3月留存,
    COUNT(DISTINCT CASE WHEN a.active_month = a.register_month THEN a.uid ELSE NULL END) 新用户数,
    COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND next_active - active_date BETWEEN 30 AND 59 THEN next_month_uid ELSE NULL END) 新用户次月留存,
    COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND next_active - active_date BETWEEN 60 AND 89 THEN next_month_uid ELSE NULL END) 新用户2月留存,
    COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND next_active - active_date BETWEEN 90 AND 119  THEN next_month_uid ELSE NULL END) 新用户3月留存,
    COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND a.campaign_id NOT IN ('0', '') THEN a.uid ELSE NULL END) 新推广用户数,
    COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND next_active - active_date BETWEEN 30 AND 59 AND a.campaign_id NOT IN ('0', '')  THEN next_month_uid ELSE NULL END) 新推广用户次月留存,
    COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND next_active - active_date BETWEEN 60 AND 89 AND a.campaign_id NOT IN ('0', '') THEN next_month_uid ELSE NULL END) 新推广用户2月留存,
    COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND next_active - active_date BETWEEN 90 AND 119 AND a.campaign_id NOT IN ('0', '')  THEN next_month_uid ELSE NULL END) 新推广用户3月留存,
    COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND a.campaign_id IN ('0', '') THEN a.uid ELSE NULL END) 新自然用户数,
    COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND next_active - active_date BETWEEN 30 AND 59 AND a.campaign_id IN ('0', '')  THEN next_month_uid ELSE NULL END) 新自然用户次月留存,
    COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND next_active - active_date BETWEEN 60 AND 89 AND a.campaign_id IN ('0', '') THEN next_month_uid ELSE NULL END) 新自然用户2月留存,
    COUNT(DISTINCT CASE WHEN a.active_month = a.register_month AND next_active - active_date BETWEEN 90 AND 119 AND a.campaign_id IN ('0', '')  THEN next_month_uid ELSE NULL END) 新自然用户3月留存,
    COUNT(DISTINCT CASE WHEN a.active_month <> a.register_month THEN a.uid ELSE NULL END) 老用户数,
    COUNT(DISTINCT CASE WHEN a.active_month <> a.register_month AND next_active - active_date BETWEEN 30 AND 59 THEN next_month_uid ELSE NULL END) 老用户次月留存,
    COUNT(DISTINCT CASE WHEN a.active_month <> a.register_month AND next_active - active_date BETWEEN 60 AND 89 THEN next_month_uid ELSE NULL END) 老用户2月留存,
    COUNT(DISTINCT CASE WHEN a.active_month <> a.register_month AND next_active - active_date BETWEEN 90 AND 119 THEN next_month_uid ELSE NULL END) 老用户3月留存
FROM (
    SELECT
        a.active_month,
        a.区域,
        a.系统,
        a.国家,
        a.uid,
        b.uid AS next_month_uid,
        a.campaign_id,
        -- a.event,
        a.register_date,
        a.register_month,
        a.active_date,
        b.active_date AS next_active
        --, EXTRACT(YEAR FROM b.active_date) - EXTRACT(YEAR FROM a.active_date) * 12 + EXTRACT(MONTH FROM b.active_date) - EXTRACT(MONTH FROM a.active_date) AS month_difference
    FROM t3 a LEFT JOIN t3 b ON a.uid = b.uid AND b.active_month > a.active_month
) a
GROUP BY a.active_month,
    a.区域,
    a.系统,
    a.国家;

