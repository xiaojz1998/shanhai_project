WITH user_registration AS (
    SELECT
        TO_TIMESTAMP(created_at)::date AS register_date,
        created_at,
        uid
    FROM (
        SELECT
            created_at,
            uid,
            ROW_NUMBER() OVER (PARTITION BY uid ORDER BY created_at) AS rk
        FROM "user_log"
        WHERE event = 1
          AND created_date >= 20240701
    ) a
    WHERE rk = 1
),
tabs AS (
    SELECT DISTINCT CAST(unnest(string_to_array(tab_ids, ',')) AS INTEGER) AS tab_id
    FROM "oversea-api_osd_home_page"
    WHERE status = 1
    AND deleted_at IS NULL
    AND name NOT LIKE '%官网%'
),
recommends AS (
    SELECT DISTINCT
        id AS tab_id,
        CAST(unnest(string_to_array(recommend_ids, ',')) AS INTEGER) AS recommend_id
    FROM "oversea-api_osd_tabs"
    WHERE status = 1
    AND deleted_at IS NULL
    AND sort = 1
    AND name NOT LIKE '%官网%'
),
ranked_recommendations AS (
    SELECT
        b.tab_id,
        a.id,
        a.chinese_name,
        a.english_name,
        a.sort,
        ROW_NUMBER() OVER (
            PARTITION BY b.tab_id
            ORDER BY
                CASE WHEN a.sort=0 THEN 1 ELSE 0 END,
                a.sort ASC
        ) AS rank
    FROM "oversea-api_osd_recommend" a
    INNER JOIN recommends b ON a.id = b.recommend_id
    INNER JOIN tabs c ON b.tab_id = c.tab_id
    WHERE a.status = 1
    AND a.deleted_at IS NULL
    AND a.chinese_name NOT LIKE '%越南语%'
    AND a.chinese_name NOT LIKE '%土耳其%'
),
recommendation_config AS (
    SELECT DISTINCT
        id,
        chinese_name,
        english_name,
        sort
    FROM (
        SELECT
            tab_id,
            id,
            chinese_name,
            english_name,
            sort
        FROM ranked_recommendations
        WHERE rank <= 2
        ORDER BY tab_id, sort ASC
    ) tt
),
exposure_data1 AS (
    SELECT
        TO_TIMESTAMP(created_at)::date AS p_date,
        min(created_at) AS show_at,
        ed.uid,
        TRIM(BOTH '{}' FROM model_id::text)::bigint AS recommendation_id,
        CASE
            WHEN ed.uid % 10 = 1 THEN '对照组'
            WHEN ed.uid % 10 = 5 THEN '实验组'
            --ELSE NULL
        END AS group_type
    FROM public.app_user_cover_show_log ed
    INNER JOIN recommendation_config rc ON ed.model_id::bigint = rc.id
    WHERE event = 111
      AND TO_TIMESTAMP(created_at)::date BETWEEN '2025-03-26' AND '2025-03-26'
      AND CAST(ext_body::json->>'page' AS int) = 1
    GROUP BY
        TO_TIMESTAMP(created_at)::date,
        ed.uid,
        TRIM(BOTH '{}' FROM model_id::text)::bigint,
        CASE
            WHEN ed.uid % 10 = 1 THEN '对照组'
            WHEN ed.uid % 10 = 5 THEN '实验组'
            --ELSE NULL
        END
),
exposure_data AS (
    SELECT a.*
    FROM exposure_data1 a
    INNER JOIN user_registration b ON a.uid = b.uid AND a.show_at - b.created_at <= 24*60*60
),
click_data AS (
    SELECT
        TO_TIMESTAMP(cd.created_at)::date AS p_date,
        cd.uid,
        TRIM(BOTH '{}' FROM NULLIF(column1, '')::text)::bigint AS recommendation_id,
        event,
        watch_time,
        case when order_id like '%SH%' then order_id else CONCAT('SH', order_id) end AS order_id,
        CASE
            WHEN cd.uid % 10 = 1 THEN '对照组'
            WHEN cd.uid % 10 = 5 THEN '实验组'
            ELSE NULL
        END AS group_type
    FROM public.app_user_track_log cd
    LEFT JOIN user_registration ur ON cd.uid = ur.uid
    WHERE event IN (112, 1, 192, 191, 2, 13, 14)
      AND TO_TIMESTAMP(cd.created_at)::date BETWEEN '2025-03-26' AND '2025-03-26'
      AND CAST(ext_body::json->>'page' AS int) = 1
),
payment_data AS (
    SELECT
        TO_TIMESTAMP(created_at)::date AS p_date,
        uid,
        order_num,
        money * 1.0 / 100 AS total_payment_amount
    FROM public.all_order_log
    WHERE status = 1
      AND environment = 1
      AND TO_TIMESTAMP(created_at)::date BETWEEN '2025-03-26' AND '2025-03-26'
),
aggregated_exposure AS (
    SELECT
        p_date,
        recommendation_id,
        group_type,
        COUNT(DISTINCT uid) AS exposure_users,
        COUNT(*) AS exposure_times
    FROM exposure_data
    GROUP BY p_date, recommendation_id, group_type
),
aggregated_click AS (
    SELECT
        a.p_date,
        a.recommendation_id,
        a.group_type,
        COUNT(DISTINCT CASE WHEN event = 112 THEN a.uid END) AS click_users,
        COUNT(DISTINCT CASE WHEN event IN (1,2,13,14) THEN a.uid END) AS play_users,
        COUNT(DISTINCT CASE WHEN event = 192 THEN a.uid END) AS recharge_submission_users,
        COUNT(DISTINCT CASE WHEN event = 191 THEN a.uid END) AS episode_unlock_users,
        COUNT(CASE WHEN event = 112 THEN a.uid END) AS click_times,
        COUNT(CASE WHEN event IN (1,2,13,14) THEN a.uid END) AS play_times,
        COUNT(CASE WHEN event = 192 THEN a.uid END) AS recharge_submission_times,
        COUNT(CASE WHEN event = 191 THEN a.uid END) AS episode_unlocks,
        ROUND(COALESCE(SUM(CASE WHEN event = 2 THEN watch_time ELSE 0 END) / 60.0, 0)) AS watch_duration_minutes
    FROM click_data a
    INNER JOIN exposure_data b ON a.uid = b.uid AND a.p_date = b.p_date AND a.recommendation_id = b.recommendation_id
    GROUP BY a.p_date, a.recommendation_id, a.group_type
),
aggregated_payment AS (
    SELECT
        pd.p_date,
        cd.recommendation_id,
        cd.group_type,
        COALESCE(COUNT(DISTINCT pd.uid), 0) AS successful_payment_users,
        COALESCE(COUNT(distinct pd.order_num), 0) AS successful_payment_times,
        COALESCE(SUM(pd.total_payment_amount), 0) AS total_payment_amount
    FROM payment_data pd
    inner JOIN (
        SELECT DISTINCT p_date, uid, recommendation_id, order_id, group_type
        FROM click_data
        WHERE event = 192
    ) cd ON pd.order_num = cd.order_id AND pd.p_date = cd.p_date AND pd.uid = cd.uid
    inner join exposure_data b ON pd.uid = b.uid AND pd.p_date = b.p_date AND cd.recommendation_id = b.recommendation_id
    GROUP BY pd.p_date, cd.recommendation_id, cd.group_type
)


SELECT
    ae.p_date,
    ae.recommendation_id,
    rc.chinese_name AS recommendation_name,
    ae.group_type,
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
    COALESCE(ap.total_payment_amount, 0) AS total_payment_amount,
    '新用户策略AB测试' AS experiment_type
FROM aggregated_exposure ae
LEFT JOIN aggregated_click ac ON ae.p_date = ac.p_date AND ae.recommendation_id = ac.recommendation_id AND ae.group_type = ac.group_type
LEFT JOIN aggregated_payment ap ON ae.p_date = ap.p_date AND ae.recommendation_id = ap.recommendation_id AND ae.group_type = ap.group_type
INNER JOIN recommendation_config rc ON ae.recommendation_id = rc.id
ORDER BY ae.p_date, ae.recommendation_id, ae.group_type