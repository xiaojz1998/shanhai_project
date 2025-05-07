WITH user_registration AS (
    SELECT
        TO_TIMESTAMP(created_at)::date AS register_date,
        created_at,
        app_language,
        uid
    FROM (
        SELECT
            created_at,
            uid,
            app_language,
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
            WHEN ed.uid % 100 >= 10 AND ed.uid % 100 <= 19 THEN '对照组'
            WHEN ed.uid % 100 >= 0 AND ed.uid % 100 <= 9 THEN '实验组'
        END AS group_type,
        COUNT(1) AS model_display
    FROM public.app_user_cover_show_log ed
    INNER JOIN recommendation_config rc ON ed.model_id::bigint = rc.id
    WHERE event = 111
      AND TO_TIMESTAMP(created_at)::date BETWEEN '2025-04-21' AND '2025-04-21'
      AND CAST(ext_body::json->>'page' AS int) = 1
    GROUP BY
        TO_TIMESTAMP(created_at)::date,
        ed.uid,
        model_id,
        CASE
            WHEN ed.uid % 100 >= 10 AND ed.uid % 100 <= 19 THEN '对照组'
            WHEN ed.uid % 100 >= 0 AND ed.uid % 100 <= 9 THEN '实验组'
        END
),
exposure_data AS (
    SELECT
        TO_TIMESTAMP(ed.created_at)::date AS p_date,
        CASE WHEN diff_time > 24 * 60 * 60 AND diff_time <= 24 * 60 * 60 * 2 THEN '24-48h'
             WHEN diff_time > 24 * 60 * 60 * 2 AND diff_time <= 24 * 60 * 60 * 4 THEN '48-96h'
             WHEN diff_time > 24 * 60 * 60 * 4 AND diff_time <= 24 * 60 * 60 * 7 THEN '96-192h'
             WHEN diff_time > 24 * 60 * 60 * 7 THEN '>192h'
        END AS res_type,
        ed.uid,
        CASE
            WHEN ed.uid % 100 >= 10 AND ed.uid % 100 <= 19 THEN '对照组'
            WHEN ed.uid % 100 >= 0 AND ed.uid % 100 <= 9 THEN '实验组'
        END AS group_type,
        COUNT(1) AS model_display
    FROM public.app_user_cover_show_log ed
    INNER JOIN (
        SELECT DISTINCT a.uid, recommendation_id,a.show_at-b.created_at AS diff_time
        FROM exposure_data1 a
        INNER JOIN user_registration b ON a.uid = b.uid AND a.show_at - b.created_at > 24 * 60 * 60
        --WHERE b.app_language NOT IN ('vi_VN','tr_TR')
    ) c ON ed.uid = c.uid and TRIM(BOTH '{}' FROM ed.model_id::text)::bigint=c.recommendation_id
    INNER JOIN recommendation_config rc ON ed.model_id::bigint = rc.id
    WHERE event = 111
      AND TO_TIMESTAMP(ed.created_at)::date BETWEEN '2025-04-21' AND '2025-04-21'
      AND CAST(ext_body::json->>'page' AS int) = 1
    GROUP BY
        TO_TIMESTAMP(ed.created_at)::date,
        CASE WHEN diff_time > 24 * 60 * 60 AND diff_time <= 24 * 60 * 60 * 2 THEN '24-48h'
             WHEN diff_time > 24 * 60 * 60 * 2 AND diff_time <= 24 * 60 * 60 * 4 THEN '48-96h'
             WHEN diff_time > 24 * 60 * 60 * 4 AND diff_time <= 24 * 60 * 60 * 7 THEN '96-192h'
             WHEN diff_time > 24 * 60 * 60 * 7 THEN '>192h'
        END,
        ed.uid,
        CASE
            WHEN ed.uid % 100 >= 10 AND ed.uid % 100 <= 19 THEN '对照组'
            WHEN ed.uid % 100 >= 0 AND ed.uid % 100 <= 9 THEN '实验组'
            else null
        END
),
click_data AS (
    SELECT
        TO_TIMESTAMP(cd.created_at)::date AS p_date,
        cd.uid,
        cd.vid,
        cd.eid,
        TRIM(BOTH '{}' FROM NULLIF(column1, '')::text)::bigint AS recommendation_id,
        event,
        watch_time,
        CASE WHEN order_id LIKE '%SH%' THEN order_id ELSE CONCAT('SH', order_id) END AS order_id,
        CASE
            WHEN cd.uid % 100 >= 10 AND cd.uid % 100 <= 19 THEN '对照组'
            WHEN cd.uid % 100 >= 0 AND cd.uid % 100 <= 9 THEN '实验组'
            ELSE NULL
        END AS group_type
    FROM public.app_user_track_log cd
    LEFT JOIN user_registration ur ON cd.uid = ur.uid
    WHERE event IN (112, 1, 192, 191, 2, 13, 14)
      AND TO_TIMESTAMP(cd.created_at)::date BETWEEN '2025-04-21' AND '2025-04-21'
      --AND CAST(ext_body::json->>'page' AS int) = 1
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
      AND TO_TIMESTAMP(created_at)::date BETWEEN '2025-04-21' AND '2025-04-21'
),
aggregated_exposure AS (
    SELECT
        p_date,
        -- recommendation_id,
        res_type,
        group_type,
        COUNT(DISTINCT uid) AS exposure_users,
        SUM(model_display) AS exposure_times
    FROM exposure_data
    GROUP BY p_date,  group_type, res_type--recommendation_id,
),
aggregated_click AS (
    SELECT
        a.p_date,
        -- a.recommendation_id,
        a.group_type,
        b.res_type,
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
    INNER JOIN exposure_data b ON a.uid = b.uid AND a.p_date = b.p_date-- AND a.recommendation_id = b.recommendation_id
    GROUP BY a.p_date,  a.group_type, b.res_type--a.recommendation_id,
),
aggregated_payment AS (
    SELECT
        pd.p_date,
        -- cd.recommendation_id,
        cd.group_type,
        b.res_type,
        COALESCE(COUNT(DISTINCT pd.uid), 0) AS successful_payment_users,
        COALESCE(COUNT(DISTINCT pd.order_num), 0) AS successful_payment_times,
        COALESCE(SUM(pd.total_payment_amount), 0) AS total_payment_amount
    FROM payment_data pd
    INNER JOIN (
        SELECT DISTINCT p_date, uid, group_type--,  order_id --recommendation_id,
        FROM click_data
        WHERE event = 192
    ) cd ON  pd.p_date = cd.p_date AND pd.uid = cd.uid --AND pd.order_num = cd.order_id
    INNER JOIN exposure_data b ON pd.uid = b.uid AND pd.p_date = b.p_date --AND cd.recommendation_id = b.recommendation_id
    GROUP BY pd.p_date,  cd.group_type, b.res_type--cd.recommendation_id,
),
aggregated_k AS (
    SELECT
        a.p_date,
        -- cd.recommendation_id,
        cd.group_type,
        b.res_type,
        SUM(money) AS k_num
    FROM (
        SELECT TO_TIMESTAMP(created_at)::date p_date, uid, goods_id vid, goods_sku_id eid, money
        FROM "middle_user_consume_record_00"
        WHERE TO_TIMESTAMP(created_at)::date BETWEEN '2025-04-21' AND '2025-04-21' AND type=0
        UNION
        SELECT TO_TIMESTAMP(created_at)::date p_date, uid, goods_id vid, goods_sku_id eid, money
        FROM "middle_user_consume_record_01"
        WHERE TO_TIMESTAMP(created_at)::date BETWEEN '2025-04-21' AND '2025-04-21' AND type=0
        UNION
        SELECT TO_TIMESTAMP(created_at)::date p_date, uid, goods_id vid, goods_sku_id eid, money
        FROM "middle_user_consume_record_02"
        WHERE TO_TIMESTAMP(created_at)::date BETWEEN '2025-04-21' AND '2025-04-21' AND type=0
        UNION
        SELECT TO_TIMESTAMP(created_at)::date p_date, uid, goods_id vid, goods_sku_id eid, money
        FROM "middle_user_consume_record_03"
        WHERE TO_TIMESTAMP(created_at)::date BETWEEN '2025-04-21' AND '2025-04-21' AND type=0
        UNION
        SELECT TO_TIMESTAMP(created_at)::date p_date, uid, goods_id vid, goods_sku_id eid, money
        FROM "middle_user_consume_record_04"
        WHERE TO_TIMESTAMP(created_at)::date BETWEEN '2025-04-21' AND '2025-04-21' AND type=0
    ) a
    INNER JOIN (
        SELECT DISTINCT p_date, uid, vid, eid, group_type--, recommendation_id
        FROM click_data
        WHERE event = 191
    ) cd ON a.uid = cd.uid AND a.vid = cd.vid AND a.eid = cd.eid
    INNER JOIN exposure_data b ON a.uid = b.uid AND a.p_date = b.p_date --AND cd.recommendation_id = b.recommendation_id
    GROUP BY a.p_date,  cd.group_type, b.res_type --cd.recommendation_id,
),
D1retention AS (
    SELECT
        a.p_date,
        a.group_type,
        a.res_type,
        COUNT(DISTINCT a.uid) uv,
        COUNT(DISTINCT b.uid) D1_uv
    FROM exposure_data a
    LEFT JOIN public.dwd_user_active b ON a.uid = b.uid AND b.d_date - a.p_date = 1
    GROUP BY a.p_date, a.group_type, a.res_type
),
D3retention AS (
    SELECT
        a.p_date,
        a.group_type,
        a.res_type,
        COUNT(DISTINCT a.uid) uv,
        COUNT(DISTINCT b.uid) D3_uv
    FROM exposure_data a
    LEFT JOIN public.dwd_user_active b ON a.uid = b.uid AND b.d_date - a.p_date = 3
    GROUP BY a.p_date, a.group_type, a.res_type
),
D7retention AS (
    SELECT
        a.p_date,
        a.group_type,
        a.res_type,
        COUNT(DISTINCT a.uid) uv,
        COUNT(DISTINCT b.uid) D7_uv
    FROM exposure_data a
    LEFT JOIN public.dwd_user_active b ON a.uid = b.uid AND b.d_date - a.p_date = 7
    GROUP BY a.p_date, a.group_type, a.res_type
)

SELECT
    ae.p_date,
    -- ae.recommendation_id,
    -- rc.chinese_name AS recommendation_name,
    ae.group_type,
    SUM(COALESCE(ae.exposure_users, 0)) AS exposure_users,
    SUM(COALESCE(ac.click_users, 0)) AS click_users,
    SUM(COALESCE(ac.play_users, 0)) AS play_users,
    SUM(COALESCE(ac.recharge_submission_users, 0)) AS recharge_submission_users,
    SUM(COALESCE(ap.successful_payment_users, 0)) AS successful_payment_users,
    SUM(COALESCE(ac.episode_unlock_users, 0)) AS episode_unlock_users,
    SUM(COALESCE(ap.total_payment_amount, 0)) AS total_payment_amount,
    SUM(COALESCE(ak.k_num, 0)) AS k_num,
    SUM(COALESCE(ac.recharge_submission_times, 0)) AS recharge_submission_times,
    SUM(COALESCE(ap.successful_payment_times, 0)) AS successful_payment_times,
    SUM(COALESCE(ac.episode_unlocks, 0)) AS episode_unlocks,
    SUM(COALESCE(ac.watch_duration_minutes, 0)) AS watch_duration_minutes,
    CASE WHEN SUM(COALESCE(d1r.uv, 0)) = 0 THEN 0
         ELSE SUM(COALESCE(d1r.D1_uv, 0)) * 1.0 / SUM(COALESCE(d1r.uv, 0)) END AS D1_rate,
    CASE WHEN SUM(COALESCE(d3r.uv, 0)) = 0 THEN 0
         ELSE SUM(COALESCE(d3r.D3_uv, 0)) * 1.0 / SUM(COALESCE(d3r.uv, 0)) END AS D3_rate,
    CASE WHEN SUM(COALESCE(d7r.uv, 0)) = 0 THEN 0
         ELSE SUM(COALESCE(d7r.D7_uv, 0)) * 1.0 / SUM(COALESCE(d7r.uv, 0)) END AS D7_rate,
    '老用户策略AB测试' AS experiment_type
FROM aggregated_exposure ae
LEFT JOIN aggregated_click ac ON ae.p_date = ac.p_date
    -- AND ae.recommendation_id = ac.recommendation_id
    AND ae.group_type = ac.group_type
    AND ae.res_type = ac.res_type
LEFT JOIN aggregated_payment ap ON ae.p_date = ap.p_date
    -- AND ae.recommendation_id = ap.recommendation_id
    AND ae.group_type = ap.group_type
    AND ae.res_type = ap.res_type
LEFT JOIN aggregated_k ak ON ae.p_date = ak.p_date
    -- AND ae.recommendation_id = ak.recommendation_id
    AND ae.group_type = ak.group_type
    AND ae.res_type = ak.res_type
-- INNER JOIN recommendation_config rc ON ae.recommendation_id = rc.id
LEFT JOIN D1retention d1r ON ae.p_date = d1r.p_date
    AND ae.group_type = d1r.group_type
    AND ae.res_type = d1r.res_type
LEFT JOIN D3retention d3r ON ae.p_date = d3r.p_date
    AND ae.group_type = d3r.group_type
    AND ae.res_type = d3r.res_type
LEFT JOIN D7retention d7r ON ae.p_date = d7r.p_date
    AND ae.group_type = d7r.group_type
    AND ae.res_type = d7r.res_type
GROUP BY ae.p_date, ae.group_type--, ae.recommendation_id, rc.chinese_name
ORDER BY ae.p_date,  ae.group_type--ae.recommendation_id,