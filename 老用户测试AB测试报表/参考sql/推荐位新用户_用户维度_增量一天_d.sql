------------------------------------------
-- file: 推荐位新用户_用户维度_增量一天_d.新sql_v1
-- author: xiaoj
-- time: 2025/4/24 11:53
-- description:
------------------------------------------
SET timezone = 'UTC-0';

-- truncate table public.dim_playretain_user;
-- INSERT INTO public.dim_playretain_user
delete from public.dim_playretain_user where d_date between (current_date+interval '-2 day') and (current_date+interval '-1 day');
INSERT INTO public.dim_playretain_user
WITH user_registration AS (
    SELECT
        TO_TIMESTAMP(created_at) :: date AS register_date,
        TO_TIMESTAMP(created_at) AS register_timestamp,
        uid::int8 AS uid,
        country_code,
        ad_channel
    FROM (
        SELECT
            created_at,
            uid,
            area AS country_code,
            ad_channel,
            ROW_NUMBER() OVER (PARTITION BY uid ORDER BY created_at) AS rk
        FROM "user_log"
        WHERE event = 1
          AND created_date >= 20240701
    ) a
    WHERE rk = 1
),
user_exposure AS (
    SELECT
        atl.uid,
        TO_TIMESTAMP(atl.created_at)::date AS d_date,
        CAST(atl.ext_body::json ->> 'show_title' AS text) AS show_title,
        MIN(TO_TIMESTAMP(atl.created_at)) AS min_exposure_time,
        ur.register_timestamp
    FROM public.app_user_cover_show_log atl
    LEFT JOIN user_registration ur ON atl.uid = ur.uid
    WHERE event = 111
    --   AND TO_TIMESTAMP(atl.created_at)::date BETWEEN '2025-03-01' AND (current_date + INTERVAL '-1 day')
      AND TO_TIMESTAMP(atl.created_at)::date BETWEEN (current_date+interval '-2 day') and (current_date+interval '-1 day')
      AND CAST(ext_body::json ->> 'page' AS int) = 3
      AND ext_body::json ->> 'show_title' = 'playRetain'
    GROUP BY atl.uid, TO_TIMESTAMP(atl.created_at)::date, ur.register_timestamp, CAST(atl.ext_body::json ->> 'show_title' AS text)
)
SELECT
    ue.uid,
    ue.d_date,
    CASE
        WHEN ue.register_timestamp IS NULL THEN 'Unknown'
        WHEN ue.min_exposure_time <= (ue.register_timestamp + INTERVAL '24 hours') THEN 'newuser'
        ELSE 'olduser'
    END AS user_type,
    ue.show_title,
    ue.min_exposure_time,
    ue.register_timestamp
FROM user_exposure ue
;
-------------------------------------------------------
-- 表名 dim_homepage_user 用户维度表
-- 基本描述： 每一日用户曝光的基本信息维表
-- 维度： uid, 日期
-- 用处： 判断新老用户、排除越南语、土耳其语用户
-------------------------------------------------------
-- ddl:
-- CREATE TABLE public.dim_homepage_user (
--     uid bigint,
--     d_date date,
--     user_type character varying(20),             判断新老用户的字段
--     id integer,
--     chinese_name character varying(255),
--     min_exposure_time timestamp without time zone,
--     register_timestamp timestamp without time zone
-- );


-- truncate table public.dim_homepage_user;
-- INSERT INTO public.dim_homepage_user
delete from public.dim_homepage_user where d_date between (current_date+interval '-2 day') and (current_date+interval '-1 day');
INSERT INTO public.dim_homepage_user
WITH user_registration AS (
    SELECT
        TO_TIMESTAMP(created_at) :: date AS register_date,
        TO_TIMESTAMP(created_at) AS register_timestamp,
        uid::int8 AS uid,
        country_code,
        ad_channel
    FROM (
        SELECT
            created_at,
            uid,
            area AS country_code,
            ad_channel,
            ROW_NUMBER() OVER (PARTITION BY uid ORDER BY created_at) AS rk
        FROM "user_log"
        WHERE event = 1
          AND created_date >= 20240701
    ) a
    WHERE rk = 1
),
user_exposure AS (
    SELECT
        atl.uid,
        TO_TIMESTAMP(atl.created_at)::date AS d_date,
        model_id,
        MIN(TO_TIMESTAMP(atl.created_at)) AS min_exposure_time,
        ur.register_timestamp
    FROM public.app_user_cover_show_log atl
    LEFT JOIN user_registration ur ON atl.uid = ur.uid
    WHERE event = 111
      AND TO_TIMESTAMP(atl.created_at)::date BETWEEN (current_date+interval '-2 day') and (current_date+interval '-1 day')
    --   AND TO_TIMESTAMP(atl.created_at)::date BETWEEN '2025-03-10' and (current_date+interval '-1 day')
      AND CAST(ext_body::json ->> 'page' AS int) = 1
    GROUP BY atl.uid, TO_TIMESTAMP(atl.created_at)::date, ur.register_timestamp, model_id
),
tabs AS (
    SELECT DISTINCT CAST(unnest(string_to_array(tab_ids, ',')) AS INTEGER) AS tab_id
    FROM "oversea-api_osd_home_page"        --客户端首页表
    WHERE status = 1
    AND deleted_at IS NULL
    AND name NOT LIKE '%官网%'
),
recommends AS (
    SELECT DISTINCT
        id AS tab_id,
        CAST(unnest(string_to_array(recommend_ids, ',')) AS INTEGER) AS recommend_id
    FROM "oversea-api_osd_tabs"            --首页顶部tab表
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
                CASE WHEN a.sort=0 THEN 1 ELSE 0 END, -- NULLs come last
                a.sort ASC                            -- Then sort by actual sort value
        ) AS rank
    FROM "oversea-api_osd_recommend" a
    INNER JOIN recommends b ON a.id = b.recommend_id
    INNER JOIN tabs c ON b.tab_id = c.tab_id
    WHERE a.status = 1
    AND a.deleted_at IS NULL
    AND a.chinese_name not like '%越南语%'
    AND a.chinese_name not like '%土耳其%'
),
final_recommendations AS (
    SELECT
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
    GROUP BY id, chinese_name, english_name, sort
)
SELECT
    ue.uid,
    ue.d_date,
    CASE
        WHEN ue.register_timestamp IS NULL THEN 'Unknown'
        WHEN ue.min_exposure_time <= (ue.register_timestamp + INTERVAL '24 hours') THEN 'newuser'
        ELSE 'olduser'              -- 判断新老用户的 字段
    END AS user_type,
    -- ue.model_id,
    fr.id,
    fr.chinese_name,
    ue.min_exposure_time,
    ue.register_timestamp
FROM user_exposure ue
LEFT JOIN final_recommendations fr ON ue.model_id::int = fr.id
where fr.id is not null
;